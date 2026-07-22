//! Embedded REST routes for the explorer and descriptor wallet.
//!
//! Bind this router only to loopback. Watch-only wallet routes enforce bearer
//! authentication, no-store responses, and address-revelation rate limits;
//! TLS termination, token rotation, and broader authorization remain daemon
//! deployment concerns.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::wallet::{
    EmbeddedWallet, WalletAddress, WalletBalance, WalletStatus, WalletTransaction, WalletUtxo,
};

/// Explorer block summary returned by the embedded API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerBlock {
    /// Block height.
    pub height: u32,
    /// Display-order block hash.
    pub hash: String,
    /// Block timestamp.
    pub time: u64,
    /// Number of transactions in the block.
    pub transaction_count: u32,
}

/// Explorer transaction summary returned by the embedded API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerTransaction {
    /// Display-order transaction ID.
    pub txid: String,
    /// Confirmed height, if known.
    pub confirmed_height: Option<u32>,
    /// Serialized transaction size.
    pub vbytes: u32,
}

/// UTXO response for an address/script search.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerUtxo {
    /// Txid containing the output.
    pub txid: String,
    /// Output index.
    pub vout: u32,
    /// Value in satoshis.
    pub value_sats: u64,
    /// Confirmed block height.
    pub height: u32,
}

/// A bounded page of current address UTXOs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExplorerUtxoPage {
    /// UTXOs in deterministic outpoint order.
    pub utxos: Vec<ExplorerUtxo>,
    /// Zero-based number of matching entries skipped.
    pub offset: u32,
    /// Requested maximum number of returned entries.
    pub limit: u32,
    /// Whether at least one additional matching entry exists.
    pub has_more: bool,
}

/// A bounded page of current wallet UTXOs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WalletUtxoPage {
    /// Current unspent wallet outputs.
    pub utxos: Vec<WalletUtxo>,
    /// Zero-based number of entries skipped.
    pub offset: u32,
    /// Requested maximum number of returned entries.
    pub limit: u32,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
}

/// A bounded page of canonical wallet transactions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WalletTransactionPage {
    /// Canonical transactions in newest-first order.
    pub transactions: Vec<WalletTransaction>,
    /// Zero-based number of entries skipped.
    pub offset: u32,
    /// Requested maximum number of returned entries.
    pub limit: u32,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
}

/// Default UTXO page size when the query omits `limit`.
pub const DEFAULT_UTXO_PAGE_SIZE: u32 = 50;
/// Maximum accepted UTXO page size.
pub const MAX_UTXO_PAGE_SIZE: u32 = 100;
/// Maximum accepted offset, bounding work for offset-based pagination.
pub const MAX_UTXO_PAGE_OFFSET: u32 = 10_000;
const MIN_WALLET_AUTH_TOKEN_LEN: usize = 32;
const MAX_WALLET_AUTH_TOKEN_LEN: usize = 256;
const WALLET_ADDRESS_BURST: u8 = 20;
const WALLET_ADDRESS_REFILL_INTERVAL: Duration = Duration::from_secs(60);

/// In-memory bearer credential protecting every wallet route.
///
/// The value is intentionally not printable through `Debug` or `Display`.
#[derive(Clone)]
pub struct WalletAuthToken(Arc<[u8]>);

struct WalletApiState {
    wallet: Arc<EmbeddedWallet>,
    address_limiter: Mutex<AddressRateLimiter>,
}

struct AddressRateLimiter {
    available: u8,
    last_refill: Instant,
}

impl AddressRateLimiter {
    fn new() -> Self {
        Self {
            available: WALLET_ADDRESS_BURST,
            last_refill: Instant::now(),
        }
    }

    fn take(&mut self, now: Instant) -> bool {
        let intervals = now.saturating_duration_since(self.last_refill).as_secs()
            / WALLET_ADDRESS_REFILL_INTERVAL.as_secs();
        if intervals > 0 {
            let refill = u8::try_from(intervals.min(u64::from(WALLET_ADDRESS_BURST)))
                .expect("refill is capped to u8 burst");
            self.available = self
                .available
                .saturating_add(refill)
                .min(WALLET_ADDRESS_BURST);
            self.last_refill += Duration::from_secs(
                intervals.saturating_mul(WALLET_ADDRESS_REFILL_INTERVAL.as_secs()),
            );
        }
        if self.available == 0 {
            return false;
        }
        self.available -= 1;
        true
    }
}

impl WalletAuthToken {
    /// Validates an ASCII token read from an owner-only file.
    pub fn new(token: impl AsRef<str>) -> Result<Self, &'static str> {
        let token = token.as_ref().as_bytes();
        if !(MIN_WALLET_AUTH_TOKEN_LEN..=MAX_WALLET_AUTH_TOKEN_LEN).contains(&token.len())
            || !token.iter().all(u8::is_ascii_graphic)
        {
            return Err("wallet API token must be 32-256 printable ASCII bytes");
        }
        Ok(Self(Arc::from(token)))
    }

    fn authorizes(&self, header: Option<&HeaderValue>) -> bool {
        let Some(header) = header.and_then(|value| value.to_str().ok()) else {
            return false;
        };
        let Some((scheme, supplied)) = header.split_once(' ') else {
            return false;
        };
        scheme.eq_ignore_ascii_case("Bearer")
            && supplied.len() <= MAX_WALLET_AUTH_TOKEN_LEN
            && constant_time_eq(supplied.as_bytes(), &self.0)
    }
}

/// Read-only index required by explorer routes. Implement this against the node's block/tx indexes.
pub trait ExplorerIndex: Send + Sync + 'static {
    /// Returns a block summary by height.
    fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String>;
    /// Returns a transaction summary by txid.
    fn transaction(&self, txid: &str) -> Result<Option<ExplorerTransaction>, String>;
    /// Validates an address against the index network before querying storage.
    fn validate_address(&self, _address: &str) -> Result<(), String> {
        Ok(())
    }
    /// Returns a bounded slice of current UTXOs for a checked Bitcoin address.
    fn address_utxos(
        &self,
        address: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ExplorerUtxo>, String>;
}

/// Thread-safe in-memory explorer index for embedded and regtest deployments.
///
/// A production daemon should replace this with a persistent projection that is
/// updated in the same lifecycle as validated block connect/disconnect events.
#[derive(Default)]
pub struct MemoryExplorerIndex {
    blocks: RwLock<HashMap<u32, ExplorerBlock>>,
    transactions: RwLock<HashMap<String, ExplorerTransaction>>,
    address_utxos: RwLock<HashMap<String, Vec<ExplorerUtxo>>>,
}

impl MemoryExplorerIndex {
    /// Records or replaces a block summary at its height.
    pub fn upsert_block(&self, block: ExplorerBlock) {
        self.blocks
            .write()
            .expect("explorer block lock not poisoned")
            .insert(block.height, block);
    }

    /// Records or replaces a transaction summary by txid.
    pub fn upsert_transaction(&self, transaction: ExplorerTransaction) {
        self.transactions
            .write()
            .expect("explorer transaction lock not poisoned")
            .insert(transaction.txid.clone(), transaction);
    }

    /// Replaces the current UTXO projection for an address.
    pub fn set_address_utxos(&self, address: impl Into<String>, mut utxos: Vec<ExplorerUtxo>) {
        utxos.sort_unstable_by(|left, right| {
            left.txid.cmp(&right.txid).then(left.vout.cmp(&right.vout))
        });
        self.address_utxos
            .write()
            .expect("explorer address lock not poisoned")
            .insert(address.into(), utxos);
    }
}

impl ExplorerIndex for MemoryExplorerIndex {
    fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String> {
        Ok(self
            .blocks
            .read()
            .map_err(|_| "explorer block lock poisoned".to_owned())?
            .get(&height)
            .cloned())
    }

    fn transaction(&self, txid: &str) -> Result<Option<ExplorerTransaction>, String> {
        Ok(self
            .transactions
            .read()
            .map_err(|_| "explorer transaction lock poisoned".to_owned())?
            .get(txid)
            .cloned())
    }

    fn address_utxos(
        &self,
        address: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ExplorerUtxo>, String> {
        if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE + 1 {
            return Err("explorer page window exceeds limits".to_owned());
        }
        let offset = usize::try_from(offset).map_err(|error| error.to_string())?;
        let limit = usize::try_from(limit).map_err(|error| error.to_string())?;
        Ok(self
            .address_utxos
            .read()
            .map_err(|_| "explorer address lock poisoned".to_owned())?
            .get(address)
            .into_iter()
            .flatten()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect())
    }
}

/// Health response for load balancers and browser frontends.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Health {
    /// Service label.
    pub service: &'static str,
    /// Service status.
    pub status: &'static str,
}

/// Creates REST routes for the embedded read-only block explorer.
pub fn explorer_router<I: ExplorerIndex>(index: Arc<I>) -> Router {
    Router::new()
        .route("/", get(explorer_page))
        .route("/api/v1/health", get(health))
        .route("/api/v1/blocks/{height}", get(block::<I>))
        .route("/api/v1/tx/{txid}", get(transaction::<I>))
        .route("/api/v1/address/{address}/utxos", get(address_utxos::<I>))
        .with_state(index)
}

const EXPLORER_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>rBTC Explorer</title><style>
:root{color-scheme:dark;background:#0b0f14;color:#d9e2ec;font:15px system-ui,sans-serif}body{max-width:900px;margin:0 auto;padding:32px 20px}h1{color:#f7931a}section{background:#131a22;border:1px solid #263241;border-radius:10px;padding:18px;margin:16px 0}form{display:flex;gap:8px}input{flex:1;background:#0b0f14;color:inherit;border:1px solid #405166;border-radius:6px;padding:10px}button{background:#f7931a;color:#111;border:0;border-radius:6px;padding:10px 16px;font-weight:700}pre{white-space:pre-wrap;word-break:break-word;min-height:24px}.muted{color:#91a4b7}</style></head>
<body><h1>rBTC Explorer</h1><p class="muted">Local, read-only active-chain explorer</p>
<section><h2>Block height</h2><form data-kind="blocks"><input inputmode="numeric" required placeholder="Height"><button>Search</button></form><pre></pre></section>
<section><h2>Transaction</h2><form data-kind="tx"><input required placeholder="txid"><button>Search</button></form><pre></pre></section>
<section><h2>Address UTXOs</h2><form data-kind="address"><input required placeholder="Checked Bitcoin address"><button>Search</button></form><pre></pre></section>
<section><h2>Watch-only wallet</h2><p class="muted">Optional authenticated wallet; token stays only in this page's memory.</p><form id="wallet"><input type="password" autocomplete="off" required placeholder="Bearer token"><button value="status">Status</button><button value="balance">Balance</button><button value="transactions">Transactions</button><button value="utxos">UTXOs</button><button value="address">New address</button></form><pre></pre></section>
<script>for(const f of document.querySelectorAll('form[data-kind]'))f.addEventListener('submit',async e=>{e.preventDefault();const q=f.querySelector('input').value.trim(),k=f.dataset.kind,o=f.nextElementSibling;const u=k==='blocks'?`/api/v1/blocks/${encodeURIComponent(q)}`:k==='tx'?`/api/v1/tx/${encodeURIComponent(q)}`:`/api/v1/address/${encodeURIComponent(q)}/utxos`;o.textContent='Loading…';try{const r=await fetch(u);o.textContent=r.ok?JSON.stringify(await r.json(),null,2):`HTTP ${r.status}`}catch(x){o.textContent=String(x)}});const w=document.querySelector('#wallet');w.addEventListener('submit',async e=>{e.preventDefault();const t=w.querySelector('input').value,a=e.submitter.value,o=w.nextElementSibling;o.textContent='Loading…';try{const r=await fetch(`/api/v1/wallet/${a}`,{method:a==='address'?'POST':'GET',headers:{Authorization:`Bearer ${t}`}});o.textContent=r.ok?JSON.stringify(await r.json(),null,2):`HTTP ${r.status}`}catch(x){o.textContent=String(x)}});</script>
</body></html>"#;

async fn explorer_page() -> (HeaderMap, Html<&'static str>) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    (headers, Html(EXPLORER_HTML))
}

/// Creates bearer-authenticated REST routes for the in-process descriptor wallet.
pub fn wallet_router(wallet: Arc<EmbeddedWallet>, token: WalletAuthToken) -> Router {
    let state = Arc::new(WalletApiState {
        wallet,
        address_limiter: Mutex::new(AddressRateLimiter::new()),
    });
    Router::new()
        .route("/api/v1/wallet/status", get(wallet_status))
        .route("/api/v1/wallet/balance", get(wallet_balance))
        .route("/api/v1/wallet/transactions", get(wallet_transactions))
        .route("/api/v1/wallet/utxos", get(wallet_utxos))
        .route("/api/v1/wallet/address", post(next_address))
        .with_state(state)
        .route_layer(middleware::from_fn_with_state(token, require_wallet_auth))
}

async fn require_wallet_auth(
    State(token): State<WalletAuthToken>,
    request: Request,
    next: Next,
) -> Response {
    if token.authorizes(request.headers().get(header::AUTHORIZATION)) {
        let mut response = next.run(request).await;
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"rbtc-wallet\""),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

async fn health() -> Json<Health> {
    Json(Health {
        service: "rbtc",
        status: "ok",
    })
}
async fn block<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(height): Path<u32>,
) -> ApiResult<ExplorerBlock> {
    index
        .block(height)
        .map_err(internal)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}
async fn transaction<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(txid): Path<String>,
) -> ApiResult<ExplorerTransaction> {
    if txid.len() != 64 || !txid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    index
        .transaction(&txid)
        .map_err(internal)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}
async fn address_utxos<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(address): Path<String>,
    Query(query): Query<UtxoPageQuery>,
) -> ApiResult<ExplorerUtxoPage> {
    if address.is_empty() || address.len() > 128 {
        return Err(StatusCode::BAD_REQUEST);
    }
    index
        .validate_address(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_UTXO_PAGE_SIZE);
    if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit_usize = usize::try_from(limit).map_err(internal)?;
    let mut utxos = index
        .address_utxos(&address, offset, limit + 1)
        .map_err(internal)?;
    let has_more = utxos.len() > limit_usize;
    utxos.truncate(limit_usize);
    Ok(Json(ExplorerUtxoPage {
        utxos,
        offset,
        limit,
        has_more,
    }))
}
async fn wallet_balance(State(state): State<Arc<WalletApiState>>) -> ApiResult<WalletBalance> {
    state.wallet.balance().map_err(internal).map(Json)
}
async fn wallet_status(State(state): State<Arc<WalletApiState>>) -> ApiResult<WalletStatus> {
    state.wallet.status().map_err(internal).map(Json)
}
async fn wallet_transactions(
    State(state): State<Arc<WalletApiState>>,
    Query(query): Query<UtxoPageQuery>,
) -> ApiResult<WalletTransactionPage> {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_UTXO_PAGE_SIZE);
    if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit_usize = usize::try_from(limit).map_err(internal)?;
    let mut transactions = state
        .wallet
        .transactions(offset, limit + 1)
        .map_err(internal)?;
    let has_more = transactions.len() > limit_usize;
    transactions.truncate(limit_usize);
    Ok(Json(WalletTransactionPage {
        transactions,
        offset,
        limit,
        has_more,
    }))
}
async fn wallet_utxos(
    State(state): State<Arc<WalletApiState>>,
    Query(query): Query<UtxoPageQuery>,
) -> ApiResult<WalletUtxoPage> {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_UTXO_PAGE_SIZE);
    if offset > MAX_UTXO_PAGE_OFFSET || limit == 0 || limit > MAX_UTXO_PAGE_SIZE {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit_usize = usize::try_from(limit).map_err(internal)?;
    let mut utxos = state.wallet.utxos(offset, limit + 1).map_err(internal)?;
    let has_more = utxos.len() > limit_usize;
    utxos.truncate(limit_usize);
    Ok(Json(WalletUtxoPage {
        utxos,
        offset,
        limit,
        has_more,
    }))
}
async fn next_address(State(state): State<Arc<WalletApiState>>) -> ApiResult<WalletAddress> {
    let allowed = state
        .address_limiter
        .lock()
        .map_err(internal)?
        .take(Instant::now());
    if !allowed {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    state
        .wallet
        .reveal_receive_address()
        .map_err(internal)
        .map(Json)
}

type ApiResult<T> = Result<Json<T>, StatusCode>;
fn internal<E>(_: E) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

#[derive(Default, Deserialize)]
struct UtxoPageQuery {
    offset: Option<u32>,
    limit: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use bitcoin::Network;
    use tower::ServiceExt;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";

    struct TestIndex;
    impl ExplorerIndex for TestIndex {
        fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String> {
            Ok((height == 1).then(|| ExplorerBlock {
                height,
                hash: "00".into(),
                time: 1,
                transaction_count: 1,
            }))
        }
        fn transaction(&self, _: &str) -> Result<Option<ExplorerTransaction>, String> {
            Ok(None)
        }
        fn validate_address(&self, address: &str) -> Result<(), String> {
            if address == "invalid" {
                Err("invalid address".to_owned())
            } else {
                Ok(())
            }
        }
        fn address_utxos(
            &self,
            _: &str,
            offset: u32,
            limit: u32,
        ) -> Result<Vec<ExplorerUtxo>, String> {
            Ok((0..3)
                .map(|vout| ExplorerUtxo {
                    txid: format!("{vout:064x}"),
                    vout,
                    value_sats: u64::from(vout),
                    height: 1,
                })
                .skip(usize::try_from(offset).unwrap())
                .take(usize::try_from(limit).unwrap())
                .collect())
        }
    }

    #[tokio::test]
    async fn explorer_returns_health_and_not_found() {
        let app = explorer_router(Arc::new(TestIndex));
        let page = app
            .clone()
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        assert!(page.headers().contains_key(header::CONTENT_SECURITY_POLICY));
        let health = app
            .clone()
            .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let missing = app
            .oneshot(
                Request::get("/api/v1/blocks/2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn explorer_bounds_and_pages_untrusted_queries() {
        let app = explorer_router(Arc::new(TestIndex));
        let page = app
            .clone()
            .oneshot(
                Request::get("/api/v1/address/bcrt1test/utxos?offset=1&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        let body = axum::body::to_bytes(page.into_body(), 4096).await.unwrap();
        let page: ExplorerUtxoPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.utxos.len(), 1);
        assert_eq!(page.utxos[0].vout, 1);
        assert!(page.has_more);

        for uri in [
            "/api/v1/address/bcrt1test/utxos?limit=0",
            "/api/v1/address/bcrt1test/utxos?limit=101",
            "/api/v1/address/bcrt1test/utxos?offset=10001",
            "/api/v1/address/invalid/utxos",
            "/api/v1/address/bcrt1test/utxos?limit=invalid",
            "/api/v1/tx/not-a-txid",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    #[tokio::test]
    async fn wallet_routes_require_bearer_authentication() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(wallet, WalletAuthToken::new(&token).unwrap());

        for authorization in [None, Some("Bearer wrong-token")] {
            let mut request = Request::get("/api/v1/wallet/balance");
            if let Some(value) = authorization {
                request = request.header(header::AUTHORIZATION, value);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        }

        let balance = app
            .clone()
            .oneshot(
                Request::get("/api/v1/wallet/balance")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(balance.status(), StatusCode::OK);
        assert_eq!(balance.headers()[header::CACHE_CONTROL], "no-store");
        let utxos = app
            .clone()
            .oneshot(
                Request::get("/api/v1/wallet/utxos?offset=0&limit=1")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(utxos.status(), StatusCode::OK);
        let body = axum::body::to_bytes(utxos.into_body(), 4096).await.unwrap();
        let utxos: WalletUtxoPage = serde_json::from_slice(&body).unwrap();
        assert!(utxos.utxos.is_empty());
        assert!(!utxos.has_more);
        let address = app
            .clone()
            .oneshot(
                Request::post("/api/v1/wallet/address")
                    .header(header::AUTHORIZATION, format!("bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(address.status(), StatusCode::OK);
        let body = axum::body::to_bytes(address.into_body(), 4096)
            .await
            .unwrap();
        let address: WalletAddress = serde_json::from_slice(&body).unwrap();
        assert_eq!(address.index, 0);

        for _ in 1..WALLET_ADDRESS_BURST {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/api/v1/wallet/address")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let limited = app
            .oneshot(
                Request::post("/api/v1/wallet/address")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn wallet_utxo_pages_reject_unbounded_queries() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(wallet, WalletAuthToken::new(&token).unwrap());
        for route in ["utxos", "transactions"] {
            for query in ["limit=0", "limit=101", "offset=10001"] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::get(format!("/api/v1/wallet/{route}?{query}"))
                            .header(header::AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            }
        }
    }

    #[tokio::test]
    async fn wallet_history_and_status_are_authenticated_and_typed() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = Arc::new(
            EmbeddedWallet::open_or_create(
                directory.path().join("wallet.sqlite"),
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let token = "a".repeat(MIN_WALLET_AUTH_TOKEN_LEN);
        let app = wallet_router(wallet, WalletAuthToken::new(&token).unwrap());
        let request = |path: &'static str| {
            Request::get(path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        let status = app
            .clone()
            .oneshot(request("/api/v1/wallet/status"))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = axum::body::to_bytes(status.into_body(), 4096)
            .await
            .unwrap();
        let status: WalletStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.tip_height, 0);
        assert_eq!(status.issued_receive_addresses, 0);

        let history = app
            .oneshot(request("/api/v1/wallet/transactions?limit=1"))
            .await
            .unwrap();
        assert_eq!(history.status(), StatusCode::OK);
        let body = axum::body::to_bytes(history.into_body(), 4096)
            .await
            .unwrap();
        let history: WalletTransactionPage = serde_json::from_slice(&body).unwrap();
        assert!(history.transactions.is_empty());
        assert!(!history.has_more);
    }

    #[test]
    fn wallet_auth_token_rejects_short_or_non_graphic_values() {
        assert!(WalletAuthToken::new("short").is_err());
        assert!(WalletAuthToken::new(format!("{} ", "a".repeat(31))).is_err());
        assert!(WalletAuthToken::new("a".repeat(256)).is_ok());
        assert!(WalletAuthToken::new("a".repeat(257)).is_err());
        let token = WalletAuthToken::new("a".repeat(32)).unwrap();
        let oversized = HeaderValue::from_str(&format!("Bearer {}", "a".repeat(257))).unwrap();
        assert!(!token.authorizes(Some(&oversized)));
    }

    #[test]
    fn wallet_address_rate_limit_refills_one_token_per_minute() {
        let mut limiter = AddressRateLimiter::new();
        let start = limiter.last_refill;
        for _ in 0..WALLET_ADDRESS_BURST {
            assert!(limiter.take(start));
        }
        assert!(!limiter.take(start));
        assert!(limiter.take(start + WALLET_ADDRESS_REFILL_INTERVAL));
        assert!(!limiter.take(start + WALLET_ADDRESS_REFILL_INTERVAL));

        let much_later = start + Duration::from_secs(1_000 * 60);
        for _ in 0..WALLET_ADDRESS_BURST {
            assert!(limiter.take(much_later));
        }
        assert!(!limiter.take(much_later));
    }

    #[test]
    fn memory_index_returns_cloned_projections() {
        let index = MemoryExplorerIndex::default();
        index.upsert_block(ExplorerBlock {
            height: 5,
            hash: "block".into(),
            time: 10,
            transaction_count: 2,
        });
        index.set_address_utxos(
            "bcrt1test",
            vec![ExplorerUtxo {
                txid: "tx".into(),
                vout: 0,
                value_sats: 1,
                height: 5,
            }],
        );
        assert_eq!(index.block(5).unwrap().unwrap().hash, "block");
        assert_eq!(index.address_utxos("bcrt1test", 0, 100).unwrap().len(), 1);
        assert!(index.transaction("missing").unwrap().is_none());
    }
}
