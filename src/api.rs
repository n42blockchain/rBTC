//! Embedded REST routes for the explorer and descriptor wallet.
//!
//! Bind this router only to loopback by default. Authentication, rate limiting,
//! TLS termination, and wallet-change persistence belong in the daemon layer.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Html,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::wallet::{EmbeddedWallet, WalletAddress, WalletBalance};

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

/// Read-only index required by explorer routes. Implement this against the node's block/tx indexes.
pub trait ExplorerIndex: Send + Sync + 'static {
    /// Returns a block summary by height.
    fn block(&self, height: u32) -> Result<Option<ExplorerBlock>, String>;
    /// Returns a transaction summary by txid.
    fn transaction(&self, txid: &str) -> Result<Option<ExplorerTransaction>, String>;
    /// Returns current UTXOs for a checked Bitcoin address.
    fn address_utxos(&self, address: &str) -> Result<Vec<ExplorerUtxo>, String>;
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
    pub fn set_address_utxos(&self, address: impl Into<String>, utxos: Vec<ExplorerUtxo>) {
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

    fn address_utxos(&self, address: &str) -> Result<Vec<ExplorerUtxo>, String> {
        Ok(self
            .address_utxos
            .read()
            .map_err(|_| "explorer address lock poisoned".to_owned())?
            .get(address)
            .cloned()
            .unwrap_or_default())
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
<script>for(const f of document.querySelectorAll('form'))f.addEventListener('submit',async e=>{e.preventDefault();const q=f.querySelector('input').value.trim(),k=f.dataset.kind,o=f.nextElementSibling;const u=k==='blocks'?`/api/v1/blocks/${encodeURIComponent(q)}`:k==='tx'?`/api/v1/tx/${encodeURIComponent(q)}`:`/api/v1/address/${encodeURIComponent(q)}/utxos`;o.textContent='Loading…';try{const r=await fetch(u);o.textContent=r.ok?JSON.stringify(await r.json(),null,2):`HTTP ${r.status}`}catch(x){o.textContent=String(x)}});</script>
</body></html>"#;

async fn explorer_page() -> (HeaderMap, Html<&'static str>) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    (headers, Html(EXPLORER_HTML))
}

/// Creates REST routes for the in-process descriptor wallet.
pub fn wallet_router(wallet: Arc<EmbeddedWallet>) -> Router {
    Router::new()
        .route("/api/v1/wallet/balance", get(wallet_balance))
        .route("/api/v1/wallet/address", post(next_address))
        .with_state(wallet)
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
    index
        .transaction(&txid)
        .map_err(internal)?
        .ok_or(StatusCode::NOT_FOUND)
        .map(Json)
}
async fn address_utxos<I: ExplorerIndex>(
    State(index): State<Arc<I>>,
    Path(address): Path<String>,
) -> ApiResult<Vec<ExplorerUtxo>> {
    index.address_utxos(&address).map_err(internal).map(Json)
}
async fn wallet_balance(State(wallet): State<Arc<EmbeddedWallet>>) -> Json<WalletBalance> {
    Json(wallet.balance())
}
async fn next_address(State(wallet): State<Arc<EmbeddedWallet>>) -> Json<WalletAddress> {
    Json(wallet.reveal_receive_address())
}

type ApiResult<T> = Result<Json<T>, StatusCode>;
fn internal(_: String) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

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
        fn address_utxos(&self, _: &str) -> Result<Vec<ExplorerUtxo>, String> {
            Ok(vec![])
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
        assert_eq!(index.address_utxos("bcrt1test").unwrap().len(), 1);
        assert!(index.transaction("missing").unwrap().is_none());
    }
}
