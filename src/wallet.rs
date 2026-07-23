//! Embedded descriptor wallet built on Bitcoin Dev Kit (BDK).
//!
//! Private descriptors and signing keys never cross the HTTP boundary. The
//! current implementation is deliberately watch-only: it rejects secret-key
//! descriptors and commits every newly revealed address through BDK's
//! transactional SQLite persister before returning it.

use std::{
    collections::HashSet,
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
};

use bdk_wallet::{
    ChangeSet, KeychainKind, PersistedWallet, Update, Wallet, WalletPersister,
    chain::{BlockId, ChainPosition},
    miniscript::{Descriptor, DescriptorPublicKey},
    psbt::PsbtUtils,
    rusqlite::{Connection, OptionalExtension, params},
    signer::SignOptions,
};
use bitcoin::{
    Address, Amount, Block, BlockHash, EcdsaSighashType, FeeRate, Network, OutPoint, Psbt,
    ScriptBuf, TapSighashType, Txid, consensus::serialize, hex::DisplayHex,
};
use thiserror::Error;

use crate::{consensus::verify_transaction_scripts_with_flags, utxo::Utxo};

/// BIP44-style default number of consecutive unused scripts to scan.
pub const DEFAULT_WALLET_GAP_LIMIT: u32 = 20;
/// Defensive upper bound for descriptor scan lookahead.
pub const MAX_WALLET_GAP_LIMIT: u32 = 1_000;
/// Maximum accepted JSON descriptor configuration size.
pub const MAX_WALLET_DESCRIPTOR_CONFIG_BYTES: usize = 64 * 1024;
/// Maximum accepted wallet PSBT creation request body.
pub const MAX_WALLET_PSBT_REQUEST_BYTES: usize = 32 * 1024;
/// Maximum accepted JSON body containing an externally signed PSBT.
pub const MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES: usize = 768 * 1024;
/// Maximum recipients in one wallet-created PSBT.
pub const MAX_WALLET_PSBT_RECIPIENTS: usize = 16;
/// Maximum wallet inputs considered or explicitly selected for one PSBT.
pub const MAX_WALLET_PSBT_INPUTS: usize = 100;
/// Maximum accepted fee rate in satoshis per virtual byte.
pub const MAX_WALLET_PSBT_FEE_RATE: u64 = 1_000;
const MAX_BITCOIN_MONEY_SATS: u64 = 2_100_000_000_000_000;
const MAX_WALLET_PSBT_BYTES: usize = 512 * 1024;
const MAX_DERIVATION_INDEX: u32 = (1 << 31) - 1;
const NEXT_RECEIVE_INDEX_KEY: &str = "next_receive_index";
const NEXT_CHANGE_INDEX_KEY: &str = "next_change_index";
const SCAN_START_HEIGHT_KEY: &str = "scan_start_height";
const MAX_WALLET_PAGE_OFFSET: u32 = 10_000;
const MAX_WALLET_PAGE_READ: u32 = 101;

/// A compact, serializable address response for the embedded wallet API.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletAddress {
    /// BIP32 child index.
    pub index: u32,
    /// Network-checked Bitcoin address.
    pub address: String,
}

/// Segmented wallet balance in satoshis.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletBalance {
    /// Confirmed balance, including mature coinbase outputs.
    pub confirmed: u64,
    /// Pending balance from transactions trusted by the wallet's chain graph.
    pub trusted_pending: u64,
    /// Pending balance from transactions not yet trusted by the wallet's chain graph.
    pub untrusted_pending: u64,
    /// Coinbase funds that have not reached maturity.
    pub immature: u64,
}

/// A current unspent output controlled by the watch-only wallet.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletUtxo {
    /// Transaction containing the output.
    pub txid: String,
    /// Output index within the transaction.
    pub vout: u32,
    /// Output value in satoshis.
    pub value_sats: u64,
    /// Receive or change descriptor keychain.
    pub keychain: String,
    /// Descriptor derivation index.
    pub derivation_index: u32,
    /// Confirmation height, or none for an unconfirmed transaction.
    pub confirmed_height: Option<u32>,
}

/// A canonical transaction affecting the watch-only wallet.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletTransaction {
    /// Display-order transaction ID.
    pub txid: String,
    /// Value sent from wallet-controlled outputs.
    pub sent_sats: u64,
    /// Value received by wallet-controlled outputs.
    pub received_sats: u64,
    /// Signed net wallet value change.
    pub net_sats: i64,
    /// Transaction fee when every input amount is known.
    pub fee_sats: Option<u64>,
    /// Virtual transaction size.
    pub vbytes: u64,
    /// Active-chain confirmation height.
    pub confirmed_height: Option<u32>,
    /// Confirmation block timestamp.
    pub confirmation_time: Option<u64>,
    /// First mempool observation time, when known.
    pub first_seen: Option<u64>,
    /// Most recent mempool observation time, when known.
    pub last_seen: Option<u64>,
}

/// Durable wallet synchronization metadata.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletStatus {
    /// Wallet active-chain checkpoint height.
    pub tip_height: u32,
    /// Wallet active-chain checkpoint hash.
    pub tip_hash: String,
    /// Earliest height covered by a converged descriptor scan.
    pub scan_start_height: Option<u32>,
    /// Number of receive indices durably reserved for API issuance.
    pub issued_receive_addresses: u32,
}

/// Canonical public descriptors safe for authenticated watch-only export.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletPublicDescriptors {
    /// External/receive public descriptor, including its checksum.
    pub receive_descriptor: String,
    /// Internal/change public descriptor, including its checksum.
    pub change_descriptor: String,
}

/// One address/value output requested for an unsigned wallet PSBT.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletPsbtRecipient {
    /// Network-checked Bitcoin address.
    pub address: String,
    /// Output value in satoshis.
    pub value_sats: u64,
}

/// One current wallet outpoint selected for exclusive coin control.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletPsbtOutPoint {
    /// Transaction ID containing the selected output.
    pub txid: String,
    /// Output index within the transaction.
    pub vout: u32,
}

/// Strict request for a bounded, unsigned PSBT.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletPsbtRequest {
    /// One to sixteen recipient outputs.
    pub recipients: Vec<WalletPsbtRecipient>,
    /// Fee rate from 1 through 1,000 sat/vB.
    pub fee_rate_sat_vb: u64,
    /// Optional exclusive wallet-UTXO selection. Empty uses automatic selection.
    #[serde(default)]
    pub selected_utxos: Vec<WalletPsbtOutPoint>,
}

/// Unsigned PSBT created from validated wallet state.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletPsbt {
    /// Base64 BIP174 PSBT.
    pub psbt: String,
    /// Transaction ID of the unsigned transaction.
    pub unsigned_txid: String,
    /// Exact input value minus output value.
    pub fee_sats: u64,
    /// Number of selected transaction inputs.
    pub input_count: u32,
    /// Number of recipient plus optional change outputs.
    pub output_count: u32,
}

/// Strict request containing one externally signed, non-final PSBT.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalletPsbtFinalizeRequest {
    /// Base64 BIP174 PSBT created by this watch-only wallet and signed elsewhere.
    pub psbt: String,
}

/// Fully signed transaction verified by the pinned Bitcoin Core script engine.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WalletFinalizedTransaction {
    /// Base64 BIP174 object with final script fields populated.
    pub psbt: String,
    /// Consensus-encoded transaction in lowercase hexadecimal.
    pub transaction_hex: String,
    /// Legacy transaction identifier.
    pub txid: String,
    /// Witness transaction identifier.
    pub wtxid: String,
    /// Exact fee derived from current wallet prevouts.
    pub fee_sats: u64,
    /// Final virtual transaction size.
    pub vbytes: u64,
}

/// Parses one strict, size-bounded unsigned-PSBT creation request.
///
/// Semantic address/network, amount, fee, and coin-control checks run inside
/// [`EmbeddedWallet::create_psbt`], where the selected wallet network and UTXO
/// set are available.
pub fn parse_wallet_psbt_request(input: &[u8]) -> Result<WalletPsbtRequest, WalletError> {
    if input.len() > MAX_WALLET_PSBT_REQUEST_BYTES {
        return Err(WalletError::Psbt("request exceeds 32768 bytes"));
    }
    serde_json::from_slice(input)
        .map_err(|_| WalletError::Psbt("expected the documented strict JSON object"))
}

/// Parses one strict, size-bounded external-signature finalization request.
pub fn parse_wallet_psbt_finalize_request(
    input: &[u8],
) -> Result<WalletPsbtFinalizeRequest, WalletError> {
    if input.len() > MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES {
        return Err(WalletError::Psbt("finalize request exceeds 786432 bytes"));
    }
    serde_json::from_slice(input)
        .map_err(|_| WalletError::Psbt("expected the documented strict finalize JSON object"))
}

/// Validated watch-only descriptor configuration loaded by the daemon.
#[derive(Clone, Eq, PartialEq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalletDescriptorConfig {
    /// External/receive public descriptor.
    pub receive_descriptor: String,
    /// Internal/change public descriptor.
    pub change_descriptor: String,
    /// Bounded descriptor discovery gap.
    #[serde(default = "default_wallet_gap_limit")]
    pub gap_limit: u32,
    /// Earliest block height that may contain wallet activity.
    #[serde(default)]
    pub birthday_height: u32,
}

/// A durable checkpoint in the wallet's view of the validated active chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletTip {
    /// Block height.
    pub height: u32,
    /// Validated block hash.
    pub hash: BlockHash,
}

/// Wallet construction and persistence errors.
#[derive(Debug, Error)]
pub enum WalletError {
    /// A descriptor contained secret key material or was malformed.
    #[error("wallet requires a valid public descriptor for the {keychain} keychain")]
    PublicDescriptor {
        /// Descriptor role.
        keychain: &'static str,
    },
    /// The wallet database file could not be safely prepared.
    #[error("wallet file: {0}")]
    File(#[from] std::io::Error),
    /// SQLite could not open or update the wallet database.
    #[error("wallet database: {0}")]
    Database(#[from] bdk_wallet::rusqlite::Error),
    /// Stored wallet state did not match the requested descriptors or network.
    #[error("wallet load: {0}")]
    Load(String),
    /// A new wallet could not be created and committed.
    #[error("wallet creation: {0}")]
    Create(String),
    /// A previous wallet operation panicked while holding the state lock.
    #[error("wallet state lock is poisoned")]
    LockPoisoned,
    /// The supplied validated-chain transition did not connect to wallet state.
    #[error("wallet chain update: {0}")]
    Chain(String),
    /// A wallet query exceeded its bounded read window.
    #[error("wallet query exceeds bounded page limits")]
    QueryLimit,
    /// An unsigned PSBT request was invalid or cannot be funded.
    #[error("wallet PSBT: {0}")]
    Psbt(&'static str),
    /// The descriptor configuration container is malformed or unbounded.
    #[error("wallet descriptor configuration: {0}")]
    Configuration(&'static str),
}

struct WalletState {
    wallet: PersistedWallet<Connection>,
    database: Connection,
}

/// Parses and validates one bounded watch-only descriptor JSON object.
///
/// Parser errors deliberately never include the supplied JSON or descriptor
/// text, because rejected input may contain private key material.
pub fn parse_wallet_descriptor_config(input: &[u8]) -> Result<WalletDescriptorConfig, WalletError> {
    if input.len() > MAX_WALLET_DESCRIPTOR_CONFIG_BYTES {
        return Err(WalletError::Configuration("file exceeds 65536 bytes"));
    }
    let config: WalletDescriptorConfig = serde_json::from_slice(input)
        .map_err(|_| WalletError::Configuration("expected the documented JSON object"))?;
    if !(1..=MAX_WALLET_GAP_LIMIT).contains(&config.gap_limit) {
        return Err(WalletError::Configuration(
            "gap_limit must be between 1 and 1000",
        ));
    }
    public_descriptor(&config.receive_descriptor, "receive")?;
    public_descriptor(&config.change_descriptor, "change")?;
    Ok(config)
}

const fn default_wallet_gap_limit() -> u32 {
    DEFAULT_WALLET_GAP_LIMIT
}

/// Mutex-protected, transactionally persisted BDK watch-only wallet.
pub struct EmbeddedWallet {
    state: Mutex<WalletState>,
    receive_descriptor: Descriptor<DescriptorPublicKey>,
    change_descriptor: Descriptor<DescriptorPublicKey>,
    network: Network,
}

impl EmbeddedWallet {
    /// Opens an existing wallet or creates one at `database_path`.
    ///
    /// Both descriptors must contain public keys only. On load, BDK verifies the
    /// exact receive descriptor, change descriptor, and Bitcoin network before
    /// any wallet state is exposed.
    pub fn open_or_create(
        database_path: impl AsRef<Path>,
        receive_descriptor: impl AsRef<str>,
        change_descriptor: impl AsRef<str>,
        network: Network,
    ) -> Result<Self, WalletError> {
        let receive_descriptor = public_descriptor(receive_descriptor.as_ref(), "receive")?;
        let change_descriptor = public_descriptor(change_descriptor.as_ref(), "change")?;
        let mut database = open_database(database_path.as_ref())?;

        let wallet = Wallet::load()
            .descriptor(KeychainKind::External, Some(receive_descriptor.clone()))
            .descriptor(KeychainKind::Internal, Some(change_descriptor.clone()))
            .check_network(network)
            .load_wallet(&mut database)
            .map_err(|error| WalletError::Load(error.to_string()))?
            .map_or_else(
                || {
                    Wallet::create(receive_descriptor.clone(), change_descriptor.clone())
                        .network(network)
                        .create_wallet(&mut database)
                        .map_err(|error| WalletError::Create(error.to_string()))
                },
                Ok,
            )?;
        initialize_metadata(&mut database, &wallet)?;

        Ok(Self {
            state: Mutex::new(WalletState { wallet, database }),
            receive_descriptor,
            change_descriptor,
            network,
        })
    }

    /// Reveals and transactionally persists the next receive address.
    ///
    /// A failed SQLite commit is returned to the caller and BDK retains the
    /// staged changes for a later persistence attempt.
    pub fn reveal_receive_address(&self) -> Result<WalletAddress, WalletError> {
        let mut state = self.state()?;
        let WalletState { wallet, database } = &mut *state;
        let index = reserve_receive_index(database)?;
        wallet
            .reveal_addresses_to(KeychainKind::External, index)
            .for_each(drop);
        let address = wallet.peek_address(KeychainKind::External, index);
        wallet.persist(database)?;
        Ok(WalletAddress {
            index: address.index,
            address: address.address.to_string(),
        })
    }

    /// Extends both descriptor keychains to maintain `gap_limit` unused scripts.
    ///
    /// Returns `true` when new scripts were revealed and historical blocks must
    /// be replayed to determine whether the window needs extending again.
    pub fn ensure_scan_lookahead(&self, gap_limit: u32) -> Result<bool, WalletError> {
        if !(1..=MAX_WALLET_GAP_LIMIT).contains(&gap_limit) {
            return Err(WalletError::Chain(format!(
                "gap limit must be between 1 and {MAX_WALLET_GAP_LIMIT}"
            )));
        }
        let mut state = self.state()?;
        let mut changed = false;
        for keychain in [KeychainKind::External, KeychainKind::Internal] {
            let last_used = state.wallet.spk_index().last_used_index(keychain);
            let target = last_used.map_or(gap_limit - 1, |index| {
                index.saturating_add(gap_limit).min(MAX_DERIVATION_INDEX)
            });
            let last_revealed = state.wallet.spk_index().last_revealed_index(keychain);
            if last_revealed.is_none_or(|index| index < target) {
                state
                    .wallet
                    .reveal_addresses_to(keychain, target)
                    .for_each(drop);
                changed = true;
            }
        }
        if changed {
            let WalletState { wallet, database } = &mut *state;
            wallet.persist(database)?;
        }
        Ok(changed)
    }

    /// Returns the earliest height covered by the last converged descriptor scan.
    pub fn scan_start_height(&self) -> Result<Option<u32>, WalletError> {
        let state = self.state()?;
        read_metadata_u32(&state.database, SCAN_START_HEIGHT_KEY)
    }

    /// Records a successfully converged descriptor scan boundary.
    pub fn record_scan_start_height(&self, height: u32) -> Result<(), WalletError> {
        let state = self.state()?;
        state.database.execute(
            "INSERT INTO rbtc_wallet_metadata (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = MIN(value, excluded.value)",
            params![SCAN_START_HEIGHT_KEY, i64::from(height)],
        )?;
        Ok(())
    }

    /// Advances the wallet chain through already-validated headers without
    /// scanning raw blocks before a configured descriptor birthday.
    pub fn advance_checkpoint(&self, target: WalletTip) -> Result<(), WalletError> {
        let mut state = self.state()?;
        let current = state.wallet.latest_checkpoint();
        if target.height <= current.height() {
            let existing = current.get(target.height).ok_or_else(|| {
                WalletError::Chain(format!("missing checkpoint at height {}", target.height))
            })?;
            if existing.hash() != target.hash {
                return Err(WalletError::Chain(format!(
                    "checkpoint hash mismatch at height {}",
                    target.height
                )));
            }
            return Ok(());
        }
        let update = current
            .push(BlockId {
                height: target.height,
                hash: target.hash,
            })
            .map_err(|_| WalletError::Chain("checkpoint height did not advance".to_owned()))?;
        state
            .wallet
            .apply_update(Update {
                chain: Some(update),
                ..Update::default()
            })
            .map_err(|error| WalletError::Chain(error.to_string()))?;
        let WalletState { wallet, database } = &mut *state;
        wallet.persist(database)?;
        Ok(())
    }

    /// Returns the BDK balance categorization in satoshis.
    pub fn balance(&self) -> Result<WalletBalance, WalletError> {
        let state = self.state()?;
        let balance = state.wallet.balance();
        Ok(WalletBalance {
            confirmed: balance.confirmed.to_sat(),
            trusted_pending: balance.trusted_pending.to_sat(),
            untrusted_pending: balance.untrusted_pending.to_sat(),
            immature: balance.immature.to_sat(),
        })
    }

    /// Returns a bounded slice of current wallet UTXOs.
    pub fn utxos(&self, offset: u32, limit: u32) -> Result<Vec<WalletUtxo>, WalletError> {
        ensure_query_window(offset, limit)?;
        let state = self.state()?;
        Ok(state
            .wallet
            .list_unspent()
            .skip(usize::try_from(offset).unwrap_or(usize::MAX))
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(|output| WalletUtxo {
                txid: output.outpoint.txid.to_string(),
                vout: output.outpoint.vout,
                value_sats: output.txout.value.to_sat(),
                keychain: match output.keychain {
                    KeychainKind::External => "receive",
                    KeychainKind::Internal => "change",
                }
                .to_owned(),
                derivation_index: output.derivation_index,
                confirmed_height: output.chain_position.confirmation_height_upper_bound(),
            })
            .collect())
    }

    /// Returns canonical wallet transactions in newest-first order.
    pub fn transactions(
        &self,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<WalletTransaction>, WalletError> {
        ensure_query_window(offset, limit)?;
        let state = self.state()?;
        let wallet = &state.wallet;
        Ok(wallet
            .transactions_sort_by(|left, right| {
                right
                    .chain_position
                    .cmp(&left.chain_position)
                    .then_with(|| right.tx_node.txid.cmp(&left.tx_node.txid))
            })
            .into_iter()
            .skip(usize::try_from(offset).unwrap_or(usize::MAX))
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(|transaction| {
                let tx = &transaction.tx_node.tx;
                let (sent, received) = wallet.sent_and_received(tx);
                let (confirmed_height, confirmation_time, first_seen, last_seen) =
                    match transaction.chain_position {
                        ChainPosition::Confirmed { anchor, .. } => (
                            Some(anchor.block_id.height),
                            Some(anchor.confirmation_time),
                            None,
                            None,
                        ),
                        ChainPosition::Unconfirmed {
                            first_seen,
                            last_seen,
                        } => (None, None, first_seen, last_seen),
                    };
                let sent_sats = sent.to_sat();
                let received_sats = received.to_sat();
                WalletTransaction {
                    txid: transaction.tx_node.txid.to_string(),
                    sent_sats,
                    received_sats,
                    net_sats: i64::try_from(received_sats).expect("Bitcoin amount fits i64")
                        - i64::try_from(sent_sats).expect("Bitcoin amount fits i64"),
                    fee_sats: wallet.calculate_fee(tx).ok().map(bitcoin::Amount::to_sat),
                    vbytes: u64::try_from(tx.vsize()).unwrap_or(u64::MAX),
                    confirmed_height,
                    confirmation_time,
                    first_seen,
                    last_seen,
                }
            })
            .collect())
    }

    /// Returns durable chain, scan, and address-issuance state.
    pub fn status(&self) -> Result<WalletStatus, WalletError> {
        let state = self.state()?;
        let tip = state.wallet.latest_checkpoint().block_id();
        let issued_receive_addresses = read_metadata_u32(&state.database, NEXT_RECEIVE_INDEX_KEY)?
            .ok_or_else(|| WalletError::Chain("missing receive issuance cursor".to_owned()))?;
        Ok(WalletStatus {
            tip_height: tip.height,
            tip_hash: tip.hash.to_string(),
            scan_start_height: read_metadata_u32(&state.database, SCAN_START_HEIGHT_KEY)?,
            issued_receive_addresses,
        })
    }

    /// Returns canonical public descriptors without scan-policy metadata.
    ///
    /// Both stored descriptor types are statically public-key-only, so this
    /// export cannot contain private key material. The returned two-field JSON
    /// object is accepted by [`parse_wallet_descriptor_config`] with default
    /// gap and birthday policy.
    #[must_use]
    pub fn public_descriptors(&self) -> WalletPublicDescriptors {
        WalletPublicDescriptors {
            receive_descriptor: self.receive_descriptor.to_string(),
            change_descriptor: self.change_descriptor.to_string(),
        }
    }

    /// Creates and persists change derivation for a bounded unsigned PSBT.
    ///
    /// A non-empty `selected_utxos` list is exclusive: automatic coin
    /// selection cannot add other wallet outputs. The PSBT is returned only
    /// after BDK's change revelation is committed to SQLite.
    pub fn create_psbt(&self, request: &WalletPsbtRequest) -> Result<WalletPsbt, WalletError> {
        let recipients = validate_psbt_recipients(self.network, request)?;
        let selected = validate_psbt_outpoints(request)?;
        let fee_rate = FeeRate::from_sat_per_vb(request.fee_rate_sat_vb)
            .filter(|_| (1..=MAX_WALLET_PSBT_FEE_RATE).contains(&request.fee_rate_sat_vb))
            .ok_or(WalletError::Psbt(
                "fee rate must be between 1 and 1000 sat/vB",
            ))?;
        if self
            .receive_descriptor
            .desc_type()
            .segwit_version()
            .is_none()
            || self
                .change_descriptor
                .desc_type()
                .segwit_version()
                .is_none()
        {
            return Err(WalletError::Psbt(
                "PSBT creation requires SegWit or Taproot wallet descriptors",
            ));
        }
        let mut state = self.state()?;
        if selected.is_empty()
            && state
                .wallet
                .list_unspent()
                .take(MAX_WALLET_PSBT_INPUTS + 1)
                .count()
                > MAX_WALLET_PSBT_INPUTS
        {
            return Err(WalletError::Psbt(
                "automatic coin selection exceeds the 100-input wallet bound",
            ));
        }
        let psbt = {
            let WalletState { wallet, database } = &mut *state;
            let change_index = reserve_change_index(database)?;
            wallet
                .reveal_addresses_to(KeychainKind::Internal, change_index)
                .for_each(drop);
            let change_script = wallet
                .peek_address(KeychainKind::Internal, change_index)
                .address
                .script_pubkey();
            wallet.persist(database)?;
            let mut builder = wallet.build_tx();
            builder.fee_rate(fee_rate);
            builder.drain_to(change_script);
            builder.only_witness_utxo();
            for (script, amount) in recipients {
                builder.add_recipient(script, amount);
            }
            if !selected.is_empty() {
                builder.add_utxos(&selected).map_err(|_| {
                    WalletError::Psbt("selected UTXO is not a current wallet output")
                })?;
                builder.manually_selected_only();
            }
            builder.finish().map_err(|_| {
                WalletError::Psbt("transaction cannot be funded under the requested policy")
            })?
        };
        if psbt.inputs.len() > MAX_WALLET_PSBT_INPUTS
            || psbt.serialize().len() > MAX_WALLET_PSBT_BYTES
            || psbt.inputs.iter().any(|input| {
                !input.partial_sigs.is_empty()
                    || input.final_script_sig.is_some()
                    || input.final_script_witness.is_some()
            })
        {
            return Err(WalletError::Psbt(
                "constructed PSBT exceeds bounds or is not unsigned",
            ));
        }
        let WalletState { wallet, database } = &mut *state;
        wallet.persist(database)?;
        let fee_sats = psbt
            .fee_amount()
            .ok_or(WalletError::Psbt(
                "constructed PSBT is missing input values",
            ))?
            .to_sat();
        Ok(WalletPsbt {
            psbt: psbt.to_string(),
            unsigned_txid: psbt.unsigned_tx.compute_txid().to_string(),
            fee_sats,
            input_count: u32::try_from(psbt.inputs.len()).expect("PSBT input bound fits u32"),
            output_count: u32::try_from(psbt.outputs.len()).expect("PSBT output bound fits u32"),
        })
    }

    /// Finalizes and consensus-verifies a PSBT signed by an external device.
    ///
    /// Every input must still be an unspent output in this wallet and its
    /// submitted `witness_utxo` must match the local transaction graph. The
    /// watch-only wallet never signs; BDK assembles final scripts from partial
    /// signatures and Bitcoin Core's consensus engine verifies the result.
    pub fn finalize_psbt(
        &self,
        request: &WalletPsbtFinalizeRequest,
    ) -> Result<WalletFinalizedTransaction, WalletError> {
        self.finalize_psbt_with_transaction(request)
            .map(|(response, _)| response)
    }

    /// Finalizes a PSBT and also returns its consensus-verified transaction.
    ///
    /// This is the handoff used by an external relay layer so it cannot
    /// substitute different bytes after wallet and consensus verification.
    pub fn finalize_psbt_with_transaction(
        &self,
        request: &WalletPsbtFinalizeRequest,
    ) -> Result<(WalletFinalizedTransaction, bitcoin::Transaction), WalletError> {
        if request.psbt.len() > MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES {
            return Err(WalletError::Psbt("encoded PSBT exceeds request bound"));
        }
        let mut psbt = Psbt::from_str(&request.psbt)
            .map_err(|_| WalletError::Psbt("PSBT is not valid base64 BIP174"))?;
        if psbt.serialize().len() > MAX_WALLET_PSBT_BYTES
            || !(1..=MAX_WALLET_PSBT_INPUTS).contains(&psbt.inputs.len())
            || !(1..=MAX_WALLET_PSBT_RECIPIENTS + 1).contains(&psbt.outputs.len())
            || psbt.inputs.iter().any(|input| {
                input.non_witness_utxo.is_some()
                    || input.final_script_sig.is_some()
                    || input.final_script_witness.is_some()
            })
        {
            return Err(WalletError::Psbt(
                "PSBT size, input/output count, or finalization state is invalid",
            ));
        }
        if !psbt_uses_safe_sighashes(&psbt) {
            return Err(WalletError::Psbt(
                "PSBT signatures must use SIGHASH_ALL or Taproot default",
            ));
        }

        let state = self.state()?;
        let (prevouts, fee_sats) = validated_wallet_prevouts(&state, &psbt)?;

        let finalized = state
            .wallet
            .finalize_psbt(&mut psbt, SignOptions::default())
            .map_err(|_| WalletError::Psbt("PSBT signatures cannot be finalized"))?;
        if !finalized
            || psbt.inputs.iter().any(|input| {
                input.final_script_sig.is_none() && input.final_script_witness.is_none()
            })
        {
            return Err(WalletError::Psbt(
                "PSBT does not contain sufficient valid partial signatures",
            ));
        }
        let transaction = psbt
            .clone()
            .extract_tx()
            .map_err(|_| WalletError::Psbt("finalized transaction fee is unsafe"))?;
        let vbytes = u64::try_from(transaction.vsize()).unwrap_or(u64::MAX);
        let maximum_fee = vbytes
            .checked_mul(MAX_WALLET_PSBT_FEE_RATE)
            .and_then(|fee| fee.checked_add(1_000))
            .ok_or(WalletError::Psbt("finalized transaction fee is unsafe"))?;
        if fee_sats == 0 || fee_sats > maximum_fee {
            return Err(WalletError::Psbt("finalized transaction fee is unsafe"));
        }
        verify_transaction_scripts_with_flags(
            &transaction,
            &prevouts,
            bitcoinconsensus::VERIFY_ALL_PRE_TAPROOT | bitcoinconsensus::VERIFY_TAPROOT,
        )
        .map_err(|_| WalletError::Psbt("finalized transaction signatures are invalid"))?;

        let response = WalletFinalizedTransaction {
            psbt: psbt.to_string(),
            transaction_hex: serialize(&transaction).to_lower_hex_string(),
            txid: transaction.compute_txid().to_string(),
            wtxid: transaction.compute_wtxid().to_string(),
            fee_sats,
            vbytes,
        };
        Ok((response, transaction))
    }

    /// Returns the latest persisted validated-chain checkpoint.
    pub fn chain_tip(&self) -> Result<WalletTip, WalletError> {
        let state = self.state()?;
        Ok(wallet_tip(state.wallet.latest_checkpoint().block_id()))
    }

    /// Returns wallet checkpoints from newest to oldest.
    ///
    /// The daemon uses these to locate a common ancestor after an active-chain
    /// reorganization without trusting unexecuted headers as wallet state.
    pub fn chain_checkpoints(&self) -> Result<Vec<WalletTip>, WalletError> {
        let state = self.state()?;
        Ok(state
            .wallet
            .checkpoints()
            .map(|checkpoint| wallet_tip(checkpoint.block_id()))
            .collect())
    }

    /// Indexes a block only after the node has fully validated and committed it.
    ///
    /// BDK filters the block against the wallet's revealed scripts and stages
    /// the chain and transaction-graph changes together. The SQLite commit is
    /// completed before this method returns success.
    pub fn apply_validated_block(&self, block: &Block, height: u32) -> Result<(), WalletError> {
        let mut state = self.state()?;
        state
            .wallet
            .apply_block(block, height)
            .map_err(|error| WalletError::Chain(error.to_string()))?;
        let WalletState { wallet, database } = &mut *state;
        wallet.persist(database)?;
        Ok(())
    }

    /// Durably removes wallet chain checkpoints above `target`.
    ///
    /// BDK deliberately keeps transactions learned from disconnected blocks in
    /// its graph so they can become confirmed again or be superseded. Removing
    /// their anchors from the local chain makes them non-confirmed immediately.
    pub fn rewind_to(&self, target: WalletTip) -> Result<(), WalletError> {
        let mut state = self.state()?;
        let current = state.wallet.latest_checkpoint();
        let Some(checkpoint) = current.get(target.height) else {
            return Err(WalletError::Chain(format!(
                "missing checkpoint at target height {}",
                target.height
            )));
        };
        if checkpoint.hash() != target.hash {
            return Err(WalletError::Chain(format!(
                "checkpoint hash mismatch at target height {}",
                target.height
            )));
        }
        if current.height() <= target.height {
            return Ok(());
        }

        // Persist any previously staged retry before writing an explicit local
        // chain removal. One SQLite persister call is transactional.
        let WalletState { wallet, database } = &mut *state;
        wallet.persist(database)?;
        let mut changeset = ChangeSet::default();
        for checkpoint in current.iter().take_while(|cp| cp.height() > target.height) {
            changeset
                .local_chain
                .blocks
                .insert(checkpoint.height(), None);
        }
        <Connection as WalletPersister>::persist(database, &changeset)?;

        // PersistedWallet does not expose LocalChain::disconnect_from mutably.
        // Reloading applies the exact durable changeset to both the in-memory
        // chain and transaction graph while retaining revealed indices.
        *wallet = load_wallet(
            database,
            &self.receive_descriptor,
            &self.change_descriptor,
            self.network,
        )?;
        Ok(())
    }

    fn state(&self) -> Result<MutexGuard<'_, WalletState>, WalletError> {
        self.state.lock().map_err(|_| WalletError::LockPoisoned)
    }
}

fn validate_psbt_recipients(
    network: Network,
    request: &WalletPsbtRequest,
) -> Result<Vec<(ScriptBuf, Amount)>, WalletError> {
    if !(1..=MAX_WALLET_PSBT_RECIPIENTS).contains(&request.recipients.len()) {
        return Err(WalletError::Psbt(
            "recipient count must be between 1 and 16",
        ));
    }
    let mut total = 0_u64;
    request
        .recipients
        .iter()
        .map(|recipient| {
            if recipient.address.len() > 128
                || recipient.value_sats == 0
                || recipient.value_sats > MAX_BITCOIN_MONEY_SATS
            {
                return Err(WalletError::Psbt("recipient address or value is invalid"));
            }
            total = total
                .checked_add(recipient.value_sats)
                .filter(|total| *total <= MAX_BITCOIN_MONEY_SATS)
                .ok_or(WalletError::Psbt("recipient total exceeds MoneyRange"))?;
            let address = Address::from_str(&recipient.address)
                .ok()
                .and_then(|address| address.require_network(network).ok())
                .ok_or(WalletError::Psbt(
                    "recipient address is invalid for the wallet network",
                ))?;
            Ok((
                address.script_pubkey(),
                Amount::from_sat(recipient.value_sats),
            ))
        })
        .collect()
}

fn validate_psbt_outpoints(request: &WalletPsbtRequest) -> Result<Vec<OutPoint>, WalletError> {
    if request.selected_utxos.len() > MAX_WALLET_PSBT_INPUTS {
        return Err(WalletError::Psbt(
            "selected UTXO count exceeds the 100-input bound",
        ));
    }
    let mut unique = HashSet::with_capacity(request.selected_utxos.len());
    request
        .selected_utxos
        .iter()
        .map(|selected| {
            if selected.txid.len() != 64
                || !selected.txid.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(WalletError::Psbt("selected UTXO outpoint is invalid"));
            }
            let outpoint = OutPoint {
                txid: Txid::from_str(&selected.txid)
                    .map_err(|_| WalletError::Psbt("selected UTXO outpoint is invalid"))?,
                vout: selected.vout,
            };
            if !unique.insert(outpoint) {
                return Err(WalletError::Psbt("selected UTXOs must be unique"));
            }
            Ok(outpoint)
        })
        .collect()
}

fn validated_wallet_prevouts(
    state: &WalletState,
    psbt: &Psbt,
) -> Result<(Vec<Utxo>, u64), WalletError> {
    let mut seen = HashSet::with_capacity(psbt.unsigned_tx.input.len());
    let mut prevouts = Vec::with_capacity(psbt.unsigned_tx.input.len());
    let mut input_value = 0_u64;
    for (transaction_input, psbt_input) in psbt.unsigned_tx.input.iter().zip(&psbt.inputs) {
        let outpoint = transaction_input.previous_output;
        if !seen.insert(outpoint) {
            return Err(WalletError::Psbt("PSBT inputs must be unique"));
        }
        let local = state.wallet.get_utxo(outpoint).ok_or(WalletError::Psbt(
            "PSBT input is not a current wallet output",
        ))?;
        if psbt_input.witness_utxo.as_ref() != Some(&local.txout) {
            return Err(WalletError::Psbt(
                "PSBT witness UTXO does not match current wallet state",
            ));
        }
        let confirmation_height = local
            .chain_position
            .confirmation_height_upper_bound()
            .unwrap_or(0);
        let is_coinbase = state
            .wallet
            .get_tx(outpoint.txid)
            .is_some_and(|transaction| transaction.tx_node.tx.is_coinbase());
        if is_coinbase
            && state.wallet.latest_checkpoint().height() < confirmation_height.saturating_add(100)
        {
            return Err(WalletError::Psbt(
                "PSBT input is an immature coinbase output",
            ));
        }
        input_value = input_value
            .checked_add(local.txout.value.to_sat())
            .filter(|value| *value <= MAX_BITCOIN_MONEY_SATS)
            .ok_or(WalletError::Psbt("PSBT input value exceeds MoneyRange"))?;
        prevouts.push(Utxo {
            value_sats: local.txout.value.to_sat(),
            height: confirmation_height,
            is_coinbase,
            last_touched: 0,
            creation_mtp: 0,
            script_pubkey: local.txout.script_pubkey.into_bytes(),
        });
    }
    let output_value = psbt
        .unsigned_tx
        .output
        .iter()
        .try_fold(0_u64, |total, output| {
            total
                .checked_add(output.value.to_sat())
                .filter(|value| *value <= MAX_BITCOIN_MONEY_SATS)
                .ok_or(WalletError::Psbt("PSBT output value exceeds MoneyRange"))
        })?;
    let fee = input_value
        .checked_sub(output_value)
        .ok_or(WalletError::Psbt("PSBT outputs exceed wallet inputs"))?;
    Ok((prevouts, fee))
}

fn psbt_uses_safe_sighashes(psbt: &Psbt) -> bool {
    psbt.inputs.iter().all(|input| {
        let declared = input.sighash_type;
        (declared.is_none()
            || declared == Some(EcdsaSighashType::All.into())
            || declared == Some(TapSighashType::Default.into()))
            && input
                .partial_sigs
                .values()
                .all(|signature| signature.sighash_type == EcdsaSighashType::All)
            && input.tap_key_sig.as_ref().is_none_or(|signature| {
                matches!(
                    signature.sighash_type,
                    TapSighashType::Default | TapSighashType::All
                )
            })
            && input.tap_script_sigs.values().all(|signature| {
                matches!(
                    signature.sighash_type,
                    TapSighashType::Default | TapSighashType::All
                )
            })
    })
}

fn wallet_tip(block: BlockId) -> WalletTip {
    WalletTip {
        height: block.height,
        hash: block.hash,
    }
}

fn load_wallet(
    database: &mut Connection,
    receive_descriptor: &Descriptor<DescriptorPublicKey>,
    change_descriptor: &Descriptor<DescriptorPublicKey>,
    network: Network,
) -> Result<PersistedWallet<Connection>, WalletError> {
    Wallet::load()
        .descriptor(KeychainKind::External, Some(receive_descriptor.clone()))
        .descriptor(KeychainKind::Internal, Some(change_descriptor.clone()))
        .check_network(network)
        .load_wallet(database)
        .map_err(|error| WalletError::Load(error.to_string()))?
        .ok_or_else(|| WalletError::Load("persisted wallet disappeared during reload".to_owned()))
}

fn initialize_metadata(
    database: &mut Connection,
    wallet: &PersistedWallet<Connection>,
) -> Result<(), WalletError> {
    database.execute_batch(
        "CREATE TABLE IF NOT EXISTS rbtc_wallet_metadata (
            key TEXT PRIMARY KEY NOT NULL,
            value INTEGER NOT NULL CHECK (value >= 0)
        );",
    )?;
    for (key, keychain) in [
        (NEXT_RECEIVE_INDEX_KEY, KeychainKind::External),
        (NEXT_CHANGE_INDEX_KEY, KeychainKind::Internal),
    ] {
        let existing = database
            .query_row(
                "SELECT value FROM rbtc_wallet_metadata WHERE key = ?1",
                params![key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if existing.is_none() {
            let next = wallet
                .spk_index()
                .last_revealed_index(keychain)
                .map_or(0, |index| index.saturating_add(1));
            database.execute(
                "INSERT INTO rbtc_wallet_metadata (key, value) VALUES (?1, ?2)",
                params![key, i64::from(next)],
            )?;
        }
    }
    Ok(())
}

fn reserve_receive_index(database: &mut Connection) -> Result<u32, WalletError> {
    reserve_derivation_index(database, NEXT_RECEIVE_INDEX_KEY, "receive")
}

fn reserve_change_index(database: &mut Connection) -> Result<u32, WalletError> {
    reserve_derivation_index(database, NEXT_CHANGE_INDEX_KEY, "change")
}

fn reserve_derivation_index(
    database: &mut Connection,
    key: &str,
    label: &'static str,
) -> Result<u32, WalletError> {
    let transaction = database.transaction()?;
    let raw = transaction.query_row(
        "SELECT value FROM rbtc_wallet_metadata WHERE key = ?1",
        params![key],
        |row| row.get::<_, i64>(0),
    )?;
    let index = u32::try_from(raw)
        .ok()
        .filter(|index| *index <= MAX_DERIVATION_INDEX)
        .ok_or_else(|| WalletError::Chain(format!("{label} derivation index exhausted")))?;
    let next = index
        .checked_add(1)
        .ok_or_else(|| WalletError::Chain(format!("{label} derivation index exhausted")))?;
    transaction.execute(
        "UPDATE rbtc_wallet_metadata SET value = ?1 WHERE key = ?2",
        params![i64::from(next), key],
    )?;
    transaction.commit()?;
    Ok(index)
}

fn read_metadata_u32(database: &Connection, key: &str) -> Result<Option<u32>, WalletError> {
    database
        .query_row(
            "SELECT value FROM rbtc_wallet_metadata WHERE key = ?1",
            params![key],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map(|raw| {
            u32::try_from(raw)
                .map_err(|_| WalletError::Chain(format!("wallet metadata {key} is out of range")))
        })
        .transpose()
}

fn ensure_query_window(offset: u32, limit: u32) -> Result<(), WalletError> {
    if offset > MAX_WALLET_PAGE_OFFSET || limit == 0 || limit > MAX_WALLET_PAGE_READ {
        return Err(WalletError::QueryLimit);
    }
    Ok(())
}

fn public_descriptor(
    descriptor: &str,
    keychain: &'static str,
) -> Result<Descriptor<DescriptorPublicKey>, WalletError> {
    // Do not include parser diagnostics: they may echo a supplied secret.
    Descriptor::from_str(descriptor).map_err(|_| WalletError::PublicDescriptor { keychain })
}

fn open_database(path: &Path) -> Result<Connection, WalletError> {
    #[cfg(unix)]
    restrict_wallet_file(path)?;
    Ok(Connection::open(path)?)
}

#[cfg(unix)]
fn restrict_wallet_file(path: &Path) -> Result<(), std::io::Error> {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "wallet database must be a regular file, not a symlink",
                ));
            }
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map(drop),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bitcoin::{
        Address, Amount, OutPoint, Psbt, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime, blockdata::constants::genesis_block, hashes::Hash,
        transaction::Version,
    };

    use super::*;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";
    const PRIVATE_DESCRIPTOR: &str = "wpkh(cVpPVruEDdmutPzisEsYvtST1usBR3ntr8pXSyt6D2YYqXRyPcFW)";

    fn signing_descriptors() -> (String, String, String, String) {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let receive = bitcoin::PrivateKey::from_slice(&[1; 32], Network::Testnet).unwrap();
        let change = bitcoin::PrivateKey::from_slice(&[2; 32], Network::Testnet).unwrap();
        (
            format!("wpkh({receive})"),
            format!("wpkh({change})"),
            format!("wpkh({})", receive.public_key(&secp)),
            format!("wpkh({})", change.public_key(&secp)),
        )
    }

    fn paying_block(parent: BlockHash, address: &str, value: u64) -> Block {
        let script_pubkey = Address::from_str(address)
            .unwrap()
            .require_network(Network::Testnet)
            .unwrap()
            .script_pubkey();
        let transaction = Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![1, 1]),
                sequence: Sequence::MAX,
                witness: Witness::default(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(value),
                script_pubkey,
            }],
        };
        let mut block = Block {
            header: genesis_block(Network::Testnet).header,
            txdata: vec![transaction],
        };
        block.header.prev_blockhash = parent;
        block.header.time = block.header.time.saturating_add(1);
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        block
    }

    #[test]
    fn revealed_addresses_survive_restart_without_reuse() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");

        let first = {
            let wallet = EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap();
            wallet.reveal_receive_address().unwrap()
        };
        let second = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap()
        .reveal_receive_address()
        .unwrap();

        assert_eq!(first.index, 0);
        assert_eq!(second.index, 1);
        assert_ne!(first.address, second.address);
    }

    #[test]
    fn validated_block_balance_and_tip_survive_restart() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let genesis = genesis_block(Network::Testnet).block_hash();
        let (address, block, expected_tip) = {
            let wallet = EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap();
            let address = wallet.reveal_receive_address().unwrap();
            let block = paying_block(genesis, &address.address, 42_000);
            wallet.apply_validated_block(&block, 1).unwrap();
            let expected_tip = WalletTip {
                height: 1,
                hash: block.block_hash(),
            };
            assert_eq!(wallet.chain_tip().unwrap(), expected_tip);
            assert_eq!(wallet.balance().unwrap().immature, 42_000);
            let utxos = wallet.utxos(0, 10).unwrap();
            assert_eq!(utxos.len(), 1);
            assert_eq!(utxos[0].value_sats, 42_000);
            assert_eq!(utxos[0].confirmed_height, Some(1));
            let transactions = wallet.transactions(0, 10).unwrap();
            assert_eq!(transactions.len(), 1);
            assert_eq!(transactions[0].received_sats, 42_000);
            assert_eq!(transactions[0].net_sats, 42_000);
            assert_eq!(transactions[0].confirmed_height, Some(1));
            assert_eq!(wallet.transactions(1, 10).unwrap(), Vec::new());
            assert!(matches!(
                wallet.transactions(0, 0),
                Err(WalletError::QueryLimit)
            ));
            let status = wallet.status().unwrap();
            assert_eq!(status.tip_height, 1);
            assert_eq!(status.tip_hash, expected_tip.hash.to_string());
            assert_eq!(status.scan_start_height, None);
            assert_eq!(status.issued_receive_addresses, 1);
            (address, block, expected_tip)
        };

        let wallet = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        assert_eq!(wallet.chain_tip().unwrap(), expected_tip);
        assert_eq!(wallet.balance().unwrap().immature, 42_000);
        assert_eq!(wallet.utxos(1, 10).unwrap(), Vec::new());
        assert_eq!(
            block.txdata[0].output[0].script_pubkey,
            Address::from_str(&address.address)
                .unwrap()
                .require_network(Network::Testnet)
                .unwrap()
                .script_pubkey()
        );
    }

    #[test]
    fn rewind_removes_confirmed_anchor_and_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let genesis = genesis_block(Network::Testnet).block_hash();
        let wallet = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        let address = wallet.reveal_receive_address().unwrap();
        let block = paying_block(genesis, &address.address, 21_000);
        wallet.apply_validated_block(&block, 1).unwrap();
        wallet
            .rewind_to(WalletTip {
                height: 0,
                hash: genesis,
            })
            .unwrap();
        assert_eq!(wallet.chain_tip().unwrap().height, 0);
        assert_eq!(wallet.balance().unwrap().immature, 0);
        assert!(wallet.transactions(0, 10).unwrap().is_empty());
        drop(wallet);

        let wallet = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        assert_eq!(
            wallet.chain_tip().unwrap(),
            WalletTip {
                height: 0,
                hash: genesis,
            }
        );
        assert_eq!(wallet.balance().unwrap().immature, 0);
        assert_eq!(wallet.reveal_receive_address().unwrap().index, 1);
    }

    #[test]
    fn lookahead_scan_replays_older_blocks_without_advancing_issued_addresses() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let wallet = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        assert!(wallet.ensure_scan_lookahead(30).unwrap());
        assert!(!wallet.ensure_scan_lookahead(30).unwrap());
        let (address_zero, address_edge, address_beyond_lookahead) = {
            let state = wallet.state().unwrap();
            (
                state
                    .wallet
                    .peek_address(KeychainKind::External, 0)
                    .address
                    .to_string(),
                state
                    .wallet
                    .peek_address(KeychainKind::External, 29)
                    .address
                    .to_string(),
                state
                    .wallet
                    .peek_address(KeychainKind::External, 60)
                    .address
                    .to_string(),
            )
        };
        let genesis = genesis_block(Network::Testnet).block_hash();
        let mut first = paying_block(genesis, &address_edge, 10_000);
        first.txdata[0].output.push(TxOut {
            value: Amount::from_sat(20_000),
            script_pubkey: Address::from_str(&address_beyond_lookahead)
                .unwrap()
                .require_network(Network::Testnet)
                .unwrap()
                .script_pubkey(),
        });
        first.header.merkle_root = first.compute_merkle_root().unwrap();
        let second = paying_block(first.block_hash(), &address_zero, 30_000);
        wallet.apply_validated_block(&first, 1).unwrap();
        wallet.apply_validated_block(&second, 2).unwrap();
        assert_eq!(wallet.balance().unwrap().immature, 40_000);

        assert!(wallet.ensure_scan_lookahead(30).unwrap());
        wallet.apply_validated_block(&first, 1).unwrap();
        wallet.apply_validated_block(&second, 2).unwrap();
        assert_eq!(wallet.chain_tip().unwrap().height, 2);
        assert_eq!(wallet.balance().unwrap().immature, 60_000);
        assert!(wallet.ensure_scan_lookahead(30).unwrap());
        wallet.apply_validated_block(&first, 1).unwrap();
        wallet.apply_validated_block(&second, 2).unwrap();
        assert!(!wallet.ensure_scan_lookahead(30).unwrap());
        assert_eq!(
            wallet
                .transactions(0, 10)
                .unwrap()
                .into_iter()
                .map(|transaction| transaction.confirmed_height)
                .collect::<Vec<_>>(),
            vec![Some(2), Some(1)]
        );

        assert_eq!(wallet.reveal_receive_address().unwrap().index, 0);
        assert_eq!(wallet.reveal_receive_address().unwrap().index, 1);
    }

    #[test]
    fn sparse_birthday_checkpoint_persists_and_connects_next_block() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let target = WalletTip {
            height: 100,
            hash: BlockHash::from_byte_array([7; 32]),
        };
        {
            let wallet = EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap();
            assert_eq!(wallet.scan_start_height().unwrap(), None);
            wallet.record_scan_start_height(100).unwrap();
            wallet.record_scan_start_height(200).unwrap();
            wallet.record_scan_start_height(10).unwrap();
            wallet.advance_checkpoint(target).unwrap();
            assert_eq!(wallet.chain_tip().unwrap(), target);
        }

        let wallet = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        assert_eq!(wallet.chain_tip().unwrap(), target);
        assert_eq!(wallet.scan_start_height().unwrap(), Some(10));
        let address = wallet.reveal_receive_address().unwrap();
        let block = paying_block(target.hash, &address.address, 55_000);
        wallet.apply_validated_block(&block, 101).unwrap();
        assert_eq!(wallet.chain_tip().unwrap().height, 101);
        assert_eq!(wallet.balance().unwrap().immature, 55_000);
    }

    #[test]
    fn concurrent_address_revelation_is_serialized_and_persisted() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let wallet = std::sync::Arc::new(
            EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );
        let threads = (0..8)
            .map(|_| {
                let wallet = std::sync::Arc::clone(&wallet);
                std::thread::spawn(move || wallet.reveal_receive_address().unwrap())
            })
            .collect::<Vec<_>>();
        let addresses = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            addresses
                .iter()
                .map(|address| address.index)
                .collect::<HashSet<_>>()
                .len(),
            8
        );
        drop(wallet);

        let next = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap()
        .reveal_receive_address()
        .unwrap();
        assert_eq!(next.index, 8);
    }

    #[test]
    fn private_descriptors_are_rejected_without_echoing_them() {
        let directory = tempfile::tempdir().unwrap();
        let error = EmbeddedWallet::open_or_create(
            directory.path().join("wallet.sqlite"),
            PRIVATE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .err()
        .expect("private descriptor must be rejected");

        let message = error.to_string();
        assert!(message.contains("public descriptor"));
        assert!(!message.contains("cVpPV"));
    }

    #[test]
    fn public_descriptor_export_roundtrips_through_strict_import() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = EmbeddedWallet::open_or_create(
            directory.path().join("wallet.sqlite"),
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        let exported = wallet.public_descriptors();
        assert_eq!(
            public_descriptor(&exported.receive_descriptor, "receive").unwrap(),
            public_descriptor(RECEIVE_DESCRIPTOR, "receive").unwrap()
        );
        assert_eq!(
            public_descriptor(&exported.change_descriptor, "change").unwrap(),
            public_descriptor(CHANGE_DESCRIPTOR, "change").unwrap()
        );
        let encoded = serde_json::to_vec(&exported).unwrap();
        let imported = parse_wallet_descriptor_config(&encoded).unwrap();
        assert_eq!(imported.receive_descriptor, exported.receive_descriptor);
        assert_eq!(imported.change_descriptor, exported.change_descriptor);
        assert_eq!(imported.gap_limit, DEFAULT_WALLET_GAP_LIMIT);
        assert_eq!(imported.birthday_height, 0);
        assert!(!String::from_utf8(encoded).unwrap().contains("prv"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn unsigned_psbt_coin_control_persists_unique_change_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let (request, selected_outpoint, recipient_script) = {
            let wallet = EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap();
            let funded = wallet.reveal_receive_address().unwrap();
            let block = paying_block(
                genesis_block(Network::Testnet).block_hash(),
                &funded.address,
                100_000,
            );
            wallet.apply_validated_block(&block, 1).unwrap();
            wallet
                .advance_checkpoint(WalletTip {
                    height: 101,
                    hash: BlockHash::from_byte_array([9; 32]),
                })
                .unwrap();
            let recipient = wallet.reveal_receive_address().unwrap();
            let recipient_script = Address::from_str(&recipient.address)
                .unwrap()
                .require_network(Network::Testnet)
                .unwrap()
                .script_pubkey();
            let selected = wallet.utxos(0, 1).unwrap().remove(0);
            (
                WalletPsbtRequest {
                    recipients: vec![WalletPsbtRecipient {
                        address: recipient.address,
                        value_sats: 50_000,
                    }],
                    fee_rate_sat_vb: 2,
                    selected_utxos: Vec::new(),
                },
                WalletPsbtOutPoint {
                    txid: selected.txid,
                    vout: selected.vout,
                },
                recipient_script,
            )
        };

        let first = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap()
        .create_psbt(&request)
        .unwrap();
        let first_psbt = Psbt::from_str(&first.psbt).unwrap();
        assert_eq!(
            first.unsigned_txid,
            first_psbt.unsigned_tx.compute_txid().to_string()
        );
        assert_eq!(first.input_count, 1);
        assert_eq!(first.output_count, 2);
        assert!(first.fee_sats > 0);
        assert!(
            first_psbt
                .inputs
                .iter()
                .all(|input| input.partial_sigs.is_empty()
                    && input.final_script_sig.is_none()
                    && input.final_script_witness.is_none()
                    && input.witness_utxo.is_some()
                    && input.non_witness_utxo.is_none())
        );
        let first_change = first_psbt
            .unsigned_tx
            .output
            .iter()
            .find(|output| output.script_pubkey != recipient_script)
            .unwrap()
            .script_pubkey
            .clone();
        drop(first_psbt);

        let mut selected_request = request.clone();
        selected_request.selected_utxos.push(selected_outpoint);
        let second = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap()
        .create_psbt(&selected_request)
        .unwrap();
        let second_psbt = Psbt::from_str(&second.psbt).unwrap();
        let second_change = second_psbt
            .unsigned_tx
            .output
            .iter()
            .find(|output| output.script_pubkey != recipient_script)
            .unwrap()
            .script_pubkey
            .clone();
        assert_ne!(first_change, second_change);
    }

    #[test]
    fn unsigned_psbt_rejects_unbounded_invalid_and_unknown_coin_control() {
        let directory = tempfile::tempdir().unwrap();
        let wallet = EmbeddedWallet::open_or_create(
            directory.path().join("wallet.sqlite"),
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .unwrap();
        let recipient = wallet.reveal_receive_address().unwrap();
        let mut request = WalletPsbtRequest {
            recipients: vec![WalletPsbtRecipient {
                address: recipient.address,
                value_sats: 1,
            }],
            fee_rate_sat_vb: 0,
            selected_utxos: Vec::new(),
        };
        assert!(matches!(
            wallet.create_psbt(&request),
            Err(WalletError::Psbt(
                "fee rate must be between 1 and 1000 sat/vB"
            ))
        ));
        request.fee_rate_sat_vb = 1;
        request.recipients.clear();
        assert!(matches!(
            wallet.create_psbt(&request),
            Err(WalletError::Psbt(
                "recipient count must be between 1 and 16"
            ))
        ));
        request.recipients.push(WalletPsbtRecipient {
            address: "1BitcoinEaterAddressDontSendf59kuE".to_owned(),
            value_sats: 1,
        });
        assert!(matches!(
            wallet.create_psbt(&request),
            Err(WalletError::Psbt(
                "recipient address is invalid for the wallet network"
            ))
        ));
        request.recipients[0].address = wallet.reveal_receive_address().unwrap().address;
        request.selected_utxos = vec![WalletPsbtOutPoint {
            txid: Txid::all_zeros().to_string(),
            vout: 0,
        }];
        assert!(matches!(
            wallet.create_psbt(&request),
            Err(WalletError::Psbt(
                "selected UTXO is not a current wallet output"
            ))
        ));

        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(parse_wallet_psbt_request(&encoded).unwrap(), request);
        assert!(
            parse_wallet_psbt_request(br#"{"recipients":[],"fee_rate_sat_vb":1,"unknown":true}"#)
                .is_err()
        );
        assert!(parse_wallet_psbt_request(&vec![b' '; MAX_WALLET_PSBT_REQUEST_BYTES + 1]).is_err());
    }

    #[test]
    fn unsigned_psbt_rejects_legacy_descriptors_before_coin_selection() {
        let directory = tempfile::tempdir().unwrap();
        let receive = RECEIVE_DESCRIPTOR
            .split_once('#')
            .map_or(RECEIVE_DESCRIPTOR, |(descriptor, _)| descriptor)
            .replacen("wpkh(", "pkh(", 1);
        let change = CHANGE_DESCRIPTOR
            .split_once('#')
            .map_or(CHANGE_DESCRIPTOR, |(descriptor, _)| descriptor)
            .replacen("wpkh(", "pkh(", 1);
        let wallet = EmbeddedWallet::open_or_create(
            directory.path().join("wallet.sqlite"),
            receive,
            change,
            Network::Testnet,
        )
        .unwrap();
        let recipient = wallet.reveal_receive_address().unwrap();
        let request = WalletPsbtRequest {
            recipients: vec![WalletPsbtRecipient {
                address: recipient.address,
                value_sats: 50_000,
            }],
            fee_rate_sat_vb: 2,
            selected_utxos: Vec::new(),
        };

        assert!(matches!(
            wallet.create_psbt(&request),
            Err(WalletError::Psbt(
                "PSBT creation requires SegWit or Taproot wallet descriptors"
            ))
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn externally_signed_psbt_is_finalized_and_consensus_verified() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        let (private_receive, private_change, public_receive, public_change) =
            signing_descriptors();
        let wallet = EmbeddedWallet::open_or_create(
            &database,
            &public_receive,
            &public_change,
            Network::Testnet,
        )
        .unwrap();
        let funded = wallet.reveal_receive_address().unwrap();
        let block = paying_block(
            genesis_block(Network::Testnet).block_hash(),
            &funded.address,
            100_000,
        );
        wallet.apply_validated_block(&block, 1).unwrap();
        wallet
            .advance_checkpoint(WalletTip {
                height: 101,
                hash: BlockHash::from_byte_array([8; 32]),
            })
            .unwrap();
        let created = wallet
            .create_psbt(&WalletPsbtRequest {
                recipients: vec![WalletPsbtRecipient {
                    address: funded.address,
                    value_sats: 50_000,
                }],
                fee_rate_sat_vb: 2,
                selected_utxos: Vec::new(),
            })
            .unwrap();
        let mut partial = Psbt::from_str(&created.psbt).unwrap();
        let signer = Wallet::create(private_receive, private_change)
            .network(Network::Testnet)
            .create_wallet_no_persist()
            .unwrap();
        let complete = signer
            .sign(
                &mut partial,
                SignOptions {
                    trust_witness_utxo: true,
                    try_finalize: false,
                    ..SignOptions::default()
                },
            )
            .unwrap();
        assert!(!complete);
        assert!(partial.inputs.iter().all(|input| {
            !input.partial_sigs.is_empty()
                && input.final_script_sig.is_none()
                && input.final_script_witness.is_none()
        }));

        let finalized = wallet
            .finalize_psbt(&WalletPsbtFinalizeRequest {
                psbt: partial.to_string(),
            })
            .unwrap();
        let final_psbt = Psbt::from_str(&finalized.psbt).unwrap();
        let transaction = final_psbt.extract_tx().unwrap();
        assert_eq!(finalized.txid, transaction.compute_txid().to_string());
        assert_eq!(finalized.wtxid, transaction.compute_wtxid().to_string());
        assert_eq!(
            finalized.transaction_hex,
            serialize(&transaction).to_lower_hex_string()
        );
        assert_eq!(finalized.fee_sats, created.fee_sats);
        assert_eq!(
            finalized.vbytes,
            u64::try_from(transaction.vsize()).unwrap()
        );
        assert!(
            transaction
                .input
                .iter()
                .all(|input| !input.witness.is_empty())
        );
    }

    #[test]
    fn finalized_psbt_rejects_tampering_and_untrusted_prevouts() {
        let directory = tempfile::tempdir().unwrap();
        let (private_receive, private_change, public_receive, public_change) =
            signing_descriptors();
        let wallet = EmbeddedWallet::open_or_create(
            directory.path().join("wallet.sqlite"),
            &public_receive,
            &public_change,
            Network::Testnet,
        )
        .unwrap();
        let funded = wallet.reveal_receive_address().unwrap();
        let block = paying_block(
            genesis_block(Network::Testnet).block_hash(),
            &funded.address,
            100_000,
        );
        wallet.apply_validated_block(&block, 1).unwrap();
        wallet
            .advance_checkpoint(WalletTip {
                height: 101,
                hash: BlockHash::from_byte_array([7; 32]),
            })
            .unwrap();
        let created = wallet
            .create_psbt(&WalletPsbtRequest {
                recipients: vec![WalletPsbtRecipient {
                    address: funded.address,
                    value_sats: 50_000,
                }],
                fee_rate_sat_vb: 2,
                selected_utxos: Vec::new(),
            })
            .unwrap();
        let mut partial = Psbt::from_str(&created.psbt).unwrap();
        Wallet::create(private_receive, private_change)
            .network(Network::Testnet)
            .create_wallet_no_persist()
            .unwrap()
            .sign(
                &mut partial,
                SignOptions {
                    trust_witness_utxo: true,
                    try_finalize: false,
                    ..SignOptions::default()
                },
            )
            .unwrap();

        let mut wrong_prevout = partial.clone();
        wrong_prevout.inputs[0].witness_utxo.as_mut().unwrap().value += Amount::from_sat(1);
        assert!(matches!(
            wallet.finalize_psbt(&WalletPsbtFinalizeRequest {
                psbt: wrong_prevout.to_string()
            }),
            Err(WalletError::Psbt(
                "PSBT witness UTXO does not match current wallet state"
            ))
        ));

        let mut unsafe_sighash = partial.clone();
        unsafe_sighash.inputs[0]
            .partial_sigs
            .values_mut()
            .next()
            .unwrap()
            .sighash_type = EcdsaSighashType::None;
        assert!(matches!(
            wallet.finalize_psbt(&WalletPsbtFinalizeRequest {
                psbt: unsafe_sighash.to_string()
            }),
            Err(WalletError::Psbt(
                "PSBT signatures must use SIGHASH_ALL or Taproot default"
            ))
        ));

        let mut damaged_signature = partial;
        damaged_signature.unsigned_tx.output[0].value -= Amount::from_sat(1);
        assert!(matches!(
            wallet.finalize_psbt(&WalletPsbtFinalizeRequest {
                psbt: damaged_signature.to_string()
            }),
            Err(WalletError::Psbt(
                "finalized transaction signatures are invalid"
            ))
        ));
    }

    #[test]
    fn strict_wallet_psbt_request_parser_is_bounded() {
        let request = parse_wallet_psbt_request(
            br#"{"recipients":[{"address":"tb1qexample","value_sats":50000}],"fee_rate_sat_vb":2}"#,
        )
        .unwrap();
        assert_eq!(request.recipients.len(), 1);
        assert!(request.selected_utxos.is_empty());
        assert!(
            parse_wallet_psbt_request(br#"{"recipients":[],"fee_rate_sat_vb":1,"unknown":true}"#)
                .is_err()
        );
        assert!(parse_wallet_psbt_request(&vec![b' '; MAX_WALLET_PSBT_REQUEST_BYTES + 1]).is_err());
    }

    #[test]
    fn strict_wallet_psbt_finalize_parser_is_bounded() {
        let request =
            parse_wallet_psbt_finalize_request(br#"{"psbt":"cHNidP8BAAoCAAAA"}"#).unwrap();
        assert_eq!(request.psbt, "cHNidP8BAAoCAAAA");
        assert!(parse_wallet_psbt_finalize_request(br#"{"psbt":"x","unknown":true}"#).is_err());
        assert!(
            parse_wallet_psbt_finalize_request(&vec![
                b' ';
                MAX_WALLET_PSBT_FINALIZE_REQUEST_BYTES + 1
            ])
            .is_err()
        );
    }

    #[test]
    fn descriptor_configuration_is_bounded_strict_and_watch_only() {
        let valid = serde_json::json!({
            "receive_descriptor": RECEIVE_DESCRIPTOR,
            "change_descriptor": CHANGE_DESCRIPTOR
        });
        let config = parse_wallet_descriptor_config(valid.to_string().as_bytes()).unwrap();
        assert_eq!(config.gap_limit, DEFAULT_WALLET_GAP_LIMIT);
        assert_eq!(config.birthday_height, 0);

        for invalid in [
            serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR,
                "gap_limit": 0
            }),
            serde_json::json!({
                "receive_descriptor": RECEIVE_DESCRIPTOR,
                "change_descriptor": CHANGE_DESCRIPTOR,
                "unknown": true
            }),
        ] {
            assert!(parse_wallet_descriptor_config(invalid.to_string().as_bytes()).is_err());
        }

        let secret = serde_json::json!({
            "receive_descriptor": PRIVATE_DESCRIPTOR,
            "change_descriptor": CHANGE_DESCRIPTOR
        });
        let error = parse_wallet_descriptor_config(secret.to_string().as_bytes())
            .err()
            .expect("private descriptor configuration must fail");
        assert!(error.to_string().contains("public descriptor"));
        assert!(!error.to_string().contains("cVpPV"));

        let oversized = vec![b' '; MAX_WALLET_DESCRIPTOR_CONFIG_BYTES + 1];
        assert!(matches!(
            parse_wallet_descriptor_config(&oversized),
            Err(WalletError::Configuration("file exceeds 65536 bytes"))
        ));
    }

    #[test]
    fn persisted_wallet_rejects_network_and_descriptor_mismatch() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        drop(
            EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );

        let network_error = EmbeddedWallet::open_or_create(
            &database,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Bitcoin,
        )
        .err()
        .expect("network mismatch must fail");
        assert!(matches!(network_error, WalletError::Load(_)));

        let descriptor_error = EmbeddedWallet::open_or_create(
            &database,
            CHANGE_DESCRIPTOR,
            RECEIVE_DESCRIPTOR,
            Network::Testnet,
        )
        .err()
        .expect("descriptor mismatch must fail");
        assert!(matches!(descriptor_error, WalletError::Load(_)));
    }

    #[cfg(unix)]
    #[test]
    fn wallet_database_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("wallet.sqlite");
        drop(
            EmbeddedWallet::open_or_create(
                &database,
                RECEIVE_DESCRIPTOR,
                CHANGE_DESCRIPTOR,
                Network::Testnet,
            )
            .unwrap(),
        );

        let mode = std::fs::metadata(database).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn wallet_database_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target.sqlite");
        std::fs::File::create(&target).unwrap();
        let link = directory.path().join("wallet.sqlite");
        symlink(target, &link).unwrap();

        let error = EmbeddedWallet::open_or_create(
            link,
            RECEIVE_DESCRIPTOR,
            CHANGE_DESCRIPTOR,
            Network::Testnet,
        )
        .err()
        .expect("symlink must be rejected");
        assert!(matches!(error, WalletError::File(_)));
    }
}
