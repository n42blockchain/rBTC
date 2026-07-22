//! Embedded descriptor wallet built on Bitcoin Dev Kit (BDK).
//!
//! Private descriptors and signing keys never cross the HTTP boundary. The
//! current implementation is deliberately watch-only: it rejects secret-key
//! descriptors and commits every newly revealed address through BDK's
//! transactional SQLite persister before returning it.

use std::{
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
};

use bdk_wallet::{
    ChangeSet, KeychainKind, PersistedWallet, Update, Wallet, WalletPersister,
    chain::{BlockId, ChainPosition},
    miniscript::{Descriptor, DescriptorPublicKey},
    rusqlite::{Connection, OptionalExtension, params},
};
use bitcoin::{Block, BlockHash, Network};
use thiserror::Error;

/// BIP44-style default number of consecutive unused scripts to scan.
pub const DEFAULT_WALLET_GAP_LIMIT: u32 = 20;
/// Defensive upper bound for descriptor scan lookahead.
pub const MAX_WALLET_GAP_LIMIT: u32 = 1_000;
const MAX_DERIVATION_INDEX: u32 = (1 << 31) - 1;
const NEXT_RECEIVE_INDEX_KEY: &str = "next_receive_index";
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
}

struct WalletState {
    wallet: PersistedWallet<Connection>,
    database: Connection,
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
    let existing = database
        .query_row(
            "SELECT value FROM rbtc_wallet_metadata WHERE key = ?1",
            params![NEXT_RECEIVE_INDEX_KEY],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    if existing.is_none() {
        let next = wallet
            .spk_index()
            .last_revealed_index(KeychainKind::External)
            .map_or(0, |index| index.saturating_add(1));
        database.execute(
            "INSERT INTO rbtc_wallet_metadata (key, value) VALUES (?1, ?2)",
            params![NEXT_RECEIVE_INDEX_KEY, i64::from(next)],
        )?;
    }
    Ok(())
}

fn reserve_receive_index(database: &mut Connection) -> Result<u32, WalletError> {
    let transaction = database.transaction()?;
    let raw = transaction.query_row(
        "SELECT value FROM rbtc_wallet_metadata WHERE key = ?1",
        params![NEXT_RECEIVE_INDEX_KEY],
        |row| row.get::<_, i64>(0),
    )?;
    let index = u32::try_from(raw)
        .ok()
        .filter(|index| *index <= MAX_DERIVATION_INDEX)
        .ok_or_else(|| WalletError::Chain("receive derivation index exhausted".to_owned()))?;
    let next = index
        .checked_add(1)
        .ok_or_else(|| WalletError::Chain("receive derivation index exhausted".to_owned()))?;
    transaction.execute(
        "UPDATE rbtc_wallet_metadata SET value = ?1 WHERE key = ?2",
        params![i64::from(next), NEXT_RECEIVE_INDEX_KEY],
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
        Address, Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
        absolute::LockTime, blockdata::constants::genesis_block, hashes::Hash,
        transaction::Version,
    };

    use super::*;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";
    const PRIVATE_DESCRIPTOR: &str = "wpkh(cVpPVruEDdmutPzisEsYvtST1usBR3ntr8pXSyt6D2YYqXRyPcFW)";

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
