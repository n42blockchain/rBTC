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
    ChangeSet, KeychainKind, PersistedWallet, Wallet, WalletPersister,
    chain::BlockId,
    miniscript::{Descriptor, DescriptorPublicKey},
    rusqlite::Connection,
};
use bitcoin::{Block, BlockHash, Network};
use thiserror::Error;

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
        let address = state.wallet.reveal_next_address(KeychainKind::External);
        let WalletState { wallet, database } = &mut *state;
        wallet.persist(database)?;
        Ok(WalletAddress {
            index: address.index,
            address: address.address.to_string(),
        })
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
        absolute::LockTime, blockdata::constants::genesis_block, transaction::Version,
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
