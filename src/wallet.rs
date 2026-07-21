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
    KeychainKind, PersistedWallet, Wallet,
    miniscript::{Descriptor, DescriptorPublicKey},
    rusqlite::Connection,
};
use bitcoin::Network;
use thiserror::Error;

/// A compact, serializable address response for the embedded wallet API.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct WalletAddress {
    /// BIP32 child index.
    pub index: u32,
    /// Network-checked Bitcoin address.
    pub address: String,
}

/// Segmented wallet balance in satoshis.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
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
}

struct WalletState {
    wallet: PersistedWallet<Connection>,
    database: Connection,
}

/// Mutex-protected, transactionally persisted BDK watch-only wallet.
pub struct EmbeddedWallet {
    state: Mutex<WalletState>,
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
                    Wallet::create(receive_descriptor, change_descriptor)
                        .network(network)
                        .create_wallet(&mut database)
                        .map_err(|error| WalletError::Create(error.to_string()))
                },
                Ok,
            )?;

        Ok(Self {
            state: Mutex::new(WalletState { wallet, database }),
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

    fn state(&self) -> Result<MutexGuard<'_, WalletState>, WalletError> {
        self.state.lock().map_err(|_| WalletError::LockPoisoned)
    }
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

    use super::*;

    const RECEIVE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/0/*)#g0w0ymmw";
    const CHANGE_DESCRIPTOR: &str = "wpkh([41f2aed0/84h/1h/0h]tpubDDFSdQWw75hk1ewbwnNpPp5DvXFRKt68ioPoyJDY752cNHKkFxPWqkqCyCf4hxrEfpuxh46QisehL3m8Bi6MsAv394QVLopwbtfvryFQNUH/1/*)#emtwewtk";
    const PRIVATE_DESCRIPTOR: &str = "wpkh(cVpPVruEDdmutPzisEsYvtST1usBR3ntr8pXSyt6D2YYqXRyPcFW)";

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
