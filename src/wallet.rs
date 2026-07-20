//! Embedded descriptor wallet built on Bitcoin Dev Kit (BDK).
//!
//! Private descriptors and signing keys never cross the HTTP boundary. A
//! production daemon must persist BDK's staged changes before responding with a
//! newly revealed address; the storage adapter is a service-layer responsibility.

use std::sync::Mutex;

use bdk_wallet::{KeychainKind, Wallet};
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

/// Wallet construction errors.
#[derive(Debug, Error)]
pub enum WalletError {
    /// The supplied descriptor or network is invalid.
    #[error("wallet descriptor: {0}")]
    Descriptor(#[from] bdk_wallet::descriptor::DescriptorError),
}

/// Mutex-protected BDK wallet intended to be embedded in the node process.
pub struct EmbeddedWallet {
    wallet: Mutex<Wallet>,
}

impl EmbeddedWallet {
    /// Builds a descriptor wallet. Use a public descriptor for watch-only mode.
    pub fn create(
        receive_descriptor: impl Into<String>,
        change_descriptor: impl Into<String>,
        network: Network,
    ) -> Result<Self, WalletError> {
        let wallet = Wallet::create(receive_descriptor.into(), change_descriptor.into())
            .network(network)
            .create_wallet_no_persist()?;
        Ok(Self {
            wallet: Mutex::new(wallet),
        })
    }

    /// Reveals the next receive address. Persist the resulting BDK changes before returning it from a daemon.
    #[must_use]
    pub fn reveal_receive_address(&self) -> WalletAddress {
        let mut wallet = self.wallet.lock().expect("wallet lock not poisoned");
        let address = wallet.reveal_next_address(KeychainKind::External);
        WalletAddress {
            index: address.index,
            address: address.address.to_string(),
        }
    }

    /// Returns the BDK balance categorization in satoshis.
    #[must_use]
    pub fn balance(&self) -> WalletBalance {
        let wallet = self.wallet.lock().expect("wallet lock not poisoned");
        let balance = wallet.balance();
        WalletBalance {
            confirmed: balance.confirmed.to_sat(),
            trusted_pending: balance.trusted_pending.to_sat(),
            untrusted_pending: balance.untrusted_pending.to_sat(),
            immature: balance.immature.to_sat(),
        }
    }
}
