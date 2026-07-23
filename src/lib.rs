//! rBTC's storage and validation kernel.
//!
//! This crate intentionally keeps consensus, storage, snapshot, and transport
//! boundaries explicit. It is not yet a complete mainnet node; see
//! `docs/ROADMAP.md` for the completion gates.

pub mod api;
pub mod archive;
pub mod block_execution;
pub mod blockchain;
pub mod chain_store;
pub mod chainstate;
pub mod consensus;
pub mod deployments;
pub mod execution_store;
pub mod explorer_store;
pub mod fee_estimator;
pub mod header_store;
pub mod headers;
pub mod ibd;
pub mod ledger;
#[cfg(feature = "mdbx")]
pub mod mdbx_utxo;
pub mod merkle_proof;
pub mod p2p;
pub mod peer_store;
pub mod rebroadcast_store;
pub mod signet;
pub mod snapshot;
pub mod transaction_admission;
pub mod transaction_policy;
pub mod transaction_pool_store;
pub mod undo_store;
pub mod utxo;
pub mod validation_owner;
pub mod wallet;

pub use utxo::{OutPointKey, Utxo, UtxoStore};
