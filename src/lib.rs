//! rBTC's storage and validation kernel.
//!
//! This crate intentionally keeps consensus, storage, snapshot, and transport
//! boundaries explicit. It is not yet a complete mainnet node; see
//! `docs/ROADMAP.md` for the completion gates.

pub mod api;
pub mod archive;
pub mod blockchain;
pub mod chainstate;
pub mod consensus;
pub mod header_store;
pub mod headers;
pub mod ledger;
pub mod p2p;
pub mod snapshot;
pub mod utxo;
pub mod wallet;

pub use utxo::{OutPointKey, Utxo, UtxoStore};
