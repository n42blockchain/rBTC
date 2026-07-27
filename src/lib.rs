//! rBTC's storage and validation kernel.
//!
//! This crate intentionally keeps consensus, storage, snapshot, and transport
//! boundaries explicit. It is not yet a complete mainnet node; see
//! `docs/ROADMAP.md` for the completion gates.

extern crate self as rbtc;

pub mod api;
pub mod archive;
pub mod auxiliary_index;
pub mod block_execution;
pub mod blockchain;
pub mod chain_store;
pub mod chainstate;
pub mod consensus;
pub mod core_snapshot;
pub mod deployments;
pub mod diagnostics;
pub mod execution_store;
pub mod explorer_store;
pub mod fee_estimator;
pub mod header_store;
pub mod headers;
pub mod ibd;
pub mod index_policy;
pub mod ledger;
#[cfg(feature = "mdbx")]
pub mod mdbx_utxo;
pub mod merkle_proof;
pub mod node;
pub mod p2p;
pub mod peer_store;
pub mod rebroadcast_store;
pub mod signet;
pub mod snapshot;
pub mod snapshot_download;
pub mod transaction_admission;
pub mod transaction_policy;
pub mod transaction_pool_store;
pub mod undo_store;
pub mod utxo;
pub mod validation_owner;
pub mod wallet;

pub use utxo::{OutPointKey, Utxo, UtxoStore};

/// Emits one bounded structured informational diagnostic.
#[macro_export]
macro_rules! rbtc_info {
    ($($argument:tt)*) => {
        if $crate::diagnostics::enabled($crate::diagnostics::LogLevel::Info) {
            $crate::diagnostics::emit(
                $crate::diagnostics::LogLevel::Info,
                module_path!(),
                format!($($argument)*),
            )
        }
    };
}

/// Emits one bounded structured warning diagnostic.
#[macro_export]
macro_rules! rbtc_warn {
    ($($argument:tt)*) => {
        if $crate::diagnostics::enabled($crate::diagnostics::LogLevel::Warn) {
            $crate::diagnostics::emit(
                $crate::diagnostics::LogLevel::Warn,
                module_path!(),
                format!($($argument)*),
            )
        }
    };
}

/// Emits one bounded structured error diagnostic.
#[macro_export]
macro_rules! rbtc_error {
    ($($argument:tt)*) => {
        if $crate::diagnostics::enabled($crate::diagnostics::LogLevel::Error) {
            $crate::diagnostics::emit(
                $crate::diagnostics::LogLevel::Error,
                module_path!(),
                format!($($argument)*),
            )
        }
    };
}

/// Emits one bounded structured debug diagnostic.
#[macro_export]
macro_rules! rbtc_debug {
    ($($argument:tt)*) => {
        if $crate::diagnostics::enabled($crate::diagnostics::LogLevel::Debug) {
            $crate::diagnostics::emit(
                $crate::diagnostics::LogLevel::Debug,
                module_path!(),
                format!($($argument)*),
            )
        }
    };
}
