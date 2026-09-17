# Typed archive admission failures at the node boundary

Shared temporary-disk admission failures were previously converted into generic archive I/O, then formatted into transient peer-session errors during block staging. Archive compression and staged-prefix comparison now preserve these failures as `ArchiveError::ResourceBudget` through `LedgerError`.

All three live download/execution staging branches and the post-execution staged-publication branch classify that explicit variant as `PeerFailureKind::LocalResource`. They preserve existing behavior for other error classes. A resource failure after execution publication is not retried by this change; automatic downshift/retry still requires an unchanged-tip guard and explicit recovery of staged data.

A real ledger regression exhausts its bound shared temporary allowance, invokes archive staging, verifies the typed archive/ledger error and node-local classification, confirms no staged segment exists, releases pressure and successfully stages the same data with a full temporary-budget refund.

This covers those live node boundaries, not every startup/tooling String-based error path. Native archive memory admission, total physical disk accounting, resumable scheduling and sustained whole-node/final-scale acceptance remain open.

Prior CI35210343231/0d17316 completed successfully. Validation: all-feature library suite 1,053 passed, 0 failed, 12 ignored (72.38 s); final focused resource regression passed (0.07 s); final strict all-target/all-feature Clippy passed (22.06 s). Formatting and diff checks passed.
