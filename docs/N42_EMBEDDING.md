# n42-26 embedding contract

Status date: 2026-07-26.

## Release gate

rBTC is currently GPL-3.0-only and `../n42-26` is MIT OR Apache-2.0. The
runtime boundary below is technically ready, but a distributed combined binary
needs a recorded GPL-compatible licensing decision. Until then, keep production
distribution behind an authenticated local process/IPC boundary. Do not copy
consensus state, UTXOs, or signing keys between the two implementations merely
to avoid this decision.

## Ownership

The host owns the Tokio runtime and critical-task supervision. rBTC owns its
Bitcoin peers, headers, maximum-work selection, chainstate, freezer, mempool,
and durable checkpoint barriers. Each `NodeBuilder::launch` creates isolated
runtime control, so separate networks and data directories can coexist in one
process.

The intended `n42-node` assembly follows the existing
`task_executor.spawn_critical_task(name, Box::pin(future))` shape:

```rust,ignore
use bitcoin::Network;
use rbtc::node::{NodeBuilder, NodeController};

let handle = NodeBuilder::new(Network::Bitcoin, bitcoin_data_dir)
    .mempool_full_rbf(false)
    .ledger_retention(1_008, 1024 * 1024 * 1024)
    .launch()?;

// Retain this in N42Node or the outer service container before moving the
// wait future into Reth's critical-task executor.
let controller: NodeController = handle.controller();
let mut lifecycle = controller.subscribe_lifecycle();
let mut events = controller.subscribe_events();

task_executor.spawn_critical_task(
    "rbtc-bitcoin-node",
    Box::pin(async move {
        if let Err(error) = handle.wait().await {
            panic!("embedded rBTC critical task failed: {error}");
        }
    }),
);

// A host shutdown hook calls this once. It never depends on an OS signal.
controller.request_shutdown();
```

`NodeLifecycle` is a latest-value Tokio watch channel. `NodeEvent` is a
32-entry broadcast ring: a slow consumer receives `Lagged` and must resample
the lifecycle value instead of causing unbounded node memory. The event stream
currently covers task lifecycle. P1.0 adds typed peer, header, execution,
reorg, pruning, index, and failure progress without exposing mutable consensus
internals.

## Integration acceptance

The repository-owned external-crate test already verifies:

1. `NodeHandle::wait` can move into a critical-task-shaped executor while the
   host retains lifecycle/event observation and graceful shutdown control.
2. Two isolated regtest instances establish separate peer sessions in one
   Tokio runtime.
3. Each controller shuts down only its own instance and waits for that
   instance's in-flight durable checkpoint barrier.

The final sibling acceptance fixture will live in `../n42-26` after the
licensing decision. It must use an isolated Bitcoin data directory, forward
host shutdown exactly once, fail the enclosing node on an unexpected rBTC task
failure, and never reinterpret rBTC readiness as N42 consensus readiness.
