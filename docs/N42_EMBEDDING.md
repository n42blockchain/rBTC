# n42-26 embedding contract

Status date: 2026-07-27.

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
use rbtc::node::{NodeBuilder, NodeController, NodeIndexConfig, NodeRuntimeStatus};

let handle = NodeBuilder::new(Network::Bitcoin, bitcoin_data_dir)
    .mempool_full_rbf(false)
    .indexes(NodeIndexConfig {
        transaction: true,
        spent_output: false,
        basic_filter: true,
    })
    .ledger_retention(1_008, 1024 * 1024 * 1024)
    .launch()?;

// Retain this in N42Node or the outer service container before moving the
// wait future into Reth's critical-task executor.
let controller: NodeController = handle.controller();
let mut lifecycle = controller.subscribe_lifecycle();
let mut status = controller.subscribe_status();
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

`NodeLifecycle` and `NodeRuntimeStatus` are latest-value Tokio watch channels.
The latter exposes bounded peer count, maximum-work header tip, execution and
index tips, freezer footprint/range, trust state, and the latest task error
without requiring the HTTP API. `NodeEvent` is a 32-entry broadcast ring of
lifecycle, peer, header, execution, reorg, index, freezer, and failure deltas.
A slow consumer receives `Lagged` and must resample `status` instead of causing
unbounded node memory. None of these views exposes mutable consensus internals.

## Integration acceptance

The repository-owned external-crate tests verify:

1. `NodeHandle::wait` can move into a critical-task-shaped executor while the
   host retains lifecycle/event observation and graceful shutdown control.
2. Two isolated regtest instances establish separate peer sessions in one
   Tokio runtime.
3. Each controller shuts down only its own instance and waits for that
   instance's in-flight durable checkpoint barrier.
4. A real regtest P2P v1 handshake publishes peer, header, execution, index,
   freezer, and trust state without enabling an HTTP listener.

The exact sibling acceptance fixture lives at
`../n42-26/integration-tests/rbtc-embedding`. It is an isolated Cargo fixture
so CI compiles rBTC and Reth's task executor without rebuilding the unrelated
EVM, RocksDB, and full `n42-node` dependency graph. It uses an isolated data
directory, Reth's real `TaskExecutor::spawn_critical_task`, and retained
controller shutdown. It is a technical integration test, not authorization to
distribute a combined binary. Production assembly must still fail the
enclosing node on an unexpected rBTC task failure and must never reinterpret
rBTC readiness as N42 consensus readiness.
