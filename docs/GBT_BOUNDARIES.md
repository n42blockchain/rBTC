# Mining interface boundaries

Status date: 2026-08-11.

This is a design note, not a plan of record. It maps the ownership and
concurrency boundaries a `getblocktemplate`/`submitblock` implementation
would have to cross, so the cost is understood before any code is written.
The roadmap item is deployment-triggered — "only if rBTC is deployed for
mining" ([ROADMAP.md](ROADMAP.md)) — and nothing here changes that.

## Status

`rbtc.submitblock` exists as of 2026-08-11, built along option A below for the
test-self-sufficiency target. Gaps 1 and 2 are closed for that target; gaps 3
and 4, and everything `getblocktemplate` needs, are untouched. What the
implementation does **not** do is listed under [What submitblock does
not do](#what-submitblock-does-not-do).

## Summary

The binding constraint is **header-chain ownership, not chainstate
ownership**. Chainstate already has a shared read handle that a template
builder can use unchanged. The header DAG does not: it is owned by value
inside the execution loop, and no code path exists that inserts a locally
produced header. A `submitblock` cannot connect anything until that gap is
closed, and closing it touches the most safety-critical loop in the node.

Reading for a template is cheap. Writing a block is not.

## What already exists

| Need | Available today | Reference |
| --- | --- | --- |
| UTXO and tip reads off the execution loop | `Arc<RedbChainStore>` shared into `NodeInboundSource`, exposed read-only through `InboundDataSource` | `src/node.rs:12291`, `src/inbound.rs:490` |
| Mempool with fees and sizes | `relay_snapshot()` returns `{transaction, fee_sats, policy_vsize}` | `src/transaction_admission.rs:1105` |
| Dependency-correct ordering | `entries` preserves admission order and packages are topologically ordered on insert, so parents always precede children | `src/transaction_admission.rs:1202`, `1439` |
| Next-block `nBits` | `HeaderDag::expected_next_bits`, including retarget, testnet minimum difficulty, and BIP94 | `src/headers.rs:372` |
| Block assembly from a chosen set | `block_assembly::assemble_block`: BIP34 height, witness commitment over the whole set, subsidy plus fees | `src/block_assembly.rs:93` |
| An ingress pattern from RPC into the execution loop | `rbtc.submitrawtransaction` → `InboundDataSource::submit_transaction` → bounded queue → drained by the loop | `src/node.rs:3918`, `4926`, `12378` |

## The four gaps

### 1. Header insertion (blocking)

`HeaderDag` is a plain struct with no interior mutability
(`src/headers.rs:155`), owned by the execution loop as a `mut` local
(`src/node.rs:11954`). Every insertion in the running node comes from a peer
`headers` message inside `sync_headers` (`src/node.rs:10257`), which appends
to the durable store and then commits the staged batch, rolling back on
failure (`src/node.rs:10293`).

Block connection requires the header to be present **and** to be the active
chain's tip+1, checked twice — once before validation
(`src/block_execution.rs:763`) and again inside the commit transaction
(`src/chain_store.rs:2398`).

So a self-mined block needs a new operation: insert one locally produced
header, append it durably, and make it the active tip, with the same
rollback guarantee. There is no such operation today.

The `Arc<RwLock<HeaderDag>>` on `NodeInboundSource` (`src/node.rs:4508`) is
**not** a shared view of the live DAG — it is a snapshot copy replaced only
after a resync (`src/node.rs:12484`). A template built from it can be stale,
and it cannot be used to insert anything.

### 2. A block ingress channel with a result

There is no block equivalent of `InboundTransactionQueue`; `submit_transaction`
is the only ingress on `InboundDataSource` (`src/inbound.rs:490`). The
transaction queue is also fire-and-forget — the RPC answers
`{"txid":…, "queued": true}` (`src/node.rs:3945`) — whereas `submitblock`
must return either nothing or a rejection reason, so the channel needs a
reply path the existing pattern does not have.

The natural drain point is gated on being caught up and past minimum
chainwork (`src/node.rs:12369`). That is correct for a miner but means a
submitted block is only considered on that branch.

### 3. Template selection

`block_assembly` takes a caller-chosen set verbatim and says so
(`src/block_assembly.rs:6`). Missing:

- fee-rate ordering, and ancestor-package scoring for CPFP. The pool's
  ancestor/descendant helpers are private and txid-only
  (`src/transaction_admission.rs:1583`), with no ancestor fee/size cache;
- weight accounting against `MAX_BLOCK_WEIGHT`, which is enforced only during
  validation (`src/blockchain.rs:823`), so assembly can currently produce an
  oversize block;
- sigop accounting against `MAX_BLOCK_SIGOPS_COST`. `policy_vsize` is already
  sigop-adjusted (`src/transaction_admission.rs:2171`) but the raw sigop cost
  is discarded (`:371`), so it must be recomputed;
- splitting assembly from the proof-of-work grind: a template server must not
  grind, and `assemble_block` always does (`src/block_assembly.rs:147`).

### 4. Version bits

`block_deployment_context_for_headers` yields consensus flags but no block
version (`src/deployments.rs:404`). `minimum_block_version` gives the buried
floor but is `pub(crate)` (`src/deployments.rs:369`). The BIP9 machinery —
`threshold_state_cached`, `ThresholdState`, the top-mask and bit constants —
is private (`src/deployments.rs:22`, `556`). In practice every configured
deployment is long active, so a template version is `0x20000000` with no bits
set, but nothing computes that today, and `vbavailable`/`vbrequired` would
need the private machinery exposed.

## Options for the write path

**A — new bounded block queue, header insertion in the execution loop.**
Mirrors the transaction ingress: a `submit_block` method on
`InboundDataSource`, a bounded queue, a drain point that inserts the header,
connects the block, and returns the verdict over a oneshot. Keeps single
ownership of both the DAG and chainstate. This is the recommended shape; the
header-insertion operation in gap 1 is the bulk of the work.

**B — share the header DAG.** Move the live DAG behind `Arc<RwLock<…>>` so
other tasks can insert. Rejected as a starting point: it widens the mutable
surface of the node's most safety-critical loop for one feature, and the
existing snapshot copy shows the current design deliberately keeps the live
DAG unshared.

**C — loopback peer (test-only).** Feed a self-mined block through the
ordinary peer path by announcing its header and serving the block, which is
exactly what the regtest fixtures already do. Adds no ownership surface and
needs no new consensus-adjacent code, but it is a harness technique, not a
mining interface.

## What `submitblock` does not do

- **It does not report whether the block connected.** `execute_data_query` is
  synchronous (`src/node.rs:3657`), so the handler cannot await the execution
  loop. It answers `queued` after context-free checks — proof of work against
  the block's own target, and `validate_block_structure_with_deployments` for
  the coinbase, Merkle root, weight, and witness commitment — and everything
  needing chain context is decided later and only logged. Callers confirm with
  `getbestblockhash`. Giving Core's verdict semantics means making the RPC
  dispatch async, which is a larger change than the submission path itself.
- **It caps blocks at 32 KiB**, half the shared 64 KiB JSON-RPC body limit
  (`src/api.rs:139`), because the hex encoding doubles the payload. Regtest
  fixtures fit; real blocks do not.
- **It has no reorg behaviour.** A submitted header that does not take the
  active tip is recorded and its body is dropped, deliberately: the execution
  path matches prefetched bodies positionally against the active chain, so
  queueing a losing block's body would abort the peer run. Connecting a block
  that causes a reorg remains unimplemented, as noted below.
- **It defers rather than interleaves.** When a previous batch's prefetch is
  still buffered, staging is skipped for that iteration and the submission
  stays queued.

## Two scope questions to settle first

1. **Which target?** Settled on 2026-08-11: test-toolchain self-sufficiency.
   A production miner would still need the full BIP22/23/9 surface —
   `mutable`, `capabilities`, `longpollid`, `vbavailable`, fee-optimal
   selection — which remains out of scope and roughly an order of magnitude
   more work.
2. **Which chainstate modes?** The snapshot-overlay and AssumeUTXO-completion
   paths use different `ExecutionChainStore` implementations
   (`src/snapshot_overlay.rs:1775`, `src/snapshot_overlay_redb.rs:941`).
   Supporting mining on them is a separate decision, and refusing them is a
   legitimate answer.

## Open questions this note does not answer

- Reorg handling on submission. `commit_disconnect` exists
  (`src/chain_store.rs:2407`) but no caller was found on the runtime path, so
  a `submitblock` that arrives for a stale tip has no defined behaviour yet.
- Whether a second writer to `RedbChainStore` is safe. `commit_connect` and
  `commit_connect_batch` take `&self` and are `pub`, and the store serialises
  writers with its own guard (`src/chain_store.rs:196`), but the journal
  ordering invariants under an interleaved writer were not verified. Option A
  avoids the question by keeping one writer.
- `commit_connect` refuses to run when a validation journal is configured
  (`src/chain_store.rs:2211`), so any submission path must use the batch form
  on those nodes.
