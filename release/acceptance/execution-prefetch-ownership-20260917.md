# Execution prefetch admission and ownership (2026-09-17)

Overall allocation/RSS, resumable scheduling and production gates: **OPEN**.

Node-bound batch execution now reserves external-input discovery containers and
its sharded overlay map allowance before constructing them. Both read-ahead
entry points and the direct batch path use the same admitted prefetch reader.
It reserves the final entry vector/lease metadata first, then queries at most
128 keys under a temporary read allowance based on canonical spendable coins
(maximum script length 10,000 bytes), container copies and scratch headroom.
After each returned chunk moves into the final vector, its temporary containers
are gone and the lease shrinks to the actual retained script capacities. Earlier
chunks remain charged while subsequent chunks are read.

`ActiveBlockUtxoPrefetch` owns entries before their leases in destruction order.
The unrestricted `entries_mut` accessor is replaced by `refresh(store)`: the old
payload and its allowance remain until a completely read, aligned replacement
is ready. Refresh failure preserves that old value. Node read-ahead uses this
method. The separate compatibility `reconcile_prefetch` also moves validated
returned values instead of cloning the entire returned coin set again.

When prefetch entries move into overlay shards, their leases move into that
owner before inserting any coins. They also follow a detached `BlockDelta`.
Dropping the wrapper therefore does not refund live coin storage. Bound batch
consumers reject unleased/foreign-node prefetch owners; callers can read or
refresh through the intended node owner before submission. Stores outside a
registered node retain the unleased compatibility reader.

The sharded map allowance includes conservative bucket/growth and partitioning
headroom. The input-discovery lease releases only after its key vector is gone.
Read errors, mismatched key order, oversized unspendable records or excess
returned capacity release unpublished partial results and remain local errors.
Oversized records are never accepted as charged prefetch state.

**Limit:** a generic backend may allocate internally before `get_many` returns,
including while reading a corrupt oversized record. This caller-side allowance
and post-return validation are not a hard cap on those engine allocations.
Engine-level pre-decode bounds remain required. Worker/prepared-result copies,
script queues, non-prefetched lookups, indexed undo copies, thread stacks and
allocator/engine overhead likewise remain outside this change. Reservations
can fail closed; fair resumable memory scheduling still needs implementation.
More read transactions from chunking may affect throughput and must be measured
with final-source replay. No RSS or long-node/public acceptance is claimed.

Real Redb regressions verify retained payload accounting; refresh failure leaves
old data and usage unchanged; successful larger refresh charges the growth;
malformed oversized refresh leaves old data intact; unleased/foreign owners are
rejected; overlay and detached-delta transfers retain charges until destruction.
A 256-coin read with 10,000-byte scripts exhausts a 6 MiB remaining allowance on
a later chunk, refunds all partial results, and succeeds after headroom returns.
The lease shrink regression checks refused growth, exact refunds, duplicate
reservation size and final zero usage.

Final local validation: all-feature library suite **1,020 passed, 0 failed,
12 ignored** (70.07 s); strict all-target/all-feature Clippy passed (9.84 s);
formatting and diff checks passed. Earlier focused prefetch lifetime and late
chunk-failure tests both passed (0.42 s). Previous source 3a853b2 CI run
35192673238 completed successfully on Windows, Linux (including 90% coverage)
and supply-chain checks; it does not cover this change or pending 8021fcf.
