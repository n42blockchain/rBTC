# Finite memory retry before staging

Normal sync and overlay/replay callers now enter the same retry wrapper around
the production download/validate/stage/execute attempt. Only a typed memory
reservation failure can reduce the current window. The guard requires an exact
unchanged execution height AND hash, a readable absent stage, and no outstanding
deferred scripts (including at entry). Disk pressure, unknown local errors,
protocol/transport errors, an unreadable tip, a stage or unreadable/corrupt stage
all fail closed with the original error. Existing stages are never overwritten
or discarded by this wrapper.

Each eligible retry halves the actual bounded remaining window, rounding down.
Nine blocks therefore fall back to four, two, one; a one-block failure returns
instead of looping. Attempt workers have joined before retry. Speculative
prefetch owners are dropped, and both network prefetch and replay read-ahead are
disabled for subsequent attempts. Initial replay read-ahead behavior is retained.
Limits apply to this invocation; this is not a persistent adaptive scheduler.

Validation on Mac:

- Full all-feature library suite: 1,069 passed, zero failed, 12 ignored, 68.30 s.
- Strict locked all-target/all-feature Clippy: passed, 37.68 s.
- Fuzz locked all-target Clippy: passed, 2.71 s.
- Formatting and diff checks passed.
- Real Regtest replay uses four mined blocks with large unspendable payloads,
  an admitted source ledger, a real destination ledger and redb chainstate.
  A measured one-block allowance rejects a four-block read. The production
  wrapper then advances exactly one block per call to height four, leaves no
  staged residue, and reproduces every source block byte-for-byte.
- Holding one more byte makes even one block fail. The same production wrapper
  terminates within the test deadline, returns typed memory pressure, leaves
  the execution tip at genesis and destination unpublished, and refunds all
  attempt ownership. Releasing that byte allows the successful replay above.
- Guard tests reject changed height, changed hash, unreadable tip, pending scripts,
  disk/unknown/protocol errors, existing valid and corrupt stages. Existing stage
  identity/content remains unchanged. Finite halving reaches one without retrying it.

This does not resume a segment already staged or retry publication after an
execution commit. Those require phase-aware, identity-bound recovery. Persistent
fair scheduling, complete memory ownership/startup RSS, whole-node physical disk,
final-source and seven-day acceptance remain open. No total gate is closed here.
