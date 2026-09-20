# Validated block serialization admission — 2026-09-19

Base source: `c158c6b2a0dcad887f2d99859b1dfac3c4602cd7` plus the accompanying
change. This is not frozen-source production acceptance.

Structure validation previously called `serialize(block)` and wrapped its output
in an uncharged `ArchiveBlock`. Live batch execution and local freezer reindex now
serialize through the destination ledger's shared allowance. The constructor
checks the archive block-byte ceiling, reserves payload plus shared-owner overhead
before allocating an exact-capacity vector, and transfers the reservation into
the existing immutable payload owner. Clones, carried prefetch and parallel worker
results retain that charge until the last handle drops. Replay read-ahead keeps
its existing admitted archive bytes and is not charged a second time here.

Budget denial retains its typed local-memory classification through archive,
ledger and peer-run errors, so the existing unchanged-checkpoint retry logic can
shrink a batch. Consensus validation, durable writes, notification ordering and
post-commit retry restrictions are unchanged. This does not account for all
network/decoded blocks, caller-supplied transaction IDs or other node allocations,
and does not establish a total RSS bound.

## Regression evidence

The new real ledger-bound test checks exact normal and witness serialization,
shared-clone lifetime, and a 192-block parallel batch with an allowance one byte
short of its full serialized ownership. Failure refunds all partial results and
leaves stage/retained tip absent; removing pressure succeeds with exact charges,
and dropping all original results leaves only the escaped clone's reservation.

The first full run had 1082 passes and one failure in the existing staged-pressure
test: its old read-only allowance now rejects the newly charged serialization plus
stage verification *before* execution. The fixture now independently measures
read, staging and publication peaks. Its post-commit case uses the measured
staging allowance while still requiring publication to fail after exactly one
executed block. The original one-byte read deficit, immutable stage, no duplicate
execution, released-pressure recovery and owner-refund checks remain intact.
Historical failed logs are preserved.

## Remaining release gates

The release preflight still rejects admission-resources, header-resources and
public-soak. Final-source scale/history evidence and optimizer/storage rebinding,
whole-node concurrency/resource/fault measurements, Bitcoin/Testnet4 catch-up
followed by 604800 seconds of soak, native signing and final exact-main CI remain
required. No acceptance status, threshold or frozen-source identity was changed.

## Final validation

- `cargo test --locked --all-features --lib`: 1083 passed, 0 failed,
  12 ignored (72.51 seconds), including the recalibrated pressure scenario.
- Focused new serialization admission regression: 1 passed (0.08 seconds).
- Focused recalibrated staged pressure regression: 1 passed (34.46 seconds).
- Strict locked all-target all-feature Clippy passed after the final test change.
  Default all-target and fuzz Clippy passed with the same production code.
- Root/fuzz formatting and diff checks passed.
- Previous owner-lifetime commit `c158c6b` is pushed and remote-verified;
  its CI `35480161201` was still in progress at the last check. These new local
  results do not substitute for exact-source Linux/Windows CI.

Logs, including the failed first full run, are retained in the original workspace
under `session-state/2026-09-19/validated-serialization-memory`.
