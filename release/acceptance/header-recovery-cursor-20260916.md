# Durable header recovery cursor

Status: partial implementation; `header-resources` remains open.
This change follows `b221e19` and does not replace the historical resource
measurements or rebind the release's accepted evidence to the new source.

## Behavior

- Header append and the recovery tip are committed in the same redb transaction.
  Failed append/checkpoint operations publish neither partial rows nor a new tip.
- A full already-known prefix can checkpoint its last validated header without
  duplicating stored headers. The metadata contains one 32-byte hash, not an
  accumulating per-peer journal.
- Disconnect, cancellation or subsequent batch failure leaves the last committed
  cursor available. Reopen validates the retained DAG before using the hash as a
  locator hint; the hint cannot choose the active chain or bypass consensus.
- Completed synchronization clears the hint. Idle eviction clears it atomically
  if the pointed-to header is removed; a failed eviction restores both rows and
  metadata. A missing hint or one absent from the validated DAG falls back to
  the active tip. Malformed cursor encoding is a local storage error.
- The first response from a replacement peer can switch to a lower known prefix
  of a different branch. Subsequent non-progressing known responses still stop.

## Verification

The header-store suite covers durable reopen, append failure after private row
insertion, duplicate rejection, empty-batch checkpoint, explicit clearing,
successful eviction and injected eviction failure with cursor preservation.

The loopback recovery test covers all four combinations of retained/evicted
2,000-header fork prefixes and uninterrupted/interrupted synchronization.
The interrupted cases close the connection after the next request, discard the
in-memory DAG, reopen the database, and verify that the replacement connection's
first locator starts at the losing fork's saved tip. Two further headers make
that fork win; reopen matches the selected tip and the cursor is cleared.
The existing cancellation and invalid-batch tests also assert cursor state.

Local verification on 2026-09-16:

- `cargo test --locked --all-features --lib header_store`: 9 passed.
- `cargo test --locked --all-features --lib header_resync`: 7 passed, 1 ignored.
- `cargo clippy --locked --all-features --all-targets -- -D warnings`: passed.
- `cargo test --locked --all-features --lib -- --test-threads=4`: 938 passed,
  0 failed, 11 ignored, 75.55 seconds.
- Full-suite log: `target/current-acceptance-20260915/header-cursor-full-lib.log`
  (local generated artifact, not committed).
- `cargo fmt --all -- --check` and `git diff --check`: passed.

## Limits

This is resumable locator state, **not disk-backed oversized-candidate storage**.
The full retained DAG is still replayed under the existing entry ceiling. A
candidate exceeding that ceiling still defers and cannot make bounded-memory
forward progress. Independent byte/work budgets, startup memory guarantees,
full-block execution reorg/failover and sustained whole-node RSS/disk acceptance
remain outstanding. These tests do not constitute a seven-day public soak or
power-loss fault injection.
