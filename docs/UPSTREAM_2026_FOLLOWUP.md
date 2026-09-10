# Bitcoin upstream P0/P1 follow-up

Tracking started 2026-09-08 for releases/disclosures from 2025-09-08 through
2026-09-08. This is an active implementation and acceptance ledger, not a
claim that the older P1 roadmap or every item below has passed this checkout.

## Current acceptance (2026-09-09)

The dependency blocker is resolved for this checkout. The default local and
live Core gates now pass, as do the non-ignored all-feature tests and strict
all-feature Clippy check. The resource-retention implementation and operational
gates listed below remain open; this is not closure of the entire P0/P1 ledger.

| Gate | Observed result |
| --- | --- |
| Independent algorithms and framing | 21 passed: fee diagram 12, script queue 5, PSBT envelope 4. |
| Default library and integration tests | 857 passed, 0 failed, 21 ignored; includes 819 library tests and 38 integration tests. |
| All-feature library and integration tests | 904 passed, 0 failed, 26 ignored; includes 865 library tests and 39 integration tests, including MDBX crash recovery. Doc tests also passed (0 tests). |
| Live Core 31.0 block/transport differential | 9 passed, including BIP activation boundaries, encrypted transport and v1 fallback. |
| Live Core 31.0 replacement/pressure differential | 2 passed. These fixtures do not establish exact cluster optimizer or rolling-floor parity. |
| Strict Clippy | `cargo clippy --locked --all-targets --all-features -- -D warnings` passed. |
| Formatting / acceptance script syntax | `cargo fmt --all -- --check` and `bash -n scripts/check-upstream-followup.sh` passed. |

Counts exclude duplicate output from subprocess helper invocations. The 11
Core tests are ignored in ordinary Cargo runs and were then explicitly run
by the Core gates; the default and all-feature counts overlap and must not be
added together. Compiler output still includes the vendored consensus C
compiler's existing string-initializer warning.

Final report: `target/upstream-followup/run.rE20fwvB/summary.tsv`, with per-gate
logs and `environment.log` beside it. Supplemental `all-features-tests.log`
and `clippy-all-features.log` record the separate successful checks. These
reports are local build artifacts, not tracked fixtures.

### Changes completed during integration

- Dependency downloads now work, but the default `/home/n42/.cargo` cache is
  read-only in this environment. This run used `/tmp/rbtc-cargo-home`, seeded
  with the existing registry cache/index and populated by locked Cargo builds.
  No dependency versions or lockfiles were changed.
- The initial library run found two failures (815 passed, 2 failed, 3 ignored).
  The rolling-fee test incorrectly expected a newly cheap transaction to evict
  a better retained chunk from a full pool. It now checks atomic capacity
  refusal after floor decay, then verifies acceptance after freeing a slot.
- An intermittent v2 fallback failure exposed a TCP-close distinction: unread
  handshake bytes can produce a reset instead of clean EOF. Outbound handshake
  reset/abort/broken-pipe/EOF errors now allow the existing single v1 retry.
  Other I/O errors and protocol violations remain failures. Deterministic
  read/write/flush fault injection and a two-connection SOCKS5 onion regression
  passed; the latter verifies the same proxy target and unspecified receiver
  address on fallback. See [private broadcast validation](PRIVATE_BROADCAST.md).
- The acceptance script uses an explicit temporary datadir for Core's version
  probe, avoiding writes to the operator's default datadir. It records mode and
  Cargo cache location and uses `--no-fail-fast` so one test target's failure
  does not suppress independent targets. An intermediate invocation was
  interrupted by an edit to the running script; only the completed final run
  above is the acceptance report.

The Core binary and checksum manifest came from the
[official Core 31.0 release directory](https://bitcoincore.org/bin/bitcoin-core-31.0/).
`bitcoin-31.0-x86_64-linux-gnu.tar.gz` matched the published SHA256SUMS entry;
the fixtures independently required RPC version 310000. The checksum manifest
is retained with the logs. Release signatures were not independently verified.

Reproduce while the temporary cache and Core binary remain available:

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
export RBTC_BITCOIND=/tmp/rbtc-core-31/bitcoin-31.0/bin/bitcoind
bash scripts/check-upstream-followup.sh core
cargo test --locked --all-features --no-fail-fast
cargo clippy --locked --all-targets --all-features -- -D warnings
```

### Remaining gates

- Implement and measure valid competing-header retention, bounded recovery
  and durable eviction as specified in [the resource gate](UPSTREAM_HEADER_RESOURCE_GATE.md).
- Bound and measure admission replay and whole-pipeline allocations; a pending
  script queue accounting limit is not an RSS bound or a chain-bound validity cache.
- Run real Tor/I2P, sustained public-network and resource-pressure workloads,
  plus fuzz acceptance and optional ignored storage-scale gates. Non-ignored
  MDBX tests passing does not close its scale/benchmark gates.
- Broaden live cluster optimizer/fee-floor differential coverage before claiming
  complete Core policy parity. Existing successful fixtures establish only
  their tested scenarios.

The entries below preserve the original findings and earlier blocked runs.
Their pending-test statements are historical and are superseded by the current
acceptance results above.

## Initial acceptance matrix (2026-09-08)

| Item | Implementation evidence / action | Acceptance still required |
| --- | --- | --- |
| P0: BIP30/BIP34 | `deployments::bip30_enforced` already checks the activation height, exact historical exceptions, known BIP34 anchor and height 1,983,702. `block_execution` preserves overwritten coins for undo. | Run deployment and execution regressions and live block differential. |
| P0: script lifetime | `consensus` synchronously calls the repository-owned adapter. `blockchain::ScriptValidationJob` owns serialized bytes and prevouts; workers contain panics. Pending work now has a 64-MiB accounting budget, returns full/oversized work for inline execution, and shares a cancellation token with its submitting batch. Dropping the batch removes queued jobs; executing groups check cancellation between transactions. Submission streams groups instead of materializing a second whole-batch work vector. | Run parallel early-failure/rollback, worker-panic and new inline-failure/batch-drop integration regressions. The independent queue's five tests pass. The queue limit is not an RSS limit for executing workers or the caller's existing validation batch. |
| P0: parsing/crypto | Added exact-length checks for supported P2P payloads shared by v1 and v2. Added WIF scalar, Schnorr verification and BIP350 checksum-family regressions. Wallet finalization now checks bounded base64 decoding, canonical map lengths, exact PSBT map count and full final-witness consumption through `psbt_envelope`. | Run dependency-boundary and wallet integration tests. The four independent PSBT envelope tests pass; that does not establish full BDK integration acceptance. |
| P1: CPU/disk/orphan DoS | Existing logs have bounded records, queues, rate and rotation. Added orphan entry-plus-input work accounting alongside existing count/byte limits. | Run hostile orphan/log/P2P tests; assess repeated admission replay and rejected-transaction work. |
| P1: cluster mempool | Existing 64-tx/101-kvB cluster limits and diagram RBF retained. Chunk identities now drive terminal-chunk fee-based eviction, relay snapshots and template selection using canonical txid ordering. Templates score sigop-adjusted vsize and exclude chunks depending on a skipped parent; shared parents are charged once. | Run new capacity/CPFP/template tests and live Core differential. The independent fee module's twelve tests pass. Greedy linearization is not a claim of exact Core optimizer parity. |
| P1: fees/package relay | Existing 100 sat/kvB relay/incremental floors, TRUC/sibling eviction and package rules retained. Replay of already-admitted zero-fee parents no longer incorrectly reapplies an individual relay floor. | Run low-fee/package/TRUC and rolling-floor differential suites. |
| P1: headers-first IBD | Existing `node` headers-first staging precedes durable append; P2P responses are capped at 2,000 headers. | Run failover, timeout, race and interop coverage; audit persistent competing-chain resource consumption. |
| P1: chainstate I/O | Production storage is redb, with optional MDBX, not Core's LevelDB. `write_back_chainstate` already coalesces mutations and waits before durable-state operations. Do not port LevelDB tuning to unrelated engines. | Run write-back equivalence/recovery tests; measure workload I/O before changing engine policy. |
| P1: low-work/reorg DoS | Contextual difficulty precedes insertion and cumulative work is cached. Added a repeated cheap-header refusal test and changed active-chain promotion/rollback to rebuild only the changed suffix. | Run new reorg tests. Cheap-invalid-header rejection does not establish a bound on all valid competing-header retention; that remains open. |

## Four constraints

1. `OP_CODESEPARATOR` in an unexecuted legacy branch is refused only by
   `transaction_policy`. Its regression also verifies the spend succeeds at
   the consensus boundary. Pushed opcode bytes and SegWit are not confused
   with legacy opcodes. No consensus flags are strengthened for this policy.
2. Multiple OP_RETURN outputs and the aggregate 100,000-byte policy limit
   remain in `transaction_policy`; no datacarrier restriction is added to
   block acceptance. Existing policy boundary tests must be rerun.
3. Knots BIP110/RDTS is not adopted. Its release announcement is not evidence
   of Bitcoin-wide activation. The strict config parser explicitly tests
   rejection of copied `consensusrules=rdts` and `bip110=true` settings.
4. rBTC's wallet is BDK descriptor/watch-only, not Core's unnamed BDB wallet
   migration. Core's deletion fix has no matching migration path to port.
   The existing private-broadcast path uses only short-lived proxied onion
   or I2P sessions and returns to the retry queue instead of falling back to
   clearnet. Rerun both private-broadcast network tests and audit all local
   transaction entry points and v2-to-v1 fallback routes before closing it.

## Initial validation in this environment

- `rustc --edition=2024 --test src/feerate_diagram.rs`: 12 tests passed,
  including chunk membership, shared parents, equal-rate dependencies,
  component-size limits and cycle rejection (2026-09-08).
- `rustc --edition=2024 --test src/script_queue.rs`: 5 tests passed,
  covering exact work accounting, full/oversized ownership return,
  cancellation/cleanup isolation and waking a waiting consumer (2026-09-08).
- `rustc --edition=2024 --test src/psbt_envelope.rs`: 4 tests passed,
  covering RFC4648 padding/bounds, exact maps, unknown keys, witness trailing
  bytes and canonical/oversized CompactSize lengths (2026-09-08).
- Full Cargo tests have not run: crates.io DNS resolution fails, and an
  offline attempt reports no cached `bitcoin` package. This is a build
  dependency blocker, not a successful test result.
- Historical Windows/macOS Core results in `CORE31_COMPATIBILITY.md` remain
  historical evidence only. They do not validate these changes.

Once dependencies are available, run the full library and
`upstream_2026_boundaries` tests, then the Core block/replacement differentials
with a supported official binary. Include the networking/anonymous fallback
and storage recovery gates; report actual results before closing this ledger.

## Remaining implementation work at the initial handoff

- Run template and script-queue integration regressions once the missing
  dependencies are available. Their independent algorithm tests are not a
  substitute for compiling and exercising the production adapters.
- Establish a memory/disk/work bound for valid low-work competing header
  chains during bootstrap and sustained operation. A hard rejection rule
  must not masquerade as consensus invalidity or strand valid reorgs.
- Admission now reuses prepared prevouts instead of resolving them a second
  time for contextual validation, checks policy and sigop limits before script
  execution, and mutates the overlay only after both consensus and standard
  script checks succeed. Mutation still performs its own UTXO conflict checks.
  This adapter change has not been compiled or tested. Incumbent transactions
  still undergo replay validation; removing that work requires a chain-bound
  validity cache, not an assumption that an unchanged height means unchanged
  UTXOs. Pending script work is bounded and cancellable; assess whole-pipeline
  allocation bounds independently.
- Static private-broadcast ingress/fallback audit found and closed the raw
  operator RPC bypass: `rbtc.submitrawtransaction` now refuses with `-32041`
  in private mode before reaching ordinary relay, including RPC-only setups.
  The wallet PSBT route retains its anonymity wave; proxied v2-to-v1 retry
  keeps the same SOCKS5 proxy and target. See `PRIVATE_BROADCAST.md`.
  The new RPC regression and existing network tests still require Cargo
  dependencies; run the complete adversarial, storage and upstream
  differential acceptance set before claiming integration acceptance.

## Primary upstream references

- https://github.com/btcsuite/btcd/releases/tag/v0.26.0
- https://github.com/btcsuite/btcd/releases/tag/v0.26.2
- https://github.com/btcsuite/btcd/pull/2467
- https://github.com/btcsuite/btcd/pull/2485
- https://bitcoincore.org/en/releases/30.0/
- https://bitcoincore.org/en/releases/30.2/
- https://bitcoincore.org/en/releases/31.0/
- https://bitcoincore.org/en/releases/31.1/
- https://bitcoincore.org/en/2026/05/05/disclose-cve-2024-52911/
- https://github.com/getfloresta/Floresta/releases/tag/v0.9.1
- https://github.com/bitcoinknots/bitcoin/releases/tag/v29.3.knots20260508
- https://github.com/bitcoin/bips/blob/master/bip-0350.mediawiki

## Continuation validation (2026-09-08)

- Re-ran the independent fee diagram, script queue and PSBT envelope suites:
  12 + 5 + 4 tests passed.
- `cargo fmt --all -- --check` passed after the RPC privacy guard change.
- `cargo test --offline --lib` still fails before compilation: no cached
  `bitcoin` package. The new node adapter and RPC regression remain uncompiled.

## Earlier continuation review (2026-09-09, before dependency recovery)

Overall status: implementation is partial and integration acceptance is blocked.
None of the nine matrix rows is closed by this continuation. Existing working
tree changes were retained; no production consensus or policy change was added
in this continuation.

### Reproducible acceptance entry point

Added `scripts/check-upstream-followup.sh` with three cumulative modes:

```sh
# Dependency-free modules plus repository formatting; partial scope only.
bash scripts/check-upstream-followup.sh standalone
# Above plus compilation and all non-ignored default-feature library/integration tests.
bash scripts/check-upstream-followup.sh local
# Above plus both explicit live Core differential suites.
RBTC_BITCOIND=/absolute/path/to/bitcoin-31/bin/bitcoind \
  bash scripts/check-upstream-followup.sh core
```

Each run writes individual logs, HEAD/working-tree/toolchain information and a
PASS/FAIL/NOT_RUN summary under `target/upstream-followup/run.*`. Compilation
failures are reported separately from tests that did not execute. Failed gates
return nonzero. A successful `standalone` run does not imply Cargo acceptance;
a successful `local` run does not imply live Core acceptance. The Core fixtures
perform their own version checks and require `bitcoin-cli` beside `bitcoind`.
Real Tor/I2P tests marked ignored, optional MDBX checks, fuzzing and public-network
soak remain explicit separate gates. Do not globally enable ignored tests:
some are subprocess helpers or require specific external services.

Observed during this continuation:

- Independent modules: 12 fee-diagram, 5 script-queue and 4 PSBT-envelope tests
  passed, for 21 total. Repository formatting passed.
- `cargo test --offline --lib`: failed before compilation, missing cached
  `bitcoin` package.
- `CARGO_NET_RETRY=0 CARGO_HTTP_TIMEOUT=15 cargo test --locked --lib --no-run`:
  failed fetching `bdk_wallet` because `index.crates.io` could not resolve.
- The `local` script in offline mode correctly returned exit 1, recording the
  Cargo build exit 101 and marking integration/Core tests NOT_RUN.
- No `bitcoind` was found on PATH or in the searched workspace, `/tmp` and
  Cargo cache. Live Core differentials were not run.

### Resource-bound follow-up

Added [competing-header findings and acceptance requirements](UPSTREAM_HEADER_RESOURCE_GATE.md).
The audit identified two persistence ingress paths, restart reconstruction and
inbound DAG copies that a complete limit must cover. A per-message header cap
and cheap-invalid-header rejection do not close this issue. Numerical budgets,
resumable stronger-fork recovery and durable eviction remain implementation work.

### Primary-source spot check

Rechecked the [Core script-lifetime disclosure](https://bitcoincore.org/en/2026/05/05/disclose-cve-2024-52911/):
the vulnerable early-return path can release data still used by background
validation. The owned-job and cancellation audit is relevant; this does not
establish that rBTC has the same vulnerability or has passed its regression gates.
Rechecked [Core 31.0 release notes](https://bitcoincore.org/en/releases/31.0/):
cluster/chunk ordering, diagram RBF, private broadcast and zero-fee package
parents support the existing follow-up scope. This was a spot check of the
existing analysis, not a new exhaustive release survey.
