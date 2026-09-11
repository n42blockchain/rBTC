# Upstream follow-up progress review, 2026-09-11

Reviewed the existing 2025-09-08–2026-09-08 upstream-analysis follow-up at
`c63a8761254a90d4ea4a4acce306560334b60885`. The summary ledger had stopped at
`dff94ba` and 936 passing tests. Detailed acceptance already included the
`c01c2ad` refused-port fixture fix and `c63a876` reconciliation growth work,
whose latest full run passed 940 tests. The summary and open-item introductions
are now synchronized with those reports and the new preflight work below.

## What is established

The three original P0 areas have the acceptance documented for their covered
boundaries: BIP30/BIP34 and undo behavior, script-job ownership/cancellation,
and parser/crypto/wallet integration. The six P1 rows remain partially complete.
Previous bounded fuzz, Tor/I2P, fee-policy, generated-storage and reconciliation
results remain valid at their stated scope; historical “not run” entries do not
supersede later successful acceptance.

The six publication manifests from fee decay through growth pruning contain
41 file-hash records. Every SHA-256 matches the corresponding committed file.
This checks evidence provenance; it does not rerun those historical workloads.
The retained audit is `progress-review/progress-audit.json` under the usual
`target/upstream-followup/2026-09-11/` evidence directory.

## Remaining principal gates

| Gate | Concrete remaining work |
| --- | --- |
| Core optimizer and occupancy parity | Full work-budgeted optimizer, reuse of prior orders, and matched occupancy/eviction accounting. The two-pass refinement and selected live fee scenarios are already accepted. |
| Aggregate admission resources | Account for and bound new-package replay, candidate metadata copies, fresh UTXO/input/script work, graph checks and snapshots across peers and repeated chain changes. Preserve contextual validation and atomic admission. |
| Competing-header retention/recovery | Cover both ingress paths, resource deferral, bounded stronger-fork recovery, durable eviction and bounded restart/failover loading. Serving projection and within-session DAG reuse do not bound primary retention. |
| Full storage lifecycle | Run the 160M-UTXO / 900,000-transition 64/256 lanes, including compaction copy space, restart/reference checks and peak RSS. The million-UTXO lane is accepted at its smaller scale. |
| Cold/mainnet replay | Obtain the immutable corpus and matched starting state, then retain comparable engine and physical-I/O evidence. Generated fixtures do not establish mainnet replay. |
| Seven-day public-network soak | Recover complete frozen-run evidence or conduct a new caught-up run with required faults/restarts and the fail-closed finalizer. Calendar time alone is insufficient. |

Fresh local preflight found approximately 218.6 GiB available on the shared
workspace volume, below the two storage lanes' combined configured 256 GiB
ceilings before a compact copy and reserve. This is a planning gap, not an
assertion that both ceilings are immediately allocated. The `/tmp` tmpfs is
not a durable replacement volume. Searches of the workspace and `/tmp` found
no `blk[0-9]*.dat`, `rbtcd-soak-start` or generated soak-report JSON;
`/home/n42/.bitcoin` was absent. These are scoped local findings.

## Completed in this review: package preflight before pool cloning

Previously, package admission cloned the whole pool before checking membership
and duplicate transaction IDs. A request containing only retained transactions,
or duplicate txids that immediately fail, still copied all admission metadata,
request caches and owned orphan payloads. This cost existed even though no new
transaction required contextual validation.

Package bounds and rolling-fee decay retain their previous order. Membership,
duplicate detection and topological ordering now run before candidate cloning.
All-known packages return their existing outcome immediately; duplicate-txid
packages return the same error. Membership uses the existing txid index rather
than scanning every retained wtxid. Known witness variants still substitute the
retained transaction, matching the existing package behavior.

Packages containing new entries still clone one private candidate, replay
retained transactions with fresh context, validate the new package, and publish
only after all checks succeed. This change does not weaken contextual checks,
introduce a validity cache, or establish an aggregate resource quota.

Three added regressions cover:

- all-known packages, including an invalid alternate witness: zero candidate
  clones or contextual validations, unchanged admitted/orphan allocation
  identity, and the same rolling-fee decay;
- duplicate txids for both retained and new transactions: rejection before any
  candidate clone, with no pool publication;
- mixed known/new packages: one candidate, full incumbent/child validation,
  retained-witness substitution, successful child admission and failed-child
  rollback.

The fixtures retain a 60,000-byte orphan output so unintended deep copying is
observable through its allocation identity. Both no-clone regressions fail on
the instrumented baseline with one candidate clone instead of zero; the mixed
package test passes on both versions. These are deterministic work/ownership
checks, not allocator-byte or whole-node RSS measurements.

## Current validation

All **83 admission tests** pass. The full all-feature run passes **943 tests**
(904 library + 39 integration), with **33 ignored**, no failures and four test
threads. Subprocess-helper output is excluded from those totals. All **four
live Core 31 differential tests** pass. Strict all-target/all-feature Clippy,
formatting and diff checks pass. Only documentation/comments changed after
the full regression checks; the final live Core run includes those comments.

Evidence is under `target/upstream-followup/2026-09-11/progress-review/`, including
`preflight-before-fix.log`, `admission-tests.log`, `all-features-tests.log`,
`core-replacement.log`, `clippy.log`, `progress-audit.json`, `environment.json`
and `published-source.json`.

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo test --locked --all-features --lib transaction_admission::tests
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
cargo clippy --locked --all-targets --all-features -- -D warnings
RBTC_BITCOIND=/absolute/path/to/bitcoin-31.0/bin/bitcoind \
  cargo test --locked --all-features --test core_replacement_differential \
  -- --ignored --nocapture
```
