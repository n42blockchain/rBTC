# rBTC code audit

Audit date: 2026-07-27. Reviewed revision: `3d36e87` (branch `main`).

Scope: the complete `src/` tree (51,015 lines across 30 modules), the integration
and fuzz test suites, the build configuration, and the vendored
`bitcoinconsensus` build script. Reference implementation for every consensus
comparison is Bitcoin Core v26.0, which is the release pinned by
`vendor/bitcoinconsensus/depend/bitcoin`.

Method: module-by-module reading of every consensus, storage, policy, transport,
and API path, differentially compared against the corresponding Bitcoin Core 26
sources; targeted empirical probes to confirm or refute each suspected defect
before it was recorded; then fixes with regression tests.

Environment: `x86_64-pc-windows-msvc`, Rust 1.85.0 (the toolchain pinned by
`rust-toolchain.toml`).

## Verification status

| Gate | Before | After |
| --- | --- | --- |
| `cargo fmt --check` | pass | pass |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | **fail** (7 lints) | pass |
| `cargo build` / link | **fail** (18 unresolved symbols) | pass |
| `cargo test --locked --all-features` | **fail** (22 failures) | pass — 484 tests, 0 failures |

The pre-audit numbers are for this platform. The link failure and the ledger
test failures are Windows-specific; the consensus defects (A-01, A-02) and the
parser panic (A-03) are platform independent.

## Summary of findings

| ID | Severity | Area | Title | Status |
| --- | --- | --- | --- | --- |
| A-01 | High | Consensus | Accurate sigop counting undercounts relative to Core | Fixed |
| A-02 | Medium | Consensus | BIP68 activation gate used a signed transaction version | Fixed |
| A-03 | Medium | DoS | Manifest digest parser panics on non-ASCII input | Fixed |
| A-04 | High | Build | Vendored build omits `SECP256K1_STATIC` on Windows | Fixed |
| A-05 | High | Build | Vendored build omits `ENABLE_MODULE_ELLSWIFT` | Fixed |
| A-06 | High | Portability | Pruned ledger cannot fsync a directory on Windows | Fixed |
| A-07 | Medium | Liveness | Panicking script worker hangs block validation forever | Fixed |
| A-08 | Medium | Recovery | Crash recovery rejects a transition with no net UTXO effect | Fixed |
| A-09 | Low | Consensus | `IsFinalTx` missed Core's zero-locktime early return | Fixed |
| A-10 | Low | Tooling | The documented clippy gate did not pass off Unix | Fixed |
| A-11 | — | Performance | libsecp256k1 builds in 32-bit field mode under MSVC | **Withdrawn — not a defect** |
| A-12 | Info | Portability | Filesystem hardening is Unix-only | Fixed in part; documented |
| A-13 | Low | Performance | Header DAG is deep-cloned per header batch | Fixed |
| A-14 | Low | Performance | Several paths materialize the entire UTXO set | Fixed in part; documented |
| A-15 | Low | Robustness | Fresh-output fast path defers a duplicate probe to commit | Fixed |
| A-16 | Info | Consensus | `consensus_id()` omits the network | Fixed; **premise partly withdrawn** |
| A-17 | Medium | Consensus | Testnet4 has no Core-26 rule set | Fixed (BIP94 implemented) |
| A-18 | Info | Robustness | Validation-delta decode re-derives bounds unchecked | Fixed |
| A-19 | Info | Hygiene | Deployment context accepts two unused parameters | Fixed |
| A-20 | Info | Hygiene | Block locator step schedule differs from Core by one | Fixed |

A-21 was found while fixing A-17 and is not in the original numbering. A-22
and the rejected A-23 proposal came from the later third audit wave.

| ID | Severity | Area | Title | Status |
| --- | --- | --- | --- | --- |
| A-21 | Low | Consensus | Testnet4 used Signet's BIP9 threshold and had no trust anchors | Fixed |
| A-22 | Low | Consensus | Regtest inherited rust-bitcoin's 2,016-block interval instead of Core's 144 | Fixed |
| A-23 | Info | Consensus | Proposed unconditional regtest BIP94 enforcement | Rejected — Core v31.1 defaults it off |

## Post-audit integration disposition

The detailed finding text below records what was observed at audited revision
`3d36e87`; this section and the status table record the later main-branch
disposition. The actual fixes were reviewed and integrated by commit rather
than treating the report as proof:

| Audit branch commit | Main integration | Disposition |
| --- | --- | --- |
| `a779831` | `a589372`, `01e1d32` | Static/ellswift build flags retained; limb probe no longer recompiles secp256k1 |
| `a4a9371` | `bbdb24f` | Windows-safe ledger/store directory durability handling and recovery coverage |
| `2b2c8de` | `bee0a33` | Non-ASCII archive/snapshot digests reject without panic |
| `3c7fbf9` | `8454db8` | No-net-UTXO write-ahead transition recovery |
| `7612df4` | `98a13c7` | Core sigop and BIP68 parity |
| `4b8fb4b` | `29f8a77` | Full-tree report imported |
| `d95a50c` | `7c50ce5`, `01e1d32` | Native limb-width assertion retained and optimized |
| `9b333df`, `5b18a4c` | `05e9529` | Consensus identity, hot paths, Testnet4 BIP94/parameters, and migrations |
| `0a4d899` | `c43268a` | Cross-platform abrupt-kill recovery test |
| `442a8ab` | `8d66b1a` | In-place bounded header batch rollback |
| `75a0392` | `0943b67` | Corrected second-pass report integrated |
| `1b887c5` | `ee3ac1a` | 144-block regtest interval retained; incorrect unconditional BIP94 portion rejected |
| `7095993` | reviewed disposition below | Review-only; no valid code delta was missing |
| `da76a5c` | `77b8aae` plus disposition below | Review accepted; information-level relay scan replaced by bounded hash index |
| `5147e11` | reviewed disposition below | Review accepted; no new defect, header-replay operational notes retained |

This manifest is the current rolling main-branch audit integration checkpoint.
It deliberately does not create merge ancestry to
`audit/consensus-and-portability-fixes`: doing so would mark the rejected
default-regtest BIP94 change as integrated and make future containment checks
lie. Every accepted code delta through `5147e11` is present on `main` under the
commits above. Later auditor pushes remain subject to the same commit-by-commit
review, verification, and integration before the final audit closure.

- A-01 through A-10 were integrated with additional no-net-UTXO recovery and
  script-worker panic-containment tests. The current main branch passed strict
  all-target/all-feature clippy, the complete library and historical Core
  fixture suites, embedded-node tests, and repeated abrupt-kill recovery after
  integration.
- A-11's premise was false for the vendored secp256k1 revision.
  `SECP256K1_WIDEMUL_INT128` is selected from native `__int128` or MSVC's
  intrinsic-backed `int128_struct`; the removed `USE_FIELD_*`/`USE_SCALAR_*`
  defines were unused. The build now compiles a direct selection assertion and
  does not compile the complete secp source a second time for that probe.
- A-13 is closed in the live synchronizer, standby validator, and freezer
  reindex path by an in-place `O(batch)` guard. Persistence occurs before
  commit; validation/persistence failure rolls back inserted hashes, and only
  the rare stronger-side-chain rollback rebuilds the former active vector.
- A-14 is closed for runtime counting and aging: both validation and admission
  overlays compute a bounded net population delta, and hot-to-cold aging
  buffers only fixed-size outpoint keys. Explicit whole-set snapshot
  compatibility methods remain bulk APIs; live status, paging, and validation
  do not call them.
- A-15, A-16, and A-18 through A-21 were integrated with migration and
  regression coverage. Testnet4 uses the Core release family that defines it
  for BIP94, threshold, minimum-chainwork, and assume-valid values.
- The third audit wave's A-22 parameter correction was independently confirmed
  against Core v31.1 and integrated. Its accompanying A-23 change was rejected:
  Core's `RegTestOptions::enforce_bip94` defaults to `false` and is enabled only
  by the testing-only `-test=bip94` option. Unconditionally enabling it would
  have created the divergence the change claimed to remove. The main branch
  keeps default regtest BIP94 disabled, uses Core's one-day target timespan and
  144-block interval, and has a regression covering both facts.
- A-12's abrupt-kill test is now portable (`SIGKILL` on Unix,
  `TerminateProcess` on Windows). Windows ACL inheritance, audit-log file
  identity, and directory-flush behavior remain explicitly documented platform
  limitations; passing compilation alone is not reported as equivalent Unix
  filesystem hardening.

---

## Fixed findings

### A-01 — Accurate signature-operation counting undercounts relative to Core

**Severity: High (consensus divergence — block acceptance).**
**Files: `src/chainstate.rs`, `src/transaction_policy.rs`.**

`p2sh_sigops` and `witness_program_sigops` delegated the accurate sigop count to
`rust-bitcoin`'s `Script::count_sigops`. That implementation clears its
multisig key-count hint only on data pushes:

```rust
OP_CHECKSIG | OP_CHECKSIGVERIFY => { n += 1; }              // hint untouched
OP_CHECKMULTISIG | OP_CHECKMULTISIGVERIFY => { ... }        // hint untouched
_ => { pushnum_cache = opcode.decode_pushnum(); }
```

Bitcoin Core's `CScript::GetSigOpCount(bool fAccurate)` instead reassigns
`lastOpcode` after **every** opcode, including the signature operations
themselves, so a key count consumed by an intervening `OP_CHECKSIG` no longer
applies to a following `OP_CHECKMULTISIG`.

Measured divergence (confirmed by running the dependency):

| Script | Core 26 | rust-bitcoin 0.32.8 |
| --- | --- | --- |
| `OP_2 OP_CHECKSIG OP_CHECKMULTISIG` | 21 | 3 |
| `OP_2 OP_CHECKMULTISIG OP_CHECKMULTISIG` | 22 | 4 |

Both inputs to the accurate count are fully attacker-chosen: the P2SH redeem
script (`GetP2SHSigOpCount`) and the P2WSH witness script (`WitnessSigOps`).
Both feed `MAX_BLOCK_SIGOPS_COST` (80,000) through
`transaction_sigop_cost` → `AppliedTransaction::sigop_cost` →
`apply_prevalidated_block_with_deployments_inner`.

**Impact.** A miner can construct a block whose true Core sigop cost exceeds
80,000 while rBTC's undercount stays below it. Core rejects the block, rBTC
accepts it, and rBTC follows a chain that the network does not — a consensus
split, with rBTC on the permissive side. The same undercount also relaxes the
`MAX_STANDARD_P2SH_SIGOPS` relay limit in `validate_standard_inputs`, so
non-standard transactions could enter the local mempool.

**Fix.** `count_script_sigops` in `src/chainstate.rs` reimplements Core's
counter faithfully, tracking the previous opcode for every instruction and
preserving Core's "stop counting on a `GetOp` failure, keep the running total"
behaviour. Both the legacy and accurate modes now route through it, and
`transaction_policy.rs` uses it for the P2SH standardness ceiling. Regression
test: `chainstate::tests::accurate_sigop_count_tracks_the_previous_opcode_like_core`.

### A-02 — BIP68 activation gate used a signed transaction version

**Severity: Medium (consensus divergence — transaction acceptance).**
**Files: `src/chainstate.rs`, `src/transaction_admission.rs`.**

The relative-locktime gate read the version as a signed integer:

```rust
if csv_active && transaction.version.0 >= 2 {   // i32
```

Core's `CalculateSequenceLocks` casts before comparing, and says so explicitly:

```cpp
// tx.nVersion is signed integer so requires cast to unsigned otherwise
// we would be doing a signed comparison of versions
bool fEnforceBIP68 = static_cast<uint32_t>(tx.version) >= 2 && flags & LOCKTIME_VERIFY_SEQUENCE;
```

For every version whose high bit is set (`i32` negative, e.g. `0xFFFFFFFF`),
Core enforces BIP68 while rBTC skipped it. A transaction with such a version and
an unsatisfied relative lock would be rejected by Core and accepted by rBTC —
again a split on the permissive side. Non-standard versions cannot be relayed,
but consensus rules must still hold for blocks.

**Fix.** `enforces_bip68` performs the comparison in unsigned space, using the
same `to_ne_bytes`/`from_ne_bytes` reinterpretation already used by
`deployments::signals_taproot`. Both the consensus path and the mempool policy
path call it. Regression tests:
`chainstate::tests::bip68_enforcement_follows_core_unsigned_version_comparison`
and `chainstate::tests::negative_version_transactions_still_honor_relative_locks`.

### A-03 — Manifest digest parser panics on non-ASCII input

**Severity: Medium (denial of service via untrusted file).**
**Files: `src/archive.rs`, `src/snapshot.rs`.**

Both modules decoded a hex digest by slicing a `&str` at byte offsets after only
checking its **byte** length:

```rust
if value.len() != 64 { return None; }
...
*byte = u8::from_str_radix(&value[offset..offset + 2], 16).ok()?;
```

A JSON string of twenty-one three-byte characters plus one ASCII byte is 64
bytes long but has no character boundary at offset 2, so the slice panics.
Confirmed empirically:

```
thread '...' panicked at src\archive.rs:372:42:
byte index 2 is not a char boundary; it is inside '€' (bytes 0..3) of `€€€…€0`
```

Both reach untrusted input: `archive::decode_archive` and
`archive::read_archive_manifest` parse archive containers (the format the README
describes as ready for BitTorrent/webseed distribution, and which has a
dedicated `archive_decode` fuzz target), and `snapshot::validate_snapshot_manifest`
parses attacker-supplied snapshot manifests before any trust check.

**Fix.** Both decoders now operate on bytes with an explicit nibble table, so a
non-hex or non-ASCII digest returns `None` instead of unwinding. Regression
tests: `archive::tests::rejects_non_ascii_manifest_digests_without_panicking`
and `snapshot::tests::rejects_non_ascii_manifest_digests_without_panicking`.

### A-04 — Vendored build omits `SECP256K1_STATIC` on Windows

**Severity: High (the project did not link).**
**File: `vendor/bitcoinconsensus/build.rs`.**

`libsecp256k1` is compiled into a static archive, but its public header declares
every entry point `__declspec(dllimport)` on Windows unless the consumer defines
`SECP256K1_STATIC`:

```c
# elif !defined(SECP256K1_STATIC)
#  define SECP256K1_API extern __declspec (dllimport)
```

The C++ consensus sources (`pubkey.cpp`) were compiled without it, so the final
link failed with 17 unresolved `__imp_secp256k1_*` externals. `cargo check`
passes because it never links, which is why CI did not catch this.

**Fix.** Define `SECP256K1_STATIC` alongside `WIN32` in the shared base config.

### A-05 — Vendored build omits `ENABLE_MODULE_ELLSWIFT`

**Severity: High (the project did not link).**
**File: `vendor/bitcoinconsensus/build.rs`.**

Core 26's `pubkey.cpp` compiles `EllSwiftPubKey::Decode`, which references
`secp256k1_ellswift_decode`. The build script enables the schnorrsig, extrakeys,
and recovery modules but not ellswift, so the symbol is never emitted:

```
error LNK2019: unresolved external symbol secp256k1_ellswift_decode
```

The ellswift sources are present in the vendored tree
(`secp256k1/src/modules/ellswift`); only the feature define was missing.

**Fix.** Add `ENABLE_MODULE_ELLSWIFT` to the secp256k1 configuration.

### A-06 — Pruned ledger cannot fsync a directory on Windows

**Severity: High (component non-functional on the platform).**
**File: `src/ledger.rs`.**

`OsLedgerDurability::sync` was unconditionally:

```rust
File::open(path)?.sync_all()
```

`std::fs::File::open` cannot open a directory on Windows — it does not pass
`FILE_FLAG_BACKUP_SEMANTICS` — so every sync point returned
`ERROR_ACCESS_DENIED`. Because `sync_directory` is called on archive staging,
staged publication, slot publication, index publication, retired-slot removal,
and both truncation phases, the configurable circular pruned ledger fails on
every write. This surfaced as 22 failing `ledger::tests` with
`Io(Os { code: 5, kind: PermissionDenied })`.

Notably, the same problem was already handled elsewhere: `main.rs::sync_directory`
and `api.rs::sync_wallet_audit_parent` are `#[cfg(unix)]`-gated no-ops. Only the
ledger was left unconditional.

**Fix.** Gate the directory fsync the same way, with a comment recording that
the sync point is a documented no-op on non-Unix targets. All 22 tests now pass.

### A-07 — Panicking script worker hangs block validation forever

**Severity: Medium (liveness).**
**Files: `src/blockchain.rs`, `src/consensus.rs`, `src/chainstate.rs`.**

`DeferredScriptBatch` counts submitted work items and then blocks on
`recv()` once per item:

```rust
(0..self.work_items).filter_map(|_| self.results.recv().expect(...))
```

The result `Sender` is cloned into each `ScriptValidationWork` and consumed by a
worker. If a worker unwinds while holding one, that clone drops — but the batch
itself still owns `self.result`, so the channel never disconnects and `recv()`
blocks forever. The worker thread is also never replaced, so the pool silently
shrinks. A single panic inside a validation worker therefore stalls block
validation permanently rather than surfacing an error.

**Fix.** The worker now runs its job list inside `catch_unwind` and, on unwind,
reports a new `ConsensusError::WorkerPanicked` so the candidate block fails
closed. `ChainstateError::is_peer_invalid` returns `false` for that variant, so a
local worker fault is not misattributed to the peer that supplied the block.

### A-08 — Crash recovery rejects a transition with no net UTXO effect

**Severity: Medium (liveness after a crash).**
**File: `src/block_execution.rs`.**

`pending_utxo_state` collapsed an ambiguous observation into `Before`:

```rust
(true, _) => PendingUtxoState::Before,
```

When a write-ahead transition has no observable UTXO effect, the current
chainstate satisfies both its pre- and post-state, so both flags are true. The
connect-recovery arm for `execution_tip == pending.next` then required
`state == After` exactly and returned `InconsistentTransition`, which is fatal
and not retryable — the node refuses to start.

An empty transition is reachable: a block whose only transaction is a coinbase
with exclusively provably unspendable outputs spends nothing and creates nothing
(`created_outputs` filters `is_unspendable`, matching Core's `AddCoin`). Trivial
on regtest and signet; a mainnet miner would have to burn the reward, but the
recovery path must not be able to wedge the node.

**Fix.** Add an explicit `PendingUtxoState::Either` for the mutually-satisfiable
case and accept it from both directions via `matches_before` / `matches_after`.

### A-09 — `IsFinalTx` missed Core's zero-locktime early return

**Severity: Low (consensus parity).**
**File: `src/chainstate.rs`.**

Core returns early when there is no lock time:

```cpp
if (tx.nLockTime == 0) return true;
```

rBTC fell through to `lock_time < comparison`, which is `0 < 0` — false — at
height 0, and then depended on the sequence check. Only observable in a block at
height zero, which on every network contains just the genesis coinbase (whose
sequence is `SEQUENCE_FINAL`), so no behavioural difference was demonstrable.
Fixed anyway to keep the function a line-for-line match of Core.

### A-10 — The documented clippy gate did not pass off Unix

**Severity: Low (tooling).**
**Files: `src/api.rs`, `src/fee_estimator.rs`, `src/main.rs`,
`src/rebroadcast_store.rs`, `src/transaction_pool_store.rs`,
`tests/storage_recovery.rs`.**

`README.md` documents `cargo clippy --locked --all-targets --all-features -- -D warnings`
as a local gate, but it failed on Windows with seven lints: three
`unused_variables` for the `path` argument of `restrict_file_permissions`, four
`clippy::unnecessary_wraps` for `#[cfg(unix)]`-gated no-ops, and
`unused_imports` plus `dead_code` in `tests/storage_recovery.rs` for helpers used
only by the `#[cfg(unix)]` SIGKILL test.

**Fix.** Explicit `let _ = path;` in the non-Unix branches, targeted
`#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]` on the affected
functions with doc comments recording the platform limitation, and `#[cfg(unix)]`
on the Unix-only test imports and helper.

---

## Withdrawn finding

### A-11 — libsecp256k1 builds in 32-bit field mode under MSVC — **not a defect**

The original finding claimed MSVC builds fell back to the 10x26 field and 8x32
scalar. That was wrong, and the conclusion is withdrawn.

`USE_FIELD_5X52`, `USE_FIELD_10X26`, `USE_SCALAR_4X64`, and `USE_SCALAR_8X32`
do not appear anywhere in this libsecp256k1's sources. `field.h` and `scalar.h`
select their implementation solely from `SECP256K1_WIDEMUL_INT128`, and `util.h`
derives that itself — from a native `__int128` where one exists, and otherwise
from the `int128_struct` path, which on 64-bit MSVC is implemented with the
`_umul128` / `__umulh` intrinsics. MSVC x86_64 was therefore already getting the
64-bit limbs. The build script's defines were inert, and its
`cargo:warning=Compiling in 32-bit mode…` described its own `__int128` probe
rather than secp256k1's actual selection.

The lesson is the reverse of the finding: the build script was reporting a
configuration it did not control. It now compiles
`depend/check_widemul_int128.c`, which fails unless `SECP256K1_WIDEMUL_INT128`
is set, and warns only on a real fallback. Verified in both directions — silent
as configured, and firing under a forced `USE_FORCE_WIDEMUL_INT64`.

## Reported findings

### A-12 — Filesystem hardening is Unix-only

Every filesystem hardening measure is `#[cfg(unix)]`: the `0o600` mode on the
mempool, rebroadcast, and fee-estimator databases; the permission, hard-link,
and device/inode identity checks on the API authorization audit log; and
directory fsync. On Windows these files inherit their directory ACL, the audit
log's identity is not revalidated across the reopen, and atomic renames are not
followed by an explicit directory flush. The Rust standard library exposes no
portable equivalent for any of the three, so these remain platform limitations
and are now stated in `README.md`'s new "Supported platforms" section.

One part of this was not a limitation at all. The crash-recovery test was gated
`#[cfg(unix)]` on the grounds that signal-based termination has no Windows
equivalent, leaving redb's crash consistency and rBTC's recovery path unverified
there. But the test body only used portable APIs: `Child::kill` is `SIGKILL` on
Unix and `TerminateProcess` on Windows, and neither lets the child clean up,
which is exactly the precondition the test needs. The gate is removed and the
test now passes on Windows.

### A-13 — Header DAG is deep-cloned per header batch — fixed

`HeaderDag::validate_batch_contextual` cloned the whole DAG so a failed batch
left the original untouched:

```rust
let mut candidate = self.clone();
```

That copies the entire `HashMap<BlockHash, HeaderInfo>` plus the `active_chain`
vector for every 2,000-header batch. On mainnet the map reaches ~900k entries of
~150 bytes, so late batches copied well over 100 MB each and peak memory held
two copies, at a cost quadratic in chain length.

Replaced by `apply_batch_contextual`, which inserts in place and returns a
`HeaderBatchUndo`. Validation failure reverts before returning, so the
all-or-nothing guarantee is unchanged; the caller that appends to the durable
header journal reverts on a write failure, preserving the rule that the
in-memory DAG never retains headers it did not persist.

Undo is `O(batch)` in the common case: a plain extension only pushes onto the
active chain, so truncating to the recorded length restores it. The pre-batch
prefix is cloned only when a header could rebuild the chain rather than extend
it — and at that moment the prefix is still the original, precisely because
earlier extensions only pushed past it.

### A-14 — Several paths materialize the entire UTXO set — fixed in part

`UtxoOverlay::tier_stats` folded the entire UTXO set to take its length, which
at mainnet scale means pulling the whole chainstate into memory for one number.
Fixed: only outpoints the overlay has touched can differ from the base, so the
base count plus the overlay's net delta is exact and bounded by the overlay.
`RedbUtxoStore::age_to_cold` no longer buffers a second copy of every aged coin
either; it keeps the 36-byte keys and takes each value back from `hot.remove`.

Two full-set paths remain by construction and are now documented rather than
changed. `RedbChainStore::utxo_snapshot_page` and `snapshot_content_identity`
have no paged view while an unmaterialized validation journal is present,
because the logical set is the durable base folded with every journal delta.
Making those paged means merging journal deltas per page — a redesign of the
validation journal, not a local fix. `RedbUtxoStore::snapshot_entries` is
inherently a whole-set operation; `snapshot_page` is the bounded alternative and
is what the API path uses.

### A-15 — Fresh-output fast path defers a duplicate probe to commit — fixed

`UtxoOverlay::apply_with_undo_fresh_outputs` checks created outpoints only
against the in-memory overlay, relying on `apply_bip30_rules` having proven them
absent. But `apply_bip30_rules` returns immediately when neither
`bip30_enforced` nor `bip30_overwrite` is set — the normal case above mainnet
height 227,931 — so the durable store is never probed. The overlay then records
`original = None` for a coin that exists durably.

This is not exploitable: creating a duplicate outpoint requires a duplicate txid,
i.e. a SHA-256d collision for non-coinbase transactions, and Core's own analysis
shows no mainnet coinbase can collide before height 1,983,702 (which
`BIP34_IMPLIES_BIP30_LIMIT` re-enables the check from). It is also caught at
commit: `apply_validated_changes_transaction` returns `UtxoError::Duplicate`
because `hot.insert` reports a prior value and `replaces_spent` is false. Core
throws `std::logic_error` in the same situation. The difference is that rBTC
reports it as a persistence error rather than a validation error.

Fixed by documenting the invariant at the call site and re-checking it under
`cfg!(debug_assertions)`, so it cannot silently weaken while release builds keep
the fast path and still fail closed at commit.

### A-16 — `consensus_id()` omits the network — fixed, premise partly withdrawn

`DeploymentConfig::consensus_id()` encoded the Taproot version-bits parameters,
optional buried overrides, and an optional custom Signet challenge, but not the
network.

**Correction.** The report gave default Signet and Testnet4 as a concrete
colliding pair. That was true only because rBTC gave testnet4 Signet's BIP9
threshold of 1815; Core 28 uses the test-chain 1512 (see A-21). With the correct
value the two differ, and no current pair of networks collides. The structural
weakness is real regardless — the identity did not describe the network, so any
future pair agreeing on every parameter would collide — but the specific instance
was a symptom of A-21, not an independent defect.

Fixed anyway, because closing the class is cheap: encoding versions 4–6 append a
one-byte network tag. `legacy_consensus_id()` still produces versions 1–3, and
`bind_consensus_config` accepts those bytes once and rewrites them, so an
existing database migrates in place without a reindex. The test now asserts
injectivity by construction — distinct tags, distinct identities, and the
legacy encoding being the current one minus its version byte and tag — rather
than via a pair that no longer collides.

### A-17 — Testnet4 has no Core-26 rule set — fixed

`Network::Testnet4` was accepted throughout (deployments, checkpoints, IBD
policy) but validated with mainnet-style difficulty rules, because Core 26
predates testnet4 and the parameters came from `rust-bitcoin`'s `Params::TESTNET4`,
which carries no BIP94 flag. `expected_next_bits` therefore matched no Core
release for that network.

Both BIP94 mitigations are now implemented, ported from Core v28's source rather
than the BIP prose — the prose left the block-storm rule ambiguous enough that
implementing from it would have risked exactly the class of near-miss this audit
found in A-01 and A-02:

- **Block storm.** At a retarget, `CalculateNextWorkRequired` takes the base
  target from the *first* block of the period instead of the last. The last block
  may have used the minimum-difficulty exception, which would otherwise drag the
  entire next period down to the pow limit; the first block never can. rBTC
  already resolved that same header as the retarget timespan anchor, so this is a
  one-line change of which header supplies the base bits.
- **Timewarp.** `ContextualCheckBlockHeader` requires the first block of each
  difficulty period to be no more than `MAX_TIMEWARP` = 600 seconds earlier than
  its parent's *raw* timestamp (not its median time past). Added as
  `HeaderError::Timewarp`, classified as objectively peer-invalid, and evaluated
  between the median-time-past and future-time gates to match Core's order.

Gated on testnet4 only. Core 28 also sets `enforce_BIP94` for regtest, but rBTC
pins Core 26 for every network Core 26 defines, and adopting a later rule there
would diverge from that reference; regtest's `no_pow_retargeting` makes the
retarget half unreachable anyway.

Tests cover the timewarp floor's height selectivity and saturation, the retarget
base differing between testnet4 and testnet3, and — as the foundation the rest
rests on — testnet4's genesis hash and pow parameters matching Core.

### A-18 — Validation-delta decode re-derives bounds unchecked — fixed

`decode_validation_delta` called `inspect_validation_delta` first, which
validates every offset and length and requires the record to be exactly
consumed, then re-walked the entries with an unchecked `let end = offset + length;`
and sliced with it. Correct, but the second pass's panic-safety depended
entirely on the first. Now bounded independently with `checked_add` plus a range
check, so each pass is sound alone.

### A-19 — Deployment context accepts two unused parameters — fixed

`block_deployment_context_with_bip34_anchor` ignored `_block_time` and
`_taproot_active`, because Core 26 reduced block script flags to an
unconditional `P2SH | WITNESS | TAPROOT` plus two hash-keyed exceptions. The
public wrappers still required both, so every block-validation call site computed
a full BIP9 Taproot state — `taproot_active`, which walks and caches period-end
states — purely to discard it.

Both parameters are removed, which also takes that per-block work off the
validation path. `taproot_active` stays public as the version-bits machinery a
future deployment needs, with a doc note recording why validation does not
consult it.

### A-20 — Block locator step schedule differs from Core by one — fixed

`HeaderDag::block_locator` doubled its step once `locator.len() >= 10`; Core
doubles once `have.size() > 10`. The hash sets differed from height 21 down
(rBTC emitted `…23, 22, 20, 16, 8, 0` where Core emits `…23, 22, 21, 19, 15, 7, 0`).
Locators are peer hints with no consensus meaning and both stayed well inside the
101-hash bound, so this was cosmetic — but there is no reason to differ, and the
loop is now Core's shape: push, break at genesis, step back, then widen.

### A-21 — Testnet4 used Signet's BIP9 threshold and had no trust anchors — fixed

Found while reading Core v28's `chainparams.cpp` for A-17, and the root cause of
A-16's reported collision.

`DeploymentConfig::for_network` grouped Testnet4 with Signet, giving it the 90%
threshold of 1815; Core 28's testnet4 uses the 75% test-chain value of 1512.
Never consulted, because Taproot is always active on testnet4, but it is part of
the persisted execution identity — and it made two networks' identities
byte-identical. Verified every network's threshold against Core v28 while fixing
it: mainnet 1815, testnet3 1512, testnet4 1512, signet 1815, regtest 108. Only
testnet4 was wrong.

`IbdPolicy::for_network` also gave testnet4 a zero work floor and no assume-valid
anchor, with the comment that "Core 26 predates testnet4, so no trust constants
are invented". Core 28 defines both, so they are now pinned from it rather than
left open — the reasoning that forbade inventing them does not apply to copying
them from the release that defines the network.

---

## Areas verified as correct

These were checked line-by-line against Core 26 and found faithful. They are
recorded so a future audit does not have to redo the comparison.

**BIP325 Signet (`src/signet.rs`).** `extract_solution` reproduces
`FetchAndClearCommitmentSection` exactly, including truncating the extracted
pushdata back to the four-byte header and re-pushing it, Core's minimal-push
re-encoding thresholds (`< OP_PUSHDATA1`, `<= 0xff`, `<= 0xffff`), the
`GetOp`-failure break, and the same behaviour for a zero-length `OP_PUSHDATA1`.
The commitment output is the last match with `scriptPubKey.len() >= 38`, matching
`GetWitnessCommitmentIndex`. The synthetic `to_spend`/`to_sign` pair, the
`block_data` field order, and the `P2SH | WITNESS | DERSIG | NULLDUMMY` flag set
all match.

**BIP9 deployment state machine (`src/deployments.rs`).** `threshold_state`
and its cached variant reproduce `AbstractThresholdConditionChecker::GetStateFor`:
the same period rounding (Core's `nHeight - ((nHeight + 1) % nPeriod)`), the
`DEFINED → STARTED` MTP comparison, threshold counted before timeout so a
threshold reached at the timeout boundary still locks in, `LOCKED_IN → ACTIVE`
gated on `period_end + 1 >= min_activation_height`, and `Condition()`'s top-bits
mask evaluated in unsigned space. Keying the cache by period-end block hash
correctly isolates side branches. `ALWAYS_ACTIVE` / `NEVER_ACTIVE` sentinels and
the regtest-only `-vbparams` / `-testactivationheight` grammars match Core's
option parsing, including the `height < INT_MAX` range and last-value-wins.

**Script flags and buried deployments.** The two `script_flag_exceptions`
(the BIP16-violating block `00000000000002dc…` and the Taproot-violating block
`0000000000000000000f14c3…`) and the mainnet/testnet activation heights match
Core 26's chainparams exactly. `minimum_block_version_for_heights` returns the
maximum applicable requirement and stays correct under non-monotonic regtest
overrides.

**BIP30 (`src/deployments.rs`, `src/block_execution.rs`).** The two historical
repeat exceptions, the BIP34-anchor optimization (`pindex->pprev->GetAncestor(BIP34Height)`
compared against `BIP34Hash`), the `BIP34_IMPLIES_BIP30_LIMIT` re-enablement at
1,983,702, and the zero `BIP34Hash` on regtest/signet/testnet4 forcing permanent
enforcement all match. The exception path's overwrite undo is inserted at index 0
so reverse-order disconnect restores the overwritten coins after the block's own
creations are removed.

**Block structure (`src/blockchain.rs`).** Merkle root with CVE-2012-2459
mutation detection over complete pairs only (matching Core's
`pos + 1 < hashes.size()` loop before the odd-count duplication); coinbase
presence and uniqueness; BIP34 height encoding matching `CScript() << nHeight`
and `CScriptNum::serialize` including the sign-byte extension; weight checked
after the witness commitment, as Core does, so a coinbase-witness stuffing attack
cannot mark a block permanently invalid. `base_size * 4 > MAX_BLOCK_WEIGHT` and
the transaction-count bound are implied by `weight <= 4,000,000`.

The witness commitment check deserves a specific note. It calls
`rust-bitcoin`'s `Block::check_witness_commitment`, which returns `true` early if
no transaction carries witness data — that would skip verifying the commitment
digest entirely. rBTC is safe because it first requires the coinbase witness to
hold exactly one 32-byte element, which guarantees the block has witness data and
makes the early return unreachable. That ordering is load-bearing and should not
be rearranged.

**Transaction validation (`src/chainstate.rs`).** All of `CheckTransaction` and
`CheckTxInputs`: empty inputs/outputs, `base_size * 4` oversize,
CVE-2010-5139 per-output and cumulative value range, CVE-2018-17144 duplicate
inputs, null prevout for non-coinbase, coinbase scriptSig length 2..=100,
coinbase maturity as `nSpendHeight - nHeight < 100`, input-value range, and
inflation. `is_unspendable` matches `CScript::IsUnspendable`, and unspendable
outputs are excluded from the UTXO set exactly as Core's `AddCoin` does.

**BIP68 / BIP112 / BIP113.** `check_sequence_lock` reproduces
`CalculateSequenceLocks` and `EvaluateSequenceLocks`, including the "subtract 1
to keep nLockTime semantics" adjustment, the disable and type flag masks, the
9-bit granularity shift, and — importantly — the correct time base:
`creation_mtp` stores the MTP of the block *preceding* the output's creating
block, matching Core's `GetAncestor(max(nCoinHeight - 1, 0))->GetMedianTimePast()`,
and same-block spends resolve to the candidate height and its parent MTP.

**Headers (`src/headers.rs`).** `median_time_past` matches
`CBlockIndex::GetMedianTimePast` including its `count / 2` index on short
windows; the retarget epoch boundary, the `saturating_sub` on a non-monotonic
timespan (clamped identically to Core's `nPowTargetTimespan / 4` floor), and the
testnet minimum-difficulty walk-back all match `GetNextWorkRequired`. Regtest's
`fPowNoRetargeting` short-circuit reaches the same result as Core's
min-difficulty branch because every regtest header carries the pow limit. Best
tip selection uses a strict `>`, preserving first-received on equal work as
Core's sequence-id tiebreak does. Checkpoint hashes and the
`bad-fork-prior-to-checkpoint` gate match.

**Fee, subsidy, and sigop accounting.** Per-transaction fee derived from
validated prevouts with checked arithmetic and a `MAX_MONEY` ceiling; block fee
accumulation with overflow and range checks; `subsidy + fees` compared against
the coinbase output sum, matching `bad-cb-amount`; network-specific halving
intervals; `>= 64` halvings yielding zero.

**Relay policy (`src/transaction_policy.rs`).** Standard version, minimum
non-witness size, weight ceiling, scriptSig size and push-only, the output
template set, single `OP_RETURN` with push-only payload and the 83-byte
data-carrier limit, dust via `minimal_non_dust` (which never diverges from
`GetDustThreshold` for a standard script), P2WSH script/stack/item limits,
tapscript annex and leaf-version policy, and `GetVirtualTransactionSize`'s
sigop-weighted vsize.

**P2P transport (`src/p2p.rs`).** Every unbounded-work entry point is bounded
before allocation: the 4,000,000-byte payload cap enforced from the frame header
before `Vec::with_capacity`, the 8-message handshake budget, the 32-message
response budget, 256-byte user agent, 101-hash locator, 1,000-address `addr`
response, `MAX_INV_SIZE` inventory, 2,000-header response, the consensus-derived
16,666-transaction compact-block reference cap, and byte-and-count budgets on
the pending-message, announced-transaction, and inventory queues.

**API surface (`src/api.rs`, `src/main.rs`).** Bearer comparison is
constant-time with the supplied length bounded before the loop; the header
grammar is deliberately narrow (single space, case-insensitive scheme); tokens
must be 32–256 printable ASCII bytes read from an owner-only file, and the RPC
and wallet token files must be distinct; the audit log is append-only, size
bounded, rolled back on a partial write, and validated for a complete trailing
record on open; `--explorer-listen` is rejected unless the address is loopback,
because explorer REST routes are unauthenticated; explorer inputs are
length- and charset-checked and pagination is bounded.

**Storage decoders (`src/utxo.rs`, `src/undo_store.rs`, `src/chain_store.rs`,
`src/archive.rs`, `src/snapshot.rs`, `src/peer_store.rs`,
`src/validation_owner.rs`).** Every length prefix is bounded against the
remaining record before it drives an allocation; `serde` structs that parse
persisted state use `deny_unknown_fields`; zstd frames carry an authenticated
uncompressed length that also derives the window-log ceiling, and decompression
reads one byte past the limit to detect an overrun; archive piece digests are
verified before the record stream is decompressed; the validation bloom's byte
count has a `.max(1)` floor that makes its modulo arithmetic
division-by-zero-free. `commit_connect_batch` correctly folds an outpoint that is
created and later spent within one checkpoint, and
`apply_validated_changes_transaction` handles the spend-then-recreate case
through its merged ordered walk.

---

### A-22 — Regtest difficulty interval differed from Core — fixed

`rust-bitcoin`'s `Params::REGTEST` carries mainnet's two-week
`pow_target_timespan`, yielding a 2,016-block interval. Bitcoin Core v31.1 uses
one day with ten-minute spacing, yielding 144. rBTC now constructs its header
parameters through a narrow `core_params` correction and leaves every other
network unchanged. Regtest's no-retarget rule still returns the parent's target.

### A-23 — Unconditional regtest BIP94 proposal — rejected

The proposed follow-up stated that Core enables BIP94 for regtest and changed
rBTC accordingly. Core v31.1 does not do that by default:
`RegTestOptions::enforce_bip94` initializes to `false`;
`ReadRegTestArgs` changes it only for `-test=bip94`; and
`CRegTestParams` copies that option into consensus parameters. rBTC has no
equivalent test-only switch, so its default regtest must remain off. Testnet4
continues to enforce BIP94.

The corrected third-wave disposition and later relay-index integration passed
strict all-target/all-feature Clippy and the complete main-branch suite (570
library tests passed, two
explicitly ignored), in addition to the release-mode Core 31/btcd inbound
interoperability gate.

### Disposition of the later depth-review note (`7095993`)

The additional audit commit changed documentation only and reported no new
defects. Its address-manager constants, wallet PSBT defence-in-depth, explorer
atomic disconnect, and reorg-termination observations remain valid on main and
are retained as review evidence.

Its RBF/ancestor/CPFP narrative describes the pre-Core-31 admission code and is
not merged as a current claim. Main has since moved to Core 31's 0.1 sat/vB
incremental fee, 64-transaction/101-kvB clusters, TRUC, one-parent/one-child
package relay, ephemeral dust, and full-RBF default. The supported replacement
path is intentionally conservative; exact Core 31 feerate-diagram and
sibling-eviction ordering is documented P2 work. The note's `main.rs` wording
is also historical because the daemon is now a thin adapter over `node`.

The note explicitly left the post-handshake P2P/BIP152 state machine and several
small stores without line-by-line Core comparison. Those paths have bounded
property/adversarial tests, real Core 31/btcd handshake evidence, storage
restart/crash coverage, and the seven-matrix Core block differential, but the
report does not relabel that evidence as a line-by-line audit. No code from
`7095993` required integration.

### Disposition of the compact-block/dispatch review (`da76a5c`)

The next audit commit completed the previously open line-by-line review of
BIP152 reconstruction and post-handshake vector/message bounds and reported no
consensus, safety, or resource-bound defect. Its differential-prefill,
short-ID-collision, exact missing-response, Merkle/witness, repeated-version,
and response-budget conclusions were rechecked against the current main branch
and remain applicable. The document commit itself is not merged because its
base still contains the rejected unconditional-regtest-BIP94 claim and the
pre-Core-31 mempool narrative described above.

The review did identify one information-level CPU amplification: a maximum
50,000-entry `getdata` performed a linear scan of the 64-entry per-peer relay
cache for every distinct request, or about 3.2 million comparisons. Main now
maintains a bounded `HashMap<Inventory, usize>` beside the FIFO. Legacy txid
announcements index both ordinary and witness-aware request aliases; BIP339
wtxid announcements remain namespace-separated. FIFO insertion, duplicate
replacement, and eviction rebuild at most 128 index rows, while request lookup
is expected O(1) and a maximum batch is expected O(50,000), not O(3.2 million).
Regression coverage exercises aliasing, replacement, byte accounting, and the
64-entry eviction boundary.

### Disposition of the driver and persistence review (`5147e11`)

This documentation-only audit commit completed the depth pass over the IBD
checkpoint driver, fee estimator, header journal, and transaction-pool
persistence. Its conclusions were rechecked against current main. The IBD
driver now lives in `node.rs`, but retains the reviewed ordering: a staged
archive is not published before its checkpoint, prefetched UTXOs are matched
element-by-element to independently derived outpoints, and the commit
transaction again requires each removal to exist and each insertion not to
collide. `TrackedFee` construction and every persisted decode path reject a
zero or oversized policy vsize before rate division. Header-journal replay
revalidates each stored header contextually. Relay-attempt and admission-time
maps are decoded within fixed bounds and cross-checked against the active
transaction set.

Two header replay observations are retained as operational, not correctness,
findings: moving the local clock backwards by more than the future-time
allowance can make a previously accepted journal fail closed on restart, and
the Testnet3 minimum-difficulty walk-back has an
`O(headers × difficulty_interval)` worst case during full replay. The commit's
repeated information-level `getdata` scan description is historical on main;
the bounded relay hash index described above has already removed it.

The auditor still excludes line-by-line review of peer management, API wiring,
CLI/shutdown sequencing, and the policy behaviour of `rebroadcast_store.rs`
and `undo_store.rs`. Existing tests and soak evidence cover those paths but are
not mislabeled as completion of that remaining audit scope. No code delta from
`5147e11` required integration.

### Post-audit fuzz-workspace assurance closure

A final clean-tree supply-chain replay exposed an independent test-assurance
gap outside the auditor's code patches. Cargo patches apply at a workspace
root, and `fuzz/` is deliberately its own workspace, so it had not inherited
the repository root's vendored `bitcoinconsensus` and `redb` patches. Its stale
lock file still named the crates.io implementations; after current root
dependencies were resolved, a locked build failed because the public
`bitcoinconsensus` crate does not contain rBTC's reviewed Taproot spent-output
and transaction-batch ABI. Consequently the fuzz workflow could not be treated
as evidence that the production interpreter/storage implementations were under
test.

The fuzz manifest now repeats both path patches explicitly, its lock file names
the path packages and current root dependency closure, and CI performs a locked
check plus warning-denying Clippy before running targets. The regression script
uses the exact dated nightly installed by CI instead of the moving `+nightly`
alias. A local locked check, Clippy run, RustSec/cargo-deny pass, and all twelve
ASan seed-corpus targets completed against the corrected graph. This was a
build/test-provenance issue, not a change to runtime consensus behavior.

## Recommendations

1. Add a Windows job to `.github/workflows` that runs `cargo test --locked --all-features`.
   Every one of A-04, A-05, A-06, and A-10 would have been caught by a job that
   links and runs tests on `x86_64-pc-windows-msvc`.
2. Extend `tests/core_consensus_vectors.rs` with Core's sigop-counting vectors.
   A-01 lived in a consensus-critical helper that the existing Core vector suite
   did not exercise.
3. Treat every `rust-bitcoin` consensus helper as requiring differential
   verification before use. A-01 came from `count_sigops`, and
   `check_witness_commitment` is only safe here because of a load-bearing
   pre-check. A short list of "verified-equivalent upstream helpers" in
   `docs/ARCHITECTURE.md` would make that dependency explicit.
4. Keep the pinned Core v26 interpreter boundary separate from the tracked
   Core v31.1 consensus parameters. Testnet4 and later consensus changes require
   source comparison and dedicated vectors even though the frozen C ABI cannot
   be re-pinned past Core 27.
5. Add a test-only regtest BIP94 switch only if Core's `-test=bip94`
   interoperability is needed. It must never silently change the default
   regtest rule set.
6. Give the validation journal a paged read path. It is the last remaining
   whole-set materialization (A-14), reachable from the API's UTXO page endpoint
   while a journal is unmaterialized.

## Second pass

After the findings above were fixed, the ten items originally recorded as
"Reported" were revisited and closed. That pass produced two corrections to this
report -- A-11 withdrawn entirely, A-16's concrete instance traced to A-21 -- and
one new finding, A-21. Both corrections are stated in place above rather than
silently edited away.

Total across both passes: 484 tests passing, `fmt` and
`clippy -D warnings` clean, every commit independently buildable.

## Changes applied

```
 src/api.rs                       |   6 ++
 src/archive.rs                   |  61 +++++++++++++-
 src/block_execution.rs           |  28 +++++--
 src/blockchain.rs                |  30 ++++---
 src/chainstate.rs                | 169 +++++++++++++++++++++++++++++++++++++--
 src/consensus.rs                 |   6 ++
 src/fee_estimator.rs             |   9 +++
 src/ledger.rs                    |  15 +++-
 src/main.rs                      |   5 ++
 src/rebroadcast_store.rs         |   9 +++
 src/snapshot.rs                  |  47 ++++++++++-
 src/transaction_admission.rs     |   6 +-
 src/transaction_policy.rs        |   4 +-
 src/transaction_pool_store.rs    |   9 +++
 tests/storage_recovery.rs        |   6 +-
 vendor/bitcoinconsensus/build.rs |   9 +++
```

No public API was removed or renamed. `ConsensusError::WorkerPanicked` is the
only new public variant.
