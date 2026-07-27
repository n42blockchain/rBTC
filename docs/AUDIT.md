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
| `cargo test --locked --all-features` | **fail** (22 failures) | pass — 476 tests, 0 failures |

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
| A-11 | Info | Performance | libsecp256k1 builds in 32-bit field mode under MSVC | Reported |
| A-12 | Info | Portability | Filesystem hardening is Unix-only | Reported |
| A-13 | Low | Performance | Header DAG is deep-cloned per header batch | Reported |
| A-14 | Low | Performance | Several paths materialize the entire UTXO set | Reported |
| A-15 | Low | Robustness | Fresh-output fast path defers a duplicate probe to commit | Reported |
| A-16 | Info | Consensus | `consensus_id()` omits the network | Reported |
| A-17 | Info | Consensus | Testnet4 has no Core-26 rule set | Reported |
| A-18 | Info | Robustness | Validation-delta decode re-derives bounds unchecked | Reported |
| A-19 | Info | Hygiene | Deployment context accepts two unused parameters | Reported |
| A-20 | Info | Hygiene | Block locator step schedule differs from Core by one | Reported |

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

## Reported findings (not changed)

### A-11 — libsecp256k1 builds in 32-bit field mode under MSVC

`vendor/bitcoinconsensus/build.rs` selects `USE_FIELD_5X52` / `USE_SCALAR_4X64`
only when a `__int128` probe compiles. MSVC has no `__int128`, so every Windows
build emits:

```
warning: bitcoinconsensus: Compiling in 32-bit mode on a 64-bit architecture due to lack of uint128_t support.
```

The result is correct but uses the 10x26/8x32 field and scalar implementations,
a substantial signature-verification slowdown exactly where IBD spends its time.
This is inherited from the upstream crate. Options are to build the C sources
with clang-cl on Windows, or to document Windows as a non-performance target.

### A-12 — Filesystem hardening is Unix-only

Every filesystem hardening measure is `#[cfg(unix)]`: the `0o600` mode on the
mempool, rebroadcast, and fee-estimator databases; the permission, hard-link,
and device/inode identity checks on the API authorization audit log; and
directory fsync. On Windows these files inherit their directory ACL, the audit
log's identity is not revalidated across the reopen, and atomic renames are not
followed by an explicit directory flush. Separately, the SIGKILL crash-recovery
test in `tests/storage_recovery.rs` is `#[cfg(unix)]`, so the durability
property it proves is unverified on Windows. Recommend stating the supported
platform explicitly in `README.md`'s safety-status section.

### A-13 — Header DAG is deep-cloned per header batch

`HeaderDag::validate_batch_contextual` clones the whole DAG so a failed batch
leaves the original untouched:

```rust
let mut candidate = self.clone();
```

That copies the entire `HashMap<BlockHash, HeaderInfo>` plus the `active_chain`
vector for every 2,000-header batch. On mainnet the map reaches ~900k entries of
~150 bytes, so late batches copy well over 100 MB each and peak memory holds two
copies. Total cost is quadratic in chain length. A staged journal of pending
insertions, rolled back on failure, would keep the same atomicity guarantee at
`O(batch)` cost.

### A-14 — Several paths materialize the entire UTXO set

`RedbUtxoStore::snapshot_entries` builds a `BTreeMap` of every coin;
`UtxoOverlay::snapshot_entries`, `replace_all`, and `tier_stats` all call it
(`tier_stats` only to count). `RedbUtxoStore::age_to_cold` buffers every aged hot
row as owned `(key, value)` pairs before writing. At mainnet scale each is
multi-gigabyte. `snapshot_page` already provides the bounded alternative and
`chain_store` uses it for the API path; the overlay's `tier_stats` in particular
should count rather than materialize.

### A-15 — Fresh-output fast path defers a duplicate probe to commit

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
reports it as a persistence error rather than a validation error. Recommend a
debug assertion documenting the invariant, so it cannot silently weaken.

### A-16 — `consensus_id()` omits the network

`DeploymentConfig::consensus_id()` encodes the Taproot version-bits parameters,
optional buried overrides, and an optional custom Signet challenge, but not the
network. Default Signet and Testnet4 share identical Taproot parameters, buried
activation heights, and halving interval, so their identities collide. Not
exploitable, because `RedbExecutionStore::from_database` separately binds the
network genesis hash and rejects a mismatch, but including the network would make
the identity self-describing rather than dependent on a second check.

### A-17 — Testnet4 has no Core-26 rule set

`Network::Testnet4` is accepted throughout (deployments, checkpoints, IBD
policy), but Core 26 predates testnet4, which shipped in Core 28. The
parameters come from `rust-bitcoin`'s `Params::TESTNET4` and omit testnet4's
BIP94 timewarp-fix retarget rule and its minimum-difficulty-at-retarget
exception, so `expected_next_bits` does not match any Core release for that
network. `IbdPolicy::for_network` already declines to invent trust constants for
testnet4; the same reasoning suggests either implementing BIP94 or documenting
testnet4 as unsupported.

### A-18 — Validation-delta decode re-derives bounds unchecked

`decode_validation_delta` calls `inspect_validation_delta` first, which validates
every offset and length and requires the record to be exactly consumed, then
re-walks the entries with an unchecked `let end = offset + length;` and slices
`&encoded[offset..end]`. Correct today, but the panic-safety of the second pass
depends entirely on the first. These records come from the node's own redb rather
than the network, so severity is low; a `checked_add(...).ok_or(...)` in the
second pass would make each pass independently sound.

### A-19 — Deployment context accepts two unused parameters

`block_deployment_context_with_bip34_anchor` ignores `_block_time` and
`_taproot_active`, because Core 26 simplified block script flags to an
unconditional `P2SH | WITNESS | TAPROOT` plus two hash-keyed exceptions. The
public wrappers still require both, so every caller computes a full BIP9 Taproot
state (`taproot_active`, which walks and caches period-end states) purely to
discard it. The BIP9 machinery is correct and worth keeping for future
deployments, but the parameters should either be consumed or removed so the API
does not imply a dependency that does not exist.

### A-20 — Block locator step schedule differs from Core by one

`HeaderDag::block_locator` doubles its step once `locator.len() >= 10`; Core
doubles once `have.size() > 10`. The resulting hash sets differ slightly
(rBTC emits heights `…23, 22, 20, 16, 8, 0` where Core emits
`…23, 22, 21, 19, 15, 7, 0`). Locators are peer hints with no consensus meaning
and both stay well inside the 101-hash bound, so this is cosmetic only.

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
4. Replace the per-batch `HeaderDag` clone (A-13) before mainnet IBD is a
   headline use case; it is the clearest quadratic cost remaining on the sync
   path.
5. State the supported platform set in `README.md`. Several documented safety
   properties — file permissions, directory fsync, audit-log identity, SIGKILL
   crash recovery — hold only on Unix (A-12).

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
