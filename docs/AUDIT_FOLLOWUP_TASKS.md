# Audit follow-up work order

Open items remaining after the four audit passes recorded in
[docs/AUDIT.md](AUDIT.md). Verified against the tree at merge commit `41fa2cf`
(`origin/main` `8c7451f` plus the Windows regression fixes); every "current
state" below was re-checked in code, not carried over from the report.

**Acceptance criteria for every task in this document**, unless a task says
otherwise:

```
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
```

All three must pass on **both** `x86_64-unknown-linux-gnu` and
`x86_64-pc-windows-msvc`. The `windows` job in `.github/workflows/ci.yml` covers
the second. Do not weaken or `#[ignore]` an existing test to make a task pass; if
a test genuinely encodes a wrong expectation, say so and explain why before
changing it.

Consensus rule changes additionally require a differential citation: quote the
Bitcoin Core source that justifies the behaviour, with the release tag. rBTC
pins the Core 26 script interpreter but tracks Core 31.1's consensus *rules* —
see [docs/CORE31_COMPATIBILITY.md](CORE31_COMPATIBILITY.md).

## Resolution status

Revalidated and executed on 2026-07-27:

- **A-13 completed, with a corrected premise.** The merged production paths
  already used the `StagedHeaderBatch` in-place/RAII implementation. The stale
  cloning API survived only in tests. It has been removed; tests now stage and
  commit, and failed batches prove that both the tip and index roll back.
  Persistence keeps the guard uncommitted until the header journal append
  succeeds, while non-persisting validation commits immediately.
- **A-14 completed, with a corrected partial premise.** The journal's existing
  sorted fixed-width delta indexes are sufficient: pages now merge bounded base
  and per-row key windows, batch-resolve overlays, skip removals, and continue
  until the logical page is full. No format migration is needed.
  `snapshot_content_identity` had already been changed to a bounded visitor in
  the merged tree.
- **A-27 completed.** The external suite now checks 2,414 scripts from Core
  v26's script corpus against an independent transcription of
  `CScript::GetSigOpCount`, pins five authenticated historical block costs, and
  includes the stale-pushnum edge absent from the public corpus. Temporarily
  restoring rust-bitcoin's counter made the suite fail (`3 != 21`).
- **A-28 stopped because its required premise is unavailable.** Stock Core 31
  has no testnet4 retarget-interval override. `submitheader` checks proof of
  work before the contextual BIP94 boundary rule, and testnet4's real
  `0x1d00ffff` proof of work makes a synthetic 2,016-header boundary infeasible.
  `-testactivationheight` does not change PoW parameters, and
  `getblocktemplate` cannot synthesize a historical boundary. A test that only
  calls rBTC, or whose mutated header fails merely with `high-hash`, would not
  be differential evidence and was deliberately not added.
- **A-32 completed.** The test now receives an explicit event after both peer
  connections have been accepted but before either handshake is served. That
  event, rather than elapsed wall time, proves peer startup is concurrent. A
  separate 60-second timeout remains only as a loaded-runner liveness bound for
  deadlock detection; it no longer encodes the concurrency property.

Group B decisions:

- **B-1: match Core and adopt `bitcoin` 0.32.102.** The compatibility decoder
  accepts only an exactly eight-byte, checksum-valid out-of-range `feefilter`
  frame, after which the existing session logic ignores it. Wrong lengths and
  checksums remain protocol violations. This avoids dependency error-string
  matching and preserves Core 31's `MoneyRange`-then-ignore policy.
- **B-2: adopted, with the evidence the deferral asked for.** `sha2` 0.11 is in.
  The deferral required "immutable fixtures prove every persisted digest
  byte-identical", so `tests::digest_fixtures` in `utxo.rs` now pins the
  UTXO-set identity digest, the canonical `key || encoded UTXO` record digest,
  the canonical UTXO encoding, and the exact persisted validation-bloom record
  including its checksum. The values were captured under 0.10 and hold under
  0.11. Compressed artefacts stay unpinned on purpose: archive piece digests
  track zstd, not `sha2`, so the uncompressed stream they cover is pinned
  instead. 0.11 dropped `LowerHex` on its output, fixed with one shared
  `utxo::hex_lower`. `rand` 0.10 is in separately, as the deferral itself
  suggested; `random_range` moved to the new `RngExt` trait, and 0.9 remains in
  the dev graph only because `proptest` pins it.
- **B-3: fixed without a lock-range change.** The objection was that owner
  detail is "diagnostic-only and does not justify a cross-version lock-range
  compatibility change" -- correct, so no lock range moved. The marker is now
  also published to an unlocked `.rbtc.lock.owner` sidecar, which both platforms
  can read while the lock is held; the lock is still a whole-file lock on
  `.rbtc.lock`, so mixed-version pairs still exclude each other. The in-file
  marker is retained, and contention reads the sidecar first and falls back to
  it, so a lock taken by an older release is still attributed. The sidecar is
  owner-only, published through a create-new staging file plus rename so
  pre-positioned links cannot redirect a truncate, removed on release, and
  admitted by both data-directory allowlists.
- **B-4: retain the repository-owned Core 26 script boundary.** A future
  script-rule deployment, applicable interpreter security fix, or stable kernel
  API that preserves atomic chainstate is the trigger for a separately gated
  `libbitcoinkernel` migration; see
  [CORE31_COMPATIBILITY.md](CORE31_COMPATIBILITY.md).

The lower-depth Group D pass was also completed. It found and fixed three
non-consensus issues:

- **A-29: a full stale persisted-peer wave suppressed DNS bootstrap.**
  `MAX_CONFIGURED_PEERS` now bounds each connection wave rather than the
  lifetime candidate set. After up to 16 explicit/persisted failures the node
  still resolves one independently bounded, de-duplicated DNS wave. A daemon
  test fills the complete first wave with unreachable addresses and proves the
  fresh DNS peer is reached and later reused.
- **A-30: an old UTXO index baseline was treated as current.** Only a baseline
  at the exact execution tip may replace missing history. An older baseline
  now still requires a retained suffix or authenticated history peer.
- **A-31: unrelated CLI inbound overrides suppressed config limits.** Missing
  option-group mappings caused `listen`, connection caps, upload quota, and
  request rate to share the fallback `unknown` group. Every field now has a
  distinct group, and a file/CLI merge regression proves overriding request
  rate retains the other configured limits.

The systematic pass over `diagnostics`, `snapshot_download`,
`auxiliary_index`, `index_policy`, `node::config_file`, `undo_store`, and the
API/CLI wiring found no further correctness, durability, authentication, or
unbounded-resource defect. The accepted platform limitations in Group C remain
unchanged.

---

## Group A — ready to implement, no decision needed

### A-13 · Replace the per-batch header DAG deep clone

**Severity: Low (performance, quadratic in chain length).**
**File: `src/headers.rs:317`.**

Current state:

```rust
pub fn validate_batch_contextual(&self, headers: &[Header], adjusted_time: u32)
    -> Result<(Self, Vec<HeaderInfo>), HeaderError> {
    let mut candidate = self.clone();
    ...
}
```

This clones the whole `HashMap<BlockHash, HeaderInfo>` plus `active_chain` for
every 2,000-header batch so that a failed batch leaves the original untouched. At
the mainnet tip the map is ~900k entries, so each late batch deep-copies well over
100 MB and peak memory holds two copies; total cost is quadratic in chain length.

Required: insert in place and return an undo token. Validation failure must
restore the DAG exactly before returning, and the caller that appends to the
durable header journal must be able to restore it when *its* write fails, so the
in-memory DAG never retains headers that did not reach disk.

The undo must be `O(batch)` in the common case: a plain extension only pushes onto
`active_chain`, so recording the pre-batch length and truncating is sufficient.
Clone the pre-batch prefix only when a header could rebuild the chain rather than
extend it — at that moment the prefix is still the original, precisely because
earlier extensions only pushed past it.

A working implementation exists at commit `442a8ab` (`perf: apply header batches
in place instead of cloning the DAG`); it was not carried into main. Recover it
with `git show 442a8ab -- src/headers.rs`, or reimplement. Either way re-derive
the correctness argument rather than trusting the diff.

Call sites to update: `src/node.rs:15831`, the two tests in `src/headers.rs`
(~1091, ~1100), and the doc reference in `src/header_store.rs:97`. The two
production call sites in `node.rs` — the standby header path and the persisting
IBD path — must differ: only the latter reverts on a journal write failure.

Tests required: batch rollback on a failing member leaves tip and index
untouched; explicit revert restores a *reorganised* active chain (the case
truncation alone cannot undo).

### A-14 · Give the validation journal a paged UTXO read path

**Severity: Low (unbounded memory on a live API path).**
**File: `src/chain_store.rs:1520`.**

`utxo_snapshot_page` has no paged view while an unmaterialized validation journal
is present, so it falls back to `snapshot_entries()` — materializing the durable
base folded with every journal delta. That is reachable from the API's UTXO page
endpoint and is multi-gigabyte at mainnet scale. `snapshot_content_identity` has
the same shape.

Required: serve a bounded page without materializing the fold. The base already
has `RedbUtxoStore::snapshot_page`; the work is merging journal deltas per page in
key order. Note that a delta can both remove a base entry and add a new one, so
the merge must be able to skip and substitute while still returning exactly
`limit` entries and a correct exclusive cursor.

If a bounded merge turns out to require a journal index that does not exist yet,
say so and propose the index rather than partially implementing the merge.

### A-27 · Add Core's sigop-counting vectors to the differential suite

**Severity: Low (test coverage for a High-severity past defect).**
**File: `tests/core_consensus_vectors.rs`.**

A-01 was a consensus divergence in accurate sigop counting, and the existing Core
vector suite does not exercise sigop counting at all — `core_consensus_vectors.rs`
covers script and transaction validity only. The regression test added for A-01
lives in `chainstate.rs` unit tests and asserts hand-derived values.

Required: drive `chainstate::count_script_sigops` from Core's own data so the
counter is pinned to upstream rather than to my reading of it. Core has no
standalone sigop vector file, so derive cases from
`tests/data/bitcoin-core-26/script_tests.json` scripts plus the historical blocks
already vendored under `tests/data/bitcoin-core-26/`, and assert the block-level
`sigop_cost` of those real blocks against values computed from Core's
`GetTransactionSigOpCount` semantics.

At minimum the suite must fail if `count_script_sigops` is reverted to
`rust-bitcoin`'s `Script::count_sigops`, which is what the defect was. Verify that
by temporarily reverting and confirming a failure.

### A-28 · Extend the differential harness to testnet4 and boundary heights

**Severity: Low (unverified consensus rules).**
**File: `tests/core_block_differential.rs`.**

The harness drives a real `bitcoind` but only ever constructs `Network::Regtest`.
The BIP94 rules added for testnet4 (A-17) and the corrected testnet4 parameters
(A-21) are therefore verified only against Core's source, never against a running
node.

No version gating is needed: `core_31_bitcoind` already requires `RBTC_BITCOIND`
and `assert_core_31` pins it to version `310000`, which supports testnet4 and
BIP94. The work is adding testnet4 cases to the existing harness.

Cases to cover: a difficulty-period boundary block whose timestamp is exactly
`parent - MAX_TIMEWARP` (accepted) and one second earlier (rejected); and a
retarget where the period's first and last blocks carry different `nBits`, proving
the base comes from the first.

Practical obstacle to solve rather than skip: testnet4's interval is 2016 blocks
and its pow limit is `0x1d00ffff`, so reaching a boundary by real mining is
infeasible in a test. Decide how to drive Core to a boundary — `submitheader` with
prepared headers, a `-testactivationheight`-style knob, or asserting rBTC's
`expected_next_bits` and timewarp verdicts against Core's `getblocktemplate` /
`getdifficulty` at a synthesised boundary. If no approach lets Core actually
validate a testnet4 boundary block, report that instead of adding a case that
only exercises rBTC.

---

## Group B — needs a decision before implementation

Do not start these without an explicit answer to the question stated. Each has a
real trade-off that is not mine or Codex's to settle.

### B-1 · `bitcoin` 0.32.8 → 0.32.102 (Dependabot branch open)

Fully analysed in [docs/AUDIT.md](AUDIT.md) under "Open dependency updates".
Every behaviour the audit relies on is byte-identical, and `bdk_wallet 3.1.0`
accepts it. But it adds decode-time range validation for `feefilter`, which turns
an out-of-range value from *silently ignored* into a decode error that
`P2pError::is_protocol_violation` classifies as misbehaviour. **Core does not ban
for this** — `net_processing.cpp` applies the value only `if (MoneyRange(...))`
and otherwise ignores it. Merging unchanged makes rBTC discourage peers Core
tolerates, and breaks
`p2p::tests::bip133_filter_tracks_valid_updates_and_skips_low_fee_inventory`.

**Question:** match Core (tolerate the out-of-range value without banning), or
accept the stricter behaviour?

If matching Core: the new error is a plain `ParseFailed` string, so tolerating it
without also tolerating genuinely malformed payloads — which must stay violations
— requires either matching that message or restructuring the classification.
Prefer restructuring; string matching on a dependency's error text is fragile.

### B-2 · `rand` 0.9 → 0.10 and `sha2` 0.10 → 0.11 (Dependabot branches open)

Breaking major bumps requiring source changes. Not evaluated by the audit.
`sha2` is on the consensus path (UTXO set digests, snapshot and archive record
digests, validation-delta blooms), so its bump needs digest-stability evidence:
prove that persisted digests are unchanged, or treat it as a storage format
migration.

**Question:** adopt now, or defer? If adopting `sha2`, which of the two?

### B-3 · Windows lock-owner marker under contention (A-26 residual)

`DataDirectoryLock` writes a `pid=`/`network=` marker into the lock file so a
second process can report who holds it. Unix advisory locks let the contending
handle read it; Windows locks are mandatory, so the read fails and the message
degrades to the contention notice without owner detail. Currently documented, with
the affected assertions Unix-gated.

Fixing it means locking a byte range *outside* the marker — a lock file format
change, which affects cross-version compatibility of the lock.

**Question:** is operator-visible lock ownership on Windows worth a lock-format
change?

### B-4 · `libbitcoinkernel` migration

`libbitcoinconsensus` was removed in Core v28; rBTC pins the last release line
that ships it. No consensus rule has changed since, so the pin costs nothing
today, but Core will not maintain that ABI. Scope and rationale are in
[docs/CORE31_COMPATIBILITY.md](CORE31_COMPATIBILITY.md).

**Question:** start scoping now, or revisit when a post-Taproot script rule
actually appears? This is a project, not a task — it needs its own
differential-verification plan before any code moves.

---

## Group A addition — found while closing out

### A-32 · `active_node_and_genesis_validator_run_concurrently_and_finalize` is load-sensitive

**Severity: Low (intermittent CI failure).**
**File: `src/node.rs`, the 15-second `timeout(..)` around the concurrent `run(..)`.**

The test fails with `Elapsed(())` and the message "concurrent validation must not
wait for sequential peer startup" when the full suite runs, and passes 3/3 when
run alone. Confirmed pre-existing, not caused by the Group B changes: the same
test fails on `47c7034` with none of them applied, and passes in isolation there
too. It is a wall-clock budget competing with the rest of the suite for CPU, not
a logic fault.

This matters more now than before, because the `windows` CI job added for the
earlier findings runs the full suite on a shared runner that is slower than a
developer machine. A 15-second budget that already loses on a local full-suite
run will make that job intermittently red, which trains people to ignore it --
the opposite of why it was added.

Required: make the assertion independent of wall-clock scheduling, or give it a
budget that a loaded shared runner can meet. Prefer the former: the property
under test is that concurrent validation does not *serialize behind* peer
startup, which can be asserted from observed ordering rather than from elapsed
time. Do not simply raise the constant without saying why the new value is
defensible.

**Resolved:** the server sends a one-shot event only after accepting both
connections and before serving either handshake. The test asserts that event
first, then independently waits for node completion. The 60-second watchdog is
only a deadlock/liveness ceiling for loaded shared runners.

## Group C — accepted limitations, do not "fix"

Listed so they are not re-reported as defects. Change these only if the stated
premise changes.

- **A-12 · Unix-only filesystem hardening.** `0600` database modes, directory
  `fsync`, and the audit log's device/inode identity check have no portable
  equivalent in the Rust standard library. Documented in `README.md` under
  "Supported platforms". Adding a Windows ACL dependency to the consensus crate
  is not obviously worth it; raise it as a proposal if you disagree.
- **Header replay refuses a backwards clock.** `HeaderStore::load_dag` re-applies
  the future-timestamp check, so a system clock that moved back more than two
  hours blocks loading the node's own journal. Fail-closed and arguably correct;
  the operational edge is recorded.
- **Header replay walk-back cost.** On a min-difficulty chain, replay's
  `expected_next_bits` walk-back is `O(headers × interval)` worst case. Matters
  only for a long testnet3 journal.

---

## Group D — audited less deeply than the rest

Not findings. These areas were audited at their security boundaries but not read
line-by-line, so a defect here would not have been caught. Treat as candidates for
a future pass rather than as work items.

- `src/node.rs` API wiring and CLI argument surface. Token handling, loopback
  enforcement, and consensus call sites *were* audited; the rest was not.
- `src/undo_store.rs` beyond its encoder bounds and write-ahead protocol.
- The modules added by the 79-commit integration were audited only for the three
  regression classes that job found (A-24, A-25, A-26): `diagnostics`,
  `snapshot_download`, `auxiliary_index`, `index_policy`, and `node::config_file`
  have had no systematic pass.

---

## Reporting back

For each task attempted, state: the finding addressed, the Core citation where a
consensus rule is involved, what changed, what test now fails if the change is
reverted, and the gate results on both platforms. If a task turns out to rest on
a wrong premise — as A-11 and A-23 did in this audit — say so plainly and stop
rather than implementing around it.
