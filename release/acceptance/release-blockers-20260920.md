# Release blockers — 2026-09-20

This is a read-only acceptance audit for the scope in
`release-focus-20260920.md`. It does not change readiness, rebind evidence,
start a soak, build/sign artifacts, publish, or run local Cargo tests.

## Decision

Release acceptance is blocked. The committed readiness manifest has three open
required gates, its accepted evidence is bound to an older source, the release
preflight's exact-main-CI condition is not met, and no native signed artifact
or accepted public-soak report is available.

## Observed blockers

### 1. Admission resource gate is open

- Criterion: `readiness.json` must mark `admission-resources` accepted with
  committed evidence; the verifier requires every production gate to be
  accepted.
- Evidence/reproduction: `python3 scripts/verify-release-readiness.py` exits
  1 and reports: “admission-resources: Shared budgets exist; prevout
  preallocation, escaped allocations and resumable scheduling are not
  accepted.” The manifest has status `open` and no evidence.
- Required action: complete the current default-redb admission resource and
  recovery acceptance scenario, review its current-source evidence, then
  update the manifest only as part of the normal release evidence process.

### 2. Header resource gate is open

- Criterion: sustained whole-node header CPU, memory, disk, restart/fault and
  recovery acceptance must be evidenced for the frozen release source.
- Evidence/reproduction: the same verifier run reports: “header-resources:
  Idle retention, atomic durable fork cursors and emergency staging/replay
  entry ceilings exist; calibrated byte/work budgets, disk-backed oversized-
  fork recovery and sustained whole-node acceptance remain open.” The manifest
  has status `open` and no evidence.
- Required action: execute and review the current default-redb header
  resource/recovery acceptance scenario, including the outstanding sustained
  whole-node conditions, then bind accepted current-source evidence.

### 3. Public soak gate is open

- Criterion: one committed canonical report must pass verifier checks for both
  Bitcoin and Testnet4, the frozen commit, controlled restarts, fault
  exercises, and at least 604800 real seconds.
- Evidence/reproduction: the verifier reports “public-soak: No accepted
  604800-second Bitcoin and Testnet4 report exists for frozen release
  source.” `release/acceptance/readiness.json` has status `open` and no
  evidence; the local bounded artifact/soak inventory contains no soak report
  or release binary.
- Required action: after the final source is frozen and both networks catch
  up, run the unshortened seven-day window with the prescribed exercises and
  commit one verifier-valid canonical report.

### 4. Readiness evidence is stale for the current source

- Criterion: the verifier requires accepted evidence and `source_sha256` to
  describe the release source tree; accepted evidence cannot silently carry
  forward across source changes.
- Evidence/reproduction: HEAD is `b57a4b9dbbe1a421003c89d4cfbc2a46fe9c1dbd`,
  while the manifest tested commit is `3cf44ae68003ceb78e9f0c77042667b475091716`.
  The manifest source digest is
  `41531cb896eba13a13fabde1170e519ab3b90c4b0940232c45125120bfdc0431`, which
  matches the old tested commit; the current HEAD digest is
  `1f66c03b380db10b99c8f5c5a76e83cc83924c3886ae9730dab6edf06280a31e`.
  The verifier currently stops at the three open gates, but its source
  identity check would reject this mismatch once those gates are closed.
- Required action: freeze the release candidate, rerun/review affected
  acceptance evidence, and bind it to the final source identity. Do not relabel
  historical reports as current.

### 5. Release workflow exact-main CI prerequisite is unmet

- Criterion: `.github/workflows/release.yml` requires a successful ordinary
  `push` CI run for the exact release SHA with `head_branch == main`.
- Evidence/reproduction: GitHub run `35495893809` succeeded for exact HEAD
  `b57a4b9…`, but its branch is `release/readiness-20260914`; the exact-head
  API query returned no run on `main`, so the workflow's `eligible` set is
  empty. Full remote CI success is useful branch validation but does not meet
  this release preflight condition.
- Required action: after source/evidence readiness is actually complete, land
  the frozen release commit on the required main path and obtain successful
  exact-commit main push CI.

### 6. Native signing and artifact validation prerequisites are unverified

- Criterion: the release workflow requires all 11 release-signing inputs, then
  builds and verifies Linux, macOS x86_64/arm64, Windows, canonical manifest,
  provenance, and fresh-runner install/trust checks.
- Evidence/reproduction: `gh secret list` and `gh secret list --env
  release-signing` completed successfully with zero listed entries, but this
  read-only listing does not establish whether credentials are absent or
  hidden by repository/environment permissions. `gh run list --workflow
  release.yml` returned `[]`. No local native release binaries or signed
  artifact outputs were found in the bounded audit inventory. Secret values
  were not read or recorded.
- Required action: verify that the organization-approved signing credentials
  are available to `release-signing`, then run the release workflow's native
  build/sign/verify matrix on the final exact source and retain its
  artifact/provenance checks. Credential availability is currently unknown.

## Historical claims and unknowns

The older `release-gate-audit-20260918.md` and
`release-gate-audit-20260919.md` reports correctly describe prior audits and
remain historical context. Their optimizer and storage results are accepted
only for the old frozen source; they do not close the three open gates or
prove current-source readiness. The focus document explicitly defers MDBX
replacement criteria, full RPC parity, hot-wallet work, and performance-only
optimizations from this default-redb release; none is recorded here as a
blocker.

This audit did not independently measure network catch-up, whole-node resource
behavior, signing credentials, or native trust. Those are unknown until the
required acceptance runs and external signing setup occur. No claim is made
about failures outside the observed verifier, GitHub metadata, workflow
requirements, and local evidence inventory.

Audit logs are under
`session-state/2026-09-20/consolidated-acceptance/audit/`.
