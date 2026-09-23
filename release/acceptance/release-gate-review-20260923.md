# Release gate review — 2026-09-23

## Decision

The target is the project's advertised default-redb Bitcoin validating node,
including Bitcoin mainnet and Testnet4. A release must not accept an invalid
chain, lose committed chainstate after a crash, misclassify local resource
pressure as peer misbehavior, or become unbounded under hostile peer input.
Those are product safety requirements. A protocol rule does not prescribe this
repository's test duration, report format, branch name, or artifact workflow.

The recent Testnet4 catch-up reproduced a real release blocker: a fresh staged
batch could hit the configured memory allowance and terminate instead of
retrying at a smaller batch. Commit `f808a880e6d0536958f12b0f06b0fea153a62249`
fixes that case and reports the denied reservation size. Its focused regression
and the existing two-fork integration regression pass; the release candidate
still needs current-source resource evidence, live catch-up, CI completion, and
the remaining distribution checks.

## Keep as release blockers

- Consensus and chain selection: validate blocks and scripts, choose the
  greatest-work chain, handle competing forks and reorgs, and compare critical
  behavior against Bitcoin Core-derived references.
- Durable state safety: atomic commits, crash recovery, snapshot identity, and
  independent database audit for every advertised storage/bootstrap path.
- Bounded hostile-input behavior: memory, disk, and CPU work have enforceable
  limits; exhaustion leaves committed state valid and recovery possible.
- Network claims: the released binary catches up and remains synchronized on
  each network listed as supported. Testnet4 is included in this product's
  current claims.
- Distribution trust: each advertised native binary passes its platform build
  and install checks, and published binaries have the declared signature,
  manifest, and provenance. Missing credentials are an external blocker, not a
  code-quality defect.

## Treat as project release policy, not Bitcoin protocol law

- The 604800-second soak and one-hour header probe are conservative project
  confidence targets. Keep the seven-day window for this first production
  release under the currently published policy, but do not require a new
  seven-day window for every unrelated patch. Start it only after both networks
  have caught up on the frozen binary. The one-hour header probe is one-time
  resource calibration; later runs should be impact-based.
- Exact-source evidence is useful when a change touches the tested behavior.
  A report must retain the commit and digest it actually tested. An explicit,
  reviewed impact analysis can reuse it for an unaffected gate; silently
  relabeling an old report is never acceptable. The current readiness verifier
  enforces exact-source reports for admission and header gates, so any impact
  review must be made machine-checkable before it can replace that enforcement.
- CI branch eligibility, report layout, and fixed synthetic workload sizes are
  repository workflow choices. Keep them only where they improve reproducible
  safety evidence; do not let them trigger unrelated implementation work.

## Acceptable deferrals for this release

- MDBX replacement, its 160M/900000 compaction target, and its RSS ratio remain
  experimental and outside the supported default-redb release.
- Full Bitcoin Core RPC parity, mining, and an internal hot-key wallet are not
  part of the published product scope.
- Individual allocation leases and unrelated micro-optimizations are not
  separate release gates. The observable whole-node bounds and safe recovery
  behavior remain mandatory.

## Current disposition

This review does not waive the current seven-day first-release soak or lower a
measured resource ceiling. It corrects the classification: protocol and data
safety are mandatory product behavior; elapsed-time floors, exact report
binding, CI branch rules, and artifact workflow are project policy. The next
release work should close demonstrated failures and produce evidence for the
frozen candidate, rather than reopen implementation tasks that do not map to a
failed product requirement.
