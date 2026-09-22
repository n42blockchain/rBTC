# Product maturity gate

Status date: 2026-09-14.

This is the release-facing maturity view. Detailed implementation history stays
in [ARCHITECTURE.md](ARCHITECTURE.md) and [ROADMAP.md](ROADMAP.md).
Current blockers and evidence preflight are in
[RELEASE_READINESS_2026-09-14.md](RELEASE_READINESS_2026-09-14.md).
The authoritative bounded gate definitions are in
[RELEASE_POLICY.md](RELEASE_POLICY.md).

| Area | Repository status | Release evidence |
| --- | --- | --- |
| Consensus and chainstate | Complete | Core-derived vectors, historical blocks, live Core 31 differential tests, atomic recovery tests |
| Fast bootstrap | Complete | Maximum-work headers, pinned Core AssumeUTXO identity, live catch-up, independent genesis replay |
| Storage lifecycle | Complete | Bounded freezer/undo retention, hot/cold data-backed policy, disk forecasts, audit/reindex/backup procedures |
| Snapshot-backed overlay | Experimental; maintenance resource gate open | Replay/content and recovery tests pass; MDBX maintenance peak RSS still exceeds the replacement target |
| Network contribution | Functional; retention gate open | Interoperability tests pass; safe automatic competing-header retention/reacquisition remains open |
| Admission and policy resources | Acceptance open | Shared budgets and indexed dependencies exist; whole-pipeline resource and adversarial optimizer acceptance remain open |
| Operations | Complete | Strict config, `--check-config`, version identity, structured logs, health/readiness/metrics, authenticated stop, recovery runbook |
| Embedding | Complete technically | Library-owned runtime and `n42-26` executor fixture; combined distribution still requires a GPL-compatible policy decision |
| Security | Complete for release candidate | Four audit passes integrated, dependency/fuzz/dynamic-analysis gates, private reporting policy |
| Cross-platform packaging | Automation implemented; signed run pending | Native suites, downloaded-artifact smoke, native trust, SBOM, provenance and manifest v2 bound to daemon schema 4 |
| Public operations | Acceptance open | No accepted seven-day Bitcoin/Testnet4 report for frozen release source; historical elapsed time does not close this gate |
| Signed release | Externally blocked | Developer ID Application and Windows Authenticode identities plus a real protected tagged run |

## Non-negotiable release invariants

1. `Cargo.toml`, `rbtcd --version`, P2P subversion, RPC version, tag, and release
   manifest version describe the same software version.
2. A tagged workflow refuses a tag that is not exactly `v` plus the package
   version.
3. Every supported native artifact runs the complete all-feature suite and
   successfully executes `--version` and `--help` before signing or publication.
4. The canonical manifest binds version, tag, commit, toolchain, data schema,
   byte length, digest, platform, and native trust type.
5. No release claim may convert an incomplete soak, unavailable credential,
   accepted platform limitation, or deployment-specific P2 feature into a
   repository-complete checkbox.
6. Preflight requires reviewed reports bound to the release-relevant frozen
   source and successful push CI for the exact release commit on `main` or a
   `release/*` branch. Documentation and evidence-only commits may follow the
   tested source without invalidating its identity.

## Deliberate product boundaries

rBTC is an outbound-capable validating node with optional inbound contribution,
pruned storage, bounded operator APIs, and a watch-only external-signer wallet.
An internal hot-key wallet, mining, exact Bitcoin Core RPC parity, and
deployment-specific UI/service packaging are not prerequisites for this
release claim and must not silently widen its security scope.
