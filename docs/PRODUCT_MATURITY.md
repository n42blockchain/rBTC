# Product maturity gate

Status date: 2026-08-08.

This is the release-facing maturity view. Detailed implementation history stays
in [ARCHITECTURE.md](ARCHITECTURE.md) and [ROADMAP.md](ROADMAP.md).

| Area | Repository status | Release evidence |
| --- | --- | --- |
| Consensus and chainstate | Complete | Core-derived vectors, historical blocks, live Core 31 differential tests, atomic recovery tests |
| Fast bootstrap | Complete | Maximum-work headers, pinned Core AssumeUTXO identity, live catch-up, independent genesis replay |
| Storage lifecycle | Complete | Bounded freezer/undo retention, hot/cold data-backed policy, disk forecasts, audit/reindex/backup procedures |
| Snapshot-backed overlay | Experimental and bounded | Txid-group v2 index, MDBX/redb overlay catch-up, bounded undo decoding, compaction/rebase, crash-safe swap recovery |
| Network contribution | Complete | Bounded outbound failover and optional inbound service with resource accounting; real Core 31/btcd interoperability tests include BIP324 v2 transport fallback and bounded ZMQ publishing |
| Operations | Complete | Strict config, `--check-config`, version identity, structured logs, health/readiness/metrics, authenticated stop, recovery runbook |
| Embedding | Complete technically | Library-owned runtime and `n42-26` executor fixture; combined distribution still requires a GPL-compatible policy decision |
| Security | Complete for release candidate | Four audit passes integrated, dependency/fuzz/dynamic-analysis gates, private reporting policy |
| Cross-platform packaging | Automation complete | Full native all-feature tests, binary `--version`/`--help` smoke, native signatures, SBOM, provenance, manifest v2 |
| Public operations | In progress | The minimum seven-day window has elapsed, but no fail-closed accepted final report is versioned in this repository |
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

## Deliberate product boundaries

rBTC is an outbound-capable validating node with optional inbound contribution,
pruned storage, bounded operator APIs, and a watch-only external-signer wallet.
An internal hot-key wallet, mining, exact Bitcoin Core RPC parity, and
deployment-specific UI/service packaging are not prerequisites for this
release claim and must not silently widen its security scope.
