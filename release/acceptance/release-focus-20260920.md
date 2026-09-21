# Release scope and closure plan — 2026-09-20

The user's latest direction is to establish the basis, necessity and remaining
work for release conditions, then concentrate on finishing instead of adding
small optimizations. This is the current working priority; the older all-gates
engineering inventory remains historical context, not an instruction to expand
this release into every experimental project. No acceptance status or automated
threshold is changed by this assessment.

## Scope

Target the existing default redb validating-node distribution described by
`docs/PRODUCT_MATURITY.md`, keeping the current supported-platform matrix.
Experimental MDBX replacement, its 160M/900000 churn and 1.5 RSS-ratio criteria,
full RPC parity, hot-wallet work and performance-only optimizations are outside
this release's critical path. The MDBX document explicitly scopes its criteria
to selecting MDBX; `docs/RELEASE_READINESS_2026-09-14.md` explicitly excludes it
from supported default-feature release binaries. Any separate user-assigned
MDBX work is deferred, not declared accepted or deleted.

## Basis and necessity

| Condition | Authority in this repository | Disposition |
| --- | --- | --- |
| Correct chainstate/consensus, atomic recovery, no invalid-peer penalty for local resource failure | ROADMAP P0; header/admission gate invariants | Keep; these prevent incorrect state and loss of synchronization. |
| Sustained admission/header CPU, memory and disk acceptance | UPSTREAM_ADMISSION_RESOURCE_GATE and UPSTREAM_HEADER_RESOURCE_GATE; readiness manifest | Keep as concrete workload and recovery tests. Do not confuse every possible allocation optimization with a separately mandatory feature. |
| 604800-second Bitcoin/Testnet4 soak | ROADMAP; PUBLIC_NETWORK_SOAK; verifier verify_soak | Project release policy, not a consensus rule. Keep under current policy; it is a seven-day elapsed-time floor after both networks catch up, not seven days of coding. |
| Final-source evidence identity | verify-release-readiness.py and release workflow | Keep. Assess change impact and rerun affected tests; do not automatically turn every patch into a new database-replacement or all-history research project. Historical reports alone cannot be relabeled as current. |
| Native macOS/Windows signing plus native artifact validation | RELEASE_SIGNING and release.yml | Keep for the currently declared platform matrix. Credentials are external. A Linux-only or preview distribution would be a separate explicit product/workflow decision, not a bypass. |
| 160M/900000, MDBX compaction and 256/64 RSS <=1.5 | MDBX_REPLACEMENT_GATE, criteria before selecting MDBX | Defer from default redb release; not a generic Bitcoin node requirement. |
| Every allocation individually leased; transparent live retries for every failure; isolated micro-optimizations | Implementation approaches and broader engineering backlog | No standalone new release checkbox. Required observable resource/recovery behavior still must pass. Do not weaken explicit existing criteria without reviewed disposition. |

## Bounded execution plan

1. Spend at most 1–2 engineering days mapping CURRENT default-redb code to the
   existing admission/header acceptance scenarios and executing a combined
   resource/restart/fault baseline. Produce one failure list with reproduction,
   severity and the specific release criterion violated. Old open prose is not
   sufficient evidence of a current missing implementation.
2. Fix only reproducible release-blocking failures; provision signing credentials
   in parallel. Allocate an initial 2–5 engineering days, not an unlimited series
   of allocation cleanups. A significant architectural failure triggers a new
   estimate immediately; this allocation is not a promise that unknown defects
   fit it. Operator-visible safe recovery versus transparent retry must have an
   explicit acceptance disposition where the existing contract is ambiguous.
3. Freeze one release candidate. Rerun affected optimizer, historical-content,
   recovery and native CI checks; bind reviewed current-source evidence. Reserve
   1–2 engineering days plus measured data preparation/replay/catch-up time.
   Existing full bootstrap/history evidence informs the impact review; no blanket
   claim that genesis-to-tip must be repeated after every non-consensus patch.
4. After both networks catch up, run the unshortened seven-day window on the
   frozen binary, with the prescribed restarts/fault exercises. Rehearse artifacts
   when preflight and credentials allow, then complete final main CI and publish
   only after all current gates pass. During the formal window, no optional code
   or out-of-evidence documentation changes.

Planning envelope: roughly 4–9 engineering days and a mandatory seven-day live
window, with platform credentials and catch-up as external dependencies. About
2–3 calendar weeks is a CONDITIONAL target if the baseline finds only bounded
fixes and credentials/data are ready. It is not a measured ETA or an upper bound;
large resource/recovery defects, slow catch-up or missing certificates extend it.
The first 1–2-day baseline is the checkpoint for replacing this estimate with
measured remaining work. Seven days alone is not an honest completion estimate.

## Immediate actions and stop rule

- Serialization admission commit `b57a4b9` is pushed and remote verified;
  its CI35495893809 is running. Prior c158c6b CI35480161201 passed.
- The uncommitted Merkle optimization was removed from the worktree and saved
  with its failed compile log in the original workspace at
  `session-state/2026-09-20/release-focus/`. It is not validated release code.
- Next work is the consolidated default-redb acceptance baseline and failure
  triage, not more Merkle/allocator/MDBX optimization. A new change must name the
  reproduced acceptance failure it fixes or a demonstrably missing prerequisite.
- Three manifest gates remain open. No source/evidence rebinding, gate waiver,
  tag, release publication or public-soak start was performed.

## Consolidated acceptance checkpoint — 2026-09-20

Two scoped economy-model subagents executed the default-redb baseline and
release prerequisite audit; the parent reviewed their logs, exit statuses,
source identity, and conclusions and consolidated the results here.
Source: `b57a4b9dbbe1a421003c89d4cfbc2a46fe9c1dbd`.

**Decision: not ready for release; no implementation failure reproduced in
this bounded baseline.** This checkpoint is not full sustained acceptance.

- [Baseline and coverage](consolidated-baseline-20260920.md): six focused
  resource/recovery groups passed; the default library/integration suite exited
  0 (1,026 library tests passed, 10 library tests ignored; integration targets
  also passed). Admission and short header probes completed. macOS admission
  RSS was unavailable; the 50,000-sibling header sample fails the unchanged
  checker requirements for workload, plateau samples, and default duration.
- [Release blocker list](release-blockers-20260920.md): admission/header
  sustained whole-node and process-fault evidence remains incomplete; the
  seven-day two-network soak remains open; accepted manifest evidence binds
  older source; exact-source main push CI is missing; signed native artifacts
  and their verification are missing, with signing credential readiness
  unverified. Repository/environment secret listings alone do not establish
  whether inherited organization credentials exist.
- CI `35495893809` has now succeeded for this source on the release branch.
  This updates the earlier running status, but does not satisfy the release
  workflow's main-branch CI prerequisite.

### Remaining closure work, in order

1. Complete the consolidated admission/header sustained workload and real
   daemon restart/fault evidence, including the header semantic reference
   comparison and at least the checker's one-hour/one-million-sibling floor.
   Record CPU, peak RSS and disk separately. Existing short probes and unit
   tests do not close these gates. Fix only a reproduced release blocker.
2. Review change impact, freeze the candidate, rerun affected optimizer/history
   and native checks, and bind reviewed evidence to the actual frozen source.
   Resolve signing availability in parallel; do not treat it as a code task.
3. After both networks catch up, complete the unchanged 604800-second window
   with prescribed exercises, then satisfy exact-commit main CI and signed
   artifact validation before release.

No optimization, production-code edit, manifest status change, evidence
rebinding, Git commit, soak start, tag, or publication was performed. Reports
are reviewable local files; raw logs remain under
`session-state/2026-09-20/consolidated-acceptance/`. This checkpoint adds no new
release criterion and does not claim that untested behavior is broken.
