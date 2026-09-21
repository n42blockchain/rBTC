# Consolidated default-redb acceptance baseline — 2026-09-20

Status: diagnostic baseline only. No release gate is closed by this report.

## Source and scope

- Source: `b57a4b9dbbe1a421003c89d4cfbc2a46fe9c1dbd` (the requested current
  checkout). The worktree had untracked acceptance notes and local session artifacts
  at capture time; no source,
  gate, or evidence identity was changed by this run.
- Product scope: default redb validating node. MDBX, public soak, native
  signing, and performance-only work were not run.
- Run artifacts, command outputs, exit statuses, and JSONL probe evidence are
  under [`session-state/2026-09-20/consolidated-acceptance/`](../../session-state/2026-09-20/consolidated-acceptance/).

## Executed baseline

| Criterion / scenario | Command | Result | Evidence and interpretation |
| --- | --- | --- | --- |
| Admission resource ownership, deferral, queue preservation, and related local recovery regressions | `cargo test --locked --lib admission_resources`; `header_recovery`; `header_replay`; `memory_plan`; `resource_deferral`; `header_store` (each with `-- --nocapture`) | All six exit 0 | Focused unit/in-process transport coverage passed. It does not establish whole-node RSS, disk, peer diversity, or sustained scheduling acceptance. |
| Default redb library/integration regression baseline | `cargo test --locked --lib --tests --no-fail-fast` | Exit 0; 1,026 library tests passed, 10 ignored; integration targets passed as logged | `default-suite.log`; ignored tests include resource/scale workloads and are not acceptance evidence. |
| Admission workload and clone/reconcile behavior | `cargo run --locked --release --example admission_resource_probe -- 256 8` | Exit 0 | Synthetic regtest workload: 256 entries, 50 cluster queries, 8 simultaneous clones, reconcile. Seed 16,406,654 µs; admission 92,161 µs; retained bytes 14,429,043. RSS fields were `null` on macOS because the probe reads Linux `/proc/self/status`; no RSS gate result is claimed. |
| Header retained storage, bounded eviction, same-process reopen, fresh-process reopen | `cargo run --locked --release --example header_resource_probe -- retained 2500 50000` | Exit 0 | 2,500 active headers + 50,000 side headers; final file length 67,907,584 bytes; final sampled RSS 75,408 KiB; fresh-process reopen retained the same tip/count. This is a short kernel/store probe, not whole-node peer/fault acceptance. |
| Header evidence checker against the actual probe | `python3 scripts/check-header-resource-probe.py session-state/2026-09-20/consolidated-acceptance/header-probe-retained-2500-50000.jsonl --minimum-seconds 1 --max-rss-mib 256 --max-disk-mib 128` | Exit 1 (reproduced evidence insufficiency) | Checker errors: fewer than one million generated siblings; insufficient plateau samples. The checker also declares `headers_production_gate_closed: false`. The minimum was lowered only to diagnose the short run; the normal checker default is 3,600 seconds. |
| Header checker rejection tests and readiness self-tests | `python3 scripts/test-header-resource-probe.py`; `python3 scripts/test-release-readiness.py` | Both exit 0 | Checker behavior and current readiness assertions are healthy; readiness remains open for admission-resources, header-resources, and public-soak. |

## Current blockers and gaps

The baseline reproduced no implementation failure in the focused/default test
suites. It did reproduce an acceptance failure: the available header probe is
insufficient for the declared header criterion. It covers only 50,000 siblings
and about 1.16 seconds, while the checker requires at least 1,000,000 generated
siblings, sustained sampling, and by default 3,600 seconds. The short probe's
RSS/disk samples are useful diagnostics but cannot establish a continuous
high-water mark or a whole-node bound.

Admission-resources remains unaccepted. The synthetic probe passed its local
invariants, but macOS RSS is unavailable in its implementation, and the run
does not include concurrent peers, rejected/accepted package pressure, full
node execution, disk pressure, restart, or process-fault injection.

Header-resources remains unaccepted. The bounded probe showed successful
reopen and a 50,000-side-entry result, but did not run the required unbounded
reference semantic comparison, 1M-sibling workload, sustained 3,600-second
sampling, concurrent peer/local ingress, cancellation or failed-append fault,
evicted-fork promotion, interrupted compaction, or whole-node RSS/disk test.

Restart evidence here is store/probe reopen and existing in-process tests. A
real daemon process kill/restart under concurrent resource pressure and fault
recovery was not run. Therefore this report distinguishes “reproduced
insufficient evidence” from “untested acceptance”; it does not convert either
into a gate failure caused by source behavior.

## Next acceptance work

After source freeze, run the header probe/checker with the prescribed sustained
minimum (including the 1M-sibling workload and 3,600-second default), plus the
small unbounded-reference semantic comparison. Run admission and header tests
with concurrent peers, execution, restart/process-kill, cancellation,
failed-append, and disk/RSS measurements. Bind those results to this exact
source before changing readiness or starting the seven-day public soak.

## Parent review

The full default-suite log and exit status were reviewed. Integration test
summaries also show zero failures; ignored external-Core and scale tests remain
unexecuted, not accepted. No tracked source changes were present after testing.

The parent reran the header checker against the same samples with its default
3,600-second minimum (RSS allowance 256 MiB, disk allowance 128 MiB, diagnostic
allowances rather than new release policy). Exit 1 additionally confirms the
missing duration; see `header-probe-check-default.json`, `.exit`, and `.command`
in the artifact directory. No threshold in the repository was changed.

The consolidated release decision and external prerequisites are in
[release-blockers-20260920.md](release-blockers-20260920.md).
