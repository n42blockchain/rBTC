# Release readiness, 2026-09-14

rBTC remains pre-release. Ordinary CI does not close the production resource
and public-network acceptance gates. This current inventory supersedes old
release-facing summaries without replacing dated measurements or failed evidence.

| Gate | Current evidence and remaining work |
| --- | --- |
| Optimizer budget | 4,096 Core comparisons passed, including 2,048 permuted DAGs. Broader adversarial and exhausted-budget acceptance remains open; budget units are not claimed identical to Core. |
| Admission resources | Shared work/candidate leases, atomic deferral and a pinned durable pool snapshot exist. Prevout preallocation, escaping allocations, concurrency and resumable scheduling remain open. |
| Competing headers | [Single-slot disk candidates and bounded atomic promotion](../release/acceptance/header-node-candidates-20260916.md) now run in primary header sync; primary/standby/local validation share a work pool. Huge-winner activation, shared byte/disk budgets, total startup memory and long-duration whole-node acceptance remain open. |
| Selected historical replay | At `b078798`, both engines executed 28,350 blocks (935001–963350), reopened and produced canonical digest `aadd289f6edf154e55aec63c9b4c22cd46e2d7836dc55382d0036523247f2819`. This selected window used file-page-cold inputs on a shared host; it is not genesis or isolated device performance. Preserve this evidence and freeze the final source before final acceptance. |
| Public soak | No accepted 604,800-second Bitcoin/Testnet4 report exists for final source. Freeze production changes and catch up both networks before starting; exercise restarts/faults after day one. Historical calendar age is not acceptance. |
| Experimental MDBX replacement | Full no-maintenance churn passed at a 1.1227 RSS ratio; reduced maintenance still failed at 3.387 after allocation improvements. Bound transitions, undo, folded indexes and dirty pages together. MDBX remains excluded from supported default-feature release binaries; its separate replacement gate is not accepted. |
| Native signing | No matching Developer ID Application identity on the Mac. The `release-signing` environment was absent (admin API returned 404). Organization-owned Apple/notary and Windows credentials and the protected environment need provisioning. |
| Release immutability | Enabled on 2026-09-14; read-back returned `enabled: true`, `enforced_by_owner: false`. This does not prove a signed release exists. |

## Enforced release preflight

`scripts/verify-release-readiness.py` reads committed
`release/acceptance/readiness.json`. Each production gate must be accepted and
reference a nonempty committed report with its SHA-256. Current entries remain
open. Missing, edited, untracked, oversized, path-escaping or symlinked reports
fail closed. Duplicate JSON keys are rejected.

The manifest binds a full frozen `tested_commit` and a SHA-256 over Git tree
records. Every tracked file and mode is included except `release/acceptance/`.
Reports may therefore be committed after testing without a circular commit
reference. Production, test, dependency, workflow, script or documentation
changes invalidate the identity. Keep executable code outside the evidence
directory. The tested commit must be an ancestor of the release commit.

For public soak, preserve the unedited final report produced by
`scripts/public-network-soak-report.sh SOAK_DIR` with its default seven-day
minimum. Preflight checks its tested commit, binary digest, three PASS fields,
the finalizer's required minimum (at least 604,800 seconds), exact UTC duration,
non-future end, both network rows and completed restart/fault
counts. A short fixture or INCOMPLETE report cannot close the gate. These are
reviewed acceptance records, not independent cryptographic proof of a workload:
reviewers must inspect the immutable baseline, raw metrics and fault records
described in [PUBLIC_NETWORK_SOAK.md](PUBLIC_NETWORK_SOAK.md).

After freezing and completing acceptance:

```sh
git rev-parse HEAD
python3 scripts/verify-release-readiness.py --print-source-digest
# Record those identities, reviewed reports and their hashes under
# release/acceptance/, then commit only that directory.
python3 scripts/verify-release-readiness.py
```

The release workflow checks evidence before requesting signing secrets. It also
requires the latest main CI run for the exact release commit to have succeeded.
Complete main CI before tagging, or rerun release after it passes. Manual signed
matrix rehearsals enforce the same acceptance checks and never publish.

## Packaging corrections and verification

The manifest derives root schema **4** from the daemon's single Rust declaration.
Generation and verification fail if the declaration is absent or ambiguous. The
v2 format and ten-asset matrix are unchanged. Exact nonempty fields are required,
including rejection of extra tabs and carriage returns.

Manifest tests cover current/future schema, mismatched source schema,
absent/ambiguous declarations, tampering, tag mismatch, malformed fields,
traversal, wrong trust and symlinks. Ten readiness tests cover accepted synthetic
evidence, evidence-only commits, changed/dirty source, missing/open gates,
tampered reports and invalid soak claims. Fixtures are never release evidence.

Binary smoke now also executes `--check-config` without creating a data
directory. After assembly and attestation, five fresh native runner jobs
download assets, verify the manifest, signer workflow, source commit/ref, execute the binary
and recheck macOS/Windows trust before publication. They do not build or import
signing keys. An actual signed run remains due; hosted-runner smoke is not
public deployment, upgrade, backup/restore or seven-day acceptance.

Immutability was enabled using the documented
[GitHub repository API](https://docs.github.com/en/rest/repos/repos#enable-immutable-releases).
Recheck it before tagging. Draft-upload-publish allows complete asset assembly
before publication makes assets immutable.

## Native validation follow-up

The main CI run `34886107084` at `3cf1ea8` passed Linux test/coverage and
supply-chain jobs, but Windows timed out in
`host_observes_typed_peer_header_execution_and_freezer_state` after its
three-second startup guard. The earlier branch run at the same commit passed.
The Windows log does not identify which startup operation consumed the time.

On Mac, a temporary four-second delay before the fixture accepted its peer
reproduced that exact timeout at 3.02 seconds. The same delayed scenario passed
at 5.03 seconds with the existing twenty-second startup guard used elsewhere in
this test file. Embedded startup waits now share that guard; protocol-frame and
shutdown deadlines and all state/event assertions remain. Timeout diagnostics
include the node status/lifecycle and whether the fixture peer exited.
The injected delay was removed. The seven-test file then passed ten consecutive
runs (70 tests); default and all-feature strict Clippy also passed.
The final Mac all-feature suite passed: **976 passed, 0 failed, 34 ignored**.
Ignored external/scale gates retain their existing opt-in requirements.

The schema-source reader accepts both LF and Windows CRLF Rust checkouts;
generated manifests still require canonical LF records. Both checkout styles
are covered by the future-schema fixture. Same-ref release runs are serialized
so signing and publication jobs cannot interleave uploads to one draft.
