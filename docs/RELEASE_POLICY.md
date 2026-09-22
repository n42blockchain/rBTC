# Release policy

Status date: 2026-09-22. This policy supersedes open-ended release-gate
interpretations in older dated audits. Historical measurements remain evidence
for their recorded revisions, not new requirements.

## Release claims

The production claim is the default-redb validating node on the supported
Linux, macOS and Windows matrix. Consensus and chainstate correctness, atomic
recovery, bounded handling of untrusted input, exact-source CI, artifact
identity and native trust are hard requirements for that claim.

An explicitly named Linux preview may be built for evaluation before native
Apple/Windows credentials exist, but it is not a production release, must not
use a stable release tag, and does not waive any production gate. Experimental
MDBX replacement, full RPC parity, hot-key wallet support and performance-only
optimizations are outside the default-redb production claim.

## Required gates

`release/acceptance/readiness.json` format 2 has exactly four gates:

1. `admission-resources` covers admission correctness, finite optimizer
   differential checks, bounded resource behavior and recovery/fault behavior.
   Optimizer work is not a separate, indefinitely extensible checkbox.
2. `header-resources` covers semantic equivalence, retained-header resource
   plateau, bounded re-fetch/promotion, restart and fault recovery.
3. `storage-replay` proves selected real-block content and fresh-process
   recovery for the supported default storage path. MDBX evidence may be
   retained for comparison but cannot expand this gate into MDBX selection.
4. `public-soak` remains the first-production-release policy: the frozen binary
   must complete 604,800 seconds on Bitcoin and Testnet4 with the documented
   restart, fault, sampling, peer and persistence checks.

The resource gates have one canonical command:

```bash
RBTC_CORE_LINEARIZE=/absolute/path/to/pinned-core31-linearize \
RBTC_CORE_POSTLINEARIZE=/absolute/path/to/pinned-core31-postlinearize \
scripts/run-default-redb-resource-acceptance.sh /new/evidence-directory
```

It must run on Linux with `/proc` RSS accounting. It executes the finite
admission, optimizer, header semantic/network/recovery tests; three fresh
256-entry/eight-clone admission probes; and a one-hour retained-header workload
that generates at least one million siblings. The fixed component allowances
are 512 MiB peak RSS for both probes, 64 MiB admission clone RSS delta, 256 MiB
header database allocation and at most 10% median plateau growth. Timing bounds
are deliberately broad regression ceilings, not performance promises.

The command emits exactly one canonical report for each resource gate. Review
the raw logs and hashes before copying those reports to `release/acceptance/`.
The readiness verifier rejects a report unless its commit matches the frozen
source and every required component is `PASS`. Passing a unit test or probe in
isolation cannot close a gate. A new release requirement needs an owner,
rationale, bounded procedure, pass/fail rule and disposition of prior evidence;
open-ended implementation inventories are backlog, not release gates.

These component gates deliberately do not make an unmeasured whole-process
claim. End-to-end daemon RSS, disk growth, peer diversity, tip consistency and
restart behavior over time belong to the frozen-binary public soak, which
already records them. This avoids requiring two differently specified
“whole-node” gates while preserving both bounded adversarial tests and real
network operation.

## Frozen-source identity

The source digest recursively binds all tracked release-relevant files,
including production source, dependencies, lockfiles, build configuration,
tests, examples, fuzz targets, CI/release workflows and acceptance tooling.
It intentionally excludes `docs/`, `README.md` and `release/acceptance/` so
reviewed documentation and evidence-only commits may follow a frozen candidate.
No excluded file is embedded into the binary. Any release-relevant change
requires impact review and repetition of affected acceptance; an accepted report
may never be relabeled for a different source digest.

The signed workflow requires successful ordinary push CI for the exact release
SHA on `main` or a `release/*` branch. The exact SHA is the technical invariant;
the branch name is repository workflow policy. A stable tag still requires all
four accepted gates and the complete signed native matrix.

## Stop rule

After the candidate is frozen, do not add optional optimizations. Fix only a
reproduced violation of a required gate, restart affected evidence, and record
the impact. Missing organization signing credentials are an external blocker,
not justification for more production-code work.
