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

## What is a release requirement

Do not describe repository release policy as a Bitcoin protocol rule. BIPs
distinguish consensus specifications from other proposal types and state that
an individual BIP does not define Bitcoin or automatically represent community
consensus. The client must follow the consensus rules actually active for each
supported network; it does not need to implement every BIP, Bitcoin Core RPC,
or Bitcoin Core's local mempool policy to be a Bitcoin validating node. See
[BIP 3](https://github.com/bitcoin/bips/blob/master/bip-0003.md).

| Class | What belongs here | Release treatment |
| --- | --- | --- |
| Protocol and data-safety invariants | Correct validation under the active network rules; cumulative-work chain selection and reorganization; no publication of invalid state; durable, atomic state transitions and restart recovery; bounded handling of untrusted network and operator input. | Mandatory for every release claiming a validating node. A defect here blocks release regardless of benchmark or soak status. |
| Project release policy | Supported operating systems and artifact trust; exact-source CI; resource ceilings and adversarial test envelopes; Core 31 differential checks for local transaction ordering; seven-day public soak; signed artifacts, provenance and manifest. | Mandatory for the production claim while this policy is in force. These are project choices, not Bitcoin consensus rules. Each must have a stated risk, owner, finite evidence procedure and explicit disposition. A policy change requires a recorded decision; it must not be disguised as a protocol exception. |
| Deferred or excluded features | Full Bitcoin Core RPC parity, hot-key wallet, mining, experimental MDBX replacement, and deployment-specific packaging. | Do not gate this product claim unless the release begins claiming the feature. Document limitations honestly. |

The fixed RSS, disk, time and load values below are provisional project
acceptance envelopes. They are not universal limits imposed by Bitcoin. Keep
them only where the test models a named threat or supported deployment and the
ceiling leaves adequate operating headroom. If the envelope fails, first
determine whether the run exposed unbounded behavior, an unsuitable test
workload, or an unjustified number; do not automatically add another
optimization task. Passing a synthetic probe is not proof of whole-node safety
or production readiness.

In particular, the one-hour million-sibling workload is a bounded hostile-input
regression test, while 604,800 seconds is the current first-production soak
policy. Neither duration is protocol-mandated. Keep the soak as a production
gate until a release owner explicitly revises that policy based on equivalent
or stronger operational evidence; elapsed time by itself is never evidence of
correctness.

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

The listed counts, ceilings and durations make this round reproducible; they
are project regression parameters, not maximum valid Bitcoin conditions or
protocol conformance limits. An out-of-envelope result is evidence to diagnose,
not an automatic mandate for more optimization work. Before changing a value,
record which safety risk the workload represents and why the replacement gives
equal or better coverage.

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
