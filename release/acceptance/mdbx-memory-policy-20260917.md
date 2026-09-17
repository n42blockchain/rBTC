# MDBX environment memory ownership (2026-09-17)

Overall memory and maintenance RSS gates: **OPEN**.

Both the standalone MDBX chainstore and snapshot-overlay MDBX backend now use a
single environment constructor with explicit dirty-page policy. All normal,
identity/audit, rebase, compact validation, rollback and final-reopen paths use
it. Defaults are 4,096 transaction dirty pages and 64 reusable pages. Previously
MDBX selected the dirty-page threshold from host total/available RAM (documented
in the bundled dependency's mdbx.h); it could therefore ignore the node limit.

Each environment reserves 260 MiB in a registered node ledger before environment
allocation or directory creation: (4096 + 64) times the largest supported 64 KiB
page size. Existing database page sizes remain unchanged. On a 16 KiB page
host, the configured page counts correspond to 65 MiB of nominal page storage;
reservation is deliberately conservative across existing database geometries.
Unregistered library environments still get the fixed spill policy but have no
node ledger. Durability, geometry ceiling, batch size and compaction acceptance
thresholds are unchanged.

The limit is an MDBX spill threshold, **not a complete hard allocation or RSS
ceiling**. Mapped resident pages, overflow values, engine metadata, copy buffers,
application inputs and allocator overhead remain outside this reservation.
This does not establish that the historical maintenance RSS failure is fixed.
The earlier 4,096-page diagnostic failed RSS too; it remains historical evidence,
not superseded by passing small tests here.

The environment owns its reservation until the database closes. A retained
allowance pins the active reservation throughout compact/rebase close and
reopen, so competing node work cannot take its reopen capacity. Fresh compact
validation and rebase environments acquire separate allowances even when their
sibling directories are outside the original registered directory. Standalone
chainstore compact reserves the extra allowance before copying or changing
maintenance artifacts. Snapshot-overlay startup preflight includes two MDBX
allowances for rebase; its snapshot generation and other allocations still need
complete accounting.

Validation:

- Final all-feature library suite: 998 passed, 0 failed, 12 ignored, 69.09 s.
- Both focused environment allowance tests passed after strengthening the
  SourceRenamed-phase race regression: the closed source retains its allowance,
  competing work fills all remaining bytes, and the source still reopens.
- Native subprocess compact crash matrix passed at all five durable boundaries;
  four-table content identity survived process termination and restart.
- Strict all-target/all-feature Clippy passed; default-feature Clippy also passed.
  Formatting/diff checks passed. No RSS measurement ran concurrently with tests.
- Prior CI 35187130337/ac3fe74 ended with Linux tests/90% coverage and supply-chain
  successful, Windows failed on the CLI future-size lint. The earlier d3565b0
  commit contains its production boxing fix; new CI must validate that fix.

No full-scale or reduced RSS acceptance is claimed. These functional tests do
not replace the failed workloads or the required continuous whole-node run.

Required follow-up: bound/stream/spill execution input, prepared, transition and
undo allocations before rerunning the failed maintenance workload; verify true
whole-node RSS/disk limits and final-source replay/optimizer behavior. No public
soak has begun and no overall gate is closed by this change.
