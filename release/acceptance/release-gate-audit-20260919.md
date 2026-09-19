# Release gate audit — 2026-09-19

Prior exact source dee1752bcbb77c8c87737b63c2c36d7b56be2df4 passed
CI35389996036 (Linux, Windows and supply-chain). This audit accompanies further
staged-checkpoint changes and is not final-source release acceptance.

The release preflight still rejects admission-resources, header-resources and
public-soak. Optimizer-budget and storage-replay remain accepted only for the
older frozen source3cf44ae68003ceb78e9f0c77042667b475091716, not the current tree.
No readiness status or evidence identity has been rebound.

## Progress and remaining gates

1. Staged checkpoints now preserve an immutable source across smaller batches;
   normal and overlay startup publish a matching executed prefix before resuming
   its suffix. A stale future suffix is removed only after publishing its valid
   executed prefix. See [checkpoint tests](staged-checkpoint-recovery-20260919.md).
   Automatic staged pressure downshifts, live publication retry, fair scheduling
   and broader post-commit auxiliary-index recovery are still open. Checkpoint
   scans remain linear in the full stage per pass; aggregate work admission is
   not established by bounded-memory scans.
2. Shared allocation ownership and the startup/steady-state total memory bound
   remain incomplete. The user-selected32GiB default limits registered
   reservations, not total RSS. Native stacks, engine working sets and remaining
   decoded/network/metadata/legacy allocations still need accounting or measured
   bounds. Temporary spool reservations do not establish a physical whole-node
   disk quota. This change keeps an original stage alongside retained prefixes;
   full physical peak-disk acceptance remains open.
3. Headers candidate scheduling, byte/work/disk bounds, cancellation/restart and
   atomic-winner behavior still need sustained concurrent whole-node fault and
   resource acceptance. Existing primitives and ordinary CI cannot close this.
4. Freeze final source, rerun/rebind affected optimizer/storage evidence and
   complete outstanding scale/history validation. The selected935001–963350
   replay is not genesis-to-tip; the4M maintenance RSS result is not160M/900000
   acceptance. Default supported storage remains redb; MDBX replacement has its
   own experimental gate under the user's broader all-gates objective.
5. Both Bitcoin and Testnet4 must catch up using the frozen binary before the
   full604800-second soak. No accepted final-source report exists. Sample
   coverage, restart/fault exercises, peer diversity, tip consistency, freezer
   rotation and sustained RSS/disk records are required.
6. Configure real signing credentials and rehearse signed native artifacts;
   complete final exact-commit main push CI, source/evidence verification and
   version/tag prerequisites before release.

## GitHub checks repeated today

- Environment release-signing secrets:0; repository Actions secrets:0.
- Release workflow run list remains empty.
- Latest ordinary main push CI34890148170/b74c4e0 passed. Old scheduled Fuzz
  regression35202180108 and Dynamic analysis35070441497 failed. These are
  separate workflows and must not be called an ordinary push CI failure.
- Work-branch success does not substitute for successful main push CI on the
  exact eventual release commit.

No tag, signed artifact publication or public seven-day soak was started. All
historical failed measurements remain historical evidence, not accepted results.
