# Bounded archive range rewriting

The existing streamed prefix writer now delegates to a contiguous range writer.
It can also produce a suffix or middle range with the original starting height.
Each of two passes checks the expected identity and the complete source framing,
compressed pieces and record digest, including skipped records. Skipped payloads
use fixed scratch; selected records are visited individually. Compression retains
the shared native memory admission, bounded anonymous spool and manifest owners.
The destination is not opened until validation and compression finish.

Validation on Mac:

- Four targeted streaming tests passed in 0.66 s.
- Full locked all-feature library suite: 1,073 passed, zero failed, 12 ignored,
  71.00 s.
- Strict locked all-target/all-feature Clippy passed in 16.90 s.
- Fuzz locked all-target Clippy passed in 15.53 s; formatting/diff checks passed.
- New regressions read back complete/middle/suffix outputs and verify heights
  and exact payloads. They reject invalid/overflowing ranges, changed identities
  and invalid full-record commitments even when both end regions are skipped.
- Exhausted memory and one-byte-insufficient spool allowances retain both input
  and existing destination bytes. Leases and anonymous scratch are released;
  releasing pressure permits the suffix rewrite and exact readback.

This is a file transformation primitive, not a ledger publication protocol.
Existing prefix publication uses the generalized implementation; the node still
requires a complete staged segment to fit its execution batch. Checkpoint-aware
suffix publication, its durable interruption journal/recovery, startup integration
and whole-node fault/resource acceptance remain open. No readiness status changes.

Previous exact CI35388353685/7940749 passed all jobs. This change requires its own
CI; local tests and earlier CI do not establish final-source release acceptance.
