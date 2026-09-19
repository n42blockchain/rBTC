# Finite memory retries for existing staged segments

The execution wrapper captures the fully verified initial stage identity before
attempting a batch. A typed memory-admission failure can now halve an existing
staged batch if the exact execution tip (height and hash) is unchanged, no deferred
scripts existed at entry or remain after the attempt, and full re-verification
still returns the same stage identity. No initial stage requires continued absence;
a new, missing, replaced, corrupt or unreadable stage does not authorize a retry.
Disk/ordinary local/I/O/protocol errors and any committed-tip change stop retries.

The next window is capped by the remaining staged blocks and strictly decreases
to one. Attempts return and join scoped workers before the guard; speculative
ownership is released and network/replay read-ahead disabled before retrying.
Existing checkpoint publication and startup recovery remain the only mechanisms
that advance/remove staged data. The guard never discards or rewrites a stage.
The captured identity holds its admitted manifest owner throughout the wrapper.

Validation on Mac:

- Full locked all-feature library suite:1,082 passed, zero failed,12 ignored,
  73.49 seconds, including the final one-byte-deficit case.
- Strict locked all-target/all-feature Clippy:10.32 seconds; default-feature
  all-target Clippy:29.89 seconds; fuzz locked all-target Clippy:3.12 seconds.
  Formatting and diff checks passed.
- Guard coverage includes windows larger than the actual stage, finite halving,
  zero/one windows, changed height/hash, unreadable execution tip, pending scripts,
  every non-memory error class, stage removal/replacement/corruption, and unchanged
  source bytes. Existing unstaged retry tests continue to pass.
- A real-node test mines16 Regtest blocks, each with96 unspendable8192-byte outputs.
  Three fresh redb/ledger fixtures share this corpus without database copies. An
  idle localhost peer supplies only a handshake, never block data. A64MiB budget
  is bound to each fixture's ledger path; this measures ledger reservations, not
  whole-node memory or RSS.
- The calibration fixture measures both a one-block read with the wrapper/attempt
  identity owners and a full one-block execution/publication checkpoint. Under
  the checkpoint allowance, a16-block read demonstrably fails; the production
  wrapper shrinks and eventually executes/publishes all16 exact original payloads.
  Intermediate checkpoints preserve the original immutable stage.
- Holding one additional byte beyond the calibrated one-block read allowance
  makes the finite sequence stop with typed memory pressure, unchanged genesis
  execution tip, no published blocks, identical stage bytes and refunded attempt
  owners. Releasing that extra pressure restores progress.
- With only the calibrated read allowance, execution commits exactly block1 but
  publication fails. The wrapper returns the typed error without retrying the
  committed transition. Releasing pressure and invoking startup recovery publishes
  that committed prefix at the same execution tip. Shared memory/spool leases
  return to zero after the test owners are released.

The pre-extension targeted integration passed in21.92 seconds; the final complete
suite includes the additional one-byte-deficit assertions. Logs are retained in
session-state/2026-09-19/staged-memory-retry outside the development checkout.
Previous exact CI35430419364/9d0b300 passed Linux, Windows and supply-chain; this
source still needs its own CI.

This closes automatic finite downshifts for an unchanged existing stage, not the
shared-resource release gate. If even identity verification cannot be admitted,
resumption still needs resources to become available. Fair waiting, persistent
adaptive batch sizing across calls, whole-stage aggregate work admission, automatic
post-commit publication retry and auxiliary-index recovery remain open. Read
admission does not reserve future publication capacity. Whole-node startup/steady
RSS, physical disk, Headers concurrent fault acceptance, final-source replay,
seven-day public soak and signed artifact release prerequisites remain outstanding.
