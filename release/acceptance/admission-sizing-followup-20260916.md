# Admission sizing follow-up, 2026-09-16

Status: implementation verified locally; admission-resources remains open.

The peer admission path previously released the transaction-pool mutex after
calculating its pipeline allocation estimate, then reacquired it to clone the
candidate. Another admission could grow the pool in that interval, so the
candidate and later snapshots could exceed the pool size used for reservation.

The path now retains the same pool guard from sizing through reservation,
candidate construction, validation and publication. The latter phases already
held that mutex; the change extends the critical section over snapshot decoding
and pending-package preparation. Those phases are synchronous and contain no
network waits. This trades a longer critical section for stable sizing.

The three node admission-resource tests pass, including orphan recovery,
replacement persistence, invalid-script rejection, dry-run resource deferral,
and queue preservation before/after draining. The queue test now also covers
zero candidate-memory capacity with sufficient work budget and checks that
deferral releases the pool mutex using try_lock.

This fix does not establish accurate accounting of all allocations or resumable
scheduling. Reports accepting optimizer-budget and storage-replay still refer
to source 3cf44ae; they cannot establish release acceptance for this modified
source. Repeat affected acceptance and update the frozen identity before release.
