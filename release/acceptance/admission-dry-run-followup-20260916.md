# Dry-run result allocation follow-up, 2026-09-16

Status: locally verified implementation; admission-resources remains open.

Previously a successful dry-run admission cloned and ordered the entire
candidate pool through relay_snapshot, then hashed every cloned transaction
into a temporary map to extract fee and policy-size values for the request.
That result-construction phase introduced full-pool payload copies and graph
work even for a single requested transaction.

The result path now reads indexed, validated scalar metadata for each requested
txid. It does not create a relay snapshot, clone retained transaction payloads,
recompute their identities or optimize relay order. Snapshot-stage work is
charged against request count (512 accounting units per requested identity).
The private candidate and upstream validation still have pool-dependent costs;
this change only removes them from result construction.

The new regression compares indexed metadata with relay values, checks absent
transactions, withholds stale values after a chain change, and restores them
after reconciliation. Four resource-budget tests and four tests selected by
the dry_run filter passed, including RPC result shape, dry-run acceptance with
no live-pool mutation, resource deferral, and an unrelated prune dry-run test.

Remaining admission requirements include complete allocation accounting and
resumable scheduling. Existing accepted reports remain bound to 3cf44ae;
changed production source requires fresh acceptance binding before release.
