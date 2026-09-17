# Bounded exact comparison of already-published staged prefixes

Interrupted publication reconciliation previously loaded every staged block, then decoded an entire retained archive again for each compared block. It now verifies and streams the staged prefix into an anonymous canonical-record spool, then verifies each overlapping retained archive once while comparing lengths and bytes directly through fixed 64 KiB scratch. It does not substitute hashes for byte equality. Heights must be contiguous and both staged/retained archive identities and full record digests are verified.

The comparison spool obtains a conservative 1 GiB reservation from the existing shared logical temporary disk allowance before file creation. The archive scanner rejects records beyond that bound before invoking the writer callback. Errors and mismatches leave staged and retained files intact; scratch closes before its reservation returns. Successful comparison alone permits the existing staged-removal sync protocol to proceed.

The normal single-block ledger reader now also uses the bounded archive selection reader, retaining only its target block instead of all archive records. It still verifies the full archive and checks the manifest against the ledger index.

A regression exercises a prefix spanning two retained slots, a leading retained record outside the compared range, pressure denial/retry with exact staged identity preserved, and both same-length byte mismatch beyond the first scratch chunk and different-length mismatch. It checks unchanged retained tip and complete temporary budget refund.

This removes whole-record materialization from both initial partial staged publication and already-published-prefix comparison. Legacy staged materialization and retained-prefix truncation paths still exist; codec/manifest/read scratch memory admission, physical disk inventory, typed node resource recovery, fair resumable scheduling and final-scale/long-duration acceptance remain open. Comparison uses additional bounded temporary disk I/O; it is not a whole-node RSS result.

Validation: all-feature library suite 1,051 passed, 0 failed, 12 ignored (72.25 s); final strict all-target/all-feature Clippy passed (43.29 s). After a redundant-borrow cleanup, the final comparison regression passed (0.23 s). Formatting and diff checks passed.
