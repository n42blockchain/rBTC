# Streaming ledger truncation recovery

Partial archive truncation during reorg/recovery now reuses the verified streaming prefix writer. It no longer decodes an entire retained archive into a block vector and truncates that vector before rewriting. The source prefix is streamed into admitted bounded compression scratch, with complete source identity/framing/digest checks before the replacement file is opened.

The durable truncate-intent protocol is unchanged: record intent, rewrite affected prefixes, synchronize the temporary archive, rename, synchronize ledger mutations, recover the index and clear the intent. The existing seven sync-failure boundaries remain covered by the recovery matrix.

A real bound-ledger regression exhausts the shared temporary allowance before truncation, verifies the original slot bytes and durable intent survive, confirms reopen still fails under pressure, then releases pressure and reopens successfully. The retained prefix is exact, later blocks disappear, the intent clears, temporary usage returns to zero, and normal append resumes.

Production ledger read/recovery/publication/truncation paths addressed in this series now avoid whole source-archive record vectors. The public compatibility `staged()` API still intentionally materializes its returned segment, but node startup uses the manifest/visitor APIs. This does not complete shared native codec/read-buffer admission, total physical disk accounting, memory-pressure scheduling, startup RSS or full-scale/long-duration acceptance.

Validation: truncation-focused tests 14 passed (1.68 s); all-feature library suite 1,052 passed, 0 failed, 12 ignored (66.57 s); strict all-target/all-feature Clippy passed (7.33 s). Formatting and diff checks passed.
