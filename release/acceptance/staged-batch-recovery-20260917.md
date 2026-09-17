# Bounded staged-segment recovery reads

The ledger now offers `staged_manifest`, which fully verifies staged records without materializing their block payloads, and `read_staged_batch`, which verifies a bounded contiguous selection against a previously observed complete archive identity. An absent/changed/truncated archive or insufficient first-record byte allowance is an error; reads never publish, rewrite or remove staged data.

Production startup reconciliation and offline reindex reconciliation now validate staged prefixes in batches of at most 16 blocks and 32 MiB of record bytes. They retain their existing active-chain and consensus checks and publish only after the entire required prefix validates. A wrong-chain result still discards the staged segment, and validation errors still propagate. The MDBX driver's presence-only startup check uses the streaming manifest verifier instead of loading the complete staged payload before discard.

Tests preserve staged file bytes across successful and denied reads, reopen and resume, reject same-height replacement using the old identity, and prove unpublished blocks remain invisible through retained reads. A 35-block visitor test crosses multiple batches, checks order, stops on a later-batch fork result, propagates a validation error, and confirms neither path publishes data.

This is intermediate progress, not completion of the resource gates. Each recovery batch currently repeats whole-archive verification: memory is bounded, but work scales with the number of batches times archive size. Replace this with one identity-checked streaming traversal before declaring the work budget closed. Manifest/piece/zstd scratch still needs shared admission. Partial-prefix `commit_staged` still loads the entire archive during publication, so this change alone is not a complete startup RSS bound. The node's memory-pressure downshift/retry loop and preservation/reuse of the uncommitted suffix remain pending.

Validation: all-feature library suite 1,048 passed, 0 failed, 12 ignored (69.32 s); strict all-target/all-feature Clippy passed (19.72 s); focused ledger regression passed (0.26 s). Formatting and diff checks passed.
