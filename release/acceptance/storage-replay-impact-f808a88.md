# Storage replay impact review — `f808a88`

The accepted dual-engine replay at
[`storage-replay-20260915.md`](storage-replay-20260915.md) remains bound to
commit `3cf44ae68003ceb78e9f0c77042667b475091716`. This review does not relabel
that historical run as an `f808a88` replay.

## Changed behavior

`f808a88` changes only the block-download retry path in `src/node.rs` and
reservation-denial diagnostics in `src/node_memory.rs`. After a pre-commit
memory refusal, the node may pin the exact contiguous staged batch created by
that attempt and retry it at a smaller size. The execution tip must remain
unchanged; a replaced stage, committed state, or non-memory failure still
refuses the retry. Each retry reruns normal consensus validation. Block
execution, UTXO transition logic, serialization, database transaction code,
snapshot parsing and storage recovery formats are unchanged.

## Current-source evidence for the changed path

- The `f808a88` all-features suite passed 1,101 library tests; its focused
  memory-budget tests cover fresh-stage adoption, immutable stage identity,
  retry termination and post-commit refusal.
- The live Testnet4 failure that motivated the change was reproduced at height
  51,456 under the configured memory allowance. The diagnostic run confirmed
  that the staged batch could be retried in smaller pieces without changing
  the execution tip; the default `f808a88` binary then continued on the same
  preserved database.
- Admission and header resource reports for `f808a88` are separately recorded
  in the readiness manifest. They do not claim to repeat the historical
  28,350-block cross-engine replay.

## Decision and limit

Retain the 28,350-block replay as evidence for deterministic content, atomic
storage transactions and fresh-process audits on the unchanged successful
execution path. Pair it with the current-source forced-memory retry tests for
the newly changed pre-commit branch. The old report remains attached to its
actual tested commit. A future change to block execution results, UTXO
transitions, serialization, database transactions, snapshots or recovery
formats requires another historical replay.

The selected replay is not genesis-to-tip validation, and this impact review
does not establish full-history bootstrap or whole-node soak behavior.
