# Publication owner lifetime — 2026-09-19

Base source: `6e2f76c102e76c16781dd1c7bc52fa42ee5d4c04` plus the accompanying
`src/node.rs` change. This is engineering validation, not frozen-source acceptance.

After durable execution and all index updates, the node now releases serialized
batch payloads, transaction IDs, deployment contexts and applied undo results
before ledger publication verifies/compresses the archive. Without a ZMQ notifier,
it also releases decoded blocks at that boundary; with a notifier, blocks remain
available until the existing post-publication notification. The saved batch count
continues to drive publication and diagnostics.

This only shortens ownership lifetime. It adds no execution retry, index replay,
durable format, or change to validation and publication order. In particular,
publication memory exhaustion after durable execution still returns an error;
the exact-tip retry guard still prevents duplicate execution. Broader live
publication/index recovery remains open. No whole-node RSS reduction has been
measured or claimed by this patch.

## Release checks repeated

- Base-commit CI `35474087150`: completed successfully for exact `6e2f76c`.
  This does not validate the accompanying source change on Linux/Windows.
- `python3 scripts/verify-release-readiness.py`: rejects admission-resources,
  header-resources and public-soak, as required.
- `gh secret list` and `gh secret list --env release-signing`: both empty.
- `gh run list --workflow release.yml --limit 3`: empty.
- Optimizer/storage evidence remains bound to old `3cf44ae`; final-source reruns
  and evidence binding remain necessary after engineering changes stop.

The shortest valid release path still requires finishing and measuring the
whole-node resource/fault gates, freezing source, rerunning affected scale/history
checks, catching up Bitcoin and Testnet4, collecting the full 604800-second soak,
and provisioning/rehearsing native signing plus final exact-commit main CI.

## Validation

- Final `cargo test --locked --all-features --lib`: 1082 passed, 0 failed,
  12 ignored (76.12 seconds), including staged pressure, checkpoint and recovery
  regressions. No new test-only hooks were needed.
- `cargo test --locked --all-features --test embedded_node_api host_zmq_endpoint`:
  actual executed-block notification passed (1/1, 2.34 seconds).
- Strict locked all-target Clippy passed with both all features and defaults.
- `cargo fmt --check` and `git diff --check` passed.
- An earlier focused staged-pressure run passed (1/1, 24.20 seconds) before the
  decoded-block release was added; the final full library run covers that addition.

Local logs are preserved under `session-state/2026-09-19/publication-owner-lifetime`
in the original rBTC workspace. The native Bitcoin dependency emitted its existing
C string initializer warning; it did not fail compilation or strict Rust Clippy.
