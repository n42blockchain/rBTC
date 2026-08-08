# Test execution report — 2026-08-08

- Branch: `audit/group-b-decisions`
- Commit checked: `f9bc657`
- Working directory: `/Users/jieliu/Documents/n42/rBTC`
- Executed environment:
  - `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind`
  - `RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd`
  - `cargo` profile: release for differential/interop suites, debug for unit suites
- Wall time window: 2026-08-08 (real daemon-backed acceptance checks)

## Commands run

1. `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd cargo test --release --test core_block_differential -- --ignored --nocapture`
   - Result: `9 passed; 0 failed`
   - Runtime: `21.21s`
   - Coverage includes: Core 31 and btcd interop matrix, v2 transport interop test, and fallback test.

2. `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd cargo test --locked --lib -- --ignored --exact inbound::tests::core31_and_btcd_complete_real_inbound_handshakes --nocapture`
   - Result: `1 passed; 0 failed`
   - Runtime: `0.39s`
   - Coverage includes: real inbound Torv3/btcd handshake smoke test with fixed seed and one-time port lifecycle.

3. `cargo test --locked tor_control::tests -- --nocapture`
   - Result: `5 passed; 0 failed`
   - Runtime: `0.61s`
   - Coverage includes: SAFECOOKIE, non-loopback/cookie checks, and service control-path failure behavior.

4. `cargo test --locked zmq_publisher::tests -- --nocapture`
   - Result: `5 passed; 0 failed`
   - Runtime: `0.61s`
   - Coverage includes: topic filtering, sequence labels, non-blocking backlog and slow-subscriber drop policy.

## Overall result

All requested real-daemon acceptance checks passed at this point. No regressions were introduced in the test-targeted command set.

## Notes

- This run does not include the separate seven-day public-network-soak finalizer; that remains open in `docs/PUBLIC_NETWORK_SOAK.md`.
- Minor compiler/dependency warnings were observed (unused constants in `redb` and `src/node.rs`) but no test failures.
