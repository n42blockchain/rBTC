# Failed-peer fixture isolation, 2026-09-11

The failed-peer fixtures released their ephemeral loopback ports before dialing
them. Another parallel test could acquire one of those ports and receive a
connection intended for the failed endpoint. The DNS fallback fixture released
an entire candidate wave this way; it could even allocate duplicate addresses
within its own wave.

The fixtures now retain bound, unlistened TCP sockets until the operation has
finished. This reserves each address while preserving immediate connection
refusal. The change covers two name-proxy/bootstrap tests, the persisted-fallback
test, the full stale-wave DNS test, and the HTTPS range-failure test. The aborted
name-proxy session is awaited before releasing its reservation. All changes are
test-only; the original failover test and its three nonce assertions remain
unchanged.

## Reproduction and limits

The original admission-index run recorded deficient-peer nonce
`11058300987366363687` instead of `99`. Its logs did not record the source socket
or sending test, so they cannot establish the historical client's identity.

A temporary, controlled schedule reproduces the mechanism in the original
failover test:

1. Allocate and release a failed endpoint using the old fixture pattern.
2. Bind the deficient-peer listener to that exact address, representing another
   test obtaining the released port.
3. Dial the stale address with `connect_peer`, explicitly supplying a foreign
   nonce, before starting the intended node with nonce `99`.
4. Let the original node complete interrupted IBD and recovery. Its unchanged
   deficient-peer assertion fails on the foreign nonce.

The experiment deliberately supplied the historical nonce value; its exact
numerical match is not independent evidence of the historical sender. It proves
that the discovered fixture race produces this failure signature without a
production nonce mutation. The temporary injection is retained as a patch in
the evidence directory and is absent from the committed test.

The permanent regression attempts both rebinding and connecting to the reserved
endpoint three times. It requires `AddrInUse` for the competing listener and
`ConnectionRefused` for each client. A temporary helper variant that releases
the reservation before returning fails the competing-listener assertion; the
fixed helper passes. This verifies port ownership and refusal directly, without
depending on a lucky parallel schedule or weakening the nonce check.

## Validation

Evidence is retained under
`target/upstream-followup/2026-09-11/failover-isolation/`, including
`controlled-collision.log`, `node-fixed.rs.reproducer.patch`,
`reservation-before-fix.log`, `test-support-fixed.rs.reproducer.patch`, and
`reservation-fixed.log`.

The all-feature suite passed **937 tests** (898 library + 39 integration), with
**32 ignored** and no failures, using eight test threads. Subprocess-helper
output is excluded from those totals. The original failover test passed with
all three nonce assertions intact. Strict all-target/all-feature Clippy,
formatting and diff checks passed. Logs are `all-features-tests.log` and
`clippy.log`; `environment.json` records the platform and tested source hashes.

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
cargo test --locked --all-features --lib \
  refused_endpoint_reserves_its_port_through_repeated_connections
cargo test --locked --all-features --no-fail-fast -- --test-threads=8
cargo clippy --locked --all-targets --all-features -- -D warnings
```

This closes the demonstrated unreserved-port route between these fixtures.
Broader production failover, header retention, restart loading and seven-day
soak gates remain separate.
