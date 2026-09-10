# Upstream operational acceptance, 2026-09-10

This continuation runs the previously skipped anonymity-network and generated
storage gates on Linux. Storage and existing transport tests used `51aee2a`.
The added live wallet-broadcast tests exposed a delivery-lifetime defect; this
change fixes the private wave and records the regression evidence.
It does not establish a competing-header retention bound, a complete admission
work/RSS budget, a seven-day public-network soak, or full mainnet storage replay.

## Environment and evidence

The host is Linux/x86-64 with an AMD EPYC 9B45 CPU, approximately 136 GiB RAM,
and an ext4 data volume. Storage temporary directories were explicitly placed
on that volume, not the host's tmpfs `/tmp`. Working sets fit RAM: these are warm,
generated workloads, not cold-disk or full-node validation measurements.

All runs use the locked Rust 1.85.0 toolchain and `--release --all-features`.
The executables were built before timing the storage lanes. `/usr/bin/time -v`
records process peak RSS and filesystem I/O; it is not an allocator-only bound.
The small Cargo wrapper is included in the first storage-lane measurements,
with compilation already complete. The uninterrupted reference directly runs
the same precompiled test executable.

Tor 0.4.9.11 and i2pd 2.59.0 were extracted from Ubuntu packages into an isolated
temporary directory, together with missing runtime libraries. No system service
or package installation was changed. Tor ran as a client with loopback SOCKS
and cookie-authenticated control listeners. i2pd used a loopback SAM bridge,
with UPnP and transit traffic disabled. Reseed verification stayed enabled.
Both temporary daemons were stopped after acceptance, their listeners were
verified closed, and their ephemeral runtime/key directories were removed.

Raw evidence is in `target/upstream-followup/2026-09-10/operational/`:
`environment.json` records versions, package SHA256 hashes and filesystem;
`anonymity-interop.log`, `private-broadcast-live.log` and
`testnet4-name-proxy.log` record network results. Storage JSON reports and
`time -v` logs use the names below. These are local artifacts; daemon cookies,
private destination keys and wallet databases are not repository fixtures.

## Real anonymity-network coverage

All five existing `anonymity_network_interop` tests passed in 64.61 seconds:

- Publish an ephemeral onion service, complete a real Tor circuit and withdraw it.
- Create and reopen stable I2P destinations through a real SAM bridge.
- Exchange I2P destinations between two peers with `addrv2`.
- Reach the production inbound service through I2P and retrieve the regtest
  genesis block, with accepted-peer and handshake accounting checked.
- Refuse a non-loopback SAM endpoint.

The configured name-proxy test also passed through the real Tor SOCKS endpoint:
a pinned Testnet4 seed led to a public peer handshake without incrementing the
node's local DNS lookup counter. This proves a bootstrap handshake, not sustained
synchronization or the public-network soak.

Both new live wallet gates passed after the lifetime correction below (the first
successful rerun took 152.96 seconds; the final receiver-lifetime regression
rerun passed again in 139.98 seconds). `node::tests::private_broadcast_interop`
exercises the production
wallet broadcast queue and private wave, including durable rebroadcast state and
completion notification. A local ordinary peer remains connected and is observed
until EOF; a quiet interval during circuit setup cannot prematurely end the leak
check. The standby relay receiver must also stay empty. The I2P case sends two
waves from the same wallet runtime and compares their remote destinations.
Only synthetic regtest transactions are sent to test-owned receivers. These gates
start at the wallet broadcast queue; they do not exercise HTTP PSBT signing or
claim to observe every system-level network packet.

## Private-wave lifetime correction

An initial live wallet run reported two successful I2P queue completions, but the
receiver timed out waiting for both transactions. The failed result is retained
in `private-broadcast-before-fix.log`; it is not counted as acceptance. The earlier
transport-only tests did not exercise this transient sender lifecycle.

Previously, `private_broadcast_wave` counted a successful write to the local
proxy and immediately dropped the peer stream and transient SAM control session.
i2pd's session teardown stops its local destination and closes associated
streams ([i2pd 2.59.0 SAM implementation](https://github.com/PurpleI2P/i2pd/blob/2.59.0/libi2pd_client/SAM.cpp#L1571)).
Premature session teardown is consistent with the observed local-success /
remote-timeout failure.

The private Tor and I2P paths now send a ping after the transaction and wait for
the matching pong before counting delivery. The existing 30-second peer deadline,
frame budget and pending-message byte limit still apply. Failure returns to the
existing queued retry path. This confirms remote receipt through the proxy; it
is not evidence of mempool acceptance. Ordinary relay semantics are unchanged.
A deterministic regression requires the matching nonce and refuses an unrelated
pong followed by closure, even after the transaction write itself succeeded.

## Generated storage acceptance

The default storage microbenchmark passed with 10,000 live UTXOs, 100 transitions,
100 updates per transition and 20,000 lookups. It covered redb normal/quick repair,
MDBX, SQLite and snapshot export/verification/import (`storage-bench.json`).

The matched complete-chainstate benchmark passed in both engine execution orders.
Each lane used 200,000 UTXOs, 256 transitions, 1,000 updates per transition and
100,000 lookups. Every mutation includes UTXOs, undo and the execution tip.
Serving commits each transition; IBD commits batches of 256. The ranges below
are the two observed runs, not statistical confidence intervals.

| Engine / mode | Synthetic transitions/s | Lookups/s | Allocated bytes after compaction |
| --- | ---: | ---: | ---: |
| redb serving | 160.0–160.3 | 2.967–2.975 million | 110,641,152 |
| MDBX serving | 300.4–302.8 | 4.068–4.069 million | 50,343,936 |
| redb IBD-256 | 305.1–308.5 | 2.845–2.905 million | 148,316,160 |
| MDBX IBD-256 | 535.2–557.3 | 4.059–4.078 million | 50,339,840–50,343,936 |

Reports: `engine-forward.json` and `engine-reverse.json`. The workload is too
small to select a production engine or extrapolate mainnet validation TPS.

## Million-UTXO churn, compaction and process restart

A separate MDBX lane kept **1,000,000 live UTXOs**, replaced 1,000 per synthetic
transition, committed batches of 256 and retained 288 undo rows. Capacity was
1 GiB. Compaction was enabled with a 15% capacity trigger and 20% minimum reclaim.
This deliberately low trigger exercises copying at the selected scale; it does
not change daemon defaults.

The first process reached transition 4,096 and exited successfully. A second
process reopened that same database and continued to 8,192. A fresh third
process independently ran all 8,192 transitions with compaction disabled.

| Lane | Start → finish | Peak process RSS (KiB) | Final allocated bytes | Compactions |
| --- | --- | ---: | ---: | ---: |
| Initial | 0 → 4,096 | 475,716 | 167,776,256 | 1 |
| Resumed | 4,096 → 8,192 | 434,396 | 184,553,472 | 0 |

At transition 768, the verified compact copy reduced high-water bytes from
161,804,288 to 118,521,856 and free-page bytes from 43,237,376 to zero, preserving
the content hash. Temporary copy space is additional to the final allocation.

The resumed compacted store and fresh uninterrupted uncompacted reference have
identical logical-content SHA256:

```text
7b827334d5f49592d5ad7da96de0e310ddb6c86b7e26372c941b083a51f3d6af
```

The comparison also requires identical record bytes, UTXO/undo/metadata counts
and execution tip. Both end with 1,000,000 UTXOs and 288 undo rows. Reports are
`churn-initial.json`, `churn-resumed.json`, `churn-reference.json` and
`churn-equivalence.json`.

This closes the selected generated benchmark and restart/compaction workload.
The 160-million-UTXO/900,000-transition gate, cold-disk acceptance and actual
mainnet replay remain separate requirements in `MDBX_REPLACEMENT_GATE.md`.

## Regression acceptance

The final all-feature suite passed **914 tests**, with zero failures and 28
ignored tests (875 library + 39 integration, excluding duplicate subprocess
helper output). The two added ignored tests are the explicit live wallet gates.
Strict all-target/all-feature Clippy and formatting checks passed. The production
fix adds no dependency or configuration changes; pong receipt may add one network
round trip and a peer that closes without acknowledging remains eligible for
bounded retry rather than a false delivery report.

## Reproduction

Use already-running real daemons with operator-selected loopback endpoints:

```sh
export CARGO_HOME=/tmp/rbtc-cargo-home
export RBTC_TOR_CONTROL=127.0.0.1:19051
export RBTC_TOR_COOKIE=/tmp/rbtc-network-interop/tor-data/control_auth_cookie
export RBTC_TOR_SOCKS=127.0.0.1:19050
export RBTC_I2P_SAM=127.0.0.1:17656
cargo test --locked --release --all-features --test anonymity_network_interop -- --ignored --nocapture
cargo test --locked --release --all-features --lib live_private_broadcast -- --ignored --nocapture
RBTC_NAME_PROXY="$RBTC_TOR_SOCKS" cargo test --locked --release --all-features --lib \
  a_real_seed_authority_bootstraps_testnet4_through_the_configured_name_proxy -- --ignored --nocapture
```

For storage, set `TMPDIR` to an existing isolated directory on the intended data
volume. Build before timing. Use `RBTC_BENCH_REPORT` and
`RBTC_ENGINE_BENCH_REPORT` to retain JSON; the reverse comparison adds
`RBTC_ENGINE_BENCH_REVERSE=1`.

```sh
cargo test --locked --release --all-features --test storage_bench -- --ignored --nocapture
cargo test --locked --release --all-features --test storage_engine_comparison -- --ignored --nocapture
RBTC_ENGINE_BENCH_REVERSE=1 cargo test --locked --release --all-features \
  --test storage_engine_comparison -- --ignored --nocapture

export RBTC_MDBX_GATE_UTXOS=1000000
export RBTC_MDBX_GATE_UPDATES=1000
export RBTC_MDBX_GATE_CAPACITY_BYTES=1073741824
export RBTC_MDBX_GATE_COMPACT_PERCENT=15
export RBTC_MDBX_GATE_MIN_RECLAIM_PERCENT=20
export RBTC_MDBX_GATE_REPORT_INTERVAL=1024
# Set RBTC_MDBX_GATE_DIR to a fresh isolated persistent directory, and set
# RBTC_MDBX_GATE_REPORT to a different JSON path for each process.
RBTC_MDBX_GATE_BLOCKS=4096 cargo test --locked --release --all-features \
  --test mdbx_mainnet_scale_gate -- --ignored --nocapture
RBTC_MDBX_GATE_BLOCKS=8192 cargo test --locked --release --all-features \
  --test mdbx_mainnet_scale_gate -- --ignored --nocapture
# Repeat 8192 in a DIFFERENT fresh directory with RBTC_MDBX_GATE_COMPACT=0;
# compare final_audit.content_sha256, record counts and tip in both reports.
```
