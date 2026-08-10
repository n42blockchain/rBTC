# Test execution report — 2026-08-08

- Branch: `audit/group-b-decisions`
- Revision checked: `audit/group-b-decisions` at `e3d99cc`; the earlier
  `cf87ee2`, `0f47054`, and `4f8084f` real-daemon results are retained below
  as historical baselines only.
- Working directory: `/Users/jieliu/Documents/n42/rBTC`
- Executed environment:
  - `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind`
  - `RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd`
  - `cargo` profile: release for differential/interop suites, debug for unit suites
- Wall time window: 2026-08-08—2026-08-09 (real daemon-backed acceptance checks)

## Commands run

1. `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd cargo test --release --test core_block_differential -- --ignored --nocapture`
   - Result: `9 passed; 0 failed`
   - Runtime: `21.21s`
   - Coverage includes: Core 31 and btcd interop matrix, v2 transport interop test, and fallback test.

2. `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd cargo test --locked --lib -- --ignored --exact inbound::tests::core31_and_btcd_complete_real_inbound_handshakes --nocapture`
   - Result: `1 passed; 0 failed`
   - Runtime: `0.39s`
   - Coverage includes: real inbound Core 31/btcd v1 handshake smoke test with fixed seed and one-time port lifecycle. The test source contains no TorV3, onion, or `addrv2` path.

3. `cargo test --locked tor_control::tests -- --nocapture`
   - Result: `5 passed; 0 failed`
   - Runtime: `0.61s`
   - Coverage includes: SAFECOOKIE, non-loopback/cookie checks, and service control-path failure behavior.

4. `cargo test --locked zmq_publisher::tests -- --nocapture`
   - Result: `5 passed; 0 failed`
   - Runtime: `0.61s`
   - Coverage includes: topic filtering, sequence labels, non-blocking backlog and slow-subscriber drop policy.

## Overall result

Initial real-daemon acceptance checks passed at this point. No regressions were introduced in the earlier command set.

## Notes

- This run does not include the separate seven-day public-network-soak finalizer; that remains open in `docs/PUBLIC_NETWORK_SOAK.md`.
- Minor compiler/dependency warnings were observed (unused constants in `redb` and `src/node.rs`) but no test failures.

### 补一轮 — 2026-08-08（复验）

- Branch: `audit/group-b-decisions`
- Working directory: `/Users/jieliu/Documents/n42/rBTC`
- Executed environment:
  - `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind`
  - `RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd`
  - `RBTC_TOR_CONTROL=127.0.0.1:9051`
  - `RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie`
  - `RBTC_TOR_SOCKS=127.0.0.1:9050`
  - `RBTC_I2P_SAM=127.0.0.1:7656`
  - `cargo` profile: release for differential/interop suites, debug for unit suites
- Wall time window: 2026-08-08（复验复跑）

#### Mock 级（模拟级，隔离真实外部依赖）

1. `cargo test --locked --all-features -- i2p tor_control`
   - Result: `14 passed; 0 failed`
   - Runtime: 本机快速执行
   - 说明：该集合包含 14 个 mock 级单元测试（i2p/sam + tor_control），不连接真实 Tor/I2P Daemon；仅验证参数边界、请求报文与本地控制路径行为。

#### 真实外部依赖级（含真实守护进程）

1. `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd cargo test --release --test core_block_differential -- --ignored --nocapture`
   - Result: `9 passed; 0 failed`
   - Runtime: `22.75s`
   - Coverage includes: Core 31 和 btcd 的区块差异与一致性矩阵、V2 互通回归、V1 回退行为。

2. `RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd cargo test --locked --lib -- --ignored --exact inbound::tests::core31_and_btcd_complete_real_inbound_handshakes --nocapture`
   - Result: `1 passed; 0 failed`
   - Runtime: `0.40s`
   - Coverage includes: 实际 btcd/bitcoind 环境下的 inbound 握手链路。

3. `RBTC_TOR_CONTROL=127.0.0.1:9051 RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656 cargo test --release --all-features --test anonymity_network_interop -- --ignored --nocapture`
   - Result: `1 passed; 2 failed`
   - Runtime: `9.88s`
   - Environment validation: tor (`--datadir /tmp/rbtc-nettest/tor/data`) 与 i2pd (`--datadir /tmp/rbtc-i2pd --sam.enabled 1 --sam.address 127.0.0.1 --sam.port 7656 --http.enabled 0 --daemon`) 已就绪。
   - Failures:
     - `sam_sessions_produce_stable_reusable_destinations`: `create a transient SAM session: MalformedReply`
     - `published_onion_service_is_reachable_through_the_real_tor_network`: `read over onion: Io(Custom { kind: UnexpectedEof, error: "early eof" })`

Overall result: 真实拨号级场景尚未通过（2 项失败）。两处失败已定位为代码缺陷并修复，见下节。

### 更正 — 2026-08-08

对照测试源码后更正上节两处记录，并说明两项失败的根因。本节未重跑任何测试。

- `inbound::tests::core31_and_btcd_complete_real_inbound_handshakes` 原记为
  「含 TorV3 地址场景」。该测试源码中不含任何 TorV3、onion 或 `addrv2` 处理，
  它在回环 TCP 上与 Core 31 及 btcd 完成 v1 握手。TorV3 一项撤回。
- `tor_control::tests` 原同时列在「Mock 级」与「真实外部依赖级」两栏。它完全
  对着 `src/tor_control.rs` 内定义的模拟控制端口运行，在未安装 Tor 的机器上
  同样通过，因此只属于 Mock 级；其中的 onion 发布与回收也发生在模拟端口上，
  两次运行都没有在 Tor 网络上创建过服务。已从真实外部依赖级移除。

#### 两项失败的根因与修复

- `sam_sessions_produce_stable_reusable_destinations`（`MalformedReply`）是
  实现缺陷。`src/i2p_sam.rs` 的 `command()` 为每条命令新建一个 `BufReader`，
  它一次从 socket 读最多 8 KiB，并在被丢弃时连同换行之后的字节一并丢失。真实
  路由器把 `HELLO REPLY` 与 `SESSION STATUS` 合并进同一个 TCP 段时，第二行即
  被吞掉。同一模式在 `STREAM CONNECT` 之后更危险：那时 socket 承载的是对端的
  Bitcoin 流量，提前读取会静默吞掉握手的头几个字节而不报错。已改为逐字节读取
  回复行，socket 精确停在换行之后，并新增回归测试
  `i2p_sam::tests::coalesced_replies_are_read_one_line_at_a_time`，该测试对旧
  实现超时失败、对新实现通过。
- `published_onion_service_is_reachable_through_the_real_tor_network`
  （`early eof`）是测试自身的缺陷，不是节点代码问题。测试等待观察一个 ping，
  但 `PeerSession::read_message` 在内部应答 keepalive、只向调用方暴露应用消息，
  因此该等待永不结束，对端结束并断开后表现为 EOF。这也说明该次运行中控制端口
  认证、`ADD_ONION`、描述符扩散、SOCKS5 域名回拨与握手协商均已走通。收尾交换
  已改为 `GetAddr`/`AddrV2` 请求响应往返。

#### 最终复验 — 2026-08-08

修复后在同一台 macOS 主机上重跑真实守护进程 gate，结果达到预期：

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656   cargo test --release --all-features --test anonymity_network_interop   -- --ignored --nocapture
```

- Result: `3 passed; 0 failed`
- Runtime: `18.65s`
- Daemons: Tor control `127.0.0.1:9051`, SOCKS `127.0.0.1:9050`, cookie
  `/tmp/rbtc-nettest/tor/data/control_auth_cookie`; i2pd 2.61.0 SAM
  `127.0.0.1:7656` (`/tmp/rbtc-i2pd`).
- Coverage: `sam_bridge_is_refused_on_a_non_loopback_address` verifies the
  loopback guard; `sam_sessions_produce_stable_reusable_destinations` creates
  a real SAM session, replays its persisted destination key, and verifies the
  unreachable-destination deadline; the Tor case publishes an ephemeral v3
  onion, reaches it through SOCKS5, completes the v1 handshake, and performs
  a GetAddr/AddrV2 round trip.
- Mock regression re-run: `cargo test --locked --all-features -- i2p tor_control`
  — `16 passed; 0 failed` (the extra case covers replayed destination-key
  command construction).

During this final run, i2pd exposed one additional framing edge in the same
SAM command path: writing the command body and its newline as separate writes
could make i2pd close before replying. `command()` now writes the complete
line atomically, and replayed destination keys omit the transient-only
`SIGNATURE_TYPE` option. The regression
`i2p_sam::tests::replayed_destination_keys_do_not_set_a_transient_signature_type`
covers the latter; the final mock-filtered run passed `16/16`.

Tor and I2P therefore now have real daemon-level evidence in this report. The
separate seven-day public-network-soak finalizer remains open in
`docs/PUBLIC_NETWORK_SOAK.md`.

### `0f47054` follow-up revalidation — 2026-08-09

`0f47054` changes the Tor and I2P node startup paths (onion republication,
destination hashing, persisted-key error handling, and I2P transport fallback),
so the preceding `4f8084f` `3/3` result does not cover this revision.

#### Current real-daemon interop gate

The following command was run twice against the same Tor and i2pd daemons:

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656 cargo test --release --all-features --test anonymity_network_interop -- --ignored --nocapture
```

| Run | Result | Runtime | I2P address printed by the test | Onion address printed by the test |
| --- | --- | ---: | --- | --- |
| 1 | `3 passed; 0 failed` | `20.17s` | `znkzunzaibatgkaioly5ja4xaliy556sld3kqpbcebvzflwnuwga.b32.i2p` | `47g6dlgbnzfgdu3yud3oyu3kuy5z6pcgkbiks62oeayseyrild43xgad.onion` |
| 2 | `3 passed; 0 failed` | `12.30s` | `psdncwzouhqfcnnx3bgtgq7dpzzmerfswlzk3hon4xer425bqgba.b32.i2p` | `hb76wbsqulj7i4rzo5ivpgt6w7bcy2syn65bkpyafslxsuxqvj5bj4qd.onion` |

These two test invocations pass on the current revision, but each invocation's
interop test intentionally creates a fresh temporary identity. Their differing
addresses are therefore expected and are not cross-process replay evidence.

#### Cross-process persistence check

To exercise the changed node startup paths, I launched the current
`target/debug/rbtcd` twice with the same `/tmp/rbtc-node-replay` data directory:

```bash
target/debug/rbtcd --network regtest --no-dns-seeds \
  --data-dir /tmp/rbtc-node-replay --listen 127.0.0.1:18446 \
  --torcontrol 127.0.0.1:9051 \
  --torcontrol-cookie /tmp/rbtc-nettest/tor/data/control_auth_cookie \
  --i2psam 127.0.0.1:7656 --log-level info
```

- First process (fresh data directory): created I2P
  `ffai23hj75a733hy4dqftiycc2curnrybojofdf2jeq6ju24nidq.b32.i2p` and
  published onion
  `sbcobh37lmnwdqcgxzfplbzr5miwv73legnlgtdbafauhzfqxzliz6yd.onion:18446`.
- Process was stopped cleanly; owner-only key files were present at
  `i2p_destination_key` and `onion_service_key` (mode `0600`).
- Second process (same data directory): logged exactly the same I2P destination
  and onion address, then reached the normal no-peer retry loop.

This is the first current-revision evidence that the node's I2P destination
and Tor v3 onion identity survive a process restart and are actually accepted
by the live SAM and Tor control services. The seven-day public-network-soak
finalizer remains open.

### `cf87ee2` follow-up revalidation — 2026-08-09

`cf87ee2` changes the Tor/I2P startup and relay paths again, including the new
I2P `addrv2` advertisement and the onlynet DNS short-circuit. Consequently the
`8bc42f1`/`0f47054` `3/3` results above do not cover this revision.

#### Real-daemon interop gate (two fresh runs)

Both runs used the live Tor control/SOCKS pair (`127.0.0.1:9051` /
`127.0.0.1:9050`, cookie authentication) and i2pd SAM (`127.0.0.1:7656`):

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 \
RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie \
RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  -- --ignored --nocapture
```

| Run | Result | Runtime | I2P address | Onion address |
| --- | --- | ---: | --- | --- |
| 1 | `3 passed; 0 failed` | `20.17s` | `znkzunzaibatgkaioly5ja4xaliy556sld3kqpbcebvzflwnuwga.b32.i2p` | `47g6dlgbnzfgdu3yud3oyu3kuy5z6pcgkbiks62oeayseyrild43xgad.onion` |
| 2 | `3 passed; 0 failed` | `12.30s` | `psdncwzouhqfcnnx3bgtgq7dpzzmerfswlzk3hon4xer425bqgba.b32.i2p` | `hb76wbsqulj7i4rzo5ivpgt6w7bcy2syn65bkpyafslxsuxqvj5bj4qd.onion` |

The interop fixture intentionally creates a fresh temporary identity per
invocation, so the differing addresses are expected. They are not replay
evidence; the current-revision replay check is separate below.

#### Same-data-directory replay (current revision)

`target/debug/rbtcd` was launched, stopped cleanly, and launched again with
the same `/tmp/rbtc-cf87-replay` directory and the live Tor/i2pd daemons.
Both processes logged exactly the same values:

- I2P: `kxkyk4rreybwa7hfpjmx4bwqaxvi3dc4pzprvsre64w6ndmyhkdq.b32.i2p`
- onion: `gbwbr72vtvehrjwfvhoowgkfuac2g7cdpwimwsxfygeyw5fzcxlaprqd.onion:18446`

The persisted `i2p_destination_key` and `onion_service_key` were both
owner-only (`0600`). This is current-code, cross-process evidence that the
two identities are reused rather than regenerated.

#### Focused regression and DNS restriction checks

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked --all-features -- i2p tor_control zmq_publisher
```

Result: `22 passed; 0 failed`. This includes the coalesced SAM-reply framing
regression, replayed-key option regression, Tor control tests, I2P peer-store
tests, and ZMQ publisher tests. The dedicated
`node::tests::anonymity_only_restrictions_send_no_dns_query` also passed
(`1 passed; 0 failed`). Real `--onlynet onion` and `--onlynet i2p` launches
logged `dns=disabled` and exited without a DNS-seed attempt.

#### Two-node I2P/addrv2 attempt — not accepted as pass

This remains the one requested end-to-end gap. A first attempt with both nodes
on SAM `127.0.0.1:7656` failed with `DUPLICATED_ID`: the current node uses the
fixed SAM session id `rbtc`, so two simultaneous node sessions cannot share
one bridge. A second attempt used two live local bridges (`7656` and `7657`)
and confirmed that both nodes could create real I2P destinations, but the
TCP peer handshake was reset before an `addrv2` response was observed. The
logs report `p2p io: Connection reset by peer`; the isolated node has no active
consensus data source yet, so its inbound handshake is closed before it can
serve addresses. A Core 31-assisted bootstrap also connected successfully but
then terminated with `inbound P2P listener task missing` before the second
node could be attached.

Therefore this run provides no end-to-end proof that a second rbtcd process
received the I2P entry in `addrv2`; that evidence remains open under task #12.
The failure is recorded rather than counted as an addrv2 pass.

#### DNS packet capture limitation

`tcpdump` was attempted on both `en0` and `lo0` with a `port 53` filter, but
this macOS account cannot open `/dev/bpf0` (`Permission denied`); non-interactive
`sudo` also requires a password. Consequently there is no packet-capture
claim in this report. The source-level DNS guard test above passed, and the
real onlynet launches reported `dns=disabled`, but a privileged capture is
still required for the requested wire-level proof.

**Current-revision conclusion:** Tor and I2P each have two fresh `3/3`
real-daemon interop runs, and same-directory identity replay is proven. The
I2P `addrv2` two-node receipt and privileged DNS packet capture remain open;
the seven-day public-network-soak finalizer is also still open in
`docs/PUBLIC_NETWORK_SOAK.md`.

### `de61422` / `b159fe6` follow-up — 2026-08-09

The branch was fast-forwarded through `de61422`, `b159fe6`, and `9695792`.
This round specifically rechecked the new inbound-I2P service wiring, the
two-session SAM exchange, and the existing Core 31/btcd inbound regression.

#### Existing real-daemon interop cases

With the live Tor pair and i2pd SAM on `127.0.0.1:7656`, the three previously
passing ignored cases (excluding the new `two_nodes` case) were rerun:

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 \
RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie \
RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  -- --ignored --nocapture --skip two_nodes_exchange_i2p_destinations_over_addrv2
```

Result: `3 passed; 0 failed` in `28.33s` (non-loopback guard, real SAM
replay/deadline, and real Tor onion/GetAddr exchange).

#### New `two_nodes` case

The supplied command was run against the shared bridge:

```bash
RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  two_nodes -- --ignored --nocapture
```

The two sessions were both created on the same bridge and printed distinct
destinations, so the former `DUPLICATED_ID` failure did not recur. The test did
not, however, complete its unmodified source form:

- first run: `153.49s`, EOF during the I2P P2P handshake;
- after restarting i2pd: `108.40s`, EOF while reading the `AddrV2` response;
- split-bridge variant (`RBTC_I2P_SAM_B=127.0.0.1:7657`): `150.64s`, EOF while
  reading the `AddrV2` response.

As a diagnostic only, an uncommitted five-second keepalive was inserted after
the responder wrote its `AddrV2` frame. The split-bridge variant then passed
`1/1` in `83.03s`, including the STREAM ACCEPT peer-Destination assertion and
both directions of `addrv2`. This shows that the response-stage EOF is caused
by the test responder dropping the SAM stream immediately after its final
write (or by the router requiring that stream to remain open), not by a
`DUPLICATED_ID` or address-hash failure. The temporary change was reverted and
is not part of the branch. A shared-bridge run with the same temporary delay
still hit an earlier routing EOF, so the requested shared-bridge `1/1` remains
unproven on this router.

The test should keep the responder stream alive until the dialling side has
acknowledged the address response (a protocol-level acknowledgement is better
than a fixed sleep) before it is used as a merge gate.

#### Core 31/btcd inbound regression

```bash
RBTC_BITCOIND=/Users/jieliu/tools/bitcoin-31.0/bin/bitcoind \
RBTC_BTCD=/Users/jieliu/tools/go/bin/btcd \
  cargo test --locked --lib -- --ignored --exact \
  inbound::tests::core31_and_btcd_complete_real_inbound_handshakes --nocapture
```

Result: `1 passed; 0 failed` in `0.49s`. The TCP inbound path remains healthy.
This test does not exercise the newly added I2P branch of
`run_listener_with_i2p`; that branch still lacks an equivalent real-daemon
inbound-service assertion.

#### Focused current-code regressions

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked --all-features -- i2p tor_control zmq_publisher
```

Result: `23 passed; 0 failed`, including the new accepted-stream peer
Destination/first-byte unit test and all prior SAM, Tor control, peer-store,
and ZMQ checks.

**Current conclusion:** `de61422` passes the requested Core31/btcd TCP gate and
the three established real-daemon anonymity cases remain green. The supplied
unmodified `two_nodes` test is not a `1/1` acceptance on this host because its
responder closes the I2P stream too soon; the temporary keepalive proves the
exchange itself on the split-bridge variant, but shared-bridge end-to-end
evidence and a real `run_listener_with_i2p` acceptance test remain open.

### `5d638ae` task #12 revalidation — 2026-08-09

`5d638ae` changes the real two-node I2P test's stream-lifetime handshake: the
dialling side sends a nonce-bearing ping only after it has received the peer's
`addrv2`, and the accepting side keeps its SAM stream open for that protocol
round trip. This section is the acceptance record for the current revision;
the earlier `3/3`, replay, and TCP results are retained above as history.

#### Real daemon gate (two fresh 3/3 runs)

With Tor control/SOCKS (`127.0.0.1:9051`/`127.0.0.1:9050`, cookie
`/tmp/rbtc-nettest/tor/data/control_auth_cookie`) and i2pd SAM
(`127.0.0.1:7656`) running, the established cases were run twice, excluding
the separately selected two-node test:

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 \
RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie \
RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  -- --ignored --nocapture --skip two_nodes_exchange_i2p_destinations_over_addrv2
```

- Run 1: `3 passed; 0 failed` in `23.88s` (Tor onion reachability, real SAM
  replay/deadline, and the loopback bridge guard).
- Run 2: `3 passed; 0 failed` in `51.58s` (same three live-daemon cases).

#### Task #12 two-node I2P/addrv2 exchange

The split-bridge variant passed on the current test commit:

```bash
RBTC_I2P_SAM=127.0.0.1:7656 RBTC_I2P_SAM_B=127.0.0.1:7657 \
  cargo test --release --all-features --test anonymity_network_interop \
  two_nodes -- --ignored --nocapture
```

Result: `1 passed; 0 failed` in `30.38s`. The assertions covered the real
`STREAM ACCEPT` peer Destination, the completed v1 handshake, and both
directions of the I2P `addrv2` exchange.

The shared-bridge variant was attempted three times with the required command:

```bash
RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  two_nodes -- --ignored --nocapture
```

It is **not accepted as 1/1** on this macOS router. The first attempt failed
at `STREAM CONNECT` with `CANT_REACH_PEER / LeaseSet not found`; after the
router was restarted and warmed, the next two attempts completed the connect,
handshake, and bidirectional `addrv2` assertions but ended with EOF while
waiting for the final ping acknowledgement (`150.10s` and `108.06s`). The
i2pd diagnostics concurrently report repeated `Publish confirmation was not
received`/`destination is not ready` and failed tunnel tests. This is retained
as a real-daemon failure, not counted as a code pass or silently reclassified
as a mock result. The split-bridge `1/1` is the current positive evidence for
the new stream-lifetime change.

#### Same-data-directory identity replay

Using the current `target/debug/rbtcd`, the same fresh data directory was
started, stopped, and started again after the SAM bridge released the previous
session. Both successful launches logged exactly:

- I2P: `ock3syazsempnx2joqzov62cbxtx4fffxokplngdqigpjfjqcl3a.b32.i2p`
- onion: `rkubhi6eg3g32rfnqw6f4fz26rqnqbdsxu2qfyq36pxs5hkkewc25nad.onion:18448`

`i2p_destination_key` and `onion_service_key` were both mode `0600`. An
immediate restart before i2pd released the prior SAM ID returned
`DUPLICATED_ID`; waiting for the bridge teardown and retrying produced the
same addresses, so the persistence result is positive while the daemon
release timing remains an operational caveat.

#### Required regression and repository gates

- Core 31/btcd inbound regression:
  `inbound::tests::core31_and_btcd_complete_real_inbound_handshakes` —
  `1 passed; 0 failed` in `0.37s`.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
- Focused current-code set (`i2p`, `tor_control`, `zmq_publisher`, and the
  source-level anonymity-only DNS guard) — `24 passed; 0 failed`.
- `cargo clippy --locked --all-targets --all-features -- -D warnings` — passed.
- `cargo test --locked --all-features` — `706 passed; 0 failed; 3 ignored`.

The full repository gates are green on this macOS host. The audit task book's
Windows gate remains CI-owned and was not executable on this host.

#### Remaining #12 evidence gaps

The new `run_listener_with_i2p` inbound branch still has no real-daemon node
acceptance test; the Core31/btcd regression above exercises only the TCP
branch. A privileged DNS packet capture is also still unavailable: current
`tcpdump -ni lo0 -c 1 'port 53'` fails with `/dev/bpf0: Permission denied`,
and non-interactive `sudo` requires a password. The source guard and prior
real `--onlynet onion`/`--onlynet i2p` launches report `dns=disabled`, but no
wire-level capture is claimed.

**Current task #12 conclusion:** two fresh 3/3 daemon gates, split-bridge
two-node `addrv2` 1/1, same-directory identity replay, Core31/btcd inbound,
Clippy, and full tests pass. Shared-bridge 1/1, a real
`run_listener_with_i2p` acceptance, and privileged DNS capture remain open.

### `e3d99cc` targeted I2P revalidation — 2026-08-10

`e3d99cc` adds the first real-router acceptance case for
`run_listener_with_i2p` and changes the production SAM session identifier from
a stable per-data-directory value to a per-launch value with a random suffix.
The following results supersede the two corresponding open items above.

#### Real inbound-service acceptance

```bash
RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  the_inbound_service -- --ignored --nocapture
```

Result: `1 passed; 0 failed` in `76.56s` against i2pd 2.61.0. The test dialled
the real SAM Destination through the router, entered
`run_listener_with_i2p`, completed the v1 handshake, requested and received
the regtest genesis block, and asserted `accepted_total == 1`,
`handshakes_total == 1`, with no capacity/source rejection. This closes the
previous absence of any real-daemon evidence for the I2P branch of the inbound
service select.

#### Complete five-case anonymity suite

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 \
RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie \
RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656 \
  cargo test --release --all-features --test anonymity_network_interop \
  -- --ignored --nocapture
```

Result: `3 passed; 2 failed` in `139.38s`; this is not a green gate.

- Passed: non-loopback SAM refusal, real SAM destination replay/deadline, and
  the real Tor onion circuit.
- `the_inbound_service_accepts_and_serves_a_real_i2p_peer` failed at
  `STREAM CONNECT` with `CANT_REACH_PEER / LeaseSet not found`. It had passed
  alone immediately before this run. The concurrent suite made several fresh
  Destinations compete on one router; matching i2pd logs repeatedly report
  `destination is not ready`, missing publish confirmations, unavailable
  inbound/outbound tunnels, and a missing published LeaseSet.
- `two_nodes_exchange_i2p_destinations_over_addrv2` again completed far enough
  to print two distinct Destinations, but ended with `early eof` while waiting
  for the final address-response acknowledgement. This reproduces the
  shared-bridge failure recorded for `5d638ae`.

The targeted inbound-service result is valid positive coverage of the core
path, but the exact five-case command is currently sensitive to concurrent
i2pd publication and does not pass as a suite. Both facts are retained rather
than replacing one with the other.

#### Immediate same-directory restart

The current debug binary was started twice with the same fresh data directory,
Tor control endpoint, and i2pd SAM bridge. The second process was spawned
immediately after the first process exited; the harness inserted no sleep or
bridge-release delay.

- First launch created its SAM session after `22s`.
- The immediate second launch created its SAM session after `4s` and did not
  return `DUPLICATED_ID`.
- Both launches published exactly the same identities:
  - I2P: `4ke742y3tfamzvkrevny5xbpsvrnui6cveyalf2leof2uq4tej4a.b32.i2p`
  - onion: `5bpttc7znpihtzf2cvx6ylblnaq5emws3ptbijxnctqqex344docnnqd.onion:18449`
- `i2p_destination_key` and `onion_service_key` remained mode `0600`.

This is real-bridge evidence that the random session-ID suffix removes the
restart collision without changing the persisted public identity.

**Current `e3d99cc` conclusion:** the newly targeted inbound-service test and
the no-delay restart test pass. The complete five-case command remains red
(`3/5`) because concurrent Destination publication failed once and the known
shared-bridge acknowledgement EOF remains. Privileged DNS packet capture is
unchanged and still open.
