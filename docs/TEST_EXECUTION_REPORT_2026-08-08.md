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

#### 待重跑

修复后 `anonymity_network_interop` 尚未在真实守护进程上重跑。在配置了 tor 与
i2pd 的主机上执行下列命令，预期 `3 passed; 0 failed`：

```bash
RBTC_TOR_CONTROL=127.0.0.1:9051 RBTC_TOR_COOKIE=/tmp/rbtc-nettest/tor/data/control_auth_cookie RBTC_TOR_SOCKS=127.0.0.1:9050 RBTC_I2P_SAM=127.0.0.1:7656   cargo test --release --all-features --test anonymity_network_interop   -- --ignored --nocapture
```

在此之前，Tor 与 I2P 仍无真实守护进程级证据：目前唯一的真实外部依赖证据是
`core_block_differential`（含 BIP324 v2 互联与 v1 回退）与 inbound 握手测试。
