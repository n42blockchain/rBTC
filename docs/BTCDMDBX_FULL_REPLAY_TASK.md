# btcdmdbx 更新复核与全量 Bitcoin 回放任务书

日期：2026-08-20  
复核仓库：`../btcdmdbx`  
复核 HEAD：`b135528bcf96330e50bee5f107257c1ed60e27b2`  
功能候选：`fa64adfa76fb7163a4aa5d5cfeac8ccfca36eb01`  
可比基线：`2d036b154a28a863abcb22d946f22853758506f4`

## 0. 2026-08-21 执行结果（Windows 证据机）

lane 按第 4 节准备完毕（两 lane 字节一致的 `--json` 测量补丁、只读 `census` /
`corpusmanifest` 工具、固定参数、串行执行），但 lane B（`fa64adfa`）三次尝试均在
主机内存耗尽时失败：attempt 1（go1.26.5，与 41 GiB 无关进程并存）12 分钟后 GC
访问违例；attempt 2（`GOEXPERIMENT=nogreenteagc`，主机空闲）25.6 分钟后源文件读取
报 `ERROR_NO_SYSTEM_RESOURCES`，工作集 114.8 GiB；attempt 3（go1.27.0、依赖全部更新、
`--memlimit` 降为 80）**24m01s 到达 checkpoint 810,071**，随后在第二个回放进程占满
内存时于 ~821k 崩溃。固定的 `--gogc=400 --memlimit=112` 在 125.6 GiB 主机上没有余量，
这是参数本身的问题。用户以"fast-add 到 810k 约 24 分钟"关闭本轨道；全量耗时、
交易总数与 durable TPS 仍未取得。lane A 未运行。证据与偏差记录见证据机
`D:\rbtc-bench\btcdmdbx-full-replay-20260820\artifacts`；rBTC 侧随后的真实块
A/B 见 [REAL_BLOCK_OVERLAY_REPLAY_2026-08-21.md](REAL_BLOCK_OVERLAY_REPLAY_2026-08-21.md)。

## 1. 当前结论

更新有借鉴意义，但不能把提交说明里的局部速率当作全量结论，也不能把
btcd 的 Go 数据结构直接移植到 rBTC。

| 项目 | 当前证据 | 结论 |
|---|---|---|
| 旧的 stock-btcd MDBX 回放 | 完成至 951,225；200,000 块为 35 秒、5,653 blocks/s | 已证明能完成，未记录全量总耗时 |
| 旧回放交易量与平均 TPS | 工具只打印块数和 blocks/s；仓库内无原始全量日志 | 缺证，不能由 200,000 块区间外推 |
| `2d036b15` checkpoint/验证修正 | 200,000 块 32 秒；150,000 块完整验证 17 秒、8,566 blocks/s（提交说明） | 正确性修正优先于性能比较；尚无本仓库可审计原始日志 |
| `463398ca` 分片 UTXO cache/并行应用 | 1 worker 与 12 workers 的 200,000 块逻辑 census 字节一致（提交说明） | 候选优化；缺全量时间、TPS、峰值内存与最终状态证据 |
| `fa64adfa` cache-hit 快路与大 undo 并行编码 | 新增跨 511/512/513/1000/4999 条目的字节等价测试 | 可借鉴，但阈值和收益需要 rBTC 自己的 profile/基准 |
| `b135528b` `.gitignore` | 把原 58 行规则整体替换成 `blockchain/testdbs/` | 不应照搬；会重新暴露日志、二进制、数据库等产物 |

本机复核通过：

```text
go test ./blockchain -run 'TestMapSlice|TestMapsliceConcurrency|TestSerializeSpendJournalParallel' -count=1
ok github.com/btcsuite/btcd/blockchain

go test ./cmd/replayblocks -count=1
? github.com/btcsuite/btcd/cmd/replayblocks [no test files]

go test -race ./blockchain -run 'TestMapSlice|TestMapsliceConcurrency|TestSerializeSpendJournalParallel' -count=1
ok github.com/btcsuite/btcd/blockchain
```

这些只验证实现和编码等价性，不是性能证据。

## 2. 对 rBTC 的可迁移判断

### 应直接吸收进回放门槛

1. **固定验证边界。** `2d036b15` 修复了回放器没有向链实例传入
   `params.Checkpoints` 的问题。旧工具虽设置 fast-add，checkpoint hash 实际没有被
   链实例钉住。今后任何全量报告必须同时记录 checkpoint 高度、其 hash、fast-add
   区间及完整验证区间，不能只写“fast-add=true”。
2. **把独立工作移出串行 connect 临界路径。** 块解析、txid/wtxid、结构检查可以在
   有界工作队列中提前完成，但必须按链序交付。rBTC 已经预计算交易 ID、批量预取
   UTXO、延迟并行脚本验证，并单列 `validate/submit/script_wait/commit`；后续应用应以
   这些分项 profile 为依据，而不是再建一套无界流水线。
3. **等价性先于速度。** 分片更新必须保留同一 outpoint 的块内顺序，undo 的逻辑
   顺序和编码必须固定。A/B 除时间外必须比较 tip/hash、累计交易数、逻辑表 census
   和可重启性。

### 值得做 rBTC 专项基准，不应现在照搬

- rBTC 的累计 overlay 已用一次 `get_many` 读取外部 prevout，并排除批内新建后又花掉
  的 output；这已经覆盖了 btcd cache-hit 快路所解决的主要数据库探测问题。
- rBTC 的 `UtxoOverlay` 仍是单个 `Mutex<AHashMap>`，但 UTXO 状态转换目前保持串行。
  只有 profile 显示 map probe/锁是 `validate` 的主要成本时，才值得做“按 outpoint
  分片、同 shard 保序”的 A/B。否则 64 shard 会增加内存和复杂度而不产生并行收益。
- rBTC 的 `encode_mdbx_block_undo` 当前串行编码。可以另做 512/1024 spends 阈值基准，
  但必须像 btcd 一样对阈值两侧及最大真实块做 byte-for-byte 等价测试，并报告分配量、
  峰值 RSS 和 `commit` 时间；不能只报整轮 blocks/s。
- Go 的 `runtime.LockOSThread`、GC 百分比和 ffldb 512 MB metadata cache 属于 Go/适配层
  细节。rBTC 不应移植这些旋钮。

## 3. 为什么必须由持有全量语料的机器执行

Windows 证据机已有 771 GB、1,387 个 `.fdb` 的 mainnet corpus；Mac 没有该语料。
把语料复制到 Mac 既额外占用约 771 GB，也改变磁盘、缓存和平台变量，没有审计收益。

规则：

- source corpus 只读，不复制、不重命名、不让回放器写入；
- 数据库输出和原始日志都放在仓库外的专用 benchmark 根目录；
- Mac 只接收文本报告、JSON、SHA-256 和必要的小型 profile，不接收 `.fdb`、MDBX
  数据文件或数据库压缩包；
- 全量 lane 串行执行并复用同一输出盘，避免同时保留两个 120+ GB chainstate；
- 每个 lane 的 census、重启验证、证据哈希完成并由第二人确认后，才允许删除该 lane。

## 4. 被指派研发的任务

### 4.1 先补“测量而不改行为”的输出

当前 `cmd/replayblocks` 的 `elapsed` 在 UTXO flush 前停止，而且只输出 blocks/s。
先提交一个独立测量补丁，不得改变验证 flags、cache、worker、数据库写入或 flush 行为。

至少新增下列字段，并写入机器可读 JSON：

```text
schema_version
benchmark_revision
measurement_revision
source_file_count/source_logical_bytes/source_manifest_sha256
start_height/start_hash/start_total_transactions
final_height/final_hash/final_total_transactions
blocks_replayed/transactions_replayed
fastadd_checkpoint_height/fastadd_checkpoint_hash
fastadd_blocks/fastadd_transactions/fastadd_elapsed_seconds
full_validation_blocks/full_validation_transactions/full_validation_elapsed_seconds
connect_elapsed_seconds/flush_elapsed_seconds/durable_elapsed_seconds/process_wall_seconds
average_connect_tps/average_durable_tps/average_blocks_per_second
utxo_cache_mib/parse_workers/utxo_workers/depth/gogc/memlimit_gib/sigcache
metadata_bytes/total_database_bytes/peak_rss_bytes/exit_code
```

计数直接使用 `blockchain.BestState.TotalTxns`，不要另写容易漏计的交易解析器：

```text
transactions_replayed = final.TotalTxns - start.TotalTxns
average_connect_tps = transactions_replayed / connect_elapsed_seconds
average_durable_tps = transactions_replayed / durable_elapsed_seconds
durable_elapsed = connect 开始至 FlushUtxoCache + DB Close 成功
```

`average_durable_tps` 是本任务的主平均 TPS。它表示“历史交易/回放墙钟秒”，包括
coinbase，但不是 Bitcoin 网络实时 TPS 或容量上限。`average_connect_tps` 只作诊断。
恢复运行只能报告本次 delta；不能把恢复段的 TPS 冒充从 genesis 开始的全量 TPS。

测量补丁必须分别叠在基线和候选上，且两边内容一致。保存底层 revision 和测量
revision；不得只记录 dirty worktree。

### 4.2 固定两个可比 lane

| lane | revision | 目的 |
|---|---|---|
| A | `2d036b154a28a863abcb22d946f22853758506f4` + 同一测量补丁 | 已修 checkpoint/验证/预哈希，但没有分片 UTXO 应用和 journal 快路 |
| B | `fa64adfa76fb7163a4aa5d5cfeac8ccfca36eb01` + 同一测量补丁 | 当前功能候选 |

不能用 `56cc436d` 作性能基线：它没有把 checkpoints 传进 chain，验证语义不同。
`b135528b` 只改 `.gitignore`，性能代码与 lane B 相同。

两个 lane 的固定参数：

```text
--fastadd=true
--utxocache=24576
--workers=8
--utxoworkers=12
--depth=1024
--gogc=400
--memlimit=112
--sigcache=1000000
--report=30
--maxheight=0
```

若 125 GiB 证据机的可用内存或 CPU 拓扑已变化，停止并记录，不要悄悄调参。

### 4.3 运行前证据

在 PowerShell 中建立仓库外的专用目录；路径由执行者填写为明确的绝对路径：

```powershell
$Source = 'D:\bitcoin-corpus\blocks'
$BenchRoot = 'E:\rbtc-bench\btcdmdbx-full-replay-20260820'
$Artifacts = Join-Path $BenchRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $Artifacts | Out-Null

git rev-parse HEAD | Tee-Object "$Artifacts\git-head.txt"
git status --porcelain=v1 | Tee-Object "$Artifacts\git-status.txt"
go version | Tee-Object "$Artifacts\go-version.txt"
Get-CimInstance -ClassName Win32_OperatingSystem |
  Format-List * | Out-File "$Artifacts\host.txt"
Get-CimInstance -ClassName Win32_ComputerSystem |
  Format-List * | Out-File "$Artifacts\host.txt" -Append
Get-Volume | Format-Table -AutoSize | Out-File "$Artifacts\volumes.txt"
```

另生成 source manifest：按路径排序，记录每个 `.fdb` 的相对路径和字节数，再对 manifest
做 SHA-256。若完整逐文件哈希已存在则复用并核对；不要为了形式重复读取 771 GB。
记录当前文件数、总字节数、最后修改时间以及既有 manifest 的来源。

输出盘最低要求：单 lane 开始前至少 **200 GiB 可用**，并保留系统盘安全余量。
source 与 destination 必须不同，destination 必须位于 `$BenchRoot` 下且开始时不存在。

### 4.4 全量执行顺序

先跑 lane B，先交付当前代码的答案；再跑 lane A 做归因。每次都从空 destination
开始，不得用旧 chainstate 作为主结果。命令形态如下：

```powershell
$Binary = Join-Path $Artifacts 'replayblocks-lane-b.exe'
$ReplayDst = Join-Path $BenchRoot 'lane-b-db'
$Log = Join-Path $Artifacts 'lane-b.log'
$Json = Join-Path $Artifacts 'lane-b.json'

Get-FileHash -Algorithm SHA256 $Binary |
  Format-List | Out-File "$Artifacts\lane-b-binary-sha256.txt"

$Clock = [Diagnostics.Stopwatch]::StartNew()
& $Binary --src $Source --dst $ReplayDst --fastadd=true `
  --utxocache=24576 --workers=8 --utxoworkers=12 --depth=1024 `
  --gogc=400 --memlimit=112 --sigcache=1000000 --report=30 `
  --maxheight=0 --log=$Log --json=$Json
$Exit = $LASTEXITCODE
$Clock.Stop()
"exit_code=$Exit`nprocess_wall_seconds=$($Clock.Elapsed.TotalSeconds)" |
  Out-File "$Artifacts\lane-b-wrapper-result.txt"
if ($Exit -ne 0) { throw "lane B failed with exit code $Exit" }
```

`--json` 是 4.1 测量补丁应新增的参数。lane A 使用不同 binary、destination、log、
JSON，其他参数完全相同。运行期间每 60 秒采样进程 RSS、CPU 和目标卷读写计数；采样
脚本及其间隔也要入 artifacts。

禁止在两轮之间改变电源计划、Defender 排除项、source/destination 磁盘、Go 版本、
worker 数或其他工作负载。确需改变时本次 A/B 作废并重开两轮。机器睡眠、重启、手动
中断或恢复运行均标记为非主结果。

### 4.5 每个 lane 完成后的验证

完成一个 lane 后、删除数据库前，依次执行：

1. JSON 与日志均显示 exit code 0，最终高度、hash、`TotalTxns` 与另一个 lane 一致；
2. checkpoint 高度/hash 与 chain params 一致，且 900,000 以上计入完整验证区间；
3. UTXO cache flush 和 DB close 均成功；
4. 用 `--loadonly` 至少重开两次，分别记录首次和紧接着的第二次 load 时间；不能把
   第二次 page-cache 热启动冒充冷启动；
5. 运行完整逻辑 census，记录各 bucket 的 entry/key/value bytes 与 MDBX live/free/
   allocated/file bytes；全扫描，不得抽样；
6. 对 JSON、日志、census、host、manifest、binary 分别 SHA-256，再生成总
   `SHA256SUMS.txt`；
7. 由另一位研发核对 `SHA256SUMS.txt`、最终 hash/tx 数和报告表后签名确认。

主结果接受标准：

- lane B 从 genesis 数据库开始，一次完成到 corpus tip；
- 有精确 full elapsed 和 `average_durable_tps`，不得使用推算值；
- lane A/B 最终共识状态相同；
- 无超过 10 分钟且 CPU、磁盘、进度同时不增长的未解释停顿；
- 报告分开列出 connect、flush、process wall、checkpoint 前/后，不混成一个数字。

如果 lane A 超时或失败，保留失败日志、最后高度、最后交易数和资源采样；不得用该段
速率线性外推“预计全量时间”。lane B 的完整结果仍可独立验收，但优化倍数记为未定。

## 5. 节省磁盘的清理规则

数据库只能在第 4.5 节全部完成、证据已复制到两个位置且复核人明确确认后删除。
执行删除前必须打印并人工核对 source 与 target：

```powershell
$ResolvedSource = (Resolve-Path -LiteralPath $Source).Path
$ResolvedTarget = (Resolve-Path -LiteralPath $ReplayDst).Path
$ResolvedRoot = (Resolve-Path -LiteralPath $BenchRoot).Path

if ($ResolvedTarget -eq $ResolvedSource) { throw 'target equals source' }
if (-not $ResolvedTarget.StartsWith($ResolvedRoot + '\')) {
  throw 'target is outside dedicated benchmark root'
}
if (-not (Test-Path -LiteralPath (Join-Path $ResolvedTarget 'metadata'))) {
  throw 'target does not look like a replay database'
}

Write-Host "READ-ONLY SOURCE: $ResolvedSource"
Write-Host "DELETE TARGET:    $ResolvedTarget"
```

人工确认后才执行对这个明确路径的删除。不得使用 glob、环境变量展开后的空路径、盘符
根目录、仓库根目录或 source 的父目录。删除不可从 Git 恢复，必须在最终报告写明删除
时间、目标绝对路径及已保留的证据位置。

lane B 完成并验收后可删除其数据库再跑 lane A；这样额外空间峰值约为单个 MDBX
chainstate，而不是两份。最终只把下列小文件交回 rBTC：

```text
report.md
lane-a.json / lane-b.json
lane-a.log / lane-b.log（允许 zstd 压缩）
host.txt / volumes.txt / go-version.txt
source-manifest.txt 或其既有文件引用 + SHA-256
resource-samples.csv（允许 zstd 压缩）
census JSON
binary/revision hashes
SHA256SUMS.txt
```

不要交回 corpus、数据库目录、MDBX data file、完整内存 dump 或无界 CPU profile。

## 6. 最终报告必须回答的问题

1. 当前功能候选回放全部 Bitcoin mainnet corpus 的 connect、durable 和进程总时间各是
   多少？
2. 精确处理多少 blocks 和 transactions？平均 durable TPS 是多少？
3. checkpoint 前 fast-add 与 checkpoint 后完整验证各自处理多少交易、耗时多少？
4. lane B 相对 lane A 的总时间、TPS、峰值 RSS、数据库大小变化是多少？
5. 两 lane 的最终 tip/hash/TotalTxns 和逻辑 census 是否一致？
6. 速度变化来自 UTXO cache 并行、journal 编码，还是仍由验证/存储/flush 的某一阶段
   主导？没有 profile 证据时写“未定”，不要猜测。

在这份报告返回前，rBTC 可以借鉴正确性规则和测量方法；不能声称已经获得新版本的
“全量回放时间”或“平均 TPS”。
