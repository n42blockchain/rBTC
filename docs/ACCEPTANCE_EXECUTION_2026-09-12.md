# 六项剩余门槛：执行条件、Mac 与耗时

2026-09-12。六项均不能因本次脚本改进而标记关闭。前三项仍缺生产实现；
后三项缺完整规模、不可变语料或完整运行证据。本文把可执行的存储长跑入口
补齐，并明确执行顺序和时间边界。

后续审计补充：用户指出 `/data` 独立数据卷。实际检查为 `/dev/nvme1n1p1`，
总容量约 7.0 TiB、可用约 **2.9 TiB**；对 `/data/bench/rbtc-mdbx-full`
的只读容量预检已通过。下面关于约 197 GiB 的记录仅指 home/workspace 卷，
不能再据此认定本机缺少大盘。本次沙箱只允许写 workspace 和 `/tmp`，
对 `/data` 仍是只读，因此未在该卷启动长跑或清理已有数据。

## 哪些可以放在 Mac

可以。项目已有 [M1 Max / 64 GB / APFS 的实测](LOCAL_STORAGE_PRESSURE_2026-08-24.md)，
包括存储比较、紧容量下的 64/256 churn、compact-copy 崩溃恢复；
[公网验收程序](PUBLIC_NETWORK_SOAK.md)也曾在 Mac 上启动。这证明原生运行路径
可行，不代表当前修改已在 macOS 实测，更不代表 160M 或七天验收已通过。
Apple Silicon 是 Rust 的[受支持原生平台](https://doc.rust-lang.org/rustc/platform-support.html)。

若仍使用上述 64 GB Mac，可先做小规模兼容性检查，再在足够空间的本地持久化
SSD 上执行完整规模。64 GB 是已有实测配置，不是本项目已证明的全量最低内存要求；
实际峰值、swap、内存压力都需要采集。Mac 上的数据与性能证据必须标明芯片、内存、
macOS、APFS 和存储接口，不能和 Linux/ext4 的时间直接做引擎 A/B。

专用存储卷建议预留 **500 GiB 可用空间**（工程余量，不是实测占用）：
现行双通道保留数据库，规划下限为 `2 × 128 + 1.1 × 128 + 16 = 412.8 GiB`，
涵盖两个上限、同时存在的一份 compact copy 及余量、16 GiB 存储保留空间。
数据库不会立即分配到上限。新入口在写盘和编译前检查此条件；续跑扣除已经
分配给两条数据库的物理空间，日志和其他文件不抵扣。APFS 的可用空间仍可能
被同容器其他卷占用，因此预检不是运行期间的空间保证。

此预算**不含**主网语料、匹配的起始快照、redb/MDBX 回放输出和公网节点数据。
历史任务书中的约 771 GB 语料属于指定历史数据集，不能当成当前完整主网大小。
如果语料仍在 Windows 证据机，优先在该机完成匹配的两引擎回放；Mac 可承担
生成式存储测试或公网长跑，不必为它们复制整套语料。

## 完整运行需要多久

| 项目 | 时间边界和估算方法 |
| --- | --- |
| 完整优化器 | 仍须实现有工作预算的搜索、旧顺序复用及对应差分验收。研发时间另算；重跑当前两遍 refinement 不能关闭。 |
| 准入总预算 | 仍须覆盖所有准入阶段及跨 peer / chain-change 的累计预算。现有小规模 profile 不是限额实现，也不能估计完成研发的时间。 |
| 分叉头保留恢复 | 仍须实现双入口预留、资源延后、强分叉重新获取、持久化淘汰和有界重开。不能用简单丢弃所有分叉替代恢复。 |
| 160M / 900k × 两通道 | `T = seed64 + 900000/r64 + audit64 + seed256 + 900000/r256 + audit256 + recovery`。速率须来自同一 Mac、160M live set 的检查点，并包含周期性维护成本。 |
| 冷盘 / 主网回放 | 语料准备、两引擎串行回放、冷盘准备、完整内容审计及重启检查之和。当前缺匹配语料和初态，尚无可信的全量小时数。 |
| 七天公网验收 | 两网络追平后的 **604,800 秒 / 168 小时**，再完成报告；构建、同步、数据准备另算。演练在一天后进行，不会把七天缩短。非计划长停机不能靠旧开始时间继续计入。 |

仅作为算术示例：两条通道若各稳定达到 100、20、5 transitions/s，
180 万次 churn 分别需要 **5、25、100 小时**，另加初始化和最终审计。
这些不是 Mac 全量测速或承诺。已有 2M 的约 72–86 transitions/s 不应直接
套到 160M。已有真实 28,350 块窗口的 790 秒也不能外推 genesis-to-tip。

因此当前只能承诺验收的日历下限超过七天，不能声称“七天内六项全关”。
不同机器可以在代码冻结后并行执行，关键路径是最长的一项；同一 Mac 上
先串行做存储/冷盘性能测量，再做公网长跑，避免把相互争用的结果当成可比基准。

## 存储执行入口

依赖：项目锁定的 Rust 工具链及 C/C++ 构建依赖、Python 3、Git、系统
`/usr/bin/time`。新 runner 不需要 jq，不调用 GNU 专属 `date` / `timeout`；
Linux 使用 `time -v`（RSS 为 KiB），macOS 使用 `time -l`（RSS 为 bytes）。
通过 Cargo JSON 获取可执行文件路径，支持自定义 `CARGO_TARGET_DIR`。

在项目根目录运行。以下 `/Volumes/RBTC` 是需要替换的已挂载持久化测试卷，
输出目录必须全新；不要把目录名当作自动挂载命令。

```sh
# 只读检查；不会建立目录、构建或启动长跑。
contrib/run_mdbx_replacement_gate.sh /Volumes/RBTC/storage-full --preflight

# 独立的小规模兼容性检查：不计入完整规模证据。
RBTC_MDBX_GATE_UTXOS=20000 \
RBTC_MDBX_GATE_BLOCKS=512 \
RBTC_MDBX_GATE_UPDATES=100 \
RBTC_MDBX_GATE_CAPACITY_BYTES=1073741824 \
RBTC_MDBX_GATE_REPORT_INTERVAL=128 \
contrib/run_mdbx_replacement_gate.sh /Volumes/RBTC/storage-smoke

# 默认就是 160M / 900,000 / 5,000，64 与 256 串行。
# Mac 在插电、防止自动睡眠的状态运行；终端任务应使用持久会话托管。
contrib/run_mdbx_replacement_gate.sh /Volumes/RBTC/storage-full

# 读取已有检查点；同 live set 的近期速率估算，不混入 seed 时间。
contrib/run_mdbx_replacement_gate.sh /Volumes/RBTC/storage-full --status

# 中断后使用原来的冻结二进制和参数；不重新编译。
contrib/run_mdbx_replacement_gate.sh /Volumes/RBTC/storage-full --resume
```

Mac 可按 [Apple 的睡眠设置说明](https://support.apple.com/en-ie/guide/mac-help/mchle41a6ccd/mac)
开启插电时防止自动睡眠；关显示器与系统睡眠是不同状态。要保证电源、网络和
测试卷持续可用。此 runner 自身不安装后台服务，也不负责合盖、重启后的自动启动。

所有 `RBTC_MDBX_GATE_*` 工作负载参数在首次构建后存入 `run.json`；续跑默认
继承这些值，环境中不同的值会被拒绝。矩阵入口自行管理 DIR、REPORT、COMMIT_BATCH，
不能从外部覆盖。直接绕过入口手工改变数据库、参数或冻结文件，会破坏这套证据。

每次尝试独立保留 `attempt-NNN/{report.json,test.log,time.txt,attempt.json}`；
SIGINT / SIGTERM 会结束本次测试进程组，数据库按已有原子提交边界恢复。
目录锁防止同一输出同时启动两套 runner，子进程也持有锁，父进程意外消失时
不能立即绕过锁再开一套。不同输出目录可以运行不同实验，操作者仍应避免并发压盘。

构建时冻结 scale/crash 两个可执行文件，记录 SHA-256、tracked 和 untracked
源码哈希、版本、构建工具及文件系统；计时阶段直接运行测试二进制。
runner 执行五个 compact-copy 崩溃边界、每条 lane 的独立进程重开审计、两 lane
最终内容 digest 比较。`matrix.json` 标记是否完整默认工作负载，以及 RSS 是否
超过 1.5；超过时保留报告但返回非零，不偷偷调小批次。

`--status` 在至少两个有效 churn 检查点后才给剩余 churn 时间。它不预测
未开始的第二条 lane、未来维护变化和最后全表审计；续跑后的峰值汇总包含旧尝试，
每次尝试的报告仍独立保留。单次 seed 中断的初始化耗时不能从最后一次报告冒充恢复。

审计后的续跑汇总只把实际 seed/churn 尝试用于 RSS 比值；纯审计重开不能
抬高 64 lane 的分母，把原本超限的 256 lane 变成通过。每次启动会保存旧
矩阵并写独立执行状态，失败时不留下冒充本次结果的旧 `matrix.json`。
清理一次性数据库后保留 `retired.json`，禁止把已清理的实验当作原运行续跑。

这仍是测量入口，**不会自动发布完整 MDBX replacement gate 通过结论**。
完整存储报告还需要按 [MDBX 门槛](MDBX_REPLACEMENT_GATE.md)审阅高水位、
维护频率、同时占盘量、峰值 RSS、恢复和 canonical content，并完成独立真实回放。

## 前三项实现顺序与关闭证据

1. 完整优化器：以本地 pinned Core 31 `SpanningForestState` / `Linearize`
   为参考；端到端加入搜索预算、旧顺序重用和最优/未收敛结果。
   验收要覆盖不同预算、旧顺序有效/失效、同费率与一般 DAG 的差分，不能只比较
   `PostLinearize`。随后验证生产替换、淘汰和相同占用区间的 fee-floor。
2. 准入总预算：为 payload、metadata、prevout lookup/hash、脚本、图处理、
   relay/persistence snapshots 统一记账；预算由节点共享，拒绝请求和重复链变更
   也消耗预算。耗尽须延期/背压，保留新上下文重验证和候选原子发布。
   先独立压测，再把优化器消耗计入该预算。单个函数的常量上限不能替代全流水线限额。
3. 分叉头：先定义资源延期结果和可恢复的候选状态，再覆盖 peer 与本地区块
   两条入口及持久化淘汰；重开时也要有界加载。以被淘汰的分叉后来更强为核心
   反例，验证重新获取、难度/MTP/deployment 上下文和执行回滚。
   最终用多消息/重连/本地提交、取消/写盘失败、崩溃重开证明 RSS/磁盘平台期。

以上三项本次没有生产实现变更，仍分别按
[优化器](UPSTREAM_CLUSTER_POSTLINEARIZE_GATE.md)、
[准入](UPSTREAM_ADMISSION_RESOURCE_GATE.md)、
[分叉头](UPSTREAM_HEADER_RESOURCE_GATE.md)保留未关闭状态。
完成改造和相关回归后冻结版本，再开始该版本的正式七天验收。

## 本次验证与未启动工作

公网 finalizer 也修复了一处实际误报：旧脚本会接受只有两条进程样本的稀疏
时间线。新的负例在旧版本上复现误接收，当前版本拒绝；验收现在要求两网络
的采样覆盖开始到结束，开始时已追平，拒绝陈旧/乱序/窗口外样本和失败演练。
只有有起止记录、PID 变更一致且不超过一小时的受控重启可以解释进程采样缺口。
新脚本内嵌 Python 3 校验，必须随新验收运行一起冻结。正常缩短窗口的 fixture
及受控重启 fixture 通过，但缩短窗口不能以 604,800 秒门槛通过。

Linux 下 6 项 runner 验证通过：RSS 单位及缺测拒绝、续跑继承/参数变更拒绝、
缺失或不匹配结果拒绝、只读容量预检、排除 seed 的 ETA、零选中测试拒绝。
真实 release 二进制试跑覆盖两 lane、五处 compact-copy 崩溃边界及独立进程重开。
另一次 20k-live / 10,000-transition 测试在首个检查点后主动 Ctrl+C（exit 130），
并验证重复 runner 被拒绝、锁释放、参数变更被拒绝、旧报告原样保留及续跑内容一致。
该小规模续跑矩阵 RSS 比约 1.96，正确报告需内存预算审查，exit 1；不算存储门槛通过。

本机完整规模预检：可用约 196.7 GiB，小于 412.8 GiB 规划要求，未启动全量。
没有已连接的 Mac 执行环境、匹配主网语料或可恢复的完整七天报告；本次未在 Mac
运行新入口、未执行全主网回放、未启动七天验收。
证据保留在 `target/upstream-followup/2026-09-12/`，包括
`storage-runner-smoke/`、`storage-runner-interrupt/`、`runner-integration.json`。
