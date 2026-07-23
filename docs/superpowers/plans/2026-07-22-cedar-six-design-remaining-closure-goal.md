# Cedar 六份设计剩余收口 Goal

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在保持 clean-break、内部数据库格式号为 1、无外部 V2/Vn 命名和无旧布局兼容的前提下，完成六份权威设计尚未闭合的功能、约束、回归和 release/paper 证据。

**当前基线（2026-07-22）：** bundled LZ4/Zstd、启动自检、Float XOR、
SST self-description、checked metadata bounds、identity-scoped cache、
持久化 block/page BLAKE3 commitment 以及 Manifest-owned schema catalog
接入之后，normal 完整矩阵通过 774/774；schema/Manifest focused 子集在
ASAN、UBSAN、TSAN 下各通过 49/49，fresh no-Zstd focused 通过 4/4。
完整 sanitizer
矩阵仍需在最终统一验证阶段重新执行。因此这些数字只表示当前回归证据，不等于
六份设计的 release/paper gate 已经完成。

## 未完成项

### 1. Columnar 功能与发布证据闭合

- page codec 已补齐 frame-of-reference、Bool bitmap、整数 bit-packing、delta-of-delta、Float32/Float64 XOR 和 Zstd；整数/Bool codec 已接入 Granule 候选比较。浮点 inline values 现使用定宽原始位流，生产 Granule 会请求 XOR 并由 page codec 的 12.5% 收益门槛确定是否保留，完整与 selective SST 读取共享 typed 解码；当前 Granule/page-directory identity 为 `GBK5/5` 与 `PDR3`，每个 encoded page 都有持久化 BLAKE3 commitment，旧 block 布局不兼容。LZ4 1.10.0、Zstd 1.5.7 已固定源码、license 与 release archive SHA-256 并默认静态构建，数据库启动在任何持久状态恢复前执行有界 round-trip 自检，运行时不联网安装。仍需补齐 codec capability 的正式 release artifact。
- 让 codec 选择真正消费 typed statistics，并在 page/SST metadata 中可解释地记录选择结果。
- SchemaRegistry 的 schema epoch/identity 已纳入 clean-break `MSC1`
  VersionSet/Manifest edit：catalog 编解码、连续唯一 epoch、live SST/index
  引用、physical type、index-fragment/source partition 绑定、generation CAS、
  256 MiB recovery/encoding bound、精确 rewrite admission、post-rename
  indeterminate mutation gate 和 reopen 恢复均已有回归；schema DDL 不得借用
  prepared-transaction completion reserve。独立 `SCH1`/`manifest/SCHEMA`
  runtime 已删除。
  剩余工作仅是把该证据纳入正式 release artifact 和最终全量 sanitizer gate。
- SST header/footer、bounded statistics region、重复 file identity、稳定算法
  ID、checked footer ranges、固定长度 hash-bearing BlockIndex、identity-scoped
  cache 及 Manifest ownership 已完成；剩余工作是把这些证据纳入正式
  release artifact。
- granule 的 Inline/Blob presence bitmap、rank 访问和 ValueClass 一致性校验已经实现；仍需把该格式证据纳入 release artifact，并验证 selective/read-boundary 统计。
- Blob 写入已经使用约 1 MiB `CBB1` indexed block、oversized dedicated block
  和 64-bit directory；事务批量写、重开校验、精确写入估算、fault orphan、
  stale hint 以及 GC 共用 writer 已有正常与 focused sanitizer 回归。剩余仅是
  将这些证据纳入正式 release artifact。
- 对 `ReadSst`/`ReadSstFile` 的完整 materialize 行为作出明确离线 API 边界；若保留，必须记录内存/调用约束，不能让生产查询依赖它。

### 2. HTAP / Resource Scheduling 闭合（端到端证据缺口）

- 逐条证明 flush、compaction、index/statistics、Blob-GC、query、commit-critical 等生产任务都经过 typed admission、bounded grant、计量、取消和 shutdown release。
- 增加队列状态重启可重建、prepared transaction 不被压力阻塞、冻结 MemTable/sidecar/Blob/compaction 引用安全以及跨并发确定性测试。
- 生成 write-amplification、fsync latency、conflict-abort、visible-prefix stall、cache/IO/resource accounting 的可追溯 benchmark artifacts。

### 3. T-Cypher 支持矩阵闭合

- 让所有声明支持的 physical range/change candidate 都能进入 ExplainAnalyze；清除或收窄 `range/change physical runtime is not available yet` 的宽泛分支。
- 对 mixed variable-length + fixed relationship、多跳 node/relationship property projection、range/change projection 等形态逐一选择：实现 typed/vectorized/spill/cancellation/oracle 路径，或保留 deterministic `NotSupported` 并在 support matrix、negative tests、release wording 中明确标注。
- 保持 ExecuteTcypher 单入口，不恢复 materializing legacy executor/fallback。

### 4. Temporal Index/CBO 闭合

- graph-order 的 index-first 与 adjacency/Expand-first candidate、costing、预算、runtime 选择、plan fingerprint/validation 和 EXPLAIN 字段已经实现，并有两种选择、预算耗尽、selective root 与七 root greedy 回归。
- 仍需增加并归档 base/index/hybrid/intersection/graph-order 的 release/paper artifact，覆盖 partial coverage、repair/drop/reopen、runtime feedback 衰减和 resource accounting；不能只依赖 unit test。

### 5. Observability / Benchmark release closure

- 固定 corpus、dataset/workload/resource/durability/cache/open-loop/closed-loop/recovery/fault profiles。
- 将 `cedar_bench` 多次运行、manifest、summary、raw artifact、baseline/candidate paired gate 和离线 report regeneration 串成真正的 release driver。
- 每个 profile 至少五次有效重复，输出配对统计、置信区间、CV/回归阈值、baseline key、run_id、环境、资源配置和数据 hash。
- 归档 paper-scale artifacts，清理没有 artifact provenance 的性能声明。

### 6. 统一收口

- 运行 normal、ASAN、UBSAN、TSAN、fault/crash/reopen/oracle、scheduler/HTAP stress 和 benchmark reproducibility 全矩阵，统一使用 `-j1`。
- 更新六设计 completion matrix：每一项必须有代码/测试/artifact 证据，或有批准的 out-of-scope 决策和 tested failure contract。
- 不 reset、clean、stage、commit、push；不恢复旧运行时、旧 manifest、旧磁盘布局或外部 V2/Vn 名称。

## 当前 goal 的明确 backlog（按执行顺序）

1. **Columnar 格式闭合：** codec release capability artifact、typed-statistics 选择解释、
   SST metadata 与 presence bitmap release evidence、
   Blob block/index 与 schema/Manifest 原子生命周期的正式 release evidence，以及
   `ReadSst`/`ReadSstFile` API 边界。
2. **T-Cypher support matrix：** 逐项处理 range/change ExplainAnalyze、mixed
   variable+fixed relationship、多跳 node/relationship projection；实现则增加
   typed physical regression，否则增加 deterministic `NotSupported` negative test
   与 release wording。
3. **HTAP scheduler E2E：** 对每个生产任务建立 admission/grant/metrics/
   cancellation/shutdown 证据，补 queue restart reconstruction、prepared
   transaction pressure、冻结引用安全和并发确定性 stress。
4. **Benchmark/release driver：** 固定 corpus/profile，真实执行 baseline/candidate
   paired 多轮（每 profile 至少 5 次有效重复），归档 raw artifact、manifest、
   environment/resource/durability/cache provenance，生成可离线重建的 report、
   confidence interval 和 regression gate。
5. **统一验证：** 在上述功能变更之后，以 `-j1` 刷新 normal/ASAN/UBSAN/TSAN、
   fault/crash/reopen/oracle、scheduler/HTAP stress 和 benchmark reproducibility，
   并把命令输出与 artifact ID 回填 completion matrix。

## 完成判据

只有当上述功能缺口已实现或明确批准为 out-of-scope，且所有 release/paper 数字都能追溯到 manifest、artifact、环境和统计方法时，当前 active goal 才可标记完成。
