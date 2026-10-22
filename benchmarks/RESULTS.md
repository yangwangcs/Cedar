# Cedar 0.1 存储层基准（P0 基线）

> 对应计划 Task 0.1（P0 基线）与设计文档第 5 节“性能模型与目标”。
> 本文件只记录**基线**；Phase 5 的 before/after 对比追加在本文件末尾（Task 5.2）。

## 1. 环境与构建

| 项 | 值 |
|---|---|
| 日期 | 2026-10-22 |
| 工作树 / 分支 | /Users/wangyang/Desktop/分布式/Cedar-0.1 · 0.1 |
| commit | `22364f4dc4e0`（基线提交，随后由各任务追加） |
| 机器 | wangyangdeMac-mini.local（Apple Silicon，arm64，8 线程，16 GB） |
| 系统 | macOS kernel 27.0.0（AppleClang 21.0.0.21000334，SDK 头文件经 `xcrun` 注入） |
| 构建 | CMake 4.2.1，`-DCMAKE_BUILD_TYPE=Release`，Makefile generator，`cmake --build build -j8` |
| 依赖 | pkg-config 2.5.1；lz4 1.10.0；gRPC 1.78.1；protobuf/libprotoc 34.1；GoogleTest 1.17.0（Homebrew） |

构建与运行命令：

    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
    cmake --build build -j8
    ./build/benchmarks/storage_bench --json > benchmarks/baseline.json   # 基线
    ./build/benchmarks/storage_bench                                      # 缩进可读 JSON
    ./build/benchmarks/storage_bench --rows=1000000 --memtable-rows=1000000 \
        --merge-files=32 --merge-rows=31250                               # 100 万行口径

可选参数：`--rows=N`（VSL 每个 case 总行数）、`--memtable-rows=N`、`--merge-files=N`、
`--merge-rows=N`。默认参数即基线口径；bench 单次运行约 0.3 s（远低于 5 分钟门限）。

## 2. 基线数据（`benchmarks/baseline.json`）

单次运行（同机重复 3 次的波动范围见括号）。所有吞吐/延迟均由 `std::chrono::steady_clock`
测量，延迟样本取 p50/p99。

| Case | 参数 | 关键指标 |
|---|---|---|
| `vsl_insert_1t` | 1 线程 × 100,000 行 | 12.41 M ops/s（11.8–12.4 M），80.6 ns/op，traversed=100000 |
| `vsl_insert_8t` | 8 线程 × 100,000 行 | 9.81 M ops/s（9.8–11.3 M），102.0 ns/op，**traversed=97,827（丢 2,173 行）** |
| `memtable_getattime` | 100,000 行 / 20,000 次查询 | p50 250 ns，p99 1,375 ns，mean 347.5 ns，2.88 M ops/s，hits=20000 |
| `sst_write_read_1` | 1 行 SST | 文件 590 B；**get_hits=0（点查全部失败）**；scan 1 行 |
| `sst_write_read_8192` | 8,192 行 SST | 文件 157,243 B；写 1.71 M rows/s；点读 p50 208 ns / p99 10.0 µs；扫描 374 M rows/s |
| `sst_write_read_8193` | 8,193 行 SST（跨 block） | 文件 157,282 B；写 1.99 M rows/s；点读 p50 250 ns / p99 13.5 µs；扫描 364 M rows/s |
| `compaction_merge_32way` | 32 文件 × 2,000 行 = 64,000 行 | **156.1 ms**（154.8–156.1 ms），409,936 rows/s，输出 1 文件 64,000 行 |

说明：

- `vsl_insert_8t` 的 `traversed_rows` 由 `LockFreeVSL::Traverse` 实际计数，是**正确性信号**，
  不是吞吐指标。当前并发插入不是 CAS（审计 1.1），多次运行会丢 0.02%–40% 的节点；
  样本：99,973 / 99,496 / 97,827 / 820。
- `sst_write_read_1` 的 `get_hits=0` 是审计 10.1 的直接量化：单行 SST 的时间戳解码为空，
  `Get/GetAtTime/GetRange` 全部失败。修复后该值必须为 1。
- 8,192 → 8,193 行的 p99 从 10.0 µs 升到 13.5 µs，说明跨 block 后 target/entity 的
  惰性索引路径存在额外成本（审计 2.1/2.9/3.2 的观察入口）。
- 32 路合并 = 64,000 行 / 156 ms。当前 `MergeZones` 每行线性找最小（O(N×K)，审计 4.1），
  Phase 4 的 ≥5x 目标以此为分子。
- **flush 吞吐基线不在 Task 0.1 的 4 个 case 内**，由 Task 4.5（flush 分组与分配优化）补测，
  届时同样写回本文件。

## 3. 与目标（设计文档 `5 / 计划 Phase 5）的映射

| 目标 | 基线口径 | 当前值 |
|---|---|---|
| VSL 8 线程插入 ≥3x | 8t vs 1t ops/s | 0.79x（并发未兑现，审计 1.1/1.8） |
| SST 点读 p99 ≥5x | 8,193 行 SST p99 | 13.5 µs |
| 打开 100MB SST 峰值 RSS −50% | 本 bench 暂未采集 RSS（Phase 3 补） | — |
| 32 路合并 ≥5x | 64k 行 merge ms | 156.1 ms |
| flush 吞吐 ≥1.25x | Task 4.5 补测 | — |
| 全量扫描 O(n²)→O(n) | 8,193 行扫描吞吐 | 364 M rows/s（小文件线性；1M 行口径用 `--rows` 复测） |

## 4. 复现与对比约定

- 同机、同编译参数、同 commit 前缀；每次改动前后各跑 3 次，取中位数；波动 >5% 视为回归
  （计划 Global Constraints）。
- `baseline.json` 是机器可读基线（schema `cedar-storage-bench/v1`，含 commit/compiler/
  机器/hardware_threads 元数据），Phase 5 的对比脚本按 `case.name` 对齐。
- 基准代码：`benchmarks/storage_bench.cc`；构建：`benchmarks/CMakeLists.txt`（`BUILD_BENCHMARKS`，默认 OFF）。

## 5. Task 1.4-1.6（列式格式 / target 贯穿 / 批量边）后对比

> 本节由 t3（columnar）追加，与第 2 节 baseline 同一机器/编译参数；
> 构建：`cmake -S . -B <dir> -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON`。
> **注意**：测量期间同机还有其它 agent 的构建/测试并行运行，数值波动明显大于第 4 节约定的
> 5% 门限；下表给 5 次运行的中位数与 (min,max)。Phase 5（t8）应在机器空闲时复测后替换。

| case / 指标 | baseline | 1.4-1.6 后（中位数） | 说明 |
|---|---|---|---|
| `sst_write_read_1.get_hits` | 0 | **16 / 16** | 审计 10.1 修复：单行 SST 的 Get/GetAtTime/GetRange/Iterator 全部可读 |
| `sst_write_read_8192.get_p50_ns` | 167 | 292 (291–500) | 点查改为完整 key 匹配（target + sequence + flags），多读 zone2/zone3 |
| `sst_write_read_8192.get_p99_ns` | 10,125 | 25,666 (10,708–38,792) | 同上 + 并行负载噪声；t6（P3 列式性能）以此为新起点 |
| `sst_write_read_8192.open_ms` | 0.110 | 0.176 (0.131–0.227) | Open 时一次性展开 target（O(n)，替代旧 Iterator 的 O(n²) 逐行扫描） |
| `sst_write_read_8192.scan_rows_per_sec` | 372.4 M | 353.0 M (337.8–356.2 M) | 扫描只读 value；−5% 在噪声范围内，t6 复测 |
| `sst_write_read_8193.get_p50_ns / p99_ns` | 250 / 12,875 | 250 / 12,500 | 跨 block 边界不再错位（审计 2.1） |
| `sst_write_read_8193.open_ms` | 0.105 | 0.120 | 文件级 target 解码 |
| `compaction_merge_32way.merge_ms` | 155.76 | 101.82 (98.1–112.8) | 合并路径经 `Iterator::Key()` 重建 key；target 解码由 O(n²) 降到 O(n)（审计 10.9），顺带收益 |
| `vsl_insert_1t / 8t.ops_per_sec` | 10.84 M / 11.41 M（8t traversed=23） | 12.93 M / 15.17 M（8t traversed=100000） | 属 t5（VSL）；这里只记录同树状态，1.4-1.6 未改跳表 |
| `memtable_getattime.p50 / p99_ns` | 292 / 1,750 | 292 / 1,666 | 属 t5；1.4-1.6 只给 VSLMemTable 增加只增不改的 target 重载 |

结论：
- 正确性信号（`get_hits`、`traversed_rows`、跨 block target）已全部转为"修复态"。
- 1.4 的 target 文件级解码把"每行 O(idx)"变成"每文件 O(n) 一次展开"，Open 增加
  0.01–0.12 ms（8192 行），换来 Iterator/GetAll/合并路径的线性化；点查 p50 的上升来自
  完整 key 匹配（审计 2.4/2.5 的正确性要求），留给 t6 用块表/reader cache 优化摊薄。
- 旧格式（flags 未置位）走逐块解码，不改变上述基线；`LegacyTargetZoneFixtureReadable`
  覆盖单块/多块旧文件。

## 6. t9（直接写 WAL 化 + 组提交）后写路径对比

> 本节由 durability（t9）追加，同一机器/编译参数；测量期间同机仍有其它 agent 并行运行，
> 数值取 2 次运行（波动 <8%）。命令：
> `./build/benchmarks/storage_bench --json`（新增 case 见下）。
> **口径**：T2 版直接写不写 WAL（审计 3.3 后半）；事务提交在 T2 是"每次 append+sync（不组提交）"。

| case / 指标 | T2 基线（等价口径） | t9 后 | 说明 |
|---|---|---|---|
| `lsm_put_no_wal_1t.ops_per_sec` | 4.74–6.56 M（0.15–0.21 µs/op） | — | 参考线：直接写完全不写 WAL 时的上界 |
| `lsm_put_wal_flush_1t.ops_per_sec` | — | **345–381 K**（2.62–2.90 µs/op） | t9 默认：直接写 append + flush 到 OS（进程崩溃可恢复），WAL 引入约 2.5 µs/op |
| `lsm_put_wal_sync_1t.ops_per_sec` | — | 428–434（2.34 ms/op） | `SetDirectWriteSync(true)`：直接写也 fsync（macOS F_FULLFSYNC ≈ 2.3 ms） |
| `txn_commit_sync_1t.commits_per_sec` | 410–445 | 410–445 | 等价 T2 行为：`SetWALGroupCommitWindow(0)`，每提交一次 fsync |
| `txn_commit_group_1t.commits_per_sec` | — | 397（2.52 ms/commit） | 单线程无可合并对象，与 T2 同量级（差异在噪声内） |
| `txn_commit_sync_8t.commits_per_sec` | 428–450（每提交 1 fsync） | 428–450 | 对照：8 线程但组提交关闭时仍然串行 fsync |
| `txn_commit_group_8t.commits_per_sec` | — | **2,760–2,916**（≈ 6.5× 同步模式） | 组提交：`wal_records_per_sync ≈ 8.0`，8 线程共享同一次 fsync |

结论：
- 直接写现在具备"已提交"语义：调用返回即已 append 到 WAL 文件（默认 flush 到 OS，
  进程被 kill 后可重放；`SetDirectWriteSync(true)` 可提升为掉电可恢复）。
  代价是 WAL 引入约 2.5 µs/op（no-WAL 参考线 0.15 µs/op）——这是审计 3.3 后半
  "直接写不可恢复"的必修成本；批量导入可走 `BatchExecutor`（批量 record）或临时关闭 WAL。
- 组提交把并发事务提交从"每提交一次 fsync"恢复到 **~8 record/次 fsync**，
  8 线程吞吐 ≈6.5×；单线程事务与 T2 同量级（组提交窗口上限 50 µs，
  见 `WalOptions::kMaxAccumulateUs`）。
- 直接写默认不进组提交队列（kWrite 无 fsync 可共享，逐条 append+flush 最快）；
  事务提交（kSync）走组提交线程。两种记录写在同一条 WAL 流里，重放顺序一致。

## 7. t6（Phase 3 列式性能：mmap / 块表 / Bloom / EntityIndex / reader cache / K 路归并）

> 本节由 columnar（t6）追加，同一机器/编译参数；测量期间同机仍有其它 agent 并行运行，
> 数值为 2–3 次运行范围。命令：
> - `./build/tests/test_zone_columnar_roundtrip`（内置 Phase 3 目标断言，见输出 [perf] 行）
> - `./build/benchmarks/storage_bench --json`

| 目标（设计文档 §5） | 基线 | t6 后 | 结论 |
|---|---|---|---|
| SST 点读 p99 ≥5x | 10,125 ns（8192 行，T1 基线） | **167–500 ns**（p50 42–125 ns） | **≥20–60x**，且 Get 为完整 key 匹配 |
| 1M 行全量扫描 O(n²)→O(n)，≥10x | t3 后 9000 行 Key+Value ≈74 ms（≈8 µs/行，entity 解码 O(block) + 旧 target 逐行扫描） | **1,000,000 行 / 19–20 ms（≈20 ns/行）** | 相对 t3 代 ≈400x；旧实现按 O(n²) 推算为分钟级 |
| 打开 100MB SST 峰值 RSS −50% | 旧 reader：heap 整文件 + 5 份 zone 拷贝 ≈ 文件 2x（≈195MB） | **100.46MB 文件，Open RSS 增量 10.09MB** | **≈95% 下降**（mmap + Slice 视图 + 懒解码 + 持久 EntityIndex/Bloom 零拷贝视图） |
| >10 文件结果完整 | 旧 kMaxFilesToQuery=10 静默截断 | 15 文件全部返回；limit=5 返回最新 5 条 | 通过（RangeLimitAcrossManyFilesIsCompleteAndNewestFirst） |
| 并发 reader cache 不忙等 | 容量满且全部 pin 住时 while 死循环 | 16 线程 × 20 文件 × 容量 16 × 300 次查询 0 失败，Size <= Capacity | 通过（SstReaderCacheConcurrentQueries） |
| 32 路合并吞吐 | 155.8 ms（T1 基线）；101.8 ms（t3 后） | **33.6–42.7 ms** | 合并经 Iterator/GetAllEntries 重建完整 key，t6 的 O(1) entity 游标 + 直索引 value 继续收益 |
| SST 8192 行点读 p50 | 167 ns（T1） | 83–166 ns | 见上（p99 收益更大） |
| SST 8192 行全量扫描（只读 Value） | 372.4 M rows/s（T1） | 336–337 M rows/s | 差异在并行负载噪声内；该路径 t6 未增加逐行工作（entity 游标懒同步） |

实现要点（与数字对应）：
- ZoneReaderView：mmap(PROT_READ)，回退 pread；zone 全部为 mmap 上的 Slice，decoder 零拷贝。
- 懒解码：timestamp/target/entity 块表、Bloom、EntityIndex 全部 one-time（std::call_once），
  Open 只解析 header/footer/zone 边界（不再触碰整条 zone）→ 打开大文件 RSS 骤降。
- Entity：行对齐块表（修审计 2.9 的 run 跨块漏索引）+ 顺序 Cursor（迭代器每行 O(1)）；
  持久化紧凑 EntityIndex（12B/entity，二分，零拷贝视图），无索引旧文件回退线性扫描。
- Value：写端固定 kRaw → Get(idx) 直接 idx*8 索引，不再复制整列 Descriptor cache。
- Bloom：写端按真实 entity 集合分配（审计 2.10），读端零拷贝视图；不存在 entity 跳过率 >=90%。
- LsmEngine：每 (entity_type,column) 按 min_entity_id 排序 + 前缀最大 max_entity 的区间索引
  （O(log F + k) 选文件，compaction 完成自动刷新）；所有读路径统一 SstReaderCache
  （key=file_number，淘汰不忙等）；去掉 10/20/50 文件静默截断；GetRangeLimit 用 K 路归并
  + 墓碑/去重后截断；批量查询用所有 entity_ids 的文件并集；写入后失效跨查询缓存。

## 8. t10（WAL checkpoint / 截断：SST 先 fsync，再回收 WAL）

> 本节由 durability（t10）追加，同一机器/编译参数（Release），测量期间同机仍有其它
> agent 并行运行；数值为 3 次运行范围。命令：
> - ./build/benchmarks/storage_bench --json（case：flush_wal_truncation_off/on、
>   wal_checkpoint_long_run_100r）
> - ./build/tests/test_flush_recovery（26 个用例，含崩溃注入）

### 8.1 顺序保证（不可协商）

flush 成功后推进 WAL 水位并删段，严格按以下顺序：

1. 本轮 SST 逐个 Sync()（macOS = F_FULLFSYNC，数据落盘）；
2. 持久化 MANIFEST-wal-checkpoint（水位 + 段号下限 + CRC；文件名以 MANIFEST
   开头，PosixEnv::Sync 会顺带 fsync 父目录 → 同批 SST 的目录项也落盘）；
3. 才删除段号 < 水位的 .wal 段（删除失败只影响空间）。

任何一步失败都不删 WAL：checkpoint 写失败时 ForceFlush 仍成功（数据已落 SST），
但水位不前进、段不删除；SST Sync 失败会让 flush 失败并删掉半截 SST，WAL 保留。
重放只读段号 >= 水位的段；checkpoint 缺失/损坏 → 水位 0 → 重放全部（安全方向）。

水位采用 **2 轮滞后**：WAL-first 下存在"append 已完成、memtable 写入尚未执行"的
跨界窗口（直接写与事务提交都先 append 再拿 memtable 共享锁），段 S_i 的记录要到
第 i+1 轮 flush 后才全部落在 SST，因此水位只覆盖 S_{i-1}。clean Close()（写者已
停止、所有数据已 flush + fsync）把水位推进到当前活动段，只留一个空段。

### 8.2 flush 成本（SST 逐文件 fsync + checkpoint 的代价）

形状：200,000 行 × 16 列（一次 flush 产出 16 个 SST，14.6MB WAL）。

| case | flush 耗时（3 次） | 吞吐 | flush 后 WAL | Close 后 WAL |
|---|---|---|---|---|
| flush_wal_truncation_off（t7 快路径：SST 只 Flush） | 22.4 / 24.1 / 27.2 ms | 7.4–8.9 M rows/s | 1 段 / 14.6 MB（不回收） | 1 段 / 14.6 MB |
| flush_wal_truncation_on（t10：SST 逐文件 fsync） | 70.4 / 74.9 / 88.2 ms | 2.3–2.8 M rows/s | 2 段 / 14.6 MB | **1 段 / 0 B** |

（冷缓存的首次测量会出现 213–219 ms：首个 F_FULLFSYNC 还要回写整轮脏页；后续稳定在 70–88 ms。）

结论：**打开截断后 flush 慢 ~3.1–3.9x**（16 次 F_FULLFSYNC，每次 ~3–4 ms），
这是"删 WAL 前必须确认 SST 落盘"的直接成本（t7 在 FlushSstWritableFile 注释里已
预警）。可逆开关：LsmEngine::SetWALTruncationEnabled(false) 恢复 t7 快路径，
代价是 WAL 永不回收。后续优化方向（本任务不做）：把 SST fsync 移到后台
checkpoint 线程（flush 只写 + Flush()，水位在 fsync 完成后推进；WAL 由滞后窗口
兜住），可把 flush 延迟恢复到 t7 水平且不牺牲顺序保证。

### 8.3 长跑：WAL 目录有界

100 轮"3 条写 + flush"（wal_checkpoint_long_run_100r）：

| 指标 | 结果 |
|---|---|
| WAL 段数峰值 | **2**（2 轮滞后 + 活动段；理论上界 3） |
| WAL 字节峰值 | **219 B**（每轮 3 条小 record） |
| 每轮耗时（写 3 条 + flush + checkpoint） | 10.4–12.2 ms（SST fsync 主导） |
| Close 后段数 | 1（空活动段） |
| 水位 | 99 |

t10 之前：同样的 100 轮会留下 ~100 个段、线性增长的 WAL（旧行为 WAL 永不回收），
且每次重开都把已落 SST 的记录重新塞回 memtable。

### 8.4 测试（tests/storage/test_flush_recovery.cc，26 用例）

新增/改写的截断相关用例：
- WalTruncatedAfterFlushLeavesOnlyTailReplayed：崩溃后 0.wal 已被删，重开只重放
  B/C 两条 record（replay.records=2），A 从 SST 读；clean Close 后只剩 1 段。
- WalDirectoryBoundedOverManyFlushCycles：100 轮后段数/字节有界，300 行数据完整。
- SstSyncFailureKeepsWalAndData / HalfWrittenSstIsRemovedAndWalKept：
  "flush 成功但 SST 未落盘"与半截 SST 两种注入 → flush 失败、WAL 保留、重开恢复。
- CheckpointWriteFailureKeepsWal：checkpoint 写失败 → 水位不前进、不删段。
- FlushWithoutSstFsyncCrashRecoversFromWal：t7 快路径（截断关闭）下 flush 后立即
  SIGKILL → 重开从 WAL 重放补齐，且现有 SST 都能被 reader 打开（无半截文件）。
- WalFileNumbersStayMonotonicAcrossTruncation：WAL 目录被整体清理后，新段号仍
  >= 水位（防回绕导致重放静默跳过新记录）。
- WalReplayIsIdempotentAcrossReopen：跨段重复 record 去重（1 行而非 2 行）；
  clean close 后重开 ops_applied=0、数据从 SST 可读。

### 8.5 已知偏差（记录，不追）

- t7 的 flush 临时内存 −30% 目标未达标（41.98MB vs 目标 <=40MB，基线 42.58MB）：
  **接受偏差**。t10 恢复 SST fsync 后该指标不变（临时内存在 FlushEntityGroup
  之前的排序/分组阶段，与落盘策略无关）。
- flush 吞吐相对 t7 的 2.6x 提升在打开截断后回落到 ~1.0x（见 8.2）：这是正确性
  成本，用 SetWALTruncationEnabled(false) 可换回吞吐（WAL 不回收）。

