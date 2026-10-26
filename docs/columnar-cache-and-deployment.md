# Cedar 列式缓存与部署验证

状态：2026-10-26 后续验收收口（T8 文档更新）。**本阶段最终工作树指纹以 `_gates/final-acceptance-20261026/REPORT.md` 记录为准（T9 收口；本文不写会随文档修改漂移的 manifest 值）**；HEAD `8bef978`、未提交/未推送；本阶段源码指纹 `3c771167dfce134c333e5513a2279dd2f171b97a6d8b67b6fbeb03ee42f4e961`（147 文件，容器内 `sha256sum -c` 0 mismatch）；**二阶段（T1–T3，sanitizer-aware 超时补丁）源码指纹 `45521691da17dc3d3a83d7d3408becaad7b26320c8676d706e32ac35fd3aff4a`（147 文件、0 mismatch；相对 `3c771167…` 仅该 python 测试脚手架 + 3 文档，C++/CMake 零变化），补丁后全量 ASan/Debug 各取得 1245/1245；二阶段收口后的最终工作树指纹以 REPORT 中的最新记录为准（本文不写会随文档修改漂移的 manifest 值）**。

以下为 **2026-10-25 历史状态**：**最终工作树指纹以 `_gates/final-acceptance-20261025/REPORT.md` 记录为准**（不在本文写会随文档修改漂移的 manifest 值；146 个同步文件、HEAD `8bef978`、未提交/未推送）；**代码/测试/benchmark 基线指纹 `aa1eedb9d53b5f8abd5ba6315dd15a4af34eda85876058d2b5540ca98bbe8849`**（W2c/ASan/UBSan/C4 运行基线；最终工作树仅多 3 个 Markdown 文档，**未重跑测试**）。本文区分配置约束、**代码基线已验证结果**、**历史证据（2026-10-24 及更早，仅作历史引用）** 与未完成项。

2026-10-25 已覆盖：W1 恢复预算门禁、T4 生产形状 metadata 测量、W2c 代码基线完整 Debug、W2d `CedarInstallConsumer` 补跑、定向 ASan/UBSan、W3 的 C4 四路并发（10/10 accepted）与三种挂载 × 5 轮（120/120）。**1245 项注册测试全部有结论（2026-10-25 口径：1244 passed + 1 failed，即 #556 当时按“已接受行为变更”处理；2026-10-26 修正该测试期望后全量 Debug 为 1245/1245 passed）**；其中 W2d #1245 `CedarInstallConsumer` **Passed 209.15 s**（预热 `deb3d58b…` 缓存后）。**2026-10-25 阶段未完成/未验证**：sanitizer 全量；C3 三档生产性能矩阵未在该代码基线重跑（10-24 结果仅历史引用）。**这些项已于 2026-10-26 后续验收补全**（全量 ASan/UBSan 各 1245 项——ASan 在 sanitizer-aware 超时补丁后重跑取得 **1245/1245**、C3 三档各 40/40、bind/overlay 上的 C4 各 10/10、经 Windows 路径的挂载矩阵 40/40，详见下文「2026-10-26 后续验收」）；仍未验证的是**生产 profile 的混合负载/容量结论与真实设备冷读**，因此本文仍不给出生产缓存容量或性能提升比例。

benchmark-only 说明：T13 只改 `benchmarks/`（显式 fixture metadata，见 C4 小节）与对应合约测试，T14 只改 query benchmark 固定预算（1→1.1 GiB）；两者均**未改库/引擎/持久格式**，且已包含在代码基线指纹（aa1eedb9）内。T13 后仅重跑 benchmark/Python 合约测试，产品面由 W2c 全量覆盖。

## 缓存层次

Linux 文件页缓存保存 SST、Parquet、projection 和 WAL 的文件字节。当前采用 buffered I/O，系统负责回收和预读；`fill_cache=false` 仍可能填充系统文件页缓存。Docker 镜像层缓存、构建缓存和 native 编译缓存只服务构建，不是数据库运行时的数据块缓存。

Cedar 列页缓存保存解码后的不可变 typed 列页，减少重复读取和解码。canonical 与列扫描共享 sort/value 页的 owner；查询可见性和时间过滤仍逐次执行。普通范围扫描默认 lookup-only，热点读取可以显式 admit。派生 projection 页使用独立、严格有界的缓存，`projection_cache_bytes=0` 禁用它。

## 容量归属

| 项目 | 约束与解释 |
| --- | --- |
| shared cache | `block_cache_bytes` 限制共享 charge，包含 block 数据、Cedar 列页及 WBM reservation；不是 RSS 上限 |
| memtable / WBM | 非 developer profile 将 WBM 关联到 shared cache；reservation 代表实际 memtable 的预算占位，不能再作为一份物理内存叠加；WBM 也不是无超调的硬 RSS 上限 |
| read working | 解压输入、转换 scratch、未缓存列页受 DB pool 约束；自动值为 `max(4096, min(64 MiB, resolved_storage_memory_budget / 16))`，显式非零值覆盖 |
| query | 查询输出及读取工作受单查询 reservation 约束；与 DB pool 对同一分配的约束重叠，不代表两份分配 |
| cache pin | resident 的子集；查询 pin 计逻辑额度，物理归属不能把 resident 与 pin 相加 |
| projection cache / delta | 分别计独立的配置容量；projection cache 不占 canonical shared cache 的同一份内容 |
| reader metadata | footer、索引、Bloom 及打开过程 scratch 使用独立 DB pool，分配前预留；`storage_reader_metadata_bytes=0` 自动取 `max(4096, min(1 GiB, resolved_storage_memory_budget / 16))`（2026-10-25 起；此前为 `64 MiB` 封顶），显式非零值仍然优先。该自动值在 16 GiB 预算处达到 1 GiB 上限，预算更低时按 1/16 线性（1 MiB kernel-test profile 仍为 64 KiB），并计入 Open 容量预检。reader 持有保留额度，压力时清除未引用的 table reader 并重试一次，活跃 reader 不被强制释放；metadata 拒绝与 read working 拒绝分别报告所属 pool，文案不再互相混淆。2026-10-25 Debug 实测（`tests/storage/test_l0_reader_metadata_boundary.cc`）：单个常驻 reader 约 19–22 KiB 固定开销；160 KiB 仅覆盖 7 张并发 L0 reader（peak 158900 B，余量不足 5 KiB），reopen 9 张即被 metadata 预算拒绝（256 KiB 下 peak 200938 B）；12 张的 reopen 超出 1 MiB kernel-test profile 的 672 KiB 读取池（metadata 已占 272351 B，单次 working 请求 >157 KiB 且池内已占用约 231 KiB），以明确预算拒绝失败。估算容量与 reservation 分别报告，均不是 RSS。生产形状测量（T4）与 1 MiB kernel-test 边界（T3）见下节。 |
| 其他进程开销 | 线程栈、allocator、索引和维护峰值仍需余量，不能用 cache capacity 代替进程峰值 |
| kernel / file cache | 看 cgroup 的 file/anon、events、PSI 和实际内存限制；file 是整个 cgroup 的文件占用，不能全归本数据库 |

保守的物理容量规划应分别观察实际 memtable、共享缓存中扣除 WBM reservation 后的数据 charge、独立缓存、瞬时工作、查询输出及其他开销，并对共享 owner 去重。Open 的容量预检是配置校验，不能证明任意负载的 RSS 或容器峰值。

## 代码基线验证状态（2026-10-25 基线 + 2026-10-26 后续验收）

环境：`cedar-final-debug` 在 `/build` 使用 named volume，实际 backing filesystem 为 ext4 `/dev/sdd`；容器未设置内存限额（`memory.max=max`），只适合功能回归，不能作为受控内存压力实验环境。2026-10-25 的 W0 基线（`_gates/final-acceptance-20261025/00-freeze/`）记录了容器、卷、镜像 digest 与构建树；环境采样对象仍是构建卷，不是数据库性能数据卷。

当前工作树 / 代码基线：HEAD `8bef9785a63598fc61fac1aba9d6b1e818807570`（分支 `fix/storage-review-20261023`），146 个同步文件（148 文件 / 147 porcelain 条目，2 个 `__pycache__` 排除），容器内 `sha256sum -c` **146/146 OK、0 mismatch**，**代码/测试基线指纹 `aa1eedb9d53b5f8abd5ba6315dd15a4af34eda85876058d2b5540ca98bbe8849`**（T13 的 6 个 benchmark/合约文件落地后；W2c、UBSan 与 W3 均构建/运行该指纹；ASan 使用的是前一批 `dab3a073…`，差异仅 3 个 benchmark 文件）；**最终工作树指纹以 `_gates/final-acceptance-20261025/REPORT.md` 记录为准**（T19 收口时相对代码基线 changed=3/added=0/removed=0，全部为 Markdown 文档——本文、`release-notes.md`、`CEDAR_PRODUCTION_CAPACITY.zh-CN.md`；**未重跑测试**；T20/T21 只调整文档表述，具体 manifest/容器校验值由 T22 最终同步写入 REPORT）。未提交、未推送。

### W1 恢复预算门禁（2026-10-25，代码基线）

- 维护测试 `KernelTestProfileCompletesNativeFlushesAndReopens` **20/20 通过**（`--gtest_repeat=20 --gtest_break_on_failure`；min 9030 ms / max 12752 ms / avg 10186 ms，进程墙钟 204 s）。成功路径不打印逐轮 metadata 读数，替代证据为下节 L0 边界 XML；说明见 `01-recovery/summary.md` §2.2。
- 1 MiB envelope：`KernelTestProfileFitsOneMiBEnvelopeIncludingDefaultWriteBuffer` 通过（Open 预检口径的显式分配合计 **1016 KiB**（facts 32K + meta 8K + block cache 256K + working 512K + metadata 160K + query/projection/delta 各 16K）+ default write buffer 8 KiB = **1024 KiB**；`database.cc` 预检仍只计 1016 KiB，未改）。
- R2 预算种类：`RejectionNamesTheExhaustedPoolKind` 通过，断言 metadata 与 working 两类拒绝文案不同、各自包含所属 pool 名、默认 kind 为 working、`Grow` 沿用 metadata 文案。
- L0 边界（kernel-test 1 MiB profile，reopen 实际 reader 数 = 构造数 +1）：见下表；全部 4/4 通过（32.45 s），读数与实现阶段一致。
- 最小边界回归 **89/89** 通过（25.61 s），覆盖 reader / read budget / storage review / projection 预算拒绝 / 读预留释放。

| L0 边界用例 | 构造 → reopen | metadata 限额 | metadata peak | metadata 拒绝 | working 限额 / peak | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `ReopenAfterSixFactsL0SstUnder160KiBMetadata` | 6 → 7 | 163840 | 158900 | 0 | 524288 / 198472 | PASS |
| `ReopenAfterEightFactsL0SstUnder256KiBMetadata` | 8 → 9 | 262144 | 200938 | 0 | 425984 / 305976 | PASS |
| `EightFactsL0Rejects160KiBMetadataAndNamesTheMetadataPool` | 8 → 9 | 163840 | — | **>0（确定性拒绝）** | — | PASS（断言只出现 metadata 文案） |
| `TwelveFactsL0ExceedTheOneMiBReadPoolEnvelope` | 12 → 13 | 288 KiB 拆分 | 272351 已占满 | 0 | 384 KiB 下仍拒绝 | PASS（超 672 KiB 读取池，明确预算拒绝） |

**D2 口径**：1 MiB kernel-test profile 下，160 KiB metadata 只覆盖约 **7 个** reopen reader（6 张构造 = 7 个 reader，peak 158900/163840，余量 < 5 KiB）；8 张构造（9 个 reader）在 160 KiB 下被 metadata 预算**确定性拒绝**，改用 256 KiB 后通过；12 张无论在 672 KiB 读取池内如何拆分都放不下，以明确预算拒绝失败。不得把该 profile 描述为“160 KiB 覆盖 6/8/12 张”。本轮**没有**抬高 profile、放宽断言或跳过恢复校验。

### reader metadata 生产形状测量与 1 GiB 上限验证（T4，2026-10-25）

测量使用生产 writer 配置（`kProductionAppend`、facts memtable 64/128/256 MiB、kernel 模式、CedarParquet + LZ4、`min_write_buffer_number_to_merge=2`），在只读实例上同时 pin N 个 reader 共享同一个 metadata 池；测量构建为 T3/T4 源码 `91b5b8a3…`（D1 公式此后未变，仅 benchmark/测试与文档变化）；辅助程序位于 `_gates/final-acceptance-20261025/01-recovery/metadata-measurement/`，未进 CMake、未改产品源码或默认值。

| 项目 | 实测值 |
| --- | ---: |
| 最大 fixture（≈生产 512 MiB 文件行数） | 3,309,568 facts / 202 row groups / 80,800 列页 |
| 单 reader @3.31M facts（current / peak） | 16,206,198 / 18,369,016 B |
| 单 reader 斜率（64/128/256 MiB writer scale，current） | 4.906 / 4.900 / 4.897 B/row（偏差 < 0.2%） |
| **36 reader @3.31M facts（实测 peak）** | **585,585,946 B = 1 GiB 的 54.54%**，rejections 0，pin 失败 0 |
| 36×512 MiB 生产映射（3,302,607 facts/文件，162.56 B/fact） | current 582,199,414 B / peak 659,896,547 B = **54.22% / 61.46%** |
| 64 reader / 70 reader 边界 | 64：peak 1,039,359,490 B（96.8%），0 拒绝；70：peak 1,073,737,788 B，**8 次 metadata 拒绝、4 个 pin 失败**，错误为 `Cedar Parquet reader metadata budget exhausted` |
| 55 GiB 预算 probe | metadata 1,073,741,824 B、working 67,108,864 B；Open 预检 52,143,587,328 / 59,055,800,320 = **88.3%**（16 GiB 预算 96.33%） |
| 1 GiB 的 break-even（每文件行数） | current 6,090,949 / peak **5,373,792 facts**（peak 口径对应密度 ≥ 88.14 B/fact） |

**结论与敏感度**：1 GiB 自动上限在“55 GiB 预算 + 36 张 512 MiB L0（≈3.30M facts/文件）”场景下足够（peak 61.46%，实测 54.54%），本轮**不再抬高默认值**；单 reader metadata 随行数严格线性，因此按行数外推可靠。生产 fixture 密度为 162.56 B/fact，而本次 synthetic fixture 为 **48.107 B/fact**（高度可压缩，约密 3.4 倍）；若未来生产文件密度提高到 ≤ 约 88 B/fact（即 512 MiB 文件 > 5.37M facts），36 reader 才可能触顶，届时应先按证据复核是否降低占用，而不是静默加码。运维应按 `parquet_reader_metadata_peak_bytes / storage_reader_metadata_limit_bytes` 告警；metadata 计费是 reservation/估算口径，不是 RSS。

**诚实边界**：pinmulti 是“同一 SST 由 N 个只读实例打开”的 fixture 组装，不是 36 次 flush 产生的 36 张不同 L0 SST；单 reader 与深验证 reopen 的交叉校验（4,744,206 vs 4,689,878 B，差值为 close 时新增的小 SST）以及 N=8/24/36/64 的严格线性（每 reader 恒为 16,206,198 B）支持该等价性。未生成 512 MiB writer scale，未读取真实 EURDATA 数据。

### W2c 代码基线完整 Debug（2026-10-25，最终样本；最终工作树代码与之相同）

- 构建：`cmake --build /build/retest-debug --parallel 16` exit 0（0.9 s，`ninja: no work to do`；已用 T13 源码 mtime 12:44 UTC 与 `tests/test_columnar_cache_bench_options` 12:46:29 UTC 交叉验证构建树与代码基线一致）。
- 全量 CTest（`-E CedarInstallConsumer`）：注册 **1245** / 执行 **1244**；**最终样本 run2 = 1243 passed / 1 failed（仅 #556）/ 99% / 680.42 s**（LastTest.log 2,019,026 B）。**W2d 补跑 #1245 `CedarInstallConsumer` → Passed 209.15 s（3 分 29 秒）**；合计 **1245 注册 = 1244 passed + 1 已知接受失败（#556）**。
- 首轮 run1（同源码、-j8）= 1242 passed / 2 failed（#556 + #1095 时序抖动）/ 656.06 s；#1095 经 `-j1` 单跑 3/3 通过（18.74/20.79/20.61 s）与 run2(-j8) 通过分类为**负载敏感时序 flake**，未改断言、未跳过；首轮原文保留 `01-debug/w2c-03-ctest.log` 与 `w2c-08-failure-1095-run1.log`。
- T13 相关测试全部通过：`ColumnarCacheBenchOptionsTest` 8/8（各 0.01 s）、`BenchmarkMustObserveParquetReads` 78.91 s、`ColumnarCacheMatrixContract` 0.22 s。
- 唯一失败 **#556 `CedarParquetFactTableFactoryTest.RejectsLegacyBlockBasedFactsTable`**（`Subprocess aborted`）：已接受的已知行为变更（legacy block-based facts 表改为首次访问时拒绝），未修产品、未改测试、未重试掩盖；首轮/最终原文 `01-debug/w2b-06-failure-556.log`、`w2c-09-failure-556-run2.log`，语义与迁移建议见 `release-notes.md`。**（2026-10-26 已闭环**：#556 测试期望已按“Open 成功 + 首次访问 Corruption”修正并两轮通过，见下文「2026-10-26 后续验收」；本行为 2026-10-25 历史记录。）
- 同轮 W1 回归确认：L0 边界 4/4（17.90 / 18.72 / 22.61 / 29.11 s）、envelope 0.02 s、budget-kind 0.01 s、维护 reopen 21.78 s、`StorageReviewTest.MetadataPressureEvictsIdleReadersAndPreservesPinnedIterators` 10.63 s。
- `CedarInstallConsumer` **已补跑并通过（W2d，2026-10-25）**：先以 `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=16` 预热 `deb3d58b949ff6a24cb6` 缓存（configure exit 0，149.0 s；`lib/librocksdb.a` 1,063,082,720 B，幂等复核 1 s），再 `ctest -R '^CedarInstallConsumer$'` → **1/1 Passed 209.15 s（3 分 29 秒）**，exit 0；合计 1245 注册 = 1244 passed + 1 failed（#556 已接受）。此前“冷缓存需 1.5–2 h”的估计**不准确**：t5 时 producer 实际走同一 `…-deb3d58b…/build`（当时缺 `lib/librocksdb.a` 且内部 `--parallel 1`），`96899d1c…` 与该门禁无关；预热后复跑约 3.5 分钟。证据：`01-debug/w2d-install-consumer/`（含预热日志、缓存 manifest、ctest/LastTest、清理记录）。

### W2 定向 ASan / UBSan（D3 定向集，2026-10-25）

- **ASan**（源码 `dab3a073…`，与代码基线 aa1eedb9 仅差 3 个 benchmark 文件）：`/build/retest-asan`（Debug、`CEDAR_ENABLE_ASAN=ON`、链接 libasan.so.8、41 个 `__asan_` 符号）；14 目标构建 exit 0（98 编译 + 16 链接）；过滤器 278 用例，run1/run2 均 **276 passed / 2 failed**，**ASan/LSan 标记计数 = 0**（含 LastTest.log 438,220 B）。2 个失败均为 `KernelBoundedBenchmarkTest` 的时序敏感用例（#448 维护 reopen 的 `maintenance_flush_deadline_yields 1 vs 0`；#449 1 MiB 压力夹具的 append admission 拒绝），无 sanitizer 输出；#448 `-j1` 单跑 3/3 通过，#449 为已知夹具抖动。LSan 有效性用故意泄漏 64 B 探针验证（容器内报 `LeakSanitizer`、64 byte(s) leaked、exit 1）。
- **UBSan**（源码 `aa1eedb9…`，**与代码基线完全相同，也与最终工作树的代码相同**）：`/build/retest-ubsan`（Debug、`CEDAR_ENABLE_UBSAN=ON`、`-fsanitize=undefined`、libubsan.so.1、15 个 `__ubsan_` 符号）；14 目标构建 exit 0（378 s）；同 278 用例 **277 passed / 1 failed**（#448 时序断言，#448 `-j1` 3/3 通过），**UBSan 标记 = 0**（LastTest.log 437,650 B）；复跑中 #449 在 retry1/retry3 失败、retry2 通过，属同一已知压力夹具抖动。`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1` 用非法移位探针验证生效（exit 1 + `runtime error: shift exponent 32…` + 栈回溯）。
- **口径**：两项均为 D3 推荐的**定向集**（修改路径 + owner/lifetime/cancel/并发 + 新回归），不是 sanitizer 全量；全量需构建全部 125+ 目标，2026-10-25 阶段列为未完成项（**2026-10-26 已补全量，见下文「2026-10-26 后续验收 / 全量 ASan / UBSan」**）。

### W3 C4 四路同库并发（机制验证，2026-10-25）

- 环境：专用容器 `cedar-w3-perf-20261025`（`cedar:final-base`），memory.max **1 GiB**、memory.swap.max **0**、cpu.max **2 CPU**（cpuset 0,1），kernel 6.18.54.1-WSL2；数据在 named volume（ext4 `/dev/sdc`）；测量期间无编译。runner 为代码基线（aa1eedb9）增量构建的 RelWithDebInfo（sha256 `604d0e1e…`，identity build_type=RelWithDebInfo、engine digest `46f2252d…`）。
- fixture：`--setup-only --entities 20000 --seed 42 --part-id 7 --query-memory-bytes 268435456` → Parquet 9,750,816 B、4 row groups、1,475 列页、60,000 canonical rows、`facts_flushed=true`；每个 interval 使用独立副本并在前后校验 manifest 不变。**失败样本保留**：同 setup 用 64 MiB 查询额度确定性失败（`ResourceExhausted: query: memory_bytes budget exhausted`，rc=1，`04-c4/logs/FAILURE-fixture-setup-64MiB-query.log`）。
- 配置（**实验 fixture，非生产默认**）：`--write-existing --workload concurrent --seconds 10 --repetitions 5`、working 64 MiB、query 64 MiB、**reader metadata 显式 16 MiB**（T13 的 `--storage-reader-metadata-bytes`；CSV 每行 `reader_metadata_limit_bytes=16777216`；environment.json 写 `experiment_fixture_metadata: not a production default`）。
- **结果：10/10 accepted=true**（5 repetitions × lookup-only/admit 交错）。每 interval：facts compaction **12–17** 次且与另三路 overlap **2.66–3.73 s**（四路同活 overlap 3.13–4.14 s / 10 s）；`wal_sync_writes 57–64`、`wal_unsynced_writes=0`；`file_read_calls 90,730–129,978`、`column_decodes 84,452–116,134`；全部 lane errors=0、background_errors=0、stderr 空；10/10 无 interval OOM；源 manifest 不变；metadata peak 最大 791,352 B（16 MiB 池的 4.7%）、rejections 0。
- 结论边界：只证明**机制成立**（四路真实并发、facts compaction 与三路重叠、同步 WAL、真实 Parquet 解码），延迟数字来自 debug-small-thresholds 机制 profile 与 2 CPU 限额，**不是生产延迟/吞吐结论**；C4 在 2026-10-25 仅在 named-volume 上完成（**2026-10-26 已补 linux-bind 与 overlay，各 10/10，见下文**）。

### W3 三种挂载 × 5 轮（read-shape，机制验证，2026-10-25）

- 定义与校验：named-volume（ext4，`/dev/sdc[…/cedar-w3-20261025-named/_data]`）、linux-bind（Docker VM 内 ext4 `/var/lib/docker/volumes/cedar-w3-20261025-bind/_data/bind`）、overlay（容器可写层）。每个挂载用 `findmnt -J -T`/`df -T` 校验实际 fstype 与 source 与 label 一致（三例均一致）；**Windows NTFS bind = unavailable**（2026-10-25 未做，不混排；**2026-10-26 已可运行，但实测 fstype 为 9p 而非 NTFS 语义直通，见下文**）。
- 协议与结果：每挂载独立 fixture（同上 9,750,816 B），5 repetitions × 4 workloads（point/narrow/wide/temporal）× 2 policies = 40 运行，共 **120/120 成功**；`accepted` 全 true、dataset 前后不变、interval OOM 0、working/metadata 拒绝 0、stderr 空；metadata bytes 最大 295,386 B（显式 16 MiB 池）。
- 机制观察：**point** 的 lookup-only 每次有文件请求与解码（reads 6,444–6,816、decodes 4,296–4,512/轮），admit **全部 0 文件请求、0 解码、decode_us=0**，p99 从 602–634 µs 降到 228–248 µs；**narrow/wide/temporal** 的 admit 与 lookup-only 混合、无一致收益（wide 仅 named-volume 出现 reads -14.3%，linux-bind/overlay 无变化），与“扫描保持 lookup-only”一致。
- 边界：全部轮次 `cold_unverified`（只预读文件，不证明驻留/设备冷读）；cgroup io.stat 区间 rbytes 增量 0（页缓存命中），不能给出各挂载物理设备读取量；三挂载差异落在轮次波动内，**不据此选择生产挂载**；C4 在 2026-10-25 未在 bind/overlay 重复（**2026-10-26 已补做，各 10/10，见下文**）。

### 2026-10-26 后续验收（#556 测试期望修正、sanitizer-aware 超时补丁、全量 sanitizer、C3、bind/overlay C4、Windows 路径挂载）

源码基线：同步指纹 `3c771167dfce134c333e5513a2279dd2f171b97a6d8b67b6fbeb03ee42f4e961`（147 文件，容器内 `sha256sum -c` 147/147 OK、0 mismatch），等于 2026-10-25 工作树加上 `cedar_parquet_table_factory_test.cc` 的 #556 测试修正。HEAD 仍为 `8bef978`，未提交/未推送。Debug / ASan / UBSan 三套构建树（`/build/retest-{debug,asan,ubsan}`）与 C3/C4/挂载用的专用容器（`cedar-c3-perf-20261026`、`cedar-w3-perf-20261025`、`cedar-ntfs-perf-20261026`）在同一源码指纹上串行运行，测量窗口内无并行构建。证据根目录：`_gates/final-acceptance-20261026/`。

**二阶段（2026-10-26 13:12 起，sanitizer-aware 超时补丁后）**：源码指纹 `45521691da17dc3d3a83d7d3408becaad7b26320c8676d706e32ac35fd3aff4a`（147 文件、0 mismatch），相对 `3c771167…` **changed=4 / added=0 / removed=0**——`tests/performance/test_columnar_cache_dataset.py`（超时补丁）加 3 份 Markdown 文档，**C++/CMake/third_party 零变化**，因此编译产物与一阶段相同，全量重跑用于验证补丁后门禁可常绿（证据 `01-debug/1233-timeout-fix/`、`01-asan/full2/`、`01-debug/w2f-debug2/`）。

#### #556 测试期望修正（仅改测试）

- 只改测试文件 `src/engine/rocksdb/table/cedar_parquet/cedar_parquet_table_factory_test.cc`：`CedarParquetFactTableFactoryTest.RejectsLegacyBlockBasedFactsTable` 改为断言 **`DB::Open` 成功**，并在首次 `Get`、`NewIterator`+`SeekToFirst`、`Seek` 上断言 `Corruption("facts table is not Cedar Parquet v2")`；Open 之后统一用 `EXPECT` 并无条件销毁两个 CF handle。旧 `ColumnFamilySet::~ColumnFamilySet(): Assertion last_ref failed` abort 不再复现。产品代码、持久格式、WAL、MVCC 未改动。
- 完整 Debug 全量（1245 注册，含 `CedarInstallConsumer` #1245）：**run2 1245/1245 passed、0 failed、760.50 s**（`CedarInstallConsumer` Passed 256.03 s，预热后）；#556 用例 run1/run2 均 Passed（4.61 s / 3.25 s）。首轮 run1 = 1244 passed / 1 failed（#1095 `maintenance_flush_deadline_yields 1 vs 0` 负载敏感时序 flake，`-j1` 复跑 3/3 Passed，run2 -j8 通过），未改断言、未跳过。
- 口径变化：**#556 由 2026-10-25 的“已接受已知行为变更”改为“测试期望已修正并通过”**。行为合同不变：legacy block-based facts 表仍在首次访问（Get/Iterator/compaction）被拒绝，legacy 数据从不按 Cedar Parquet 解码；**空 legacy facts 列族仍可能 Open 成功**，该边界保留在 `release-notes.md`。

#### 全量 ASan / UBSan（各 1245 项，0 sanitizer 报告）

- 两套构建树原为 `BUILD_BENCHMARKS=OFF`（ASan 仅注册 1236 项、UBSan 仅 480–489 项）。为与 Debug 注册集一致且**不排除任何测试**，分别以 `-DBUILD_BENCHMARKS=ON` 重配置（exit 0；UBSan 侧配置 642.4 s，含预构建 UBSan RocksDB 缓存），重配后 `ctest -N` 均为 **1245**。
- **#1233 sanitizer-aware 超时补丁（仅测试脚手架，二阶段）**：`tests/performance/test_columnar_cache_dataset.py` 的 `run_bench` 原以硬编码 `timeout=90` 拉起单次 benchmark 子进程；ASan 插桩下单次子进程实测约 94.8 s（write 11.29 s + flush 7.05 s + recovery_validation 76.49 s），因此确定性 `TimeoutExpired`。补丁新增 `BENCH_TIMEOUT_SECONDS=90`、`SANITIZER_TIMEOUT_SCALE=3` 与 `bench_timeout_seconds()`：**检测到 `ASAN_OPTIONS` / `UBSAN_OPTIONS` 时把 90 s 子进程超时按 3× 缩放为 270 s**（`CEDAR_TEST_TIMEOUT_SCALE` 可显式覆盖；探针实测 90 / 270 / 270 / 180 / 360 / 135），`run_bench` 改用它；**断言、workload、flag、产品代码、CMake 一律未改**（2 hunk、0 断言行）。补丁后 #1233：ASan 全量三轮全 **Passed**（146.02 / 153.22 / 151.08 s，远低于 270 s 有效超时）、ASan `-j1` **Passed 116.46 s**；Debug（无 sanitizer 环境 ⇒ 保持 90 s 默认）三轮 Passed（76.23 / 83.29 / 85.20 s）。
- ASan 一阶段（补丁前，源码 `3c771167…`）：`/build/retest-asan`（Debug + `-fsanitize=address`；`ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:print_summary=1`、`LSAN_OPTIONS=report_objects=0`；`-j4`）全部目标构建 exit 0（997.5 s + 29.8 s）；**1245 执行 / 1243 passed / 2 failed / 1372.20 s**，两个失败为 #1095（时序 flake）与 #1233（硬 90 s 子进程超时）。
- ASan 二阶段（补丁后，源码 `45521691…`；同一命令 `-j4` 全量三轮）：run1 **1244/1245**（917.88 s，仅 #1096）、run2 **1243/1245**（940.73 s，#1096 + #1225）、**run3 1245/1245 passed / 0 failed / ctest exit 0 / 939.20 s**；`CedarInstallConsumer` Passed 283.69 / 258.91 / 261.35 s，#1236 `QueryCampaignOptionsContract` Passed 848.63 / 885.90 / 886.04 s（决定整轮墙钟）。全部日志（ctest、LastTest、build、reconfigure、8 次串行复跑）对 `AddressSanitizer|LeakSanitizer|detected memory leaks|SUMMARY` 的命中为 **0**（`detect_leaks=1`、`print_summary=1` 均开启）。
- ASan 二阶段的失败项**均非 sanitizer 报告，也非本轮补丁引入**（补丁只动 python 测试脚手架，C++ 二进制未重编译）：**#1096** `KernelTestProfileDoesNotRejectAsyncWritersAtFlushDebtLimit`（`append admission is under pressure` / `reader metadata budget exhausted`；ASan `-j1` 8 次复跑 **3 Passed / 5 Failed** ⇒ 1 MiB envelope fixture 的固有边界抖动，2026-10-25 ledger 已记为 open item）；**#1225** `QueryCrashMatrix…AuthoritativeFactsCorruptionRequiresRecoveryWhileProjectionFailsClosed`（`append admission deadline expired under pressure`；Debug `-j1` 3/3 Passed）。**#1095** `maintenance_flush_deadline_yields 1 vs 0` 在二阶段 ASan 三轮全 Passed（23.24 / 18.99 / 20.65 s），仍按**负载敏感时序 flake**记录（此前 `-j1` 3/3 Passed）。
- Debug 二阶段（同一源码 `45521691…`；`-j8` 全量三轮）：run1 **1243/1245**（782.34 s，#242 + #1096）、run2 **1244/1245**（751.09 s，#1225）、**run3 1245/1245 passed / 0 failed / ctest exit 0 / 724.15 s**；`CedarInstallConsumer` Passed 319.90 / 262.49 / 258.56 s，`QueryCampaignOptionsContract` 782.22 / 750.97 / 724.02 s 决定整轮墙钟。失败项均非源码回归：**#242** `FailedDirectoryCreationReleasesFreeSpaceAdmission`（free-space reserve 按运行时刻 `space().available` 计算，与并行测试腾挪磁盘存在竞态；`-j1` 5/5 Passed）、**#1096**（`-j1` 2/3 Passed）、**#1225**（`-j1` 3/3 Passed）。
- UBSan（`/build/retest-ubsan`，Debug + `-fsanitize=undefined`；`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`、`UBSAN_SYMBOLIZER_PATH=/usr/bin/addr2line`（容器无 llvm-symbolizer）；`-j4`）：构建 exit 0（575.2 s，0 error/0 warning）；**1245 执行 / 1245 passed / 0 failed / 1262.92 s**。大小写敏感扫描 `runtime error:` / `UndefinedBehaviorSanitizer` 在 ctest/LastTest/build/reconfigure 日志均为 **0**（大小写不敏感的 “ubsan” 命中只是路径与 flags 文本）。#1233 在 UBSan 下 Passed 109.66 s（该轮测量早于超时补丁；单次子进程调用未触及 90 s，补丁后上限为 270 s），#1095 Passed 20.86 s。补丁只改 python 测试脚手架，UBSan 未重跑（C++ 产物不变）。
- 结论：**全量 sanitizer 在本阶段完成且 0 sanitizer 报告；ASan 在 sanitizer-aware 超时补丁后取得 1245/1245（run3，939.20 s，ctest exit 0）；Debug 同源码同样 1245/1245（run3，724.15 s，exit 0）**。剩余失败全部是**非 sanitizer 的准入/时序抖动**（#1096 高频、#1225/#242 偶发），已逐条分类并保留原始日志，不构成内存错误或断言回归，**未跳过、未弱化任何断言**。`02-ubsan/full/LastTestsFailed-stale-from-20261025.log` 是 2026-10-25 的历史残留（内容为 #449）；本轮 0 失败时 ctest 不重写该文件，已改名标注，不作为本轮失败。

#### C3 三档 read-shape 矩阵（当前源码；只谈文件请求/解码与代价）

- 环境：专用容器 `cedar-c3-perf-20261026`（memory.max **128 MiB**、memory.swap.max **0**、cpu.max **2 CPU**），数据在 named volume（ext4 `/dev/sdc[…cedar-c3-20261026-data]`，findmnt 校验）；runner `/build/cache-perf-release/cedar_query_storage_read_bench` 为当前源码增量重建（sha256 `b8bf3c00…`，identity fingerprint `a073082a9a64c36faf46`、build_type RelWithDebInfo）。该 runner 必须重建：RocksDB 缓存键由 `src/engine/rocksdb` 全目录摘要派生，#556 的测试修正改变了缓存键，2026-10-25 的旧 runner（sha256 `604d0e1e…`）不对应当前源码。
- 协议：复用 2026-10-24 的 flushed fixture（manifest 与历史逐一相等，另用 `cp -a` 保留 mtime 复制到独立卷），5 repetitions × 4 workloads（point/narrow/wide/temporal）× 2 policies（lookup-only / admit）= **每档 40 运行，三档共 120 运行，全部成功**（0 错误、0 OOM、0 预算拒绝，working_rejections 全 0、cgroup memory.events 增量全 0，三档 manifest 前后不变）。
- 结果（中位数；比值 = admit / lookup-only）：
  - **small**（1000 entities，Parquet 723,493 B，cache 8 MiB，完全驻留）：4 种 workload 的测量区间文件请求与列解码**全部降到 0**，p50 为 lookup-only 的 **0.36–0.70 倍**；
  - **medium / large 的 point**：cache 1 MiB 足以容纳点查工作集，文件请求/解码同样**归零**，p50 为 **0.33–0.35 倍**，1 s 内操作数约 2.9–3.0 倍；
  - **medium / large 的 scan（narrow/wide/temporal）**：cache_hits=0，文件请求/解码基本不降（仅 medium narrow 降 6%，来自同查询内页复用），p50 为 **0.99–1.07 倍**（略有代价）。
- 内存：RSS 20.5–22.1 MiB，各档各策略基本恒定；峰值 memory.current 32.3 MiB，远低于 128 MiB 限额；cpu.stat throttled 增量 0。
- 结论：**文件请求/解码的减少取决于工作集能否驻留缓存**；扫描没有一致收益，**lookup-only 保持默认**（本轮未改任何产品代码或默认值）。以上不构成生产环境提升百分比或容量结论。
- 限制：全部 120 轮 `cold_unverified`；cgroup io.stat rbytes 增量合计 < 32 KiB（读取基本命中宿主 page cache），绝对延迟**不代表设备/冷启动行为**，且 runner identity 与 2026-10-24（`6ce22aaff7f6294e7b2a`）不同，**不可跨会话比较绝对延迟**。

#### bind / overlay 上的 C4 四路并发（各 10/10）

- 环境：专用容器 `cedar-w3-perf-20261025`（memory.max **1 GiB**、memory.swap.max **0**、cpu.max **2 CPU**、cpuset 0,1）；runner 同 C3（RelWithDebInfo `b8bf3c00…`，mtime 早于测量窗口）。findmnt 实测：`/data-bind` → ext4 `/dev/sdc[…cedar-w3-20261025-bind/_data/bind]`（label `linux-bind`）、`/data-overlay` → `overlay`（容器可写层）。
- 协议：每挂载 10 个 interval，每 interval 10 s、独立 fixture 副本、lookup-only/admit 各 5 次；debug-small-thresholds 机制 profile、working 64 MiB、query 64 MiB、reader metadata 显式 16 MiB（`experiment_fixture_metadata: not a production default`）。
- 结果：**linux-bind 10/10、overlay 10/10（共 20/20）accepted=true**；四 lane 均有真实操作；facts compaction 10–13 次且与另三路 overlap 2.99–3.44 s（四路同活 overlap 3.60–4.15 s / 10 s）；`wal_sync_writes` 42–51、`wal_unsynced_writes` 全 0；`file_read_calls` 82,391–128,176、`column_decodes` 76,604–115,325；全部 lane errors=0、background_errors=0、stderr 空；20/20 interval 无 OOM；源 fixture manifest 前后相同（bind `292f5777…`、overlay `b894b4d6…`），每 interval 副本 `dataset_before` 等于源 manifest；metadata peak 670,750–790,328 B、rejections 0。
- cgroup/PSI：memory.pressure 0/0；io.pressure some ≈14.6/14.3 Mµs、full ≈0.89 Mµs；io.stat rbytes 增量 0（全部命中 page cache，cold_unverified）、wbytes 41.7/40.2 MB；cpu 214 CPU·s，throttled 181/203 次；memory.current 峰值 41.2/70.7 MiB。
- 结论：C4 机制在 ext4 bind 与 overlay 上与 named-volume 历史结果一致（历史 runner identity 不同，**只作机制一致性对照，不比较延迟**）。未覆盖生产 profile 与真实冷读。

#### 经 Windows 路径的 bind 挂载（label `windows-bind`，实际 fstype 9p）

- 挂载：`-v E:\wy_cedar_test\_gates\final-acceptance-20261026\05-ntfs:/data-ntfs`（可写），容器 `cedar-ntfs-perf-20261026`（1 GiB / swap=0 / 2 CPU）。
- **实际文件系统不是 NTFS 语义直通**：findmnt 报 `fstype=9p`、source `E:\[…05-ntfs]`，`stat -f` 报 `v9fs`，`df -T` 报 9p（Docker Desktop/WSL2 以 9p/drvfs 暴露 Windows E: NTFS）。因此 `windows-bind` 只是操作者标签，本列**只作单列对照**，不与 Linux ext4/overlay 结果混排，也不作为 NTFS 特性/性能证据。写入与锁探针（write/read、unlink、mkdir、rename、rmdir、`fcntl.lockf(LOCK_EX|LOCK_NB)`）全部成功。
- fixture 在 9p 上直接生成：`--setup-only --entities 20000 --seed 42 --part-id 7 --query-memory-bytes 268435456` → Parquet **9,750,816 B**、4 row groups、1,475 列页、60,000 canonical rows，dataset manifest `9e2719d3…`（13 文件）前后不变。
- 矩阵：5 repetitions × 4 workloads × 2 policies = **40 运行，40/40 成功**（0 错误、0 OOM、0 working 拒绝、CSV 单行、warmup 证据齐全）；validation 714 项检查、0 失败；全部 `cold_unverified`。
- 机制（与 C3 一致）：point 的 admit 使 physical_bytes/file_read_calls/column_decodes **归零**（cache_hits 11,666，p50 163 µs vs lookup-only 2,691 µs）；narrow/wide/temporal 的 admit 与 lookup-only 的请求/解码**完全相同**（cache_hits=0），p50 略慢（192,629 vs 175,396 / 918,525 vs 860,003 / 240,194 vs 224,471 µs）→ **扫描无收益，lookup-only 保持默认**。
- 边界：9p 绝对延迟显著高于 ext4（point 2,691 µs vs C3 medium 647 µs），**不可外推**；9p 无块设备计量（容器 `io.stat` 为空，rbytes/wbytes 恒 0 不代表无 I/O）。

#### 本阶段收敛与仍未验证

- **本轮已补全**（相对 2026-10-25 的未完成项）：#556 测试期望（改为 Open 成功 + 首次访问 Corruption，全量 Debug 0 failed）、全量 ASan/UBSan（各 1245 项、0 sanitizer 报告；**ASan 在 sanitizer-aware 超时补丁后重跑为 1245/1245**）、C3 三档矩阵（各 40/40）、bind/overlay 上的 C4（各 10/10）、经 Windows 路径的挂载矩阵（40/40，实际 9p）。
- **仍未验证/未做**：生产 profile 的缓存容量与混合负载（含 C2）、真实设备冷读（全部 `cold_unverified`）、NTFS 语义直通（仅有 9p）、多主机/多容器竞争、F-07（bounded 策略下 corrupt/legacy SST 的读取期检测）无单独用例。
- 环境边界必须随结果引用：所有性能/机制数字来自 128 MiB–1 GiB、2 CPU 的专用容器与 debug-small-thresholds 或显式 fixture 参数，不是生产 profile；`io.stat`/`file_read_calls` 只说明请求，不证明物理设备 I/O。

### 历史证据（2026-10-24 及更早，仅历史引用）

以下数字属于更早源码/更早构建，**不代表最终工作树（代码基线 `aa1eedb9…`；最终工作树指纹见 REPORT）**：Debug 引擎 fingerprint `8d9ec4b96a0da345e113`，source digest `55be5f7a891d6ac6f46940317d34309609c9416ab1456fc72710fb91f5dc100c`；元数据生命周期及公开指标测试 2/2 通过；reader、columnar、profile 相关回归 113/113 通过（78.38 秒）；全部目标随后完成 228 步重建。原始日志位于 `_gates/cache-implementation-20261023/docker-rebuild-20261024/`（`metadata-peak-build.log`、`metadata-peak-focused.log`、`metadata-peak-related.log`、`metadata-peak-environment/environment.json`）。DockerData 重建前导出的历史日志不证明当前二进制或当前代码基线的行为。

## 可重复采样与实验

宿主只读环境采样（输出目录必须是新的目录）：

```powershell
python benchmarks/collect_cache_environment.py --container cedar-final-debug --data-directory /path/to/dedicated-db --dataset-manifest --output-directory E:/wy_cedar_test/_gates/new-cache-environment
```

固定数据由 `cedar_query_storage_read_bench --setup-only --db-path PATH` 创建，已有目录不会被静默覆盖。实际参数和规模须与数据 manifest 一致；`--read-existing` 不重建数据。先通过 `BenchmarkMustObserveParquetReads` 与参数/CSV 合约测试，再执行性能实验。

矩阵工具 `benchmarks/run_columnar_cache_matrix.sh` / `columnar_cache_matrix.py` 在同一专用数据集上交错运行 lookup-only 和 admit，默认五轮；记录原始 CSV、环境和 cgroup 区间采样。正式性能运行要求记录为 RelWithDebInfo 的引擎身份，Debug 的 `--smoke` 仅验证工具流程。`--prewarm-system` 顺读文件并不证明全部驻留；进程重启、reader namespace 更新也不证明设备冷读。无法证明冷状态时保留 `cold_unverified`。

三个实际字节规模档位、专用 cgroup 压力与回收及同 reader 解码热区间的结果见下文（**均为 2026-10-24 历史数据**）。本轮 W3 已在代码基线（aa1eedb9）上完成 C4 四路并发（10/10 accepted）与 named-volume/Linux-bind/overlay 三挂载 read-shape（各 40/40），结果见上文「W3 C4 四路同库并发」与「W3 三种挂载 × 5 轮」；2026-10-25 时仍未完成的是代码基线的 C2/C3 生产性能矩阵与生产 profile 的混合负载结论（**C3 三档已于 2026-10-26 在当前源码补做，各 40/40；C2 与生产 profile 混合负载仍未做**）。当前 `mixed` 只交错读取形态，不代表并发负载。失败、OOM、错误行数或资源拒绝必须作为失败样本保留。

## 部署选择与结论边界

> 本节三档实验数据来自 **2026-10-24 源码与 RelWithDebInfo 引擎 `6ce22aaff7f6294e7b2a` / `96899d1c54f04feb5369`，仅作历史证据**；最终代码基线（指纹 `aa1eedb9…`）在 2026-10-25 尚未重跑 C2/C3 性能矩阵；**C3 三档矩阵已于 2026-10-26 在当前源码（`3c771167…`）补做，见下文**（C2 未做）。部署路径与结论边界（buffered I/O、范围扫描 lookup-only、`cold_unverified` 等）仍是当前策略。

2026-10-24 正式前两档实验：RelWithDebInfo 引擎 `6ce22aaff7f6294e7b2a`，
独立容器 `cedar-cache-perf-20261024`，128 MiB memory.max、swap.max=0、2 CPU，
新 named volume、ext4 `/dev/sdd`。测量时无并行构建。v2 fixture 每版本按
20000 实体分批提交，保持数据库 2 MiB 硬限额，完整恢复及 Cypher 交叉校验。

两档各四种负载、两种策略、五轮交错（每轮 1 秒），均预读文件并 warmup 一次。
各 40/40 成功且数据 manifest 未变，80 个区间的 memory.events 所有键增量均为零。

| 档位 | 实际 Parquet 字节 | cache | 工作池/单查询额度 |
| --- | ---: | ---: | --- |
| 小热点 1000 实体 | 488210 | 8 MiB | 8 MiB / 16 MiB |
| 超缓存 20000 实体 | 9750816 | 1 MiB | 8 MiB / 16 MiB |

小档 admit 四种负载测量区间的文件请求、解码和 decode_us 均为零。
小档 point p99 五轮中位数（范围）：lookup-only 652 us（584–1039），
admit 256 us（232–350）。中档 point 为 647 us（612–651）与 342 us（256–354）；
narrow 为 64398 us（56047–69506）与 69084 us（67329–83837）；
wide 为 183412 us（166008–197471）与 182245 us（170261–187254）；
temporal 为 56929 us（54604–60529）与 59549 us（56934–61874）。
扫描没有一致收益，继续保留 lookup-only 默认。每轮样本量随操作耗时变化，
应结合原始样本，不能由此提出生产容量常数。

第一次矩阵在工具一次性读取带符号 runner 做哈希时 OOM，未产生测量行。
现改为 1 MiB 流式 SHA256，历史 oom_kill=1 保留，正式轮次取各区间差值。
100000 实体 setup 原因单笔超过 2 MiB 被拒绝；分批后完整 Cypher 校验仍超过
显式 256 MiB 查询额度而失败。失败日志保留；后续第三档结果见下文。
原始 80 轮 CSV、summary 和 cgroup/PSI/io.stat/guest diskstats 在外层证据目录
`docker-rebuild-20261024/results-small-v2-streamed` 与 `results-medium-v2-streamed`。
所有轮次维持 `cold_unverified`；共享 file-cache 归属和宿主 VHDX 缓存不可控，
不作设备冷读提速结论。尚缺并发 WAL/compaction 和 mount 对照。

第三档使用当前 runtime 重建的 RelWithDebInfo runner（相同 native engine），
300000 实体、900000 事实。完整恢复/canonical/Cypher 验证在 setup 容器以显式
4 GiB 查询额度完成；setup 查询池与请求额度对齐，没有关闭验证。
实验副本包含 3 个 Parquet 文件、56 row groups、22025 列页，实际 Parquet
146303758 字节，大于独立测量容器的 134217728 字节内存限额。
源库与副本 19 个文件的内容哈希及元数据一致。

同一 128 MiB / swap0 / 2 CPU 容器，1 MiB cache、8 MiB working、16 MiB query，
四负载、两策略、五轮交错，预读并 warmup 一次，40/40 正确且数据 manifest 未变。
40 个区间 memory.events 的 oom/oom_kill/oom_group_kill 增量均为零；max 增量合计
15064。pgscan/pgsteal 增量合计 674718/670791，file refault 599657。
区间端点 file 为 108400640–118644736 字节，anon 为 9183232–9539584 字节；
这些是 worker 退出后的端点采样，不是运行峰值。
memory PSI some/full total 增量合计 23542558/23534578 us；guest cgroup rbytes
合计 2456199168，wbytes 385024。该口径包含 reader open、warmup、采样日志及延迟
writeback，不能等同 CSV 测量时段、专属数据库设备读取或宿主 VHDX 物理 I/O。

| 负载 | lookup-only p99 中位数（范围）us | admit p99 中位数（范围）us |
| --- | ---: | ---: |
| point | 1401（1213–2171） | 687（471–738） |
| narrow | 928377（842092–1347286） | 982243（889985–1159866） |
| wide | 7381128（4746425–9400872） | 8048636（6665312–10878959） |
| temporal | 362549（358039–380958） | 383114（370270–391521） |

每轮目标时长仅 1 秒，单次 wide 扫描可能超过该时长，扫描分位数的样本稀疏，
不能作为生产尾延迟估计。point admit 测量区间文件请求为零，扫描没有一致改善，
继续保留 lookup-only 默认。文件工作集确实超过容器内存并发生回收，仍标
`cold_unverified`，不声称设备冷读。容器在首个区间前已有 oom=6/oom_kill=2
（此前工具哈希 OOM 及本次准备阶段新增事件）；完整历史保留，不能声称整个
容器从未 OOM。工具新增区间 OOM 判定，正确 CSV 也不能覆盖区间内 OOM 失败。
原始结果 `results-large-v4-streamed`，独立汇总 `large-v4-pressure-analysis.json`，
源/副本内容清单及 setup 日志均在相同外层证据目录。

2026-10-24 解码计时工具 smoke：Debug 引擎 `96899d1c54f04feb5369`，
named volume `/build/cache-decode-smoke-20261024`，seed=42、part=7、1000
entities/3000 canonical rows。实际 Parquet 488210 字节、1 文件、1 row group、
75 column pages。每轮预读文件、同 reader warmup 一次，point/narrow/wide/temporal
各五轮交错 lookup-only/admit，共 40/40 成功；manifest 前后相同。

四种 admit 测量区间的文件请求、typed decode 和 `decode_us` 均为零；
lookup-only 每种都有文件请求和解码。该结果只验证小工作集的解码复用与
指标区间隔离。矩阵期间同容器正在编译独立 UBSan 引擎，CPU、延迟、cgroup
file/anon 和 guest I/O 不能用于性能结论；不是三档容量或混合 WAL/compaction
验收。所有轮次维持 `cold_unverified`，恢复校验本身会预热文件。
原始 CSV/每轮 cgroup sidecar/summary 位于外层证据目录
`docker-rebuild-20261024/cache-decode-smoke-results-20261024`。

持久数据库优先使用 Linux 文件系统的 named volume 或 Linux bind mount，并核实实际底层文件系统；二者的性能差别需要实测。Windows NTFS bind 与 overlay 单独记录，不混合推断缓存热度。WAL 持久同步语义保持不变，不将 tmpfs 用作持久库，不执行全局 drop_caches 或修改宿主 sysctl。

当前继续保留范围扫描 lookup-only 和已有容量默认值。尚无依据提高 cache、引入压缩字节副本缓存或调整 direct I/O、mmap、fadvise。文件请求计数只能证明发出了多少文件读取请求，不能证明物理设备 I/O；guest 设备和宿主 VHDX I/O 也不能混为一个口径。生产容量建议仍需在**生产 profile 的压力/混合负载**实验后再更新；本轮的 C4 与挂载只提供机制证据。

## 同库并发实验接口（W3 已完成，结果见上文「W3 C4 四路同库并发」与「W3 三种挂载 × 5 轮」）

`--workload mixed --read-existing` 仍表示只读交替负载。新增
`--write-existing --workload concurrent` 专用于可丢弃的独立副本：同一个
FactStore 内运行热点点读、宽列顺扫、正常同步 Commit 和 native maintenance
四条线程。固定四条路径，不接受额外 sessions 或 warmup。写入实体范围从
`entities + 1` 开始，fixture 范围的三版本行数与 checksum 单独核验。

示例（路径必须指向专用副本，命令会真实写入）：

```bash
timeout 120 /build/retest-debug/cedar_query_storage_read_bench \
  --db-path /build/dedicated-concurrent-copy --write-existing \
  --workload concurrent --entities 20000 --seed 42 --part-id 7 \
  --seconds 10 --cache-admission lookup-only \
  --storage-read-working-bytes 67108864 --query-memory-bytes 67108864
```

接口使用 debug-small-thresholds 存储 profile 产生维护债务，CSV 分开报告
profile 实际 cache 容量与请求容量；不能把请求的 cache-bytes 当作生效值。
该 profile 用于正确性和干扰机制验证，其延迟不能代替生产 profile 的容量结论。

该 fixture 的 reader metadata 自 T13 起显式传入（矩阵默认 **16 MiB**，CLI `--storage-reader-metadata-bytes`；0 保持 profile 自动/工厂 fallback），因此不再存在旧版“并发 64 KiB vs 只读 64 MiB”的口径差异。W3 的 C4 与三挂载都记录 `storage_reader_metadata_bytes=16777216`，并在 environment.json 标注“实验 fixture，非生产默认”；生产预算下 metadata 自动值为 1 GiB（见上文 T4 测量），两者不得混用。

每次比较须从同一已关闭 fixture 创建新的独立副本；named volume、Linux bind、
overlay 分别建立清单、采样区间与热度，不能连续重用已写入的库冒充相同初态。

输出每条路径的操作数、行数或维护输入字节、checksum 或维护输出字节、错误、
点读/提交分位数，以及整体 compaction 输入/输出、债务前后值和实际四路径
活动重叠时长。延迟数组只保留最初 65536 个样本，CSV 明确标记 first_65536。
只有全部路径成功、确实完成 facts compaction、该 compaction 与另三条路径
同时活动、观测到同步写且没有异步写、确实读取/解码 Parquet、后台错误未增加，
才标 accepted=true。超时、预算不足、无债务、没有重叠等情况须保留失败，
不能将维护请求次数当作 compaction 成功次数。

W3 已于 2026-10-25 完成：C4 **10/10 accepted**（named-volume，四路真实并发 + facts compaction 重叠 + 同步 WAL + 真实 Parquet 解码）；三挂载各 **40/40**（findmnt 实际 fstype/source 与 label 一致，120/120 运行成功）。2026-10-25 未做：bind/overlay 上的 C4、Windows NTFS bind（`unavailable`）、生产 profile 的容量/延迟结论。所有轮次 `cold_unverified`；fixture setup 的 64 MiB 失败样本保留。**2026-10-26 后续验收已补：bind/overlay 上的 C4 各 10/10、经 Windows 路径的挂载矩阵 40/40（实测 fstype=9p，非 NTFS 语义直通）、C3 三档各 40/40、全量 ASan/UBSan**；生产 profile 与真实冷读仍未验证。最终结论见 `_gates/final-acceptance-20261026/REPORT.md`（2026-10-25 历史基线见 `_gates/final-acceptance-20261025/REPORT.md`）。
