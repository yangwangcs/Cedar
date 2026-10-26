# Cedar 本机生产容量配置

日期：2026-10-13。

本配置用于 EURDATA 全量导入和后续生产容器。通用 Cedar API 中未显式填写的 memtable 与 worker 参数仍解析为原有默认值，避免小内存调用方被强制套用本机大容量配置。

## Docker 与内存

| 项目 | 实际配置 |
|---|---:|
| Docker Linux 可见内存 | 115,388,212 KiB，约 110.04 GiB |
| Cedar 生产内存预算 | Linux 可见内存的 50%，约 55.02 GiB |
| Docker CPU 限额 | 无，使用 Docker Desktop 总额度 |
| Docker 内存 / swap 限额 | 无，使用 Docker Desktop 总额度 |
| Docker PID 限额 | 无 |
| `nofile` | soft=hard=65,536 |

50% 是 Cedar 的受控预算，不是进程 RSS 上限。剩余内存供邻接索引、版本历史、事务状态、压缩与 flush 临时内存、分配器、线程栈和 Linux 文件页缓存使用。当前实现拒绝超过宿主可见内存 50% 的显式预算。

预算内部保持现有比例：facts WriteBufferManager 额度 25%，meta/default 共用额度 5%，共享 block cache 上限 55%。EURDATA 入口的查询 memory、projection cache、query delta 各为 256 MiB。

2026-10-24 归属说明：上述是配置额度，不是三份独立物理占用。
非 developer profile 的 WBM 使用 shared cache reservation；reservation 对应的
实际 memtable 不能与该 charge 再重复相加。共享 cache 同时容纳 block 和 Cedar
解码列页；pin 是 resident 的子集。read-working、独立 projection cache、查询
输出及其他开销需分别观察，重叠的查询/DB reservation 不代表两份分配。
本页 2026-10-13 的镜像与容量观测是历史结果，DockerData 重建后不再代表当前
容器状态；新分层缓存尚未完成生产压力验收，不据此修改本页生产容量数值。
参见[列式缓存与部署验证](../docs/columnar-cache-and-deployment.md)。

2026-10-25 补充（**不改本页任何数值**）：reader metadata 的自动上限改为
`max(4096, min(1 GiB, storage_memory_budget / 16))`，并计入 Open 容量预检；
这只是归属与预检解释的变化，本页容量数字与比例保持原样。生产形状测量（T4）显示
55 GiB 预算下 36 个 512 MiB L0 的 metadata peak 约占 1 GiB 的 61.5%，1 GiB 的
break-even 约为每文件 5.37M facts；但该数据来自 synthetic/lab fixture（synthetic
48.107 B/fact vs 仓库生产 fixture 162.56 B/fact），**尚未在生产容器内存压力下验收**。
容量规划仍须按实际数据密度、L0 数量与压力测试复核，不能把本页额度当作已验收峰值。

2026-10-26 补充（**仍然不改本页任何数值**）：本阶段完成的是 C3 三档
read-shape **性能矩阵**（small/medium/large 各 40/40，128 MiB / swap0 / 2 CPU
专用容器，全部 `cold_unverified`）与 bind/overlay 上的 C4 机制验证，不是容量
验收；它不产生新的容量结论，因此本页 memtable、cache、比例与镜像数值保持
2026-10-13 原样。归属提醒：C3 的“文件请求/列解码归零”只在工作集可驻留缓存时
出现（small 档与 medium/large 的 point），扫描无一致收益，不能据此调整
`block_cache_bytes`、reader metadata 或其他容量额度；生产 profile 的混合负载与
真实冷读仍未验证。证据见
`_gates/final-acceptance-20261026/{03-c3,04-c4-mounts,05-ntfs}/` 与同目录 REPORT。

## RocksDB 列族

| 项目 | facts | meta | default |
|---|---:|---:|---:|
| 单个 memtable | 512 MiB | 128 MiB | 64 MiB |
| 最大 memtable 数 | 4 | 3 | 3 |
| 每次 flush 最少合并 | 2 | 1 | 1 |
| L0 compaction 触发 | 8 个 SST | 8 个 SST | 8 个 SST |
| L0 开始减速写入 | 24 个 SST | 24 个 SST | 24 个 SST |
| L0 停止写入 | 36 个 SST | 36 个 SST | 36 个 SST |
| 目标 SST 大小 | 512 MiB | 64 MiB | 64 MiB |
| 表与压缩 | CedarParquet，页 LZ4 Raw | BlockBased，LZ4；bottommost ZSTD level 3 | BlockBased，RocksDB 默认压缩策略 |

显式容量会校验 facts 最大 memtable 容量不超过 facts 写缓冲额度，并校验 meta 与 default 的合计最大容量不超过共享的 meta/default 额度。memtable 数、worker 数和 background job 数在传给 RocksDB 前执行范围与溢出检查；facts 至少保留 2 个 memtable，以满足每次 flush 合并 2 个表的配置。

## 写入与后台维护

| 项目 | 配置 |
|---|---:|
| 客户端 workers | 64 |
| group commit 窗口 | 1,000 us |
| group commit 最大事务数 | 128 |
| group commit 最大字节数 | 2 MiB |
| WAL | 开启并同步确认 |
| flush workers | 2 |
| compaction workers | 4 |
| `max_background_jobs` | 6 |
| `max_subcompactions` | 2 |
| normal flush credits | 2 |
| emergency flush credits | 2 |
| compaction credits | 2 |

多个业务事务可以共享一次 WAL sync，但事务边界、冲突检查和确认语义不变。

## 镜像与验收证据

生产镜像：`cedar:persistent-production-capacity-64w-20261013`

镜像 ID：`sha256:9ce6ad65079486e423fb50214b468f9d67a11cd68963407ee327e17ceb3cf201`

构建入口：`E:\\wy_cedar_test\\docker\\build-cedar-production.ps1`。Dockerfile 通过命名构建上下文显式复制当前 Cedar 源码和 EURDATA 入口，不依赖基础镜像中的旧源码。

临时空输入容器实际创建 `/data/db/OPTIONS-000007`。文件中确认：

- DB：`max_background_jobs=6`、`max_background_flushes=2`、`max_background_compactions=4`、`max_subcompactions=2`。
- facts：`write_buffer_size=536870912`、`max_write_buffer_number=4`、`min_write_buffer_number_to_merge=2`、L0 `8/24/36`、`target_file_size_base=536870912`。
- meta：`write_buffer_size=134217728`、`max_write_buffer_number=3`、L0 `8/24/36`、LZ4、bottommost ZSTD level 3。
- default：`write_buffer_size=67108864`、`max_write_buffer_number=3`、L0 `8/24/36`。
- `max_total_wal_size=35447258726`，按当前派生公式对应 59,078,764,544 字节生产预算，即 Docker 可见内存的 50%。

`commit-config.tsv` 确认 64 clients、1,000 us、128 tx、2 MiB，以及上述 memtable 和维护容量。检查容器 `Memory=0`、`MemorySwap=0`、`NanoCpus=0`、`CpuQuota=0`、空 cpuset、无 PID 限额、`nofile=65536`、未发生 OOM。

空输入验收只用于让 RocksDB 生成配置，因缺少 EURDATA 二进制输入以 `binary magic mismatch` 退出，这是预期的 fixture 输入错误，不是数据库打开或配置错误。本次未运行性能 benchmark。

原正式导入容器 `cedar-eurfull-repair-r3-full-20261012-import` 保持运行，仍使用原镜像和启动时配置；本次没有停止、替换或修改其数据卷。
