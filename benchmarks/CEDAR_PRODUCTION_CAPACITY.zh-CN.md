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
