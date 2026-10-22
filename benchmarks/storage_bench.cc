// Cedar 存储层基准（P0 基线，Task 0.1）
//
// 覆盖计划的 4 个 case：
//   1. VSL 单线程 / 8 线程插入吞吐
//   2. MemTable GetAtTime 延迟
//   3. Zone-Columnar SST 1 / 8192 / 8193 行写入与点读/扫描
//   4. 32 路 Compaction 合并吞吐
//
// 用法：
//   ./build/benchmarks/storage_bench            # 可读 JSON（缩进）
//   ./build/benchmarks/storage_bench --json     # 紧凑 JSON（用于 baseline.json）
//
// 输出为 JSON，字段在 case.params / case.metrics 中；所有值均为 JSON 字面量。

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sys/utsname.h>
#include <unistd.h>

#include "cedar/core/env.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/wal.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/storage/versioned_skiplist_lockfree.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

#ifndef CEDAR_BENCH_GIT_COMMIT
#define CEDAR_BENCH_GIT_COMMIT "unknown"
#endif
#ifndef CEDAR_BENCH_BUILD_TYPE
#define CEDAR_BENCH_BUILD_TYPE "unknown"
#endif
#ifndef CEDAR_BENCH_COMPILER
#define CEDAR_BENCH_COMPILER "unknown"
#endif

namespace {

using namespace cedar;

using Clock = std::chrono::steady_clock;

uint64_t NowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count());
}

std::string JsonEscape(const std::string& raw) {
  std::string out;
  out.reserve(raw.size() + 8);
  for (char c : raw) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default: out.push_back(c);
    }
  }
  return out;
}

std::string JsonStr(const std::string& s) { return "\"" + JsonEscape(s) + "\""; }

std::string FormatDouble(double v, int precision) {
  std::ostringstream oss;
  oss.setf(std::ios::fixed);
  oss.precision(precision);
  oss << v;
  return oss.str();
}

std::string NowIso8601() {
  std::time_t t = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return buf;
}

// 从延迟样本计算分位数（样本本身会被排序的副本处理）
double Percentile(const std::vector<uint64_t>& samples, double p) {
  if (samples.empty()) return 0.0;
  std::vector<uint64_t> sorted = samples;
  std::sort(sorted.begin(), sorted.end());
  size_t idx = static_cast<size_t>(p * static_cast<double>(sorted.size() - 1) + 0.5);
  if (idx >= sorted.size()) idx = sorted.size() - 1;
  return static_cast<double>(sorted[idx]);
}

struct BenchCase {
  std::string name;
  std::vector<std::pair<std::string, std::string>> params;
  std::vector<std::pair<std::string, std::string>> metrics;

  void ParamInt(const std::string& k, long long v) {
    params.emplace_back(k, std::to_string(v));
  }
  void ParamStr(const std::string& k, const std::string& v) {
    params.emplace_back(k, JsonStr(v));
  }
  void MetricInt(const std::string& k, long long v) {
    metrics.emplace_back(k, std::to_string(v));
  }
  void MetricDouble(const std::string& k, double v, int precision = 3) {
    metrics.emplace_back(k, FormatDouble(v, precision));
  }
};

std::string MakeTempDir(const std::string& tag) {
  const std::string dir = "/tmp/cedar_bench_" + tag + "_" +
                          std::to_string(::getpid()) + "_" +
                          std::to_string(NowNs());
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    std::fprintf(stderr, "storage_bench: cannot create temp dir %s: %s\n",
                 dir.c_str(), ec.message().c_str());
    std::exit(1);
  }
  return dir;
}

// 构建一个 Zone-Columnar SST 文件。
// rows 顺序必须与 CedarKey::Compare 一致（entity 升序；同 entity 时 timestamp 降序）。
bool BuildSst(const std::string& dir, uint64_t file_number, uint16_t column_id,
              EntityType type, const std::vector<std::pair<CedarKey, Descriptor>>& rows,
              std::string* out_path) {
  const std::string path = dir + "/" + std::to_string(file_number) + ".frond";
  WritableFile* file = nullptr;
  Status s = Env::Default()->NewWritableFile(path, &file);
  if (!s.ok() || file == nullptr) return false;

  CedarOptions options;
  options.env = Env::Default();
  ZoneColumnarSstBuilder builder(options, file, column_id, /*db_path=*/"");
  builder.SetFileNumber(file_number);
  builder.SetLevel(0);
  for (const auto& [key, value] : rows) {
    builder.Add(key, value);
  }
  s = builder.Finish();
  delete file;  // 析构时关闭
  if (!s.ok()) {
    std::fprintf(stderr, "storage_bench: build sst failed: %s\n", s.ToString().c_str());
    return false;
  }
  *out_path = path;
  return true;
}

// ---------------------------------------------------------------------------
// Case 1: VSL 插入吞吐（单线程 / 8 线程）
// ---------------------------------------------------------------------------
BenchCase RunVslInsert(int threads, uint64_t per_thread) {
  const uint64_t rows = per_thread * static_cast<uint64_t>(threads);
  BenchCase c;
  c.name = threads == 1 ? "vsl_insert_1t" : ("vsl_insert_" + std::to_string(threads) + "t");
  c.ParamInt("threads", threads);
  c.ParamInt("rows", static_cast<long long>(rows));

  LockFreeVSL vsl;
  const uint64_t t0 = NowNs();
  if (threads == 1) {
    for (uint64_t i = 0; i < rows; ++i) {
      const uint64_t eid = i + 1;
      vsl.Insert(CedarKey::Vertex(eid, 1, Timestamp(1000 + i)),
                 Descriptor::InlineInt(1, static_cast<int32_t>(i % 1000000)), Timestamp(1));
    }
  } else {
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));
    for (int t = 0; t < threads; ++t) {
      workers.emplace_back([&vsl, t, per_thread, threads]() {
        for (uint64_t i = 0; i < per_thread; ++i) {
          // 交叉分布 key，制造真实的并发插入竞争
          const uint64_t eid = i * static_cast<uint64_t>(threads) +
                               static_cast<uint64_t>(t) + 1;
          vsl.Insert(CedarKey::Vertex(eid, 1, Timestamp(1000 + i)),
                     Descriptor::InlineInt(1, static_cast<int32_t>(i % 1000000)),
                     Timestamp(1));
        }
      });
    }
    for (auto& w : workers) w.join();
  }
  const uint64_t elapsed_ns = NowNs() - t0;
  const uint64_t elapsed_ms = std::max<uint64_t>(1, elapsed_ns / 1000000);

  // 记录可遍历到的行数：并发插入丢节点时该值 < rows（审计 1.1 的正确性信号）。
  // 上限 2*rows，防止并发插入损坏 level-0 链成环时卡死基准（审计 1.7）。
  size_t traversed = 0;
  const size_t traverse_cap = static_cast<size_t>(rows) * 2 + 16;
  vsl.Traverse([&traversed, traverse_cap](const CedarKey&, const Descriptor&) {
    ++traversed;
    return traversed < traverse_cap;
  });

  c.MetricInt("elapsed_ms", static_cast<long long>(elapsed_ms));
  c.MetricDouble("ops_per_sec",
                 static_cast<double>(rows) * 1e9 / static_cast<double>(elapsed_ns), 1);
  c.MetricDouble("ns_per_op",
                 static_cast<double>(elapsed_ns) / static_cast<double>(rows), 1);
  c.MetricInt("traversed_rows", static_cast<long long>(traversed));
  return c;
}

// ---------------------------------------------------------------------------
// Case 2: MemTable GetAtTime 延迟
// ---------------------------------------------------------------------------
BenchCase RunMemtableGetAtTime(uint64_t rows, int queries) {
  BenchCase c;
  c.name = "memtable_getattime";
  c.ParamInt("rows", static_cast<long long>(rows));
  c.ParamInt("queries", queries);

  VSLMemTable mem;
  for (uint64_t i = 0; i < rows; ++i) {
    mem.Put(CedarKey::Vertex(i + 1, 1, Timestamp(1000 + i)),
            Descriptor::InlineInt(1, static_cast<int32_t>(i)), Timestamp(1));
  }

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<uint64_t> dist(1, rows);
  std::vector<uint64_t> latencies;
  latencies.reserve(static_cast<size_t>(queries));
  int hits = 0;
  const uint64_t query_ts = 1000 + rows + 1000000;  // 大于所有版本
  for (int q = 0; q < queries; ++q) {
    const uint64_t eid = dist(rng);
    const uint64_t t0 = NowNs();
    auto got = mem.GetAtTime(eid, EntityType::Vertex, 1, Timestamp(query_ts));
    const uint64_t dt = NowNs() - t0;
    latencies.push_back(dt);
    if (got.has_value()) ++hits;
  }

  const double total_ns = static_cast<double>(std::accumulate(
      latencies.begin(), latencies.end(), uint64_t{0}));
  c.MetricInt("hits", hits);
  c.MetricDouble("p50_ns", Percentile(latencies, 0.50), 0);
  c.MetricDouble("p99_ns", Percentile(latencies, 0.99), 0);
  c.MetricDouble("mean_ns", total_ns / static_cast<double>(queries), 1);
  c.MetricDouble("ops_per_sec",
                 static_cast<double>(queries) * 1e9 / std::max(1.0, total_ns), 1);
  return c;
}

// ---------------------------------------------------------------------------
// Case 3: Zone-Columnar SST 写入 / 点读 / 扫描
// ---------------------------------------------------------------------------
BenchCase RunSstCase(uint64_t rows, int measured_queries) {
  BenchCase c;
  c.name = "sst_write_read_" + std::to_string(rows);
  c.ParamInt("rows", static_cast<long long>(rows));
  c.ParamInt("measured_queries", measured_queries);
  c.ParamStr("column", "vertex_col_10");

  const std::string dir = MakeTempDir("sst" + std::to_string(rows));
  constexpr uint16_t kColumn = 10;
  const uint64_t file_number = 1;
  const uint64_t base_entity = 1;
  const uint64_t base_ts = 1000;

  std::vector<std::pair<CedarKey, Descriptor>> entries;
  entries.reserve(static_cast<size_t>(rows));
  for (uint64_t i = 0; i < rows; ++i) {
    entries.emplace_back(
        CedarKey::Vertex(base_entity + i, kColumn, Timestamp(base_ts + i)),
        Descriptor::InlineInt(kColumn, static_cast<int32_t>(i % 1000000)));
  }

  std::string path;
  const uint64_t write_t0 = NowNs();
  const bool built = BuildSst(dir, file_number, kColumn, EntityType::Vertex, entries, &path);
  const uint64_t write_ns = NowNs() - write_t0;
  entries.clear();
  entries.shrink_to_fit();
  if (!built) {
    c.MetricInt("build_ok", 0);
    std::filesystem::remove_all(dir);
    return c;
  }
  c.MetricInt("build_ok", 1);
  c.MetricInt("file_size_bytes",
              static_cast<long long>(std::filesystem::file_size(path)));
  c.MetricDouble("write_rows_per_sec",
                 static_cast<double>(rows) * 1e9 / std::max<uint64_t>(1, write_ns), 1);

  const uint64_t open_t0 = NowNs();
  ZoneColumnarSstReader reader(path);
  const Status open_status = reader.Open();
  const uint64_t open_ns = NowNs() - open_t0;
  c.MetricInt("open_ok", open_status.ok() ? 1 : 0);
  c.MetricDouble("open_ms", static_cast<double>(open_ns) / 1e6, 3);
  if (!open_status.ok()) {
    std::filesystem::remove_all(dir);
    return c;
  }
  c.MetricInt("reader_num_entries", static_cast<long long>(reader.NumEntries()));

  // 点读：预热少量查询以避开惰性索引构建的首批开销，然后测量
  std::mt19937_64 rng(7);
  std::uniform_int_distribution<uint64_t> dist(0, rows - 1);
  int hits = 0;
  const int warmup = static_cast<int>(std::min<uint64_t>(rows, 16));
  for (int q = 0; q < warmup; ++q) {
    const uint64_t i = dist(rng);
    (void)reader.Get(CedarKey::Vertex(base_entity + i, kColumn, Timestamp(base_ts + i)));
  }

  std::vector<uint64_t> latencies;
  latencies.reserve(static_cast<size_t>(measured_queries));
  for (int q = 0; q < measured_queries; ++q) {
    const uint64_t i = dist(rng);
    const uint64_t t0 = NowNs();
    auto got = reader.Get(CedarKey::Vertex(base_entity + i, kColumn, Timestamp(base_ts + i)));
    const uint64_t dt = NowNs() - t0;
    latencies.push_back(dt);
    if (got.has_value()) ++hits;
  }
  c.MetricInt("get_hits", hits);
  c.MetricDouble("get_p50_ns", Percentile(latencies, 0.50), 0);
  c.MetricDouble("get_p99_ns", Percentile(latencies, 0.99), 0);

  // 全量扫描
  std::unique_ptr<ZoneColumnarIterator> it(reader.NewIterator());
  size_t scanned = 0;
  uint64_t sum_value = 0;
  const uint64_t scan_t0 = NowNs();
  if (it) {
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ++scanned;
      sum_value += it->Value().AsRaw();
    }
  }
  const uint64_t scan_ns = NowNs() - scan_t0;
  c.MetricInt("scan_rows", static_cast<long long>(scanned));
  c.MetricDouble("scan_rows_per_sec",
                 static_cast<double>(scanned) * 1e9 / std::max<uint64_t>(1, scan_ns), 1);
  c.MetricInt("scan_value_checksum", static_cast<long long>(sum_value));

  reader.Close();
  std::filesystem::remove_all(dir);
  return c;
}

// ---------------------------------------------------------------------------
// Case 4: 32 路 Compaction 合并吞吐
// ---------------------------------------------------------------------------
BenchCase RunMerge32Way(int input_files, uint64_t rows_per_file) {
  BenchCase c;
  c.name = "compaction_merge_" + std::to_string(input_files) + "way";
  c.ParamInt("input_files", input_files);
  c.ParamInt("rows_per_file", static_cast<long long>(rows_per_file));

  const std::string dir = MakeTempDir("merge");
  constexpr uint16_t kColumn = 10;
  constexpr uint64_t kFirstFileNumber = 100;

  SizeTieredConfig config;
  config.enable_background_compaction = false;  // 同步测量，不启动后台线程
  config.compaction_threads = 0;
  config.l0_max_size = 1;               // 任一层有数据即满足容量触发
  config.level_size_trigger_ratio = 0.0;
  config.size_ratio = 1e9;              // 阻止 L1+ 级联合并
  config.max_levels = 7;

  SizeTieredCompactionEngine engine(dir, config, Env::Default());
  Status s = engine.Open();
  if (!s.ok()) {
    c.MetricInt("open_ok", 0);
    std::filesystem::remove_all(dir);
    return c;
  }
  c.MetricInt("open_ok", 1);

  const uint64_t write_t0 = NowNs();
  for (int f = 0; f < input_files; ++f) {
    const uint64_t file_number = kFirstFileNumber + static_cast<uint64_t>(f);
    std::vector<std::pair<CedarKey, Descriptor>> entries;
    entries.reserve(static_cast<size_t>(rows_per_file));
    for (uint64_t i = 0; i < rows_per_file; ++i) {
      // 文件间 key 交叉分布，让 K 路归并真正交错
      const uint64_t eid = i * static_cast<uint64_t>(input_files) +
                           static_cast<uint64_t>(f) + 1;
      entries.emplace_back(
          CedarKey::Vertex(eid, kColumn, Timestamp(1000 + i)),
          Descriptor::InlineInt(kColumn, static_cast<int32_t>(i % 1000000)));
    }
    std::string path;
    if (!BuildSst(dir, file_number, kColumn, EntityType::Vertex, entries, &path)) {
      c.MetricInt("build_ok", 0);
      engine.Close();
      std::filesystem::remove_all(dir);
      return c;
    }
    ZoneSstMeta meta;
    meta.file_number = file_number;
    meta.file_size = std::filesystem::file_size(path);
    meta.num_entries = rows_per_file;
    meta.level = 0;
    meta.column_id = kColumn;
    meta.entity_type = static_cast<uint8_t>(EntityType::Vertex);
    meta.min_entity_id = 1;
    meta.max_entity_id = rows_per_file * static_cast<uint64_t>(input_files);
    meta.min_timestamp = 1000;
    meta.max_timestamp = 1000 + rows_per_file;
    meta.path = path;
    s = engine.AddSSTFile(meta);
    if (!s.ok()) {
      std::fprintf(stderr, "storage_bench: AddSSTFile failed: %s\n", s.ToString().c_str());
      engine.Close();
      std::filesystem::remove_all(dir);
      c.MetricInt("build_ok", 0);
      return c;
    }
  }
  const uint64_t write_ns = NowNs() - write_t0;
  c.MetricInt("build_ok", 1);
  c.MetricDouble("build_ms", static_cast<double>(write_ns) / 1e6, 3);

  auto task = engine.PickNextCompaction();
  if (!task.has_value()) {
    c.MetricInt("pick_ok", 0);
    engine.Close();
    std::filesystem::remove_all(dir);
    return c;
  }
  c.MetricInt("pick_ok", 1);
  c.MetricInt("picked_input_files", static_cast<long long>(task->input_files.size()));

  const uint64_t merge_t0 = NowNs();
  s = engine.ExecuteCompaction(*task);
  const uint64_t merge_ns = NowNs() - merge_t0;
  c.MetricInt("merge_ok", s.ok() ? 1 : 0);
  c.MetricDouble("merge_ms", static_cast<double>(merge_ns) / 1e6, 3);
  if (!s.ok()) {
    std::fprintf(stderr, "storage_bench: ExecuteCompaction failed: %s\n",
                 s.ToString().c_str());
  }

  uint64_t output_rows = 0;
  for (const auto& meta : engine.GetLevelFiles(1)) {
    output_rows += meta.num_entries;
  }
  c.MetricInt("output_rows", static_cast<long long>(output_rows));
  c.MetricInt("output_files", static_cast<long long>(engine.GetLevelFiles(1).size()));
  c.MetricDouble("merge_rows_per_sec",
                 static_cast<double>(output_rows) * 1e9 /
                     std::max<uint64_t>(1, merge_ns), 1);
  c.MetricDouble("merge_input_rows_per_sec",
                 static_cast<double>(static_cast<uint64_t>(input_files) * rows_per_file) *
                     1e9 / std::max<uint64_t>(1, merge_ns), 1);

  engine.WaitForCompactions();
  engine.Close();
  std::filesystem::remove_all(dir);
  return c;
}


// ---------------------------------------------------------------------------
// t9: 直接写 WAL 化 + 组提交（写吞吐 before/after）
// ---------------------------------------------------------------------------

// 直接写（LsmEngine::Put）吞吐：wal_sync=false 走 append+flush（默认），
// true 走 append+fsync（每次 ~F_FULLFSYNC 代价）。
BenchCase RunLsmDirectPut(int threads, uint64_t per_thread, bool wal_sync,
                          bool enable_wal = true) {
  constexpr uint16_t kColumn = 10;
  const uint64_t rows = per_thread * static_cast<uint64_t>(threads);
  BenchCase c;
  c.name = enable_wal ? (std::string("lsm_put_wal_") + (wal_sync ? "sync_" : "flush_") +
                         std::to_string(threads) + "t")
                      : ("lsm_put_no_wal_" + std::to_string(threads) + "t");
  c.ParamInt("threads", threads);
  c.ParamInt("rows", static_cast<long long>(rows));
  c.ParamStr("durability", !enable_wal ? "none(no_wal)" : (wal_sync ? "fsync" : "flush_os"));

  const std::string dir = MakeTempDir("lsm_put");
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  options.enable_wal = enable_wal;
  options.write_buffer_size = 256 * 1024 * 1024;  // 避免自动 flush 干扰写路径

  LsmEngine engine(dir, options, Env::Default());
  engine.SetWALGroupCommitWindow(1000);
  engine.SetDirectWriteSync(wal_sync);
  Status s = engine.Open();
  if (!s.ok()) {
    c.MetricInt("open_ok", 0);
    std::filesystem::remove_all(dir);
    return c;
  }
  c.MetricInt("open_ok", 1);

  std::atomic<uint64_t> failures{0};
  const uint64_t t0 = NowNs();
  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(threads));
  for (int t = 0; t < threads; ++t) {
    workers.emplace_back([&engine, &failures, t, per_thread]() {
      for (uint64_t i = 0; i < per_thread; ++i) {
        const uint64_t entity =
            1 + static_cast<uint64_t>(t) * per_thread + i;  // 每个线程唯一键
        const CedarKey key = CedarKey::Vertex(entity, kColumn, Timestamp(1000));
        if (!engine.Put(key, Descriptor::InlineInt(kColumn, static_cast<int32_t>(i % 1000)),
                        Timestamp(1)).ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& worker : workers) worker.join();
  const uint64_t elapsed = NowNs() - t0;
  c.MetricInt("failures", static_cast<long long>(failures.load()));

  if (engine.GetWalWriter() != nullptr) {
    const WalStats wal = engine.GetWalWriter()->GetStats();
    const uint64_t syncs = wal.syncs.load();
    c.MetricInt("wal_records", static_cast<long long>(wal.records_written.load()));
    c.MetricInt("wal_syncs", static_cast<long long>(syncs));
    c.MetricDouble("wal_records_per_sync",
                   syncs == 0 ? 0.0
                              : static_cast<double>(wal.records_written.load()) /
                                    static_cast<double>(syncs), 2);
  }
  c.MetricDouble("us_per_op", static_cast<double>(elapsed) / 1000.0 /
                                  static_cast<double>(std::max<uint64_t>(1, rows)), 3);
  c.MetricDouble("ops_per_sec",
                 static_cast<double>(rows) * 1e9 /
                     static_cast<double>(std::max<uint64_t>(1, elapsed)), 1);

  engine.Close();
  std::filesystem::remove_all(dir);
  return c;
}

// 事务提交吞吐：window_us=0 等价 Task 1.2 的"每提交一次 fsync 且无组提交"，
// window_us>0 为 t9 的组提交。两者都是 kSync（fsync）持久化级别。
BenchCase RunTxnCommit(int threads, uint64_t per_thread, uint32_t window_us) {
  constexpr uint16_t kColumn = 10;
  const uint64_t txns = per_thread * static_cast<uint64_t>(threads);
  BenchCase c;
  c.name = std::string("txn_commit_") + (window_us == 0 ? "sync_" : "group_") +
           std::to_string(threads) + "t";
  c.ParamInt("threads", threads);
  c.ParamInt("txns", static_cast<long long>(txns));
  c.ParamInt("group_window_us", static_cast<long long>(window_us));

  const std::string dir = MakeTempDir("txn_commit");
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  options.enable_wal = true;
  options.write_buffer_size = 256 * 1024 * 1024;

  LsmEngine engine(dir, options, Env::Default());
  engine.SetWALGroupCommitWindow(window_us);
  Status s = engine.Open();
  if (!s.ok()) {
    c.MetricInt("open_ok", 0);
    std::filesystem::remove_all(dir);
    return c;
  }
  c.MetricInt("open_ok", 1);

  std::atomic<uint64_t> failures{0};
  const uint64_t t0 = NowNs();
  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(threads));
  for (int t = 0; t < threads; ++t) {
    workers.emplace_back([&engine, &failures, t, per_thread]() {
      for (uint64_t i = 0; i < per_thread; ++i) {
        auto txn = engine.BeginTransaction();
        if (!txn) {
          failures.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        const uint64_t entity =
            1 + static_cast<uint64_t>(t) * per_thread + i;  // 每个线程唯一键
        if (!txn->Put(entity, EntityType::Vertex, kColumn,
                      Descriptor::InlineInt(kColumn, static_cast<int32_t>(i % 1000)),
                      Timestamp(1000)).ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        if (!txn->Commit().ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& worker : workers) worker.join();
  const uint64_t elapsed = NowNs() - t0;
  c.MetricInt("failures", static_cast<long long>(failures.load()));

  const WalStats wal = engine.GetWalWriter()->GetStats();
  const uint64_t syncs = wal.syncs.load();
  c.MetricInt("wal_records", static_cast<long long>(wal.records_written.load()));
  c.MetricInt("wal_syncs", static_cast<long long>(syncs));
  c.MetricDouble("wal_records_per_sync",
                 syncs == 0 ? 0.0
                            : static_cast<double>(wal.records_written.load()) /
                                  static_cast<double>(syncs), 2);
  c.MetricDouble("us_per_commit", static_cast<double>(elapsed) / 1000.0 /
                                      static_cast<double>(std::max<uint64_t>(1, txns)), 3);
  c.MetricDouble("commits_per_sec",
                 static_cast<double>(txns) * 1e9 /
                     static_cast<double>(std::max<uint64_t>(1, elapsed)), 1);

  engine.Close();
  std::filesystem::remove_all(dir);
  return c;
}

// ---------------------------------------------------------------------------
// t10: WAL checkpoint / 截断
// ---------------------------------------------------------------------------

uint64_t CountFilesWithSuffix(const std::string& dir, const std::string& suffix) {
  uint64_t count = 0;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (!entry.is_regular_file()) continue;
    const std::string path = entry.path().string();
    if (path.size() >= suffix.size() &&
        path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0) {
      ++count;
    }
  }
  return count;
}

uint64_t DirBytesWithSuffix(const std::string& dir, const std::string& suffix) {
  uint64_t total = 0;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (!entry.is_regular_file()) continue;
    const std::string path = entry.path().string();
    if (path.size() >= suffix.size() &&
        path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0) {
      std::error_code size_ec;
      const auto size = std::filesystem::file_size(entry.path(), size_ec);
      if (!size_ec) total += static_cast<uint64_t>(size);
    }
  }
  return total;
}

// flush 成本 before/after：
//  - truncation=on ：本轮 SST 每个都 fsync（F_FULLFSYNC）+ checkpoint 持久化
//                    （以 MANIFEST 开头的文件，Sync 顺带 fsync 目录）→ 才能删 WAL
//  - truncation=off：t7 快路径（SST 只 Flush，不 fsync；WAL 不回收）
BenchCase RunFlushWithTruncation(uint64_t rows, uint16_t columns,
                                 bool truncation_on) {
  BenchCase c;
  c.name = std::string("flush_wal_truncation_") + (truncation_on ? "on" : "off");
  c.ParamInt("rows", static_cast<long long>(rows));
  c.ParamInt("columns", columns);
  c.ParamStr("sst_durability", truncation_on ? "fsync_per_sst+checkpoint"
                                             : "flush_only(t7)");
  c.ParamStr("wal_truncation", truncation_on ? "on" : "off");

  const std::string dir = MakeTempDir("flush_trunc");
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  options.enable_wal = true;
  options.write_buffer_size = 256 * 1024 * 1024;  // 写阶段不自动 flush

  LsmEngine engine(dir, options, Env::Default());
  engine.SetWALTruncationEnabled(truncation_on);
  Status s = engine.Open();
  c.MetricInt("open_ok", s.ok() ? 1 : 0);
  if (!s.ok()) {
    std::filesystem::remove_all(dir);
    return c;
  }

  const uint64_t entities =
      rows / (columns == 0 ? 1 : static_cast<uint64_t>(columns));
  uint64_t write_failures = 0;
  for (uint64_t e = 0; e < entities; ++e) {
    for (uint16_t col = 0; col < columns; ++col) {
      if (!engine
               .Put(CedarKey::Vertex(1 + e, col, Timestamp(1000)),
                    Descriptor::InlineInt(col, static_cast<int32_t>(e % 1000)),
                    Timestamp(1))
               .ok()) {
        ++write_failures;
      }
    }
  }
  c.MetricInt("write_failures", static_cast<long long>(write_failures));

  const uint64_t t0 = NowNs();
  const Status flush_status = engine.ForceFlush();
  const uint64_t flush_ns = NowNs() - t0;

  c.MetricInt("flush_ok", flush_status.ok() ? 1 : 0);
  c.MetricDouble("flush_ms", static_cast<double>(flush_ns) / 1e6, 2);
  c.MetricDouble("flush_rows_per_sec",
                 static_cast<double>(rows) * 1e9 /
                     static_cast<double>(std::max<uint64_t>(1, flush_ns)), 1);
  c.MetricInt("sst_files", static_cast<long long>(CountFilesWithSuffix(dir, ".frond")));
  c.MetricInt("wal_files", static_cast<long long>(CountFilesWithSuffix(dir + "/wal", ".wal")));
  c.MetricInt("wal_bytes", static_cast<long long>(DirBytesWithSuffix(dir + "/wal", ".wal")));
  c.MetricInt("checkpoint_exists",
              std::filesystem::exists(dir + "/MANIFEST-wal-checkpoint") ? 1 : 0);
  c.MetricInt("watermark_after_flush",
              static_cast<long long>(engine.GetWALCheckpointWatermark()));

  engine.Close();
  c.MetricInt("wal_files_after_close",
              static_cast<long long>(CountFilesWithSuffix(dir + "/wal", ".wal")));
  c.MetricInt("wal_bytes_after_close",
              static_cast<long long>(DirBytesWithSuffix(dir + "/wal", ".wal")));
  std::filesystem::remove_all(dir);
  return c;
}

// 长跑：每轮"3 条写 + flush"，观察 WAL 目录是否随轮次有界。
BenchCase RunWalCheckpointLongRun(int rounds) {
  constexpr uint16_t kColumn = 12;
  BenchCase c;
  c.name = "wal_checkpoint_long_run_" + std::to_string(rounds) + "r";
  c.ParamInt("rounds", rounds);
  c.ParamInt("rows_per_round", 3);

  const std::string dir = MakeTempDir("wal_long_run");
  CedarOptions options;
  options.create_if_missing = true;
  options.env = Env::Default();
  options.enable_wal = true;

  LsmEngine engine(dir, options, Env::Default());
  Status s = engine.Open();
  c.MetricInt("open_ok", s.ok() ? 1 : 0);
  if (!s.ok()) {
    std::filesystem::remove_all(dir);
    return c;
  }

  uint64_t max_wal_files = 0;
  uint64_t max_wal_bytes = 0;
  uint64_t flush_failures = 0;
  const uint64_t t0 = NowNs();
  for (int round = 0; round < rounds; ++round) {
    for (int row = 0; row < 3; ++row) {
      const uint64_t entity =
          10 + static_cast<uint64_t>(round) * 10 + static_cast<uint64_t>(row);
      engine.Put(CedarKey::Vertex(entity, kColumn, Timestamp(2000)),
                 Descriptor::InlineInt(kColumn, round * 10 + row),
                 Timestamp(static_cast<uint64_t>(round) + 1));
    }
    if (!engine.ForceFlush().ok()) ++flush_failures;
    max_wal_files = std::max(max_wal_files,
                             CountFilesWithSuffix(dir + "/wal", ".wal"));
    max_wal_bytes = std::max(max_wal_bytes,
                             DirBytesWithSuffix(dir + "/wal", ".wal"));
  }
  const uint64_t elapsed = NowNs() - t0;

  c.MetricInt("flush_failures", static_cast<long long>(flush_failures));
  c.MetricInt("max_wal_files", static_cast<long long>(max_wal_files));
  c.MetricInt("max_wal_bytes", static_cast<long long>(max_wal_bytes));
  c.MetricDouble("round_ms", static_cast<double>(elapsed) / 1e6 /
                                 static_cast<double>(std::max(1, rounds)), 3);
  c.MetricInt("watermark",
              static_cast<long long>(engine.GetWALCheckpointWatermark()));

  engine.Close();
  c.MetricInt("wal_files_after_close",
              static_cast<long long>(CountFilesWithSuffix(dir + "/wal", ".wal")));
  std::filesystem::remove_all(dir);
  return c;
}

// ---------------------------------------------------------------------------
// JSON 输出
// ---------------------------------------------------------------------------
void AppendFields(std::ostringstream& oss,
                  const std::vector<std::pair<std::string, std::string>>& fields,
                  bool pretty, const std::string& indent) {
  bool first = true;
  for (const auto& [k, v] : fields) {
    if (!first) oss << ",";
    first = false;
    if (pretty) oss << "\n" << indent << "  ";
    oss << JsonStr(k) << ":" << v;
  }
}

void PrintJson(const std::vector<std::pair<std::string, std::string>>& meta,
               const std::vector<BenchCase>& cases, bool pretty) {
  std::ostringstream oss;
  oss << "{";
  if (pretty) oss << "\n  ";
  oss << "\"meta\":{";
  AppendFields(oss, meta, pretty, "  ");
  if (pretty) oss << "\n  ";
  oss << "},";
  if (pretty) oss << "\n  ";
  oss << "\"cases\":[";
  for (size_t i = 0; i < cases.size(); ++i) {
    const BenchCase& c = cases[i];
    if (i > 0) oss << ",";
    if (pretty) oss << "\n    ";
    oss << "{";
    if (pretty) oss << "\n      ";
    oss << "\"name\":" << JsonStr(c.name) << ",";
    if (pretty) oss << "\n      ";
    oss << "\"params\":{";
    AppendFields(oss, c.params, pretty, "      ");
    if (pretty) oss << "\n      ";
    oss << "},";
    if (pretty) oss << "\n      ";
    oss << "\"metrics\":{";
    AppendFields(oss, c.metrics, pretty, "      ");
    if (pretty) oss << "\n      ";
    oss << "}";
    if (pretty) oss << "\n    ";
    oss << "}";
  }
  if (pretty && !cases.empty()) oss << "\n  ";
  oss << "]";
  if (pretty) oss << "\n";
  oss << "}";
  std::printf("%s\n", oss.str().c_str());
}

}  // namespace

int main(int argc, char** argv) {
  bool compact_json = false;
  uint64_t insert_rows = 100000;      // 每个 VSL case 的总行数
  uint64_t memtable_rows = 100000;    // 预先装入 memtable 的行数
  uint64_t merge_files = 32;          // 32 路合并
  uint64_t merge_rows_per_file = 2000;

  for (int i = 1; i < argc; ++i) {
    const char* arg = argv[i];
    auto match_u64 = [arg](const char* prefix, uint64_t* out) {
      const size_t len = std::strlen(prefix);
      if (std::strncmp(arg, prefix, len) != 0) return false;
      *out = std::strtoull(arg + len, nullptr, 10);
      return true;
    };
    if (std::strcmp(arg, "--json") == 0) {
      compact_json = true;
    } else if (std::strcmp(arg, "--help") == 0) {
      std::printf(
          "usage: %s [--json] [--rows=N] [--memtable-rows=N]\n"
          "          [--merge-files=N] [--merge-rows=N]\n",
          argv[0]);
      return 0;
    } else if (match_u64("--rows=", &insert_rows)) {
      // parsed
    } else if (match_u64("--memtable-rows=", &memtable_rows)) {
      // parsed
    } else if (match_u64("--merge-files=", &merge_files)) {
      // parsed
    } else if (match_u64("--merge-rows=", &merge_rows_per_file)) {
      // parsed
    } else {
      std::fprintf(stderr, "storage_bench: unknown argument %s (try --help)\n", arg);
      return 2;
    }
  }
  if (insert_rows < 8) insert_rows = 8;
  if (insert_rows % 8 != 0) insert_rows -= insert_rows % 8;
  if (merge_files < 1) merge_files = 1;

  utsname un{};
  std::string host = "unknown";
  std::string release = "unknown";
  std::string machine = "unknown";
  if (uname(&un) == 0) {
    host = un.nodename;
    release = un.release;
    machine = un.machine;
  }

  std::vector<std::pair<std::string, std::string>> meta;
  meta.emplace_back("schema", JsonStr("cedar-storage-bench/v1"));
  meta.emplace_back("timestamp_utc", JsonStr(NowIso8601()));
  meta.emplace_back("git_commit", JsonStr(CEDAR_BENCH_GIT_COMMIT));
  meta.emplace_back("build_type", JsonStr(CEDAR_BENCH_BUILD_TYPE));
  meta.emplace_back("compiler", JsonStr(CEDAR_BENCH_COMPILER));
  meta.emplace_back("host", JsonStr(host));
  meta.emplace_back("kernel", JsonStr(release));
  meta.emplace_back("arch", JsonStr(machine));
  meta.emplace_back("hardware_threads",
                    std::to_string(std::thread::hardware_concurrency()));
  meta.emplace_back("cxx_standard", JsonStr("C++17"));

  std::vector<BenchCase> cases;
  // 1. VSL 插入：单线程 / 8 线程
  cases.push_back(RunVslInsert(1, insert_rows));
  cases.push_back(RunVslInsert(8, insert_rows / 8));
  // 2. MemTable GetAtTime：2 万次随机查询
  cases.push_back(RunMemtableGetAtTime(memtable_rows, 20000));
  // 3. SST 写入/读取：1 / 8192 / 8193 行（block 边界）
  cases.push_back(RunSstCase(1, 16));
  cases.push_back(RunSstCase(8192, 200));
  cases.push_back(RunSstCase(8193, 200));
  // 4. 32 路合并
  cases.push_back(RunMerge32Way(static_cast<int>(merge_files), merge_rows_per_file));
  // 5. t9：直接写 WAL 化 + 组提交写吞吐（before/after）
  //    - 直接写默认 flush-to-OS（进程崩溃可恢复），可选 fsync（掉电可恢复）
  //    - 事务提交 window=0 ≈ Task 1.2 的每提交一次 fsync；window=1000 为组提交
  cases.push_back(RunLsmDirectPut(1, 20000, /*wal_sync=*/false,
                                  /*enable_wal=*/false));  // T2 基线：直接写不写 WAL
  cases.push_back(RunLsmDirectPut(1, 20000, /*wal_sync=*/false));
  cases.push_back(RunLsmDirectPut(1, 500, /*wal_sync=*/true));
  cases.push_back(RunTxnCommit(1, 500, /*window_us=*/0));
  cases.push_back(RunTxnCommit(1, 2000, /*window_us=*/1000));
  cases.push_back(RunTxnCommit(8, 250, /*window_us=*/0));
  cases.push_back(RunTxnCommit(8, 1000, /*window_us=*/1000));
  // 6. t10：WAL checkpoint / 截断
  //    - flush 成本：截断开启（SST 逐文件 fsync + checkpoint）vs t7 快路径
  //    - 长跑 100 轮：WAL 目录（段数/字节）必须有界
  cases.push_back(RunFlushWithTruncation(200000, 16, /*truncation_on=*/false));
  cases.push_back(RunFlushWithTruncation(200000, 16, /*truncation_on=*/true));
  cases.push_back(RunWalCheckpointLongRun(100));

  PrintJson(meta, cases, /*pretty=*/!compact_json);
  return 0;
}
