#include "columnar_cache_dataset.h"

#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <array>
#include <barrier>
#include <chrono>
#include <cstdarg>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <thread>
#include <vector>

#include "cedar/cypher/session.h"
#include "cedar/database.h"
#include "cedar/fact/fact_codec.h"
#include "db/cedar_columnar_scan.h"
#include "query/resource/query_read_memory.h"
#include "rocksdb/env.h"
#include "storage/rocks/rocksdb_config.h"
#include "table/cedar_parquet/parquet_metadata.h"

namespace cedar::benchmarks {
namespace {
namespace cp = rocksdb::cedar_parquet;
using Clock = std::chrono::steady_clock;

uint64_t Micros(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
}
uint64_t Checksum(uint64_t id, uint64_t time, uint64_t seq, uint32_t part) {
  return id * 131 + time * 17 + seq * 7 + part;
}
uint64_t ExpectedChecksum(uint64_t first, uint64_t last, uint64_t offset,
                           uint32_t part, bool temporal, uint64_t entities) {
  const uint64_t count = last - first + 1;
  const uint64_t ids = count * (first + last) / 2;
  const uint64_t batches = (entities + kCacheDatasetCommitEntities - 1) /
                           kCacheDatasetCommitEntities;
  uint64_t batch_sum = 0;
  for (uint64_t begin = first; begin <= last;) {
    const uint64_t batch = (begin - 1) / kCacheDatasetCommitEntities;
    const uint64_t end = std::min(last, (batch + 1) * kCacheDatasetCommitEntities);
    batch_sum += (end - begin + 1) * (batch + 1);
    begin = end + 1;
  }
  return temporal ? ids * 131 + count * ((offset + 20) * 17 + 7 * batches + part) + 7 * batch_sum
                  : ids * 393 + count * ((offset * 3 + 60) * 17 + 21 * batches + uint64_t(part) * 3) +
                        21 * batch_sum;
}
uint64_t Quantile(std::vector<uint64_t> values, size_t percent) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  return values[(values.size() - 1) * percent / 100];
}
uint64_t RssBytes() {
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#if defined(__APPLE__)
  return usage.ru_maxrss;
#else
  return uint64_t(usage.ru_maxrss) * 1024;
#endif
}
uint64_t CpuMicros() {
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
  return uint64_t(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000 +
         usage.ru_utime.tv_usec + usage.ru_stime.tv_usec;
}
class QuietLogger final : public rocksdb::Logger {
 public:
  void Logv(const char*, va_list) override {}
};
struct RawDatabase {
  std::unique_ptr<rocksdb::DB> db;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ~RawDatabase() {
    for (auto* handle : handles) db->DestroyColumnFamilyHandle(handle).PermitUncheckedError();
  }
};
rocksdb::Status OpenRaw(const CacheBenchOptions& config, bool read_only,
                        cp::CedarParquetReadResources resources, RawDatabase* raw) {
  FactStoreOptions store;
  store.path = config.db_path;
  auto options = internal::MakeRocksDbOptions(store, false);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  options.info_log = std::make_shared<QuietLogger>();
  const auto families = internal::MakeRocksDbColumnFamilyDescriptors(store, options, nullptr,
                                                                   std::move(resources));
  return read_only ? rocksdb::DB::OpenForReadOnly(options, config.db_path, families,
                                                 &raw->handles, &raw->db)
                   : rocksdb::DB::Open(options, config.db_path, families,
                                        &raw->handles, &raw->db);
}
class QueryMemory final : public cp::CedarParquetQueryMemory {
 public:
  explicit QueryMemory(uint64_t limit) : memory_(internal::QueryReservation(limit)) {}
  rocksdb::Status Reserve(uint64_t bytes) override {
    const auto status = memory_.Reserve(bytes);
    return status.ok() ? rocksdb::Status::OK() : rocksdb::Status::MemoryLimit(status.ToString());
  }
  void Release(uint64_t bytes) override { memory_.Release(bytes); }
  rocksdb::Status Pin(const void* owner, uint64_t bytes,
                       std::shared_ptr<const void>* lease) override {
    *lease = memory_.Pin(owner, bytes);
    return *lease ? rocksdb::Status::OK() : rocksdb::Status::MemoryLimit("benchmark query pin");
  }
 private:
  internal::QueryReadMemory memory_;
};

Status Setup(const CacheBenchOptions& config) {
  const auto started = Clock::now();
  auto opened = Database::Open(DatabaseOptions{.path = config.db_path});
  if (!opened.ok()) return opened.status();
  auto database = opened.ConsumeValueOrDie();
  const uint64_t offset = config.seed % 1000;
  for (uint64_t version = 1; version <= 3; ++version) {
    for (uint64_t begin = 1; begin <= config.entities; begin += kCacheDatasetCommitEntities) {
    auto transaction = database->BeginTransaction();
    if (!transaction.ok()) return transaction.status();
    for (uint64_t entity = begin;
         entity <= std::min<uint64_t>(config.entities, begin + kCacheDatasetCommitEntities - 1);
         ++entity) {
      const auto status = transaction.ValueOrDie()->Assert(
          EntityFact::Vertex({PartId{config.part_id}, VertexId{entity}}),
          ValidTime{offset + version * 10});
      if (!status.ok()) return status;
    }
    const auto committed = transaction.ValueOrDie()->Commit();
    if (!committed.ok()) return committed.status();
    if (committed.ValueOrDie().outcome != CommitOutcome::kCommitted)
      return Status::Conflict("cache dataset", "fixture commit was not committed: " +
          committed.ValueOrDie().status.ToString());
    }
  }
  auto status = database->Close();
  if (!status.ok()) return status;
  database.reset();
  const uint64_t write_us = Micros(started);
  const auto flush_started = Clock::now();
  uint64_t files = 0, row_groups = 0, column_pages = 0, file_bytes = 0;
  {
    RawDatabase raw;
    auto native = OpenRaw(config, false, {}, &raw);
    if (!native.ok()) return Status::IOError("cache dataset open", native.ToString());
    rocksdb::FlushOptions flush;
    flush.wait = true;
    // Flush every CF so read-existing needs no WAL replay for fixture state.
    for (auto* handle : raw.handles) {
      native = raw.db->Flush(flush, handle);
      if (!native.ok()) return Status::IOError("cache dataset flush", native.ToString());
    }
    std::vector<rocksdb::LiveFileMetaData> live;
    raw.db->GetLiveFilesMetaData(&live);
    for (const auto& file : live) {
      if (file.column_family_name != "facts") continue;
      std::ifstream input(std::filesystem::path(file.directory) / file.relative_filename,
                          std::ios::binary);
      // Inspect bounded footer/index ranges; never load a whole large SST into
      // benchmark setup memory. Recovery/inspection can still warm OS pages.
      if (!input || file.size < 12) return Status::Corruption("cache dataset", "short Parquet file");
      char trailer[8];
      input.seekg(static_cast<std::streamoff>(file.size - 8));
      input.read(trailer, 8);
      if (!input || std::string_view(trailer + 4, 4) != "PAR1")
        return Status::Corruption("cache dataset", "invalid Parquet trailer");
      uint32_t footer_bytes = 0;
      for (unsigned byte = 0; byte < 4; ++byte)
        footer_bytes |= uint32_t(static_cast<unsigned char>(trailer[byte])) << (byte * 8);
      if (footer_bytes > cp::kCedarParquetDefaultMaxFooterBytes || footer_bytes > file.size - 12)
        return Status::Corruption("cache dataset", "footer exceeds file/budget");
      std::string bytes(footer_bytes, '\0');
      input.seekg(static_cast<std::streamoff>(file.size - 8 - footer_bytes));
      input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
      if (!input) return Status::Corruption("cache dataset", "truncated Parquet footer");
      cp::CedarParquetFooter footer;
      native = cp::DecodeCompactFooter(bytes, &footer);
      if (!native.ok()) return Status::Corruption("cache dataset footer", native.ToString());
      ++files;
      file_bytes += file.size;
      row_groups += footer.row_groups.size();
      for (const auto& group : footer.row_groups) {
        for (const auto& column : group.columns) {
          std::vector<cp::CedarParquetFooter::ColumnChunk::PageLocation> locations;
          if (column.offset_index_offset < 0 || column.offset_index_length <= 0 ||
              uint64_t(column.offset_index_length) > cp::kCedarParquetDefaultMaxIndexBytes ||
              uint64_t(column.offset_index_offset) > file.size ||
              uint64_t(column.offset_index_length) > file.size - column.offset_index_offset)
            return Status::Corruption("cache dataset", "invalid Parquet offset index range");
          bytes.resize(column.offset_index_length);
          input.seekg(column.offset_index_offset);
          input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
          if (!input) return Status::Corruption("cache dataset", "truncated Parquet index");
          native = cp::DecodeOffsetIndex(bytes, &locations);
          if (!native.ok()) return Status::Corruption("cache dataset index", native.ToString());
          column_pages += locations.size();
        }
      }
    }
  }
  if (!files || !row_groups || !column_pages)
    return Status::Corruption("cache dataset", "flush produced no Parquet pages");
  const uint64_t flush_us = Micros(flush_started);
  const auto recovery_started = Clock::now();
  DatabaseOptions validation_options{.path = config.db_path};
  validation_options.query_runtime.query_memory_bytes = config.query_memory_bytes;
  opened = Database::Open(validation_options);
  if (!opened.ok()) return opened.status();
  database = opened.ConsumeValueOrDie();
  uint64_t canonical_rows = 0, canonical_checksum = 0, state_rows = 0;
  {
    auto snapshot = database->BeginSnapshot();
    if (!snapshot.ok()) return snapshot.status();
    FactReadSpec spec;
    spec.part_scope = PartScope::Exact(PartId{config.part_id});
    status = snapshot.ValueOrDie().canonical_reader().ReadEvents(spec, [&](const FactEventBatch& batch) {
      for (const auto& event : batch.events) {
        ++canonical_rows;
        canonical_checksum += Checksum(event.ref.entity_id(), event.valid_from.value,
                                        event.commit_seq.value, event.ref.part_id().value);
      }
      return Status::OK();
    });
    if (!status.ok()) return status;
  }
  {
    cypher::BinderOptions binder;
    binder.part_id = PartId{config.part_id};
    binder.require_explicit_part_id = true;
    cypher::CypherSession session(*database, cypher::SchemaCatalog{}, binder);
    auto prepared = session.Prepare("FOR VALID_TIME AS OF " + std::to_string(offset + 30) +
                                    " MATCH (v) RETURN v");
    if (!prepared.ok()) return prepared.status();
    cypher::CypherRequest request;
    request.part_id = PartId{config.part_id};
    request.options.budget.memory_bytes = config.query_memory_bytes;
    auto cursor = session.Execute(prepared.ValueOrDie(), request);
    if (!cursor.ok()) return cursor.status();
    for (;;) {
      const auto batch = cursor.ValueOrDie().Next();
      if (!batch.ok()) return batch.status();
      if (!batch.ValueOrDie()) break;
      state_rows += batch.ValueOrDie()->row_count();
    }
  }
  const uint64_t expected_checksum = ExpectedChecksum(1, config.entities, offset, config.part_id, false,
                                                      config.entities);
  if (canonical_rows != uint64_t(config.entities) * 3 || state_rows != config.entities ||
      canonical_checksum != expected_checksum)
    return Status::Corruption("cache dataset", "canonical/Cypher fixture verification failed");
  status = database->Close();
  if (!status.ok()) return status;
  database.reset();
  const uint64_t recovery_us = Micros(recovery_started);
  std::ofstream manifest(std::filesystem::path(config.db_path) / "cedar-cache-bench-manifest.json",
                          std::ios::binary | std::ios::trunc);
  manifest << CacheBenchDatasetManifest(config);
  manifest.close();
  if (!manifest) return Status::IOError("cache dataset", "manifest write failed");
  if (config.setup_only) {
    std::cout << "{\"phase\":\"setup\",\"canonical_rows\":" << canonical_rows
              << ",\"state_rows\":" << state_rows << ",\"checksum\":" << canonical_checksum
              << ",\"parquet_files\":" << files << ",\"row_groups\":" << row_groups
              << ",\"column_pages\":" << column_pages << ",\"parquet_bytes\":" << file_bytes
              << ",\"write_us\":" << write_us << ",\"flush_us\":" << flush_us
              << ",\"recovery_validation_us\":" << recovery_us
              << ",\"recovery_warms_system_cache\":true}\n";
  }
  return Status::OK();
}

int Measure(const CacheBenchOptions& config) {
  auto metrics = std::make_shared<cp::CanonicalPageCacheMetrics>();
  auto working = std::make_shared<cp::CedarParquetReadBudget>(config.working_bytes);
  // A nonzero experiment value must reach the read-only path too, otherwise it
  // keeps the factory's 64 MiB fallback while the concurrent path resolves the
  // profile's automatic (64 KiB under debug-small-thresholds) pool.
  std::shared_ptr<cp::CedarParquetReadBudget> metadata;
  if (config.storage_reader_metadata_bytes != 0) {
    metadata = std::make_shared<cp::CedarParquetReadBudget>(
        config.storage_reader_metadata_bytes,
        cp::CedarParquetReadBudgetKind::kMetadata);
  }
  auto cache = rocksdb::NewLRUCache(config.cache_bytes, 0, true);
  RawDatabase raw;
  const auto open_started = Clock::now();
  const auto opened =
      OpenRaw(config, true, {cache, metrics, working, metadata}, &raw);
  if (!opened.ok()) { std::cerr << opened.ToString() << '\n'; return 1; }
  const auto open_us = Micros(open_started);
  const uint64_t offset = config.seed % 1000;
  std::atomic<uint64_t> operations{0}, total_rows{0}, total_checksum{0}, errors{0};
  std::atomic<uint64_t> pages{0}, skipped{0};
  std::mutex mutex;
  std::string first_error;
  std::vector<uint64_t> latencies;
  auto cpu_started = CpuMicros();
  auto started = Clock::now();
  auto deadline = started + std::chrono::seconds(config.seconds);
  const auto run_workers = [&](bool warmup) {
  std::vector<std::thread> workers;
  for (uint32_t worker = 0; worker < config.sessions; ++worker) {
    workers.emplace_back([&, worker] {
      uint64_t iteration = worker;
      uint64_t completed = 0;
      while (warmup ? completed < config.warmup_operations : Clock::now() < deadline) {
        const auto operation_started = Clock::now();
        std::string workload = config.workload;
        if (workload == "mixed") {
          static const char* kinds[] = {"point", "narrow", "wide", "temporal"};
          workload = kinds[iteration++ % 4];
        }
        cp::CedarParquetScanStats stats;
        cp::CedarParquetScanSpec spec;
        spec.stats = &stats;
        spec.policy.admit = config.admission == "admit";
        spec.policy.options.fill_cache = spec.policy.admit;
        spec.policy.query_memory = std::make_shared<QueryMemory>(config.query_memory_bytes);
        spec.projection = {cp::CedarParquetColumnId::kEntityId};
        if (workload == "wide") {
          spec.projection.clear();
          for (unsigned id = unsigned(cp::CedarParquetColumnId::kEncodedValue);
               id <= unsigned(cp::CedarParquetColumnId::kEdgeType); ++id)
            spec.projection.push_back(static_cast<cp::CedarParquetColumnId>(id));
        } else if (workload == "temporal") {
          spec.projection.push_back(cp::CedarParquetColumnId::kValidFrom);
          spec.projection.push_back(cp::CedarParquetColumnId::kCedarCommitSeq);
          spec.valid_from_min = offset + 20;
          spec.valid_from_max = offset + 20;
        }
        const uint64_t first = workload == "point" ? config.seed % config.entities + 1 : 1;
        const uint64_t last = workload == "point" ? first : config.entities;
        std::string bound;
        const FactRef lower{PartId{config.part_id}, FactFamily::kVertexState, PropertyId{}, first};
        const FactRef upper{PartId{config.part_id}, FactFamily::kVertexState, PropertyId{}, last};
        auto status = rocksdb::MakeCedarParquetSortLowerBound(
            EncodeFactKey(lower, ValidTime{UINT64_MAX}, CommitSeq{UINT64_MAX}), &bound);
        if (status.ok()) spec.sort_key_lower = bound;
        if (status.ok()) {
          // Match the store adapter's inclusive upper entity bound. CommitSeq
          // zero is not an encodable fact key; ff is the largest sort trailer.
          bound = EncodeFactKey(upper, ValidTime{0}, CommitSeq{1});
          bound.append(8, static_cast<char>(0xff));
          spec.sort_key_upper = bound;
        }
        uint64_t rows = 0, checksum = 0;
        if (status.ok()) status = rocksdb::ScanCedarParquetFacts(
            raw.db.get(), raw.handles[1], rocksdb::ReadOptions(), spec,
            [&](const cp::CedarParquetColumnarBatch& batch) {
              for (const auto& key : batch.internal_keys) {
                const auto decoded = DecodeFactKey(std::string_view(key).substr(0, 32));
                if (!decoded.ok()) return rocksdb::Status::Corruption(decoded.status().ToString());
                const auto& fact = decoded.ValueOrDie();
                ++rows;
                checksum += Checksum(fact.ref.entity_id(), fact.valid_from.value,
                                      fact.commit_seq.value, fact.ref.part_id().value);
              }
              return rocksdb::Status::OK();
            });
        const uint64_t expected_rows = (last - first + 1) * (workload == "temporal" ? 1 : 3);
        const uint64_t expected_checksum = ExpectedChecksum(first, last, offset, config.part_id,
                                                            workload == "temporal", config.entities);
        if (!status.ok() || rows != expected_rows || checksum != expected_checksum) {
          ++errors;
          std::lock_guard<std::mutex> lock(mutex);
          if (first_error.empty()) first_error = status.ok() ? "row count/checksum mismatch" : status.ToString();
          break;
        }
        ++operations;
        ++completed;
        total_rows += rows;
        total_checksum += checksum;
        pages += stats.pages_read;
        skipped += stats.pages_skipped;
        std::lock_guard<std::mutex> lock(mutex);
        latencies.push_back(Micros(operation_started));
      }
    });
  }
  for (auto& worker : workers) worker.join();
  };
  if (config.warmup_operations) run_workers(true);
  if (errors) {
    std::cerr << "warmup failed: " << first_error << '\n';
    return 1;
  }
  const auto warmup_operations = operations.load();
  const auto warmup_calls = metrics->file_read_calls.load();
  const auto warmup_bytes = metrics->file_read_bytes.load();
  const auto warmup_decodes = metrics->column_decodes.load();
  const auto warmup_decode_ns = metrics->decode_nanoseconds.load();
  const auto warmup_decoded_bytes = metrics->decoded_bytes.load();
  const auto warmup_hits = metrics->hits.load();
  const auto warmup_misses = metrics->misses.load();
  operations = 0;
  total_rows = 0;
  total_checksum = 0;
  pages = 0;
  skipped = 0;
  latencies.clear();
  cpu_started = CpuMicros();
  started = Clock::now();
  deadline = started + std::chrono::seconds(config.seconds);
  run_workers(false);
  const uint64_t elapsed_us = Micros(started);
  const uint64_t cpu_us = CpuMicros() - cpu_started;
  // A memtable-only result must never masquerade as a column-page experiment.
  if (!operations || !metrics->file_read_calls || !metrics->column_decodes) {
    ++errors;
    if (first_error.empty()) first_error = "benchmark did not observe Parquet reads";
  }
  std::cout << "workload,parts,entities,events,source,physical_bytes,decoded_bytes,pages_read,"
               "pages_skipped,rows,p50_us,p95_us,p99_us,rss_bytes,error,operations,checksum,"
               "cache_admission,cache_state,file_read_calls,column_decodes,cache_hits,cache_misses,"
               "resident_bytes,pinned_bytes,working_peak_bytes,working_rejections,reader_metadata_bytes,"
               "elapsed_us,open_us,cache_bytes,working_bytes,seed,setup_in_measurement,device_read_bytes,"
               "query_memory_bytes,cpu_us,decode_us,measurement_phase,warmup_operations,"
               "warmup_file_read_calls,warmup_column_decodes,warmup_cache_hits\n";
  std::cout << config.workload << ",1," << config.entities << ',' << uint64_t(config.entities) * 3
            << ",canonical," << metrics->file_read_bytes - warmup_bytes << ','
            << metrics->decoded_bytes - warmup_decoded_bytes << ','
            << pages << ',' << skipped << ',' << total_rows << ',' << Quantile(latencies, 50) << ','
            << Quantile(latencies, 95) << ',' << Quantile(latencies, 99) << ',' << RssBytes() << ','
            << errors << ',' << operations << ',' << total_checksum << ',' << config.admission
            << ",cold_unverified," << metrics->file_read_calls - warmup_calls << ','
            << metrics->column_decodes - warmup_decodes
            << ',' << metrics->hits - warmup_hits << ',' << metrics->misses - warmup_misses
            << ',' << metrics->resident_charge_bytes
            << ',' << metrics->pinned_charge_bytes << ',' << working->peak_bytes() << ','
            << working->rejections() << ',' << metrics->reader_metadata_bytes << ',' << elapsed_us
            << ',' << open_us << ',' << config.cache_bytes << ',' << config.working_bytes << ',' << config.seed
            << ",false,," << config.query_memory_bytes << ',' << cpu_us << ','
            << (metrics->decode_nanoseconds.load() - warmup_decode_ns) / 1000 << ','
            << (config.warmup_operations ? "after_warmup" : "fresh_and_repeated") << ','
            << warmup_operations << ',' << warmup_calls << ',' << warmup_decodes << ','
            << warmup_hits << '\n';
  if (!first_error.empty()) std::cerr << first_error << '\n';
  return errors ? 1 : 0;
}

// All four lanes use the same writable Cedar store. This small-threshold
// profile deliberately creates maintenance debt; report its resolved cache
// capacity separately from the read-only experiment's requested capacity.
int MeasureConcurrent(const CacheBenchOptions& config) {
  FactStoreOptions options;
  options.path = config.db_path;
  options.storage_profile = StorageProfile::kDebugSmallThresholds;
  options.storage_read_working_bytes = config.working_bytes;
  // Explicit experiment metadata (0 keeps the profile's automatic share). Four
  // concurrent lanes plus facts compaction pin several readers, so the 64 KiB
  // automatic share of debug-small-thresholds can reject the workload and look
  // like a mechanism failure instead of a fixture limit.
  options.storage_reader_metadata_bytes = config.storage_reader_metadata_bytes;
  std::atomic<uint64_t> synced_writes{0}, unsynced_writes{0};
  options.commit_write_options_observer_for_testing = [&](bool sync) {
    if (sync) ++synced_writes; else ++unsynced_writes;
  };
  const auto profile = internal::ResolveStorageProfile(options);
  if (!profile.ok()) { std::cerr << profile.status().ToString() << '\n'; return 1; }
  FactStore store(options);
  const auto opened = store.Open();
  if (!opened.ok()) { std::cerr << opened.ToString() << '\n'; return 1; }
  const auto before = store.SampleRuntimeMetrics();
  const auto cache_before = store.SampleValidationCacheMetrics();
  if (!before.ok() || !cache_before.ok()) {
    std::cerr << "cannot sample writable experiment baseline\n";
    return 1;
  }
  struct Lane {
    uint64_t operations = 0, rows = 0, checksum = 0, errors = 0;
    std::vector<uint64_t> latencies;
    void Sample(uint64_t latency) {
      // Keep timing storage bounded even for a long point-read run.
      if (latencies.size() < 65536) latencies.push_back(latency);
    }
  };
  std::array<Lane, 4> lanes;
  for (auto& lane : lanes) lane.latencies.reserve(65536);
  struct Activity {
    std::mutex mutex;
    unsigned mask = 0;
    Clock::time_point previous = Clock::now();
    std::chrono::nanoseconds overlap{0};
    uint64_t Change(unsigned lane, bool enter) {
      std::lock_guard<std::mutex> lock(mutex);
      const auto now = Clock::now();
      if (mask == 15) overlap += now - previous;
      previous = now;
      if (enter) mask |= 1U << lane; else mask &= ~(1U << lane);
      return overlap.count();
    }
  } activity;
  std::atomic<bool> stop{false}, writer_done{false};
  std::mutex error_mutex;
  std::string first_error;
  auto fail = [&](unsigned lane, const Status& status) {
    ++lanes[lane].errors;
    std::lock_guard<std::mutex> lock(error_mutex);
    if (first_error.empty()) first_error = status.ToString();
    stop = true;
  };
  const uint64_t offset = config.seed % 1000;
  const uint64_t hot_id = config.seed % config.entities + 1;
  const uint64_t batches = (uint64_t(config.entities) + kCacheDatasetCommitEntities - 1) /
                           kCacheDatasetCommitEntities;
  const uint64_t hot_sequence = 2 * batches + (hot_id - 1) / kCacheDatasetCommitEntities + 1;
  const FactRef hot{PartId{config.part_id}, FactFamily::kVertexState, PropertyId{}, hot_id};
  uint64_t flushes = 0, compactions = 0, compaction_input = 0, compaction_output = 0;
  uint64_t maintenance_yields = 0, compaction_overlap_ns = 0, facts_compactions = 0;
  uint64_t facts_compaction_overlap_ns = 0;
  CommitSeq last_writer_sequence;
  uint64_t last_writer_id = 0, last_writer_time = 0;
  std::barrier start_gate(5);
  Clock::time_point started, deadline;
  std::array<std::thread, 4> workers;
  workers[0] = std::thread([&] {
    start_gate.arrive_and_wait();
    while (!stop && Clock::now() < deadline) {
      const auto begin = Clock::now();
      activity.Change(0, true);
      auto memory = std::make_shared<internal::QueryReadMemory>(
          internal::QueryReservation(config.query_memory_bytes));
      auto snapshot = store.BeginCurrentSnapshot();
      Status status = snapshot.ok() ? Status::OK() : snapshot.status();
      if (status.ok()) {
        auto value = store.Read(snapshot.ValueOrDie(), hot, ValidTime{offset + 30}, memory);
        if (!value.ok()) status = value.status();
        else if (!value.ValueOrDie() || value.ValueOrDie()->ref != hot ||
                 value.ValueOrDie()->operation != FactOperation::kPut ||
                 value.ValueOrDie()->valid_from.value != offset + 30 ||
                 value.ValueOrDie()->commit_seq.value != hot_sequence)
          status = Status::Corruption("concurrent point", "fixture differs from expected state");
      }
      activity.Change(0, false);
      if (!status.ok()) { fail(0, status); break; }
      ++lanes[0].operations;
      ++lanes[0].rows;
      lanes[0].checksum += Checksum(hot_id, offset + 30, hot_sequence, config.part_id);
      lanes[0].Sample(Micros(begin));
    }
  });
  workers[1] = std::thread([&] {
    start_gate.arrive_and_wait();
    while (!stop && Clock::now() < deadline) {
      const auto begin = Clock::now();
      activity.Change(1, true);
      auto snapshot = store.BeginCurrentSnapshot();
      FactColumnarScanOptions scan;
      scan.admit_cache = config.admission == "admit";
      scan.read_memory = std::make_shared<internal::QueryReadMemory>(
          internal::QueryReservation(config.query_memory_bytes));
      scan.projection = {FactColumnId::kEntityId, FactColumnId::kValidFrom,
                         FactColumnId::kCedarCommitSeq, FactColumnId::kPartId};
      for (unsigned id = unsigned(FactColumnId::kFactFamily);
           id <= unsigned(FactColumnId::kEdgeType); ++id) {
        const auto column = static_cast<FactColumnId>(id);
        if (std::find(scan.projection.begin(), scan.projection.end(), column) == scan.projection.end())
          scan.projection.push_back(column);
      }
      uint64_t rows = 0, checksum = 0;
      Status status = snapshot.ok() ? store.ScanColumnar(snapshot.ValueOrDie(),
          FactPrefix::Family(PartId{config.part_id}, FactFamily::kVertexState, PropertyId{}),
          FactScanBounds{1, config.entities}, scan,
          [&](const FactColumnarBatch& batch) {
            const auto& ids = std::get<std::vector<uint64_t>>(batch.columns[0].values);
            const auto& times = std::get<std::vector<uint64_t>>(batch.columns[1].values);
            const auto& sequences = std::get<std::vector<uint64_t>>(batch.columns[2].values);
            const auto& parts = std::get<std::vector<uint32_t>>(batch.columns[3].values);
            for (size_t row = 0; row < batch.row_count(); ++row) {
              ++rows;
              checksum += Checksum(ids[row], times[row], sequences[row], parts[row]);
            }
            return Status::OK();
          }) : snapshot.status();
      activity.Change(1, false);
      if (status.ok() && (rows != uint64_t(config.entities) * 3 ||
          checksum != ExpectedChecksum(1, config.entities, offset, config.part_id, false, config.entities)))
        status = Status::Corruption("concurrent scan", "fixture row count/checksum differs");
      if (!status.ok()) { fail(1, status); break; }
      ++lanes[1].operations;
      lanes[1].rows += rows;
      lanes[1].checksum += checksum;
      lanes[1].Sample(Micros(begin));
    }
  });
  workers[2] = std::thread([&] {
    start_gate.arrive_and_wait();
    uint64_t iteration = 0;
    while (!stop && Clock::now() < deadline) {
      const auto begin = Clock::now();
      activity.Change(2, true);
      auto txn = store.AllocateTransactionId();
      Status status = txn.ok() ? Status::OK() : txn.status();
      if (status.ok()) {
        StoreCommitBatch batch;
        batch.txn_id = txn.ValueOrDie();
        batch.system_hlc = ++iteration;
        batch.fresh_transaction_id = true;
        batch.mutations.reserve(256);
        for (uint64_t index = 0; index < 256; ++index) {
          const uint64_t id = uint64_t(config.entities) + 1 +
                              ((iteration - 1) * 256 + index) % 8192;
          batch.mutations.push_back({FactRef{PartId{config.part_id},
              FactFamily::kVertexState, PropertyId{}, id}, ValidTime{offset + 40 + iteration},
              FactOperation::kPut, 0, std::nullopt});
        }
        auto committed = store.Commit(batch);
        if (!committed.ok()) status = committed.status();
        else {
          last_writer_sequence = committed.ValueOrDie().commit_seq;
          last_writer_id = batch.mutations.back().ref.entity_id();
          last_writer_time = batch.mutations.back().valid_from.value;
          lanes[2].rows += batch.mutations.size();
          for (const auto& mutation : batch.mutations)
            lanes[2].checksum += Checksum(mutation.ref.entity_id(), mutation.valid_from.value,
                last_writer_sequence.value, config.part_id);
        }
      }
      activity.Change(2, false);
      if (!status.ok()) { fail(2, status); break; }
      ++lanes[2].operations;
      lanes[2].Sample(Micros(begin));
    }
    writer_done = true;
  });
  workers[3] = std::thread([&] {
    start_gate.arrive_and_wait();
    // Keep servicing debt until a writer already blocked in Commit can finish.
    while (!writer_done || (!stop && Clock::now() < deadline)) {
      for (const auto kind : {FactStoreMaintenanceKind::kFlush, FactStoreMaintenanceKind::kCompaction}) {
        const auto sample = store.SampleRuntimeMetrics();
        if (!sample.ok()) { fail(3, sample.status()); return; }
        FactStoreMaintenanceRequest request;
        request.kind = kind;
        request.snapshot_generation = sample.ValueOrDie().maintenance_generation;
        request.max_input_bytes = std::max<uint64_t>(256ULL << 20, sample.ValueOrDie().live_sst_bytes);
        request.max_output_bytes = request.max_input_bytes > UINT64_MAX / 2
                                      ? UINT64_MAX : request.max_input_bytes * 2;
        request.deadline_us = 100000;
        request.yield_for_wal_sync = false;
        const auto begin = Clock::now();
        const uint64_t overlap_before = activity.Change(3, true);
        auto result = store.RunNativeMaintenance(request, nullptr);
        const uint64_t overlap_after = activity.Change(3, false);
        if (!result.ok()) { fail(3, result.status()); return; }
        const auto& job = result.ValueOrDie();
        if (!job.status.ok() && !job.status.IsMaintenanceBackoff()) {
          fail(3, job.status); return;
        }
        if (job.status.ok() && job.yield == FactStoreMaintenanceYield::kNone && job.input_bytes) {
          ++lanes[3].operations;
          lanes[3].rows += job.input_bytes;
          lanes[3].checksum += job.output_bytes;
          lanes[3].Sample(Micros(begin));
          if (kind == FactStoreMaintenanceKind::kFlush) ++flushes;
          else {
            ++compactions;
            if (job.selected_column_family_id == 1) {
              ++facts_compactions;
              facts_compaction_overlap_ns += overlap_after - overlap_before;
            }
            compaction_input += job.input_bytes;
            compaction_output += job.output_bytes;
            compaction_overlap_ns += overlap_after - overlap_before;
          }
        } else ++maintenance_yields;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
  const uint64_t cpu_started = CpuMicros();
  started = Clock::now();
  deadline = started + std::chrono::seconds(config.seconds);
  start_gate.arrive_and_wait();
  for (auto& worker : workers) worker.join();
  const uint64_t elapsed_us = Micros(started), cpu_us = CpuMicros() - cpu_started;
  auto after = store.SampleRuntimeMetrics();
  auto cache_after = store.SampleValidationCacheMetrics();
  if (!after.ok() || !cache_after.ok()) {
    std::cerr << "cannot sample writable experiment result\n";
    return 1;
  }
  if (last_writer_sequence.value) {
    auto value = store.ReadCurrent(FactRef{PartId{config.part_id}, FactFamily::kVertexState,
        PropertyId{}, last_writer_id}, ValidTime{last_writer_time});
    if (!value.ok() || !value.ValueOrDie() ||
        value.ValueOrDie()->commit_seq != last_writer_sequence ||
        value.ValueOrDie()->valid_from.value != last_writer_time)
      fail(2, Status::Corruption("concurrent WAL", "last committed mutation is not readable"));
  }
  bool valid = compactions && facts_compactions && facts_compaction_overlap_ns &&
               synced_writes >= lanes[2].operations && !unsynced_writes &&
               after.ValueOrDie().background_errors == before.ValueOrDie().background_errors;
  for (const auto& lane : lanes) valid = valid && lane.operations && !lane.errors;
  const auto& previous = cache_before.ValueOrDie();
  const auto& cache_result = cache_after.ValueOrDie();
  const auto& debt = after.ValueOrDie();
  valid = valid && cache_result.column_page_cache_decodes > previous.column_page_cache_decodes &&
          cache_result.storage_file_read_calls > previous.storage_file_read_calls;
  std::cout << "workload,lane,operations,rows,checksum,errors,p50_us,p95_us,p99_us,"
               "latency_samples,latency_sampling,cache_admission,storage_profile,cache_bytes,"
               "requested_cache_bytes,working_bytes,query_memory_bytes,elapsed_us,cpu_us,rss_bytes,"
               "flushes,compactions,facts_compactions,compaction_input_bytes,compaction_output_bytes,"
               "four_lane_compaction_overlap_ns,four_lane_facts_compaction_overlap_ns,"
               "maintenance_yields,wal_sync_writes,wal_unsynced_writes,"
               "pending_compaction_before,pending_compaction_after,l0_before,l0_after,"
               "file_read_calls,file_read_bytes,column_decodes,cache_hits,cache_misses,working_peak_bytes,"
               "reader_metadata_bytes,reader_metadata_reserved_bytes,reader_metadata_peak_bytes,"
               "reader_metadata_rejections,reader_metadata_limit_bytes,background_errors,accepted\n";
  const char* names[] = {"point", "scan", "wal", "maintenance"};
  for (size_t index = 0; index < lanes.size(); ++index) {
    const auto& lane = lanes[index];
    std::cout << "concurrent," << names[index] << ',' << lane.operations << ',' << lane.rows
        << ',' << lane.checksum << ',' << lane.errors << ',' << Quantile(lane.latencies, 50)
        << ',' << Quantile(lane.latencies, 95) << ',' << Quantile(lane.latencies, 99)
        << ',' << lane.latencies.size() << ",first_65536," << config.admission
        << ",debug-small-thresholds," << profile.ValueOrDie().block_cache_bytes
        << ',' << config.cache_bytes << ',' << config.working_bytes << ',' << config.query_memory_bytes
        << ',' << elapsed_us << ',' << cpu_us << ',' << RssBytes() << ',' << flushes << ',' << compactions
        << ',' << facts_compactions << ',' << compaction_input << ',' << compaction_output
        << ',' << compaction_overlap_ns << ',' << facts_compaction_overlap_ns
        << ',' << maintenance_yields << ',' << synced_writes
        << ',' << unsynced_writes << ',' << before.ValueOrDie().total_pending_compaction_bytes
        << ',' << debt.total_pending_compaction_bytes << ',' << before.ValueOrDie().total_l0_files
        << ',' << debt.total_l0_files << ',' << cache_result.storage_file_read_calls - previous.storage_file_read_calls
        << ',' << cache_result.storage_file_read_bytes - previous.storage_file_read_bytes
        << ',' << cache_result.column_page_cache_decodes - previous.column_page_cache_decodes
        << ',' << cache_result.column_page_cache_hits - previous.column_page_cache_hits
        << ',' << cache_result.column_page_cache_misses - previous.column_page_cache_misses
        << ',' << cache_result.storage_read_working_peak_bytes
        << ',' << cache_result.parquet_reader_metadata_bytes
        << ',' << cache_result.storage_reader_metadata_reserved_bytes
        << ',' << cache_result.storage_reader_metadata_peak_bytes
        << ',' << cache_result.storage_reader_metadata_rejections
        << ',' << cache_result.storage_reader_metadata_limit_bytes << ',' << debt.background_errors
        << ',' << (valid ? "true" : "false") << '\n';
  }
  if (!first_error.empty()) std::cerr << first_error << '\n';
  if (!valid) std::cerr << "writable experiment rejected: require successful lanes, facts compaction, "
                            "four-lane overlap and synchronous Cedar WAL commits\n";
  const auto closed = store.Close();
  if (!closed.ok()) { std::cerr << closed.ToString() << '\n'; return 1; }
  return valid ? 0 : 1;
}
}  // namespace

int RunColumnarCacheDataset(const CacheBenchOptions& options) {
  if (!options.read_existing && !options.write_existing) {
    const auto status = Setup(options);
    if (!status.ok()) { std::cerr << status.ToString() << '\n'; return 1; }
    if (options.setup_only) return 0;
  }
  std::ifstream input(std::filesystem::path(options.db_path) / "cedar-cache-bench-manifest.json",
                       std::ios::binary);
  const std::string bytes{std::istreambuf_iterator<char>(input), {}};
  if (!ValidateCacheBenchDatasetManifest(bytes, options)) {
    std::cerr << "dataset manifest does not match entities/seed/part/flushed format\n";
    return 2;
  }
  return options.write_existing ? MeasureConcurrent(options) : Measure(options);
}
}  // namespace cedar::benchmarks
