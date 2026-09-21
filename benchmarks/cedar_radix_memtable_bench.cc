// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "memory/concurrent_arena.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "util/coding.h"

namespace {

using Clock = std::chrono::steady_clock;
using namespace ROCKSDB_NAMESPACE;

struct Options {
  uint64_t entries = 16384;
  uint32_t writers = 1;
  std::string workload = "random";
  std::string revision = "unknown";
  uint64_t seed = 0xCEDA20260920ULL;
  std::string phase = "all";
  std::string implementation = "radix";
  bool stats = false;
};

bool IsReadPhase(const std::string& phase) {
  return phase == "point" || phase == "get" || phase == "miss" ||
         phase == "seek-prev" || phase == "range1" || phase == "range16" ||
         phase == "range256";
}

class BenchmarkKeyComparator final : public MemTableRep::KeyComparator {
 public:
  BenchmarkKeyComparator() : comparator_(BytewiseComparator()) {}

  int operator()(const char* left, const char* right) const override {
    return comparator_.Compare(decode_key(left), decode_key(right));
  }
  int operator()(const char* left, const Slice& right) const override {
    return comparator_.Compare(decode_key(left), right);
  }

 private:
  InternalKeyComparator comparator_;
};

void StoreBigEndian64(std::string* destination, size_t offset, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    (*destination)[offset++] = static_cast<char>(value >> shift);
  }
}

std::string MakeV2UserKey(uint64_t entity_id) {
  std::string user_key(32, '\0');
  user_key[0] = 2;
  user_key[5] = 1;
  StoreBigEndian64(&user_key, 8, entity_id);
  return user_key;
}

std::string MakeInternalKey(uint64_t entity_id, uint64_t sequence) {
  const std::string user_key = MakeV2UserKey(entity_id);
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(user_key, sequence, kTypeValue));
  return internal_key;
}

bool Insert(MemTableRep* table, const std::string& internal_key) {
  std::string entry;
  PutVarint32(&entry, static_cast<uint32_t>(internal_key.size()));
  entry.append(internal_key);
  char* storage = nullptr;
  const KeyHandle handle = table->Allocate(entry.size(), &storage);
  std::memcpy(storage, entry.data(), entry.size());
  return table->InsertKeyConcurrently(handle);
}

KeyHandle PrepareHandle(MemTableRep* table, const std::string& internal_key) {
  std::string entry;
  PutVarint32(&entry, static_cast<uint32_t>(internal_key.size()));
  entry.append(internal_key);
  char* storage = nullptr;
  const KeyHandle handle = table->Allocate(entry.size(), &storage);
  if (handle == nullptr || storage == nullptr) return nullptr;
  std::memcpy(storage, entry.data(), entry.size());
  return handle;
}

struct ScanResult {
  uint64_t count = 0;
  uint64_t hash = 1469598103934665603ULL;
  bool ordered = true;
};

struct WriterStats {
  uint64_t inserted = 0;
  uint64_t errors = 0;
};

ScanResult Scan(MemTableRep* table) {
  BenchmarkKeyComparator comparator;
  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  ScanResult result;
  const char* previous = nullptr;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    const char* entry = iterator->key();
    if (previous != nullptr && comparator(previous, entry) >= 0) {
      result.ordered = false;
    }
    const Slice key = GetLengthPrefixedSlice(entry);
    for (size_t index = 0; index < key.size(); ++index) {
      const unsigned char byte = static_cast<unsigned char>(key[index]);
      result.hash ^= byte;
      result.hash *= 1099511628211ULL;
    }
    previous = entry;
    ++result.count;
  }
  return result;
}

struct QueryResult {
  uint64_t operations = 0;
  uint64_t hits = 0;
  uint64_t hash = 1469598103934665603ULL;
  uint64_t errors = 0;
};

void HashEntry(const char* entry, QueryResult* result) {
  const Slice key = GetLengthPrefixedSlice(entry);
  for (size_t index = 0; index < key.size(); ++index) {
    result->hash ^= static_cast<unsigned char>(key[index]);
    result->hash *= 1099511628211ULL;
  }
}

struct PointGetState {
  QueryResult* result;
  Slice user_key;
};

bool VisitPointGet(void* argument, const char* entry) {
  auto* state = static_cast<PointGetState*>(argument);
  const Slice internal_key = GetLengthPrefixedSlice(entry);
  if (ExtractUserKey(internal_key) != state->user_key) return false;
  ++state->result->hits;
  HashEntry(entry, state->result);
  return false;
}

QueryResult RunReadPhase(MemTableRep* table, const Options& options,
                         const std::vector<uint64_t>& ids) {
  BenchmarkKeyComparator comparator;
  QueryResult result;
  const uint64_t range = options.phase == "range256"
                             ? 256
                             : options.phase == "range16" ? 16 : 1;
  std::unique_ptr<MemTableRep::Iterator> iterator(table->GetIterator());
  for (size_t index = 0; index < ids.size(); ++index) {
    const uint64_t entity = options.phase == "miss" ? ids.size() + index + 1
                                                     : ids[index];
    const uint64_t sequence = options.workload == "versions"
                                  ? static_cast<uint64_t>(index + 1)
                                  : entity + 1;
    const std::string internal_key = MakeInternalKey(entity, sequence);
    const Slice lookup(internal_key);
    if (options.phase == "get") {
      LookupKey lookup_key(MakeV2UserKey(entity), sequence);
      PointGetState state{&result, lookup_key.user_key()};
      table->Get(lookup_key, &state, VisitPointGet);
      ++result.operations;
      if (result.hits == 0 ||
          result.hits != index + 1) {
        ++result.errors;
      }
      continue;
    }
    if (options.phase == "seek-prev") {
      if (options.implementation == "vector") {
        iterator->Seek(lookup, nullptr);
        if (!iterator->Valid()) {
          iterator->SeekToLast();
        } else if (comparator(iterator->key(), lookup) > 0) {
          iterator->Prev();
        }
      } else {
        iterator->SeekForPrev(lookup, nullptr);
      }
    } else {
      iterator->Seek(lookup, nullptr);
    }
    ++result.operations;
    if (options.phase == "miss") {
      if (iterator->Valid() &&
          comparator(iterator->key(), lookup) == 0) {
        ++result.errors;
      }
      continue;
    }
    if (options.phase == "seek-prev") {
      if (!iterator->Valid()) {
        ++result.errors;
        continue;
      }
      ++result.hits;
      HashEntry(iterator->key(), &result);
      continue;
    }
    uint64_t returned = 0;
    while (iterator->Valid() && returned < range) {
      ++returned;
      HashEntry(iterator->key(), &result);
      iterator->Next();
    }
    if (returned == 0) ++result.errors;
    result.hits += returned;
  }
  return result;
}

uint64_t PeakRssBytes() {
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#if defined(__APPLE__)
  return static_cast<uint64_t>(usage.ru_maxrss);
#else
  return static_cast<uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
}

bool ParseUnsigned(const char* text, uint64_t* value) {
  if (text == nullptr || *text == '\0') return false;
  uint64_t parsed = 0;
  for (const char* cursor = text; *cursor != '\0'; ++cursor) {
    if (*cursor < '0' || *cursor > '9') return false;
    const uint64_t digit = static_cast<uint64_t>(*cursor - '0');
    if (parsed > (UINT64_MAX - digit) / 10) return false;
    parsed = parsed * 10 + digit;
  }
  *value = parsed;
  return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
  for (int index = 1; index < argc; ++index) {
    const std::string argument(argv[index]);
    if (argument == "--stats") {
      options->stats = true;
      continue;
    }
    if (argument == "--entries" || argument == "--writers" ||
        argument == "--seed" || argument == "--workload" ||
        argument == "--revision" || argument == "--phase" ||
        argument == "--implementation") {
      if (++index == argc) return false;
      if (argument == "--workload") {
        options->workload = argv[index];
        continue;
      }
      if (argument == "--revision") {
        options->revision = argv[index];
        continue;
      }
      if (argument == "--phase") {
        options->phase = argv[index];
        continue;
      }
      if (argument == "--implementation") {
        options->implementation = argv[index];
        continue;
      }
      uint64_t value = 0;
      if (!ParseUnsigned(argv[index], &value)) return false;
      if (argument == "--entries") options->entries = value;
      if (argument == "--writers") {
        if (value == 0 || value > UINT32_MAX) return false;
        options->writers = static_cast<uint32_t>(value);
      }
      if (argument == "--seed") options->seed = value;
      continue;
    }
    return false;
  }
  return options->entries != 0 &&
         (options->phase == "all" || options->phase == "index" ||
          options->phase == "memtable" || options->phase == "scan" ||
          IsReadPhase(options->phase)) &&
         (options->implementation == "radix" ||
          options->implementation == "skiplist" ||
          options->implementation == "vector") &&
         (options->workload == "random" || options->workload == "ascending" ||
          options->workload == "versions");
}

std::vector<uint64_t> BuildIds(const Options& options) {
  std::vector<uint64_t> ids(options.entries);
  for (uint64_t index = 0; index < options.entries; ++index) ids[index] = index + 1;
  if (options.workload == "random") {
    uint64_t state = options.seed;
    for (size_t index = ids.size(); index > 1; --index) {
      state ^= state << 7;
      state ^= state >> 9;
      std::swap(ids[index - 1], ids[state % index]);
    }
  }
  if (options.workload == "versions") {
    for (uint64_t& id : ids) id = 1 + (id % 64);
  }
  return ids;
}

int Run(const Options& options) {
  BenchmarkKeyComparator comparator;
  ConcurrentArena arena;
  std::atomic<uint64_t> branch_loads{0};
  std::atomic<uint64_t> table_snapshot_cas{0};
  std::atomic<uint64_t> boundary_candidates{0};
  std::unique_ptr<MemTableRepFactory> factory;
  if (options.implementation == "radix") {
    PartitionedVersionRadixFactory::Options radix_options;
    if (options.stats) {
      radix_options.branch_load_observer_for_testing = [&] {
        branch_loads.fetch_add(1, std::memory_order_relaxed);
      };
      radix_options.table_snapshot_cas_observer_for_testing = [&] {
        table_snapshot_cas.fetch_add(1, std::memory_order_relaxed);
      };
      radix_options.boundary_candidate_observer_for_testing = [&] {
        boundary_candidates.fetch_add(1, std::memory_order_relaxed);
      };
    }
    factory = std::make_unique<PartitionedVersionRadixFactory>(
        std::move(radix_options));
  } else if (options.implementation == "skiplist") {
    factory = std::make_unique<SkipListFactory>();
  } else {
    factory = std::make_unique<VectorRepFactory>();
  }
  std::unique_ptr<MemTableRep> table(
      factory->CreateMemTableRep(comparator, &arena, nullptr, nullptr));
  const std::vector<uint64_t> ids = BuildIds(options);
  std::vector<WriterStats> writer_stats(options.writers);
  std::vector<KeyHandle> prepared_handles;
  const bool index_only = options.phase == "index";
  const bool read_phase = IsReadPhase(options.phase);
  if (index_only) {
    prepared_handles.resize(ids.size(), nullptr);
    for (size_t index = 0; index < ids.size(); ++index) {
      const uint64_t sequence = options.workload == "versions"
                                    ? static_cast<uint64_t>(index + 1)
                                    : ids[index] + 1;
      prepared_handles[index] =
          PrepareHandle(table.get(), MakeInternalKey(ids[index], sequence));
      if (prepared_handles[index] == nullptr) return 1;
    }
  }
  std::barrier start(static_cast<std::ptrdiff_t>(options.writers));
  std::vector<std::thread> writers;
  const auto insert_started = Clock::now();
  for (uint32_t writer = 0; writer < options.writers; ++writer) {
    writers.emplace_back([&, writer] {
      start.arrive_and_wait();
      for (size_t index = writer; index < ids.size(); index += options.writers) {
        bool inserted_ok = false;
        if (index_only) {
          inserted_ok = table->InsertKeyConcurrently(prepared_handles[index]);
        } else {
          const uint64_t sequence = options.workload == "versions"
                                        ? static_cast<uint64_t>(index + 1)
                                        : ids[index] + 1;
          inserted_ok = Insert(table.get(), MakeInternalKey(ids[index], sequence));
        }
        if (inserted_ok) {
          ++writer_stats[writer].inserted;
        } else {
          ++writer_stats[writer].errors;
        }
      }
      table->BatchPostProcess();
    });
  }
  for (std::thread& writer : writers) writer.join();
  uint64_t inserted = 0;
  uint64_t errors = 0;
  for (const WriterStats& stats : writer_stats) {
    inserted += stats.inserted;
    errors += stats.errors;
  }
  const uint64_t insertion_branch_loads =
      branch_loads.load(std::memory_order_relaxed);
  const uint64_t insertion_table_snapshot_cas =
      table_snapshot_cas.load(std::memory_order_relaxed);
  const uint64_t insertion_boundary_candidates =
      boundary_candidates.load(std::memory_order_relaxed);
  const auto nanoseconds = [](Clock::time_point begin, Clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
        .count();
  };
  const auto inserted_at = Clock::now();
  const auto emit_stats = [&] {
    if (!options.stats || options.implementation != "radix") return;
    std::cerr << "radix_stats,branch_loads,table_snapshot_cas,boundary_candidates\n"
              << "radix_stats,"
              << insertion_branch_loads << ','
              << insertion_table_snapshot_cas << ','
              << insertion_boundary_candidates << '\n';
  };
  if (read_phase) {
    table->MarkReadOnly();
    table->PrepareForFlush();
    const auto query_started = Clock::now();
    const QueryResult query = RunReadPhase(table.get(), options, ids);
    const auto query_finished = Clock::now();
    const int64_t query_ns = nanoseconds(query_started, query_finished);
    const bool valid = query.errors == 0 && query.operations == options.entries &&
                       (((options.phase == "point" || options.phase == "get") &&
                         query.hits == options.entries) ||
                        (options.phase == "miss" && query.hits == 0) ||
                        (options.phase == "seek-prev" && query.hits == options.entries) ||
                        (options.phase == "range1" && query.hits == options.entries) ||
                        (options.phase == "range16" && query.hits >= options.entries) ||
                        (options.phase == "range256" && query.hits >= options.entries));
    std::cout << "revision,workload,seed,entries,allocated_handles,writers,repeat,seconds,ops,p50_ns,p95_ns,p99_ns,peak_rss_bytes,arena_bytes,bytes_per_handle,result_hash,errors,active_scan_ns,frozen_first_scan_ns,frozen_ready_scan_ns,phase,elapsed_ns,index_insert_ns,allocate_and_insert_ns,active_scan_ns_phase,freeze_prepare_ns,first_frozen_scan_ns,ready_frozen_scan_ns\n";
    std::cout << options.revision << ',' << options.workload << ',' << options.seed << ','
              << options.entries << ',' << options.entries << ',' << options.writers
              << ",0," << (static_cast<double>(query_ns) / 1e9) << ','
              << query.operations << ",NA,NA,NA," << PeakRssBytes() << ','
              << arena.ApproximateMemoryUsage() << ','
              << arena.ApproximateMemoryUsage() / options.entries << ','
              << query.hash << ',' << (valid ? 0 : query.errors + 1)
              << ",0,0,0," << options.phase << ',' << query_ns
              << ",0,0,0,0,0,0\n";
    emit_stats();
    return valid ? 0 : 1;
  }
  const ScanResult active = Scan(table.get());
  const auto active_scanned_at = Clock::now();
  table->MarkReadOnly();
  const auto prepare_started = Clock::now();
  table->PrepareForFlush();
  const auto prepare_finished = Clock::now();
  const ScanResult frozen_first = Scan(table.get());
  const auto frozen_first_at = Clock::now();
  const ScanResult frozen_ready = Scan(table.get());
  const auto frozen_ready_at = Clock::now();

  const uint64_t expected = options.entries;
  const bool valid = errors == 0 && inserted == expected &&
                     active.count == expected && frozen_first.count == expected &&
                     frozen_ready.count == expected && active.ordered &&
                     frozen_first.ordered && frozen_ready.ordered &&
                     active.hash == frozen_first.hash &&
                     active.hash == frozen_ready.hash;
  const int64_t insert_ns = nanoseconds(insert_started, inserted_at);
  const int64_t active_ns = nanoseconds(inserted_at, active_scanned_at);
  const int64_t first_ns = nanoseconds(active_scanned_at, frozen_first_at);
  const int64_t ready_ns = nanoseconds(frozen_first_at, frozen_ready_at);
  const int64_t prepare_ns = nanoseconds(prepare_started, prepare_finished);
  const int64_t index_ns = insert_ns;
  const int64_t allocate_and_insert_ns = insert_ns;
  const int64_t elapsed_ns = options.phase == "index"
                                 ? index_ns
                                 : options.phase == "memtable"
                                       ? allocate_and_insert_ns
                                       : options.phase == "scan"
                                             ? active_ns + first_ns + ready_ns
                                             : index_ns + active_ns + prepare_ns + first_ns + ready_ns;
  std::cout << "revision,workload,seed,entries,allocated_handles,writers,repeat,seconds,ops,p50_ns,p95_ns,p99_ns,peak_rss_bytes,arena_bytes,bytes_per_handle,result_hash,errors,active_scan_ns,frozen_first_scan_ns,frozen_ready_scan_ns,phase,elapsed_ns,index_insert_ns,allocate_and_insert_ns,active_scan_ns_phase,freeze_prepare_ns,first_frozen_scan_ns,ready_frozen_scan_ns\n";
  std::cout << options.revision << ',' << options.workload << ',' << options.seed << ','
            << options.entries << ',' << options.entries << ',' << options.writers
            << ",0," << (static_cast<double>(insert_ns) / 1e9) << ','
            << inserted << ",NA,NA,NA,"
            << PeakRssBytes() << ',' << arena.ApproximateMemoryUsage() << ','
            << (options.entries == 0 ? 0 : arena.ApproximateMemoryUsage() /
                                            options.entries)
            << ',' << active.hash << ','
            << (valid ? 0 : errors + 1) << ','
            << active_ns << ',' << first_ns << ',' << ready_ns << ','
            << options.phase << ',' << elapsed_ns << ',' << index_ns << ','
            << allocate_and_insert_ns << ',' << active_ns << ',' << prepare_ns
            << ',' << first_ns << ',' << ready_ns << '\n';
  emit_stats();
  return valid ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseOptions(argc, argv, &options)) {
    std::cerr << "usage: cedar_radix_memtable_bench [--entries N] [--writers N] "
                 "[--seed N] [--revision ID] [--implementation radix|skiplist|vector] "
                 "[--stats] "
                 "[--phase index|memtable|scan|point|get|miss|seek-prev|range1|range16|range256|all] "
                 "[--workload random|ascending|versions]\n";
    return 2;
  }
  return Run(options);
}
