// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <array>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "memtable/cedar_pure_radix_index.h"
#include "memory/concurrent_arena.h"

namespace {

using ROCKSDB_NAMESPACE::CedarPureRadixIndex;
using ROCKSDB_NAMESPACE::ConcurrentArena;

struct Counters {
  std::atomic<uint64_t> root_attempts{0};
  std::atomic<uint64_t> root_failures{0};
  std::atomic<uint64_t> segment_attempts{0};
  std::atomic<uint64_t> segment_failures{0};
  std::atomic<uint64_t> local_retries{0};
  std::atomic<uint64_t> local_successes{0};
  std::atomic<uint64_t> root_restarts{0};
};

CedarPureRadixIndex::Key MakeKey(uint64_t id) {
  CedarPureRadixIndex::Key key{};
  key[0] = 2;
  key[5] = 1;
  for (size_t i = 0; i < 8; ++i) {
    key[15 - i] = static_cast<unsigned char>(id >> (8 * i));
  }
  const uint64_t inverted_tag = ~(((id + 1) << 8) | 1);
  for (size_t i = 0; i < 8; ++i) {
    key[39 - i] = static_cast<unsigned char>(inverted_tag >> (8 * i));
  }
  return key;
}

std::vector<uint64_t> BuildIds(size_t count, uint64_t seed) {
  std::vector<uint64_t> ids(count);
  for (size_t i = 0; i < count; ++i) ids[i] = i + 1;
  uint64_t state = seed;
  for (size_t i = ids.size(); i > 1; --i) {
    state ^= state << 7;
    state ^= state >> 9;
    std::swap(ids[i - 1], ids[state % i]);
  }
  return ids;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "usage: publication_diagnostic SEED ENTRIES WRITERS\n";
    return 2;
  }
  const uint64_t seed = std::strtoull(argv[1], nullptr, 10);
  const size_t count = std::strtoull(argv[2], nullptr, 10);
  const size_t writers = std::strtoull(argv[3], nullptr, 10);
  if (seed == 0 || count == 0 || writers == 0 || writers > count) return 2;

  Counters counters;
  CedarPureRadixIndex::TestHooks hooks;
  hooks.root_cas_result_for_testing = [&](bool published) {
    counters.root_attempts.fetch_add(1, std::memory_order_relaxed);
    if (!published) {
      counters.root_failures.fetch_add(1, std::memory_order_relaxed);
    }
  };
  hooks.segment_cas_result_for_testing = [&](bool published) {
    counters.segment_attempts.fetch_add(1, std::memory_order_relaxed);
    if (!published) {
      counters.segment_failures.fetch_add(1, std::memory_order_relaxed);
    }
  };
#ifdef CEDAR_HAS_LOCAL_RETRY
  hooks.local_retry_for_testing = [&](bool compatible, bool published) {
    if (!compatible) std::abort();
    counters.local_retries.fetch_add(1, std::memory_order_relaxed);
    if (published) {
      counters.local_successes.fetch_add(1, std::memory_order_relaxed);
    }
  };
  hooks.root_restart_for_testing = [&] {
    counters.root_restarts.fetch_add(1, std::memory_order_relaxed);
  };
#endif

  ConcurrentArena arena;
  CedarPureRadixIndex index(&arena, hooks);
  const auto ids = BuildIds(count, seed);
  std::barrier start(static_cast<std::ptrdiff_t>(writers));
  std::atomic<uint64_t> errors{0};
  std::vector<std::thread> threads;
  for (size_t writer = 0; writer < writers; ++writer) {
    threads.emplace_back([&, writer] {
      start.arrive_and_wait();
      for (size_t i = writer; i < ids.size(); i += writers) {
        const auto key = MakeKey(ids[i]);
        char* entry = nullptr;
        void* handle = index.Allocate(1, &entry);
        *entry = 'x';
        if (!index.Insert(handle, key)) {
          errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& thread : threads) thread.join();
  for (uint64_t id : ids) {
    if (!index.Contains(MakeKey(id))) {
      errors.fetch_add(1, std::memory_order_relaxed);
    }
  }

  const auto value = [](const std::atomic<uint64_t>& counter) {
    return counter.load(std::memory_order_relaxed);
  };
  if (value(errors) != 0 ||
      value(counters.local_successes) > value(counters.local_retries) ||
      value(counters.local_retries) > value(counters.segment_failures) ||
      value(counters.root_attempts) - value(counters.root_failures) +
              value(counters.segment_attempts) -
              value(counters.segment_failures) != count) {
    return 1;
  }
  std::cout << "{\"seed\":" << seed << ",\"entries\":" << count
            << ",\"writers\":" << writers
            << ",\"root_attempts\":" << value(counters.root_attempts)
            << ",\"root_failures\":" << value(counters.root_failures)
            << ",\"segment_attempts\":" << value(counters.segment_attempts)
            << ",\"segment_failures\":" << value(counters.segment_failures)
            << ",\"local_retries\":" << value(counters.local_retries)
            << ",\"local_successes\":" << value(counters.local_successes)
            << ",\"root_restarts\":"
#ifdef CEDAR_HAS_LOCAL_RETRY
            << value(counters.root_restarts)
#else
            << "null"
#endif
            << ",\"arena_bytes\":" << arena.ApproximateMemoryUsage()
            << ",\"errors\":" << value(errors) << "}\n";
}
