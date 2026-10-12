// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_RECENT_FACT_WRITE_INDEX_H_
#define CEDAR_FACT_RECENT_FACT_WRITE_INDEX_H_

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "storage/facts/group_commit_planner.h"

namespace cedar::internal {

struct RecentFactWriteIndexMetrics {
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t resets = 0;
  uint64_t exact_hits = 0;
  uint64_t bloom_hits = 0;
  uint64_t exact_absent = 0;
  uint64_t exact_newer = 0;
  uint64_t exact_evictions = 0;
  uint64_t exact_resets = 0;
  uint64_t coverage_misses = 0;
};

// A bounded exact latest-write summary and rolling Bloom version windows.
// Exact entries retain the complete identity and last published sequence.
// Missing/evicted entries never prove absence; they fall back to the Bloom
// windows and then canonical validation. Each Bloom generation is a version
// window, not a map from identity to heap objects. A clear bit proves that the
// identity had no write in that window; a set bit is only a conservative
// "maybe" and sends validation to canonical RocksDB. Once a generation is
// reused, snapshots that would need its information likewise fall back.
//
// This retains the correctness direction of the old exact index while keeping
// resident memory fixed even when every commit introduces a fresh entity.
//
// The publisher serializes CanProveUnchanged and Publish. Metrics are atomic
// because they are sampled by a concurrent observability reader.
class RecentFactWriteIndex {
 public:
  RecentFactWriteIndex(size_t max_bytes, CommitSeq coverage_start_seq,
                       uint64_t window_sequences = 16384);

  bool CanProveUnchanged(const FactRef& ref, CommitSeq snapshot_seq);
  void Publish(const FactRef& ref, CommitSeq commit_seq);

  // The maximum contiguous sequence span represented before the oldest window
  // is reused. It is deliberately a sequence span, not an entity count.
  size_t capacity() const { return window_sequences_ * filters_.size(); }
  size_t resident_bytes() const { return resident_bytes_; }
  size_t filter_count() const { return filters_.size(); }
  size_t exact_capacity() const { return exact_buckets_.size() * 4; }
  size_t exact_resident_bytes() const { return exact_buckets_.size() * sizeof(ExactBucket); }
  RecentFactWriteIndexMetrics metrics() const;

 private:
  struct ExactSlot {
    FactIdentity identity{};
    uint64_t latest_sequence = 0;
    bool occupied = false;
  };
  struct ExactBucket {
    std::array<ExactSlot, 4> slots;
    uint8_t next_replace = 0;
  };
  struct Filter {
    uint64_t first_sequence = 0;
    uint64_t last_sequence = 0;
    uint64_t published_through = 0;
    bool active = false;
    std::vector<uint64_t> words;
  };

  static constexpr size_t kFilterCount = 4;

  static size_t WordsPerFilter(size_t max_bytes);
  static FactIdentity Identity(const FactRef& ref);
  static uint64_t Mix(uint64_t value);
  uint64_t WindowStart(uint64_t sequence) const;
  Filter* FindFilter(uint64_t sequence);
  const Filter* FindFilter(uint64_t sequence) const;
  Filter* ActivateFilter(uint64_t first_sequence);
  void Clear(Filter* filter, uint64_t first_sequence);
  bool MightContain(const Filter& filter, const FactIdentity& identity) const;
  void Add(Filter* filter, const FactIdentity& identity);
  bool HasActiveFilter() const;
  ExactBucket* ExactBucketFor(const FactIdentity& identity);
  void PublishExact(const FactIdentity& identity, uint64_t sequence);

  std::vector<Filter> filters_;
  const uint64_t window_sequences_;
  const uint64_t window_anchor_sequence_;
  size_t resident_bytes_ = 0;
  std::vector<ExactBucket> exact_buckets_;
  uint64_t exact_coverage_start_seq_ = 0;
  uint64_t coverage_start_seq_ = 0;
  uint64_t latest_published_seq_ = 0;
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> misses_{0};
  std::atomic<uint64_t> resets_{0};
  std::atomic<uint64_t> exact_hits_{0};
  std::atomic<uint64_t> bloom_hits_{0};
  std::atomic<uint64_t> exact_absent_{0};
  std::atomic<uint64_t> exact_newer_{0};
  std::atomic<uint64_t> exact_evictions_{0};
  std::atomic<uint64_t> exact_resets_{0};
  std::atomic<uint64_t> coverage_misses_{0};
};

}  // namespace cedar::internal

#endif  // CEDAR_FACT_RECENT_FACT_WRITE_INDEX_H_
