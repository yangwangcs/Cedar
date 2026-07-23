// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TEST_MODEL_BITEMPORAL_ORACLE_H_
#define CEDAR_TEST_MODEL_BITEMPORAL_ORACLE_H_

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <vector>

#include "cedar/transaction/logical_key.h"
#include "cedar/types/value.h"

namespace cedar {

// Independent scalar reference model used only by correctness tests. It does
// not call TemporalEventResolver, MemTable, SST, or query execution code.
class BitemporalOracle {
 public:
  struct Record {
    LogicalKey key;
    uint64_t valid_from = 0;
    uint64_t commit_seq = 0;
    bool deleted = false;
    Value value = Value::Binary("");
  };

  struct Interval {
    uint64_t valid_from = 0;
    uint64_t valid_to = 0;
    uint64_t commit_seq = 0;
    Value value = Value::Binary("");
  };

  void AppendPut(const LogicalKey& key, uint64_t valid_from, uint64_t commit_seq,
                 Value value) {
    records_[key].push_back(Record{key, valid_from, commit_seq, false, std::move(value)});
  }

  void AppendDelete(const LogicalKey& key, uint64_t valid_from, uint64_t commit_seq) {
    records_[key].push_back(Record{key, valid_from, commit_seq, true, Value::Binary("")});
  }

  std::optional<Value> ResolveValue(const LogicalKey& key, uint64_t valid_time,
                                    uint64_t snapshot_seq) const {
    const Record* selected = ResolveRecord(key, valid_time, snapshot_seq);
    if (selected == nullptr || selected->deleted) return std::nullopt;
    return selected->value;
  }

  std::vector<Interval> ResolveIntervals(
      const LogicalKey& key, uint64_t valid_start, uint64_t valid_end,
      uint64_t snapshot_seq) const {
    std::vector<Interval> intervals;
    if (valid_start >= valid_end) return intervals;
    std::vector<uint64_t> boundaries{valid_start};
    const auto found = records_.find(key);
    if (found != records_.end()) {
      for (const Record& record : found->second) {
        if (record.commit_seq <= snapshot_seq &&
            record.valid_from > valid_start) {
          boundaries.push_back(record.valid_from);
        }
      }
    }
    std::sort(boundaries.begin(), boundaries.end());
    boundaries.erase(std::unique(boundaries.begin(), boundaries.end()),
                     boundaries.end());
    for (size_t index = 0; index < boundaries.size() &&
         boundaries[index] < valid_end; ++index) {
      const Record* selected =
          ResolveRecord(key, boundaries[index], snapshot_seq);
      if (selected == nullptr || selected->deleted) continue;
      const uint64_t valid_to = index + 1 < boundaries.size()
          ? boundaries[index + 1]
          : std::numeric_limits<uint64_t>::max();
      intervals.push_back(Interval{
          boundaries[index], valid_to, selected->commit_seq,
          selected->value});
    }
    return intervals;
  }

  std::vector<Record> ResolveChanges(
      const LogicalKey& key, uint64_t valid_start, uint64_t valid_end,
      uint64_t snapshot_seq) const {
    std::vector<Record> changes;
    const auto found = records_.find(key);
    if (found == records_.end() || valid_start >= valid_end) return changes;
    for (const Record& record : found->second) {
      if (record.commit_seq <= snapshot_seq &&
          record.valid_from >= valid_start &&
          record.valid_from < valid_end) {
        changes.push_back(record);
      }
    }
    std::sort(changes.begin(), changes.end(),
              [](const Record& left, const Record& right) {
                return left.valid_from < right.valid_from ||
                    (left.valid_from == right.valid_from &&
                     left.commit_seq < right.commit_seq);
              });
    return changes;
  }

  std::vector<Interval> ResolveIntersection(
      const std::vector<LogicalKey>& keys, uint64_t valid_start,
      uint64_t valid_end, uint64_t snapshot_seq) const {
    std::vector<Interval> intervals;
    if (keys.empty() || valid_start >= valid_end) return intervals;
    std::vector<uint64_t> boundaries{valid_start};
    for (const LogicalKey& key : keys) {
      const auto found = records_.find(key);
      if (found == records_.end()) continue;
      for (const Record& record : found->second) {
        if (record.commit_seq <= snapshot_seq &&
            record.valid_from > valid_start) {
          boundaries.push_back(record.valid_from);
        }
      }
    }
    std::sort(boundaries.begin(), boundaries.end());
    boundaries.erase(std::unique(boundaries.begin(), boundaries.end()),
                     boundaries.end());
    for (size_t index = 0; index < boundaries.size() &&
         boundaries[index] < valid_end; ++index) {
      uint64_t maximum_commit_seq = 0;
      bool all_present = true;
      for (const LogicalKey& key : keys) {
        const Record* selected =
            ResolveRecord(key, boundaries[index], snapshot_seq);
        if (selected == nullptr || selected->deleted) {
          all_present = false;
          break;
        }
        maximum_commit_seq =
            std::max(maximum_commit_seq, selected->commit_seq);
      }
      if (!all_present) continue;
      const uint64_t valid_to = index + 1 < boundaries.size()
          ? boundaries[index + 1]
          : std::numeric_limits<uint64_t>::max();
      intervals.push_back(Interval{
          boundaries[index], valid_to, maximum_commit_seq,
          Value::Binary("")});
    }
    return intervals;
  }

  const std::map<LogicalKey, std::vector<Record>>& records() const { return records_; }

 private:
  const Record* ResolveRecord(const LogicalKey& key, uint64_t valid_time,
                              uint64_t snapshot_seq) const {
    const auto found = records_.find(key);
    if (found == records_.end()) return nullptr;
    const Record* selected = nullptr;
    for (const Record& record : found->second) {
      if (record.commit_seq > snapshot_seq || record.valid_from > valid_time) {
        continue;
      }
      if (selected == nullptr || record.valid_from > selected->valid_from ||
          (record.valid_from == selected->valid_from &&
           record.commit_seq > selected->commit_seq)) {
        selected = &record;
      }
    }
    return selected;
  }

  std::map<LogicalKey, std::vector<Record>> records_;
};

}  // namespace cedar

#endif  // CEDAR_TEST_MODEL_BITEMPORAL_ORACLE_H_
