// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_OPERATOR_RUNTIME_STATS_H_
#define CEDAR_OBSERVABILITY_OPERATOR_RUNTIME_STATS_H_

#include <cstdint>
#include <map>
#include <mutex>

namespace cedar {

struct OperatorRuntimeKey {
  uint64_t source_plan_id = 0;
  uint32_t operator_id = 0;

  friend bool operator<(const OperatorRuntimeKey& left,
                        const OperatorRuntimeKey& right) {
    return left.source_plan_id < right.source_plan_id ||
        (left.source_plan_id == right.source_plan_id &&
         left.operator_id < right.operator_id);
  }
};

struct OperatorRuntimeCounters {
  uint64_t input_rows = 0;
  uint64_t output_rows = 0;
  uint64_t batches = 0;
  uint64_t input_intervals = 0;
  uint64_t output_intervals = 0;
  uint64_t pages_read = 0;
  uint64_t index_candidates = 0;
  uint64_t blob_payload_reads = 0;
  uint64_t memory_peak_bytes = 0;
  uint64_t spill_bytes = 0;
  uint64_t cpu_ns = 0;
  uint64_t blocked_ns = 0;
  uint64_t build_input_rows = 0;
  uint64_t probe_input_rows = 0;
  uint64_t spill_starts = 0;
};

class OperatorRuntimeStatsRegistry {
 public:
  void RecordBatch(OperatorRuntimeKey key, uint64_t input_rows,
                   uint64_t output_rows, uint64_t input_intervals = 0,
                   uint64_t output_intervals = 0);
  void AddInputRows(OperatorRuntimeKey key, uint64_t rows,
                    uint64_t intervals = 0);
  void RecordOutputBatch(OperatorRuntimeKey key, uint64_t rows,
                         uint64_t intervals = 0);
  void AddPagesRead(OperatorRuntimeKey key, uint64_t pages);
  void AddIndexCandidates(OperatorRuntimeKey key, uint64_t candidates);
  void AddBlobPayloadReads(OperatorRuntimeKey key, uint64_t reads);
  void AddCpuNs(OperatorRuntimeKey key, uint64_t nanoseconds);
  void AddBlockedNs(OperatorRuntimeKey key, uint64_t nanoseconds);
  void ObserveMemory(OperatorRuntimeKey key, uint64_t bytes);
  void AddSpill(OperatorRuntimeKey key, uint64_t bytes);
  void RecordSpillStart(OperatorRuntimeKey key);
  void RecordHashJoinInput(OperatorRuntimeKey key, bool build,
                           uint64_t rows);
  void RecordHashJoinOutput(OperatorRuntimeKey key, uint64_t rows);
  void RecordHashJoinSpill(OperatorRuntimeKey key);
  void RecordCrossJoinInput(OperatorRuntimeKey key, bool replay,
                            uint64_t rows);
  void RecordCrossJoinOutput(OperatorRuntimeKey key, uint64_t rows);
  void RecordCrossJoinSpill(OperatorRuntimeKey key);

  std::map<OperatorRuntimeKey, OperatorRuntimeCounters> Snapshot() const;

 private:
  mutable std::mutex mutex_;
  std::map<OperatorRuntimeKey, OperatorRuntimeCounters> counters_;
};

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_OPERATOR_RUNTIME_STATS_H_
