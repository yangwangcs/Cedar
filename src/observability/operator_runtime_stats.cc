// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/observability/operator_runtime_stats.h"

#include <algorithm>

namespace cedar {

void OperatorRuntimeStatsRegistry::RecordBatch(
    OperatorRuntimeKey key, uint64_t input_rows, uint64_t output_rows,
    uint64_t input_intervals, uint64_t output_intervals) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  counters.input_rows += input_rows;
  counters.output_rows += output_rows;
  counters.input_intervals += input_intervals;
  counters.output_intervals += output_intervals;
  ++counters.batches;
}

void OperatorRuntimeStatsRegistry::AddInputRows(
    OperatorRuntimeKey key, uint64_t rows, uint64_t intervals) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  counters.input_rows += rows;
  counters.input_intervals += intervals;
}

void OperatorRuntimeStatsRegistry::RecordOutputBatch(
    OperatorRuntimeKey key, uint64_t rows, uint64_t intervals) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  counters.output_rows += rows;
  counters.output_intervals += intervals;
  ++counters.batches;
}

void OperatorRuntimeStatsRegistry::AddPagesRead(
    OperatorRuntimeKey key, uint64_t pages) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].pages_read += pages;
}

void OperatorRuntimeStatsRegistry::AddIndexCandidates(
    OperatorRuntimeKey key, uint64_t candidates) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].index_candidates += candidates;
}

void OperatorRuntimeStatsRegistry::AddBlobPayloadReads(
    OperatorRuntimeKey key, uint64_t reads) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].blob_payload_reads += reads;
}

void OperatorRuntimeStatsRegistry::AddCpuNs(
    OperatorRuntimeKey key, uint64_t nanoseconds) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].cpu_ns += nanoseconds;
}

void OperatorRuntimeStatsRegistry::AddBlockedNs(
    OperatorRuntimeKey key, uint64_t nanoseconds) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].blocked_ns += nanoseconds;
}

void OperatorRuntimeStatsRegistry::ObserveMemory(
    OperatorRuntimeKey key, uint64_t bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].memory_peak_bytes =
      std::max(counters_[key].memory_peak_bytes, bytes);
}

void OperatorRuntimeStatsRegistry::AddSpill(
    OperatorRuntimeKey key, uint64_t bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[key].spill_bytes += bytes;
}

void OperatorRuntimeStatsRegistry::RecordSpillStart(
    OperatorRuntimeKey key) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_[key].spill_starts;
}

void OperatorRuntimeStatsRegistry::RecordHashJoinInput(
    OperatorRuntimeKey key, bool build, uint64_t rows) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  if (build) counters.build_input_rows += rows;
  else counters.probe_input_rows += rows;
  counters.input_rows += rows;
}

void OperatorRuntimeStatsRegistry::RecordHashJoinOutput(
    OperatorRuntimeKey key, uint64_t rows) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  counters.output_rows += rows;
  ++counters.batches;
}

void OperatorRuntimeStatsRegistry::RecordHashJoinSpill(
    OperatorRuntimeKey key) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_[key].spill_starts;
}

void OperatorRuntimeStatsRegistry::RecordCrossJoinInput(
    OperatorRuntimeKey key, bool replay, uint64_t rows) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  if (replay) counters.build_input_rows += rows;
  else counters.probe_input_rows += rows;
  counters.input_rows += rows;
}

void OperatorRuntimeStatsRegistry::RecordCrossJoinOutput(
    OperatorRuntimeKey key, uint64_t rows) {
  std::lock_guard<std::mutex> lock(mutex_);
  OperatorRuntimeCounters& counters = counters_[key];
  counters.output_rows += rows;
  ++counters.batches;
}

void OperatorRuntimeStatsRegistry::RecordCrossJoinSpill(
    OperatorRuntimeKey key) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++counters_[key].spill_starts;
}

std::map<OperatorRuntimeKey, OperatorRuntimeCounters>
OperatorRuntimeStatsRegistry::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return counters_;
}

}  // namespace cedar
