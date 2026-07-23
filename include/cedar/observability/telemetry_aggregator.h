// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_TELEMETRY_AGGREGATOR_H_
#define CEDAR_OBSERVABILITY_TELEMETRY_AGGREGATOR_H_

#include <cstdint>
#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/observability/event_ring.h"

namespace cedar {

struct TelemetryAggregatorConfig {
  uint64_t input_capacity = 1024;
  uint64_t correctness_reserve = 64;
  uint64_t retained_capacity = 4096;
  uint32_t base_sampling_per_mille = 100;
  uint32_t minimum_sampling_per_mille = 1;
  uint32_t high_watermark_per_mille = 800;
  uint32_t low_watermark_per_mille = 300;
};

struct TelemetryAggregatorStats {
  uint64_t accepted_correctness = 0;
  uint64_t accepted_spans = 0;
  uint64_t unsampled_spans = 0;
  uint64_t dropped_verbose_spans = 0;
  uint64_t dropped_normal_spans = 0;
  uint64_t dropped_critical_events = 0;
  uint64_t retained_events = 0;
  uint32_t current_sampling_per_mille = 0;
};

class TelemetryAggregator {
 public:
  explicit TelemetryAggregator(TelemetryAggregatorConfig config = {});
  ~TelemetryAggregator();

  TelemetryAggregator(const TelemetryAggregator&) = delete;
  TelemetryAggregator& operator=(const TelemetryAggregator&) = delete;

  Status Start();
  Status Stop();
  Status Flush();

  TraceContext NewTrace(TracePriority priority);
  TraceContext NewChild(const TraceContext& parent) const;
  bool RecordSpan(const TraceContext& trace, TracePriority priority,
                  std::string category, std::string name,
                  uint64_t start_time_ns, uint64_t duration_ns,
                  std::string terminal_status);
  bool RecordCorrectness(std::string category, uint64_t value);

  TelemetryAggregatorStats stats() const;
  std::string ExportTracesJson(bool clear);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_TELEMETRY_AGGREGATOR_H_
