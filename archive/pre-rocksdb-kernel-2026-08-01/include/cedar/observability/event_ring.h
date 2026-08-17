// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_EVENT_RING_H_
#define CEDAR_OBSERVABILITY_EVENT_RING_H_

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>

namespace cedar {

enum class TelemetryEventKind : uint8_t { kCorrectness, kSpan };
enum class TracePriority : uint8_t { kVerbose, kNormal, kTail, kError };

struct TraceContext {
  uint64_t trace_id = 0;
  uint64_t span_id = 0;
  uint64_t parent_span_id = 0;
  bool sampled = false;
};

struct TelemetryEvent {
  TelemetryEventKind kind = TelemetryEventKind::kSpan;
  TraceContext trace;
  uint64_t monotonic_time_ns = 0;
  std::string category;
  uint64_t value = 0;
  TracePriority priority = TracePriority::kNormal;
  std::string name;
  uint64_t duration_ns = 0;
  std::string terminal_status = "OK";
};

struct EventRingStats {
  uint64_t accepted_correctness = 0;
  uint64_t accepted_spans = 0;
  uint64_t dropped_correctness = 0;
  uint64_t dropped_spans = 0;
  uint64_t queued = 0;
};

// The ring is an observation boundary, not a correctness queue. Correctness
// events may consume its reserved capacity; spans cannot. A full correctness
// reserve is reported to the caller so it can synchronously record diagnostics.
class EventRing {
 public:
  EventRing(uint64_t capacity, uint64_t correctness_reserve);

  bool Push(TelemetryEvent event);
  std::optional<TelemetryEvent> Pop();
  EventRingStats stats() const;

 private:
  const uint64_t capacity_;
  const uint64_t correctness_reserve_;
  mutable std::mutex mutex_;
  std::deque<TelemetryEvent> events_;
  EventRingStats stats_;
};

class TraceSampler {
 public:
  // Sampling is deterministic for a trace id so retries and distributed
  // consumers do not disagree about whether a trace should exist.
  explicit TraceSampler(uint32_t per_mille) : per_mille_(per_mille > 1000 ? 1000 : per_mille) {}
  bool ShouldSample(uint64_t trace_id) const;
  uint32_t per_mille() const { return per_mille_; }
 private:
  uint32_t per_mille_;
};

TraceContext NewTraceContext(const TraceSampler& sampler);
TraceContext NewChildTraceContext(const TraceContext& parent);
uint64_t MonotonicTimeNs();

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_EVENT_RING_H_
