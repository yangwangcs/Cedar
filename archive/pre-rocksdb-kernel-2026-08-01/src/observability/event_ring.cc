// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/observability/event_ring.h"

#include <chrono>

namespace cedar {
namespace {

std::atomic<uint64_t> g_next_trace_id{1};
std::atomic<uint64_t> g_next_span_id{1};

uint64_t Mix(uint64_t value) {
  value ^= value >> 30;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27;
  value *= 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

}  // namespace

EventRing::EventRing(uint64_t capacity, uint64_t correctness_reserve)
    : capacity_(capacity),
      correctness_reserve_(correctness_reserve > capacity ? capacity : correctness_reserve) {}

bool EventRing::Push(TelemetryEvent event) {
  std::lock_guard<std::mutex> lock(mutex_);
  const bool correctness = event.kind == TelemetryEventKind::kCorrectness;
  const uint64_t span_capacity = capacity_ - correctness_reserve_;
  const uint64_t queued = events_.size();
  const bool accepted = correctness ? queued < capacity_ : queued < span_capacity;
  if (!accepted) {
    if (correctness) ++stats_.dropped_correctness;
    else ++stats_.dropped_spans;
    return false;
  }
  events_.push_back(std::move(event));
  if (correctness) ++stats_.accepted_correctness;
  else ++stats_.accepted_spans;
  stats_.queued = events_.size();
  return true;
}

std::optional<TelemetryEvent> EventRing::Pop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (events_.empty()) return std::nullopt;
  TelemetryEvent event = std::move(events_.front());
  events_.pop_front();
  stats_.queued = events_.size();
  return event;
}

EventRingStats EventRing::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

bool TraceSampler::ShouldSample(uint64_t trace_id) const {
  return per_mille_ != 0 && (Mix(trace_id) % 1000) < per_mille_;
}

TraceContext NewTraceContext(const TraceSampler& sampler) {
  const uint64_t trace_id = g_next_trace_id.fetch_add(1, std::memory_order_relaxed);
  return TraceContext{trace_id, g_next_span_id.fetch_add(1, std::memory_order_relaxed), 0,
                      sampler.ShouldSample(trace_id)};
}

TraceContext NewChildTraceContext(const TraceContext& parent) {
  return TraceContext{parent.trace_id, g_next_span_id.fetch_add(1, std::memory_order_relaxed),
                      parent.span_id, parent.sampled};
}

uint64_t MonotonicTimeNs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());
}

}  // namespace cedar
