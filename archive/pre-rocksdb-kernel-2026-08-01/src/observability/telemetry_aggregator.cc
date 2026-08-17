// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/observability/telemetry_aggregator.h"

#include "cedar/observability/instrumentation_profile.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <sstream>
#include <thread>
#include <utility>

namespace cedar {
namespace {

std::string EscapeJson(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (const unsigned char character : input) {
    switch (character) {
      case '"': output.append("\\\""); break;
      case '\\': output.append("\\\\"); break;
      case '\b': output.append("\\b"); break;
      case '\f': output.append("\\f"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default: output.push_back(static_cast<char>(character)); break;
    }
  }
  return output;
}

const char* EventKindName(TelemetryEventKind kind) {
  return kind == TelemetryEventKind::kCorrectness ? "correctness" : "span";
}

const char* PriorityName(TracePriority priority) {
  switch (priority) {
    case TracePriority::kVerbose: return "verbose";
    case TracePriority::kNormal: return "normal";
    case TracePriority::kTail: return "tail";
    case TracePriority::kError: return "error";
  }
  return "normal";
}

uint32_t ClampPerMille(uint32_t value) {
  return value > 1000 ? 1000 : value;
}

}  // namespace

class TelemetryAggregator::Impl {
 public:
  explicit Impl(TelemetryAggregatorConfig requested)
      : config_(std::move(requested)),
        input_(config_.input_capacity, config_.correctness_reserve),
        current_sampling_per_mille_(ClampPerMille(config_.base_sampling_per_mille)) {
    config_.base_sampling_per_mille = ClampPerMille(config_.base_sampling_per_mille);
    config_.minimum_sampling_per_mille = std::min(
        ClampPerMille(config_.minimum_sampling_per_mille),
        config_.base_sampling_per_mille);
  }

  ~Impl() { Stop().IgnoreError(); }

  Status Start() {
    if (config_.input_capacity == 0 || config_.retained_capacity == 0 ||
        config_.correctness_reserve > config_.input_capacity ||
        config_.low_watermark_per_mille > config_.high_watermark_per_mille ||
        config_.high_watermark_per_mille > 1000) {
      return Status::InvalidArgument("telemetry aggregator", "invalid bounded telemetry config");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_) return Status::OK();
    stopping_ = false;
    started_ = true;
    worker_ = std::thread([this] { WorkerLoop(); });
    return Status::OK();
  }

  Status Stop() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!started_) return Status::OK();
      stopping_ = true;
    }
    work_ready_.notify_all();
    if (worker_.joinable()) worker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    started_ = false;
    stopping_ = false;
    worker_active_ = false;
    drained_.notify_all();
    return Status::OK();
  }

  Status Flush() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!started_) {
        return Status::InvalidArgument("telemetry aggregator", "aggregator is not started");
      }
    }
    work_ready_.notify_one();
    std::unique_lock<std::mutex> lock(mutex_);
    drained_.wait(lock, [&] {
      return input_.stats().queued == 0 && !worker_active_;
    });
    return Status::OK();
  }

  TraceContext NewTraceFor(TracePriority priority) {
    uint32_t rate = current_sampling_per_mille_.load(std::memory_order_acquire);
    if (priority == TracePriority::kTail) {
      rate = std::max(rate, std::min<uint32_t>(1000, config_.base_sampling_per_mille * 4));
    } else if (priority == TracePriority::kError) {
      rate = 1000;
    }
    return NewTraceContext(TraceSampler(rate));
  }

  bool RecordSpan(const TraceContext& trace, TracePriority priority,
                  std::string category, std::string name,
                  uint64_t start_time_ns, uint64_t duration_ns,
                  std::string terminal_status) {
    if (!trace.sampled) {
      unsampled_spans_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    TelemetryEvent event;
    event.kind = TelemetryEventKind::kSpan;
    event.trace = trace;
    event.monotonic_time_ns = start_time_ns;
    event.category = std::move(category);
    event.priority = priority;
    event.name = std::move(name);
    event.duration_ns = duration_ns;
    event.terminal_status = std::move(terminal_status);
    if (!input_.Push(std::move(event))) {
      CountDropped(priority);
      return false;
    }
    work_ready_.notify_one();
    return true;
  }

  bool RecordCorrectness(std::string category, uint64_t value) {
    TelemetryEvent event;
    event.kind = TelemetryEventKind::kCorrectness;
    event.monotonic_time_ns = MonotonicTimeNs();
    event.category = std::move(category);
    event.value = value;
    event.priority = TracePriority::kError;
    event.name = "correctness";
    if (!input_.Push(std::move(event))) {
      dropped_critical_events_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    work_ready_.notify_one();
    return true;
  }

  TelemetryAggregatorStats Stats() const {
    const EventRingStats input_stats = input_.stats();
    TelemetryAggregatorStats stats;
    stats.accepted_correctness = input_stats.accepted_correctness;
    stats.accepted_spans = input_stats.accepted_spans;
    stats.unsampled_spans = unsampled_spans_.load(std::memory_order_acquire);
    stats.dropped_verbose_spans =
        dropped_verbose_spans_.load(std::memory_order_acquire);
    stats.dropped_normal_spans =
        dropped_normal_spans_.load(std::memory_order_acquire);
    stats.dropped_critical_events =
        dropped_critical_events_.load(std::memory_order_acquire);
    stats.current_sampling_per_mille =
        current_sampling_per_mille_.load(std::memory_order_acquire);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stats.retained_events = retained_.size();
    }
    return stats;
  }

  std::string Export(bool clear) {
    Flush().IgnoreError();
    std::deque<TelemetryEvent> snapshot;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      snapshot = retained_;
    }
    const TelemetryAggregatorStats stats = Stats();
    std::ostringstream output;
    output << "{\"trace_schema_version\":1"
           << ",\"sampling_per_mille\":"
           << stats.current_sampling_per_mille
           << ",\"stats\":{\"accepted_correctness\":"
           << stats.accepted_correctness
           << ",\"accepted_spans\":" << stats.accepted_spans
           << ",\"unsampled_spans\":" << stats.unsampled_spans
           << ",\"dropped_verbose_spans\":"
           << stats.dropped_verbose_spans
           << ",\"dropped_normal_spans\":"
           << stats.dropped_normal_spans
           << ",\"dropped_critical_events\":"
           << stats.dropped_critical_events
           << ",\"retained_events\":" << stats.retained_events
           << "},\"events\":[";
    for (size_t index = 0; index < snapshot.size(); ++index) {
      if (index != 0) output << ',';
      const TelemetryEvent& event = snapshot[index];
      output << "{\"kind\":\"" << EventKindName(event.kind)
             << "\",\"trace_id\":" << event.trace.trace_id
             << ",\"span_id\":" << event.trace.span_id
             << ",\"parent_span_id\":" << event.trace.parent_span_id
             << ",\"sampled\":" << (event.trace.sampled ? "true" : "false")
             << ",\"priority\":\"" << PriorityName(event.priority)
             << "\",\"monotonic_time_ns\":" << event.monotonic_time_ns
             << ",\"duration_ns\":" << event.duration_ns
             << ",\"category\":\"" << EscapeJson(event.category)
             << "\",\"name\":\"" << EscapeJson(event.name)
             << "\",\"terminal_status\":\""
             << EscapeJson(event.terminal_status)
             << "\",\"value\":" << event.value << '}';
    }
    output << "]}";
    if (clear) {
      std::lock_guard<std::mutex> lock(mutex_);
      retained_.clear();
      UpdateSamplingLocked();
    }
    return output.str();
  }

 private:
  void WorkerLoop() {
    for (;;) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        work_ready_.wait(lock, [&] {
          return stopping_ || input_.stats().queued != 0;
        });
        if (stopping_ && input_.stats().queued == 0) break;
        worker_active_ = true;
      }
      while (true) {
        std::optional<TelemetryEvent> event = input_.Pop();
        if (!event.has_value()) break;
        Retain(std::move(*event));
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        worker_active_ = false;
      }
      drained_.notify_all();
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      worker_active_ = false;
    }
    drained_.notify_all();
  }

  void Retain(TelemetryEvent event) {
    std::lock_guard<std::mutex> lock(mutex_);
    const bool critical = event.kind == TelemetryEventKind::kCorrectness ||
        event.priority == TracePriority::kError ||
        event.priority == TracePriority::kTail;
    if (retained_.size() < config_.retained_capacity) {
      retained_.push_back(std::move(event));
      UpdateSamplingLocked();
      return;
    }
    if (critical) {
      const auto victim = std::find_if(
          retained_.begin(), retained_.end(), [](const TelemetryEvent& retained) {
            return retained.kind == TelemetryEventKind::kSpan &&
                retained.priority != TracePriority::kError &&
                retained.priority != TracePriority::kTail;
          });
      if (victim != retained_.end()) {
        CountDropped(victim->priority);
        retained_.erase(victim);
        retained_.push_back(std::move(event));
      } else {
        dropped_critical_events_.fetch_add(1, std::memory_order_relaxed);
      }
    } else {
      CountDropped(event.priority);
    }
    UpdateSamplingLocked();
  }

  void CountDropped(TracePriority priority) {
    if (priority == TracePriority::kVerbose) {
      dropped_verbose_spans_.fetch_add(1, std::memory_order_relaxed);
    } else if (priority == TracePriority::kNormal) {
      dropped_normal_spans_.fetch_add(1, std::memory_order_relaxed);
    } else {
      dropped_critical_events_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void UpdateSamplingLocked() {
    const uint64_t occupancy = retained_.size() * 1000 /
        config_.retained_capacity;
    if (occupancy >= config_.high_watermark_per_mille) {
      current_sampling_per_mille_.store(
          config_.minimum_sampling_per_mille, std::memory_order_release);
    } else if (occupancy <= config_.low_watermark_per_mille) {
      current_sampling_per_mille_.store(
          config_.base_sampling_per_mille, std::memory_order_release);
    }
  }

  TelemetryAggregatorConfig config_;
  EventRing input_;
  mutable std::mutex mutex_;
  std::condition_variable work_ready_;
  std::condition_variable drained_;
  std::deque<TelemetryEvent> retained_;
  std::thread worker_;
  bool started_ = false;
  bool stopping_ = false;
  bool worker_active_ = false;
  std::atomic<uint32_t> current_sampling_per_mille_;
  std::atomic<uint64_t> unsampled_spans_{0};
  std::atomic<uint64_t> dropped_verbose_spans_{0};
  std::atomic<uint64_t> dropped_normal_spans_{0};
  std::atomic<uint64_t> dropped_critical_events_{0};
};

TelemetryAggregator::TelemetryAggregator(TelemetryAggregatorConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

TelemetryAggregator::~TelemetryAggregator() = default;

Status TelemetryAggregator::Start() { return impl_->Start(); }
Status TelemetryAggregator::Stop() { return impl_->Stop(); }
Status TelemetryAggregator::Flush() { return impl_->Flush(); }

TraceContext TelemetryAggregator::NewTrace(TracePriority priority) {
  if (kCedarMinimalInstrumentation && priority != TracePriority::kError) {
    return TraceContext{};
  }
  return impl_->NewTraceFor(priority);
}

TraceContext TelemetryAggregator::NewChild(const TraceContext& parent) const {
  return NewChildTraceContext(parent);
}

bool TelemetryAggregator::RecordSpan(
    const TraceContext& trace, TracePriority priority, std::string category,
    std::string name, uint64_t start_time_ns, uint64_t duration_ns,
    std::string terminal_status) {
  if (kCedarMinimalInstrumentation && priority != TracePriority::kError) {
    return false;
  }
  return impl_->RecordSpan(trace, priority, std::move(category), std::move(name),
                           start_time_ns, duration_ns,
                           std::move(terminal_status));
}

bool TelemetryAggregator::RecordCorrectness(std::string category,
                                            uint64_t value) {
  return impl_->RecordCorrectness(std::move(category), value);
}

TelemetryAggregatorStats TelemetryAggregator::stats() const {
  return impl_->Stats();
}

std::string TelemetryAggregator::ExportTracesJson(bool clear) {
  return impl_->Export(clear);
}

}  // namespace cedar
