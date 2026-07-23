// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/db/cedar_database.h"

#include <atomic>
#include <chrono>
#include <utility>

#include "cedar/columnar/page_format.h"
#include "cedar/observability/metric_exporter.h"
#include "cedar/observability/instrumentation_profile.h"

namespace cedar {

struct QueryStorageMetricSink {
  void Record(const TcypherExecutionStats& stats) {
    page_bytes_decoded.fetch_add(stats.page_bytes_decoded,
                                 std::memory_order_relaxed);
    page_bytes_skipped.fetch_add(stats.page_bytes_skipped,
                                 std::memory_order_relaxed);
    sst_physical_bytes_read.fetch_add(stats.sst_physical_bytes_read,
                                      std::memory_order_relaxed);
    page_decode_count.fetch_add(stats.page_decode_count,
                                std::memory_order_relaxed);
    page_decode_latency_ns.fetch_add(stats.page_decode_latency_ns,
                                     std::memory_order_relaxed);
  }

  StorageRuntimeStats Snapshot() const {
    StorageRuntimeStats stats;
    stats.page_bytes_decoded =
        page_bytes_decoded.load(std::memory_order_relaxed);
    stats.page_bytes_skipped =
        page_bytes_skipped.load(std::memory_order_relaxed);
    stats.sst_physical_bytes_read =
        sst_physical_bytes_read.load(std::memory_order_relaxed);
    stats.page_decode_count =
        page_decode_count.load(std::memory_order_relaxed);
    stats.page_decode_latency_ns =
        page_decode_latency_ns.load(std::memory_order_relaxed);
    return stats;
  }

  std::atomic<uint64_t> page_bytes_decoded{0};
  std::atomic<uint64_t> page_bytes_skipped{0};
  std::atomic<uint64_t> sst_physical_bytes_read{0};
  std::atomic<uint64_t> page_decode_count{0};
  std::atomic<uint64_t> page_decode_latency_ns{0};
};

namespace {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr uint64_t kMemtableSoftPressureBytes = 32 * kMiB;
constexpr uint64_t kMemtableHardPressureBytes = 48 * kMiB;
constexpr uint64_t kMemtableEmergencyPressureBytes = 60 * kMiB;
constexpr uint64_t kInteractiveQueryMemoryBytes = 16 * kMiB;
constexpr uint64_t kAnalyticalQueryMemoryBytes = 32 * kMiB;
constexpr std::array<uint64_t, 12> kLatencyBoundsNs = {
    1'000, 5'000, 10'000, 50'000, 100'000, 500'000,
    1'000'000, 5'000'000, 10'000'000, 50'000'000, 100'000'000, 1'000'000'000};

uint64_t ElapsedNs(std::chrono::steady_clock::time_point start) {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - start).count());
}

uint64_t LogicalResultBytes(const ResultBatch& result) {
  const auto add = [](uint64_t left, uint64_t right) {
    return right > UINT64_MAX - left ? UINT64_MAX : left + right;
  };
  uint64_t bytes = 0;
  for (uint32_t row = 0; row < result.batch().row_count(); ++row) {
    for (uint32_t column = 0; column < result.batch().column_count(); ++column) {
      bytes = add(bytes, 1);  // Canonical null/presence marker.
      if (result.batch().IsStructured(column)) {
        const std::optional<StructValue> value =
            result.batch().StructAt(column, row);
        if (!value.has_value()) continue;
        bytes = add(bytes, sizeof(uint32_t));
        for (const StructField& field : value->fields) {
          bytes = add(bytes, sizeof(uint32_t));
          bytes = add(bytes, field.name.size());
          bytes = add(bytes, 1);
          if (field.value.has_value()) {
            bytes = add(bytes, field.value->Encode().size());
          }
        }
        continue;
      }
      if (result.batch().IsList(column)) {
        const std::optional<ListValue> value =
            result.batch().ListAt(column, row);
        if (!value.has_value()) continue;
        bytes = add(bytes, sizeof(uint32_t));
        for (const std::optional<Value>& element : value->elements) {
          bytes = add(bytes, 1);
          if (element.has_value()) {
            bytes = add(bytes, element->Encode().size());
          }
        }
        continue;
      }
      const std::optional<Value> value = result.batch().ValueAt(column, row);
      if (!value.has_value()) continue;
      bytes = add(bytes, value->Encode().size());
    }
  }
  return bytes;
}

void RegisterDatabaseMetrics(MetricRegistry* metrics) {
  const auto counter = [metrics](const char* name) {
    metrics->Register(MetricDefinition{name, MetricType::kCounter, "count", 1}).IgnoreError();
  };
  const auto histogram = [metrics](const char* name) {
    metrics->Register(MetricDefinition{name, MetricType::kHistogram, "ns", 1,
                                       std::vector<uint64_t>(kLatencyBoundsNs.begin(),
                                                             kLatencyBoundsNs.end())}).IgnoreError();
  };
  const auto gauge = [metrics](const char* name, const char* unit) {
    metrics->Register(
        MetricDefinition{name, MetricType::kGauge, unit, 1}).IgnoreError();
  };
  counter("cedar_txn_started_total");
  counter("cedar_txn_committed_total");
  counter("cedar_txn_aborted_total");
  counter("cedar_txn_indeterminate_total");
  counter("cedar_txn_conflict_total");
  histogram("cedar_txn_commit_latency_ns");
  histogram("cedar_txn_prepare_latency_ns");
  histogram("cedar_txn_decision_latency_ns");
  histogram("cedar_decisionlog_fsync_latency_ns");
  histogram("cedar_txn_visible_prefix_stall_ns");
  counter("cedar_commit_completion_stall_total");
  gauge("cedar_txn_visible_prefix_lag_seq", "sequence");
  counter("cedar_point_read_total");
  counter("cedar_point_read_failed_total");
  histogram("cedar_point_read_latency_ns");
  counter("cedar_maintenance_completed_total");
  counter("cedar_maintenance_failed_total");
  counter("cedar_page_codec_self_test_total");
  counter("cedar_page_codec_self_test_failed_total");
  counter("cedar_database_queries_cancelled_total");
  histogram("cedar_maintenance_latency_ns");
  gauge("cedar_trace_sampling_per_mille", "per_mille");
  gauge("cedar_trace_retained_events", "events");
  gauge("cedar_trace_accepted_events", "events");
  gauge("cedar_trace_unsampled_events", "events");
  gauge("cedar_trace_dropped_events", "events");
  gauge("cedar_database_lifecycle_phase", "phase");
  gauge("cedar_database_active_commits", "operations");
  gauge("cedar_database_active_point_reads", "operations");
  gauge("cedar_database_active_queries", "queries");
  gauge("cedar_database_active_query_calls", "calls");
  gauge("cedar_database_active_maintenance", "operations");
  gauge("cedar_index_health_events", "events");
  gauge("cedar_index_health_repairs_scheduled", "repairs");
  gauge("cedar_index_health_repair_failures", "failures");
  gauge("cedar_index_health_repairs_pending", "repairs");
  metrics->Register(MetricDefinition{
      "cedar_storage_bytes", MetricType::kCounter, "bytes", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_storage_operations", MetricType::kCounter, "count", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_storage_latency_ns", MetricType::kCounter, "ns", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_page_uncompressed_bytes_total", MetricType::kCounter,
      "bytes", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_page_stored_bytes_total", MetricType::kCounter,
      "bytes", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_cache_hit_total", MetricType::kCounter, "count", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_cache_miss_total", MetricType::kCounter, "count", 1}).IgnoreError();
}

const char* TransactionMeasurementModeName(TransactionMeasurementMode mode) {
  return mode == TransactionMeasurementMode::kStrict ? "strict" : "snapshot";
}

const char* TransactionMeasurementOutcomeName(
    TransactionMeasurementOutcome outcome) {
  switch (outcome) {
    case TransactionMeasurementOutcome::kCommitted: return "committed";
    case TransactionMeasurementOutcome::kAborted: return "aborted";
    case TransactionMeasurementOutcome::kIndeterminate: return "indeterminate";
    case TransactionMeasurementOutcome::kSucceeded: return "succeeded";
    case TransactionMeasurementOutcome::kFailed: return "failed";
  }
  return "failed";
}

void RecordTransactionMetricEvent(MetricRegistry* metrics,
                                  const TransactionMeasurementEvent& event) {
  if (metrics == nullptr) return;
  const std::string mode = TransactionMeasurementModeName(event.mode);
  const std::string outcome = TransactionMeasurementOutcomeName(event.outcome);
  const std::string phase_label = mode + ":" + outcome;
  switch (event.kind) {
    case TransactionMeasurementKind::kStarted:
      metrics->AddCounter("cedar_txn_started_total", mode, 1).IgnoreError();
      return;
    case TransactionMeasurementKind::kTerminal:
      metrics->ObserveHistogram("cedar_txn_commit_latency_ns", phase_label,
                                event.duration_ns).IgnoreError();
      if (event.outcome == TransactionMeasurementOutcome::kCommitted) {
        metrics->AddCounter("cedar_txn_committed_total", mode, 1).IgnoreError();
      } else if (event.outcome == TransactionMeasurementOutcome::kIndeterminate) {
        metrics->AddCounter("cedar_txn_indeterminate_total", mode, 1).IgnoreError();
      } else {
        metrics->AddCounter("cedar_txn_aborted_total",
                            mode + ":" + event.reason, 1).IgnoreError();
        if (event.reason == "serialization_conflict") {
          metrics->AddCounter("cedar_txn_conflict_total", mode, 1).IgnoreError();
        }
      }
      return;
    case TransactionMeasurementKind::kPrepareLatency:
      metrics->ObserveHistogram("cedar_txn_prepare_latency_ns", phase_label,
                                event.duration_ns).IgnoreError();
      return;
    case TransactionMeasurementKind::kDecisionLatency:
      metrics->ObserveHistogram("cedar_txn_decision_latency_ns", phase_label,
                                event.duration_ns).IgnoreError();
      return;
    case TransactionMeasurementKind::kDecisionFsyncLatency:
      metrics->ObserveHistogram("cedar_decisionlog_fsync_latency_ns", phase_label,
                                event.duration_ns).IgnoreError();
      return;
    case TransactionMeasurementKind::kVisiblePrefixWait:
      metrics->ObserveHistogram("cedar_txn_visible_prefix_stall_ns", phase_label,
                                event.duration_ns).IgnoreError();
      if (event.outcome == TransactionMeasurementOutcome::kSucceeded) {
        metrics->SetGauge("cedar_txn_visible_prefix_lag_seq", mode,
                          event.lag_seq).IgnoreError();
        if (event.nonzero_stall) {
          metrics->AddCounter("cedar_commit_completion_stall_total", mode, 1)
              .IgnoreError();
        }
      }
      return;
  }
}

void ExportResourceMetrics(const ResourceProfile& used,
                           MetricRegistry* metrics) {
  metrics->Register(MetricDefinition{
      "cedar_resource_used_bytes", MetricType::kGauge, "bytes", 1}).IgnoreError();
  metrics->Register(MetricDefinition{
      "cedar_resource_used_units", MetricType::kGauge, "units", 1}).IgnoreError();
  metrics->SetGauge("cedar_resource_used_bytes", "memory_bytes",
                    used.memory_bytes).IgnoreError();
  metrics->SetGauge("cedar_resource_used_bytes", "temporary_bytes",
                    used.temporary_bytes).IgnoreError();
  metrics->SetGauge("cedar_resource_used_bytes", "sequential_read_bytes",
                    used.sequential_read_bytes).IgnoreError();
  metrics->SetGauge("cedar_resource_used_bytes", "write_bytes",
                    used.write_bytes).IgnoreError();
  metrics->SetGauge("cedar_resource_used_units", "io_tokens",
                    used.io_tokens).IgnoreError();
  metrics->SetGauge("cedar_resource_used_units", "descriptors",
                    used.descriptors).IgnoreError();
  metrics->SetGauge("cedar_resource_used_units", "cpu_slots",
                    used.cpu_slots).IgnoreError();
  metrics->SetGauge("cedar_resource_used_units", "random_read_ops",
                    used.random_read_ops).IgnoreError();
  metrics->SetGauge("cedar_resource_used_units", "metadata_ops",
                    used.metadata_ops).IgnoreError();
}

void RecordMaintenance(MetricRegistry* metrics, const char* work_class,
                       std::chrono::steady_clock::time_point start, const Status& status) {
  if (kCedarMinimalInstrumentation) {
    if (!status.ok()) {
      metrics->AddCounter("cedar_maintenance_failed_total", work_class, 1)
          .IgnoreError();
    }
    return;
  }
  metrics->ObserveHistogram("cedar_maintenance_latency_ns", work_class, ElapsedNs(start)).IgnoreError();
  metrics->AddCounter(status.ok() ? "cedar_maintenance_completed_total"
                                  : "cedar_maintenance_failed_total",
                      work_class, 1).IgnoreError();
}

const char* TraceStatus(const Status& status) {
  if (status.ok()) return "OK";
  if (status.IsNotFound()) return "NOT_FOUND";
  if (status.IsCorruption() || status.IsBlobCorruption()) return "CORRUPTION";
  if (status.IsIOError()) return "IO_ERROR";
  if (status.IsInvalidArgument() || status.IsParseError() ||
      status.IsBindError() || status.IsSchemaMismatch()) {
    return "INVALID_REQUEST";
  }
  if (status.IsConflict()) return "CONFLICT";
  if (status.IsQueryCancelled()) return "CANCELLED";
  if (status.IsQueryMemoryLimit() || status.IsResourceExhausted()) {
    return "RESOURCE_EXHAUSTED";
  }
  if (status.IsWriteStalled()) return "WRITE_STALLED";
  if (status.IsIndeterminate()) return "INDETERMINATE";
  if (status.IsRecoveryRequired()) return "RECOVERY_REQUIRED";
  return "ERROR";
}

void RecordOperationTrace(
    const std::shared_ptr<TelemetryAggregator>& telemetry,
    TraceContext trace, const char* category, const char* name,
    uint64_t start_time_ns, const Status& status) {
  if (!telemetry) return;
  if (kCedarMinimalInstrumentation && status.ok()) return;
  const TracePriority priority = status.ok() ? TracePriority::kNormal
                                              : TracePriority::kError;
  if (!status.ok() && !trace.sampled) trace = telemetry->NewTrace(priority);
  const uint64_t end_time_ns = MonotonicTimeNs();
  telemetry->RecordSpan(
      trace, priority, category, name, start_time_ns,
      end_time_ns >= start_time_ns ? end_time_ns - start_time_ns : 0,
      TraceStatus(status));
  if (!status.ok()) {
    telemetry->RecordCorrectness(std::string(category) + "_failure", 1);
  }
}

class TracedResultStream final : public QueryResultStream {
 public:
  TracedResultStream(
      std::unique_ptr<QueryResultStream> input,
      std::shared_ptr<TelemetryAggregator> telemetry, TraceContext trace,
      std::string name, uint64_t start_time_ns,
      std::shared_ptr<RuntimeFeedbackStore> runtime_feedback,
      std::shared_ptr<TcypherExecutionStats> execution_stats,
      std::shared_ptr<QueryStorageMetricSink> storage_metrics)
      : input_(std::move(input)), telemetry_(std::move(telemetry)),
        trace_(trace), name_(std::move(name)), start_time_ns_(start_time_ns),
        runtime_feedback_(std::move(runtime_feedback)),
        execution_stats_(std::move(execution_stats)),
        storage_metrics_(std::move(storage_metrics)) {}

  ~TracedResultStream() override {
    if (!finished_ && telemetry_) {
      telemetry_->RecordSpan(
          trace_, TracePriority::kTail, "query", name_, start_time_ns_,
          MonotonicTimeNs() - start_time_ns_, "ABANDONED");
    }
  }

  Status Next(ResultBatch* batch) override {
    if (!input_) {
      const Status status =
          Status::InvalidArgument("query trace", "missing result stream");
      Finish(status);
      return status;
    }
    const Status next = input_->Next(batch);
    if (!kCedarMinimalInstrumentation && next.ok() && batch != nullptr &&
        execution_stats_) {
      const uint64_t logical_bytes = LogicalResultBytes(*batch);
      execution_stats_->logical_result_bytes =
          logical_bytes > UINT64_MAX - execution_stats_->logical_result_bytes
              ? UINT64_MAX
              : execution_stats_->logical_result_bytes + logical_bytes;
    }
    if (next.IsNotFound()) {
      Finish(input_->terminal_status());
    } else if (!next.ok()) {
      Finish(next);
    }
    return next;
  }

  Status terminal_status() const override {
    return input_ ? input_->terminal_status()
                  : Status::InvalidArgument("query trace", "missing result stream");
  }

 private:
  void Finish(const Status& status) {
    if (finished_) return;
    finished_ = true;
    if (!kCedarMinimalInstrumentation && storage_metrics_ && execution_stats_) {
      storage_metrics_->Record(*execution_stats_);
    }
    if (status.ok() && runtime_feedback_ && execution_stats_ &&
        execution_stats_->runtime_feedback_key.has_value() &&
        !(execution_stats_->runtime_feedback_applied &&
          execution_stats_->runtime_feedback_source ==
              CandidateSource::kBase)) {
      uint64_t intervals = 0;
      uint64_t pages = 0;
      uint64_t blobs = 0;
      if (execution_stats_->operator_runtime) {
        for (const auto& entry :
             execution_stats_->operator_runtime->Snapshot()) {
          intervals += entry.second.output_intervals;
          pages += entry.second.pages_read;
          blobs += entry.second.blob_payload_reads;
        }
      }
      runtime_feedback_->Observe(
          *execution_stats_->runtime_feedback_key,
          RuntimeFeedbackObservation{
              execution_stats_->has_selected_access_path &&
                      execution_stats_->selected_access_path ==
                          CandidateSource::kBase
                  ? execution_stats_->physical_output_rows
                  : execution_stats_->index_candidate_entity_count,
              execution_stats_->physical_output_rows,
              intervals, pages, blobs});
    }
    if (telemetry_) {
      RecordOperationTrace(
          telemetry_, trace_, "query", name_.c_str(), start_time_ns_, status);
    }
  }

  std::unique_ptr<QueryResultStream> input_;
  std::shared_ptr<TelemetryAggregator> telemetry_;
  TraceContext trace_;
  std::string name_;
  uint64_t start_time_ns_ = 0;
  std::shared_ptr<RuntimeFeedbackStore> runtime_feedback_;
  std::shared_ptr<TcypherExecutionStats> execution_stats_;
  std::shared_ptr<QueryStorageMetricSink> storage_metrics_;
  bool finished_ = false;
};

ResourceProfile DefaultResourceLimits() {
  return ResourceProfile{
      256 * kMiB, 4096, 256, 1024 * kMiB,
      4, 64 * kMiB, 4096, 64 * kMiB, 4096};
}

ResourceProfile DefaultCriticalReserve() {
  return ResourceProfile{
      8 * kMiB, 128, 8, 0,
      1, 0, 0, 4 * kMiB, 128};
}

IoGovernorLimits DefaultIoLimits() {
  return IoGovernorLimits{
      IoTokenBudget{64 * kMiB, 64 * kMiB},
      IoTokenBudget{1U << 20, 1U << 20},
      IoTokenBudget{64 * kMiB, 64 * kMiB},
      IoTokenBudget{4096, 4096}};
}

IoTokenSnapshot DefaultIoCriticalReserve() {
  return IoTokenSnapshot{0, 0, 4 * kMiB, 128};
}

}  // namespace

CedarDatabase::CedarDatabase(
    std::string db_path, uint32_t shard_count, uint64_t hash_seed,
    TelemetryAggregatorConfig telemetry_config)
    : lifecycle_(std::make_shared<DatabaseLifecycle>()),
      resource_governor_(DefaultResourceLimits(), DefaultCriticalReserve()),
      io_governor_(DefaultIoLimits(), DefaultIoCriticalReserve()),
      cache_manager_(64 * kMiB, &resource_governor_),
      metrics_(32),
      telemetry_(std::make_shared<TelemetryAggregator>(
          std::move(telemetry_config))),
      work_scheduler_(std::make_shared<WorkScheduler>()),
      work_execution_service_(
          std::make_shared<WorkExecutionService>(
              work_scheduler_,
              static_cast<size_t>(DefaultResourceLimits().cpu_slots))),
      runtime_feedback_(std::make_shared<RuntimeFeedbackStore>(64)),
      query_storage_metrics_(std::make_shared<QueryStorageMetricSink>()),
      pressure_controller_(kMemtableHardPressureBytes, kMemtableSoftPressureBytes,
                           kMemtableEmergencyPressureBytes),
      coordinator_(std::move(db_path), shard_count, hash_seed) {
  RegisterDatabaseMetrics(&metrics_);
  coordinator_.SetResourceGovernor(&resource_governor_);
  coordinator_.SetIoGovernor(&io_governor_);
  coordinator_.SetCacheManager(&cache_manager_);
  coordinator_.SetPressureController(&pressure_controller_);
  coordinator_.SetTransactionMeasurementsEnabled(!kCedarMinimalInstrumentation);
  coordinator_.SetTransactionMeasurementSink(
      [this](const TransactionMeasurementEvent& event) {
        RecordTransactionMetricEvent(&metrics_, event);
      });
}

CedarDatabase::~CedarDatabase() {
  Close(ClosePolicy::kCancelQueries).IgnoreError();
}

Status CedarDatabase::Open() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  const Status opening = lifecycle_->BeginOpen();
  if (!opening.ok()) return opening;
  const Status codecs = VerifyPageCodecCapabilities();
  metrics_.AddCounter(codecs.ok() ? "cedar_page_codec_self_test_total"
                                  : "cedar_page_codec_self_test_failed_total",
                      "startup", 1).IgnoreError();
  if (!codecs.ok()) return codecs;
  const Status bound =
      coordinator_.SetWorkExecutionService(work_execution_service_.get());
  if (!bound.ok()) return bound;
  const Status configured =
      work_execution_service_->ConfigureResourceGovernor(
          &resource_governor_);
  if (!configured.ok()) return configured;
  const Status telemetry_started = telemetry_->Start();
  if (!telemetry_started.ok()) return telemetry_started;
  const Status started = work_execution_service_->Start();
  if (!started.ok()) {
    telemetry_->Stop().IgnoreError();
    return started;
  }
  const Status opened = coordinator_.Open();
  if (!opened.ok()) {
    work_execution_service_->Stop().IgnoreError();
    telemetry_->Stop().IgnoreError();
  }
  return opened;
}

Status CedarDatabase::Close(ClosePolicy policy) {
  if (work_execution_service_->IsCurrentWorkerThread()) {
    return Status::InvalidArgument(
        "database close",
        "blocking close cannot run on an execution-service worker");
  }
  if (!lifecycle_->BeginClose()) return lifecycle_->WaitForClose();

  const auto cancel_optional_maintenance = [this] {
    work_execution_service_->CancelPreemptible(
        WorkClass::kCompactionNormal);
    work_execution_service_->CancelPreemptible(WorkClass::kIndexBuild);
    work_execution_service_->CancelPreemptible(WorkClass::kStatsMerge);
    work_execution_service_->CancelPreemptible(WorkClass::kBlobGc);
  };
  cancel_optional_maintenance();

  lifecycle_->WaitForNoOperations(DatabaseOperationClass::kCommit);
  lifecycle_->WaitForNoOperations(DatabaseOperationClass::kPointRead);
  lifecycle_->WaitForNoOperations(DatabaseOperationClass::kQuery);
  lifecycle_->WaitForNoOperations(DatabaseOperationClass::kMaintenance);
  // An accepted flush may derive optional work after the first cancellation
  // pass. Repeat after all accepted maintenance calls leave their lifecycle
  // lease, before the typed shutdown task starts checkpointing.
  cancel_optional_maintenance();

  uint64_t cancelled_queries = 0;
  if (policy == ClosePolicy::kCancelQueries) {
    work_execution_service_->CancelQueued(WorkClass::kInteractiveQuery);
    work_execution_service_->CancelQueued(WorkClass::kAnalyticalQuery);
    cancelled_queries = lifecycle_->CancelQueriesAndWaitForCalls();
  } else {
    lifecycle_->WaitForQueriesDrained();
    cancelled_queries = lifecycle_->CancelQueriesAndWaitForCalls();
  }
  metrics_.AddCounter("cedar_database_queries_cancelled_total", "shutdown",
                      cancelled_queries).IgnoreError();

  Status close_status = Status::OK();
  const auto remember_first = [&close_status](const Status& status) {
    if (close_status.ok() && !status.ok()) close_status = status;
  };
  auto submitted = work_execution_service_->Submit(
      WorkTaskRequest{WorkClass::kShutdown,
      ResourceProfile{0, 0, 0, 0, 1}, true, 0},
      [this] {
        if (shutdown_execution_hook_) shutdown_execution_hook_();
        lifecycle_->SetPhase(DatabasePhase::kDrainingCommits);
        lifecycle_->WaitForNoOperations(DatabaseOperationClass::kCommit);
        lifecycle_->SetPhase(DatabasePhase::kDrainingMaintenance);
        work_execution_service_->CancelQueued(WorkClass::kInteractiveQuery);
        work_execution_service_->CancelQueued(WorkClass::kAnalyticalQuery);
        work_execution_service_->CancelPreemptible(
            WorkClass::kCompactionNormal);
        work_execution_service_->CancelPreemptible(WorkClass::kIndexBuild);
        work_execution_service_->CancelPreemptible(WorkClass::kStatsMerge);
        work_execution_service_->CancelPreemptible(WorkClass::kBlobGc);
        lifecycle_->WaitForNoOperations(DatabaseOperationClass::kMaintenance);
        lifecycle_->SetPhase(DatabasePhase::kCheckpointing);
        const Status flushed = coordinator_.Flush();
        return flushed.ok() ? coordinator_.CheckpointDurableLogs() : flushed;
      });
  if (!submitted.ok()) {
    remember_first(submitted.status());
  } else {
    remember_first(
        work_execution_service_->WaitForTask(submitted.ValueOrDie()));
  }
  remember_first(work_execution_service_->Stop());
  remember_first(telemetry_->Stop());
  lifecycle_->FinishClose(close_status);
  return close_status;
}
Status CedarDatabase::RegisterColumn(const ColumnSchema& schema,
                                       ColumnSchema* registered) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  return coordinator_.RegisterColumn(schema, registered);
}
Status CedarDatabase::RegisterIndex(IndexDefinition definition, uint64_t* index_id) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  return coordinator_.RegisterIndex(std::move(definition), index_id);
}
Status CedarDatabase::SetIndexState(uint64_t index_id, IndexState state) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  return coordinator_.SetIndexState(index_id, state);
}
Status CedarDatabase::RepairIndexes() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  return coordinator_.RepairIndexes();
}
Status CedarDatabase::DropIndex(uint64_t index_id) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  return coordinator_.DropIndex(index_id);
}
Status CedarDatabase::Put(const LogicalKey& key, uint64_t valid_from,
                            uint32_t schema_epoch, Value value) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  uint64_t ignored_commit_seq = 0;
  const Status status = coordinator_.Commit(coordinator_.visible_seq(), {
      PendingEvent::Put(key, valid_from, schema_epoch, std::move(value)),
  }, &ignored_commit_seq);
  RecordOperationTrace(telemetry_, trace, "transaction", "put", trace_start,
                       status);
  return status;
}
Status CedarDatabase::Delete(const LogicalKey& key, uint64_t valid_from,
                               uint32_t schema_epoch) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kCommit);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  uint64_t ignored_commit_seq = 0;
  const Status status = coordinator_.Commit(coordinator_.visible_seq(), {
      PendingEvent{key, valid_from, schema_epoch, TemporalOperation::kDelete,
                   Value::Binary("", 0)},
  }, &ignored_commit_seq);
  RecordOperationTrace(telemetry_, trace, "transaction", "delete",
                       trace_start, status);
  return status;
}
StatusOr<std::optional<Value>> CedarDatabase::Get(const LogicalKey& key,
                                                     uint64_t valid_time,
                                                     uint64_t snapshot_seq) const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const auto start = std::chrono::steady_clock::now();
  StatusOr<std::optional<Value>> value = Status::InvalidArgument(
      "point read", "point-read task did not execute");
  const auto submitted = work_execution_service_->Submit(
      WorkTaskRequest{WorkClass::kPointRead,
                      ResourceProfile{0, 0, 0, 0, 1}, false, 0},
      [this, &key, valid_time, snapshot_seq, &value] {
        value = coordinator_.GetChecked(key, valid_time, snapshot_seq);
        return value.ok() ? Status::OK() : value.status();
      });
  if (!submitted.ok()) {
    value = submitted.status();
  } else {
    const Status completed =
        work_execution_service_->WaitForTask(submitted.ValueOrDie());
    if (!completed.ok()) value = completed;
  }
  if (!kCedarMinimalInstrumentation) {
    metrics_.ObserveHistogram("cedar_point_read_latency_ns", "point",
                              ElapsedNs(start)).IgnoreError();
    metrics_.AddCounter(value.ok() ? "cedar_point_read_total"
                                   : "cedar_point_read_failed_total",
                        "point", 1).IgnoreError();
  } else if (!value.ok()) {
    metrics_.AddCounter("cedar_point_read_failed_total", "point", 1)
        .IgnoreError();
  }
  RecordOperationTrace(telemetry_, trace, "point_read", "get", trace_start,
                       value.ok() ? Status::OK() : value.status());
  return value;
}
StatusOr<std::unique_ptr<TcypherSession>> CedarDatabase::CreateTcypherSession() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kQuery);
  if (!entered.ok()) return entered.status();
  return std::make_unique<TcypherSession>(&coordinator_, lifecycle_);
}

StatusOr<std::unique_ptr<QueryResultStream>> CedarDatabase::ExecuteTcypher(
    const std::string& query, const TcypherQueryOptions& options) {
  return ExecuteTcypherWithSession(query, nullptr, options);
}

StatusOr<std::unique_ptr<QueryResultStream>> CedarDatabase::ExecuteTcypher(
    TcypherSession& session, const std::string& query,
    const TcypherQueryOptions& options) {
  if (!session.IsBoundTo(&coordinator_, lifecycle_.get())) {
    return Status::InvalidArgument("T-Cypher session", "session belongs to another database");
  }
  return ExecuteTcypherWithSession(query, &session, options);
}

StatusOr<std::unique_ptr<QueryResultStream>> CedarDatabase::ExecuteTcypherWithSession(
    const std::string& query, TcypherSession* session,
    const TcypherQueryOptions& options) {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kQuery);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const std::string trace_name =
      options.workload_class == TcypherWorkloadClass::kAnalytical
          ? "analytical_execute" : "interactive_execute";
  const auto fail = [&](const Status& status)
      -> StatusOr<std::unique_ptr<QueryResultStream>> {
    RecordOperationTrace(telemetry_, trace, "query", trace_name.c_str(),
                         trace_start, status);
    return status;
  };
  if (options.cancellation != nullptr && options.cancellation->IsCancelled()) {
    return fail(Status::QueryCancelled(
        "query admission", "query was cancelled before admission"));
  }
  const bool analytical = options.workload_class == TcypherWorkloadClass::kAnalytical;
  const Status pressure_admission = coordinator_.AdmitQuery(analytical);
  if (!pressure_admission.ok()) return fail(pressure_admission);

  TcypherQueryOptions admitted_options = options;
  admitted_options.cancellation =
      std::make_shared<QueryCancellation>(options.cancellation);
  auto registration = lifecycle_->RegisterQuery(
      admitted_options.cancellation);
  if (!registration.ok()) return fail(registration.status());
  if (admitted_options.execution_stats == nullptr) {
    admitted_options.execution_stats =
        std::make_shared<TcypherExecutionStats>();
  }
  if (admitted_options.memory_account == nullptr) {
    const uint64_t hard_limit = analytical ? kAnalyticalQueryMemoryBytes
                                           : kInteractiveQueryMemoryBytes;
    admitted_options.memory_account = std::make_shared<QueryMemoryAccount>(
        hard_limit / 2, hard_limit);
  }
  const ResourceProfile query_reservation{
      admitted_options.memory_account->hard_limit_bytes(), 0, 0, 0, 1};
  auto acquired = resource_governor_.Acquire(query_reservation);
  if (!acquired.ok()) return fail(acquired.status());
  admitted_options.spill_resource_extensions =
      resource_governor_.SharedExtension();

  TcypherExecutionContext context{
      coordinator_.commit_timeline(), 0, std::move(admitted_options), nullptr, {}};
  context.transaction_coordinator = &coordinator_;
  context.session = session;
  context.work_execution_service = work_execution_service_;
  context.io_governor = &io_governor_;
  context.runtime_feedback = runtime_feedback_;
  const Status captured = coordinator_.PopulateTcypherContext(&context);
  if (!captured.ok()) return fail(captured);
  const auto execution_stats = context.options.execution_stats;
  auto executed = cedar::ExecuteTcypher(query, std::move(context));
  if (!executed.ok()) return fail(executed.status());
  std::unique_ptr<QueryResultStream> accounted =
      std::make_unique<ResourceAccountedResultStream>(
          executed.ConsumeValueOrDie(),
          std::move(acquired).ConsumeValueOrDie());
  std::unique_ptr<QueryResultStream> traced = std::make_unique<TracedResultStream>(
      std::move(accounted), telemetry_, trace, trace_name, trace_start,
      runtime_feedback_, execution_stats, query_storage_metrics_);
  return std::unique_ptr<QueryResultStream>(
      std::make_unique<LifecycleTrackedResultStream>(
          std::move(traced), registration.ConsumeValueOrDie()));
}
Status CedarDatabase::Flush() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kMaintenance);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const auto start = std::chrono::steady_clock::now();
  const Status status = coordinator_.Flush();
  RecordMaintenance(&metrics_, "flush", start, status);
  RecordOperationTrace(telemetry_, trace, "maintenance", "flush", trace_start,
                       status);
  return status;
}
Status CedarDatabase::Compact() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kMaintenance);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const auto start = std::chrono::steady_clock::now();
  const Status status = coordinator_.Compact();
  RecordMaintenance(&metrics_, "compaction", start, status);
  RecordOperationTrace(telemetry_, trace, "maintenance", "compaction",
                       trace_start, status);
  return status;
}
Status CedarDatabase::RotateBlobSegments() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kMaintenance);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const auto start = std::chrono::steady_clock::now();
  const Status status = coordinator_.RotateBlobSegments();
  RecordMaintenance(&metrics_, "blob_rotation", start, status);
  RecordOperationTrace(telemetry_, trace, "maintenance", "blob_rotation",
                       trace_start, status);
  return status;
}
Status CedarDatabase::CollectBlobGarbage() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kMaintenance);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const auto start = std::chrono::steady_clock::now();
  const Status status = coordinator_.CollectBlobGarbage();
  RecordMaintenance(&metrics_, "blob_gc", start, status);
  RecordOperationTrace(telemetry_, trace, "maintenance", "blob_gc",
                       trace_start, status);
  return status;
}

Status CedarDatabase::Checkpoint() {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kMaintenance);
  if (!entered.ok()) return entered.status();
  const uint64_t trace_start = MonotonicTimeNs();
  const TraceContext trace = telemetry_->NewTrace(TracePriority::kNormal);
  const auto start = std::chrono::steady_clock::now();
  const Status flushed = coordinator_.Flush();
  const Status status = flushed.ok() ? coordinator_.CheckpointDurableLogs() : flushed;
  RecordMaintenance(&metrics_, "checkpoint", start, status);
  RecordOperationTrace(telemetry_, trace, "maintenance", "checkpoint",
                       trace_start, status);
  return status;
}

std::string CedarDatabase::ExportMetricsJson() const {
  RefreshTelemetry();
  return cedar::ExportMetricsJson(metrics_);
}

std::string CedarDatabase::ExportTracesJson(bool clear) const {
  return telemetry_->ExportTracesJson(clear);
}

const MetricRegistry& CedarDatabase::metrics() const {
  RefreshTelemetry();
  return metrics_;
}

StatusOr<uint64_t> CedarDatabase::visible_seq() const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  return coordinator_.visible_seq();
}

StatusOr<CacheStats> CedarDatabase::cache_stats() const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  return cache_manager_.stats();
}

StatusOr<StorageRuntimeStats> CedarDatabase::storage_stats() const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  StorageRuntimeStats stats = coordinator_.storage_stats();
  const StorageRuntimeStats query = query_storage_metrics_->Snapshot();
  const auto add = [](uint64_t left, uint64_t right) {
    return right > UINT64_MAX - left ? UINT64_MAX : left + right;
  };
  stats.page_bytes_decoded =
      add(stats.page_bytes_decoded, query.page_bytes_decoded);
  stats.page_bytes_skipped =
      add(stats.page_bytes_skipped, query.page_bytes_skipped);
  stats.sst_physical_bytes_read =
      add(stats.sst_physical_bytes_read, query.sst_physical_bytes_read);
  stats.page_decode_count =
      add(stats.page_decode_count, query.page_decode_count);
  stats.page_decode_latency_ns =
      add(stats.page_decode_latency_ns, query.page_decode_latency_ns);
  return stats;
}

StatusOr<BenchmarkStorageStats> CedarDatabase::benchmark_storage_stats(
    bool include_logical_live_bytes) const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  return coordinator_.benchmark_storage_stats(include_logical_live_bytes);
}

StatusOr<TransactionMeasurementSnapshot> CedarDatabase::transaction_measurements() const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  return coordinator_.transaction_measurements();
}

StatusOr<IndexHealthStats> CedarDatabase::index_health_stats() const {
  auto entered = lifecycle_->TryEnter(DatabaseOperationClass::kPointRead);
  if (!entered.ok()) return entered.status();
  return coordinator_.index_health_stats();
}

void CedarDatabase::RefreshTelemetry() const {
  work_execution_service_->ExportMetrics(&metrics_).IgnoreError();
  pressure_controller_.ExportMetrics(&metrics_).IgnoreError();
  ExportResourceMetrics(resource_governor_.used(), &metrics_);
  metrics_.SetGauge("cedar_database_lifecycle_phase", "current",
                    static_cast<uint64_t>(lifecycle_->phase())).IgnoreError();
  metrics_.SetGauge(
      "cedar_database_active_commits", "current",
      lifecycle_->active_operations(DatabaseOperationClass::kCommit))
      .IgnoreError();
  metrics_.SetGauge(
      "cedar_database_active_point_reads", "current",
      lifecycle_->active_operations(DatabaseOperationClass::kPointRead))
      .IgnoreError();
  metrics_.SetGauge("cedar_database_active_queries", "current",
                    lifecycle_->active_query_count()).IgnoreError();
  metrics_.SetGauge("cedar_database_active_query_calls", "current",
                    lifecycle_->active_query_calls()).IgnoreError();
  metrics_.SetGauge(
      "cedar_database_active_maintenance", "current",
      lifecycle_->active_operations(DatabaseOperationClass::kMaintenance))
      .IgnoreError();
  const IndexHealthStats index_health = coordinator_.index_health_stats();
  metrics_.SetGauge("cedar_index_health_events", "current",
                    index_health.event_count).IgnoreError();
  metrics_.SetGauge("cedar_index_health_repairs_scheduled", "current",
                    index_health.repair_schedule_count).IgnoreError();
  metrics_.SetGauge("cedar_index_health_repair_failures", "current",
                    index_health.repair_failure_count).IgnoreError();
  metrics_.SetGauge("cedar_index_health_repairs_pending", "current",
                    index_health.pending_repair_count).IgnoreError();
  PublishStorageSnapshot();
  const TelemetryAggregatorStats trace_stats = telemetry_->stats();
  metrics_.SetGauge("cedar_trace_sampling_per_mille", "current",
                    trace_stats.current_sampling_per_mille).IgnoreError();
  metrics_.SetGauge("cedar_trace_retained_events", "current",
                    trace_stats.retained_events).IgnoreError();
  metrics_.SetGauge("cedar_trace_accepted_events", "total",
                    trace_stats.accepted_correctness +
                        trace_stats.accepted_spans).IgnoreError();
  metrics_.SetGauge("cedar_trace_unsampled_events", "total",
                    trace_stats.unsampled_spans).IgnoreError();
  metrics_.SetGauge("cedar_trace_dropped_events", "total",
                    trace_stats.dropped_verbose_spans +
                        trace_stats.dropped_normal_spans +
                        trace_stats.dropped_critical_events).IgnoreError();
}

void CedarDatabase::PublishStorageSnapshot() const {
  const StorageRuntimeStats current = coordinator_.storage_stats();
  const StorageRuntimeStats query_current = query_storage_metrics_->Snapshot();
  std::lock_guard<std::mutex> lock(storage_metrics_mutex_);
  const auto publish = [this](const char* metric, const char* label,
                              uint64_t current_value,
                              uint64_t* published_value) {
    const uint64_t delta = current_value >= *published_value
        ? current_value - *published_value : current_value;
    metrics_.AddCounter(metric, label, delta).IgnoreError();
    *published_value = current_value;
  };
  publish("cedar_storage_bytes", "page_decoded",
          current.page_bytes_decoded,
          &published_storage_stats_.page_bytes_decoded);
  publish("cedar_storage_bytes", "page_skipped",
          current.page_bytes_skipped,
          &published_storage_stats_.page_bytes_skipped);
  publish("cedar_storage_bytes", "sst_physical_read",
          current.sst_physical_bytes_read,
          &published_storage_stats_.sst_physical_bytes_read);
  publish("cedar_storage_operations", "page_decode",
          current.page_decode_count,
          &published_storage_stats_.page_decode_count);
  publish("cedar_storage_latency_ns", "page_decode",
          current.page_decode_latency_ns,
          &published_storage_stats_.page_decode_latency_ns);
  publish("cedar_storage_bytes", "blob_payload_read",
          current.blob_payload_bytes_read,
          &published_storage_stats_.blob_payload_bytes_read);
  publish("cedar_storage_bytes", "blob_payload_written",
          current.blob_payload_bytes_written,
          &published_storage_stats_.blob_payload_bytes_written);
  publish("cedar_storage_bytes", "blob_payload_deduplicated",
          current.blob_payload_bytes_deduplicated,
          &published_storage_stats_.blob_payload_bytes_deduplicated);
  publish("cedar_storage_operations", "blob_lookup",
          current.blob_lookup_count,
          &published_storage_stats_.blob_lookup_count);
  publish("cedar_storage_latency_ns", "blob_lookup",
          current.blob_lookup_latency_ns,
          &published_storage_stats_.blob_lookup_latency_ns);
  publish("cedar_storage_bytes", "compaction_input",
          current.compaction_input_bytes,
          &published_storage_stats_.compaction_input_bytes);
  publish("cedar_storage_bytes", "compaction_output",
          current.compaction_output_bytes,
          &published_storage_stats_.compaction_output_bytes);
  publish("cedar_storage_bytes", "compaction_blob_payload_read",
          current.compaction_blob_payload_bytes_read,
          &published_storage_stats_.compaction_blob_payload_bytes_read);
  publish("cedar_storage_bytes", "blob_gc_live",
          current.blob_gc_live_bytes,
          &published_storage_stats_.blob_gc_live_bytes);
  publish("cedar_storage_bytes", "blob_gc_rewritten",
          current.blob_gc_rewritten_bytes,
          &published_storage_stats_.blob_gc_rewritten_bytes);
  static constexpr const char* kPageTypeLabels[kPageTypeMetricSlots] = {
      "unused", "entity_id", "target_id", "valid_from", "commit_seq",
      "operation", "value_class", "typed_value", "blob_ref", "edge_id",
      "inline_presence", "blob_presence"};
  for (size_t slot = 1; slot < kPageTypeMetricSlots; ++slot) {
    publish("cedar_page_uncompressed_bytes_total", kPageTypeLabels[slot],
            current.page_uncompressed_bytes_written[slot],
            &published_storage_stats_.page_uncompressed_bytes_written[slot]);
    publish("cedar_page_stored_bytes_total", kPageTypeLabels[slot],
            current.page_stored_bytes_written[slot],
            &published_storage_stats_.page_stored_bytes_written[slot]);
  }
  const CacheStats cache = cache_manager_.stats();
  static constexpr const char* kCacheKindLabels[kCacheKindCount] = {
      "metadata", "page", "blob_location", "blob_value"};
  for (size_t kind = 0; kind < kCacheKindCount; ++kind) {
    publish("cedar_cache_hit_total", kCacheKindLabels[kind],
            cache.hits_by_kind[kind],
            &published_cache_stats_.hits_by_kind[kind]);
    publish("cedar_cache_miss_total", kCacheKindLabels[kind],
            cache.misses_by_kind[kind],
            &published_cache_stats_.misses_by_kind[kind]);
  }
  publish("cedar_storage_bytes", "page_decoded",
          query_current.page_bytes_decoded,
          &published_query_storage_stats_.page_bytes_decoded);
  publish("cedar_storage_bytes", "page_skipped",
          query_current.page_bytes_skipped,
          &published_query_storage_stats_.page_bytes_skipped);
  publish("cedar_storage_bytes", "sst_physical_read",
          query_current.sst_physical_bytes_read,
          &published_query_storage_stats_.sst_physical_bytes_read);
  publish("cedar_storage_operations", "page_decode",
          query_current.page_decode_count,
          &published_query_storage_stats_.page_decode_count);
  publish("cedar_storage_latency_ns", "page_decode",
          query_current.page_decode_latency_ns,
          &published_query_storage_stats_.page_decode_latency_ns);
}
}  // namespace cedar
