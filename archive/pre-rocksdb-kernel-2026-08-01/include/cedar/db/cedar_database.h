// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_DB_CEDAR_DATABASE_H_
#define CEDAR_DB_CEDAR_DATABASE_H_

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "cedar/cache/cache_manager.h"
#include "cedar/db/database_lifecycle.h"
#include "cedar/observability/metric_registry.h"
#include "cedar/observability/telemetry_aggregator.h"
#include "cedar/optimizer/runtime_feedback.h"
#include "cedar/runtime/io_governor.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/runtime/pressure_controller.h"
#include "cedar/runtime/work_execution_service.h"
#include "cedar/runtime/work_scheduler.h"
#include "cedar/tcypher/executor.h"
#include "cedar/tcypher/session.h"
#include "cedar/transaction/transaction_coordinator.h"

namespace cedar {

enum class ClosePolicy : uint8_t {
  kCancelQueries = 0,
  kDrainQueries = 1,
};

struct QueryStorageMetricSink;
class BenchmarkFaultCampaignAccess;
class BenchmarkSchedulerCampaignAccess;
class CedarDatabaseTestAccess;

// Public clean-break API. All values are typed and every write carries an
// explicit registered schema epoch.
class CedarDatabase {
 public:
  CedarDatabase(
      std::string db_path, uint32_t shard_count, uint64_t hash_seed,
      TelemetryAggregatorConfig telemetry_config = {});
  ~CedarDatabase();
  Status Open();
  Status Close(ClosePolicy policy = ClosePolicy::kCancelQueries);
  Status RegisterColumn(const ColumnSchema& schema, ColumnSchema* registered);
  Status RegisterIndex(IndexDefinition definition, uint64_t* index_id);
  Status SetIndexState(uint64_t index_id, IndexState state);
  Status RepairIndexes();
  Status DropIndex(uint64_t index_id);
  Status Put(const LogicalKey& key, uint64_t valid_from, uint32_t schema_epoch,
             Value value);
  Status Delete(const LogicalKey& key, uint64_t valid_from, uint32_t schema_epoch);
  StatusOr<std::optional<Value>> Get(const LogicalKey& key, uint64_t valid_time,
                                     uint64_t snapshot_seq = 0) const;
  StatusOr<std::unique_ptr<TcypherSession>> CreateTcypherSession();
  StatusOr<std::unique_ptr<QueryResultStream>> ExecuteTcypher(
      const std::string& query, const TcypherQueryOptions& options = {});
  StatusOr<std::unique_ptr<QueryResultStream>> ExecuteTcypher(
      TcypherSession& session, const std::string& query,
      const TcypherQueryOptions& options = {});
  Status Flush();
  Status Compact();
  Status RotateBlobSegments();
  Status CollectBlobGarbage();
  Status Checkpoint();
  StatusOr<uint64_t> visible_seq() const;
  StatusOr<CacheStats> cache_stats() const;
  StatusOr<StorageRuntimeStats> storage_stats() const;
  StatusOr<BenchmarkStorageStats> benchmark_storage_stats(
      bool include_logical_live_bytes = false) const;
  StatusOr<TransactionMeasurementSnapshot> transaction_measurements() const;
  StatusOr<IndexHealthStats> index_health_stats() const;
  const MetricRegistry& metrics() const;
  std::string ExportMetricsJson() const;
  std::string ExportTracesJson(bool clear = false) const;

 private:
  friend class BenchmarkFaultCampaignAccess;
  friend class BenchmarkSchedulerCampaignAccess;
  friend class CedarDatabaseTestAccess;
  void RefreshTelemetry() const;
  void PublishStorageSnapshot() const;
  StatusOr<std::unique_ptr<QueryResultStream>> ExecuteTcypherWithSession(
      const std::string& query, TcypherSession* session,
      const TcypherQueryOptions& options);
  std::shared_ptr<DatabaseLifecycle> lifecycle_;
  ResourceGovernor resource_governor_;
  IoGovernor io_governor_;
  CacheManager cache_manager_;
  mutable MetricRegistry metrics_;
  std::shared_ptr<TelemetryAggregator> telemetry_;
  std::shared_ptr<WorkScheduler> work_scheduler_;
  std::shared_ptr<WorkExecutionService> work_execution_service_;
  std::shared_ptr<RuntimeFeedbackStore> runtime_feedback_;
  std::shared_ptr<QueryStorageMetricSink> query_storage_metrics_;
  PressureController pressure_controller_;
  TransactionCoordinator coordinator_;
  mutable std::mutex storage_metrics_mutex_;
  mutable StorageRuntimeStats published_storage_stats_;
  mutable StorageRuntimeStats published_production_storage_stats_;
  mutable uint64_t published_production_blob_hash_lookups_ = 0;
  mutable StorageRuntimeStats published_query_storage_stats_;
  mutable std::array<uint64_t, 2> published_query_started_{};
  mutable std::array<uint64_t, 2> published_query_completed_{};
  mutable std::array<uint64_t, 2> published_query_result_rows_{};
  mutable std::array<uint64_t, 2> published_operator_output_rows_{};
  mutable uint64_t published_index_candidate_rows_ = 0;
  mutable CacheStats published_cache_stats_;
  std::function<void()> shutdown_execution_hook_;
};

}  // namespace cedar

#endif  // CEDAR_DB_CEDAR_DATABASE_H_
