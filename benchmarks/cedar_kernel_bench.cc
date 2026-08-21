// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "benchmarks/cedar_kernel_bench_options.h"
#include "benchmarks/cedar_kernel_bench_workload.h"
#include "cedar/database.h"

namespace {
using Clock = std::chrono::steady_clock;

void PrintLatencyPercentiles(const cedar::CommitLatencyHistogram& histogram) {
  std::cout << histogram.ApproximatePercentile(50) << ','
            << histogram.ApproximatePercentile(95) << ','
            << histogram.ApproximatePercentile(99);
}

cedar::Status WriteVertex(cedar::Database* database, uint64_t id) {
  auto transaction = database->BeginTransaction();
  if (!transaction.ok()) return transaction.status();
  const cedar::Status asserted = transaction.ValueOrDie()->Assert(
      cedar::EntityFact::Vertex({cedar::PartId{1}, cedar::VertexId{id}}),
      cedar::ValidTime{1});
  if (!asserted.ok()) return asserted;
  auto committed = transaction.ValueOrDie()->Commit();
  if (!committed.ok()) return committed.status();
  if (committed.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted) {
    return committed.ValueOrDie().status;
  }
  return cedar::Status::OK();
}

cedar::Status PointRead(const cedar::Snapshot& snapshot, uint64_t id) {
  auto found = snapshot.Exists(
      cedar::EntityFact::Vertex({cedar::PartId{1}, cedar::VertexId{id}}),
      cedar::ValidTime{1});
  return found.ok() ? cedar::Status::OK() : found.status();
}

cedar::Status Scan(const cedar::Snapshot& snapshot,
                   const std::vector<cedar::FactColumnId>& projection) {
  const cedar::FactScanSpec spec{cedar::PartId{1},
                                 cedar::FactFamily::kVertexState,
                                 cedar::PropertyId{}, cedar::ValidTime{1}, 256};
  return snapshot.EventColumnarScan(
      spec, projection, [](const cedar::FactColumnarBatch&) { return cedar::Status::OK(); });
}

const std::vector<cedar::FactColumnId>& ProjectedColumns() {
  static const std::vector<cedar::FactColumnId> columns = {
      cedar::FactColumnId::kEntityId};
  return columns;
}

const std::vector<cedar::FactColumnId>& CanonicalColumns() {
  static const std::vector<cedar::FactColumnId> columns = {
      cedar::FactColumnId::kPartId, cedar::FactColumnId::kFactFamily,
      cedar::FactColumnId::kPropertyId, cedar::FactColumnId::kEntityId,
      cedar::FactColumnId::kValidFrom, cedar::FactColumnId::kCedarCommitSeq,
      cedar::FactColumnId::kOperation, cedar::FactColumnId::kSchemaEpoch,
      cedar::FactColumnId::kPhysicalType, cedar::FactColumnId::kBoolValue,
      cedar::FactColumnId::kInt32Value, cedar::FactColumnId::kInt64Value,
      cedar::FactColumnId::kFloat32Value, cedar::FactColumnId::kFloat64Value,
      cedar::FactColumnId::kTimestamp64Value, cedar::FactColumnId::kBytesValue,
      cedar::FactColumnId::kSourcePartId, cedar::FactColumnId::kSourceVertexId,
      cedar::FactColumnId::kTargetPartId, cedar::FactColumnId::kTargetVertexId,
      cedar::FactColumnId::kEdgeType};
  return columns;
}

int Run(const cedar::benchmark::KernelBenchmarkOptions& options) {
  if (options.path.empty()) {
    std::cerr << "--path is required\n";
    return 2;
  }
  const auto populate = [&](cedar::Database* database) -> cedar::Status {
    const bool seed_required =
        options.workload != cedar::benchmark::KernelWorkload::kPropertyPut;
    if (!seed_required) return cedar::Status::OK();
    for (uint64_t index = 0; index < options.operations; ++index) {
      const auto status = WriteVertex(database, index + 1);
      if (!status.ok()) return status;
    }
    return cedar::Status::OK();
  };
  cedar::DatabaseOptions db_options =
      cedar::benchmark::MakeBenchmarkDatabaseOptions(options);
  if (!options.seed_database.empty()) {
    std::error_code filesystem_error;
    if (options.prepare_seed_database) {
      std::filesystem::remove_all(options.seed_database, filesystem_error);
      if (filesystem_error) {
        std::cerr << "remove seed database: " << filesystem_error.message() << '\n';
        return 1;
      }
      cedar::DatabaseOptions seed_options = db_options;
      seed_options.path = options.seed_database;
      auto seed = cedar::Database::Open(seed_options);
      if (!seed.ok()) {
        std::cerr << "open seed database: " << seed.status().ToString() << '\n';
        return 1;
      }
      const cedar::Status populated = populate(seed.ValueOrDie().get());
      if (!populated.ok()) {
        std::cerr << "seed write: " << populated.ToString() << '\n';
        return 1;
      }
      const cedar::Status closed = seed.ValueOrDie()->Close();
      if (!closed.ok()) {
        std::cerr << "close seed database: " << closed.ToString() << '\n';
        return 1;
      }
    }
    if (!std::filesystem::exists(
            std::filesystem::path(options.seed_database) / "CURRENT",
            filesystem_error) || filesystem_error) {
      std::cerr << "seed database is not prepared\n";
      return 1;
    }
    std::filesystem::remove_all(options.path, filesystem_error);
    if (filesystem_error) {
      std::cerr << "remove benchmark database: " << filesystem_error.message()
                << '\n';
      return 1;
    }
    cedar::DatabaseOptions seed_options = db_options;
    seed_options.path = options.seed_database;
    auto source = cedar::Database::Open(seed_options);
    if (!source.ok()) {
      std::cerr << "open seed checkpoint source: "
                << source.status().ToString() << '\n';
      return 1;
    }
    const cedar::Status checkpoint =
        source.ValueOrDie()->CreateCheckpoint(options.path);
    const cedar::Status closed = source.ValueOrDie()->Close();
    if (!checkpoint.ok()) {
      std::cerr << "create seed checkpoint: " << checkpoint.ToString() << '\n';
      return 1;
    }
    if (!closed.ok()) {
      std::cerr << "close checkpoint source: " << closed.ToString() << '\n';
      return 1;
    }
  } else {
    std::error_code filesystem_error;
    std::filesystem::remove_all(options.path, filesystem_error);
    if (filesystem_error) {
      std::cerr << "remove benchmark database: " << filesystem_error.message()
                << '\n';
      return 1;
    }
  }
  auto opened = cedar::Database::Open(db_options);
  if (!opened.ok()) {
    std::cerr << opened.status().ToString() << '\n';
    return 1;
  }
  auto database = std::move(opened).ConsumeValueOrDie();
  const auto verify_reopen = [&](uint64_t expected_vertex_id) -> cedar::Status {
    if (!options.verify_reopen) return cedar::Status::OK();
    const cedar::Status closed = database->Close();
    if (!closed.ok()) return closed;
    auto reopened = cedar::Database::Open(db_options);
    if (!reopened.ok()) return reopened.status();
    database = std::move(reopened).ConsumeValueOrDie();
    auto reopened_snapshot = database->BeginSnapshot();
    if (!reopened_snapshot.ok()) return reopened_snapshot.status();
    return PointRead(reopened_snapshot.ValueOrDie(), expected_vertex_id);
  };
  const auto workload = options.workload;
  if (options.seed_database.empty()) {
    const cedar::Status populated = populate(database.get());
    if (!populated.ok()) {
      std::cerr << "seed write: " << populated.ToString() << '\n';
      return 1;
    }
  }
  auto snapshot_result = database->BeginSnapshot();
  if (!snapshot_result.ok()) {
    std::cerr << "snapshot: " << snapshot_result.status().ToString() << '\n';
    return 1;
  }
  auto snapshot = std::make_unique<cedar::Snapshot>(
      std::move(snapshot_result).ConsumeValueOrDie());
  const auto initial_runtime_result = database->SampleRuntimeMetrics();
  if (!initial_runtime_result.ok()) {
    std::cerr << "initial runtime sample: "
              << initial_runtime_result.status().ToString() << '\n';
    return 1;
  }
  const cedar::RuntimeMetrics initial_runtime =
      initial_runtime_result.ValueOrDie();
  const bool concurrent_writers =
      workload == cedar::benchmark::KernelWorkload::kPropertyPut &&
      options.writer_clients > 1;
  const bool duration_bounded = concurrent_writers ||
      options.campaign == cedar::benchmark::CampaignKind::kWarm ||
      options.campaign == cedar::benchmark::CampaignKind::kPreflight ||
      options.campaign == cedar::benchmark::CampaignKind::kSustained;
  const uint64_t operation_limit = workload == cedar::benchmark::KernelWorkload::kPropertyPut
                                       ? options.operations
                                       : options.read_operations;
  const auto started = Clock::now();
  uint64_t completed_operations = 0;
  cedar::benchmark::BoundedWriterResult bounded_writers;
  if (concurrent_writers) {
    bounded_writers = cedar::benchmark::RunBoundedWriters(
        database.get(), options.writer_clients, options.duration_seconds);
    completed_operations = bounded_writers.committed;
    if (!bounded_writers.status.ok()) {
      std::cerr << "bounded writers: " << bounded_writers.status.ToString() << '\n';
      const auto runtime = database->SampleRuntimeMetrics();
      if (runtime.ok()) {
        const auto& metrics = runtime.ValueOrDie();
        std::cerr << "runtime at writer failure: write_stopped="
                  << metrics.write_stopped << " background_errors="
                  << metrics.background_error_count << " immutable_facts="
                  << metrics.immutable_fact_count << " l0_files="
                  << metrics.l0_file_count << " pending_compaction_bytes="
                  << metrics.pending_compaction_bytes << " retained_wal_bytes="
                  << metrics.retained_wal_bytes << " free_disk_bytes="
                  << metrics.free_disk_bytes << " free_disk_percent="
                  << metrics.free_disk_percent << " write_buffer_bytes="
                  << metrics.write_buffer_bytes << " write_buffer_limit_bytes="
                  << metrics.write_buffer_limit_bytes << " immutable_fact_bytes="
                  << metrics.immutable_fact_bytes << " running_flushes="
                  << metrics.running_flushes << " running_compactions="
                  << metrics.running_compactions
                  << " maintenance_flush_grants_requested="
                  << metrics.maintenance_flush_grants_requested
                  << " maintenance_flush_grants_accepted="
                  << metrics.maintenance_flush_grants_accepted
                  << " maintenance_completed_grants="
                  << metrics.maintenance_completed_grants
                  << " maintenance_flush_wal_sync_yields="
                  << metrics.maintenance_flush_wal_sync_yields
                  << " maintenance_flush_deadline_yields="
                  << metrics.maintenance_flush_deadline_yields
                  << " maintenance_last_flush_queue_depth="
                  << metrics.maintenance_last_flush_queue_depth
                  << " maintenance_last_unscheduled_flushes="
                  << metrics.maintenance_last_unscheduled_flushes
                  << " maintenance_last_scheduled_flushes="
                  << metrics.maintenance_last_scheduled_flushes
                  << " maintenance_last_running_flushes="
                  << metrics.maintenance_last_running_flushes << '\n';
      } else {
        std::cerr << "runtime at writer failure: " << runtime.status().ToString()
                  << '\n';
      }
      return 1;
    }
  } else do {
    cedar::Status status = cedar::Status::OK();
    switch (workload) {
      case cedar::benchmark::KernelWorkload::kPropertyPut:
        status = WriteVertex(database.get(), completed_operations + 1);
        break;
      case cedar::benchmark::KernelWorkload::kPointRead:
        status = PointRead(*snapshot,
                           (completed_operations % options.operations) + 1);
        break;
      case cedar::benchmark::KernelWorkload::kMultiGet:
        {
          std::vector<cedar::EntityFact> batch;
          batch.reserve(16);
          for (uint64_t slot = 0; slot < 16; ++slot) {
            batch.push_back(cedar::EntityFact::Vertex(
                {cedar::PartId{1}, cedar::VertexId{
                    ((completed_operations * 16 + slot) % options.operations) + 1}}));
          }
          auto found = snapshot->MultiExists(batch, cedar::ValidTime{1});
          if (!found.ok()) status = found.status();
        }
        break;
      case cedar::benchmark::KernelWorkload::kProjectedEventScan:
        status = Scan(*snapshot, ProjectedColumns());
        break;
      case cedar::benchmark::KernelWorkload::kFullEventScan:
        status = Scan(*snapshot, CanonicalColumns());
        break;
      case cedar::benchmark::KernelWorkload::kMixed90Write10PointRead:
        if (completed_operations % 10 == 0) {
          status = PointRead(*snapshot,
                             (completed_operations % options.operations) + 1);
        } else {
          status = WriteVertex(database.get(), options.operations + completed_operations + 1);
        }
        break;
      case cedar::benchmark::KernelWorkload::kMixedAppendProjectedScan:
        status = WriteVertex(database.get(), options.operations + completed_operations + 1);
        if (status.ok() && completed_operations % 10 == 0) {
          status = Scan(*snapshot, ProjectedColumns());
        }
        break;
    }
    if (!status.ok() && !status.IsNotFound()) {
      std::cerr << "workload operation: " << status.ToString() << '\n';
      return 1;
    }
    ++completed_operations;
  } while ((duration_bounded && Clock::now() - started <
                                std::chrono::seconds(options.duration_seconds)) ||
           (!duration_bounded && completed_operations < operation_limit));
  if (workload == cedar::benchmark::KernelWorkload::kPropertyPut) {
    auto post_write_snapshot = database->BeginSnapshot();
    if (!post_write_snapshot.ok()) {
      std::cerr << "post-write snapshot: " << post_write_snapshot.status().ToString() << '\n';
      return 1;
    }
    const cedar::Status projected = Scan(post_write_snapshot.ValueOrDie(), ProjectedColumns());
    const cedar::Status canonical = Scan(post_write_snapshot.ValueOrDie(), CanonicalColumns());
    if ((!projected.ok() && !projected.IsNotFound()) ||
        (!canonical.ok() && !canonical.IsNotFound())) {
      std::cerr << "post-write scan failed\n";
      return 1;
    }
  }
  const auto elapsed = std::chrono::duration<double>(Clock::now() - started).count();
  const auto metrics = database->GetCommitPipelineMetrics();
  const auto runtime = database->SampleRuntimeMetrics();
  if (!runtime.ok()) {
    std::cerr << "runtime sample: " << runtime.status().ToString() << '\n';
    return 1;
  }
  cedar::benchmark::KernelBenchmarkSample sample;
  sample.operations = completed_operations;
  sample.elapsed_seconds = elapsed;
  sample.operations_per_second = elapsed == 0 ? 0 : completed_operations / elapsed;
  sample.point_read_operations = runtime.ValueOrDie().point_read_operations;
  sample.multi_get_operations = runtime.ValueOrDie().multi_get_operations;
  sample.live_sst_bytes = runtime.ValueOrDie().live_fact_bytes;
  sample.retained_wal_bytes = runtime.ValueOrDie().retained_wal_bytes;
  sample.pending_compaction_bytes = runtime.ValueOrDie().pending_compaction_bytes;
  sample.maintenance_max_snapshot_age_us =
      runtime.ValueOrDie().maintenance_snapshot_age_us;
  sample.maintenance_errors = runtime.ValueOrDie().maintenance_errors;
  const auto delta = [](uint64_t current, uint64_t initial) {
    return current >= initial ? current - initial : 0;
  };
  sample.maintenance_recovery_exception_jobs = delta(
      runtime.ValueOrDie().recovery_flush_exceptions,
      initial_runtime.recovery_flush_exceptions);
  const uint64_t autonomous_flushes =
      delta(runtime.ValueOrDie().background_flush_calls,
            initial_runtime.background_flush_calls) >=
              delta(runtime.ValueOrDie().maintenance_flush_grants_accepted,
                    initial_runtime.maintenance_flush_grants_accepted) +
                  sample.maintenance_recovery_exception_jobs
          ? delta(runtime.ValueOrDie().background_flush_calls,
                  initial_runtime.background_flush_calls) -
                delta(runtime.ValueOrDie().maintenance_flush_grants_accepted,
                      initial_runtime.maintenance_flush_grants_accepted) -
                sample.maintenance_recovery_exception_jobs
          : 0;
  const uint64_t autonomous_compactions =
      delta(runtime.ValueOrDie().manual_compaction_calls,
            initial_runtime.manual_compaction_calls) >=
              delta(runtime.ValueOrDie().maintenance_compaction_grants_accepted,
                    initial_runtime.maintenance_compaction_grants_accepted)
          ? delta(runtime.ValueOrDie().manual_compaction_calls,
                  initial_runtime.manual_compaction_calls) -
                delta(runtime.ValueOrDie().maintenance_compaction_grants_accepted,
                      initial_runtime.maintenance_compaction_grants_accepted)
          : 0;
  sample.unexplained_autonomous_jobs =
      autonomous_flushes + autonomous_compactions +
      delta(runtime.ValueOrDie().periodic_task_registrations,
            initial_runtime.periodic_task_registrations);
  sample.projected_scan_rows = runtime.ValueOrDie().projected_scan_rows;
  sample.projected_scan_bytes_read = runtime.ValueOrDie().projected_scan_bytes_read;
  sample.canonical_scan_bytes_read = runtime.ValueOrDie().canonical_scan_bytes_read;
  sample.logical_facts_bytes = runtime.ValueOrDie().logical_facts_bytes;
  sample.obsolete_sst_bytes = runtime.ValueOrDie().obsolete_fact_bytes;
  sample.temporary_output_bytes = runtime.ValueOrDie().temporary_output_bytes;
  sample.n_plus_one_eligible_epochs = metrics.n_plus_one_eligible_epochs;
  sample.n_plus_one_promoted_epochs = metrics.n_plus_one_promoted_epochs;
  sample.writer_clients = options.writer_clients;
  sample.writer_failures = bounded_writers.failures;
  sample.write_stopped = runtime.ValueOrDie().write_stopped;
  sample.background_errors = runtime.ValueOrDie().background_error_count;
  sample.commit_pipeline = metrics;
  if (options.verify_reopen) {
    uint64_t expected_id = options.operations;
    if (workload == cedar::benchmark::KernelWorkload::kPropertyPut) {
      expected_id = completed_operations;
    }
    snapshot.reset();
    const cedar::Status reopened = verify_reopen(expected_id);
    if (reopened.ok()) {
      sample.reopen_verified = true;
    } else {
      std::cerr << "reopen verification: " << reopened.ToString() << '\n';
    }
  } else {
    sample.reopen_verified = true;
  }
  const double transactions_per_sync =
      sample.commit_pipeline.latency.wal_sync.count == 0
          ? 0.0
          : static_cast<double>(sample.commit_pipeline.epoch_transactions) /
                sample.commit_pipeline.latency.wal_sync.count;
  const double wal_bytes_per_transaction =
      sample.commit_pipeline.epoch_transactions == 0
          ? 0.0
          : static_cast<double>(sample.commit_pipeline.epoch_bytes) /
                sample.commit_pipeline.epoch_transactions;
  std::cout << "schema_version,workload,operations,elapsed_seconds,operations_per_second,point_read_operations,multi_get_operations,"
               "live_sst_bytes,retained_wal_bytes,pending_compaction_bytes,"
               "maintenance_snapshot_age_us,maintenance_errors,"
               "maintenance_recovery_exception_jobs,unexplained_autonomous_jobs,"
               "n_plus_one_eligible_epochs,"
               "n_plus_one_promoted_epochs,projected_scan_rows,projected_scan_bytes_read,"
               "canonical_scan_bytes_read,logical_facts_bytes,obsolete_sst_bytes,"
               "temporary_output_bytes,writer_clients,writer_failures,write_stopped,"
               "background_errors,commit_epochs,epoch_transactions,epoch_bytes,wal_sync_count,transactions_per_sync,wal_bytes_per_transaction,wal_rotations,"
               "group_fill_p50,group_fill_p95,group_fill_max,"
               "queue_p50_us,queue_p95_us,queue_p99_us,validation_p50_us,validation_p95_us,validation_p99_us,"
               "assembly_p50_us,assembly_p95_us,assembly_p99_us,wal_append_p50_us,wal_append_p95_us,wal_append_p99_us,"
               "wal_sync_p50_us,wal_sync_p95_us,wal_sync_p99_us,wal_callback_p50_us,wal_callback_p95_us,wal_callback_p99_us,"
               "manifest_p50_us,manifest_p95_us,manifest_p99_us,memtable_p50_us,memtable_p95_us,memtable_p99_us,"
               "publication_p50_us,publication_p95_us,publication_p99_us,end_to_end_p50_us,end_to_end_p95_us,end_to_end_p99_us,qualification\n";
  std::cout << "1," << cedar::benchmark::KernelWorkloadName(options.workload) << ','
            << sample.operations << ',' << sample.elapsed_seconds << ','
            << sample.operations_per_second << ',' << sample.point_read_operations << ','
            << sample.multi_get_operations << ','
            << sample.live_sst_bytes << ',' << sample.retained_wal_bytes << ','
            << sample.pending_compaction_bytes << ','
            << sample.maintenance_max_snapshot_age_us << ','
            << sample.maintenance_errors << ','
            << sample.maintenance_recovery_exception_jobs << ','
            << sample.unexplained_autonomous_jobs << ','
            << sample.n_plus_one_eligible_epochs << ',' << sample.n_plus_one_promoted_epochs
            << ',' << sample.projected_scan_rows << ',' << sample.projected_scan_bytes_read
            << ',' << sample.canonical_scan_bytes_read << ',' << sample.logical_facts_bytes
            << ',' << sample.obsolete_sst_bytes << ',' << sample.temporary_output_bytes
            << ',' << sample.writer_clients << ',' << sample.writer_failures
            << ',' << sample.write_stopped << ',' << sample.background_errors
            << ',' << sample.commit_pipeline.epochs
            << ',' << sample.commit_pipeline.epoch_transactions
            << ',' << sample.commit_pipeline.epoch_bytes
            << ',' << sample.commit_pipeline.latency.wal_sync.count
            << ',' << transactions_per_sync
            << ',' << wal_bytes_per_transaction
            << ',' << sample.commit_pipeline.wal_rotations
            << ',' << sample.commit_pipeline.group_fill.ApproximatePercentile(50)
            << ',' << sample.commit_pipeline.group_fill.ApproximatePercentile(95)
            << ',' << sample.commit_pipeline.group_fill.max_transactions << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.queue);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.validation);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.assembly);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.wal_append);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.wal_sync);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.wal_callback);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.manifest);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.memtable_insert);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.publication);
  std::cout << ',';
  PrintLatencyPercentiles(sample.commit_pipeline.latency.end_to_end);
  std::cout << ',' << cedar::benchmark::BenchmarkQualificationStatus(options, sample)
            << '\n';
  return cedar::benchmark::CampaignExitCode(options, sample);
}
}  // namespace

int main(int argc, char* argv[]) {
  const auto options = cedar::benchmark::ParseKernelBenchmarkOptions(
      std::vector<std::string>(argv + 1, argv + argc));
  if (!options.ok()) {
    std::cerr << options.status().ToString() << '\n';
    return 2;
  }
  return Run(options.ValueOrDie());
}
