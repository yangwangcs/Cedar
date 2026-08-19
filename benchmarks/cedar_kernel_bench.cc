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
  std::filesystem::remove_all(options.path);
  cedar::DatabaseOptions db_options =
      cedar::benchmark::MakeBenchmarkDatabaseOptions(options);
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
  const bool seed_required = workload != cedar::benchmark::KernelWorkload::kPropertyPut;
  if (seed_required) {
    for (uint64_t index = 0; index < options.operations; ++index) {
      const auto status = WriteVertex(database.get(), index + 1);
      if (!status.ok()) {
        std::cerr << "seed write: " << status.ToString() << '\n';
        return 1;
      }
    }
  }
  auto snapshot_result = database->BeginSnapshot();
  if (!snapshot_result.ok()) {
    std::cerr << "snapshot: " << snapshot_result.status().ToString() << '\n';
    return 1;
  }
  auto snapshot = std::make_unique<cedar::Snapshot>(
      std::move(snapshot_result).ConsumeValueOrDie());
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
  sample.live_sst_bytes = runtime.ValueOrDie().live_sst_bytes;
  sample.retained_wal_bytes = runtime.ValueOrDie().retained_wal_bytes;
  sample.projected_scan_rows = runtime.ValueOrDie().projected_scan_rows;
  sample.projected_scan_bytes_read = runtime.ValueOrDie().projected_scan_bytes_read;
  sample.canonical_scan_bytes_read = runtime.ValueOrDie().canonical_scan_bytes_read;
  sample.logical_facts_bytes = runtime.ValueOrDie().logical_facts_bytes;
  sample.obsolete_sst_bytes = runtime.ValueOrDie().obsolete_sst_bytes;
  sample.temporary_output_bytes = runtime.ValueOrDie().temporary_output_bytes;
  sample.n_plus_one_eligible_epochs = metrics.n_plus_one_eligible_epochs;
  sample.n_plus_one_promoted_epochs = metrics.n_plus_one_promoted_epochs;
  sample.writer_clients = options.writer_clients;
  sample.writer_failures = bounded_writers.failures;
  sample.write_stopped = runtime.ValueOrDie().write_stopped;
  sample.background_errors = runtime.ValueOrDie().background_errors;
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
  std::cout << "profile,workload,operations,elapsed_seconds,operations_per_second,point_read_operations,multi_get_operations,"
               "live_sst_bytes,retained_wal_bytes,n_plus_one_eligible_epochs,"
               "n_plus_one_promoted_epochs,projected_scan_rows,projected_scan_bytes_read,"
               "canonical_scan_bytes_read,logical_facts_bytes,obsolete_sst_bytes,"
               "temporary_output_bytes,writer_clients,writer_failures,write_stopped,"
               "background_errors,qualification\n";
  std::cout << cedar::benchmark::BenchmarkExecutionProfileName(options.execution_profile)
            << ',' << cedar::benchmark::KernelWorkloadName(options.workload) << ','
            << sample.operations << ',' << sample.elapsed_seconds << ','
            << sample.operations_per_second << ',' << sample.point_read_operations << ','
            << sample.multi_get_operations << ','
            << sample.live_sst_bytes << ',' << sample.retained_wal_bytes << ','
            << sample.n_plus_one_eligible_epochs << ',' << sample.n_plus_one_promoted_epochs
            << ',' << sample.projected_scan_rows << ',' << sample.projected_scan_bytes_read
            << ',' << sample.canonical_scan_bytes_read << ',' << sample.logical_facts_bytes
            << ',' << sample.obsolete_sst_bytes << ',' << sample.temporary_output_bytes
            << ',' << sample.writer_clients << ',' << sample.writer_failures
            << ',' << sample.write_stopped << ',' << sample.background_errors
            << ',' << cedar::benchmark::BenchmarkQualificationStatus(options, sample) << '\n';
  return 0;
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
