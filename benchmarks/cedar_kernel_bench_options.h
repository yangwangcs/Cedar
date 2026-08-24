// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARKS_CEDAR_KERNEL_BENCH_OPTIONS_H_
#define CEDAR_BENCHMARKS_CEDAR_KERNEL_BENCH_OPTIONS_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/database.h"

namespace cedar::benchmark {

enum class CampaignKind : uint8_t { kNone, kSmoke, kWarm, kPreflight, kSustained };
enum class KernelWorkload : uint8_t {
  kPropertyPut,
  kPointRead,
  kMultiGet,
  kProjectedEventScan,
  kFullEventScan,
  kMixed90Write10PointRead,
  kMixedAppendProjectedScan,
};

struct KernelBenchmarkOptions {
  std::string path;
  std::string seed_database;
  std::string database_path;
  uint64_t seed = 0;
  bool prepare_seed_database = false;
  uint64_t duration_seconds = 30;
  uint64_t operations = 10'000;
  uint64_t read_operations = 10'000;
  uint32_t writer_clients = 1;
  uint32_t group_max_batch = 128;
  uint64_t group_max_bytes = 2ULL * 1024ULL * 1024ULL;
  uint64_t group_window_us = 200;
  uint32_t group_queue_requests = 1024;
  bool verify_reopen = true;
  CampaignKind campaign = CampaignKind::kNone;
  KernelWorkload workload = KernelWorkload::kPropertyPut;
};

struct KernelBenchmarkSample {
  uint64_t operations = 0;
  double elapsed_seconds = 0;
  double operations_per_second = 0;
  uint64_t point_read_operations = 0;
  uint64_t multi_get_operations = 0;
  uint64_t projected_scan_rows = 0;
  uint64_t projected_scan_bytes_read = 0;
  uint64_t canonical_scan_bytes_read = 0;
  uint64_t logical_facts_bytes = 0;
  uint64_t obsolete_sst_bytes = 0;
  uint64_t temporary_output_bytes = 0;
  uint64_t live_sst_bytes = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t pending_compaction_bytes = 0;
  uint64_t maintenance_max_snapshot_age_us = 0;
  uint64_t maintenance_errors = 0;
  uint64_t maintenance_recovery_exception_jobs = 0;
  uint64_t n_plus_one_eligible_epochs = 0;
  uint64_t n_plus_one_promoted_epochs = 0;
  uint32_t writer_clients = 1;
  uint64_t writer_failures = 0;
  uint64_t write_stopped = 0;
  uint64_t background_errors = 0;
  uint64_t unexplained_autonomous_jobs = 0;
  bool reopen_verified = false;
  CommitPipelineMetrics commit_pipeline;
};

StatusOr<KernelBenchmarkOptions> ParseKernelBenchmarkOptions(
    const std::vector<std::string>& arguments);
const char* KernelWorkloadName(KernelWorkload workload);
std::string BenchmarkQualificationStatus(const KernelBenchmarkOptions& options,
                                         const KernelBenchmarkSample& sample);
int CampaignExitCode(const KernelBenchmarkOptions& options,
                     const KernelBenchmarkSample& sample);

}  // namespace cedar::benchmark

#endif  // CEDAR_BENCHMARKS_CEDAR_KERNEL_BENCH_OPTIONS_H_
