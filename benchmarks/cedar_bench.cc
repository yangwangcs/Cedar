// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/benchmark/cedar_tg.h"
#include "cedar/benchmark/environment_probe.h"
#include "cedar/benchmark/fault_campaign.h"
#include "cedar/benchmark/ldbc_adapter.h"
#include "cedar/benchmark/phase_runner.h"
#include "cedar/benchmark/profile.h"
#include "cedar/benchmark/run_manifest.h"
#include "cedar/benchmark/workload_driver.h"
#include "cedar/blob/blob_store.h"
#include "cedar/db/cedar_database.h"
#include "cedar/observability/histogram.h"

#ifndef CEDAR_BENCH_VARIANT
#define CEDAR_BENCH_VARIANT "default"
#endif

namespace {

bool ParseUnsigned(const char* text, uint64_t* value) {
  if (text == nullptr || value == nullptr || *text == '\0') return false;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (end == nullptr || *end != '\0') return false;
  *value = static_cast<uint64_t>(parsed);
  return true;
}

std::string BinaryHash(const char* path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return "unknown";
  const std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
  return cedar::BlobHashHex(cedar::Blake3Hash(bytes));
}

cedar::StatusOr<std::string> ReadTextFile(const std::string& path) {
  std::ifstream input(path);
  if (!input) {
    return cedar::Status::IOError(path, "unable to open CSV input");
  }
  return std::string((std::istreambuf_iterator<char>(input)),
                     std::istreambuf_iterator<char>());
}

cedar::ColumnSchema MakeSchema(cedar::EntityType entity_type, uint16_t column_id,
                               const char* name, cedar::PhysicalType type) {
  return cedar::ColumnSchema{entity_type, column_id, 0, name, type, 4096,
                             cedar::EncodingPolicy::kAdaptive,
                             cedar::CompressionPolicy::kLz4};
}

cedar::TelemetryAggregatorConfig BenchmarkTelemetryConfig() {
  cedar::TelemetryAggregatorConfig config;
  config.input_capacity = 4096;
  config.correctness_reserve = 256;
  config.retained_capacity = 16384;
  config.base_sampling_per_mille = 1000;
  config.minimum_sampling_per_mille = 100;
  return config;
}

std::string SerializeHistogram(const cedar::Histogram& histogram) {
  std::ostringstream output;
  output << "{\"count\":" << histogram.count()
         << ",\"sum\":" << histogram.sum()
         << ",\"min\":" << histogram.min()
         << ",\"p50\":" << histogram.Quantile(0.50)
         << ",\"p95\":" << histogram.Quantile(0.95)
         << ",\"p99\":" << histogram.Quantile(0.99)
         << ",\"p999\":" << histogram.Quantile(0.999)
         << ",\"max\":" << histogram.max() << ",\"buckets\":[";
  for (size_t index = 0; index < histogram.bucket_counts().size(); ++index) {
    if (index != 0) output << ',';
    output << histogram.bucket_counts()[index];
  }
  output << "]}";
  return output.str();
}

cedar::StatusOr<std::string> ExecuteExplainProfile(cedar::CedarDatabase* database) {
  if (database == nullptr) {
    return cedar::Status::InvalidArgument("cedar_bench", "database is required for explain");
  }
  auto opened = database->ExecuteTcypher("EXPLAIN ANALYZE MATCH (n) RETURN n;");
  if (!opened.ok()) return opened.status();
  std::unique_ptr<cedar::QueryResultStream> stream = opened.ConsumeValueOrDie();
  cedar::ResultBatch batch;
  const cedar::Status next = stream->Next(&batch);
  if (!next.ok()) return next;
  if (batch.batch().row_count() == 0) {
    return cedar::Status::Corruption("cedar_bench", "explain profile returned no rows");
  }
  const auto value = batch.batch().ValueAt(0, 0);
  if (!value.has_value() || value->type() != cedar::PhysicalType::kString) {
    return cedar::Status::Corruption("cedar_bench", "explain profile is not a string");
  }
  return std::get<std::string>(value->data());
}

void PopulateMeasurementSummary(cedar::BenchmarkArtifactSummary* summary) {
  if (summary == nullptr) return;
  const std::vector<uint64_t> bounds = {1000, 10000, 100000, 1000000,
                                       10000000, 100000000, 1000000000};
  cedar::Histogram end_to_end(bounds);
  cedar::Histogram queue(bounds);
  cedar::Histogram service(bounds);
  uint64_t successful = 0;
  for (const cedar::BenchmarkOperationSample& sample : summary->measured_samples) {
    if (sample.terminal_status != "PASS" || sample.completed_ns < sample.requested_arrival_ns) {
      continue;
    }
    ++successful;
    end_to_end.Observe(sample.completed_ns - sample.requested_arrival_ns);
    if (sample.admitted_ns >= sample.requested_arrival_ns) {
      queue.Observe(sample.admitted_ns - sample.requested_arrival_ns);
    }
    if (sample.completed_ns >= sample.started_ns) {
      service.Observe(sample.completed_ns - sample.started_ns);
    }
  }
  summary->measured_work_units = successful;
  summary->latency_p50_ns = end_to_end.Quantile(0.50);
  summary->latency_p95_ns = end_to_end.Quantile(0.95);
  summary->latency_p99_ns = end_to_end.Quantile(0.99);
  summary->latency_p999_ns = end_to_end.Quantile(0.999);
  std::ostringstream payload;
  payload << "{\"latency_ns\":" << SerializeHistogram(end_to_end)
          << ",\"queue_delay_ns\":" << SerializeHistogram(queue)
          << ",\"service_time_ns\":" << SerializeHistogram(service) << "}";
  summary->histograms_json = payload.str();
  summary->histograms_artifact_present = !summary->histograms_json.empty();
}

cedar::Status RunBackgroundMaintenance(cedar::CedarDatabase* database) {
  if (database == nullptr) {
    return cedar::Status::InvalidArgument(
        "cedar_bench", "database is required for background maintenance");
  }
  const std::vector<cedar::Status (cedar::CedarDatabase::*)()> operations = {
      &cedar::CedarDatabase::Flush,
      &cedar::CedarDatabase::Compact,
      &cedar::CedarDatabase::RotateBlobSegments,
      &cedar::CedarDatabase::CollectBlobGarbage,
      &cedar::CedarDatabase::Checkpoint,
  };
  for (const auto operation : operations) {
    const cedar::Status status = (database->*operation)();
    if (!status.ok()) return status;
  }
  return cedar::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
  const bool ldbc_mode = argc >= 2 && std::string(argv[1]) == "--ldbc";
  const bool profile_mode = argc >= 2 && std::string(argv[1]) == "--profile";
  const bool fault_mode = argc >= 2 && std::string(argv[1]) == "--fault";
  if ((!ldbc_mode && !profile_mode && !fault_mode &&
       argc != 5 && argc != 6 && argc != 7) ||
      (profile_mode && (argc < 5 || argc > 7)) ||
      (fault_mode && argc != 6) ||
      (ldbc_mode && (argc < 5 || argc > 8))) {
    std::cerr << "usage: cedar_bench <seed> <vertices> <edges> <results-root> "
                 "[point-read|bitemporal-point-read|analytical-vertex-count|valid-time-range|graph-one-hop|blob-projection|"
                 "durable-ingestion|index-equality|maintenance-cycle|htap-balanced|recovery]\n"
                 "       cedar_bench --profile <ci|workstation|paper|stress> "
                 "<seed> <results-root> [workload] [cache-mode]\n"
                 "       cedar_bench --fault "
                 "<commit_after_prepare_durable|commit_after_decision_durable|manifest_after_rename|blob_index_partial_write|sst_after_rename|sidecar_after_rename|blob_gc_after_manifest_rename|accepted_work_shutdown> "
                 "<ci|workstation|paper|stress> <seed> <results-root>\n"
                 "       cedar_bench --ldbc <nodes.csv> <edges.csv> <results-root> "
                 "[workload] [source-license] [transform-policy]\n";
    return 2;
  }
  cedar::CedarTgConfig config;
  std::string results_root;
  std::string workload_name;
  cedar::CedarTgDataset dataset;
  std::string source_dataset_kind = "cedar-tg";
  std::string source_dataset_license;
  std::string source_transform_policy;
  std::string dataset_profile_id = "explicit";
  uint32_t default_workers = 1;
  uint32_t default_queue_capacity = 4096;
  uint64_t default_arrival_interval_ns = 1000;
  cedar::BenchmarkCacheMode cache_mode =
      cedar::BenchmarkCacheMode::kColdProcessAndDatabase;
  std::optional<cedar::BenchmarkFaultScenario> fault_scenario;
  if (ldbc_mode) {
    size_t next_argument = 5;
    if (argc > static_cast<int>(next_argument)) {
      const auto parsed_workload = cedar::ParseBenchmarkWorkloadFamily(argv[next_argument]);
      if (parsed_workload.ok()) {
        workload_name = argv[next_argument++];
      }
    }
    if (argc > static_cast<int>(next_argument)) {
      source_dataset_license = argv[next_argument++];
    }
    if (argc > static_cast<int>(next_argument)) {
      source_transform_policy = argv[next_argument++];
    }
    if (next_argument != static_cast<size_t>(argc)) {
      std::cerr << "invalid LDBC arguments; expected optional workload, license, and transform policy\n";
      return 2;
    }
    const auto nodes_csv = ReadTextFile(argv[2]);
    const auto edges_csv = ReadTextFile(argv[3]);
    if (!nodes_csv.ok()) {
      std::cerr << nodes_csv.status().ToString() << "\n";
      return 1;
    }
    if (!edges_csv.ok()) {
      std::cerr << edges_csv.status().ToString() << "\n";
      return 1;
    }
    cedar::LdbcAdapterConfig adapter_config;
    adapter_config.source_license = source_dataset_license;
    adapter_config.transform_policy = source_transform_policy;
    const auto adapted = cedar::AdaptLdbcCsv(
        nodes_csv.ValueOrDie(), edges_csv.ValueOrDie(), adapter_config);
    if (!adapted.ok()) {
      std::cerr << adapted.status().ToString() << "\n";
      return 1;
    }
    dataset = adapted.ValueOrDie().dataset;
    config = dataset.config;
    results_root = argv[4];
    source_dataset_kind = adapted.ValueOrDie().source_name;
    source_dataset_license = adapted.ValueOrDie().source_license;
    source_transform_policy = adapted.ValueOrDie().transform_policy;
    dataset_profile_id = "external-derived";
  } else if (profile_mode || fault_mode) {
    size_t profile_argument = 2;
    size_t seed_argument = 3;
    size_t results_argument = 4;
    if (fault_mode) {
      const auto parsed_fault = cedar::ParseBenchmarkFaultScenario(argv[2]);
      if (!parsed_fault.ok()) {
        std::cerr << parsed_fault.status().ToString() << "\n";
        return 2;
      }
      fault_scenario = parsed_fault.ValueOrDie();
      profile_argument = 3;
      seed_argument = 4;
      results_argument = 5;
    }
    const auto parsed_profile = cedar::ParseBenchmarkScaleProfile(
        argv[profile_argument]);
    if (!parsed_profile.ok()) {
      std::cerr << parsed_profile.status().ToString() << "\n";
      return 2;
    }
    if (!ParseUnsigned(argv[seed_argument], &config.seed)) {
      std::cerr << "profile seed must be an unsigned integer\n";
      return 2;
    }
    const cedar::BenchmarkProfile profile = cedar::ResolveBenchmarkProfile(
        parsed_profile.ValueOrDie(), config.seed);
    config = profile.dataset;
    dataset_profile_id = profile.name;
    default_workers = profile.worker_count;
    default_queue_capacity = profile.queue_capacity;
    default_arrival_interval_ns = profile.arrival_interval_ns;
    results_root = argv[results_argument];
    if (profile_mode && argc >= 6) workload_name = argv[5];
    if (profile_mode && argc == 7) {
      const auto parsed_cache = cedar::ParseBenchmarkCacheMode(argv[6]);
      if (!parsed_cache.ok()) {
        std::cerr << parsed_cache.status().ToString() << "\n";
        return 2;
      }
      cache_mode = parsed_cache.ValueOrDie();
    }
    const auto dataset_or = cedar::GenerateCedarTg(config);
    if (!dataset_or.ok()) {
      std::cerr << dataset_or.status().ToString() << "\n";
      return 1;
    }
    dataset = dataset_or.ValueOrDie();
  } else {
    if (!ParseUnsigned(argv[1], &config.seed) || !ParseUnsigned(argv[2], &config.vertex_count) ||
        !ParseUnsigned(argv[3], &config.edge_count)) {
      std::cerr << "seed, vertices, and edges must be unsigned integers\n";
      return 2;
    }
    results_root = argv[4];
    if (argc >= 6) workload_name = argv[5];
    if (argc == 7) {
      const auto parsed_cache = cedar::ParseBenchmarkCacheMode(argv[6]);
      if (!parsed_cache.ok()) {
        std::cerr << parsed_cache.status().ToString() << "\n";
        return 2;
      }
      cache_mode = parsed_cache.ValueOrDie();
    }
    const auto dataset_or = cedar::GenerateCedarTg(config);
    if (!dataset_or.ok()) {
      std::cerr << dataset_or.status().ToString() << "\n";
      return 1;
    }
    dataset = dataset_or.ValueOrDie();
  }
  if (const char* cache_text = std::getenv("CEDAR_BENCH_CACHE_MODE");
      cache_text != nullptr && *cache_text != '\0') {
    const auto parsed_cache = cedar::ParseBenchmarkCacheMode(cache_text);
    if (!parsed_cache.ok()) {
      std::cerr << parsed_cache.status().ToString() << "\n";
      return 2;
    }
    cache_mode = parsed_cache.ValueOrDie();
  }
  std::string durability_mode = "durable";
  if (const char* durability_text =
          std::getenv("CEDAR_BENCH_DURABILITY_MODE");
      durability_text != nullptr && *durability_text != '\0') {
    durability_mode = durability_text;
  }
  const cedar::Status durability_status =
      cedar::ValidateBenchmarkDurabilityMode(durability_mode);
  if (!durability_status.ok()) {
    std::cerr << durability_status.ToString() << "\n";
    return 2;
  }
  const cedar::BenchmarkCachePlan cache_plan =
      cedar::ResolveBenchmarkCachePlan(cache_mode);
  cedar::BenchmarkWorkloadFamily workload_family =
      cedar::BenchmarkWorkloadFamily::kPointRead;
  if (!workload_name.empty()) {
    const auto parsed_workload = cedar::ParseBenchmarkWorkloadFamily(workload_name);
    if (!parsed_workload.ok()) {
      std::cerr << parsed_workload.status().ToString() << "\n";
      return 2;
    }
    workload_family = parsed_workload.ValueOrDie();
  }
  cedar::BenchmarkRunManifest manifest;
  manifest.source_commit = CEDAR_SOURCE_COMMIT;
  manifest.source_dirty = CEDAR_SOURCE_DIRTY != 0;
  manifest.binary_hash = BinaryHash(argv[0]);
  manifest.dataset_id = ldbc_mode ? "ldbc-derived" : "cedar-tg";
  manifest.dataset_hash = dataset.dataset_hash;
  manifest.dataset_profile_id = dataset_profile_id;
  manifest.dataset_vertex_count = config.vertex_count;
  manifest.dataset_edge_count = config.edge_count;
  manifest.dataset_property_events_per_vertex =
      config.property_events_per_vertex;
  manifest.dataset_valid_time_span = config.valid_time_span;
  manifest.generator_seed = config.seed;
  manifest.source_dataset_kind = source_dataset_kind;
  manifest.source_dataset_license = source_dataset_license;
  manifest.source_transform_policy = source_transform_policy;
  manifest.workload_id = fault_scenario.has_value()
      ? std::string("cedar-fault-") +
            cedar::BenchmarkFaultScenarioName(*fault_scenario)
      : std::string("cedar-public-") +
            cedar::BenchmarkWorkloadFamilyName(workload_family);
  uint64_t configured_workers = default_workers;
  if (const char* worker_text = std::getenv("CEDAR_BENCH_WORKERS");
      worker_text != nullptr && *worker_text != '\0') {
    if (!ParseUnsigned(worker_text, &configured_workers) || configured_workers == 0 ||
        configured_workers > 1024) {
      std::cerr << "CEDAR_BENCH_WORKERS must be an integer in [1,1024]\n";
      return 2;
    }
  }
  manifest.workload_hash = cedar::BlobHashHex(cedar::Blake3Hash(
      manifest.workload_id + ":interval_ns=" +
      std::to_string(default_arrival_interval_ns) + ":workers=" +
      std::to_string(configured_workers) + ":profile=" + dataset_profile_id +
      ":cache=" + cedar::BenchmarkCacheModeName(cache_mode) +
      ":durability=" + durability_mode + ":vertices=" +
      std::to_string(config.vertex_count) + ":edges=" +
      std::to_string(config.edge_count)));
  manifest.resource_profile_id = dataset_profile_id + "-workers-" +
      std::to_string(configured_workers);
  manifest.instrumentation_profile_id =
      cedar::CedarInstrumentationProfileId();
  manifest.schema_hash = cedar::BlobHashHex(cedar::Blake3Hash(
      "vertex-existence-binary:vertex-property-string:edge-existence-binary"));
  manifest.durability_mode = durability_mode;
  manifest.cache_mode = cedar::BenchmarkCacheModeName(cache_mode);
  manifest.worker_limit = static_cast<uint32_t>(configured_workers);
  manifest.execution_nonce = std::to_string(static_cast<uint64_t>(::getpid())) + "-" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
      "-" + CEDAR_BENCH_VARIANT;
  const cedar::BenchmarkEnvironment environment = cedar::ProbeBenchmarkEnvironment();
  manifest.os_kernel = environment.os_kernel;
  manifest.cpu_model_and_count = environment.cpu_model_and_count;
  manifest.compiler_and_flags = environment.compiler_and_flags;

  cedar::BenchmarkArtifactSummary summary;
  summary.measurement_mode =
      workload_family == cedar::BenchmarkWorkloadFamily::kPointRead ||
              workload_family == cedar::BenchmarkWorkloadFamily::kBitemporalPointRead ||
              workload_family == cedar::BenchmarkWorkloadFamily::kDurableIngestion
          ? "open_loop" : "closed_loop";
  summary.cache_preparation =
      cedar::BenchmarkCachePreparationDescription(cache_mode);
  summary.maintenance_state = cache_plan.run_background_maintenance
      ? "admitted_background_maintenance_during_measurement"
      : "flush_after_measurement";
  summary.logical_work_units = 0;
  cedar::BenchmarkVerification verification;
  std::string metrics_json;
  std::string traces_json;
  std::string workload_result_checksum;
  bool workload_verified = false;
  std::unique_ptr<cedar::CedarDatabase> database;
  cedar::ColumnSchema vertex_existence;
  cedar::ColumnSchema vertex_property;
  cedar::ColumnSchema edge_existence;
  const std::string run_directory =
      (std::filesystem::path(results_root) / cedar::BenchmarkRunId(manifest)).string();
  const std::string database_path = (std::filesystem::path(run_directory) / "database").string();
  const auto make_workload_config = [&] {
    cedar::BenchmarkWorkloadConfig workload;
    workload.family = workload_family;
    workload.arrival_interval_ns = default_arrival_interval_ns;
    workload.worker_count = manifest.worker_limit;
    workload.queue_capacity = default_queue_capacity;
    workload.vertex_property_schema_epoch = vertex_property.schema_epoch;
    return workload;
  };

  cedar::BenchmarkPhaseRunner runner;
  const cedar::Status run_status = runner.Run([&](cedar::BenchmarkPhase phase) -> cedar::Status {
    switch (phase) {
      case cedar::BenchmarkPhase::kEnvironmentCheck:
        if (results_root.empty()) {
          return cedar::Status::InvalidArgument("cedar_bench", "results root is required");
        }
        return cedar::Status::OK();
      case cedar::BenchmarkPhase::kDatabaseCreateOrOpen: {
        database = std::make_unique<cedar::CedarDatabase>(
            database_path, 2, config.seed, BenchmarkTelemetryConfig());
        cedar::Status status = database->Open();
        if (!status.ok()) return status;
        status = database->RegisterColumn(MakeSchema(cedar::EntityType::Vertex, 0, "existence",
                                                      cedar::PhysicalType::kBinary),
                                          &vertex_existence);
        if (!status.ok()) return status;
        status = database->RegisterColumn(MakeSchema(cedar::EntityType::Vertex, 1, "value",
                                                      cedar::PhysicalType::kString),
                                          &vertex_property);
        if (!status.ok()) return status;
        // Edge existence is partitioned by edge type, which is also the
        // LogicalKey column id for an edge-existence event in Cedar-TG.
        return database->RegisterColumn(MakeSchema(cedar::EntityType::EdgeOut, 1, "existence",
                                                    cedar::PhysicalType::kBinary),
                                        &edge_existence);
      }
      case cedar::BenchmarkPhase::kDatasetLoad: {
        cedar::Status status = cedar::WriteCedarTgCanonicalFile(
            (std::filesystem::path(run_directory) / "dataset.cedartg").string(), dataset);
        if (!status.ok()) return status;
        for (const cedar::TemporalEvent& event : dataset.events) {
          const cedar::ColumnSchema* schema = &vertex_property;
          if (event.logical_key().entity_type() == cedar::EntityType::EdgeOut) {
            schema = &edge_existence;
          } else if (event.logical_key().IsExistence()) {
            schema = &vertex_existence;
          }
          status = event.is_delete()
              ? database->Delete(event.logical_key(), event.valid_from(), schema->schema_epoch)
              : database->Put(event.logical_key(), event.valid_from(), schema->schema_epoch,
                              event.value());
          if (!status.ok()) return status;
        }
        return cedar::Status::OK();
      }
      case cedar::BenchmarkPhase::kLoadVerification: {
        const auto value = database->Get(cedar::LogicalKey::VertexExistence(1), 0);
        if (!value.ok() || !value.ValueOrDie().has_value()) {
          return value.ok() ? cedar::Status::Corruption("cedar_bench", "loaded vertex is absent")
                            : value.status();
        }
        verification.load_passed = true;
        return cedar::Status::OK();
      }
      case cedar::BenchmarkPhase::kCachePrepare: {
        cedar::Status status = cedar::PrepareBenchmarkWorkload(
            database.get(), dataset, make_workload_config());
        if (!status.ok()) return status;
        if (cache_plan.reopen_before_measurement) {
          status = database->Flush();
          if (!status.ok()) return status;
          database.reset();
          database = std::make_unique<cedar::CedarDatabase>(
              database_path, 2, config.seed, BenchmarkTelemetryConfig());
          status = database->Open();
          if (!status.ok()) return status;
        }
        if (cache_plan.read_full_working_set) {
          status = cedar::WarmBenchmarkWorkingSet(database.get(), dataset);
          if (!status.ok()) return status;
        }
        return cedar::Status::OK();
      }
      case cedar::BenchmarkPhase::kWarmup:
        summary.warmup_sample_count = 0;
        return cedar::Status::OK();
      case cedar::BenchmarkPhase::kMeasurement: {
        cedar::Status maintenance_status = cedar::Status::OK();
        std::thread maintenance;
        if (cache_plan.run_background_maintenance) {
          maintenance = std::thread([&] {
            maintenance_status = RunBackgroundMaintenance(database.get());
          });
        }
        cedar::StatusOr<cedar::BenchmarkWorkloadResult> run =
            cedar::Status::InvalidArgument("cedar_bench", "missing workload");
        if (fault_scenario.has_value()) {
          cedar::BenchmarkFaultCampaignConfig fault_config;
          fault_config.scenario = *fault_scenario;
          fault_config.vertex_property_schema_epoch =
              vertex_property.schema_epoch;
          fault_config.valid_time = config.valid_time_span + 1000;
          fault_config.reopen_database = [&] {
            return std::make_unique<cedar::CedarDatabase>(
                database_path, 2, config.seed, BenchmarkTelemetryConfig());
          };
          run = cedar::RunBenchmarkFaultCampaign(
              &database, dataset, fault_config);
        } else {
          run = cedar::RunBenchmarkWorkload(
              database.get(), dataset, make_workload_config());
        }
        if (maintenance.joinable()) maintenance.join();
        if (!run.ok()) return run.status();
        if (!maintenance_status.ok()) return maintenance_status;
        summary.measured_samples = run.ValueOrDie().samples;
        summary.measurement_elapsed_ns = run.ValueOrDie().elapsed_ns;
        summary.logical_work_units = run.ValueOrDie().logical_work_units;
        summary.derived_metrics = run.ValueOrDie().derived_metrics;
        summary.transaction_measurements = run.ValueOrDie().transaction_measurements;
        summary.durable_write_bytes = run.ValueOrDie().durable_write_bytes;
        summary.physical_read_bytes = run.ValueOrDie().physical_read_bytes;
        summary.physical_read_bytes_available =
            run.ValueOrDie().physical_read_bytes_available;
        summary.physical_write_bytes = run.ValueOrDie().physical_write_bytes;
        summary.physical_write_bytes_available =
            run.ValueOrDie().physical_write_bytes_available;
        summary.measurement_mode = run.ValueOrDie().measurement_mode;
        workload_result_checksum = run.ValueOrDie().result_checksum;
        workload_verified = run.ValueOrDie().verified;
        PopulateMeasurementSummary(&summary);
        summary.measurement_throughput = summary.measurement_elapsed_ns == 0
            ? 0.0
            : static_cast<double>(summary.measured_work_units) * 1.0e9 /
                  static_cast<double>(summary.measurement_elapsed_ns);
        return run.ValueOrDie().terminal_status;
      }
      case cedar::BenchmarkPhase::kDrainAndMaintenance:
        return database->Flush();
      case cedar::BenchmarkPhase::kResultVerification: {
        const auto value = database->Get(cedar::LogicalKey::VertexExistence(config.vertex_count), 0);
        if (!value.ok() || !value.ValueOrDie().has_value()) {
          return value.ok() ? cedar::Status::Corruption("cedar_bench", "result vertex is absent")
                            : value.status();
        }
        if (!workload_verified || workload_result_checksum.empty()) {
          return cedar::Status::Corruption(
              "cedar_bench", "workload result verification did not complete");
        }
        verification.result_passed = true;
        verification.result_checksum = workload_result_checksum;
        return cedar::Status::OK();
      }
      case cedar::BenchmarkPhase::kReopenVerification: {
        metrics_json = database->ExportMetricsJson();
        traces_json = database->ExportTracesJson(false);
        database.reset();
        database = std::make_unique<cedar::CedarDatabase>(
            database_path, 2, config.seed, BenchmarkTelemetryConfig());
        cedar::Status status = database->Open();
        if (!status.ok()) return status;
        const auto value = database->Get(cedar::LogicalKey::VertexExistence(1), 0);
        if (!value.ok() || !value.ValueOrDie().has_value()) {
          return value.ok() ? cedar::Status::Corruption("cedar_bench", "reopened vertex is absent")
                            : value.status();
        }
        if (workload_family == cedar::BenchmarkWorkloadFamily::kDurableIngestion ||
            workload_family == cedar::BenchmarkWorkloadFamily::kHtapBalanced ||
            workload_family == cedar::BenchmarkWorkloadFamily::kRecovery) {
          const auto ingested = database->Get(
              cedar::LogicalKey::VertexProperty(config.vertex_count, 1),
              std::numeric_limits<uint64_t>::max());
          const std::string expected_text =
              workload_family == cedar::BenchmarkWorkloadFamily::kHtapBalanced
                  ? cedar::BenchmarkHtapIngestionValue(dataset, config.vertex_count)
                  : workload_family == cedar::BenchmarkWorkloadFamily::kRecovery
                      ? cedar::BenchmarkRecoveryValue(dataset, config.vertex_count)
                      : cedar::BenchmarkDurableIngestionValue(dataset, config.vertex_count);
          const std::optional<cedar::Value> expected =
              cedar::Value::String(expected_text);
          if (!ingested.ok() || ingested.ValueOrDie() != expected) {
            return ingested.ok()
                ? cedar::Status::Corruption(
                      "cedar_bench", "reopened ingestion correction differs")
                : ingested.status();
          }
        }
        const auto profile = ExecuteExplainProfile(database.get());
        if (!profile.ok()) return profile.status();
        summary.explain_json = profile.ValueOrDie();
        summary.explain_artifact_present = true;
        verification.reopen_passed = true;
        return cedar::Status::OK();
      }
      case cedar::BenchmarkPhase::kArtifactFinalize:
        if (database) {
          const auto visible = database->visible_seq();
          if (!visible.ok()) return visible.status();
          const uint64_t drained_visible_seq = visible.ValueOrDie();
          summary.derived_metrics.visible_prefix_lag = cedar::BenchmarkLag{
              true, drained_visible_seq, drained_visible_seq};
        }
        summary.metrics_json = metrics_json;
        summary.metrics_artifact_present = !metrics_json.empty();
        summary.traces_json = traces_json;
        summary.traces_artifact_present = !traces_json.empty();
        return cedar::Status::OK();
    }
    return cedar::Status::InvalidArgument("cedar_bench", "unknown benchmark phase");
  });
  summary.phases = runner.records();
  verification.detail = run_status.ok()
      ? (fault_scenario.has_value()
             ? cedar::BenchmarkFaultVerificationDetail(*fault_scenario)
             : std::string("public API load, ") +
                   cedar::BenchmarkWorkloadFamilyName(workload_family) +
                   ", flush, and reopen verification")
      : run_status.ToString();
  const auto artifacts = cedar::WriteBenchmarkArtifacts(results_root, manifest, environment, summary,
                                                         verification);
  if (!artifacts.ok()) {
    std::cerr << artifacts.status().ToString() << "\n";
    return 1;
  }
  if (!run_status.ok()) {
    std::cerr << run_status.ToString() << " artifact=" << artifacts.ValueOrDie().run_directory << "\n";
    return 1;
  }
  std::cout << "run_id=" << cedar::BenchmarkRunId(manifest)
            << " artifact=" << artifacts.ValueOrDie().run_directory
            << " events=" << dataset.events.size() << "\n";
  return 0;
}
