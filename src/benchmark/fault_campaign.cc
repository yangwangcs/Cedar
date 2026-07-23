// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/fault_campaign.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "cedar/blob/blob_store.h"
#include "cedar/db/cedar_database.h"

namespace cedar {

class BenchmarkFaultCampaignAccess {
 public:
  static void Arm(CedarDatabase* database,
                  BenchmarkFaultScenario scenario) {
    if (scenario == BenchmarkFaultScenario::kManifestAfterRename ||
        scenario == BenchmarkFaultScenario::kBlobGcAfterManifestRename) {
      database->coordinator_.SetVersionSetFaultInjectorForTesting(
          [](VersionSetFaultPoint point) {
            return point == VersionSetFaultPoint::kAfterManifestRename
                ? Status::IOError("benchmark fault campaign", "injected")
                : Status::OK();
          });
      return;
    }
    if (scenario == BenchmarkFaultScenario::kBlobIndexPartialWrite) {
      database->coordinator_.SetBlobIndexFaultInjectorForTesting(
          [](BlobStoreFaultPoint point) {
            return point == BlobStoreFaultPoint::kAfterPartialIndexWrite
                ? Status::IOError("benchmark fault campaign", "injected")
                : Status::OK();
          });
      return;
    }
    if (scenario == BenchmarkFaultScenario::kSstAfterRename) {
      database->coordinator_.SetSstPublicationFaultInjectorForTesting(
          [](SstPublicationFaultPoint point) {
            return point == SstPublicationFaultPoint::kAfterRename
                ? Status::IOError("benchmark fault campaign", "injected")
                : Status::OK();
          });
      return;
    }
    if (scenario == BenchmarkFaultScenario::kSidecarAfterRename) {
      database->coordinator_
          .SetIndexSidecarPublicationFaultInjectorForTesting(
              [](IndexSidecarPublicationFaultPoint point) {
                return point ==
                        IndexSidecarPublicationFaultPoint::kAfterRename
                    ? Status::IOError(
                          "benchmark fault campaign", "injected")
                    : Status::OK();
              });
      return;
    }
    database->coordinator_.SetCommitFaultInjectorForTesting(
        [scenario](CommitFaultPoint point) {
          const bool selected =
              (scenario == BenchmarkFaultScenario::kCommitAfterPrepareDurable &&
               point == CommitFaultPoint::kAfterPrepareDurable) ||
              (scenario == BenchmarkFaultScenario::kCommitAfterDecisionDurable &&
               point == CommitFaultPoint::kAfterDecisionDurable);
          return selected
              ? Status::IOError("benchmark fault campaign", "injected")
              : Status::OK();
        });
  }

  static Status ShutdownAcceptedWork(CedarDatabase* database) {
    if (database == nullptr || !database->work_execution_service_) {
      return Status::InvalidArgument(
          "benchmark fault campaign", "missing work execution service");
    }
    WorkExecutionService* service = database->work_execution_service_.get();
    const ResourceProfile baseline_used = database->resource_governor_.used();
    struct Gate {
      std::mutex mutex;
      std::condition_variable changed;
      bool worker_entered = false;
      bool release_worker = false;
      bool stopping = false;
    };
    auto gate = std::make_shared<Gate>();
    const ResourceProfile grant{0, 0, 0, 0, 1};
    std::vector<WorkTaskHandle> handles;
    auto blocker = service->Submit(
        WorkTaskRequest{WorkClass::kAnalyticalQuery, grant, false, 0},
        [gate] {
          std::unique_lock<std::mutex> lock(gate->mutex);
          gate->worker_entered = true;
          gate->changed.notify_all();
          gate->changed.wait(lock, [gate] { return gate->release_worker; });
          return Status::OK();
        });
    if (!blocker.ok()) return blocker.status();
    handles.push_back(blocker.ConsumeValueOrDie());
    {
      std::unique_lock<std::mutex> lock(gate->mutex);
      if (!gate->changed.wait_for(
              lock, std::chrono::seconds(5),
              [gate] { return gate->worker_entered; })) {
        gate->release_worker = true;
        lock.unlock();
        gate->changed.notify_all();
        handles.front().Wait().IgnoreError();
        return Status::IOError(
            "benchmark fault campaign", "accepted worker did not start");
      }
    }
    for (uint32_t index = 0; index < 2; ++index) {
      auto queued = service->Submit(
          WorkTaskRequest{WorkClass::kAnalyticalQuery, grant, false, 0},
          [] { return Status::OK(); });
      if (!queued.ok()) {
        {
          std::lock_guard<std::mutex> lock(gate->mutex);
          gate->release_worker = true;
        }
        gate->changed.notify_all();
        handles.front().Wait().IgnoreError();
        return queued.status();
      }
      handles.push_back(queued.ConsumeValueOrDie());
    }
    service->SetStoppingHookForTesting([gate] {
      std::lock_guard<std::mutex> lock(gate->mutex);
      gate->stopping = true;
      gate->changed.notify_all();
    });
    Status stop_status = Status::InvalidArgument(
        "benchmark fault campaign", "shutdown did not run");
    std::atomic<bool> stop_finished{false};
    std::thread stopper([service, &stop_status, &stop_finished] {
      stop_status = service->Stop();
      stop_finished.store(true, std::memory_order_release);
    });
    bool observed_stopping = false;
    {
      std::unique_lock<std::mutex> lock(gate->mutex);
      observed_stopping = gate->changed.wait_for(
          lock, std::chrono::seconds(5), [gate] { return gate->stopping; });
      gate->release_worker = true;
    }
    gate->changed.notify_all();
    stopper.join();
    if (!observed_stopping) {
      return Status::IOError(
          "benchmark fault campaign", "shutdown did not enter stopping state");
    }
    if (!stop_status.ok()) return stop_status;
    for (const WorkTaskHandle& handle : handles) {
      const Status completed = handle.Wait();
      if (!completed.ok()) return completed;
    }
    const ResourceProfile used = database->resource_governor_.used();
    if (used.memory_bytes != baseline_used.memory_bytes ||
        used.io_tokens != baseline_used.io_tokens ||
        used.descriptors != baseline_used.descriptors ||
        used.temporary_bytes != baseline_used.temporary_bytes ||
        used.cpu_slots != baseline_used.cpu_slots ||
        used.sequential_read_bytes != baseline_used.sequential_read_bytes ||
        used.random_read_ops != baseline_used.random_read_ops ||
        used.write_bytes != baseline_used.write_bytes ||
        used.metadata_ops != baseline_used.metadata_ops ||
        !stop_finished.load(std::memory_order_acquire)) {
      return Status::Corruption(
          "benchmark fault campaign",
          "accepted-work shutdown did not release every task grant");
    }
    return Status::OK();
  }
};

namespace {

using Clock = std::chrono::steady_clock;

uint64_t ElapsedNs(Clock::time_point origin) {
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::now() - origin).count();
  return elapsed <= 0 ? 0 : static_cast<uint64_t>(elapsed);
}

bool ExpectedInjectedStatus(BenchmarkFaultScenario scenario,
                            const Status& status) {
  if (scenario == BenchmarkFaultScenario::kAcceptedWorkShutdown) {
    return status.ok();
  }
  if (scenario == BenchmarkFaultScenario::kCommitAfterPrepareDurable ||
      scenario == BenchmarkFaultScenario::kBlobIndexPartialWrite) {
    return status.IsIOError();
  }
  return status.IsIndeterminate();
}

}  // namespace

const char* BenchmarkFaultScenarioName(BenchmarkFaultScenario scenario) {
  switch (scenario) {
    case BenchmarkFaultScenario::kCommitAfterPrepareDurable:
      return "commit_after_prepare_durable";
    case BenchmarkFaultScenario::kCommitAfterDecisionDurable:
      return "commit_after_decision_durable";
    case BenchmarkFaultScenario::kManifestAfterRename:
      return "manifest_after_rename";
    case BenchmarkFaultScenario::kBlobIndexPartialWrite:
      return "blob_index_partial_write";
    case BenchmarkFaultScenario::kSstAfterRename:
      return "sst_after_rename";
    case BenchmarkFaultScenario::kSidecarAfterRename:
      return "sidecar_after_rename";
    case BenchmarkFaultScenario::kBlobGcAfterManifestRename:
      return "blob_gc_after_manifest_rename";
    case BenchmarkFaultScenario::kAcceptedWorkShutdown:
      return "accepted_work_shutdown";
  }
  return "unknown";
}

std::string BenchmarkFaultVerificationDetail(
    BenchmarkFaultScenario scenario) {
  return std::string("expected fault, durable reopen, and value verification: ") +
      BenchmarkFaultScenarioName(scenario);
}

StatusOr<BenchmarkFaultScenario> ParseBenchmarkFaultScenario(
    const std::string& name) {
  for (const BenchmarkFaultScenario scenario : {
           BenchmarkFaultScenario::kCommitAfterPrepareDurable,
           BenchmarkFaultScenario::kCommitAfterDecisionDurable,
           BenchmarkFaultScenario::kManifestAfterRename,
           BenchmarkFaultScenario::kBlobIndexPartialWrite,
           BenchmarkFaultScenario::kSstAfterRename,
           BenchmarkFaultScenario::kSidecarAfterRename,
           BenchmarkFaultScenario::kBlobGcAfterManifestRename,
           BenchmarkFaultScenario::kAcceptedWorkShutdown}) {
    if (name == BenchmarkFaultScenarioName(scenario)) return scenario;
  }
  return Status::InvalidArgument("benchmark fault campaign",
                                 "unknown fault scenario");
}

std::string BenchmarkFaultValue(
    const CedarTgDataset& dataset, uint64_t vertex_id) {
  return "bench-fault-" + dataset.dataset_hash.substr(0, 16) + "-" +
      std::to_string(vertex_id);
}

StatusOr<BenchmarkWorkloadResult> RunBenchmarkFaultCampaign(
    std::unique_ptr<CedarDatabase>* database,
    const CedarTgDataset& dataset,
    const BenchmarkFaultCampaignConfig& config) {
  if (database == nullptr || !*database || dataset.config.vertex_count == 0 ||
      dataset.dataset_hash.empty() ||
      config.vertex_property_schema_epoch == 0 ||
      !config.reopen_database) {
    return Status::InvalidArgument(
        "benchmark fault campaign",
        "open database, dataset, schema epoch, and reopen factory are required");
  }
  const uint64_t vertex_id = dataset.config.vertex_count;
  const LogicalKey key = LogicalKey::VertexProperty(vertex_id, 1);
  const std::string fault_value = BenchmarkFaultValue(dataset, vertex_id) +
      (config.scenario == BenchmarkFaultScenario::kBlobIndexPartialWrite ||
               config.scenario ==
                   BenchmarkFaultScenario::kBlobGcAfterManifestRename
           ? std::string(8192, 'b') : std::string());
  const Value value = Value::String(fault_value);
  const auto before_fault = (*database)->Get(key, config.valid_time);
  if (!before_fault.ok()) return before_fault.status();

  BenchmarkWorkloadResult result;
  result.measurement_mode = "fault_recovery";
  BenchmarkOperationSample sample;
  const Clock::time_point origin = Clock::now();
  sample.requested_arrival_ns = 0;
  sample.admitted_ns = 0;
  sample.started_ns = ElapsedNs(origin);
  Status injected;
  if (config.scenario == BenchmarkFaultScenario::kManifestAfterRename ||
      config.scenario == BenchmarkFaultScenario::kSstAfterRename) {
    const Status put = (*database)->Put(
        key, config.valid_time, config.vertex_property_schema_epoch, value);
    if (!put.ok()) return put;
    BenchmarkFaultCampaignAccess::Arm(database->get(), config.scenario);
    injected = (*database)->Flush();
  } else if (config.scenario == BenchmarkFaultScenario::kSidecarAfterRename) {
    const Status put = (*database)->Put(
        key, config.valid_time, config.vertex_property_schema_epoch, value);
    if (!put.ok()) return put;
    const Status flushed = (*database)->Flush();
    if (!flushed.ok()) return flushed;
    IndexDefinition definition;
    definition.entity_type = EntityType::Vertex;
    definition.column_id = key.column_id();
    definition.schema_epoch = config.vertex_property_schema_epoch;
    definition.capabilities = kIndexEquality;
    definition.canonical_encoding_id = kIndexCanonicalEncodingSortedDelta;
    uint64_t index_id = 0;
    const Status registered =
        (*database)->RegisterIndex(definition, &index_id);
    if (!registered.ok()) return registered;
    BenchmarkFaultCampaignAccess::Arm(database->get(), config.scenario);
    const Status building =
        (*database)->SetIndexState(index_id, IndexState::kBuilding);
    if (!building.ok()) return building;
    injected = (*database)->RepairIndexes();
  } else if (config.scenario ==
             BenchmarkFaultScenario::kBlobGcAfterManifestRename) {
    const Status put = (*database)->Put(
        key, config.valid_time, config.vertex_property_schema_epoch, value);
    if (!put.ok()) return put;
    const Status flushed = (*database)->Flush();
    if (!flushed.ok()) return flushed;
    const Status rotated = (*database)->RotateBlobSegments();
    if (!rotated.ok()) return rotated;
    BenchmarkFaultCampaignAccess::Arm(database->get(), config.scenario);
    injected = (*database)->CollectBlobGarbage();
  } else if (config.scenario == BenchmarkFaultScenario::kAcceptedWorkShutdown) {
    const Status put = (*database)->Put(
        key, config.valid_time, config.vertex_property_schema_epoch, value);
    if (!put.ok()) return put;
    const Status flushed = (*database)->Flush();
    if (!flushed.ok()) return flushed;
    injected =
        BenchmarkFaultCampaignAccess::ShutdownAcceptedWork(database->get());
  } else {
    BenchmarkFaultCampaignAccess::Arm(database->get(), config.scenario);
    injected = (*database)->Put(
        key, config.valid_time, config.vertex_property_schema_epoch, value);
  }
  if (!ExpectedInjectedStatus(config.scenario, injected)) {
    sample.completed_ns = ElapsedNs(origin);
    sample.terminal_status = injected.ok()
        ? "unexpected success"
        : injected.ToString();
    result.samples.push_back(std::move(sample));
    result.elapsed_ns = ElapsedNs(origin);
    result.terminal_status = Status::Corruption(
        "benchmark fault campaign",
        "fault did not stop at the selected durability boundary");
    return result;
  }

  database->reset();
  *database = config.reopen_database();
  if (!*database) {
    return Status::InvalidArgument("benchmark fault campaign",
                                   "reopen factory returned null");
  }
  const Status opened = (*database)->Open();
  if (!opened.ok()) return opened;
  const auto recovered = (*database)->Get(key, config.valid_time);
  if (!recovered.ok()) return recovered.status();
  const bool expected_present =
      config.scenario == BenchmarkFaultScenario::kCommitAfterDecisionDurable ||
      config.scenario == BenchmarkFaultScenario::kManifestAfterRename ||
      config.scenario == BenchmarkFaultScenario::kSstAfterRename ||
      config.scenario == BenchmarkFaultScenario::kSidecarAfterRename ||
      config.scenario == BenchmarkFaultScenario::kBlobGcAfterManifestRename ||
      config.scenario == BenchmarkFaultScenario::kAcceptedWorkShutdown;
  const std::optional<Value> expected = expected_present
      ? std::optional<Value>(value) : before_fault.ValueOrDie();
  if (recovered.ValueOrDie() != expected) {
    return Status::Corruption(
        "benchmark fault campaign",
        "recovered value differs from durable commit outcome");
  }

  sample.completed_ns = ElapsedNs(origin);
  sample.terminal_status = "PASS";
  result.samples.push_back(std::move(sample));
  result.elapsed_ns = ElapsedNs(origin);
  result.logical_work_units = 1;
  result.verified = true;
  result.result_checksum = BlobHashHex(Blake3Hash(
      dataset.dataset_hash + ":" + BenchmarkFaultScenarioName(config.scenario) +
      ":" + (expected_present ? "committed" : "aborted")));
  return result;
}

}  // namespace cedar
