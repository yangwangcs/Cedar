// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/query_runtime.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <filesystem>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string_view>
#include <tuple>
#include <utility>

#include "cedar/blob/blob_store.h"
#include "cedar/columnar/sst.h"
#include "cedar/runtime/work_execution_service.h"
#include "cedar/index/index_sidecar.h"
#include "cedar/tcypher/runtime/vector_filter.h"
#include "cedar/tcypher/runtime/interval_align.h"
#include "cedar/tcypher/runtime/query_spill.h"
#include "cedar/tcypher/session.h"
#include "cedar/tcypher/storage/property_gather.h"
#include "cedar/tcypher/storage/temporal_scan.h"

namespace cedar {
namespace {

constexpr uint64_t kAdvisorySidecarByteLimit = 256U << 10;
constexpr uint64_t kAdvisorySidecarPostingLimit = 4096;
constexpr uint32_t kMaxAdvisoryOrdinalSourceBlocks = 8;
constexpr uint32_t kMaxPathFrontierPartitions = 8;

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return right > std::numeric_limits<uint64_t>::max() - left
      ? std::numeric_limits<uint64_t>::max()
      : left + right;
}

uint32_t ChoosePathFrontierPartitionCount(
    size_t frontier_size, uint32_t batch_capacity) {
  uint32_t partitions = 1;
  const uint64_t capacity = std::max<uint32_t>(batch_capacity, 1);
  while (partitions < kMaxPathFrontierPartitions &&
         frontier_size > static_cast<uint64_t>(partitions) * capacity) {
    partitions *= 2;
  }
  return partitions;
}

uint64_t StablePathFrontierHash(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

Status ResultStreamTerminalAtEnd(const QueryResultStream* input) {
  return input == nullptr
      ? Status::InvalidArgument("physical runtime", "missing input stream")
      : input->terminal_status();
}

struct PathFrontierPartitioning {
  std::vector<std::vector<size_t>> parent_indices;
  size_t max_partition_size = 0;
};

template <typename State, typename Endpoint>
PathFrontierPartitioning PartitionPathFrontier(
    const std::vector<State>& frontier, uint32_t partitions,
    Endpoint endpoint) {
  PathFrontierPartitioning result;
  result.parent_indices.resize(partitions);
  for (size_t parent = 0; parent < frontier.size(); ++parent) {
    const size_t partition = static_cast<size_t>(
        StablePathFrontierHash(endpoint(frontier[parent])) &
        static_cast<uint64_t>(partitions - 1));
    result.parent_indices[partition].push_back(parent);
  }
  for (const std::vector<size_t>& indices : result.parent_indices) {
    result.max_partition_size =
        std::max(result.max_partition_size, indices.size());
  }
  return result;
}

void RecordPathFrontierHop(
    const std::shared_ptr<TcypherExecutionStats>& stats, size_t input_states,
    size_t output_states, size_t completed_paths, uint32_t partitions,
    size_t max_partition_size) {
  if (!stats) return;
  ++stats->path_frontier_hops;
  stats->path_frontier_input_states += input_states;
  stats->path_frontier_output_states += output_states;
  stats->path_frontier_completed_paths += completed_paths;
  if (partitions > 1) ++stats->path_frontier_repartitions;
  stats->path_frontier_partitions += partitions;
  stats->path_frontier_max_partition_size = std::max<uint64_t>(
      stats->path_frontier_max_partition_size, max_partition_size);
}

class PhysicalPlanIdentityResultStream final : public QueryResultStream {
 public:
  PhysicalPlanIdentityResultStream(
      std::unique_ptr<QueryResultStream> input,
      std::shared_ptr<TcypherExecutionStats> stats, uint64_t plan_id)
      : input_(std::move(input)), stats_(std::move(stats)), plan_id_(plan_id) {}

  Status Next(ResultBatch* batch) override {
    const Status status = input_->Next(batch);
    if (stats_) {
      stats_->last_physical_plan_id = plan_id_;
      stats_->executed_physical_plan_id = plan_id_;
    }
    return status;
  }

  Status terminal_status() const override { return input_->terminal_status(); }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::shared_ptr<TcypherExecutionStats> stats_;
  uint64_t plan_id_ = 0;
};

class PhysicalPlanOpenIdentityGuard final {
 public:
  PhysicalPlanOpenIdentityGuard(
      std::shared_ptr<TcypherExecutionStats> stats, uint64_t plan_id)
      : stats_(std::move(stats)), plan_id_(plan_id) {}
  ~PhysicalPlanOpenIdentityGuard() {
    if (stats_ && plan_id_ != 0) stats_->last_physical_plan_id = plan_id_;
  }

 private:
  std::shared_ptr<TcypherExecutionStats> stats_;
  uint64_t plan_id_ = 0;
};

class RuntimeMemoryLease {
 public:
  RuntimeMemoryLease(std::shared_ptr<QueryMemoryAccount> account, uint64_t bytes)
      : account_(std::move(account)), bytes_(bytes) {}
  ~RuntimeMemoryLease() {
    if (account_ && bytes_ != 0) account_->Release(bytes_);
  }

  Status ReserveAdditional(uint64_t bytes) {
    if (bytes > std::numeric_limits<uint64_t>::max() - bytes_) {
      return Status::QueryMemoryLimit(
          "physical runtime", "runtime lease charge overflow");
    }
    if (account_) {
      const Status reserved = account_->Reserve(bytes);
      if (!reserved.ok()) return reserved;
    }
    bytes_ += bytes;
    return Status::OK();
  }

 private:
  std::shared_ptr<QueryMemoryAccount> account_;
  uint64_t bytes_;
};

uint64_t ElapsedNs(std::chrono::steady_clock::time_point start) {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - start).count());
}

struct OperatorRuntimeTimingState {
  std::atomic<uint64_t> blocked_ns{0};
};

class ScopedOperatorRuntimeTimer {
 public:
  ScopedOperatorRuntimeTimer(
      std::shared_ptr<OperatorRuntimeStatsRegistry> registry,
      std::optional<OperatorRuntimeKey> key)
      : registry_(std::move(registry)), key_(key),
        start_(std::chrono::steady_clock::now()) {}

  ~ScopedOperatorRuntimeTimer() { Finish(); }

  void RecordBlocked(uint64_t nanoseconds) {
    blocked_ns_ += nanoseconds;
    if (registry_ && key_.has_value()) {
      registry_->AddBlockedNs(*key_, nanoseconds);
    }
  }

  void Finish() {
    if (finished_) return;
    finished_ = true;
    if (!registry_ || !key_.has_value()) return;
    const uint64_t elapsed = ElapsedNs(start_);
    if (elapsed > blocked_ns_) {
      registry_->AddCpuNs(*key_, elapsed - blocked_ns_);
    }
  }

 private:
  std::shared_ptr<OperatorRuntimeStatsRegistry> registry_;
  std::optional<OperatorRuntimeKey> key_;
  std::chrono::steady_clock::time_point start_;
  uint64_t blocked_ns_ = 0;
  bool finished_ = false;
};

class OperatorRuntimeCountingResultStream final : public QueryResultStream {
 public:
  enum class Phase : uint8_t { kInput, kOutput };

  OperatorRuntimeCountingResultStream(
      std::unique_ptr<QueryResultStream> input,
      std::shared_ptr<OperatorRuntimeStatsRegistry> registry,
      OperatorRuntimeKey key, Phase phase,
      std::shared_ptr<OperatorRuntimeTimingState> timing)
      : input_(std::move(input)), registry_(std::move(registry)), key_(key),
        phase_(phase), timing_(std::move(timing)) {}

  Status Next(ResultBatch* batch) override {
    const uint64_t blocked_before = timing_
        ? timing_->blocked_ns.load(std::memory_order_relaxed) : 0;
    const auto start = std::chrono::steady_clock::now();
    const Status status = input_->Next(batch);
    const uint64_t elapsed = ElapsedNs(start);
    if (registry_) {
      if (phase_ == Phase::kInput) {
        registry_->AddBlockedNs(key_, elapsed);
        if (timing_) {
          timing_->blocked_ns.fetch_add(elapsed, std::memory_order_relaxed);
        }
      } else {
        const uint64_t blocked_after = timing_
            ? timing_->blocked_ns.load(std::memory_order_relaxed)
            : blocked_before;
        const uint64_t upstream_blocked = blocked_after - blocked_before;
        if (elapsed > upstream_blocked) {
          registry_->AddCpuNs(key_, elapsed - upstream_blocked);
        }
      }
      if (phase_ == Phase::kOutput) {
        const QueryOperatorResourceStats resources =
            input_->operator_resource_stats();
        registry_->ObserveMemory(key_, resources.memory_peak_bytes);
        if (resources.spill_bytes > reported_spill_bytes_) {
          registry_->AddSpill(
              key_, resources.spill_bytes - reported_spill_bytes_);
          reported_spill_bytes_ = resources.spill_bytes;
        }
        if (resources.spill_started && !reported_spill_started_) {
          registry_->RecordSpillStart(key_);
          reported_spill_started_ = true;
        }
      }
    }
    if (status.ok() && registry_) {
      const uint64_t rows = batch->batch().row_count();
      if (phase_ == Phase::kInput) {
        registry_->AddInputRows(key_, rows);
      } else {
        registry_->RecordOutputBatch(key_, rows);
      }
    }
    return status;
  }

  Status terminal_status() const override { return input_->terminal_status(); }

 private:
  std::unique_ptr<QueryResultStream> input_;
  std::shared_ptr<OperatorRuntimeStatsRegistry> registry_;
  OperatorRuntimeKey key_;
  Phase phase_;
  std::shared_ptr<OperatorRuntimeTimingState> timing_;
  uint64_t reported_spill_bytes_ = 0;
  bool reported_spill_started_ = false;
};

bool CompareValues(const Value& left, const Value& right, int* comparison) {
  if (comparison == nullptr || left.type() != right.type()) return false;
  switch (left.type()) {
    case PhysicalType::kInt32: {
      const int32_t a = std::get<int32_t>(left.data());
      const int32_t b = std::get<int32_t>(right.data());
      *comparison = a == b ? 0 : a < b ? -1 : 1;
      return true;
    }
    case PhysicalType::kInt64: {
      const int64_t a = std::get<int64_t>(left.data());
      const int64_t b = std::get<int64_t>(right.data());
      *comparison = a == b ? 0 : a < b ? -1 : 1;
      return true;
    }
    case PhysicalType::kTimestamp64: {
      const uint64_t a = std::get<uint64_t>(left.data());
      const uint64_t b = std::get<uint64_t>(right.data());
      *comparison = a == b ? 0 : a < b ? -1 : 1;
      return true;
    }
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      *comparison = std::get<std::string>(left.data()).compare(
          std::get<std::string>(right.data()));
      return true;
    case PhysicalType::kBool:
    case PhysicalType::kFloat32:
    case PhysicalType::kFloat64:
      return false;
  }
  return false;
}

bool MatchesPredicate(const Value& value, const PhysicalPredicate& predicate) {
  if (value.type() != predicate.type) return false;
  if (predicate.kind == PhysicalPredicateKind::kPrefix) {
    if (predicate.values.size() != 1 || value.type() != PhysicalType::kString) return false;
    const std::string& candidate = std::get<std::string>(value.data());
    const std::string& prefix = std::get<std::string>(predicate.values.front().data());
    return candidate.compare(0, prefix.size(), prefix) == 0;
  }
  if (predicate.kind == PhysicalPredicateKind::kEquality ||
      predicate.kind == PhysicalPredicateKind::kIn) {
    return std::find(predicate.values.begin(), predicate.values.end(), value) !=
        predicate.values.end();
  }
  int comparison = 0;
  if (predicate.lower_bound.has_value()) {
    if (!CompareValues(value, *predicate.lower_bound, &comparison) ||
        comparison < 0 || (comparison == 0 && !predicate.lower_inclusive)) {
      return false;
    }
  }
  if (predicate.upper_bound.has_value()) {
    if (!CompareValues(value, *predicate.upper_bound, &comparison) ||
        comparison > 0 || (comparison == 0 && !predicate.upper_inclusive)) {
      return false;
    }
  }
  return true;
}

StatusOr<std::vector<IndexPosting>> ProbeSidecar(
    const IndexSidecar& sidecar, const PhysicalPredicate& predicate) {
  if (predicate.kind == PhysicalPredicateKind::kEquality) {
    if (predicate.values.size() != 1) {
      return Status::Corruption("physical runtime", "equality predicate literal is incomplete");
    }
    return LookupIndexEquality(sidecar, predicate.values.front());
  }
  if (predicate.kind == PhysicalPredicateKind::kIn) {
    std::vector<IndexPosting> combined;
    for (const Value& value : predicate.values) {
      const auto found = LookupIndexEquality(sidecar, value);
      if (!found.ok()) return found.status();
      combined.insert(combined.end(), found.ValueOrDie().begin(), found.ValueOrDie().end());
    }
    std::sort(combined.begin(), combined.end(),
              [](const IndexPosting& left, const IndexPosting& right) {
                return left.source_row_ordinal < right.source_row_ordinal;
              });
    combined.erase(std::unique(combined.begin(), combined.end(),
                               [](const IndexPosting& left, const IndexPosting& right) {
                                 return left.source_row_ordinal == right.source_row_ordinal;
                               }), combined.end());
    return combined;
  }
  if (predicate.kind == PhysicalPredicateKind::kPrefix) {
    if (predicate.values.size() != 1) {
      return Status::Corruption("physical runtime", "prefix predicate literal is incomplete");
    }
    return LookupIndexPrefix(sidecar, predicate.values.front());
  }
  return LookupIndexRange(sidecar, predicate.lower_bound,
                          predicate.lower_inclusive, predicate.upper_bound,
                          predicate.upper_inclusive);
}

uint32_t RequiredIndexCapability(PhysicalPredicateKind kind) {
  if (kind == PhysicalPredicateKind::kPrefix) return kIndexPrefix;
  if (kind == PhysicalPredicateKind::kRange) return kIndexOrderedRange;
  return kIndexEquality;
}

bool IsPredicatePropertyEvent(const TemporalEvent& event,
                              const PhysicalPredicate& predicate) {
  const LogicalKey& key = event.logical_key();
  return key.entity_type() == predicate.column.entity_type &&
      key.kind() == LogicalKeyKind::kProperty &&
      key.column_id() == predicate.column.column_id &&
      event.schema_epoch() == predicate.column.schema_epoch;
}

Status ReserveIndexMemory(
    const std::shared_ptr<QueryMemoryAccount>& account, uint64_t bytes,
    std::vector<std::shared_ptr<RuntimeMemoryLease>>* leases) {
  if (bytes == 0) return Status::OK();
  if (account) {
    const Status reserved = account->Reserve(bytes);
    if (!reserved.ok()) return reserved;
  }
  leases->push_back(std::make_shared<RuntimeMemoryLease>(account, bytes));
  return Status::OK();
}

Status MaterializeLazySstIndexSource(
    TcypherIndexSource* source, TcypherExecutionContext* context,
    std::vector<std::shared_ptr<RuntimeMemoryLease>>* leases) {
  if (source == nullptr || context == nullptr || leases == nullptr) {
    return Status::InvalidArgument("physical runtime", "missing lazy index state");
  }
  if (!source->pinned_sst_source.has_value() ||
      source->sidecar.source_sst_id == source->source_sst_id) {
    return Status::OK();
  }
  if (context->options.cancellation &&
      context->options.cancellation->IsCancelled()) {
    return Status::QueryCancelled("physical runtime",
                                  "query cancelled while opening index source");
  }
  if (!source->definition.has_value() || !source->fragment.has_value()) {
    return Status::Corruption("physical runtime", "lazy SST index source is incomplete");
  }
  std::error_code sidecar_error;
  const uint64_t sidecar_bytes = std::filesystem::file_size(
      source->sidecar_path, sidecar_error);
  if (sidecar_error) {
    if (source->fragment.has_value()) source->fragment->usable = false;
    if (context->transaction_coordinator != nullptr &&
        context->index_catalog_snapshot != nullptr) {
      context->transaction_coordinator->ReportIndexHealthEvent(
          source->index_id, source->source_sst_id,
          context->index_catalog_snapshot->catalog_generation,
          IndexHealthFailureClass::kMissingSidecar).IgnoreError();
    }
    return Status::IOError(source->sidecar_path, sidecar_error.message());
  }
  const uint64_t posting_count = source->fragment->indexed_put_count;
  if (sidecar_bytes > kAdvisorySidecarByteLimit ||
      posting_count > kAdvisorySidecarPostingLimit) {
    return Status::ResourceExhausted(
        "physical runtime", "advisory sidecar exceeds one callback quantum");
  }
  if (sidecar_bytes > std::numeric_limits<uint64_t>::max() / 2) {
    return Status::QueryMemoryLimit("physical runtime", "index source charge overflow");
  }
  uint64_t charge = 2 * sidecar_bytes;
  if (posting_count > (std::numeric_limits<uint64_t>::max() - charge) /
                          (2 * sizeof(IndexPosting))) {
    return Status::QueryMemoryLimit("physical runtime", "index source charge overflow");
  }
  charge += posting_count * 2 * sizeof(IndexPosting);
  const Status reserved = ReserveIndexMemory(
      context->options.memory_account, charge, leases);
  if (!reserved.ok()) return reserved;
  auto sidecar = ReadVerifiedIndexSidecarFile(
      source->sidecar_path, *source->definition, source->source_sst_id,
      source->fragment->identity_checksum);
  if (!sidecar.ok()) {
    source->fragment->usable = false;
    if (context->transaction_coordinator != nullptr &&
        context->index_catalog_snapshot != nullptr) {
      context->transaction_coordinator->ReportIndexHealthEvent(
          source->index_id, source->source_sst_id,
          context->index_catalog_snapshot->catalog_generation,
          IndexHealthFailureClass::kCorruptSidecar).IgnoreError();
    }
    return sidecar.status();
  }
  if (sidecar.ValueOrDie().postings.size() != posting_count) {
    source->fragment->usable = false;
    if (context->transaction_coordinator != nullptr &&
        context->index_catalog_snapshot != nullptr) {
      context->transaction_coordinator->ReportIndexHealthEvent(
          source->index_id, source->source_sst_id,
          context->index_catalog_snapshot->catalog_generation,
          IndexHealthFailureClass::kPostingCountMismatch).IgnoreError();
    }
    return Status::Corruption(
        "physical runtime", "advisory sidecar posting count differs from fragment");
  }
  if (context->options.execution_stats) {
    context->options.execution_stats->index_advisory_sidecar_bytes_read +=
        sidecar_bytes;
  }
  source->sidecar = std::move(sidecar).ConsumeValueOrDie();
  return Status::OK();
}

const TemporalEvent* IndexedEventAt(const TcypherIndexSource& source,
                                    uint64_t ordinal) {
  if (!source.pinned_sst_source.has_value()) {
    return ordinal < source.events.size() ? &source.events[ordinal] : nullptr;
  }
  const auto loaded = source.loaded_events_by_ordinal.find(ordinal);
  return loaded == source.loaded_events_by_ordinal.end()
      ? nullptr : &loaded->second;
}

Status LoadPostingEvents(
    TcypherIndexSource* source, const std::vector<uint64_t>& ordinals,
    const TcypherExecutionContext& context) {
  if (source == nullptr || !source->pinned_sst_source.has_value()) {
    return Status::OK();
  }
  std::vector<uint64_t> missing;
  missing.reserve(ordinals.size());
  for (uint64_t ordinal : ordinals) {
    if (source->loaded_events_by_ordinal.count(ordinal) == 0) {
      missing.push_back(ordinal);
    }
  }
  std::sort(missing.begin(), missing.end());
  missing.erase(std::unique(missing.begin(), missing.end()), missing.end());
  if (missing.empty() && source->validated_source_row_count.has_value()) {
    return Status::OK();
  }
  if (context.options.execution_stats) {
    ++context.options.execution_stats->index_advisory_sst_block_count_reads;
  }
  const auto block_count = ReadSstBlockCount(
      source->pinned_sst_source->path);
  if (!block_count.ok()) return block_count.status();
  if (block_count.ValueOrDie() > kMaxAdvisoryOrdinalSourceBlocks) {
    return Status::ResourceExhausted(
        "physical runtime",
        "advisory SST ordinal metadata exceeds one callback quantum");
  }
  SstReadStats ignored_stats;
  if (context.options.execution_stats) {
    ++context.options.execution_stats->index_advisory_ordinal_read_calls;
  }
  auto selected = ReadSstEventsAtOrdinals(
      source->pinned_sst_source->path,
      source->pinned_sst_source->metadata.partition, missing,
      context.options.cancellation, context.options.memory_account,
      &ignored_stats, context.io_governor);
  if (!selected.ok()) return selected.status();
  if (!source->fragment.has_value() ||
      selected.ValueOrDie().total_row_count !=
          source->fragment->source_row_count) {
    return Status::Corruption("physical runtime", "index source row count changed");
  }
  source->validated_source_row_count = selected.ValueOrDie().total_row_count;
  for (auto& event : selected.ValueOrDie().events) {
    source->loaded_events_by_ordinal.emplace(
        event.first, std::move(event.second));
  }
  if (selected.ValueOrDie().memory_retention) {
    source->ordinal_read_retentions.push_back(
        std::move(selected.ValueOrDie().memory_retention));
  }
  return Status::OK();
}

using CanonicalValueView = MemtableDeltaIndex::CanonicalValueView;
using CanonicalValueLess = MemtableDeltaIndex::CanonicalValueLess;

struct CanonicalPhysicalPredicate {
  CanonicalPhysicalPredicate(
      PhysicalPredicateKind predicate_kind, bool predicate_lower_inclusive,
      bool predicate_upper_inclusive, uint64_t* comparison_counter,
      const bool* count_comparisons)
      : kind(predicate_kind),
        values(CanonicalValueLess{comparison_counter, count_comparisons}),
        lower_inclusive(predicate_lower_inclusive),
        upper_inclusive(predicate_upper_inclusive) {}

  PhysicalPredicateKind kind = PhysicalPredicateKind::kEquality;
  MemtableDeltaIndex::LookupValueSet values;
  std::optional<IndexCanonicalValue> lower;
  std::optional<IndexCanonicalValue> upper;
  bool lower_inclusive = true;
  bool upper_inclusive = true;
};

uint64_t CanonicalPayloadBytes(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
      return 1;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
      return 4;
    case PhysicalType::kInt64:
    case PhysicalType::kTimestamp64:
    case PhysicalType::kFloat64:
      return 8;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return std::get<std::string>(value.data()).size();
  }
  return 0;
}

StatusOr<CanonicalValueView> CanonicalViewForValue(
    const Value& value, IndexCanonicalValue* fixed_storage) {
  if (value.type() == PhysicalType::kString ||
      value.type() == PhysicalType::kBinary) {
    return CanonicalValueView{
        value.type(), std::get<std::string>(value.data())};
  }
  if (fixed_storage == nullptr) {
    return Status::InvalidArgument(
        "physical runtime", "missing fixed canonical lookup storage");
  }
  auto encoded = EncodeIndexCanonicalValue(value);
  if (!encoded.ok()) return encoded.status();
  *fixed_storage = std::move(encoded).ConsumeValueOrDie();
  return CanonicalValueView{fixed_storage->type, fixed_storage->bytes};
}

StatusOr<bool> ValueMatchesCanonicalPredicate(
    const Value& value, const CanonicalPhysicalPredicate& predicate,
    bool* count_comparisons) {
  if (predicate.kind == PhysicalPredicateKind::kEquality ||
      predicate.kind == PhysicalPredicateKind::kIn) {
    IndexCanonicalValue fixed_storage;
    auto view = CanonicalViewForValue(value, &fixed_storage);
    if (!view.ok()) return view.status();
    if (count_comparisons != nullptr) *count_comparisons = true;
    const bool found = predicate.values.find(view.ValueOrDie()) !=
        predicate.values.end();
    if (count_comparisons != nullptr) *count_comparisons = false;
    return found;
  }
  if (predicate.kind == PhysicalPredicateKind::kPrefix) {
    return predicate.values.size() == 1 &&
        value.type() == PhysicalType::kString &&
        std::get<std::string>(value.data()).compare(
            0, predicate.values.begin()->bytes.size(),
            predicate.values.begin()->bytes) == 0;
  }
  int comparison = 0;
  if (predicate.lower.has_value()) {
    IndexCanonicalValue fixed_storage;
    auto view = CanonicalViewForValue(value, &fixed_storage);
    if (!view.ok()) return view.status();
    const CanonicalValueLess less;
    comparison = less(view.ValueOrDie(), *predicate.lower)
        ? -1 : less(*predicate.lower, view.ValueOrDie()) ? 1 : 0;
    if (comparison < 0 ||
        (comparison == 0 && !predicate.lower_inclusive)) {
      return false;
    }
  }
  if (predicate.upper.has_value()) {
    IndexCanonicalValue fixed_storage;
    auto view = CanonicalViewForValue(value, &fixed_storage);
    if (!view.ok()) return view.status();
    const CanonicalValueLess less;
    comparison = less(view.ValueOrDie(), *predicate.upper)
        ? -1 : less(*predicate.upper, view.ValueOrDie()) ? 1 : 0;
    if (comparison > 0 ||
        (comparison == 0 && !predicate.upper_inclusive)) {
      return false;
    }
  }
  return true;
}

StatusOr<bool> EventMatchesCanonicalPredicate(
    const TemporalEvent& event,
    const CanonicalPhysicalPredicate& predicate,
    bool* count_comparisons) {
  if (!event.is_blob_reference()) {
    return ValueMatchesCanonicalPredicate(
        event.value(), predicate, count_comparisons);
  }
  if (predicate.kind != PhysicalPredicateKind::kEquality &&
      predicate.kind != PhysicalPredicateKind::kIn) {
    return false;
  }
  const IndexCanonicalValue hash = EncodeIndexBlobHash(*event.blob_ref());
  if (count_comparisons != nullptr) *count_comparisons = true;
  const bool found = predicate.values.find(hash) != predicate.values.end();
  if (count_comparisons != nullptr) *count_comparisons = false;
  return found;
}

bool PostingMatchesPredicate(const IndexPosting& posting,
                             const CanonicalPhysicalPredicate& predicate,
                             bool* count_comparisons) {
  if (predicate.kind == PhysicalPredicateKind::kEquality ||
      predicate.kind == PhysicalPredicateKind::kIn) {
    if (count_comparisons != nullptr) *count_comparisons = true;
    const bool found = predicate.values.find(posting.value) !=
        predicate.values.end();
    if (count_comparisons != nullptr) *count_comparisons = false;
    return found;
  }
  if (predicate.kind == PhysicalPredicateKind::kPrefix) {
    return predicate.values.size() == 1 &&
        posting.value.type == predicate.values.begin()->type &&
        posting.value.bytes.compare(0, predicate.values.begin()->bytes.size(),
                                    predicate.values.begin()->bytes) == 0;
  }
  if (predicate.lower.has_value()) {
    const int comparison = CompareIndexCanonicalValues(
        posting.value, *predicate.lower);
    if (posting.value.type != predicate.lower->type || comparison < 0 ||
        (comparison == 0 && !predicate.lower_inclusive)) {
      return false;
    }
  }
  if (predicate.upper.has_value()) {
    const int comparison = CompareIndexCanonicalValues(
        posting.value, *predicate.upper);
    if (posting.value.type != predicate.upper->type || comparison > 0 ||
        (comparison == 0 && !predicate.upper_inclusive)) {
      return false;
    }
  }
  return true;
}

struct CandidateMatchProgress {
  size_t matched_predicate_count = 0;
  size_t last_predicate = std::numeric_limits<size_t>::max();
};

bool SamePartition(const BlockPartition& left, const BlockPartition& right) {
  return left.entity_type == right.entity_type &&
      left.column_id == right.column_id &&
      left.schema_epoch == right.schema_epoch &&
      left.physical_type == right.physical_type &&
      left.edge_type == right.edge_type &&
      left.compression_id == right.compression_id &&
      left.key_kind == right.key_kind;
}

StatusOr<PinnedTemporalScanSources> BuildPinnedSources(
    const TcypherExecutionContext& context, const QuerySnapshot& snapshot) {
  if (context.runtime_sources_from_snapshot) {
    if (!context.version_snapshot ||
        context.version_snapshot.get() != snapshot.pinned_version_set.get() ||
        context.version_snapshot->generation !=
            snapshot.pinned_version_set->generation ||
        context.schema_snapshot.get() != snapshot.pinned_schema_registry.get() ||
        context.blob_reader_epoch != snapshot.blob_reader_epoch ||
        context.blob_reader_epoch != context.version_snapshot->generation ||
        !(context.statement_start_hlc == snapshot.statement_start_hlc) ||
        context.sst_event_sources.size() != snapshot.pinned_version_set->files.size()) {
      return Status::Corruption("physical runtime",
                                "runtime envelope differs from query snapshot");
    }
    std::set<uint64_t> matched_file_numbers;
    for (const PinnedSstSource& source : context.sst_event_sources) {
      const auto pinned = std::find_if(
          snapshot.pinned_version_set->files.begin(),
          snapshot.pinned_version_set->files.end(),
          [&source](const SstFileMeta& file) {
            return file.file_number == source.metadata.file_number;
          });
      if (pinned == snapshot.pinned_version_set->files.end() ||
          !matched_file_numbers.insert(pinned->file_number).second ||
          pinned->relative_path != source.metadata.relative_path ||
          pinned->file_size != source.metadata.file_size ||
          pinned->blob_refs != source.metadata.blob_refs ||
          !SamePartition(pinned->partition, source.metadata.partition)) {
        return Status::Corruption("physical runtime",
                                  "runtime SST source is not snapshot-pinned");
      }
    }
  }
  PinnedTemporalScanSources sources;
  sources.memtables = context.memtable_event_sources;
  sources.ssts = context.sst_event_sources;
  sources.io_governor = context.io_governor;
  sources.prefetch_sst_blocks =
      context.options.workload_class == TcypherWorkloadClass::kAnalytical;
  if (context.session != nullptr && context.session->active()) {
    sources.base_snapshot_seq = context.session->snapshot_seq();
  }
  if (!context.committed_events.empty()) {
    auto fixture = std::make_shared<TemporalMemTable>();
    for (const TemporalEvent& event : context.committed_events) {
      const Status inserted = fixture->Insert(event);
      if (!inserted.ok()) return inserted;
    }
    sources.memtables.push_back(std::move(fixture));
  }
  if (!context.session_overlay_events.empty()) {
    sources.session_overlay = std::make_shared<const std::vector<TemporalEvent>>(
        context.session_overlay_events);
    uint64_t overlay_ceiling = context.visible_seq_ceiling;
    for (const TemporalEvent& event : context.session_overlay_events) {
      overlay_ceiling = std::max(overlay_ceiling, event.commit_seq());
    }
    sources.overlay_snapshot_seq = overlay_ceiling;
  }
  return sources;
}

void AttachStorageStats(const std::shared_ptr<TcypherExecutionStats>& stats,
                        TemporalScanSpec* spec) {
  if (!stats || spec == nullptr) return;
  spec->storage_stats_observer = [stats](
      uint64_t sst_bytes, uint64_t decoded_bytes, uint64_t skipped_bytes,
      uint64_t decode_count, uint64_t decode_latency_ns) {
    stats->sst_physical_bytes_read += sst_bytes;
    stats->page_bytes_decoded += decoded_bytes;
    stats->page_bytes_skipped += skipped_bytes;
    stats->page_decode_count += decode_count;
    stats->page_decode_latency_ns += decode_latency_ns;
  };
}

void AttachScanStats(const std::shared_ptr<TcypherExecutionStats>& stats,
                     bool property_scan, TemporalScanSpec* spec,
                     std::optional<OperatorRuntimeKey> operator_key = std::nullopt) {
  if (!stats || spec == nullptr) return;
  spec->open_observer = [stats, property_scan] {
    if (property_scan) ++stats->pinned_property_point_scans;
    else ++stats->pinned_root_scan_opens;
  };
  spec->stats_observer = [stats, property_scan, operator_key](
      uint64_t event_delta, uint64_t block_delta, uint64_t max_buffered) {
    stats->base_events_visited += event_delta;
    stats->sst_blocks_read += block_delta;
    if (!property_scan) stats->root_sst_blocks_read += block_delta;
    stats->max_sst_cursor_buffered_events = std::max(
        stats->max_sst_cursor_buffered_events, max_buffered);
  };
  AttachStorageStats(stats, spec);
  if (operator_key.has_value() && stats->operator_runtime) {
    spec->page_read_observer =
        [registry = stats->operator_runtime, key = *operator_key](uint64_t pages) {
          registry->AddPagesRead(key, pages);
        };
  }
}

struct RuntimeTimelineSnapshot {
  ~RuntimeTimelineSnapshot() {
    std::vector<uint64_t>().swap(physical_times);
    if (account) account->Release(bytes);
  }
  std::shared_ptr<QueryMemoryAccount> account;
  uint64_t bytes = 0;
  std::vector<uint64_t> physical_times;
};

class QueryRuntimeState : public std::enable_shared_from_this<QueryRuntimeState> {
 public:
  enum class IndexPreparationPhase : uint8_t {
    kSelectDefinitions,
    kScanSstSources,
    kScanDeltaSources,
    kValidateSstCoverage,
    kValidateDeltaCoverage,
    kMaterializeSst,
    kMaterializeDelta,
    kDone,
  };
  enum class CandidatePhase : uint8_t {
    kCanonicalize,
    kSizeSst,
    kSizeDelta,
    kAdmit,
    kSst,
    kDelta,
    kCommitted,
    kOverlay,
    kFinalize,
    kDone,
  };
  static constexpr uint32_t kCandidateItemQuantum = 64;
  static constexpr uint64_t kIndexMetadataStoredItemCharge = 512;
  using IndexColumnKey = std::tuple<uint8_t, uint16_t, uint32_t>;

  QueryRuntimeState(std::shared_ptr<const PhysicalPlan> physical_plan,
                    QuerySnapshot query_snapshot, TcypherExecutionContext execution_context,
                    PinnedTemporalScanSources pinned_sources,
                    WorkExecutionService* query_execution_service)
      : plan(std::move(physical_plan)), snapshot(std::move(query_snapshot)),
        context(std::move(execution_context)), sources(std::move(pinned_sources)),
        execution_service(query_execution_service) {}

  Status Open() {
    if (!plan || !execution_service ||
        snapshot.resolved_temporal_contexts.empty()) {
      return Status::InvalidArgument("physical runtime", "incomplete query state");
    }
    valid_time = context.options.statement_start_valid_time;
    snapshot_seq = context.visible_seq_ceiling;
    for (const ResolvedTemporalContext& temporal : snapshot.resolved_temporal_contexts) {
      snapshot_seq = temporal.snapshot_seq;
      if (temporal.axis == TemporalAxis::kValidTime) valid_time = temporal.valid_time;
    }
    if (plan->temporal_mode() == PhysicalTemporalMode::kPoint &&
        plan->valid_time_as_of().has_value()) {
      valid_time = *plan->valid_time_as_of();
    }
    if ((snapshot_seq == 0 && context.session_overlay_events.empty()) ||
        (snapshot_seq > snapshot.visible_seq_ceiling &&
         context.session_overlay_events.empty())) {
      return Status::InvalidArgument("physical runtime", "invalid resolved snapshot cutoff");
    }
    std::optional<uint64_t> exact = plan->exact_entity_id();
    if (!exact.has_value() && !plan->exact_entity_parameter().empty()) {
      const auto parameter = context.options.timestamp_parameters.find(
          plan->exact_entity_parameter());
      if (parameter == context.options.timestamp_parameters.end()) {
        return Status::BindError("physical runtime", "exact entity parameter is missing");
      }
      exact = parameter->second;
    }
    if (context.session != nullptr && context.session->active() &&
        context.session->mode() == TcypherSessionMode::kStrict) {
      if (!exact.has_value()) {
        return Status::BindError("physical runtime", "strict query requires an exact entity id");
      }
      const Status existence_read = context.session->RecordRead(
          TransactionCoordinator::StrictReadPoint{
              LogicalKey::VertexExistence(*exact), valid_time});
      if (!existence_read.ok()) return existence_read;
      for (const SlotDescriptor& ignored : plan->slots()) (void)ignored;
      for (const PhysicalPropertySlot& property : plan->predicate_properties()) {
        const Status predicate_read = context.session->RecordRead(
            TransactionCoordinator::StrictReadPoint{
                LogicalKey::VertexProperty(*exact, property.column.column_id), valid_time});
        if (!predicate_read.ok()) return predicate_read;
      }
      for (const PhysicalPropertySlot& property : plan->projection_properties()) {
        const Status projection_read = context.session->RecordRead(
            TransactionCoordinator::StrictReadPoint{
                LogicalKey::VertexProperty(*exact, property.column.column_id), valid_time});
        if (!projection_read.ok()) return projection_read;
      }
    }
    TemporalScanSpec spec{valid_time, snapshot_seq, context.options.batch_capacity};
    const bool relationship_change_scan = plan->expand().has_value() &&
        (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeChanges ||
         plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges);
    spec.entity_type = relationship_change_scan
        ? plan->expand()->direction : EntityType::Vertex;
    spec.key_kind = LogicalKeyKind::kExistence;
    if (relationship_change_scan) {
      spec.edge_type = plan->expand()->edge_type;
    }
    if (exact.has_value()) spec.exact_key = LogicalKey::VertexExistence(*exact);
    if (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeChanges) {
      const auto range = plan->valid_time_range();
      if (!range.has_value()) {
        return Status::InvalidArgument(
            "physical runtime", "change plan is missing valid-time range");
      }
      spec.raw_events = true;
      spec.valid_time_start = range->first;
      spec.valid_time_end = range->second;
    } else if (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeRange) {
      if (!plan->valid_time_range().has_value()) {
        return Status::InvalidArgument(
            "physical runtime", "range plan is missing valid-time range");
      }
      spec.raw_events = true;
    }
    spec.cancellation = context.options.cancellation;
    spec.memory_account = context.options.memory_account;
    AttachScanStats(
        context.options.execution_stats, false, &spec,
        OperatorRuntimeKey{plan->plan_id(), plan->operators().front().id.value});
    if (plan->include_system_time() ||
        plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges) {
      const auto& entries = context.commit_timeline.entries();
      const size_t count = static_cast<size_t>(std::min<uint64_t>(snapshot_seq, entries.size()));
      const uint64_t bytes = sizeof(RuntimeTimelineSnapshot) +
          static_cast<uint64_t>(count) * sizeof(uint64_t);
      if (context.options.memory_account) {
        const Status reserved = context.options.memory_account->Reserve(bytes);
        if (!reserved.ok()) return reserved;
      }
      timeline = std::make_shared<RuntimeTimelineSnapshot>();
      timeline->account = context.options.memory_account;
      timeline->bytes = bytes;
      timeline->physical_times.reserve(count);
      for (size_t index = 0; index < count; ++index) {
        if (entries[index].commit_seq != index + 1) {
          return Status::Corruption("physical runtime", "CommitTimeline is not contiguous");
        }
        timeline->physical_times.push_back(entries[index].system_time_hlc.physical_us);
      }
    }
    if (plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges) {
      const auto range = plan->system_time_range();
      if (!range.has_value() || !timeline) {
        return Status::InvalidArgument(
            "physical runtime", "system-time change plan is missing timeline bounds");
      }
      spec.raw_events = true;
      spec.raw_order = RawTemporalOrder::kCommitSequence;
      if (const auto valid_range = plan->valid_time_range(); valid_range.has_value()) {
        spec.valid_time_start = valid_range->first;
        spec.valid_time_end = valid_range->second;
      }
      const uint64_t start = range->first;
      const uint64_t end = range->second;
      const std::optional<uint64_t> valid_as_of = plan->valid_time_as_of();
      spec.event_filter = [timeline = timeline, start, end, valid_as_of,
                           current_key = std::optional<LogicalKey>(),
                           selected_as_of = false](const TemporalEvent& event) mutable
          -> StatusOr<bool> {
        if (valid_as_of.has_value()) {
          if (!current_key.has_value() || *current_key != event.logical_key()) {
            current_key = event.logical_key();
            selected_as_of = false;
          }
          if (selected_as_of || event.valid_from() > *valid_as_of) return false;
          selected_as_of = true;
        }
        if (event.commit_seq() == 0 ||
            event.commit_seq() > timeline->physical_times.size()) {
          return Status::Corruption(
              "physical runtime", "commit is absent from CommitTimeline");
        }
        const uint64_t physical = timeline->physical_times[
            static_cast<size_t>(event.commit_seq() - 1)];
        if (physical < start || physical >= end) return false;
        return true;
      };
    }
    scan_spec = std::move(spec);
    if (context.options.execution_stats) {
      ++context.options.execution_stats->pipeline_builds;
      context.options.execution_stats->last_physical_plan_id = plan->plan_id();
    }
    return Status::OK();
  }

  std::optional<OperatorRuntimeKey> OperatorKeyFor(
      PhysicalOperatorKind kind, bool last = false) const {
    const auto stats = context.options.execution_stats;
    if (!stats || !stats->operator_runtime) return std::nullopt;
    const PhysicalOperatorSpec* found = nullptr;
    for (const PhysicalOperatorSpec& op : plan->operators()) {
      if (op.kind != kind) continue;
      found = &op;
      if (!last) break;
    }
    if (found == nullptr) return std::nullopt;
    return OperatorRuntimeKey{plan->plan_id(), found->id.value};
  }

  std::optional<OperatorRuntimeKey> OperatorKeyForOccurrence(
      PhysicalOperatorKind kind, size_t occurrence) const {
    const auto stats = context.options.execution_stats;
    if (!stats || !stats->operator_runtime) return std::nullopt;
    size_t current = 0;
    for (const PhysicalOperatorSpec& op : plan->operators()) {
      if (op.kind != kind) continue;
      if (current++ == occurrence) {
        return OperatorRuntimeKey{plan->plan_id(), op.id.value};
      }
    }
    return std::nullopt;
  }

  void RecordOperatorBatch(PhysicalOperatorKind kind, uint64_t input_rows,
                           uint64_t output_rows, bool last = false,
                           uint64_t input_intervals = 0,
                           uint64_t output_intervals = 0) const {
    const auto key = OperatorKeyFor(kind, last);
    if (!key.has_value()) return;
    const auto stats = context.options.execution_stats;
    stats->operator_runtime->RecordBatch(
        *key, input_rows, output_rows, input_intervals, output_intervals);
  }

  void AddOperatorInput(PhysicalOperatorKind kind, uint64_t rows,
                        bool last = false, uint64_t intervals = 0) const {
    const auto key = OperatorKeyFor(kind, last);
    if (!key.has_value()) return;
    context.options.execution_stats->operator_runtime->AddInputRows(
        *key, rows, intervals);
  }

  void RecordOperatorOutput(PhysicalOperatorKind kind, uint64_t rows,
                            bool last = false, uint64_t intervals = 0) const {
    const auto key = OperatorKeyFor(kind, last);
    if (!key.has_value()) return;
    context.options.execution_stats->operator_runtime->RecordOutputBatch(
        *key, rows, intervals);
  }

  void AddOperatorInputAt(PhysicalOperatorKind kind, size_t occurrence,
                          uint64_t rows) const {
    const auto key = OperatorKeyForOccurrence(kind, occurrence);
    if (key.has_value()) {
      context.options.execution_stats->operator_runtime->AddInputRows(*key, rows);
    }
  }

  void RecordOperatorOutputAt(PhysicalOperatorKind kind, size_t occurrence,
                              uint64_t rows) const {
    const auto key = OperatorKeyForOccurrence(kind, occurrence);
    if (key.has_value()) {
      context.options.execution_stats->operator_runtime->RecordOutputBatch(
          *key, rows);
    }
  }

  void AddOperatorCpu(PhysicalOperatorKind kind, uint64_t nanoseconds,
                      bool last = false) const {
    const auto key = OperatorKeyFor(kind, last);
    if (key.has_value()) {
      context.options.execution_stats->operator_runtime->AddCpuNs(
          *key, nanoseconds);
    }
  }

  void AddOperatorBlocked(PhysicalOperatorKind kind, uint64_t nanoseconds,
                          bool last = false) const {
    const auto key = OperatorKeyFor(kind, last);
    if (key.has_value()) {
      context.options.execution_stats->operator_runtime->AddBlockedNs(
          *key, nanoseconds);
    }
  }

  void AddOperatorCpuAt(PhysicalOperatorKind kind, size_t occurrence,
                        uint64_t nanoseconds) const {
    const auto key = OperatorKeyForOccurrence(kind, occurrence);
    if (key.has_value()) {
      context.options.execution_stats->operator_runtime->AddCpuNs(
          *key, nanoseconds);
    }
  }

  void AddOperatorBlockedAt(PhysicalOperatorKind kind, size_t occurrence,
                            uint64_t nanoseconds) const {
    const auto key = OperatorKeyForOccurrence(kind, occurrence);
    if (key.has_value()) {
      context.options.execution_stats->operator_runtime->AddBlockedNs(
          *key, nanoseconds);
    }
  }

  void AttachBlobMaterialization(TemporalScanSpec* spec,
                                 bool projection_phase) const {
    if (spec == nullptr || context.transaction_coordinator == nullptr) return;
    TransactionCoordinator* coordinator = context.transaction_coordinator;
    spec->blob_materializer = [coordinator](const TemporalEvent& event) {
      return coordinator->MaterializeBlobValue(event);
    };
    const auto key = OperatorKeyFor(
        PhysicalOperatorKind::kPropertyGather, projection_phase);
    const auto stats = context.options.execution_stats;
    if (stats) {
      spec->blob_ref_observer = [stats] { ++stats->blob_refs_seen; };
      spec->blob_read_observer = [stats, key] {
        ++stats->blob_payload_reads;
        if (key.has_value() && stats->operator_runtime) {
          stats->operator_runtime->AddBlobPayloadReads(*key, 1);
        }
      };
    }
  }

  bool UsesBlobPredicateProbesFor(
      const PhysicalPropertySlot& property) const {
    bool has_predicate = false;
    for (const PhysicalPredicate& predicate : plan->predicates()) {
      if (predicate.slot != property.slot) continue;
      has_predicate = true;
      if ((predicate.kind != PhysicalPredicateKind::kEquality &&
           predicate.kind != PhysicalPredicateKind::kIn) ||
          (predicate.type != PhysicalType::kString &&
           predicate.type != PhysicalType::kBinary)) {
        return false;
      }
      for (const Value& literal : predicate.values) {
        if (literal.type() != predicate.type) return false;
      }
    }
    return has_predicate;
  }

  StatusOr<std::shared_ptr<const std::vector<BlobPredicateProbe>>>
  BlobPredicateProbesFor(const PhysicalPropertySlot& property) const {
    if (!UsesBlobPredicateProbesFor(property)) {
      return std::shared_ptr<const std::vector<BlobPredicateProbe>>{};
    }
    uint64_t probe_count = 0;
    uint64_t charge = sizeof(std::vector<BlobPredicateProbe>);
    for (const PhysicalPredicate& predicate : plan->predicates()) {
      if (predicate.slot != property.slot) continue;
      for (const Value& literal : predicate.values) {
        if (probe_count == std::numeric_limits<uint64_t>::max()) {
          return Status::QueryMemoryLimit(
              "physical runtime", "Blob predicate probe count overflow");
        }
        ++probe_count;
        if (charge > std::numeric_limits<uint64_t>::max() -
                         sizeof(BlobPredicateProbe)) {
          return Status::QueryMemoryLimit(
              "physical runtime", "Blob predicate probe charge overflow");
        }
        charge += sizeof(BlobPredicateProbe);
        const uint64_t payload = static_cast<uint64_t>(
            std::get<std::string>(literal.data()).size());
        if (charge > std::numeric_limits<uint64_t>::max() - payload) {
          return Status::QueryMemoryLimit(
              "physical runtime", "Blob predicate literal charge overflow");
        }
        charge += payload;
      }
    }
    if (probe_count == 0) {
      return std::shared_ptr<const std::vector<BlobPredicateProbe>>{};
    }
    if (context.options.memory_account) {
      const Status reserved = context.options.memory_account->Reserve(charge);
      if (!reserved.ok()) return reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(
        context.options.memory_account, charge);
    if (context.options.execution_stats && context.options.memory_account) {
      context.options.execution_stats->blob_predicate_probe_bytes_reserved +=
          charge;
    }
    std::vector<BlobPredicateProbe> probes;
    probes.reserve(static_cast<size_t>(probe_count));
    for (const PhysicalPredicate& predicate : plan->predicates()) {
      if (predicate.slot != property.slot) continue;
      for (const Value& literal : predicate.values) {
        const std::string& payload = std::get<std::string>(literal.data());
        probes.push_back(BlobPredicateProbe{
            Blake3Hash(payload), static_cast<uint64_t>(payload.size()),
            literal, lease});
      }
    }
    return std::make_shared<const std::vector<BlobPredicateProbe>>(
        std::move(probes));
  }

  StatusOr<
      std::vector<std::shared_ptr<const std::vector<BlobPredicateProbe>>>>
  BuildBlobPredicateProbes(
      const std::vector<PhysicalPropertySlot>& properties) const {
    std::vector<std::shared_ptr<const std::vector<BlobPredicateProbe>>> probes;
    probes.reserve(properties.size());
    for (const PhysicalPropertySlot& property : properties) {
      auto built = BlobPredicateProbesFor(property);
      if (!built.ok()) return built.status();
      probes.push_back(std::move(built).ConsumeValueOrDie());
    }
    return probes;
  }

  void InvalidateHashResolvedProjectionSlots(
      std::map<SlotId, uint32_t>* layout) const {
    if (layout == nullptr) return;
    for (const PhysicalPropertySlot& property :
         plan->projection_properties()) {
      if (UsesBlobPredicateProbesFor(property)) layout->erase(property.slot);
    }
  }

  Status ScheduleAndWait() {
    if (abandoned) return Status::QueryCancelled("physical runtime", "result stream abandoned");
    if (queue.size() >= queue_capacity || eof || !terminal.ok()) {
      return Status::OK();
    }
    if (context.options.cancellation && context.options.cancellation->IsCancelled()) {
      return SetTerminal(Status::QueryCancelled(
          "physical runtime", "query cancelled before dispatch"));
    }
    const auto work_class =
        context.options.workload_class == TcypherWorkloadClass::kAnalytical
            ? WorkClass::kAnalyticalQuery : WorkClass::kInteractiveQuery;
    const auto submitted = execution_service->Submit(
        WorkTaskRequest{work_class, ResourceProfile{0, 0, 0, 0, 1}},
        [self = shared_from_this()] {
          const Status status = self->RunMorsel();
          if (self->context.options.execution_stats) {
            ++self->context.options.execution_stats->scheduler_dispatches;
            ++self->context.options.execution_stats->morsels_completed;
            self->context.options.execution_stats->executed_physical_plan_id =
                self->plan->plan_id();
          }
          return status;
        });
    if (!submitted.ok()) {
      return SetTerminal(submitted.status());
    }
    if (context.options.execution_stats) ++context.options.execution_stats->morsels_scheduled;
    const Status completed = submitted.ValueOrDie().Wait();
    if (!completed.ok()) return SetTerminal(completed);
    return terminal.ok() ? Status::OK() : terminal;
  }

  Status Pop(ResultBatch* output) {
    if (output == nullptr) {
      return Status::InvalidArgument("physical runtime", "missing result output");
    }
    while (queue.empty() && terminal.ok() && !eof) {
      const Status scheduled = ScheduleAndWait();
      if (!scheduled.ok()) break;
    }
    if (!queue.empty()) {
      *output = std::move(queue.front());
      queue.pop_front();
      return Status::OK();
    }
    if (!terminal.ok()) return terminal;
    return Status::NotFound("physical runtime", "end of stream");
  }

  void ReleaseExecutionResources() {
    scan.reset();
    pending_expand.reset();
    pending_range_expand.reset();
    pending_multi_hop_expands.clear();
    pending_multi_hop_property_gather.reset();
    pending_target_gather.reset();
    completed_node_property_bindings.clear();
    pending_gather.reset();
    pending_gather_phase = GatherPhase::kNone;
    pending_layout.clear();
    delta_index_cursor.reset();
    delta_candidate_cursor.reset();
    ReleaseAllAdvisoryIndexState();
    timeline.reset();
  }

  void ReleaseTerminalResources() {
    queue.clear();
    ReleaseExecutionResources();
  }

  Status SetTerminal(Status status) {
    if (terminal.ok()) terminal = std::move(status);
    ReleaseTerminalResources();
    return terminal;
  }

  void Abandon() {
    abandoned = true;
    ReleaseTerminalResources();
  }

  bool FinishMetadataQuantum(uint32_t processed) {
    if (context.options.execution_stats) {
      context.options.execution_stats->index_metadata_items_processed +=
          processed;
      ++context.options.execution_stats->index_metadata_yields;
      context.options.execution_stats->index_max_metadata_items_per_morsel =
          std::max<uint64_t>(
              context.options.execution_stats
                  ->index_max_metadata_items_per_morsel,
              processed);
    }
    return true;
  }

  Status CheckPreparationCancellation(const char* operation) const {
    return context.options.cancellation &&
            context.options.cancellation->IsCancelled()
        ? Status::QueryCancelled("physical runtime", operation)
        : Status::OK();
  }

  Status ReserveIndexMetadataState() {
    if (!index_metadata_lease) {
      return Status::Corruption(
          "physical runtime", "index metadata lease is missing");
    }
    return index_metadata_lease->ReserveAdditional(
        kIndexMetadataStoredItemCharge);
  }

  void InitializePredicatePreparation() {
    predicate_preparation_initialized = true;
    canonical_predicate_lease = std::make_shared<RuntimeMemoryLease>(
        context.options.memory_account, 0);
    if (plan->predicates().empty()) predicate_preparation_finished = true;
  }

  Status ReserveCanonicalValue(const Value& value) {
    constexpr uint64_t kCanonicalNodeOverhead =
        sizeof(IndexCanonicalValue) + 4 * sizeof(void*) + 64;
    const uint64_t payload = CanonicalPayloadBytes(value);
    if (payload > std::numeric_limits<uint64_t>::max() -
                      kCanonicalNodeOverhead) {
      return Status::QueryMemoryLimit(
          "physical runtime", "canonical predicate charge overflow");
    }
    if (!canonical_predicate_lease) {
      return Status::Corruption(
          "physical runtime", "canonical predicate lease is missing");
    }
    return canonical_predicate_lease->ReserveAdditional(
        kCanonicalNodeOverhead + payload);
  }

  Status ReserveCanonicalBlobHash() {
    constexpr uint64_t kCanonicalNodeOverhead =
        sizeof(IndexCanonicalValue) + 4 * sizeof(void*) + 64;
    constexpr uint64_t kBlobHashIdentityBytes = 32 + sizeof(uint64_t);
    if (!canonical_predicate_lease) {
      return Status::Corruption(
          "physical runtime", "canonical predicate lease is missing");
    }
    return canonical_predicate_lease->ReserveAdditional(
        kCanonicalNodeOverhead + kBlobHashIdentityBytes);
  }

  StatusOr<bool> AdvancePredicatePreparationQuantum() {
    if (!predicate_preparation_initialized) InitializePredicatePreparation();
    if (predicate_preparation_finished) return false;
    uint32_t processed = 0;
    uint32_t literal_charge_items = 0;
    while (predicate_preparation_predicate_index < plan->predicates().size() &&
           processed < kCandidateItemQuantum) {
      const Status cancelled = CheckPreparationCancellation(
          "query cancelled while preparing predicates");
      if (!cancelled.ok()) return cancelled;
      const PhysicalPredicate& predicate =
          plan->predicates()[predicate_preparation_predicate_index];
      if (canonical_predicates.size() ==
          predicate_preparation_predicate_index) {
        constexpr uint64_t kPredicateVectorCharge =
            2 * sizeof(CanonicalPhysicalPredicate) + 64;
        const Status reserved =
            canonical_predicate_lease->ReserveAdditional(kPredicateVectorCharge);
        if (!reserved.ok()) return reserved;
        canonical_predicates.emplace_back(
            predicate.kind, predicate.lower_inclusive,
            predicate.upper_inclusive, &predicate_literal_comparison_count,
            &count_predicate_literal_comparisons);
        ++processed;
        continue;
      }
      CanonicalPhysicalPredicate& canonical =
          canonical_predicates[predicate_preparation_predicate_index];
      if (predicate_preparation_value_index < predicate.values.size()) {
        const Value& literal =
            predicate.values[predicate_preparation_value_index++];
        const Status charged = ReserveCanonicalValue(literal);
        if (!charged.ok()) return charged;
        auto encoded = EncodeIndexCanonicalValue(literal);
        if (!encoded.ok()) return encoded.status();
        canonical.values.insert(std::move(encoded).ConsumeValueOrDie());
        if ((predicate.kind == PhysicalPredicateKind::kEquality ||
             predicate.kind == PhysicalPredicateKind::kIn) &&
            (literal.type() == PhysicalType::kString ||
             literal.type() == PhysicalType::kBinary)) {
          const Status hash_charged = ReserveCanonicalBlobHash();
          if (!hash_charged.ok()) return hash_charged;
          auto hash = EncodeIndexBlobHash(literal);
          if (!hash.ok()) return hash.status();
          canonical.values.insert(std::move(hash).ConsumeValueOrDie());
        }
        if (context.options.execution_stats) {
          ++context.options.execution_stats
                ->index_predicate_literals_canonicalized;
          ++context.options.execution_stats
                ->index_predicate_literal_charge_items;
        }
        ++literal_charge_items;
        ++processed;
        continue;
      }
      if (predicate_preparation_bound_stage == 0) {
        predicate_preparation_bound_stage = 1;
        if (predicate.lower_bound.has_value()) {
          const Status charged = ReserveCanonicalValue(*predicate.lower_bound);
          if (!charged.ok()) return charged;
          auto lower = EncodeIndexCanonicalValue(*predicate.lower_bound);
          if (!lower.ok()) return lower.status();
          canonical.lower = std::move(lower).ConsumeValueOrDie();
          ++processed;
          continue;
        }
      }
      if (predicate_preparation_bound_stage == 1) {
        predicate_preparation_bound_stage = 2;
        if (predicate.upper_bound.has_value()) {
          const Status charged = ReserveCanonicalValue(*predicate.upper_bound);
          if (!charged.ok()) return charged;
          auto upper = EncodeIndexCanonicalValue(*predicate.upper_bound);
          if (!upper.ok()) return upper.status();
          canonical.upper = std::move(upper).ConsumeValueOrDie();
          ++processed;
          continue;
        }
      }
      ++predicate_preparation_predicate_index;
      predicate_preparation_value_index = 0;
      predicate_preparation_bound_stage = 0;
      ++processed;
    }
    if (processed != 0) {
      if (predicate_preparation_predicate_index >= plan->predicates().size()) {
        predicate_preparation_finished = true;
        return false;
      }
      if (context.options.execution_stats) {
        ++context.options.execution_stats
              ->index_predicate_canonicalization_yields;
        if (literal_charge_items != 0) {
          ++context.options.execution_stats
                ->index_predicate_literal_charge_yields;
        }
      }
      return true;
    }
    predicate_preparation_finished = true;
    return false;
  }

  void InitializeIndexPreparation() {
    index_preparation_initialized = true;
    if (plan->predicates().empty() || !context.index_catalog_snapshot) {
      index_advisory_bypassed = true;
      index_preparation_phase = IndexPreparationPhase::kDone;
      return;
    }
    index_metadata_lease = std::make_shared<RuntimeMemoryLease>(
        context.options.memory_account, 0);
    selected_index_ids.assign(plan->predicates().size(), 0);
    candidate_binding_by_predicate.resize(plan->predicates().size());
    for (size_t predicate_index = 0;
         predicate_index < plan->predicates().size(); ++predicate_index) {
      candidate_binding_by_predicate[predicate_index] =
          PredicateBinding(predicate_index);
    }
    index_preparation_phase = IndexPreparationPhase::kSelectDefinitions;
  }

  BindingId PredicateBinding(size_t predicate_index) const {
    if (predicate_index >= plan->predicates().size()) return plan->binding_id();
    const SlotId slot = plan->predicates()[predicate_index].slot;
    const auto property = std::find_if(
        plan->predicate_properties().begin(),
        plan->predicate_properties().end(),
        [slot](const PhysicalPropertySlot& candidate) {
          return candidate.slot == slot;
        });
    if (property == plan->predicate_properties().end() ||
        property->binding.value == 0) {
      return plan->binding_id();
    }
    return property->binding;
  }

  bool PredicateMayUseAdvisoryIndex(size_t predicate_index) const {
    if (PredicateBinding(predicate_index) != plan->binding_id() ||
        !context.root_access_path.has_value()) {
      return true;
    }
    return std::find(context.root_access_predicate_indices.begin(),
                     context.root_access_predicate_indices.end(),
                     predicate_index) !=
        context.root_access_predicate_indices.end();
  }

  size_t ActiveCandidatePredicateCount() const {
    return static_cast<size_t>(std::count(
        active_candidate_predicates.begin(),
        active_candidate_predicates.end(), true));
  }

  size_t ActiveCandidatePredicateCount(BindingId binding) const {
    size_t count = 0;
    for (size_t predicate_index = 0;
         predicate_index < active_candidate_predicates.size();
         ++predicate_index) {
      if (active_candidate_predicates[predicate_index] &&
          candidate_binding_by_predicate[predicate_index] == binding) {
        ++count;
      }
    }
    return count;
  }

  bool IndexIsRequiredByActivePredicate(uint64_t index_id) const {
    for (size_t index = 0;
         index < selected_index_ids.size() &&
             index < active_candidate_predicates.size();
         ++index) {
      if (active_candidate_predicates[index] &&
          selected_index_ids[index] == index_id) {
        return true;
      }
    }
    return false;
  }

  enum class AdaptiveIndexSourceKind : uint8_t { kSst, kDelta };

  bool ObserveAdaptiveCandidates(
      size_t predicate_index, uint64_t matching_candidates,
      uint64_t unopened_sources, AdaptiveIndexSourceKind source_kind) {
    if (!context.options.execution_stats ||
        context.options.execution_stats->runtime_feedback_base_rows == 0 ||
        predicate_index >= active_candidate_predicates.size() ||
        !active_candidate_predicates[predicate_index]) {
      return false;
    }
    uint64_t& predicate_sample =
        adaptive_sampled_candidates_by_predicate[predicate_index];
    predicate_sample = matching_candidates >
            std::numeric_limits<uint64_t>::max() - predicate_sample
        ? std::numeric_limits<uint64_t>::max()
        : predicate_sample + matching_candidates;
    adaptive_sampled_candidates = matching_candidates >
            std::numeric_limits<uint64_t>::max() -
                adaptive_sampled_candidates
        ? std::numeric_limits<uint64_t>::max()
        : adaptive_sampled_candidates + matching_candidates;
    context.options.execution_stats->index_adaptive_sampled_candidates =
        adaptive_sampled_candidates;
    const uint64_t base_rows = context.options.execution_stats
                                   ->runtime_feedback_base_rows;
    const uint64_t threshold = base_rows / 2 + base_rows % 2;
    if (predicate_sample < threshold) return false;
    const BindingId binding = candidate_binding_by_predicate[predicate_index];
    if (ActiveCandidatePredicateCount(binding) > 1) {
      active_candidate_predicates[predicate_index] = false;
      ++context.options.execution_stats->index_adaptive_reoptimizations;
      ++context.options.execution_stats
            ->index_adaptive_intersection_predicates_dropped;
      if (context.options.execution_stats->has_executed_access_path &&
          binding == plan->binding_id() &&
          context.options.execution_stats->executed_access_path ==
              CandidateSource::kIntersection) {
        context.options.execution_stats->executed_access_path =
            CandidateSource::kIndex;
      }
      return false;
    }
    if (unopened_sources == 0) return false;
    ++context.options.execution_stats->index_adaptive_reoptimizations;
    if (source_kind == AdaptiveIndexSourceKind::kSst) {
      context.options.execution_stats
          ->index_adaptive_unopened_fragments_skipped += unopened_sources;
    } else {
      context.options.execution_stats
          ->index_adaptive_unopened_delta_sources_skipped += unopened_sources;
    }
    FallBackFromAdvisoryIndex();
    return true;
  }

  StatusOr<bool> AdvanceIndexPreparationQuantum() {
    if (!index_preparation_initialized) InitializeIndexPreparation();
    if (index_advisory_bypassed) return false;
    uint32_t processed = 0;
    while (processed < kCandidateItemQuantum) {
      if (index_preparation_phase ==
          IndexPreparationPhase::kSelectDefinitions) {
        if (index_metadata_predicate_index >= plan->predicates().size()) {
          if (selected_index_id_set.empty()) {
            index_advisory_bypassed = true;
            ReleaseIndexMetadataScratch();
            return processed == 0 ? StatusOr<bool>(false)
                                  : StatusOr<bool>(
                                        FinishMetadataQuantum(processed));
          }
          index_preparation_phase = IndexPreparationPhase::kScanSstSources;
          continue;
        }
        if (index_metadata_definition_index == 0 &&
            !PredicateMayUseAdvisoryIndex(
                index_metadata_predicate_index)) {
          ++index_metadata_predicate_index;
          ++processed;
          continue;
        }
        const auto& definitions = context.index_catalog_snapshot->definitions;
        if (index_metadata_definition_index >= definitions.size()) {
          ++index_metadata_predicate_index;
          index_metadata_definition_index = 0;
          continue;
        }
        const Status cancelled = CheckPreparationCancellation(
            "query cancelled while selecting index definitions");
        if (!cancelled.ok()) return cancelled;
        const PhysicalPredicate& predicate =
            plan->predicates()[index_metadata_predicate_index];
        const IndexDefinition& candidate =
            definitions[index_metadata_definition_index++];
        ++processed;
        if (candidate.state != IndexState::kActive ||
            candidate.entity_type != predicate.column.entity_type ||
            candidate.column_id != predicate.column.column_id ||
            candidate.schema_epoch != predicate.column.schema_epoch ||
            !IsSupportedIndexCanonicalEncoding(
                candidate.canonical_encoding_id) ||
            (candidate.capabilities &
             RequiredIndexCapability(predicate.kind)) == 0) {
          continue;
        }
        const Status reserved = ReserveIndexMetadataState();
        if (!reserved.ok()) return reserved;
        selected_index_ids[index_metadata_predicate_index] = candidate.index_id;
        selected_index_id_set.insert(candidate.index_id);
        selected_definition_by_column.insert_or_assign(
            IndexColumnKey{
                static_cast<uint8_t>(predicate.column.entity_type),
                predicate.column.column_id, predicate.column.schema_epoch},
            candidate.index_id);
        ++index_metadata_predicate_index;
        index_metadata_definition_index = 0;
        continue;
      }
      if (index_preparation_phase ==
          IndexPreparationPhase::kScanSstSources) {
        if (index_metadata_sst_source_index >= context.index_sources.size()) {
          index_preparation_phase = IndexPreparationPhase::kScanDeltaSources;
          continue;
        }
        const Status cancelled = CheckPreparationCancellation(
            "query cancelled while selecting SST index sources");
        if (!cancelled.ok()) return cancelled;
        const size_t source_index = index_metadata_sst_source_index++;
        TcypherIndexSource& source = context.index_sources[source_index];
        ++processed;
        if (selected_index_id_set.count(source.index_id) == 0) continue;
        const bool lazy = source.pinned_sst_source.has_value() &&
            source.sidecar.source_sst_id != source.source_sst_id;
        if (lazy &&
            (!source.definition.has_value() || !source.fragment.has_value() ||
             source.definition->index_id != source.index_id ||
             source.fragment->index_id != source.index_id ||
             source.fragment->source_sst_id != source.source_sst_id ||
             !source.fragment->usable || source.sidecar_path.empty() ||
             source.pinned_sst_source->metadata.file_number !=
                 source.source_sst_id)) {
          return Status::Corruption(
              "physical runtime",
              "relevant lazy SST index descriptor is incomplete");
        }
        const Status reserved = ReserveIndexMetadataState();
        if (!reserved.ok()) return reserved;
        relevant_sst_index_sources.push_back(source_index);
        sst_index_coverage.emplace(source.index_id, source.source_sst_id);
        if (lazy) lazy_sst_index_sources.push_back(source_index);
        continue;
      }
      if (index_preparation_phase ==
          IndexPreparationPhase::kScanDeltaSources) {
        if (index_metadata_delta_source_index >=
            context.delta_index_sources.size()) {
          index_preparation_phase =
              IndexPreparationPhase::kValidateSstCoverage;
          continue;
        }
        const Status cancelled = CheckPreparationCancellation(
            "query cancelled while selecting MemTable index sources");
        if (!cancelled.ok()) return cancelled;
        const size_t source_index = index_metadata_delta_source_index++;
        TcypherDeltaIndexSource& source =
            context.delta_index_sources[source_index];
        ++processed;
        if (selected_index_id_set.count(source.index_id) == 0) continue;
        if (source.pinned_memtable &&
            (!source.definition.has_value() ||
             source.definition->index_id != source.index_id)) {
          return Status::Corruption(
              "physical runtime",
              "relevant lazy delta index descriptor is incomplete");
        }
        const Status reserved = ReserveIndexMetadataState();
        if (!reserved.ok()) return reserved;
        relevant_delta_index_sources.push_back(source_index);
        if (source.pinned_memtable) {
          lazy_delta_index_sources.push_back(source_index);
        }
        const uint64_t source_generation =
            context.pinned_visible_seq_ceiling.value_or(
                context.visible_seq_ceiling);
        if (source.source_generation == source_generation) {
          ++delta_index_coverage[source.index_id];
        }
        continue;
      }
      if (index_preparation_phase ==
          IndexPreparationPhase::kValidateSstCoverage) {
        const size_t file_count = context.version_snapshot
            ? context.version_snapshot->files.size() : 0;
        if (index_metadata_version_file_index >= file_count) {
          index_preparation_phase =
              IndexPreparationPhase::kValidateDeltaCoverage;
          continue;
        }
        const Status cancelled = CheckPreparationCancellation(
            "query cancelled while validating SST index coverage");
        if (!cancelled.ok()) return cancelled;
        const SstFileMeta& file = context.version_snapshot->files[
            index_metadata_version_file_index++];
        ++processed;
        if (file.partition.key_kind != LogicalKeyKind::kProperty) continue;
        const auto definition = selected_definition_by_column.find(
            IndexColumnKey{
                static_cast<uint8_t>(file.partition.entity_type),
                file.partition.column_id, file.partition.schema_epoch});
        if (definition == selected_definition_by_column.end()) continue;
        if (sst_index_coverage.count(
                {definition->second, file.file_number}) == 0) {
          return Status::Corruption(
              "physical runtime", "relevant SST index coverage is incomplete");
        }
        continue;
      }
      if (index_preparation_phase ==
          IndexPreparationPhase::kValidateDeltaCoverage) {
        if (index_metadata_coverage_predicate_index >=
            selected_index_ids.size()) {
          active_candidate_predicates.assign(plan->predicates().size(), false);
          for (size_t predicate_index = 0;
               predicate_index < selected_index_ids.size(); ++predicate_index) {
            active_candidate_predicates[predicate_index] =
                selected_index_ids[predicate_index] != 0;
          }
          adaptive_sampled_candidates_by_predicate.assign(
              plan->predicates().size(), 0);
          index_preparation_phase = IndexPreparationPhase::kMaterializeSst;
          break;
        }
        const Status cancelled = CheckPreparationCancellation(
            "query cancelled while validating MemTable index coverage");
        if (!cancelled.ok()) return cancelled;
        const uint64_t definition_id =
            selected_index_ids[index_metadata_coverage_predicate_index++];
        if (definition_id == 0) continue;
        const auto covered = delta_index_coverage.find(definition_id);
        const size_t covered_memtables =
            covered == delta_index_coverage.end() ? 0 : covered->second;
        ++processed;
        if (covered_memtables != context.memtable_event_sources.size()) {
          return Status::Corruption(
              "physical runtime",
              "relevant MemTable index coverage is incomplete");
        }
        continue;
      }
      break;
    }
    if (processed != 0) return FinishMetadataQuantum(processed);

    if (index_preparation_phase ==
        IndexPreparationPhase::kMaterializeSst) {
      while (next_sst_index_source < lazy_sst_index_sources.size() &&
             !IndexIsRequiredByActivePredicate(
                 context.index_sources[
                     lazy_sst_index_sources[next_sst_index_source]].index_id)) {
        ++next_sst_index_source;
        if (context.options.execution_stats) {
          ++context.options.execution_stats
                ->index_adaptive_unopened_fragments_skipped;
        }
      }
      if (next_sst_index_source < lazy_sst_index_sources.size()) {
        const size_t source_index =
            lazy_sst_index_sources[next_sst_index_source++];
        const Status reserved = ReserveIndexMetadataState();
        if (!reserved.ok()) return reserved;
        prepared_sst_index_sources.push_back(source_index);
        const Status materialized = MaterializeLazySstIndexSource(
            &context.index_sources[source_index], &context,
            &index_preparation_leases);
        if (!materialized.ok()) return materialized;
        if (context.options.execution_stats) {
          ++context.options.execution_stats->index_sst_sources_materialized;
          ++context.options.execution_stats->index_preparation_yields;
        }
        const TcypherIndexSource& materialized_source =
            context.index_sources[source_index];
        for (size_t predicate_index = 0;
             predicate_index < active_candidate_predicates.size();
             ++predicate_index) {
          if (!active_candidate_predicates[predicate_index] ||
              selected_index_ids[predicate_index] !=
                  materialized_source.index_id) {
            continue;
          }
          uint64_t matching = 0;
          for (const IndexPosting& posting : materialized_source.sidecar.postings) {
            if (PostingMatchesPredicate(
                    posting, canonical_predicates[predicate_index], nullptr)) {
              ++matching;
            }
          }
          const uint64_t unopened = static_cast<uint64_t>(std::count_if(
              lazy_sst_index_sources.begin() + next_sst_index_source,
              lazy_sst_index_sources.end(),
              [this, &materialized_source](size_t remaining_source_index) {
                return context.index_sources[remaining_source_index].index_id ==
                    materialized_source.index_id;
              }));
          if (ObserveAdaptiveCandidates(
                  predicate_index, matching, unopened,
                  AdaptiveIndexSourceKind::kSst)) {
            return false;
          }
        }
        return true;
      }
      index_preparation_phase = IndexPreparationPhase::kMaterializeDelta;
    }
    if (index_preparation_phase ==
        IndexPreparationPhase::kMaterializeDelta) {
      while (next_delta_index_source < lazy_delta_index_sources.size() &&
             !IndexIsRequiredByActivePredicate(
                 context.delta_index_sources[
                     lazy_delta_index_sources[next_delta_index_source]].index_id)) {
        TcypherDeltaIndexSource& skipped = context.delta_index_sources[
            lazy_delta_index_sources[next_delta_index_source++]];
        skipped.pinned_memtable.reset();
        skipped.index.Reset(0);
        if (context.options.execution_stats) {
          ++context.options.execution_stats
                ->index_adaptive_unopened_delta_sources_skipped;
        }
      }
      if (next_delta_index_source >= lazy_delta_index_sources.size()) {
        index_preparation_phase = IndexPreparationPhase::kDone;
        return false;
      }
      const size_t source_index =
          lazy_delta_index_sources[next_delta_index_source];
      TcypherDeltaIndexSource& source =
          context.delta_index_sources[source_index];
      if (!delta_index_cursor.has_value()) {
        const uint64_t event_count = source.pinned_memtable->event_count();
        const uint64_t approximate_bytes =
            source.pinned_memtable->approximate_memory_bytes();
        constexpr uint64_t kIndexEntryOverhead = 128 + sizeof(uint64_t);
        if (approximate_bytes > std::numeric_limits<uint64_t>::max() / 3) {
          return Status::QueryMemoryLimit(
              "physical runtime", "delta index source charge overflow");
        }
        uint64_t charge = approximate_bytes * 3;
        if (event_count >
            (std::numeric_limits<uint64_t>::max() - charge) /
                kIndexEntryOverhead) {
          return Status::QueryMemoryLimit(
              "physical runtime", "delta index source charge overflow");
        }
        charge += event_count * kIndexEntryOverhead;
        const Status reserved = ReserveIndexMemory(
            context.options.memory_account, charge, &index_preparation_leases);
        if (!reserved.ok()) return reserved;
        const Status metadata_reserved = ReserveIndexMetadataState();
        if (!metadata_reserved.ok()) return metadata_reserved;
        prepared_delta_index_sources.push_back(source_index);
        source.events.clear();
        source.events.reserve(event_count);
        source.index.Reset(source.source_generation);
        auto opened = OpenTemporalMemTableCursor(source.pinned_memtable);
        if (!opened.ok()) return opened.status();
        delta_index_cursor.emplace(
            std::move(opened).ConsumeValueOrDie());
      }
      uint32_t indexed = 0;
      while (delta_index_cursor->valid() &&
             indexed < kDeltaIndexEventQuantum) {
        const Status cancelled = CheckPreparationCancellation(
            "query cancelled while building delta index");
        if (!cancelled.ok()) return cancelled;
        const TemporalEvent& event = delta_index_cursor->current();
        const uint64_t ordinal = source.events.size();
        source.events.push_back(event);
        const Status added =
            source.index.Add(*source.definition, ordinal, event);
        if (!added.ok()) return added;
        const Status advanced = delta_index_cursor->Advance();
        if (!advanced.ok()) return advanced;
        ++indexed;
        if (context.options.execution_stats) {
          ++context.options.execution_stats->index_delta_events_indexed;
        }
      }
      if (!delta_index_cursor->valid()) {
        if (!delta_index_cursor->terminal_status().ok()) {
          return delta_index_cursor->terminal_status();
        }
        delta_index_cursor.reset();
        source.pinned_memtable.reset();
        ++next_delta_index_source;
        if (context.options.execution_stats) {
          ++context.options.execution_stats->index_delta_sources_materialized;
        }
        for (size_t predicate_index = 0;
             predicate_index < active_candidate_predicates.size();
             ++predicate_index) {
          if (!active_candidate_predicates[predicate_index] ||
              selected_index_ids[predicate_index] != source.index_id) {
            continue;
          }
          std::set<uint64_t> matching_entities;
          for (const TemporalEvent& event : source.events) {
            if (!IsPredicatePropertyEvent(
                    event, plan->predicates()[predicate_index]) ||
                event.is_delete()) {
              continue;
            }
            auto matches = EventMatchesCanonicalPredicate(
                event, canonical_predicates[predicate_index], nullptr);
            if (!matches.ok()) return matches.status();
            if (matches.ValueOrDie()) {
              matching_entities.insert(event.logical_key().entity_id());
            }
          }
          const uint64_t unopened = static_cast<uint64_t>(std::count_if(
              lazy_delta_index_sources.begin() + next_delta_index_source,
              lazy_delta_index_sources.end(),
              [this, &source](size_t remaining_source_index) {
                return context.delta_index_sources[remaining_source_index]
                           .index_id == source.index_id;
              }));
          if (ObserveAdaptiveCandidates(
                  predicate_index, matching_entities.size(), unopened,
                  AdaptiveIndexSourceKind::kDelta)) {
            return false;
          }
        }
      }
      if (context.options.execution_stats) {
        ++context.options.execution_stats->index_preparation_yields;
      }
      return true;
    }
    return false;
  }

  Status InitializeCandidatePreparation() {
    if (selected_index_ids.size() != plan->predicates().size() ||
        candidate_binding_by_predicate.size() != plan->predicates().size() ||
        canonical_predicates.size() != plan->predicates().size()) {
      return Status::Corruption(
          "physical runtime", "selected predicate definitions are incomplete");
    }
    if (context.committed_events.size() >
        std::numeric_limits<uint64_t>::max() -
            context.session_overlay_events.size()) {
      return Status::QueryMemoryLimit(
          "physical runtime", "index candidate charge overflow");
    }
    candidate_upper_bound = context.committed_events.size() +
        context.session_overlay_events.size();
    active_candidate_predicate_indices.clear();
    active_candidate_ordinal_by_plan.assign(
        plan->predicates().size(), std::numeric_limits<size_t>::max());
    active_candidate_count_by_binding.clear();
    candidate_results_by_binding.clear();
    for (size_t predicate_index = 0;
         predicate_index < active_candidate_predicates.size();
         ++predicate_index) {
      if (active_candidate_predicates[predicate_index]) {
        const BindingId binding =
            candidate_binding_by_predicate[predicate_index];
        active_candidate_ordinal_by_plan[predicate_index] =
            active_candidate_count_by_binding[binding]++;
        candidate_results_by_binding.try_emplace(binding);
        active_candidate_predicate_indices.push_back(predicate_index);
      }
    }
    if (active_candidate_predicate_indices.empty()) {
      return Status::Corruption(
          "physical runtime", "adaptive candidate predicate set is empty");
    }
    candidate_source_index = 0;
    candidate_preparation_initialized = true;
    candidate_phase = CandidatePhase::kSst;
    return Status::OK();
  }

  Status CheckCandidateCancellation() const {
    return context.options.cancellation &&
            context.options.cancellation->IsCancelled()
        ? Status::QueryCancelled(
              "physical runtime", "query cancelled while building index candidates")
        : Status::OK();
  }

  void RecordCandidate(uint64_t entity_id) {
    const size_t plan_predicate_index =
        active_candidate_predicate_indices[candidate_predicate_index];
    const BindingId binding =
        candidate_binding_by_predicate[plan_predicate_index];
    const size_t binding_predicate_index =
        active_candidate_ordinal_by_plan[plan_predicate_index];
    const std::pair<BindingId, uint64_t> key{binding, entity_id};
    auto found = candidate_matches.find(key);
    if (binding_predicate_index == 0) {
      if (found == candidate_matches.end()) {
        candidate_matches.emplace(
            key, CandidateMatchProgress{1, binding_predicate_index});
      }
      return;
    }
    if (found == candidate_matches.end() ||
        found->second.last_predicate == binding_predicate_index) {
      return;
    }
    if (found->second.matched_predicate_count == binding_predicate_index) {
      ++found->second.matched_predicate_count;
    }
    found->second.last_predicate = binding_predicate_index;
  }

  uint64_t CandidateIdentity(const TemporalEvent& event,
                             size_t plan_predicate_index) const {
    return plan->predicates()[plan_predicate_index].column.entity_type ==
            EntityType::Vertex
        ? event.logical_key().entity_id()
        : event.logical_key().edge_id();
  }

  Status ValidateAndRecordSstOrdinals(
      TcypherIndexSource* source, const std::vector<uint64_t>& ordinals,
      const PhysicalPredicate& predicate,
      const CanonicalPhysicalPredicate& canonical_predicate,
      size_t plan_predicate_index) {
    const Status loaded = LoadPostingEvents(source, ordinals, context);
    if (!loaded.ok()) return loaded;
    for (uint64_t ordinal : ordinals) {
      const TemporalEvent* event = IndexedEventAt(*source, ordinal);
      if (event == nullptr || !IsPredicatePropertyEvent(*event, predicate) ||
          event->is_delete()) {
        return Status::Corruption(
            "physical runtime", "index posting does not match its source event");
      }
      auto matches = EventMatchesCanonicalPredicate(
          *event, canonical_predicate,
          &count_candidate_literal_lookup_comparisons);
      if (!matches.ok()) return matches.status();
      if (!matches.ValueOrDie()) {
        return Status::Corruption(
            "physical runtime", "index posting does not match its source event");
      }
      RecordCandidate(CandidateIdentity(*event, plan_predicate_index));
    }
    return Status::OK();
  }

  bool FinishCandidateQuantum(uint32_t processed) {
    if (context.options.execution_stats) {
      context.options.execution_stats->index_candidate_items_processed +=
          processed;
      ++context.options.execution_stats->index_candidate_preparation_yields;
      const auto scan_key = OperatorKeyFor(plan->operators().front().kind);
      if (scan_key.has_value()) {
        context.options.execution_stats->operator_runtime->AddIndexCandidates(
            *scan_key, processed);
      }
    }
    return true;
  }

  StatusOr<bool> AdvanceCandidatePreparationQuantum() {
    if (!candidate_preparation_initialized) {
      const Status initialized = InitializeCandidatePreparation();
      if (!initialized.ok()) return initialized;
    }
    while (candidate_phase != CandidatePhase::kDone) {
      if (candidate_phase == CandidatePhase::kCanonicalize) {
        uint32_t processed = 0;
        uint32_t literal_charge_items = 0;
        while (candidate_canonical_predicate_index <
                   plan->predicates().size() &&
               processed < kCandidateItemQuantum) {
          const Status cancelled = CheckCandidateCancellation();
          if (!cancelled.ok()) return cancelled;
          const PhysicalPredicate& predicate =
              plan->predicates()[candidate_canonical_predicate_index];
          if (canonical_predicates.size() ==
              candidate_canonical_predicate_index) {
            uint64_t* comparison_counter =
                context.options.execution_stats
                    ? &context.options.execution_stats
                           ->index_candidate_literal_lookup_comparisons
                    : nullptr;
            canonical_predicates.emplace_back(
                predicate.kind, predicate.lower_inclusive,
                predicate.upper_inclusive, comparison_counter,
                &count_candidate_literal_lookup_comparisons);
            ++processed;
            continue;
          }
          CanonicalPhysicalPredicate& canonical =
              canonical_predicates[candidate_canonical_predicate_index];
          if (candidate_canonical_value_index < predicate.values.size()) {
            const Value& literal =
                predicate.values[candidate_canonical_value_index];
            const Status charged = ReserveCanonicalValue(literal);
            if (!charged.ok()) return charged;
            auto value = EncodeIndexCanonicalValue(
                literal);
            if (!value.ok()) return value.status();
            ++candidate_canonical_value_index;
            const auto inserted = canonical.values.insert(
                std::move(value).ConsumeValueOrDie());
            (void)inserted;
            if ((predicate.kind == PhysicalPredicateKind::kEquality ||
                 predicate.kind == PhysicalPredicateKind::kIn) &&
                (literal.type() == PhysicalType::kString ||
                 literal.type() == PhysicalType::kBinary)) {
              const Status hash_charged = ReserveCanonicalBlobHash();
              if (!hash_charged.ok()) return hash_charged;
              auto hash = EncodeIndexBlobHash(literal);
              if (!hash.ok()) return hash.status();
              canonical.values.insert(std::move(hash).ConsumeValueOrDie());
            }
            if (context.options.execution_stats) {
              ++context.options.execution_stats
                    ->index_predicate_literals_canonicalized;
              ++context.options.execution_stats
                    ->index_predicate_literal_charge_items;
            }
            ++literal_charge_items;
            ++processed;
            continue;
          }
          if (candidate_canonical_bound_stage == 0) {
            candidate_canonical_bound_stage = 1;
            if (predicate.lower_bound.has_value()) {
              const Status charged = ReserveCanonicalValue(
                  *predicate.lower_bound);
              if (!charged.ok()) return charged;
              auto lower = EncodeIndexCanonicalValue(
                  *predicate.lower_bound);
              if (!lower.ok()) return lower.status();
              canonical.lower = std::move(lower).ConsumeValueOrDie();
              ++processed;
              continue;
            }
          }
          if (candidate_canonical_bound_stage == 1) {
            candidate_canonical_bound_stage = 2;
            if (predicate.upper_bound.has_value()) {
              const Status charged = ReserveCanonicalValue(
                  *predicate.upper_bound);
              if (!charged.ok()) return charged;
              auto upper = EncodeIndexCanonicalValue(
                  *predicate.upper_bound);
              if (!upper.ok()) return upper.status();
              canonical.upper = std::move(upper).ConsumeValueOrDie();
              ++processed;
              continue;
            }
          }
          ++candidate_canonical_predicate_index;
          candidate_canonical_value_index = 0;
          candidate_canonical_bound_stage = 0;
          ++processed;
        }
        if (processed != 0) {
          if (context.options.execution_stats) {
            ++context.options.execution_stats
                  ->index_predicate_canonicalization_yields;
            if (literal_charge_items != 0) {
              ++context.options.execution_stats
                    ->index_predicate_literal_charge_yields;
            }
          }
          return true;
        }
        candidate_phase = CandidatePhase::kSst;
        candidate_predicate_index = 0;
        continue;
      }
      if (candidate_predicate_index >=
          active_candidate_predicate_indices.size()) {
        if (candidate_phase != CandidatePhase::kFinalize) {
          candidate_phase = CandidatePhase::kFinalize;
          candidate_finalize_iterator = candidate_matches.begin();
        }
      }
      if (candidate_phase == CandidatePhase::kFinalize) {
        uint32_t processed = 0;
        while (candidate_finalize_iterator != candidate_matches.end() &&
               processed < kCandidateItemQuantum) {
          const Status cancelled = CheckCandidateCancellation();
          if (!cancelled.ok()) return cancelled;
          const BindingId binding = candidate_finalize_iterator->first.first;
          if (candidate_finalize_iterator->second.matched_predicate_count ==
              active_candidate_count_by_binding[binding]) {
            candidate_results_by_binding[binding].insert(
                candidate_finalize_iterator->first.second);
          }
          ++candidate_finalize_iterator;
          ++processed;
        }
        if (processed != 0) return FinishCandidateQuantum(processed);
        candidate_phase = CandidatePhase::kDone;
        return false;
      }

      const size_t plan_predicate_index =
          active_candidate_predicate_indices[candidate_predicate_index];
      const PhysicalPredicate& predicate =
          plan->predicates()[plan_predicate_index];
      const uint64_t definition_id =
          selected_index_ids[plan_predicate_index];
      uint32_t processed = 0;
      if (candidate_phase == CandidatePhase::kSst) {
        std::vector<uint64_t> matching_ordinals;
        matching_ordinals.reserve(kCandidateItemQuantum);
        TcypherIndexSource* ordinal_source = nullptr;
        while (candidate_source_index < relevant_sst_index_sources.size() &&
               processed < kCandidateItemQuantum) {
          const Status cancelled = CheckCandidateCancellation();
          if (!cancelled.ok()) return cancelled;
          TcypherIndexSource& source = context.index_sources[
              relevant_sst_index_sources[candidate_source_index]];
          if (source.index_id != definition_id) {
            ++candidate_source_index;
            candidate_item_index = 0;
            ++processed;
            continue;
          }
          if (source.sidecar.source_sst_id != source.source_sst_id) {
            return Status::Corruption(
                "physical runtime", "selected SST sidecar was not materialized");
          }
          candidate_healthy_source = true;
          if (candidate_item_index == source.sidecar.postings.size()) {
            ++candidate_source_index;
            candidate_item_index = 0;
            ++processed;
            continue;
          }
          const IndexPosting& posting =
              source.sidecar.postings[candidate_item_index++];
          if (PostingMatchesPredicate(
                  posting, canonical_predicates[plan_predicate_index],
                  &count_candidate_literal_lookup_comparisons)) {
            matching_ordinals.push_back(posting.source_row_ordinal);
            ordinal_source = &source;
          }
          ++processed;
          if (candidate_item_index == source.sidecar.postings.size()) {
            ++candidate_source_index;
            candidate_item_index = 0;
          }
          if (ordinal_source != nullptr &&
              (candidate_source_index >= relevant_sst_index_sources.size() ||
               &context.index_sources[
                    relevant_sst_index_sources[candidate_source_index]] !=
                   ordinal_source)) {
            break;
          }
        }
        if (!matching_ordinals.empty()) {
          const Status recorded = ValidateAndRecordSstOrdinals(
              ordinal_source, matching_ordinals, predicate,
              canonical_predicates[plan_predicate_index],
              plan_predicate_index);
          if (!recorded.ok()) return recorded;
        }
        if (processed != 0) return FinishCandidateQuantum(processed);
        candidate_phase = CandidatePhase::kDelta;
        candidate_source_index = 0;
        candidate_item_index = 0;
        delta_candidate_cursor.reset();
        continue;
      }
      if (candidate_phase == CandidatePhase::kDelta) {
        while (candidate_source_index < relevant_delta_index_sources.size() &&
               processed < kCandidateItemQuantum) {
          const Status cancelled = CheckCandidateCancellation();
          if (!cancelled.ok()) return cancelled;
          const TcypherDeltaIndexSource& source = context.delta_index_sources[
              relevant_delta_index_sources[candidate_source_index]];
          if (source.index_id != definition_id) {
            delta_candidate_cursor.reset();
            ++candidate_source_index;
            ++processed;
            continue;
          }
          const CanonicalPhysicalPredicate& canonical =
              canonical_predicates[plan_predicate_index];
          if (!delta_candidate_cursor.has_value()) {
            MemtableDeltaIndex::LookupKind lookup_kind =
                MemtableDeltaIndex::LookupKind::kEqualityOrIn;
            if (predicate.kind == PhysicalPredicateKind::kRange) {
              lookup_kind = MemtableDeltaIndex::LookupKind::kRange;
            } else if (predicate.kind == PhysicalPredicateKind::kPrefix) {
              lookup_kind = MemtableDeltaIndex::LookupKind::kPrefix;
            }
            auto opened = source.index.OpenLookupCursor(
                source.source_generation, lookup_kind,
                &canonical.values,
                canonical.lower.has_value() ? &*canonical.lower : nullptr,
                canonical.lower_inclusive,
                canonical.upper.has_value() ? &*canonical.upper : nullptr,
                canonical.upper_inclusive);
            if (!opened.ok()) return opened.status();
            delta_candidate_cursor.emplace(
                std::move(opened).ConsumeValueOrDie());
            candidate_healthy_source = true;
            if (context.options.execution_stats) {
              ++context.options.execution_stats->memtable_delta_probes;
            }
            ++processed;
            if (processed == kCandidateItemQuantum) break;
          }
          std::vector<uint64_t> ordinals;
          uint32_t cursor_work = 0;
          const Status advanced = delta_candidate_cursor->Advance(
              kCandidateItemQuantum - processed,
              context.options.cancellation.get(), &ordinals, &cursor_work);
          if (!advanced.ok()) return advanced;
          processed += cursor_work;
          for (uint64_t ordinal : ordinals) {
            if (ordinal >= source.events.size()) {
              return Status::Corruption(
                  "physical runtime", "delta index ordinal exceeds source events");
            }
            const TemporalEvent& event = source.events[ordinal];
            if (!IsPredicatePropertyEvent(event, predicate) ||
                event.is_delete()) {
              return Status::Corruption(
                  "physical runtime", "delta index candidate differs from source event");
            }
            auto matches = EventMatchesCanonicalPredicate(
                event,
                canonical,
                &count_candidate_literal_lookup_comparisons);
            if (!matches.ok()) return matches.status();
            if (!matches.ValueOrDie()) {
              return Status::Corruption(
                  "physical runtime", "delta index value differs from predicate");
            }
            RecordCandidate(CandidateIdentity(event, plan_predicate_index));
            if (context.options.execution_stats) {
              ++context.options.execution_stats->memtable_delta_candidates;
            }
          }
          if (delta_candidate_cursor->done()) {
            delta_candidate_cursor.reset();
            ++candidate_source_index;
          } else if (cursor_work == 0) {
            return Status::Corruption(
                "physical runtime", "delta index cursor made no progress");
          }
        }
        if (processed != 0) return FinishCandidateQuantum(processed);
        delta_candidate_cursor.reset();
        candidate_phase = CandidatePhase::kCommitted;
        candidate_source_index = 0;
        candidate_item_index = 0;
        if (!context.committed_events.empty()) candidate_healthy_source = true;
        continue;
      }
      const std::vector<TemporalEvent>* events =
          candidate_phase == CandidatePhase::kCommitted
              ? &context.committed_events : &context.session_overlay_events;
      if (!events->empty()) candidate_healthy_source = true;
      while (candidate_item_index < events->size() &&
             processed < kCandidateItemQuantum) {
        const Status cancelled = CheckCandidateCancellation();
        if (!cancelled.ok()) return cancelled;
        const TemporalEvent& event = (*events)[candidate_item_index++];
        if (IsPredicatePropertyEvent(event, predicate) && !event.is_delete()) {
          auto matches = EventMatchesCanonicalPredicate(
              event, canonical_predicates[plan_predicate_index],
              &count_candidate_literal_lookup_comparisons);
          if (!matches.ok()) return matches.status();
          if (matches.ValueOrDie()) {
            RecordCandidate(CandidateIdentity(event, plan_predicate_index));
          }
        }
        ++processed;
      }
      if (processed != 0) return FinishCandidateQuantum(processed);
      if (candidate_phase == CandidatePhase::kCommitted) {
        candidate_phase = CandidatePhase::kOverlay;
        candidate_item_index = 0;
        continue;
      }
      if (!candidate_healthy_source) {
        return Status::Corruption(
            "physical runtime", "selected advisory index has no healthy source");
      }
      ++candidate_predicate_index;
      candidate_phase = CandidatePhase::kSst;
      candidate_source_index = 0;
      candidate_item_index = 0;
      delta_candidate_cursor.reset();
      candidate_healthy_source = false;
    }
    return false;
  }

  void ReleasePreparedIndexSources() {
    delta_index_cursor.reset();
    delta_candidate_cursor.reset();
    for (size_t source_index : prepared_sst_index_sources) {
      TcypherIndexSource& source = context.index_sources[source_index];
      source.sidecar = IndexSidecar{};
      source.loaded_events_by_ordinal.clear();
      source.ordinal_read_retentions.clear();
      source.validated_source_row_count.reset();
    }
    for (size_t source_index : prepared_delta_index_sources) {
      TcypherDeltaIndexSource& source =
          context.delta_index_sources[source_index];
      std::vector<TemporalEvent>().swap(source.events);
      source.index.Reset(0);
    }
    prepared_sst_index_sources.clear();
    prepared_delta_index_sources.clear();
    index_preparation_leases.clear();
  }

  void ReleaseIndexMetadataScratch() {
    std::vector<uint64_t>().swap(selected_index_ids);
    candidate_binding_by_predicate.clear();
    selected_index_id_set.clear();
    selected_definition_by_column.clear();
    relevant_sst_index_sources.clear();
    relevant_delta_index_sources.clear();
    lazy_sst_index_sources.clear();
    lazy_delta_index_sources.clear();
    sst_index_coverage.clear();
    delta_index_coverage.clear();
    active_candidate_predicates.clear();
    adaptive_sampled_candidates_by_predicate.clear();
    index_metadata_lease.reset();
    index_metadata_predicate_index = 0;
    index_metadata_definition_index = 0;
    index_metadata_sst_source_index = 0;
    index_metadata_delta_source_index = 0;
    index_metadata_version_file_index = 0;
    index_metadata_coverage_predicate_index = 0;
  }

  void ReleaseCandidateScratch() {
    delta_candidate_cursor.reset();
    canonical_predicates.clear();
    canonical_predicate_lease.reset();
    candidate_matches.clear();
    candidate_results_by_binding.clear();
    active_candidate_predicate_indices.clear();
    active_candidate_ordinal_by_plan.clear();
    active_candidate_count_by_binding.clear();
    candidate_canonical_predicate_index = 0;
    candidate_canonical_value_index = 0;
    candidate_canonical_bound_stage = 0;
    count_candidate_literal_lookup_comparisons = false;
    candidate_phase = CandidatePhase::kDone;
  }

  void ReleaseAllAdvisoryIndexState() {
    ReleasePreparedIndexSources();
    ReleaseCandidateScratch();
    index_candidate_leases.clear();
    ReleaseIndexMetadataScratch();
    advisory_candidates_by_binding.clear();
    scan_spec.allowed_candidate_entity_ids.reset();
  }

  void FallBackFromAdvisoryIndex() {
    ReleaseAllAdvisoryIndexState();
    if (context.options.memory_account != nullptr &&
        context.options.batch_capacity > 1) {
      const uint64_t per_row_scan_charge = sizeof(TemporalEvent) +
          10 * (sizeof(Value) + sizeof(bool));
      if (per_row_scan_charge != 0 &&
          context.options.memory_account->hard_limit_bytes() /
                  per_row_scan_charge < context.options.batch_capacity) {
        context.options.batch_capacity = 1;
        scan_spec.batch_capacity = 1;
      }
    }
    index_preparation_finished = true;
    index_advisory_bypassed = true;
    if (context.options.execution_stats) {
      ++context.options.execution_stats->index_advisory_fallbacks;
      if (context.root_access_path.has_value() &&
          *context.root_access_path != CandidateSource::kBase) {
        context.options.execution_stats->has_executed_access_path = true;
        context.options.execution_stats->executed_access_path =
            CandidateSource::kBase;
        context.options.execution_stats->access_path_fallback = true;
      }
    }
  }

  struct RangePendingEvent {
    uint64_t entity_id = 0;
    uint64_t valid_from = 0;
    uint64_t commit_seq = 0;
    int32_t operation = 0;
  };

  struct RangeOutputRow {
    RangePendingEvent event;
    uint64_t valid_to = std::numeric_limits<uint64_t>::max();
  };

  struct PendingRangeExpand {
    std::vector<RangeOutputRow> sources;
    size_t source_index = 0;
    std::vector<RawRangeExpandRow> expanded;
    struct ChainRow {
      std::vector<RawTemporalFact> nodes;
      std::vector<RawTemporalFact> edges;
      std::vector<uint32_t> segment_edge_ends;
      std::set<LogicalKey> visited_edges;
      uint64_t valid_from = 0;
      uint64_t valid_to = 0;
    };
    std::vector<ChainRow> expanded_chains;
    std::shared_ptr<QueryMemoryLease> expanded_chain_lease;
    std::unique_ptr<QuerySpillFile> expanded_chain_spill;
    bool expanded_chain_spill_rewound = false;
    size_t expanded_index = 0;
  };

  static uint64_t RangeChainBytes(
      const PendingRangeExpand::ChainRow& chain) {
    return sizeof(PendingRangeExpand::ChainRow) +
        static_cast<uint64_t>(chain.nodes.size() + chain.edges.size()) *
            sizeof(RawTemporalFact) +
        static_cast<uint64_t>(chain.segment_edge_ends.size()) *
            sizeof(uint32_t) +
        static_cast<uint64_t>(chain.visited_edges.size()) *
            (sizeof(LogicalKey) + 4 * sizeof(void*) + 32);
  }

  static std::string EncodeRangeFact(const RawTemporalFact& fact) {
    std::string encoded;
    encoded.reserve(47);
    encoded.push_back(static_cast<char>(fact.entity_type));
    encoded.push_back(static_cast<char>(fact.key_kind));
    AppendPointPathU64(&encoded, fact.entity_id);
    AppendPointPathU64(&encoded, fact.target_id);
    AppendPointPathU64(&encoded, fact.edge_id);
    AppendPointPathU16(&encoded, fact.edge_type);
    AppendPointPathU16(&encoded, fact.column_id);
    AppendPointPathU64(&encoded, fact.valid_from);
    AppendPointPathU64(&encoded, fact.commit_seq);
    encoded.push_back(static_cast<char>(fact.operation));
    return encoded;
  }

  static StatusOr<std::optional<RawTemporalFact>> DecodeRangeFact(
      const std::string& input) {
    if (input.size() != 47) {
      return Status::Corruption(
          "physical runtime", "invalid range frontier fact");
    }
    size_t offset = 0;
    RawTemporalFact fact;
    fact.entity_type = static_cast<EntityType>(
        static_cast<uint8_t>(input[offset++]));
    fact.key_kind = static_cast<LogicalKeyKind>(
        static_cast<uint8_t>(input[offset++]));
    const auto read_u64 = [&input, &offset]() {
      uint64_t value = 0;
      for (uint32_t shift = 0; shift < 64; shift += 8) {
        value |= static_cast<uint64_t>(
            static_cast<uint8_t>(input[offset++])) << shift;
      }
      return value;
    };
    const auto read_u16 = [&input, &offset]() {
      uint16_t value = static_cast<uint8_t>(input[offset++]);
      value |= static_cast<uint16_t>(
          static_cast<uint8_t>(input[offset++])) << 8U;
      return value;
    };
    fact.entity_id = read_u64();
    fact.target_id = read_u64();
    fact.edge_id = read_u64();
    fact.edge_type = read_u16();
    fact.column_id = read_u16();
    fact.valid_from = read_u64();
    fact.commit_seq = read_u64();
    fact.operation = static_cast<TemporalOperation>(
        static_cast<uint8_t>(input[offset++]));
    if ((fact.entity_type != EntityType::Vertex &&
         fact.entity_type != EntityType::EdgeOut &&
         fact.entity_type != EntityType::EdgeIn) ||
        (fact.key_kind != LogicalKeyKind::kExistence &&
         fact.key_kind != LogicalKeyKind::kProperty) ||
        (fact.operation != TemporalOperation::kPut &&
         fact.operation != TemporalOperation::kDelete)) {
      return Status::Corruption(
          "physical runtime", "invalid range frontier fact kind");
    }
    return std::optional<RawTemporalFact>{fact};
  }

  static StatusOr<ResultBatch> EncodeRangeChain(
      const PendingRangeExpand::ChainRow& chain) {
    ColumnBatch batch(1);
    for (const Value& value : {Value::Timestamp(chain.valid_from),
                               Value::Timestamp(chain.valid_to)}) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::vector<Value>{value}, std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    const auto make_facts = [](const std::vector<RawTemporalFact>& facts) {
      ListValue values;
      values.elements.reserve(facts.size());
      for (const RawTemporalFact& fact : facts) {
        values.elements.push_back(Value::Binary(EncodeRangeFact(fact)));
      }
      return values;
    };
    ListValue visited;
    visited.elements.reserve(chain.visited_edges.size());
    for (const LogicalKey& edge : chain.visited_edges) {
      visited.elements.push_back(Value::Binary(EncodePointPathEdge(edge)));
    }
    ListValue segment_ends;
    for (uint32_t end : chain.segment_edge_ends) {
      segment_ends.elements.push_back(
          Value::Int64(static_cast<int64_t>(end)));
    }
    for (ListValue values :
         {make_facts(chain.nodes), make_facts(chain.edges),
          std::move(segment_ends), std::move(visited)}) {
      const Status added = batch.AddVector(std::make_shared<ListVector>(
          std::vector<ListValue>{std::move(values)}, std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    return ResultBatch(
        {"valid_from", "valid_to", "nodes", "edges", "segment_ends",
         "visited"},
        std::move(batch));
  }

  static StatusOr<PendingRangeExpand::ChainRow> DecodeRangeChain(
      const ResultBatch& batch) {
    if (batch.batch().row_count() != 1 ||
        batch.batch().column_count() != 6 ||
        !batch.batch().IsList(2) || !batch.batch().IsList(3) ||
        !batch.batch().IsList(4) || !batch.batch().IsList(5)) {
      return Status::Corruption(
          "physical runtime", "invalid range frontier spill row");
    }
    const Value* valid_from = batch.batch().ValueRefAt(0, 0);
    const Value* valid_to = batch.batch().ValueRefAt(1, 0);
    if (valid_from == nullptr || valid_to == nullptr ||
        valid_from->type() != PhysicalType::kTimestamp64 ||
        valid_to->type() != PhysicalType::kTimestamp64) {
      return Status::Corruption(
          "physical runtime", "invalid range frontier interval");
    }
    PendingRangeExpand::ChainRow chain;
    chain.valid_from = std::get<uint64_t>(valid_from->data());
    chain.valid_to = std::get<uint64_t>(valid_to->data());
    for (uint32_t column : {2U, 3U}) {
      const auto values = batch.batch().ListAt(column, 0);
      if (!values.has_value()) {
        return Status::Corruption(
            "physical runtime", "missing range frontier facts");
      }
      std::vector<RawTemporalFact>& facts =
          column == 2 ? chain.nodes : chain.edges;
      for (const auto& value : values->elements) {
        if (!value.has_value() || value->type() != PhysicalType::kBinary) {
          return Status::Corruption(
              "physical runtime", "invalid range frontier fact value");
        }
        auto fact = DecodeRangeFact(std::get<std::string>(value->data()));
        if (!fact.ok() || !fact.ValueOrDie().has_value()) {
          return fact.ok()
              ? Status::Corruption(
                    "physical runtime", "missing range frontier fact")
              : fact.status();
        }
        facts.push_back(*fact.ValueOrDie());
      }
    }
    const auto segment_ends = batch.batch().ListAt(4, 0);
    if (!segment_ends.has_value()) {
      return Status::Corruption(
          "physical runtime", "missing range frontier segment bounds");
    }
    for (const auto& value : segment_ends->elements) {
      if (!value.has_value() || value->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(value->data()) < 0 ||
          static_cast<uint64_t>(std::get<int64_t>(value->data())) >
              std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption(
            "physical runtime", "invalid range frontier segment bound");
      }
      chain.segment_edge_ends.push_back(
          static_cast<uint32_t>(std::get<int64_t>(value->data())));
    }
    const auto visited = batch.batch().ListAt(5, 0);
    if (!visited.has_value()) {
      return Status::Corruption(
          "physical runtime", "missing range frontier visited edges");
    }
    for (const auto& value : visited->elements) {
      if (!value.has_value() || value->type() != PhysicalType::kBinary) {
        return Status::Corruption(
            "physical runtime", "invalid range frontier visited edge");
      }
      auto edge = DecodePointPathEdge(std::get<std::string>(value->data()));
      if (!edge.ok() || !edge.ValueOrDie().has_value()) {
        return edge.ok()
            ? Status::Corruption(
                  "physical runtime", "missing range frontier edge")
            : edge.status();
      }
      chain.visited_edges.insert(*edge.ValueOrDie());
    }
    if (chain.nodes.empty()) {
      return Status::Corruption(
          "physical runtime", "range frontier has no nodes");
    }
    return chain;
  }

  Status CollectRawFacts(
      TemporalScanSpec spec, std::vector<RawTemporalFact>* facts,
      std::shared_ptr<QueryMemoryLease>* fact_lease,
      ScopedOperatorRuntimeTimer* timer = nullptr) {
    if (facts == nullptr || fact_lease == nullptr) {
      return Status::InvalidArgument("physical runtime", "missing raw fact output");
    }
    *fact_lease = std::make_shared<QueryMemoryLease>(
        context.options.memory_account, 0);
    spec.cancellation = context.options.cancellation;
    spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &spec);
    const auto scan_start = std::chrono::steady_clock::now();
    const Status visited = VisitPinnedRawTemporalFacts(
        sources, std::move(spec), [facts, fact_lease](const RawTemporalFact& fact) {
          const Status reserved = (*fact_lease)->ReserveAdditional(
              sizeof(RawTemporalFact));
          if (!reserved.ok()) return reserved;
          facts->push_back(fact);
          return Status::OK();
        });
    if (timer != nullptr) timer->RecordBlocked(ElapsedNs(scan_start));
    return visited;
  }

  Status BuildRangeExpandRows(const RangeOutputRow& source,
                              std::vector<RawRangeExpandRow>* output,
                              ScopedOperatorRuntimeTimer* timer) {
    if (!plan->expand().has_value() || output == nullptr) {
      return Status::Corruption("physical runtime", "range expand state is incomplete");
    }
    const auto range = plan->valid_time_range();
    if (!range.has_value()) {
      return Status::Corruption("physical runtime", "range expand bounds are absent");
    }
    const RawTemporalFact source_fact{
        EntityType::Vertex, LogicalKeyKind::kExistence, source.event.entity_id, 0, 0, 0, 0,
        source.event.valid_from, source.event.commit_seq,
        static_cast<TemporalOperation>(source.event.operation)};
    const std::vector<RawTemporalInterval> source_intervals{
        RawTemporalInterval{source_fact, source.event.valid_from, source.valid_to}};
    auto source_ids = std::make_shared<std::set<uint64_t>>();
    source_ids->insert(source.event.entity_id);
    TemporalScanSpec edge_spec{0, snapshot_seq, context.options.batch_capacity};
    edge_spec.entity_type = plan->expand()->direction;
    edge_spec.key_kind = LogicalKeyKind::kExistence;
    edge_spec.edge_type = plan->expand()->edge_type;
    edge_spec.allowed_candidate_entity_ids = std::move(source_ids);
    std::vector<RawTemporalFact> edge_facts;
    std::shared_ptr<QueryMemoryLease> edge_fact_lease;
    const Status edges = CollectRawFacts(
        std::move(edge_spec), &edge_facts, &edge_fact_lease, timer);
    if (!edges.ok()) return edges;
    std::map<std::tuple<uint64_t, uint16_t, uint64_t>, std::vector<RawTemporalFact>> by_edge;
    for (const RawTemporalFact& edge : edge_facts) {
      if (edge.entity_id == source.event.entity_id &&
          edge.entity_type == plan->expand()->direction &&
          edge.key_kind == LogicalKeyKind::kExistence) {
        by_edge[std::make_tuple(edge.target_id, edge.edge_type, edge.edge_id)].push_back(edge);
      }
    }
    for (const auto& entry : by_edge) {
      const auto edge_intervals = DeriveRawTemporalIntervals(
          entry.second, range->first, range->second);
      if (!edge_intervals.ok()) return edge_intervals.status();
      const uint64_t target_id = std::get<0>(entry.first);
      TemporalScanSpec target_spec{0, snapshot_seq, context.options.batch_capacity};
      target_spec.entity_type = EntityType::Vertex;
      target_spec.key_kind = LogicalKeyKind::kExistence;
      target_spec.exact_key = LogicalKey::VertexExistence(target_id);
      std::vector<RawTemporalFact> target_facts;
      std::shared_ptr<QueryMemoryLease> target_fact_lease;
      const Status targets = CollectRawFacts(
          std::move(target_spec), &target_facts, &target_fact_lease, timer);
      if (!targets.ok()) return targets;
      const auto target_intervals = DeriveRawTemporalIntervals(
          target_facts, range->first, range->second);
      if (!target_intervals.ok()) return target_intervals.status();
      const auto expanded = ExpandRawIntervalHop(
          source_intervals, edge_intervals.ValueOrDie(), target_intervals.ValueOrDie(),
          range->first, range->second);
      if (!expanded.ok()) return expanded.status();
      output->insert(output->end(), expanded.ValueOrDie().begin(), expanded.ValueOrDie().end());
    }
    return Status::OK();
  }

  Status BuildSegmentedRangeExpandChains(
      const RangeOutputRow& source,
      std::vector<PendingRangeExpand::ChainRow>* output,
      std::shared_ptr<QueryMemoryLease>* output_lease,
      std::unique_ptr<QuerySpillFile>* output_spill) {
    if (output == nullptr || output_lease == nullptr ||
        output_spill == nullptr || plan->expand_steps().size() < 2) {
      return Status::InvalidArgument(
          "physical runtime", "segmented range frontier is incomplete");
    }
    const auto range = plan->valid_time_range();
    if (!range.has_value()) {
      return Status::Corruption(
          "physical runtime", "segmented range bounds are absent");
    }
    output->clear();
    output_spill->reset();
    *output_lease = std::make_shared<QueryMemoryLease>(
        context.options.memory_account, 0);
    const auto new_spill = [this]()
        -> StatusOr<std::unique_ptr<QuerySpillFile>> {
      const std::string directory = context.options.spill_directory.empty()
          ? "/tmp" : context.options.spill_directory;
      auto spill = std::make_unique<QuerySpillFile>(
          directory, context.options.cancellation,
          context.options.spill_resource_extensions,
          context.options.memory_account,
          [stats = context.options.execution_stats](uint64_t bytes) {
            if (stats) stats->path_frontier_spill_bytes += bytes;
          });
      const Status opened = spill->Open();
      if (!opened.ok()) return opened;
      if (context.options.execution_stats) {
        ++context.options.execution_stats->path_frontier_spill_starts;
      }
      return spill;
    };
    const auto append_spilled = [](QuerySpillFile* spill,
                                   const PendingRangeExpand::ChainRow& chain) {
      if (spill == nullptr) {
        return Status::InvalidArgument(
            "physical runtime", "missing segmented range frontier spill");
      }
      auto encoded = EncodeRangeChain(chain);
      if (!encoded.ok()) return encoded.status();
      return spill->Append(encoded.ValueOrDie());
    };
    const auto begin_spill = [&](std::vector<PendingRangeExpand::ChainRow>* states,
                                 std::shared_ptr<QueryMemoryLease>* lease,
                                 std::unique_ptr<QuerySpillFile>* spill) {
      auto opened = new_spill();
      if (!opened.ok()) return opened.status();
      *spill = std::move(opened).ConsumeValueOrDie();
      for (const PendingRangeExpand::ChainRow& state : *states) {
        const Status appended = append_spilled(spill->get(), state);
        if (!appended.ok()) return appended;
      }
      states->clear();
      lease->reset();
      return Status::OK();
    };
    const auto retain_output = [&](const PendingRangeExpand::ChainRow& state) {
      if (*output_spill) {
        return append_spilled(output_spill->get(), state);
      }
      if (context.options.memory_account &&
          context.options.memory_account->ShouldSpill()) {
        const Status started = begin_spill(output, output_lease, output_spill);
        if (!started.ok()) return started;
        return append_spilled(output_spill->get(), state);
      }
      const Status reserved = (*output_lease)->ReserveAdditional(
          RangeChainBytes(state));
      if (!reserved.ok()) {
        const Status started = begin_spill(output, output_lease, output_spill);
        if (!started.ok()) return started;
        return append_spilled(output_spill->get(), state);
      }
      output->push_back(state);
      return Status::OK();
    };
    const RawTemporalFact root{
        EntityType::Vertex, LogicalKeyKind::kExistence,
        source.event.entity_id, 0, 0, 0, 0, source.event.valid_from,
        source.event.commit_seq,
        static_cast<TemporalOperation>(source.event.operation)};
    std::vector<PendingRangeExpand::ChainRow> frontier{
        PendingRangeExpand::ChainRow{
            {root}, {}, {}, {}, source.event.valid_from, source.valid_to}};
    for (size_t segment_index = 0;
         segment_index < plan->expand_steps().size(); ++segment_index) {
      if (context.options.expand_segment_observer) {
        context.options.expand_segment_observer(
            static_cast<uint32_t>(segment_index));
      }
      if (context.options.cancellation &&
          context.options.cancellation->IsCancelled()) {
        return Status::QueryCancelled(
            "physical runtime",
            "query cancelled before expanding segmented range segment");
      }
      const PhysicalExpandSpec& step = plan->expand_steps()[segment_index];
      std::vector<PendingRangeExpand::ChainRow> segment_frontier =
          std::move(frontier);
      std::vector<PendingRangeExpand::ChainRow> next_segment;
      for (uint32_t hop = 1;
           hop <= step.max_hops && !segment_frontier.empty(); ++hop) {
        AddOperatorInputAt(PhysicalOperatorKind::kExpand,
                           static_cast<uint32_t>(segment_index),
                           segment_frontier.size());
        const auto stats = context.options.execution_stats;
        ScopedOperatorRuntimeTimer timer(
            stats ? stats->operator_runtime : nullptr,
            OperatorKeyForOccurrence(
                PhysicalOperatorKind::kExpand, segment_index));
        std::vector<PendingRangeExpand::ChainRow> next_hop;
        size_t completed_this_hop = 0;
        for (const PendingRangeExpand::ChainRow& chain : segment_frontier) {
          if (context.options.cancellation &&
              context.options.cancellation->IsCancelled()) {
            return Status::QueryCancelled(
                "physical runtime",
                "query cancelled while expanding segmented range frontier");
          }
          if (chain.nodes.empty()) {
            return Status::Corruption(
                "physical runtime", "segmented range chain lacks source");
          }
          const RawTemporalFact& source_fact = chain.nodes.back();
          const std::vector<RawTemporalInterval> source_intervals{
              RawTemporalInterval{
                  source_fact, chain.valid_from, chain.valid_to}};
          auto source_ids = std::make_shared<std::set<uint64_t>>();
          source_ids->insert(source_fact.entity_id);
          TemporalScanSpec edge_spec{
              0, snapshot_seq, context.options.batch_capacity};
          edge_spec.entity_type = step.direction;
          edge_spec.key_kind = LogicalKeyKind::kExistence;
          edge_spec.edge_type = step.edge_type;
          edge_spec.allowed_candidate_entity_ids = std::move(source_ids);
          std::vector<RawTemporalFact> edge_facts;
          std::shared_ptr<QueryMemoryLease> edge_fact_lease;
          Status status = CollectRawFacts(
              std::move(edge_spec), &edge_facts, &edge_fact_lease, &timer);
          if (!status.ok()) return status;
          std::map<std::tuple<uint64_t, uint16_t, uint64_t>,
                   std::vector<RawTemporalFact>> by_edge;
          for (const RawTemporalFact& edge : edge_facts) {
            if (edge.entity_id == source_fact.entity_id &&
                edge.entity_type == step.direction &&
                edge.key_kind == LogicalKeyKind::kExistence) {
              by_edge[std::make_tuple(
                  edge.target_id, edge.edge_type, edge.edge_id)]
                  .push_back(edge);
            }
          }
          for (const auto& entry : by_edge) {
            const auto edge_intervals = DeriveRawTemporalIntervals(
                entry.second, range->first, range->second);
            if (!edge_intervals.ok()) return edge_intervals.status();
            const uint64_t target_id = std::get<0>(entry.first);
            TemporalScanSpec target_spec{
                0, snapshot_seq, context.options.batch_capacity};
            target_spec.entity_type = EntityType::Vertex;
            target_spec.key_kind = LogicalKeyKind::kExistence;
            target_spec.exact_key = LogicalKey::VertexExistence(target_id);
            std::vector<RawTemporalFact> target_facts;
            std::shared_ptr<QueryMemoryLease> target_fact_lease;
            status = CollectRawFacts(
                std::move(target_spec), &target_facts,
                &target_fact_lease, &timer);
            if (!status.ok()) return status;
            const auto target_intervals = DeriveRawTemporalIntervals(
                target_facts, range->first, range->second);
            if (!target_intervals.ok()) return target_intervals.status();
            const auto expanded = ExpandRawIntervalHop(
                source_intervals, edge_intervals.ValueOrDie(),
                target_intervals.ValueOrDie(), range->first, range->second);
            if (!expanded.ok()) return expanded.status();
            for (const RawRangeExpandRow& row : expanded.ValueOrDie()) {
              const LogicalKey identity = LogicalKey::EdgeExistence(
                  row.edge.entity_id, row.edge.target_id,
                  row.edge.edge_type, row.edge.edge_id,
                  row.edge.entity_type);
              if (chain.visited_edges.count(identity) != 0) continue;
              PendingRangeExpand::ChainRow extended = chain;
              extended.nodes.push_back(row.target);
              extended.edges.push_back(row.edge);
              extended.visited_edges.insert(identity);
              extended.valid_from = row.valid_from;
              extended.valid_to = row.valid_to;
              if (hop >= step.min_hops) {
                PendingRangeExpand::ChainRow complete = extended;
                complete.segment_edge_ends.push_back(
                    static_cast<uint32_t>(complete.edges.size()));
                if (segment_index + 1 ==
                    plan->expand_steps().size()) {
                  const Status retained = retain_output(complete);
                  if (!retained.ok()) return retained;
                } else {
                  next_segment.push_back(std::move(complete));
                }
                ++completed_this_hop;
              }
              if (hop < step.max_hops) {
                next_hop.push_back(std::move(extended));
              }
            }
          }
        }
        RecordOperatorOutputAt(
            PhysicalOperatorKind::kExpand,
            static_cast<uint32_t>(segment_index),
            next_hop.size() + completed_this_hop);
        RecordPathFrontierHop(
            stats, segment_frontier.size(), next_hop.size(),
            completed_this_hop,
            ChoosePathFrontierPartitionCount(
                segment_frontier.size(), context.options.batch_capacity),
            segment_frontier.size());
        timer.Finish();
        segment_frontier = std::move(next_hop);
      }
      frontier = std::move(next_segment);
    }
    return Status::OK();
  }

  Status BuildRangeExpandChains(
      const RangeOutputRow& source,
      std::vector<PendingRangeExpand::ChainRow>* output,
      std::shared_ptr<QueryMemoryLease>* output_lease,
      std::unique_ptr<QuerySpillFile>* output_spill) {
    const bool mixed = plan->expand_steps().size() > 1 &&
        std::any_of(plan->expand_steps().begin(),
                    plan->expand_steps().end(),
                    [](const PhysicalExpandSpec& step) {
                      return step.path_slot.value != 0;
                    });
    if (mixed) {
      return BuildSegmentedRangeExpandChains(
          source, output, output_lease, output_spill);
    }
    const bool variable = plan->expand().has_value() && plan->expand()->max_hops > 1;
    if (output == nullptr || output_lease == nullptr || output_spill == nullptr ||
        (!variable && plan->expand_steps().size() < 2)) {
      return Status::Corruption("physical runtime", "multi-hop range expand state is incomplete");
    }
    const auto range = plan->valid_time_range();
    if (!range.has_value()) {
      return Status::Corruption("physical runtime", "multi-hop range bounds are absent");
    }
    const RawTemporalFact root{
        EntityType::Vertex, LogicalKeyKind::kExistence, source.event.entity_id, 0, 0, 0, 0,
        source.event.valid_from, source.event.commit_seq,
        static_cast<TemporalOperation>(source.event.operation)};
    const auto new_spill = [this]()
        -> StatusOr<std::unique_ptr<QuerySpillFile>> {
      const std::string directory = context.options.spill_directory.empty()
          ? "/tmp" : context.options.spill_directory;
      auto spill = std::make_unique<QuerySpillFile>(
          directory, context.options.cancellation,
          context.options.spill_resource_extensions,
          context.options.memory_account,
          [stats = context.options.execution_stats](uint64_t bytes) {
            if (stats) stats->path_frontier_spill_bytes += bytes;
          });
      const Status opened = spill->Open();
      if (!opened.ok()) return opened;
      if (context.options.execution_stats) {
        ++context.options.execution_stats->path_frontier_spill_starts;
      }
      return spill;
    };
    const auto append_spilled = [](QuerySpillFile* spill,
                                   const PendingRangeExpand::ChainRow& chain) {
      if (spill == nullptr) {
        return Status::InvalidArgument(
            "physical runtime", "missing range frontier spill");
      }
      auto encoded = EncodeRangeChain(chain);
      if (!encoded.ok()) return encoded.status();
      return spill->Append(encoded.ValueOrDie());
    };
    const auto begin_spill = [&](
        std::vector<PendingRangeExpand::ChainRow>* states,
        std::shared_ptr<QueryMemoryLease>* lease,
        std::unique_ptr<QuerySpillFile>* spill) -> Status {
      auto opened = new_spill();
      if (!opened.ok()) return opened.status();
      *spill = std::move(opened).ConsumeValueOrDie();
      for (const PendingRangeExpand::ChainRow& state : *states) {
        const Status appended = append_spilled(spill->get(), state);
        if (!appended.ok()) return appended;
      }
      states->clear();
      lease->reset();
      return Status::OK();
    };
    const auto retain_state = [&](
        const PendingRangeExpand::ChainRow& state,
        std::vector<PendingRangeExpand::ChainRow>* states,
        std::shared_ptr<QueryMemoryLease>* lease,
        std::unique_ptr<QuerySpillFile>* spill) -> Status {
      if (*spill) return append_spilled(spill->get(), state);
      if (context.options.memory_account &&
          context.options.memory_account->ShouldSpill()) {
        const Status started = begin_spill(states, lease, spill);
        if (!started.ok()) return started;
        return append_spilled(spill->get(), state);
      }
      if (!*lease) {
        *lease = std::make_shared<QueryMemoryLease>(
            context.options.memory_account, 0);
      }
      const Status reserved =
          (*lease)->ReserveAdditional(RangeChainBytes(state));
      if (!reserved.ok()) {
        const Status started = begin_spill(states, lease, spill);
        if (!started.ok()) return started;
        return append_spilled(spill->get(), state);
      }
      states->push_back(state);
      return Status::OK();
    };

    std::vector<PendingRangeExpand::ChainRow> chains;
    auto chains_lease = std::make_shared<QueryMemoryLease>(
        context.options.memory_account, 0);
    std::unique_ptr<QuerySpillFile> chains_spill;
    PendingRangeExpand::ChainRow initial{
        {root}, {}, {}, {}, source.event.valid_from, source.valid_to};
    Status status = chains_lease->ReserveAdditional(RangeChainBytes(initial));
    if (!status.ok()) return status;
    chains.push_back(std::move(initial));
    uint64_t chain_count = 1;
    const uint32_t hop_limit = variable ? plan->expand()->max_hops
                                        : static_cast<uint32_t>(plan->expand_steps().size());
    for (uint32_t hop = 0; hop < hop_limit && chain_count != 0; ++hop) {
      const size_t operator_occurrence = variable ? 0 : hop;
      AddOperatorInputAt(PhysicalOperatorKind::kExpand,
                         operator_occurrence, chain_count);
      const auto stats = context.options.execution_stats;
      ScopedOperatorRuntimeTimer timer(
          stats ? stats->operator_runtime : nullptr,
          OperatorKeyForOccurrence(
              PhysicalOperatorKind::kExpand, operator_occurrence));
      const PhysicalExpandSpec& step =
          variable ? *plan->expand() : plan->expand_steps()[hop];
      const uint32_t partition_count = variable
          ? ChoosePathFrontierPartitionCount(
                chain_count, context.options.batch_capacity)
          : 1;
      const size_t max_partition_size = static_cast<size_t>(
          (chain_count + partition_count - 1) / partition_count);
      std::vector<PendingRangeExpand::ChainRow> next;
      auto next_lease = std::make_shared<QueryMemoryLease>(
          context.options.memory_account, 0);
      std::unique_ptr<QuerySpillFile> next_spill;
      uint64_t next_count = 0;
      size_t completed_this_hop = 0;
      const auto expand_chain = [&](
          const PendingRangeExpand::ChainRow& chain) -> Status {
        if (chain.nodes.empty()) {
          return Status::Corruption(
              "physical runtime", "multi-hop range chain lacks source");
        }
        const RawTemporalFact& source_fact = chain.nodes.back();
        const std::vector<RawTemporalInterval> source_intervals{
            RawTemporalInterval{
                source_fact, chain.valid_from, chain.valid_to}};
        auto source_ids = std::make_shared<std::set<uint64_t>>();
        source_ids->insert(source_fact.entity_id);
        TemporalScanSpec edge_spec{
            0, snapshot_seq, context.options.batch_capacity};
        edge_spec.entity_type = step.direction;
        edge_spec.key_kind = LogicalKeyKind::kExistence;
        edge_spec.edge_type = step.edge_type;
        edge_spec.allowed_candidate_entity_ids = std::move(source_ids);
        std::vector<RawTemporalFact> edge_facts;
        std::shared_ptr<QueryMemoryLease> edge_fact_lease;
        Status expanded_status = CollectRawFacts(
            std::move(edge_spec), &edge_facts, &edge_fact_lease, &timer);
        if (!expanded_status.ok()) return expanded_status;
        std::map<std::tuple<uint64_t, uint16_t, uint64_t>,
                 std::vector<RawTemporalFact>> by_edge;
        for (const RawTemporalFact& edge : edge_facts) {
          if (edge.entity_id == source_fact.entity_id &&
              edge.entity_type == step.direction &&
              edge.key_kind == LogicalKeyKind::kExistence) {
            by_edge[std::make_tuple(
                edge.target_id, edge.edge_type, edge.edge_id)].push_back(edge);
          }
        }
        for (const auto& entry : by_edge) {
          const auto edge_intervals = DeriveRawTemporalIntervals(
              entry.second, range->first, range->second);
          if (!edge_intervals.ok()) return edge_intervals.status();
          const uint64_t target_id = std::get<0>(entry.first);
          TemporalScanSpec target_spec{
              0, snapshot_seq, context.options.batch_capacity};
          target_spec.entity_type = EntityType::Vertex;
          target_spec.key_kind = LogicalKeyKind::kExistence;
          target_spec.exact_key = LogicalKey::VertexExistence(target_id);
          std::vector<RawTemporalFact> target_facts;
          std::shared_ptr<QueryMemoryLease> target_fact_lease;
          expanded_status = CollectRawFacts(
              std::move(target_spec), &target_facts,
              &target_fact_lease, &timer);
          if (!expanded_status.ok()) return expanded_status;
          const auto target_intervals = DeriveRawTemporalIntervals(
              target_facts, range->first, range->second);
          if (!target_intervals.ok()) return target_intervals.status();
          const auto expanded = ExpandRawIntervalHop(
              source_intervals, edge_intervals.ValueOrDie(),
              target_intervals.ValueOrDie(), range->first, range->second);
          if (!expanded.ok()) return expanded.status();
          for (const RawRangeExpandRow& row : expanded.ValueOrDie()) {
            const LogicalKey identity = LogicalKey::EdgeExistence(
                row.edge.entity_id, row.edge.target_id, row.edge.edge_type,
                row.edge.edge_id, row.edge.entity_type);
            if (variable && chain.visited_edges.count(identity) != 0) continue;
            PendingRangeExpand::ChainRow extended = chain;
            extended.nodes.push_back(row.target);
            extended.edges.push_back(row.edge);
            extended.visited_edges.insert(identity);
            extended.valid_from = row.valid_from;
            extended.valid_to = row.valid_to;
            if (variable && hop + 1 >= plan->expand()->min_hops) {
              const Status retained = retain_state(
                  extended, output, output_lease, output_spill);
              if (!retained.ok()) return retained;
              ++completed_this_hop;
            }
            const Status retained = retain_state(
                extended, &next, &next_lease, &next_spill);
            if (!retained.ok()) return retained;
            ++next_count;
          }
        }
        return Status::OK();
      };
      if (chains_spill) {
        status = chains_spill->Seal();
        if (!status.ok()) return status;
        status = chains_spill->Rewind();
        if (!status.ok()) return status;
        for (;;) {
          ResultBatch batch;
          status = chains_spill->Next(&batch);
          if (status.IsNotFound()) break;
          if (!status.ok()) return status;
          auto chain = DecodeRangeChain(batch);
          if (!chain.ok()) return chain.status();
          status = expand_chain(chain.ValueOrDie());
          if (!status.ok()) return status;
        }
      } else {
        for (const PendingRangeExpand::ChainRow& chain : chains) {
          status = expand_chain(chain);
          if (!status.ok()) return status;
        }
      }
      if (next_spill) {
        status = next_spill->Seal();
        if (!status.ok()) return status;
      }
      if (next_count != 0) {
        RecordOperatorOutputAt(PhysicalOperatorKind::kExpand,
                               operator_occurrence, next_count);
      }
      if (variable) {
        RecordPathFrontierHop(
            stats, chain_count, next_count, completed_this_hop,
            partition_count, max_partition_size);
      }
      timer.Finish();
      chains = std::move(next);
      chains_lease = std::move(next_lease);
      chains_spill = std::move(next_spill);
      chain_count = next_count;
    }
    if (!variable) {
      *output = std::move(chains);
      *output_lease = std::move(chains_lease);
      *output_spill = std::move(chains_spill);
    }
    if (*output_spill) {
      status = (*output_spill)->Seal();
      if (!status.ok()) return status;
    }
    return Status::OK();
  }

  Status PublishRangeExpandChains(std::vector<PendingRangeExpand::ChainRow> rows) {
    if (rows.empty()) return Status::OK();
    const std::vector<PhysicalPropertySlot> properties = RangeExpandDemandedProperties();
    const auto aligned = AlignRangeExpandChainPropertyBoundaries(rows, properties);
    if (!aligned.ok()) return aligned.status();
    rows = aligned.ValueOrDie();
    const std::vector<PhysicalExpandSpec>& steps = plan->expand_steps();
    const bool variable = plan->expand().has_value() && plan->expand()->max_hops > 1;
    const bool segmented = std::all_of(
        rows.begin(), rows.end(), [&steps](const auto& row) {
          return row.segment_edge_ends.size() == steps.size();
        });
    const uint64_t path_column_count = static_cast<uint64_t>(std::count_if(
        steps.begin(), steps.end(), [](const PhysicalExpandSpec& step) {
          return step.path_slot.value != 0;
        }));
    const uint64_t column_count = 1 + 8 * static_cast<uint64_t>(steps.size()) +
        path_column_count + static_cast<uint64_t>(properties.size());
    const uint64_t charge = sizeof(FlatVector) * column_count +
        static_cast<uint64_t>(rows.size()) * column_count * (sizeof(Value) + sizeof(bool));
    if (context.options.memory_account) {
      const Status reserved = context.options.memory_account->Reserve(charge);
      if (!reserved.ok()) return reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(context.options.memory_account, charge);
    ColumnBatch batch(static_cast<uint32_t>(rows.size()));
    std::map<SlotId, uint32_t> layout;
    const auto append = [&batch, &layout, &lease](SlotId slot, std::vector<Value> values) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::vector<bool>{}, lease));
      if (added.ok()) layout.emplace(slot, batch.column_count() - 1);
      return added;
    };
    const auto append_list = [&batch, &layout, &lease](
        SlotId slot, std::vector<ListValue> values) {
      const Status added = batch.AddVector(std::make_shared<ListVector>(
          std::move(values), std::vector<bool>{}, lease));
      if (added.ok()) layout.emplace(slot, batch.column_count() - 1);
      return added;
    };
    const auto entity_values = [&rows](size_t index) -> StatusOr<std::vector<Value>> {
      std::vector<Value> values;
      values.reserve(rows.size());
      for (const PendingRangeExpand::ChainRow& row : rows) {
        if (row.nodes.size() <= index ||
            row.nodes[index].entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return Status::InvalidArgument("physical runtime", "range chain node identity exceeds Int64");
        }
        values.push_back(Value::Int64(static_cast<int64_t>(row.nodes[index].entity_id)));
      }
      return values;
    };
    const auto first_nodes = entity_values(0);
    if (!first_nodes.ok()) return first_nodes.status();
    Status added = append(steps.front().source_slot, first_nodes.ValueOrDie());
    if (!added.ok()) return added;
    for (size_t hop = 0; hop < steps.size(); ++hop) {
      const PhysicalExpandSpec& step = steps[hop];
      const size_t target_index = segmented
          ? rows.front().segment_edge_ends[hop]
          : variable ? rows.front().nodes.size() - 1 : hop + 1;
      const auto targets = entity_values(target_index);
      if (!targets.ok()) return targets.status();
      added = append(step.target_slot, targets.ValueOrDie());
      if (!added.ok()) return added;
      std::vector<Value> edge_types;
      std::vector<Value> edge_ids;
      std::vector<Value> valid_from;
      std::vector<Value> commit_seqs;
      std::vector<Value> operations;
      std::vector<Value> system_times;
      std::vector<Value> valid_to;
      for (std::vector<Value>* values : {&edge_types, &edge_ids, &valid_from, &commit_seqs,
                                         &operations, &system_times, &valid_to}) {
        values->reserve(rows.size());
      }
      for (const PendingRangeExpand::ChainRow& row : rows) {
        const size_t edge_index = segmented
            ? static_cast<size_t>(row.segment_edge_ends[hop] - 1)
            : variable ? row.edges.size() - 1 : hop;
        if (row.edges.size() <= edge_index ||
            row.edges[edge_index].edge_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            row.edges[edge_index].commit_seq > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return Status::InvalidArgument("physical runtime", "range chain edge identity exceeds Int64");
        }
        const RawTemporalFact& edge = row.edges[edge_index];
        edge_types.push_back(Value::Int32(static_cast<int32_t>(edge.edge_type)));
        edge_ids.push_back(Value::Int64(static_cast<int64_t>(edge.edge_id)));
        valid_from.push_back(Value::Timestamp(row.valid_from));
        commit_seqs.push_back(Value::Int64(static_cast<int64_t>(edge.commit_seq)));
        operations.push_back(Value::Int32(static_cast<int32_t>(edge.operation)));
        if (plan->include_system_time()) {
          if (!timeline || edge.commit_seq == 0 ||
              edge.commit_seq > timeline->physical_times.size()) {
            return Status::Corruption(
                "physical runtime",
                "range chain commit is absent from CommitTimeline");
          }
          system_times.push_back(Value::Timestamp(
              timeline->physical_times[edge.commit_seq - 1]));
        } else {
          system_times.push_back(Value::Timestamp(0));
        }
        valid_to.push_back(Value::Timestamp(row.valid_to));
      }
      for (const auto& column : std::array<std::pair<SlotId, std::vector<Value>*>, 7>{{
               {step.edge_type_slot, &edge_types}, {step.edge_id_slot, &edge_ids},
               {step.valid_from_slot, &valid_from}, {step.commit_seq_slot, &commit_seqs},
               {step.operation_slot, &operations}, {step.system_time_slot, &system_times},
               {step.valid_to_slot, &valid_to}}}) {
        added = append(column.first, std::move(*column.second));
        if (!added.ok()) return added;
      }
      if (step.path_slot.value != 0) {
        std::vector<ListValue> paths;
        paths.reserve(rows.size());
        for (const PendingRangeExpand::ChainRow& row : rows) {
          const size_t begin = segmented
              ? (hop == 0 ? 0
                          : static_cast<size_t>(row.segment_edge_ends[hop - 1]))
              : 0;
          const size_t end = segmented
              ? static_cast<size_t>(row.segment_edge_ends[hop])
              : row.edges.size();
          if (begin >= end || end > row.edges.size()) {
            return Status::Corruption(
                "physical runtime", "range path segment bounds are invalid");
          }
          ListValue path;
          path.element_kind = ListElementKind::kStruct;
          path.structured_elements.reserve(end - begin);
          for (size_t index = begin; index < end; ++index) {
            const RawTemporalFact& edge = row.edges[index];
            path.structured_elements.push_back(StructValue{{
                StructField{"source_id", Value::Int64(
                    static_cast<int64_t>(edge.entity_id))},
                StructField{"target_id", Value::Int64(
                    static_cast<int64_t>(edge.target_id))},
                StructField{"edge_type", Value::Int32(
                    static_cast<int32_t>(edge.edge_type))},
                StructField{"edge_id", Value::Int64(
                    static_cast<int64_t>(edge.edge_id))},
                StructField{"valid_from", Value::Timestamp(row.valid_from)},
                StructField{"commit_seq", Value::Int64(
                    static_cast<int64_t>(edge.commit_seq))},
            }});
          }
          paths.push_back(std::move(path));
        }
        added = append_list(step.path_slot, std::move(paths));
        if (!added.ok()) return added;
      }
    }
    for (const PhysicalPropertySlot& property : properties) {
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(rows.size());
      validity.reserve(rows.size());
      for (const PendingRangeExpand::ChainRow& row : rows) {
        const auto value = LookupRangeExpandChainProperty(row, property, row.valid_from);
        if (!value.ok()) return value.status();
        validity.push_back(value.ValueOrDie().has_value());
        values.push_back(value.ValueOrDie().value_or(Value::Bool(false)));
      }
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::move(validity), lease));
      if (!added.ok()) return added;
      layout.emplace(property.slot, batch.column_count() - 1);
    }
    if (!plan->predicates().empty()) {
      ColumnBatch selected;
      const Status filtered = FilterColumnBatch(
          batch, [this, &layout](const ColumnBatch& candidate, uint32_t row) {
            for (const PhysicalPredicate& predicate : plan->predicates()) {
              const auto column = layout.find(predicate.slot);
              if (column == layout.end()) return false;
              const Value* value = candidate.ValueRefAt(column->second, row);
              if (value == nullptr || !MatchesPredicate(*value, predicate)) return false;
            }
            return true;
          }, &selected);
      if (!filtered.ok()) return filtered;
      return ProjectAndEnqueue(std::move(selected), layout);
    }
    return ProjectAndEnqueue(std::move(batch), layout);
  }

  std::vector<PhysicalPropertySlot> RangeExpandDemandedProperties() const {
    std::vector<PhysicalPropertySlot> properties = plan->predicate_properties();
    for (const PhysicalPropertySlot& property : plan->projection_properties()) {
      const bool known = std::any_of(
          properties.begin(), properties.end(), [&property](const PhysicalPropertySlot& other) {
            return other.slot == property.slot;
          });
      if (!known) properties.push_back(property);
    }
    return properties;
  }

  Status RangeExpandPropertyKey(const RawRangeExpandRow& row,
                                const PhysicalPropertySlot& property,
                                LogicalKey* key) const {
    if (key == nullptr) {
      return Status::InvalidArgument("physical runtime", "range property key output is absent");
    }
    if (!plan->expand().has_value()) {
      return Status::Corruption("physical runtime", "range property lookup lacks expand state");
    }
    const uint16_t column_id = property.column.column_id;
    if (property.binding == plan->expand()->source_binding) {
      *key = LogicalKey::VertexProperty(row.source.entity_id, column_id);
      return Status::OK();
    }
    if (property.binding == plan->expand()->target_binding) {
      *key = LogicalKey::VertexProperty(row.target.entity_id, column_id);
      return Status::OK();
    }
    if (property.binding == plan->expand()->relationship_binding) {
      *key = LogicalKey::EdgeProperty(
          row.edge.entity_id, row.edge.target_id, row.edge.edge_type, row.edge.edge_id,
          column_id, row.edge.entity_type);
      return Status::OK();
    }
    return Status::Corruption("physical runtime", "range property binding is unknown");
  }

  StatusOr<std::optional<Value>> LookupRangeExpandProperty(
      const RawRangeExpandRow& row, const PhysicalPropertySlot& property,
      uint64_t valid_at) {
    LogicalKey key = LogicalKey::VertexExistence(0);
    const Status key_status = RangeExpandPropertyKey(row, property, &key);
    if (!key_status.ok()) return key_status;
    TemporalScanSpec spec{valid_at, snapshot_seq, 1};
    spec.entity_type = key.entity_type();
    spec.key_kind = LogicalKeyKind::kProperty;
    spec.edge_type = key.edge_type();
    spec.column_id = property.column.column_id;
    spec.schema_epoch = property.column.schema_epoch;
    spec.exact_key = key;
    spec.cancellation = context.options.cancellation;
    spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, true, &spec);
    auto opened = OpenPinnedTemporalScan(sources, std::move(spec));
    if (!opened.ok()) return opened.status();
    ColumnBatch batch;
    const Status next = opened.ValueOrDie().NextMorsel(&batch);
    if (next.IsNotFound()) return std::optional<Value>();
    if (!next.ok()) return next;
    const Value* value = batch.ValueRefAt(kValue, 0);
    return value == nullptr ? std::optional<Value>() : std::optional<Value>(*value);
  }

  StatusOr<std::optional<uint64_t>> NextRangeExpandPropertyBoundary(
      const RawRangeExpandRow& row, const PhysicalPropertySlot& property,
      uint64_t after_valid_from) {
    LogicalKey key = LogicalKey::VertexExistence(0);
    const Status key_status = RangeExpandPropertyKey(row, property, &key);
    if (!key_status.ok()) return key_status;
    TemporalScanSpec spec{0, snapshot_seq, 1};
    spec.entity_type = key.entity_type();
    spec.key_kind = LogicalKeyKind::kProperty;
    spec.edge_type = key.edge_type();
    spec.column_id = property.column.column_id;
    spec.schema_epoch = property.column.schema_epoch;
    spec.exact_key = key;
    spec.cancellation = context.options.cancellation;
    spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, true, &spec);
    return FindNextPinnedValidBoundary(
        sources, std::move(spec), key, after_valid_from);
  }

  Status RangeExpandChainPropertyKey(
      const PendingRangeExpand::ChainRow& row, const PhysicalPropertySlot& property,
      LogicalKey* key) const {
    if (key == nullptr) {
      return Status::InvalidArgument("physical runtime", "range chain property key output is absent");
    }
    const std::vector<PhysicalExpandSpec>& steps = plan->expand_steps();
    const bool segmented = row.segment_edge_ends.size() == steps.size();
    for (size_t hop = 0; hop < steps.size(); ++hop) {
      const PhysicalExpandSpec& step = steps[hop];
      if (property.binding == step.source_binding) {
        const size_t node_index = segmented && hop != 0
            ? row.segment_edge_ends[hop - 1] : hop;
        if (row.nodes.size() <= node_index) {
          return Status::Corruption("physical runtime", "range chain source node is absent");
        }
        *key = LogicalKey::VertexProperty(
            row.nodes[node_index].entity_id, property.column.column_id);
        return Status::OK();
      }
      if (property.binding == step.relationship_binding) {
        const size_t edge_index = segmented
            ? static_cast<size_t>(row.segment_edge_ends[hop] - 1)
            : (plan->expand().has_value() && plan->expand()->max_hops > 1
                ? row.edges.size() - 1 : hop);
        if (row.edges.size() <= edge_index) {
          return Status::Corruption("physical runtime", "range chain edge is absent");
        }
        const RawTemporalFact& edge = row.edges[edge_index];
        *key = LogicalKey::EdgeProperty(edge.entity_id, edge.target_id, edge.edge_type,
                                        edge.edge_id, property.column.column_id,
                                        edge.entity_type);
        return Status::OK();
      }
      if (property.binding == step.target_binding) {
        const size_t node_index = segmented
            ? static_cast<size_t>(row.segment_edge_ends[hop])
            : (plan->expand().has_value() && plan->expand()->max_hops > 1
                ? row.nodes.size() - 1 : hop + 1);
        if (row.nodes.size() <= node_index) {
          return Status::Corruption("physical runtime", "range chain target node is absent");
        }
        *key = LogicalKey::VertexProperty(
            row.nodes[node_index].entity_id, property.column.column_id);
        return Status::OK();
      }
    }
    return Status::Corruption("physical runtime", "range chain property binding is unknown");
  }

  StatusOr<std::optional<Value>> LookupRangeExpandChainProperty(
      const PendingRangeExpand::ChainRow& row, const PhysicalPropertySlot& property,
      uint64_t valid_at) {
    LogicalKey key = LogicalKey::VertexExistence(0);
    const Status key_status = RangeExpandChainPropertyKey(row, property, &key);
    if (!key_status.ok()) return key_status;
    TemporalScanSpec spec{valid_at, snapshot_seq, 1};
    spec.entity_type = key.entity_type();
    spec.key_kind = LogicalKeyKind::kProperty;
    spec.edge_type = key.edge_type();
    spec.column_id = property.column.column_id;
    spec.schema_epoch = property.column.schema_epoch;
    spec.exact_key = key;
    spec.cancellation = context.options.cancellation;
    spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, true, &spec);
    auto opened = OpenPinnedTemporalScan(sources, std::move(spec));
    if (!opened.ok()) return opened.status();
    ColumnBatch batch;
    const Status next = opened.ValueOrDie().NextMorsel(&batch);
    if (next.IsNotFound()) return std::optional<Value>();
    if (!next.ok()) return next;
    const Value* value = batch.ValueRefAt(kValue, 0);
    return value == nullptr ? std::optional<Value>() : std::optional<Value>(*value);
  }

  StatusOr<std::optional<uint64_t>> NextRangeExpandChainPropertyBoundary(
      const PendingRangeExpand::ChainRow& row, const PhysicalPropertySlot& property,
      uint64_t after_valid_from) {
    LogicalKey key = LogicalKey::VertexExistence(0);
    const Status key_status = RangeExpandChainPropertyKey(row, property, &key);
    if (!key_status.ok()) return key_status;
    TemporalScanSpec spec{0, snapshot_seq, 1};
    spec.entity_type = key.entity_type();
    spec.key_kind = LogicalKeyKind::kProperty;
    spec.edge_type = key.edge_type();
    spec.column_id = property.column.column_id;
    spec.schema_epoch = property.column.schema_epoch;
    spec.exact_key = key;
    spec.cancellation = context.options.cancellation;
    spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, true, &spec);
    return FindNextPinnedValidBoundary(
        sources, std::move(spec), key, after_valid_from);
  }

  StatusOr<std::vector<PendingRangeExpand::ChainRow>>
  AlignRangeExpandChainPropertyBoundaries(
      const std::vector<PendingRangeExpand::ChainRow>& rows,
      const std::vector<PhysicalPropertySlot>& properties) {
    if (properties.empty()) return rows;
    std::vector<PendingRangeExpand::ChainRow> aligned;
    for (const PendingRangeExpand::ChainRow& source : rows) {
      uint64_t current = source.valid_from;
      while (current < source.valid_to) {
        uint64_t next = source.valid_to;
        for (const PhysicalPropertySlot& property : properties) {
          const auto boundary = NextRangeExpandChainPropertyBoundary(source, property, current);
          if (!boundary.ok()) return boundary.status();
          if (boundary.ValueOrDie().has_value() &&
              *boundary.ValueOrDie() > current && *boundary.ValueOrDie() < next) {
            next = *boundary.ValueOrDie();
          }
        }
        if (next <= current) {
          return Status::Corruption(
              "physical runtime", "range chain property boundary made no progress");
        }
        PendingRangeExpand::ChainRow segment = source;
        segment.valid_from = current;
        segment.valid_to = next;
        aligned.push_back(std::move(segment));
        current = next;
      }
    }
    return aligned;
  }

  StatusOr<std::vector<RawRangeExpandRow>> AlignRangeExpandPropertyBoundaries(
      const std::vector<RawRangeExpandRow>& rows,
      const std::vector<PhysicalPropertySlot>& properties) {
    if (properties.empty()) return rows;
    std::vector<RawRangeExpandRow> aligned;
    for (const RawRangeExpandRow& source : rows) {
      uint64_t current = source.valid_from;
      while (current < source.valid_to) {
        uint64_t next = source.valid_to;
        for (const PhysicalPropertySlot& property : properties) {
          const auto boundary = NextRangeExpandPropertyBoundary(source, property, current);
          if (!boundary.ok()) return boundary.status();
          if (boundary.ValueOrDie().has_value() &&
              *boundary.ValueOrDie() > current &&
              *boundary.ValueOrDie() < next) {
            next = *boundary.ValueOrDie();
          }
        }
        if (next <= current) {
          return Status::Corruption(
              "physical runtime", "range expand property boundary made no progress");
        }
        RawRangeExpandRow segment = source;
        segment.valid_from = current;
        segment.valid_to = next;
        aligned.push_back(std::move(segment));
        current = next;
      }
    }
    return aligned;
  }

  Status PublishRangeExpandRows(std::vector<RawRangeExpandRow> rows) {
    if (rows.empty()) return Status::OK();
    const std::vector<PhysicalPropertySlot> properties = RangeExpandDemandedProperties();
    const auto aligned = AlignRangeExpandPropertyBoundaries(rows, properties);
    if (!aligned.ok()) return aligned.status();
    rows = aligned.ValueOrDie();
    const uint64_t column_count = 11 + properties.size();
    const uint64_t charge = sizeof(FlatVector) * column_count +
        static_cast<uint64_t>(rows.size()) * column_count *
            (sizeof(Value) + sizeof(bool));
    if (context.options.memory_account) {
      const Status reserved = context.options.memory_account->Reserve(charge);
      if (!reserved.ok()) return reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(context.options.memory_account, charge);
    std::vector<Value> entity_types;
    std::vector<Value> source_ids;
    std::vector<Value> target_ids;
    std::vector<Value> edge_ids;
    std::vector<Value> edge_types;
    std::vector<Value> column_ids;
    std::vector<Value> valid_from;
    std::vector<Value> commit_seqs;
    std::vector<Value> operations;
    std::vector<Value> system_times;
    std::vector<Value> valid_to;
    for (std::vector<Value>* values : {&entity_types, &source_ids, &target_ids, &edge_ids,
                                       &edge_types, &column_ids, &valid_from, &commit_seqs,
                                       &operations, &system_times, &valid_to}) {
      values->reserve(rows.size());
    }
    for (const RawRangeExpandRow& row : rows) {
      if (row.source.entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
          row.target.entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
          row.edge.edge_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
          row.edge.commit_seq > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument("physical runtime", "range expand identity exceeds Int64");
      }
      entity_types.push_back(Value::Int32(static_cast<int32_t>(plan->expand()->direction)));
      source_ids.push_back(Value::Int64(static_cast<int64_t>(row.source.entity_id)));
      target_ids.push_back(Value::Int64(static_cast<int64_t>(row.target.entity_id)));
      edge_ids.push_back(Value::Int64(static_cast<int64_t>(row.edge.edge_id)));
      edge_types.push_back(Value::Int32(static_cast<int32_t>(row.edge.edge_type)));
      column_ids.push_back(Value::Int32(0));
      valid_from.push_back(Value::Timestamp(row.valid_from));
      commit_seqs.push_back(Value::Int64(static_cast<int64_t>(row.edge.commit_seq)));
      operations.push_back(Value::Int32(static_cast<int32_t>(row.edge.operation)));
      if (plan->include_system_time()) {
        if (!timeline || row.edge.commit_seq == 0 ||
            row.edge.commit_seq > timeline->physical_times.size()) {
          return Status::Corruption(
              "physical runtime",
              "range expand commit is absent from CommitTimeline");
        }
        system_times.push_back(Value::Timestamp(
            timeline->physical_times[row.edge.commit_seq - 1]));
      } else {
        system_times.push_back(Value::Timestamp(0));
      }
      valid_to.push_back(Value::Timestamp(row.valid_to));
    }
    ColumnBatch batch(static_cast<uint32_t>(rows.size()));
    for (std::vector<Value>* values : {&entity_types, &source_ids, &target_ids, &edge_ids,
                                       &edge_types, &column_ids, &valid_from, &commit_seqs,
                                       &operations, &system_times, &valid_to}) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(*values), std::vector<bool>{}, lease));
      if (!added.ok()) return added;
    }
    std::map<SlotId, uint32_t> layout{
        {plan->expand()->source_slot, 1}, {plan->expand()->target_slot, 2},
        {plan->expand()->edge_id_slot, 3}, {plan->expand()->edge_type_slot, 4},
        {plan->expand()->valid_from_slot, 6}, {plan->expand()->commit_seq_slot, 7},
        {plan->expand()->operation_slot, 8}, {plan->expand()->system_time_slot, 9},
        {plan->expand()->valid_to_slot, 10}};
    for (const PhysicalPropertySlot& property : properties) {
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(rows.size());
      validity.reserve(rows.size());
      for (const RawRangeExpandRow& row : rows) {
        const auto value = LookupRangeExpandProperty(row, property, row.valid_from);
        if (!value.ok()) return value.status();
        validity.push_back(value.ValueOrDie().has_value());
        values.push_back(value.ValueOrDie().value_or(Value::Bool(false)));
      }
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::move(validity), lease));
      if (!added.ok()) return added;
      layout[property.slot] = batch.column_count() - 1;
    }
    RecordOperatorOutput(PhysicalOperatorKind::kExpand, batch.row_count());
    if (!plan->predicates().empty()) {
      const uint64_t filter_input_rows = batch.row_count();
      ColumnBatch selected;
      const auto filter_start = std::chrono::steady_clock::now();
      const Status filtered = FilterColumnBatch(
          batch, [this, &layout](const ColumnBatch& candidate, uint32_t row) {
            for (const PhysicalPredicate& predicate : plan->predicates()) {
              const auto column = layout.find(predicate.slot);
              if (column == layout.end()) return false;
              const Value* value = candidate.ValueRefAt(column->second, row);
              if (value == nullptr || !MatchesPredicate(*value, predicate)) return false;
            }
            return true;
          }, &selected);
      AddOperatorCpu(PhysicalOperatorKind::kFilter,
                     ElapsedNs(filter_start));
      if (!filtered.ok()) return filtered;
      RecordOperatorBatch(PhysicalOperatorKind::kFilter, filter_input_rows,
                          selected.row_count());
      return ProjectAndEnqueue(std::move(selected), layout);
    }
    return ProjectAndEnqueue(std::move(batch), layout);
  }

  Status BeginRangeExpand(std::vector<RangeOutputRow> rows) {
    if (rows.empty()) return Status::OK();
    if (pending_range_expand.has_value()) {
      return Status::Corruption("physical runtime", "range expand work overlaps");
    }
    if (plan->expand_steps().size() <= 1) {
      AddOperatorInput(PhysicalOperatorKind::kExpand, rows.size());
    }
    pending_range_expand.emplace(PendingRangeExpand{std::move(rows)});
    return Status::OK();
  }

  Status ContinueRangeExpand() {
    if (!pending_range_expand.has_value()) {
      return Status::Corruption("physical runtime", "range expand work is absent");
    }
    PendingRangeExpand& pending = *pending_range_expand;
    const bool multi_hop = plan->expand_steps().size() > 1 ||
        (plan->expand().has_value() && plan->expand()->max_hops > 1);
    const auto stats = context.options.execution_stats;
    ScopedOperatorRuntimeTimer timer(
        stats ? stats->operator_runtime : nullptr,
        multi_hop ? std::optional<OperatorRuntimeKey>{}
                  : OperatorKeyFor(PhysicalOperatorKind::kExpand));
    const auto has_expanded = [&pending, multi_hop] {
      return multi_hop
          ? pending.expanded_chain_spill != nullptr ||
                pending.expanded_index < pending.expanded_chains.size()
          : pending.expanded_index < pending.expanded.size();
    };
    while (!has_expanded() && pending.source_index < pending.sources.size()) {
      pending.expanded.clear();
      pending.expanded_chains.clear();
      pending.expanded_chain_lease.reset();
      pending.expanded_chain_spill.reset();
      pending.expanded_chain_spill_rewound = false;
      pending.expanded_index = 0;
      const Status expanded = multi_hop
          ? BuildRangeExpandChains(pending.sources[pending.source_index++],
                                   &pending.expanded_chains,
                                   &pending.expanded_chain_lease,
                                   &pending.expanded_chain_spill)
          : BuildRangeExpandRows(
                pending.sources[pending.source_index++], &pending.expanded,
                &timer);
      if (!expanded.ok()) return expanded;
    }
    if (!has_expanded()) {
      pending_range_expand.reset();
      return Status::OK();
    }
    if (multi_hop) {
      std::vector<PendingRangeExpand::ChainRow> output;
      output.reserve(context.options.batch_capacity);
      auto output_lease = std::make_shared<QueryMemoryLease>(
          context.options.memory_account, 0);
      if (pending.expanded_chain_spill) {
        if (!pending.expanded_chain_spill_rewound) {
          const Status rewound = pending.expanded_chain_spill->Rewind();
          if (!rewound.ok()) return rewound;
          pending.expanded_chain_spill_rewound = true;
        }
        while (output.size() < context.options.batch_capacity) {
          ResultBatch batch;
          const Status next = pending.expanded_chain_spill->Next(&batch);
          if (next.IsNotFound()) {
            pending.expanded_chain_spill.reset();
            break;
          }
          if (!next.ok()) return next;
          auto chain = DecodeRangeChain(batch);
          if (!chain.ok()) return chain.status();
          const Status reserved = output_lease->ReserveAdditional(
              RangeChainBytes(chain.ValueOrDie()));
          if (!reserved.ok()) return reserved;
          output.push_back(std::move(chain).ConsumeValueOrDie());
        }
      } else {
        const size_t remaining =
            pending.expanded_chains.size() - pending.expanded_index;
        const size_t count = std::min<size_t>(
            remaining, context.options.batch_capacity);
        for (size_t index = 0; index < count; ++index) {
          const PendingRangeExpand::ChainRow& chain =
              pending.expanded_chains[pending.expanded_index + index];
          const Status reserved = output_lease->ReserveAdditional(
              RangeChainBytes(chain));
          if (!reserved.ok()) return reserved;
          output.push_back(chain);
        }
        pending.expanded_index += count;
      }
      if (output.empty()) return Status::OK();
      timer.Finish();
      return PublishRangeExpandChains(std::move(output));
    }
    const size_t remaining = pending.expanded.size() - pending.expanded_index;
    const size_t count = std::min<size_t>(
        remaining, context.options.batch_capacity);
    std::vector<RawRangeExpandRow> output;
    output.reserve(count);
    output.insert(output.end(), pending.expanded.begin() + pending.expanded_index,
                  pending.expanded.begin() + pending.expanded_index + count);
    pending.expanded_index += count;
    timer.Finish();
    return PublishRangeExpandRows(std::move(output));
  }

  Status PublishRangeRows(std::vector<RangeOutputRow> rows) {
    if (rows.empty()) return Status::OK();
    const uint64_t derived_intervals = rows.size();
    if (context.options.execution_stats) {
      context.options.execution_stats->candidate_intervals = SaturatingAdd(
          context.options.execution_stats->candidate_intervals,
          derived_intervals);
    }
    if (plan->expand().has_value()) {
      RecordOperatorBatch(PhysicalOperatorKind::kIntervalAlign,
                          derived_intervals, derived_intervals, false,
                          derived_intervals, derived_intervals);
      RecordOperatorBatch(PhysicalOperatorKind::kTemporalCoalesce,
                          derived_intervals, derived_intervals, false,
                          derived_intervals, derived_intervals);
      if (context.options.execution_stats) {
        context.options.execution_stats->output_intervals = SaturatingAdd(
            context.options.execution_stats->output_intervals,
            derived_intervals);
      }
      return BeginRangeExpand(std::move(rows));
    }
    if (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeRange &&
        (!plan->predicate_properties().empty() ||
         !plan->projection_properties().empty())) {
      const auto valid_time_range = plan->valid_time_range();
      if (!valid_time_range.has_value()) {
        return Status::Corruption(
            "physical runtime", "range property alignment lost its bounds");
      }
      std::vector<PhysicalPropertySlot> properties = plan->predicate_properties();
      for (const PhysicalPropertySlot& property : plan->projection_properties()) {
        const bool known = std::any_of(
            properties.begin(), properties.end(), [&property](const PhysicalPropertySlot& other) {
              return other.slot == property.slot;
            });
        if (!known) properties.push_back(property);
      }
      std::vector<RangeOutputRow> split;
      const bool provenance_demanded = plan->include_system_time() ||
          std::any_of(plan->projections().begin(), plan->projections().end(),
              [](const PhysicalExpression& expression) {
                return expression.referenced_slot == SlotId{3} ||
                    expression.referenced_slot == SlotId{4};
              });
      const auto property_value_at = [this](const PhysicalPropertySlot& property,
                                            uint64_t entity_id, uint64_t valid_time)
          -> StatusOr<std::optional<Value>> {
        TemporalScanSpec point_spec = scan_spec;
        point_spec.raw_events = false;
        point_spec.retain_selected_tombstone = false;
        point_spec.valid_time = valid_time;
        point_spec.batch_capacity = 1;
        point_spec.entity_type = EntityType::Vertex;
        point_spec.key_kind = LogicalKeyKind::kProperty;
        point_spec.column_id = property.column.column_id;
        point_spec.schema_epoch = property.column.schema_epoch;
        point_spec.exact_key = LogicalKey::VertexProperty(
            entity_id, property.column.column_id);
        auto opened = OpenPinnedTemporalScan(sources, point_spec);
        if (!opened.ok()) return opened.status();
        ColumnBatch batch;
        const Status next = opened.ValueOrDie().NextMorsel(&batch);
        if (next.IsNotFound()) return std::optional<Value>();
        if (!next.ok()) return next;
        const Value* value = batch.ValueRefAt(kValue, 0);
        return value == nullptr ? std::optional<Value>()
                                : std::optional<Value>(*value);
      };
      for (const RangeOutputRow& source : rows) {
        RangeOutputRow current = source;
        uint64_t coalesced_from = source.event.valid_from;
        for (;;) {
          uint64_t next = current.valid_to;
          for (const PhysicalPropertySlot& property : properties) {
            TemporalScanSpec boundary_spec = scan_spec;
            boundary_spec.entity_type = EntityType::Vertex;
            boundary_spec.key_kind = LogicalKeyKind::kProperty;
            boundary_spec.column_id = property.column.column_id;
            boundary_spec.schema_epoch = property.column.schema_epoch;
            boundary_spec.exact_key = LogicalKey::VertexProperty(
                current.event.entity_id, property.column.column_id);
            const auto boundary = FindNextPinnedValidBoundary(
                sources, boundary_spec, *boundary_spec.exact_key,
                current.event.valid_from);
            if (!boundary.ok()) return boundary.status();
            if (boundary.ValueOrDie().has_value() &&
                *boundary.ValueOrDie() < next) {
              next = *boundary.ValueOrDie();
            }
          }
          if (next <= current.event.valid_from) {
            return Status::Corruption("physical runtime", "range property boundary made no progress");
          }
          bool changed = provenance_demanded || next == source.valid_to;
          if (!changed) {
            for (const PhysicalPropertySlot& property : properties) {
              const auto before = property_value_at(
                  property, current.event.entity_id, current.event.valid_from);
              if (!before.ok()) return before.status();
              const auto after = property_value_at(
                  property, current.event.entity_id, next);
              if (!after.ok()) return after.status();
              if (before.ValueOrDie() != after.ValueOrDie()) {
                changed = true;
                break;
              }
            }
          }
          if (!changed) {
            current.event.valid_from = next;
            current.valid_to = source.valid_to;
            continue;
          }
          current.valid_to = next;
          current.event.valid_from = coalesced_from;
          split.push_back(current);
          if (next == source.valid_to || next >= valid_time_range->second) {
            break;
          }
          coalesced_from = next;
          current.event.valid_from = next;
          current.valid_to = source.valid_to;
        }
      }
      rows = std::move(split);
    }
    RecordOperatorBatch(PhysicalOperatorKind::kIntervalAlign,
                        derived_intervals, rows.size(), false,
                        derived_intervals, rows.size());
    RecordOperatorBatch(PhysicalOperatorKind::kTemporalCoalesce,
                        rows.size(), rows.size(), false,
                        rows.size(), rows.size());
    if (context.options.execution_stats) {
      context.options.execution_stats->output_intervals = SaturatingAdd(
          context.options.execution_stats->output_intervals, rows.size());
    }
    const uint64_t column_count = 10 + (plan->include_system_time() ? 1 : 0);
    const uint64_t charge = sizeof(FlatVector) * column_count +
        static_cast<uint64_t>(rows.size()) * column_count *
            (sizeof(Value) + sizeof(bool));
    if (context.options.memory_account) {
      const Status reserved = context.options.memory_account->Reserve(charge);
      if (!reserved.ok()) return reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(
        context.options.memory_account, charge);
    std::vector<Value> entity_types;
    std::vector<Value> entity_ids;
    std::vector<Value> target_ids;
    std::vector<Value> edge_ids;
    std::vector<Value> edge_types;
    std::vector<Value> column_ids;
    std::vector<Value> valid_from;
    std::vector<Value> commit_seq;
    std::vector<Value> operations;
    std::vector<Value> valid_to;
    std::vector<Value> system_time;
    for (std::vector<Value>* values :
         {&entity_types, &entity_ids, &target_ids, &edge_ids, &edge_types,
          &column_ids, &valid_from, &commit_seq, &operations, &valid_to}) {
      values->reserve(rows.size());
    }
    if (plan->include_system_time()) system_time.reserve(rows.size());
    for (const RangeOutputRow& row : rows) {
      if (row.event.entity_id >
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
          row.event.commit_seq >
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument(
            "physical runtime", "range identity exceeds Int64");
      }
      entity_types.push_back(Value::Int32(static_cast<int32_t>(EntityType::Vertex)));
      entity_ids.push_back(Value::Int64(
          static_cast<int64_t>(row.event.entity_id)));
      target_ids.push_back(Value::Int64(0));
      edge_ids.push_back(Value::Int64(0));
      edge_types.push_back(Value::Int32(0));
      column_ids.push_back(Value::Int32(0));
      valid_from.push_back(Value::Timestamp(row.event.valid_from));
      commit_seq.push_back(Value::Int64(
          static_cast<int64_t>(row.event.commit_seq)));
      operations.push_back(Value::Int32(row.event.operation));
      valid_to.push_back(Value::Timestamp(row.valid_to));
      if (plan->include_system_time()) {
        if (!timeline || row.event.commit_seq == 0 ||
            row.event.commit_seq > timeline->physical_times.size()) {
          return Status::Corruption(
              "physical runtime",
              "range commit is absent from CommitTimeline");
        }
        system_time.push_back(Value::Timestamp(
            timeline->physical_times[row.event.commit_seq - 1]));
      }
    }
    ColumnBatch batch(static_cast<uint32_t>(rows.size()));
    for (std::vector<Value>* values :
         {&entity_types, &entity_ids, &target_ids, &edge_ids, &edge_types,
          &column_ids, &valid_from, &commit_seq, &operations, &valid_to}) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(*values), std::vector<bool>{}, lease));
      if (!added.ok()) return added;
    }
    if (plan->include_system_time()) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(system_time), std::vector<bool>{}, lease));
      if (!added.ok()) return added;
      RecordOperatorBatch(PhysicalOperatorKind::kMetadataProject,
                          batch.row_count(), batch.row_count());
    }
    std::map<SlotId, uint32_t> layout{
        {SlotId{1}, kEntityId}, {SlotId{2}, kValidFrom},
        {SlotId{3}, kCommitSeq}, {SlotId{4}, kOperation}};
    uint32_t next_derived_slot = 5;
    if (plan->include_valid_to()) {
      layout[SlotId{next_derived_slot++}] = 9;
    }
    if (plan->include_system_time()) {
      layout[SlotId{next_derived_slot}] = 10;
    }
    if (!plan->predicate_properties().empty()) {
      return BeginGather(std::move(batch), plan->predicate_properties(),
                         GatherPhase::kPredicate, std::move(layout));
    }
    return AfterPredicate(std::move(batch), std::move(layout));
  }

  Status ProcessRangeBatch(const ColumnBatch& input) {
    const auto range = plan->valid_time_range();
    if (!range.has_value()) {
      return Status::Corruption(
          "physical runtime", "range plan lost its valid-time bounds");
    }
    std::vector<RangeOutputRow> ready;
    AddOperatorInput(PhysicalOperatorKind::kIntervalDerive,
                     input.row_count());
    ready.reserve(input.row_count());
    for (uint32_t row = 0; row < input.row_count(); ++row) {
      const Value* entity = input.ValueRefAt(kEntityId, row);
      const Value* from = input.ValueRefAt(kValidFrom, row);
      const Value* commit = input.ValueRefAt(kCommitSeq, row);
      const Value* operation = input.ValueRefAt(kOperation, row);
      if (entity == nullptr || from == nullptr || commit == nullptr ||
          operation == nullptr || entity->type() != PhysicalType::kInt64 ||
          from->type() != PhysicalType::kTimestamp64 ||
          commit->type() != PhysicalType::kInt64 ||
          operation->type() != PhysicalType::kInt32 ||
          std::get<int64_t>(entity->data()) < 0 ||
          std::get<int64_t>(commit->data()) <= 0) {
        return Status::Corruption(
            "physical runtime", "range source row has invalid temporal identity");
      }
      RangePendingEvent current{
          static_cast<uint64_t>(std::get<int64_t>(entity->data())),
          std::get<uint64_t>(from->data()),
          static_cast<uint64_t>(std::get<int64_t>(commit->data())),
          std::get<int32_t>(operation->data())};
      if (current.operation != 0 && current.operation != 1) {
        return Status::Corruption(
            "physical runtime", "range source row has unknown operation");
      }
      if (range_closed_entities.count(current.entity_id) != 0) continue;
      auto previous = range_previous_events.find(current.entity_id);
      if (previous != range_previous_events.end() &&
          previous->second.valid_from == current.valid_from) {
        if (current.commit_seq > previous->second.commit_seq) {
          previous->second = current;
        }
        continue;
      }
      if (current.valid_from >= range->second) {
        if (previous != range_previous_events.end() &&
            previous->second.operation == 0 &&
            previous->second.valid_from < range->second &&
            current.valid_from > range->first) {
          ready.push_back(RangeOutputRow{previous->second,
                                         current.valid_from});
        }
        range_closed_entities.insert(current.entity_id);
        range_previous_events.erase(current.entity_id);
        continue;
      }
      if (previous != range_previous_events.end() &&
          current.valid_from >= range->first &&
          previous->second.operation == 0 &&
          current.valid_from > range->first) {
        ready.push_back(RangeOutputRow{previous->second,
                                       current.valid_from});
      }
      range_previous_events.insert_or_assign(current.entity_id, current);
    }
    if (input.row_count() < context.options.batch_capacity) {
      for (const auto& entry : range_previous_events) {
        if (range_closed_entities.count(entry.first) == 0 &&
            entry.second.operation == 0 &&
            entry.second.valid_from < range->second) {
          ready.push_back(RangeOutputRow{
              entry.second, std::numeric_limits<uint64_t>::max()});
        }
      }
      range_previous_events.clear();
      range_closed_entities.clear();
      range_finalized = true;
    }
    if (!ready.empty()) {
      RecordOperatorOutput(PhysicalOperatorKind::kIntervalDerive,
                           ready.size(), false, ready.size());
    }
    return PublishRangeRows(std::move(ready));
  }

  Status FinalizeRange() {
    const auto range = plan->valid_time_range();
    if (!range.has_value()) {
      return Status::Corruption(
          "physical runtime", "range plan lost its valid-time bounds");
    }
    std::vector<RangeOutputRow> ready;
    for (const auto& entry : range_previous_events) {
      if (range_closed_entities.count(entry.first) == 0 &&
          entry.second.operation == 0 &&
          entry.second.valid_from < range->second) {
        ready.push_back(RangeOutputRow{
            entry.second, std::numeric_limits<uint64_t>::max()});
      }
    }
    range_previous_events.clear();
    range_closed_entities.clear();
    if (!ready.empty()) {
      RecordOperatorOutput(PhysicalOperatorKind::kIntervalDerive,
                           ready.size(), false, ready.size());
    }
    return PublishRangeRows(std::move(ready));
  }

  struct PointVariableEdge {
    uint64_t source_id = 0;
    uint64_t target_id = 0;
    uint16_t edge_type = 0;
    uint64_t edge_id = 0;
    uint64_t valid_from = 0;
    uint64_t commit_seq = 0;
    int32_t operation = 0;
  };

  struct PointVariablePath {
    uint64_t root_id = 0;
    uint64_t current_id = 0;
    PointVariableEdge last_edge;
    std::vector<PointVariableEdge> path_edges;
    std::set<LogicalKey> visited_edges;
  };

  struct PendingPointVariableExpand {
    std::vector<PointVariablePath> completed;
    std::shared_ptr<QueryMemoryLease> completed_lease;
    std::unique_ptr<QuerySpillFile> completed_spill;
    size_t next = 0;
    bool spill_rewound = false;
  };

  static uint64_t PointVariablePathBytes(const PointVariablePath& path) {
    return sizeof(PointVariablePath) +
        static_cast<uint64_t>(path.path_edges.size()) *
            sizeof(PointVariableEdge) +
        static_cast<uint64_t>(path.visited_edges.size()) *
            (sizeof(LogicalKey) + 4 * sizeof(void*) + 32);
  }

  static StructValue PointVariableEdgeValue(const PointVariableEdge& edge) {
    return StructValue{{
        StructField{"source_id", Value::Int64(static_cast<int64_t>(edge.source_id))},
        StructField{"target_id", Value::Int64(static_cast<int64_t>(edge.target_id))},
        StructField{"edge_type", Value::Int32(static_cast<int32_t>(edge.edge_type))},
        StructField{"edge_id", Value::Int64(static_cast<int64_t>(edge.edge_id))},
        StructField{"valid_from", Value::Timestamp(edge.valid_from)},
        StructField{"commit_seq", Value::Int64(static_cast<int64_t>(edge.commit_seq))},
    }};
  }

  static StatusOr<PointVariableEdge> PointVariableEdgeFromValue(
      const StructValue& value) {
    if (value.fields.size() != 6) {
      return Status::Corruption(
          "physical runtime", "variable path edge field count is invalid");
    }
    const auto int64_field = [&value](size_t index) -> StatusOr<int64_t> {
      const auto& field = value.fields[index].value;
      if (!field.has_value() || field->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(field->data()) < 0) {
        return Status::Corruption(
            "physical runtime", "variable path edge integer is invalid");
      }
      return std::get<int64_t>(field->data());
    };
    auto source = int64_field(0);
    auto target = int64_field(1);
    auto edge_id = int64_field(3);
    auto commit = int64_field(5);
    const auto& type = value.fields[2].value;
    const auto& valid_from = value.fields[4].value;
    if (!source.ok() || !target.ok() || !edge_id.ok() || !commit.ok() ||
        !type.has_value() || type->type() != PhysicalType::kInt32 ||
        std::get<int32_t>(type->data()) < 0 ||
        std::get<int32_t>(type->data()) > std::numeric_limits<uint16_t>::max() ||
        !valid_from.has_value() ||
        valid_from->type() != PhysicalType::kTimestamp64) {
      return Status::Corruption(
          "physical runtime", "variable path edge value is invalid");
    }
    return PointVariableEdge{
        static_cast<uint64_t>(source.ValueOrDie()),
        static_cast<uint64_t>(target.ValueOrDie()),
        static_cast<uint16_t>(std::get<int32_t>(type->data())),
        static_cast<uint64_t>(edge_id.ValueOrDie()),
        std::get<uint64_t>(valid_from->data()),
        static_cast<uint64_t>(commit.ValueOrDie()), 0};
  }

  static void AppendPointPathU16(std::string* output, uint16_t value) {
    output->push_back(static_cast<char>(value));
    output->push_back(static_cast<char>(value >> 8U));
  }

  static void AppendPointPathU64(std::string* output, uint64_t value) {
    for (uint32_t shift = 0; shift < 64; shift += 8) {
      output->push_back(static_cast<char>(value >> shift));
    }
  }

  static StatusOr<std::optional<LogicalKey>> DecodePointPathEdge(
      const std::string& input) {
    if (input.size() != 28) {
      return Status::Corruption(
          "physical runtime", "invalid variable path edge identity");
    }
    size_t offset = 0;
    const EntityType direction = static_cast<EntityType>(
        static_cast<uint8_t>(input[offset++]));
    const LogicalKeyKind kind = static_cast<LogicalKeyKind>(
        static_cast<uint8_t>(input[offset++]));
    const auto read_u64 = [&input, &offset]() {
      uint64_t value = 0;
      for (uint32_t shift = 0; shift < 64; shift += 8) {
        value |= static_cast<uint64_t>(
            static_cast<uint8_t>(input[offset++])) << shift;
      }
      return value;
    };
    const auto read_u16 = [&input, &offset]() {
      uint16_t value = static_cast<uint8_t>(input[offset++]);
      value |= static_cast<uint16_t>(
          static_cast<uint8_t>(input[offset++])) << 8U;
      return value;
    };
    const uint64_t source = read_u64();
    const uint64_t target = read_u64();
    const uint16_t edge_type = read_u16();
    const uint64_t edge_id = read_u64();
    if ((direction != EntityType::EdgeOut &&
         direction != EntityType::EdgeIn) ||
        kind != LogicalKeyKind::kExistence) {
      return Status::Corruption(
          "physical runtime", "invalid variable path edge kind");
    }
    return std::optional<LogicalKey>{LogicalKey::EdgeExistence(
        source, target, edge_type, edge_id, direction)};
  }

  static std::string EncodePointPathEdge(const LogicalKey& key) {
    std::string encoded;
    encoded.reserve(28);
    encoded.push_back(static_cast<char>(key.entity_type()));
    encoded.push_back(static_cast<char>(key.kind()));
    AppendPointPathU64(&encoded, key.entity_id());
    AppendPointPathU64(&encoded, key.target_id());
    AppendPointPathU16(&encoded, key.edge_type());
    AppendPointPathU64(&encoded, key.edge_id());
    return encoded;
  }

  static StatusOr<ResultBatch> EncodePointVariablePath(
      const PointVariablePath& path) {
    ColumnBatch batch(1);
    const PointVariableEdge& edge = path.last_edge;
    for (const Value& value : {
             Value::Int64(static_cast<int64_t>(path.root_id)),
             Value::Int64(static_cast<int64_t>(path.current_id)),
             Value::Int64(static_cast<int64_t>(edge.source_id)),
             Value::Int64(static_cast<int64_t>(edge.target_id)),
             Value::Int32(static_cast<int32_t>(edge.edge_type)),
             Value::Int64(static_cast<int64_t>(edge.edge_id)),
             Value::Timestamp(edge.valid_from),
             Value::Int64(static_cast<int64_t>(edge.commit_seq)),
             Value::Int32(edge.operation)}) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::vector<Value>{value}, std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    ListValue visited;
    visited.elements.reserve(path.visited_edges.size());
    for (const LogicalKey& key : path.visited_edges) {
      visited.elements.push_back(Value::Binary(EncodePointPathEdge(key)));
    }
    const Status added = batch.AddVector(std::make_shared<ListVector>(
        std::vector<ListValue>{std::move(visited)}, std::vector<bool>{}));
    if (!added.ok()) return added;
    ListValue path_edges;
    path_edges.element_kind = ListElementKind::kStruct;
    path_edges.structured_elements.reserve(path.path_edges.size());
    for (const PointVariableEdge& path_edge : path.path_edges) {
      path_edges.structured_elements.push_back(PointVariableEdgeValue(path_edge));
    }
    const Status path_added = batch.AddVector(std::make_shared<ListVector>(
        std::vector<ListValue>{std::move(path_edges)}, std::vector<bool>{}));
    if (!path_added.ok()) return path_added;
    return ResultBatch(
        {"root", "current", "edge_source", "edge_target", "edge_type",
         "edge_id", "valid_from", "commit_seq", "operation", "visited",
         "path_edges"},
        std::move(batch));
  }

  static StatusOr<PointVariablePath> DecodePointVariablePath(
      const ResultBatch& batch) {
    if (batch.batch().row_count() != 1 ||
        batch.batch().column_count() != 11 || !batch.batch().IsList(9) ||
        !batch.batch().IsList(10)) {
      return Status::Corruption(
          "physical runtime", "invalid variable path spill row");
    }
    const auto int64_at = [&batch](uint32_t column)
        -> StatusOr<int64_t> {
      const Value* value = batch.batch().ValueRefAt(column, 0);
      if (value == nullptr || value->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(value->data()) < 0) {
        return Status::Corruption(
            "physical runtime", "invalid variable path integer");
      }
      return std::get<int64_t>(value->data());
    };
    auto root = int64_at(0);
    auto current = int64_at(1);
    auto source = int64_at(2);
    auto target = int64_at(3);
    auto edge_id = int64_at(5);
    auto commit = int64_at(7);
    const Value* edge_type = batch.batch().ValueRefAt(4, 0);
    const Value* valid_from = batch.batch().ValueRefAt(6, 0);
    const Value* operation = batch.batch().ValueRefAt(8, 0);
    if (!root.ok() || !current.ok() || !source.ok() || !target.ok() ||
        !edge_id.ok() || !commit.ok() || edge_type == nullptr ||
        valid_from == nullptr || operation == nullptr ||
        edge_type->type() != PhysicalType::kInt32 ||
        valid_from->type() != PhysicalType::kTimestamp64 ||
        operation->type() != PhysicalType::kInt32 ||
        std::get<int32_t>(edge_type->data()) < 0 ||
        std::get<int32_t>(edge_type->data()) >
            std::numeric_limits<uint16_t>::max()) {
      return Status::Corruption(
          "physical runtime", "invalid variable path spill values");
    }
    PointVariablePath path{
        static_cast<uint64_t>(root.ValueOrDie()),
        static_cast<uint64_t>(current.ValueOrDie()),
        PointVariableEdge{
            static_cast<uint64_t>(source.ValueOrDie()),
            static_cast<uint64_t>(target.ValueOrDie()),
            static_cast<uint16_t>(std::get<int32_t>(edge_type->data())),
            static_cast<uint64_t>(edge_id.ValueOrDie()),
            std::get<uint64_t>(valid_from->data()),
            static_cast<uint64_t>(commit.ValueOrDie()),
            std::get<int32_t>(operation->data())},
        {}};
    const auto visited = batch.batch().ListAt(9, 0);
    if (!visited.has_value() ||
        visited->element_kind != ListElementKind::kScalar) {
      return Status::Corruption(
          "physical runtime", "missing variable path visited edges");
    }
    for (const auto& value : visited->elements) {
      if (!value.has_value() || value->type() != PhysicalType::kBinary) {
        return Status::Corruption(
            "physical runtime", "invalid variable path visited edge");
      }
      auto key = DecodePointPathEdge(std::get<std::string>(value->data()));
      if (!key.ok() || !key.ValueOrDie().has_value()) {
        return key.ok()
            ? Status::Corruption(
                  "physical runtime", "missing variable path edge")
            : key.status();
      }
      path.visited_edges.insert(*key.ValueOrDie());
    }
    const auto path_edges = batch.batch().ListAt(10, 0);
    if (!path_edges.has_value() ||
        path_edges->element_kind != ListElementKind::kStruct ||
        !path_edges->elements.empty()) {
      return Status::Corruption(
          "physical runtime", "missing variable path edge list");
    }
    path.path_edges.reserve(path_edges->structured_elements.size());
    for (const StructValue& value : path_edges->structured_elements) {
      auto edge = PointVariableEdgeFromValue(value);
      if (!edge.ok()) return edge.status();
      path.path_edges.push_back(edge.ValueOrDie());
    }
    return path;
  }

  Status LoadPointVariableEdges(
      uint64_t source_id, std::vector<PointVariableEdge>* edges,
      std::shared_ptr<QueryMemoryLease>* edge_lease) {
    return LoadPointEdgesForStep(source_id, *plan->expand(), edges, edge_lease);
  }

  Status LoadPointEdgesForStep(
      uint64_t source_id, const PhysicalExpandSpec& step,
      std::vector<PointVariableEdge>* edges,
      std::shared_ptr<QueryMemoryLease>* edge_lease) {
    if (edges == nullptr || edge_lease == nullptr ||
        step.source_binding.value == 0) {
      return Status::InvalidArgument(
          "physical runtime", "variable point edge output is absent");
    }
    auto source_ids = std::make_shared<std::set<uint64_t>>();
    source_ids->insert(source_id);
    TemporalScanSpec edge_spec{
        valid_time, snapshot_seq, context.options.batch_capacity};
    edge_spec.entity_type = step.direction;
    edge_spec.key_kind = LogicalKeyKind::kExistence;
    edge_spec.edge_type = step.edge_type;
    edge_spec.allowed_candidate_entity_ids = std::move(source_ids);
    edge_spec.cancellation = context.options.cancellation;
    edge_spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &edge_spec);
    auto opened = OpenPinnedTemporalScan(sources, std::move(edge_spec));
    if (!opened.ok()) return opened.status();
    *edge_lease = std::make_shared<QueryMemoryLease>(
        context.options.memory_account, 0);
    for (;;) {
      ColumnBatch batch;
      const Status next = opened.ValueOrDie().NextMorsel(&batch);
      if (next.IsNotFound()) break;
      if (!next.ok()) return next;
      for (uint32_t row = 0; row < batch.row_count(); ++row) {
        const Value* source = batch.ValueRefAt(kEntityId, row);
        const Value* target = batch.ValueRefAt(kTargetId, row);
        const Value* type = batch.ValueRefAt(kEdgeType, row);
        const Value* edge_id = batch.ValueRefAt(kEdgeId, row);
        const Value* valid_from_value = batch.ValueRefAt(kValidFrom, row);
        const Value* commit = batch.ValueRefAt(kCommitSeq, row);
        const Value* operation = batch.ValueRefAt(kOperation, row);
        if (source == nullptr || target == nullptr || type == nullptr ||
            edge_id == nullptr || valid_from_value == nullptr ||
            commit == nullptr || operation == nullptr ||
            source->type() != PhysicalType::kInt64 ||
            target->type() != PhysicalType::kInt64 ||
            type->type() != PhysicalType::kInt32 ||
            edge_id->type() != PhysicalType::kInt64 ||
            valid_from_value->type() != PhysicalType::kTimestamp64 ||
            commit->type() != PhysicalType::kInt64 ||
            operation->type() != PhysicalType::kInt32 ||
            std::get<int64_t>(source->data()) < 0 ||
            std::get<int64_t>(target->data()) < 0 ||
            std::get<int64_t>(edge_id->data()) < 0 ||
            std::get<int64_t>(commit->data()) < 0 ||
            std::get<int32_t>(type->data()) < 0 ||
            std::get<int32_t>(type->data()) >
                std::numeric_limits<uint16_t>::max()) {
          return Status::Corruption(
              "physical runtime", "variable point edge identity is invalid");
        }
        if (static_cast<uint64_t>(std::get<int64_t>(source->data())) !=
            source_id) {
          return Status::Corruption(
              "physical runtime", "variable point edge source differs");
        }
        const Status reserved = (*edge_lease)->ReserveAdditional(
            sizeof(PointVariableEdge));
        if (!reserved.ok()) return reserved;
        edges->push_back(PointVariableEdge{
            source_id,
            static_cast<uint64_t>(std::get<int64_t>(target->data())),
            static_cast<uint16_t>(std::get<int32_t>(type->data())),
            static_cast<uint64_t>(std::get<int64_t>(edge_id->data())),
            std::get<uint64_t>(valid_from_value->data()),
            static_cast<uint64_t>(std::get<int64_t>(commit->data())),
            std::get<int32_t>(operation->data())});
      }
    }
    std::sort(edges->begin(), edges->end(),
              [](const PointVariableEdge& left,
                 const PointVariableEdge& right) {
                return std::tie(left.source_id, left.target_id,
                                left.edge_type, left.edge_id,
                                left.valid_from, left.commit_seq,
                                left.operation) <
                       std::tie(right.source_id, right.target_id,
                                right.edge_type, right.edge_id,
                                right.valid_from, right.commit_seq,
                                right.operation);
              });
    return Status::OK();
  }

  struct SegmentedPointPath {
    uint64_t root_id = 0;
    uint64_t current_id = 0;
    std::vector<std::vector<PointVariableEdge>> segment_edges;
    std::set<LogicalKey> visited_edges;
  };

  struct PendingSegmentedPointExpand {
    std::vector<SegmentedPointPath> completed;
    std::shared_ptr<QueryMemoryLease> completed_lease;
    std::unique_ptr<QuerySpillFile> completed_spill;
    size_t next = 0;
    bool spill_rewound = false;
  };

  static uint64_t SegmentedPointPathBytes(const SegmentedPointPath& path) {
    uint64_t bytes = sizeof(SegmentedPointPath) +
        static_cast<uint64_t>(path.visited_edges.size()) *
            (sizeof(LogicalKey) + 4 * sizeof(void*) + 32);
    for (const auto& segment : path.segment_edges) {
      if (segment.size() >
          (std::numeric_limits<uint64_t>::max() - bytes) /
              sizeof(PointVariableEdge)) {
        return std::numeric_limits<uint64_t>::max();
      }
      bytes += static_cast<uint64_t>(segment.size()) *
          sizeof(PointVariableEdge);
    }
    return bytes;
  }

  static StatusOr<ResultBatch> EncodeSegmentedPointPath(
      const SegmentedPointPath& path) {
    ColumnBatch batch(1);
    for (const Value& value : {
             Value::Int64(static_cast<int64_t>(path.root_id)),
             Value::Int64(static_cast<int64_t>(path.current_id))}) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::vector<Value>{value}, std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    ListValue visited;
    visited.elements.reserve(path.visited_edges.size());
    for (const LogicalKey& key : path.visited_edges) {
      visited.elements.push_back(Value::Binary(EncodePointPathEdge(key)));
    }
    ListValue edges;
    edges.element_kind = ListElementKind::kStruct;
    for (const auto& segment : path.segment_edges) {
      for (const PointVariableEdge& edge : segment) {
        edges.structured_elements.push_back(PointVariableEdgeValue(edge));
      }
    }
    ListValue ends;
    for (const auto& segment : path.segment_edges) {
      ends.elements.push_back(Value::Int64(static_cast<int64_t>(segment.size())));
    }
    for (ListValue value : {std::move(visited), std::move(edges),
                            std::move(ends)}) {
      const Status added = batch.AddVector(std::make_shared<ListVector>(
          std::vector<ListValue>{std::move(value)}, std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    return ResultBatch(
        {"root", "current", "visited", "edges", "segment_lengths"},
        std::move(batch));
  }

  static StatusOr<SegmentedPointPath> DecodeSegmentedPointPath(
      const ResultBatch& batch, size_t segment_count) {
    if (batch.batch().row_count() != 1 || batch.batch().column_count() != 5 ||
        !batch.batch().IsList(2) || !batch.batch().IsList(3) ||
        !batch.batch().IsList(4)) {
      return Status::Corruption(
          "physical runtime", "invalid segmented point spill row");
    }
    const auto int64_at = [&batch](uint32_t column)
        -> StatusOr<uint64_t> {
      const Value* value = batch.batch().ValueRefAt(column, 0);
      if (value == nullptr || value->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(value->data()) < 0) {
        return Status::Corruption(
            "physical runtime", "invalid segmented point spill identity");
      }
      return static_cast<uint64_t>(std::get<int64_t>(value->data()));
    };
    auto root = int64_at(0);
    auto current = int64_at(1);
    if (!root.ok() || !current.ok()) {
      return root.ok() ? current.status() : root.status();
    }
    SegmentedPointPath path{
        root.ValueOrDie(), current.ValueOrDie(),
        std::vector<std::vector<PointVariableEdge>>(segment_count), {}};
    const auto visited = batch.batch().ListAt(2, 0);
    if (!visited.has_value()) {
      return Status::Corruption(
          "physical runtime", "segmented point spill visited list is absent");
    }
    for (const auto& value : visited->elements) {
      if (!value.has_value() || value->type() != PhysicalType::kBinary) {
        return Status::Corruption(
            "physical runtime", "segmented point spill edge identity is invalid");
      }
      auto key = DecodePointPathEdge(std::get<std::string>(value->data()));
      if (!key.ok() || !key.ValueOrDie().has_value()) {
        return key.ok() ? Status::Corruption(
                              "physical runtime",
                              "segmented point spill edge is absent")
                        : key.status();
      }
      path.visited_edges.insert(*key.ValueOrDie());
    }
    const auto ends = batch.batch().ListAt(4, 0);
    const auto edges = batch.batch().ListAt(3, 0);
    if (!ends.has_value() || !edges.has_value() ||
        ends->elements.size() != segment_count ||
        edges->element_kind != ListElementKind::kStruct) {
      return Status::Corruption(
          "physical runtime", "segmented point spill segment shape is invalid");
    }
    size_t edge_offset = 0;
    for (size_t segment = 0; segment < segment_count; ++segment) {
      const auto& length_value = ends->elements[segment];
      if (!length_value.has_value() ||
          length_value->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(length_value->data()) < 0) {
        return Status::Corruption(
            "physical runtime", "segmented point spill segment length is invalid");
      }
      const size_t length = static_cast<size_t>(
          std::get<int64_t>(length_value->data()));
      if (length > edges->structured_elements.size() - edge_offset) {
        return Status::Corruption(
            "physical runtime", "segmented point spill segment length overflows");
      }
      for (size_t index = 0; index < length; ++index) {
        auto edge = PointVariableEdgeFromValue(
            edges->structured_elements[edge_offset++]);
        if (!edge.ok()) return edge.status();
        path.segment_edges[segment].push_back(edge.ValueOrDie());
      }
    }
    if (edge_offset != edges->structured_elements.size()) {
      return Status::Corruption(
          "physical runtime", "segmented point spill has trailing edges");
    }
    return path;
  }

  Status BuildSegmentedPointPaths(
      const ColumnBatch& roots,
      std::vector<SegmentedPointPath>* completed,
      std::shared_ptr<QueryMemoryLease>* completed_lease,
      std::unique_ptr<QuerySpillFile>* completed_spill) {
    if (completed == nullptr || completed_lease == nullptr ||
        completed_spill == nullptr || plan->expand_steps().size() < 2) {
      return Status::InvalidArgument(
          "physical runtime", "segmented point frontier is incomplete");
    }
    struct StateSet {
      std::vector<SegmentedPointPath> states;
      std::shared_ptr<QueryMemoryLease> lease;
      std::unique_ptr<QuerySpillFile> spill;
      uint64_t count = 0;
    };
    const auto new_spill = [this]()
        -> StatusOr<std::unique_ptr<QuerySpillFile>> {
      const std::string directory = context.options.spill_directory.empty()
          ? "/tmp" : context.options.spill_directory;
      auto spill = std::make_unique<QuerySpillFile>(
          directory, context.options.cancellation,
          context.options.spill_resource_extensions,
          context.options.memory_account,
          [stats = context.options.execution_stats](uint64_t bytes) {
            if (stats) stats->path_frontier_spill_bytes += bytes;
          });
      const Status opened = spill->Open();
      if (!opened.ok()) return opened;
      if (context.options.execution_stats) {
        ++context.options.execution_stats->path_frontier_spill_starts;
      }
      return spill;
    };
    const auto append_spilled = [](QuerySpillFile* spill,
                                   const SegmentedPointPath& path) {
      if (spill == nullptr) {
        return Status::InvalidArgument(
            "physical runtime", "missing segmented point spill");
      }
      auto encoded = EncodeSegmentedPointPath(path);
      if (!encoded.ok()) return encoded.status();
      return spill->Append(encoded.ValueOrDie());
    };
    const auto begin_spill = [&](StateSet* set) -> Status {
      if (set == nullptr || set->spill) {
        return Status::InvalidArgument(
            "physical runtime", "segmented point spill state is invalid");
      }
      auto opened = new_spill();
      if (!opened.ok()) return opened.status();
      set->spill = std::move(opened).ConsumeValueOrDie();
      for (const SegmentedPointPath& path : set->states) {
        const Status appended = append_spilled(set->spill.get(), path);
        if (!appended.ok()) return appended;
      }
      set->states.clear();
      set->lease.reset();
      return Status::OK();
    };
    const auto retain = [&](const SegmentedPointPath& path,
                            StateSet* set) -> Status {
      if (set == nullptr) {
        return Status::InvalidArgument(
            "physical runtime", "segmented point retained set is absent");
      }
      if (set->spill) {
        const Status appended = append_spilled(set->spill.get(), path);
        if (appended.ok()) ++set->count;
        return appended;
      }
      if (context.options.memory_account &&
          context.options.memory_account->ShouldSpill()) {
        const Status started = begin_spill(set);
        if (!started.ok()) return started;
        const Status appended = append_spilled(set->spill.get(), path);
        if (appended.ok()) ++set->count;
        return appended;
      }
      if (!set->lease) {
        set->lease = std::make_shared<QueryMemoryLease>(
            context.options.memory_account, 0);
      }
      const Status reserved = set->lease->ReserveAdditional(
          SegmentedPointPathBytes(path));
      if (!reserved.ok()) {
        const Status started = begin_spill(set);
        if (!started.ok()) return started;
        const Status appended = append_spilled(set->spill.get(), path);
        if (appended.ok()) ++set->count;
        return appended;
      }
      set->states.push_back(path);
      ++set->count;
      return Status::OK();
    };
    const auto visit = [&](StateSet* set, const auto& callback) -> Status {
      if (set == nullptr) {
        return Status::InvalidArgument(
            "physical runtime", "segmented point source set is absent");
      }
      if (!set->spill) {
        for (const SegmentedPointPath& path : set->states) {
          const Status status = callback(path);
          if (!status.ok()) return status;
        }
        return Status::OK();
      }
      Status status = set->spill->Seal();
      if (!status.ok()) return status;
      status = set->spill->Rewind();
      if (!status.ok()) return status;
      for (;;) {
        ResultBatch batch;
        status = set->spill->Next(&batch);
        if (status.IsNotFound()) break;
        if (!status.ok()) return status;
        auto path = DecodeSegmentedPointPath(
            batch, plan->expand_steps().size());
        if (!path.ok()) return path.status();
        status = callback(path.ValueOrDie());
        if (!status.ok()) return status;
      }
      return Status::OK();
    };

    StateSet frontier;
    frontier.states.reserve(roots.row_count());
    for (uint32_t row = 0; row < roots.row_count(); ++row) {
      const Value* root = roots.ValueRefAt(kEntityId, row);
      if (root == nullptr || root->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(root->data()) < 0) {
        return Status::Corruption(
            "physical runtime", "segmented point root identity is invalid");
      }
      const SegmentedPointPath path{
          static_cast<uint64_t>(std::get<int64_t>(root->data())),
          static_cast<uint64_t>(std::get<int64_t>(root->data())),
          std::vector<std::vector<PointVariableEdge>>(
              plan->expand_steps().size()), {}};
      const Status retained = retain(path, &frontier);
      if (!retained.ok()) return retained;
    }
    for (size_t segment_index = 0;
         segment_index < plan->expand_steps().size(); ++segment_index) {
      if (context.options.expand_segment_observer) {
        context.options.expand_segment_observer(
            static_cast<uint32_t>(segment_index));
      }
      if (context.options.cancellation &&
          context.options.cancellation->IsCancelled()) {
        return Status::QueryCancelled(
            "physical runtime",
            "query cancelled before expanding segmented point segment");
      }
      const PhysicalExpandSpec& step = plan->expand_steps()[segment_index];
      StateSet segment_frontier = std::move(frontier);
      StateSet next_segment;
      for (uint32_t hop = 1; hop <= step.max_hops &&
                             segment_frontier.count != 0; ++hop) {
        const auto hop_start = std::chrono::steady_clock::now();
        AddOperatorInputAt(PhysicalOperatorKind::kExpand,
                           static_cast<uint32_t>(segment_index),
                           segment_frontier.count);
        StateSet next_hop;
        size_t completed_this_hop = 0;
        const Status visited = visit(
            &segment_frontier, [&](const SegmentedPointPath& path) -> Status {
          if (context.options.cancellation &&
              context.options.cancellation->IsCancelled()) {
            return Status::QueryCancelled(
                "physical runtime",
                "query cancelled while expanding segmented frontier");
          }
          std::vector<PointVariableEdge> edges;
          std::shared_ptr<QueryMemoryLease> edge_lease;
          const auto edge_start = std::chrono::steady_clock::now();
          const Status loaded = LoadPointEdgesForStep(
              path.current_id, step, &edges, &edge_lease);
          AddOperatorBlockedAt(
              PhysicalOperatorKind::kExpand, segment_index,
              ElapsedNs(edge_start));
          if (!loaded.ok()) return loaded;
          for (const PointVariableEdge& edge : edges) {
            const auto target_candidates = advisory_candidates_by_binding.find(
                step.target_binding);
            const auto relationship_candidates =
                advisory_candidates_by_binding.find(step.relationship_binding);
            const bool has_dynamic_filter =
                target_candidates != advisory_candidates_by_binding.end() ||
                relationship_candidates != advisory_candidates_by_binding.end();
            if (has_dynamic_filter && context.options.execution_stats) {
              ++context.options.execution_stats->index_dynamic_filter_input_rows;
            }
            if (target_candidates != advisory_candidates_by_binding.end() &&
                target_candidates->second->count(edge.target_id) == 0) {
              if (context.options.execution_stats) {
                ++context.options.execution_stats->index_dynamic_filter_rejected_rows;
              }
              continue;
            }
            if (relationship_candidates !=
                    advisory_candidates_by_binding.end() &&
                relationship_candidates->second->count(edge.edge_id) == 0) {
              if (context.options.execution_stats) {
                ++context.options.execution_stats->index_dynamic_filter_rejected_rows;
              }
              continue;
            }
            if (has_dynamic_filter && context.options.execution_stats) {
              ++context.options.execution_stats->index_dynamic_filter_output_rows;
            }
            const LogicalKey identity = LogicalKey::EdgeExistence(
                edge.source_id, edge.target_id, edge.edge_type, edge.edge_id,
                step.direction);
            if (path.visited_edges.count(identity) != 0) continue;
            auto visible = PointVariableTargetVisible(edge.target_id);
            if (!visible.ok()) return visible.status();
            if (!visible.ValueOrDie()) continue;
            SegmentedPointPath extended = path;
            extended.current_id = edge.target_id;
            extended.segment_edges[segment_index].push_back(edge);
            extended.visited_edges.insert(identity);
            if (hop >= step.min_hops) {
              if (segment_index + 1 == plan->expand_steps().size()) {
                StateSet completed_set;
                completed_set.states.swap(*completed);
                completed_set.lease = std::move(*completed_lease);
                completed_set.spill = std::move(*completed_spill);
                completed_set.count = completed_set.states.size();
                const Status retained = retain(extended, &completed_set);
                *completed = std::move(completed_set.states);
                *completed_lease = std::move(completed_set.lease);
                *completed_spill = std::move(completed_set.spill);
                if (!retained.ok()) return retained;
              } else {
                const Status retained = retain(extended, &next_segment);
                if (!retained.ok()) return retained;
              }
              ++completed_this_hop;
            }
            if (hop < step.max_hops) {
              const Status retained = retain(extended, &next_hop);
              if (!retained.ok()) return retained;
            }
          }
          return Status::OK();
        });
        if (!visited.ok()) return visited;
        RecordOperatorOutputAt(PhysicalOperatorKind::kExpand,
                               static_cast<uint32_t>(segment_index),
                               next_hop.count + completed_this_hop);
        AddOperatorCpuAt(
            PhysicalOperatorKind::kExpand, segment_index,
            ElapsedNs(hop_start));
        segment_frontier = std::move(next_hop);
      }
      frontier = std::move(next_segment);
    }
    if (*completed_spill) {
      const Status sealed = (*completed_spill)->Seal();
      if (!sealed.ok()) return sealed;
    }
    return Status::OK();
  }

  Status PublishSegmentedPointPaths(
      std::vector<SegmentedPointPath> paths) {
    if (paths.empty() || plan->expand_steps().size() < 2) {
      return Status::OK();
    }
    const uint64_t row_count = paths.size();
    struct RuntimeColumn {
      SlotId slot;
      std::vector<Value> values;
      std::vector<ListValue> lists;
      bool list = false;
    };
    std::vector<RuntimeColumn> columns;
    std::map<SlotId, uint32_t> layout;
    const auto add_scalar = [&](SlotId slot, std::vector<Value> values) {
      if (layout.count(slot) != 0) return;
      layout.emplace(slot, static_cast<uint32_t>(columns.size()));
      columns.push_back(RuntimeColumn{slot, std::move(values), {}, false});
    };
    const auto add_list = [&](SlotId slot, std::vector<ListValue> values) {
      if (layout.count(slot) != 0) return;
      layout.emplace(slot, static_cast<uint32_t>(columns.size()));
      columns.push_back(RuntimeColumn{slot, {}, std::move(values), true});
    };
    std::vector<Value> roots;
    roots.reserve(paths.size());
    for (const SegmentedPointPath& path : paths) {
      roots.push_back(Value::Int64(static_cast<int64_t>(path.root_id)));
    }
    add_scalar(plan->expand_steps().front().source_slot, std::move(roots));
    for (size_t segment_index = 0;
         segment_index < plan->expand_steps().size(); ++segment_index) {
      const PhysicalExpandSpec& step = plan->expand_steps()[segment_index];
      std::vector<Value> targets, edge_types, edge_ids, valid_from,
          commits, operations, system_times, valid_tos;
      std::vector<ListValue> path_values;
      targets.reserve(paths.size());
      edge_types.reserve(paths.size());
      edge_ids.reserve(paths.size());
      valid_from.reserve(paths.size());
      commits.reserve(paths.size());
      operations.reserve(paths.size());
      system_times.reserve(paths.size());
      valid_tos.reserve(paths.size());
      if (step.path_slot.value != 0) path_values.reserve(paths.size());
      for (const SegmentedPointPath& path : paths) {
        const auto& segment = path.segment_edges[segment_index];
        if (segment.empty()) {
          return Status::Corruption(
              "physical runtime", "segmented path has an empty segment");
        }
        const PointVariableEdge& edge = segment.back();
        targets.push_back(Value::Int64(static_cast<int64_t>(edge.target_id)));
        edge_types.push_back(Value::Int32(static_cast<int32_t>(edge.edge_type)));
        edge_ids.push_back(Value::Int64(static_cast<int64_t>(edge.edge_id)));
        valid_from.push_back(Value::Timestamp(edge.valid_from));
        commits.push_back(Value::Int64(static_cast<int64_t>(edge.commit_seq)));
        operations.push_back(Value::Int32(edge.operation));
        if (plan->include_system_time()) {
          if (!timeline || edge.commit_seq == 0 ||
              edge.commit_seq > timeline->physical_times.size()) {
            return Status::Corruption(
                "physical runtime", "segmented commit is absent from CommitTimeline");
          }
          system_times.push_back(Value::Timestamp(
              timeline->physical_times[edge.commit_seq - 1]));
        } else {
          system_times.push_back(Value::Timestamp(0));
        }
        if (plan->include_valid_to()) {
          TemporalScanSpec boundary_spec = scan_spec;
          boundary_spec.entity_type = step.direction;
          boundary_spec.key_kind = LogicalKeyKind::kExistence;
          boundary_spec.edge_type = edge.edge_type;
          const LogicalKey key = LogicalKey::EdgeExistence(
              edge.source_id, edge.target_id, edge.edge_type, edge.edge_id,
              step.direction);
          boundary_spec.exact_key = key;
          boundary_spec.allowed_candidate_entity_ids.reset();
          const auto boundary = FindNextPinnedValidBoundary(
              sources, boundary_spec, key, edge.valid_from);
          if (!boundary.ok()) return boundary.status();
          valid_tos.push_back(Value::Timestamp(
              boundary.ValueOrDie().value_or(
                  std::numeric_limits<uint64_t>::max())));
        } else {
          valid_tos.push_back(Value::Timestamp(
              std::numeric_limits<uint64_t>::max()));
        }
        if (step.path_slot.value != 0) {
          ListValue list;
          list.element_kind = ListElementKind::kStruct;
          list.structured_elements.reserve(segment.size());
          for (const PointVariableEdge& path_edge : segment) {
            list.structured_elements.push_back(PointVariableEdgeValue(path_edge));
          }
          path_values.push_back(std::move(list));
        }
      }
      add_scalar(step.target_slot, std::move(targets));
      add_scalar(step.edge_type_slot, std::move(edge_types));
      add_scalar(step.edge_id_slot, std::move(edge_ids));
      add_scalar(step.valid_from_slot, std::move(valid_from));
      add_scalar(step.commit_seq_slot, std::move(commits));
      add_scalar(step.operation_slot, std::move(operations));
      add_scalar(step.system_time_slot, std::move(system_times));
      add_scalar(step.valid_to_slot, std::move(valid_tos));
      if (step.path_slot.value != 0) add_list(step.path_slot, std::move(path_values));
    }
    ColumnBatch expanded(context.options.batch_capacity);
    for (RuntimeColumn& column : columns) {
      Status added;
      if (column.list) {
        added = expanded.AddVector(std::make_shared<ListVector>(
            std::move(column.lists), std::vector<bool>{}));
      } else {
        added = expanded.AddVector(std::make_shared<FlatVector>(
            std::move(column.values), std::vector<bool>{}));
      }
      if (!added.ok()) return added;
    }
    return ContinueMultiHopPredicateProperties(
        std::move(expanded), std::move(layout));
  }

  Status BeginSegmentedPointExpand(const ColumnBatch& roots) {
    if (pending_segmented_point_expand.has_value()) {
      return Status::Corruption(
          "physical runtime", "segmented point frontier overlaps");
    }
    std::vector<SegmentedPointPath> completed;
    std::shared_ptr<QueryMemoryLease> completed_lease;
    std::unique_ptr<QuerySpillFile> completed_spill;
    const Status built = BuildSegmentedPointPaths(
        roots, &completed, &completed_lease, &completed_spill);
    if (!built.ok()) return built;
    pending_segmented_point_expand.emplace(
        PendingSegmentedPointExpand{
            std::move(completed), std::move(completed_lease),
            std::move(completed_spill), 0, false});
    return ContinueSegmentedPointExpand();
  }

  Status ContinueSegmentedPointExpand() {
    if (!pending_segmented_point_expand.has_value()) {
      return Status::Corruption(
          "physical runtime", "segmented point frontier is absent");
    }
    PendingSegmentedPointExpand& pending = *pending_segmented_point_expand;
    if (!pending.completed_spill && pending.next == pending.completed.size()) {
      pending_segmented_point_expand.reset();
      return Status::OK();
    }
    std::vector<SegmentedPointPath> output;
    output.reserve(context.options.batch_capacity);
    if (pending.completed_spill) {
      if (!pending.spill_rewound) {
        const Status rewound = pending.completed_spill->Rewind();
        if (!rewound.ok()) return rewound;
        pending.spill_rewound = true;
      }
      while (output.size() < context.options.batch_capacity) {
        ResultBatch batch;
        const Status next = pending.completed_spill->Next(&batch);
        if (next.IsNotFound()) {
          pending.completed_spill.reset();
          break;
        }
        if (!next.ok()) return next;
        auto path = DecodeSegmentedPointPath(
            batch, plan->expand_steps().size());
        if (!path.ok()) return path.status();
        output.push_back(std::move(path).ConsumeValueOrDie());
      }
    } else {
      const size_t count = std::min<size_t>(
          context.options.batch_capacity,
          pending.completed.size() - pending.next);
      for (size_t index = 0; index < count; ++index) {
        output.push_back(std::move(
            pending.completed[pending.next + index]));
      }
      pending.next += count;
    }
    if (output.empty()) {
      pending_segmented_point_expand.reset();
      return Status::OK();
    }
    return PublishSegmentedPointPaths(std::move(output));
  }

  StatusOr<bool> PointVariableTargetVisible(uint64_t target_id) {
    TemporalScanSpec target_spec{
        valid_time, snapshot_seq, context.options.batch_capacity};
    target_spec.entity_type = EntityType::Vertex;
    target_spec.key_kind = LogicalKeyKind::kExistence;
    target_spec.exact_key = LogicalKey::VertexExistence(target_id);
    target_spec.cancellation = context.options.cancellation;
    target_spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &target_spec);
    auto opened = OpenPinnedTemporalScan(sources, std::move(target_spec));
    if (!opened.ok()) return opened.status();
    ColumnBatch batch;
    const Status next = opened.ValueOrDie().NextMorsel(&batch);
    if (next.IsNotFound()) return false;
    if (!next.ok()) return next;
    if (batch.row_count() != 1) {
      return Status::Corruption(
          "physical runtime", "variable point target scan is not unique");
    }
    const Value* entity = batch.ValueRefAt(kEntityId, 0);
    if (entity == nullptr || entity->type() != PhysicalType::kInt64 ||
        std::get<int64_t>(entity->data()) < 0 ||
        static_cast<uint64_t>(std::get<int64_t>(entity->data())) != target_id) {
      return Status::Corruption(
          "physical runtime", "variable point target identity is invalid");
    }
    return true;
  }

  Status BuildPointVariablePaths(
      const ColumnBatch& roots, std::vector<PointVariablePath>* completed,
      std::shared_ptr<QueryMemoryLease>* completed_lease,
      std::unique_ptr<QuerySpillFile>* completed_spill) {
    if (completed == nullptr || completed_lease == nullptr ||
        completed_spill == nullptr || !plan->expand().has_value() ||
        plan->expand()->max_hops <= 1) {
      return Status::InvalidArgument(
          "physical runtime", "variable point frontier is incomplete");
    }
    const auto new_spill = [this]() -> StatusOr<std::unique_ptr<QuerySpillFile>> {
      const std::string directory = context.options.spill_directory.empty()
          ? "/tmp" : context.options.spill_directory;
      auto spill = std::make_unique<QuerySpillFile>(
          directory, context.options.cancellation,
          context.options.spill_resource_extensions,
          context.options.memory_account,
          [stats = context.options.execution_stats](uint64_t bytes) {
            if (stats) stats->path_frontier_spill_bytes += bytes;
          });
      const Status opened = spill->Open();
      if (!opened.ok()) return opened;
      if (context.options.execution_stats) {
        ++context.options.execution_stats->path_frontier_spill_starts;
      }
      return spill;
    };
    const auto append_spilled = [](QuerySpillFile* spill,
                                   const PointVariablePath& path) {
      if (spill == nullptr) {
        return Status::InvalidArgument(
            "physical runtime", "missing variable frontier spill");
      }
      auto encoded = EncodePointVariablePath(path);
      if (!encoded.ok()) return encoded.status();
      return spill->Append(encoded.ValueOrDie());
    };
    const auto begin_spill = [&](
        std::vector<PointVariablePath>* states,
        std::shared_ptr<QueryMemoryLease>* lease,
        std::unique_ptr<QuerySpillFile>* spill) -> Status {
      auto opened = new_spill();
      if (!opened.ok()) return opened.status();
      *spill = std::move(opened).ConsumeValueOrDie();
      for (const PointVariablePath& state : *states) {
        const Status appended = append_spilled(spill->get(), state);
        if (!appended.ok()) return appended;
      }
      states->clear();
      lease->reset();
      return Status::OK();
    };
    const auto retain_state = [&](
        const PointVariablePath& state,
        std::vector<PointVariablePath>* states,
        std::shared_ptr<QueryMemoryLease>* lease,
        std::unique_ptr<QuerySpillFile>* spill) -> Status {
      if (*spill) return append_spilled(spill->get(), state);
      if (context.options.memory_account &&
          context.options.memory_account->ShouldSpill()) {
        const Status started = begin_spill(states, lease, spill);
        if (!started.ok()) return started;
        return append_spilled(spill->get(), state);
      }
      if (!*lease) {
        *lease = std::make_shared<QueryMemoryLease>(
            context.options.memory_account, 0);
      }
      const Status reserved =
          (*lease)->ReserveAdditional(PointVariablePathBytes(state));
      if (!reserved.ok()) {
        const Status started = begin_spill(states, lease, spill);
        if (!started.ok()) return started;
        return append_spilled(spill->get(), state);
      }
      states->push_back(state);
      return Status::OK();
    };

    std::vector<PointVariablePath> frontier;
    auto frontier_lease = std::make_shared<QueryMemoryLease>(
        context.options.memory_account, 0);
    std::unique_ptr<QuerySpillFile> frontier_spill;
    frontier.reserve(roots.row_count());
    for (uint32_t row = 0; row < roots.row_count(); ++row) {
      const Value* root = roots.ValueRefAt(kEntityId, row);
      if (root == nullptr || root->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(root->data()) < 0) {
        return Status::Corruption(
            "physical runtime", "variable point root identity is invalid");
      }
      const uint64_t id =
          static_cast<uint64_t>(std::get<int64_t>(root->data()));
      PointVariablePath path{id, id, {}, {}};
      const Status reserved = frontier_lease->ReserveAdditional(
          PointVariablePathBytes(path));
      if (!reserved.ok()) return reserved;
      frontier.push_back(std::move(path));
    }
    uint64_t frontier_count = frontier.size();
    const PhysicalExpandSpec& step = *plan->expand();
    for (uint32_t hop = 0;
         hop < step.max_hops && frontier_count != 0; ++hop) {
      AddOperatorInput(PhysicalOperatorKind::kExpand, frontier_count);
      const uint32_t partition_count = ChoosePathFrontierPartitionCount(
          frontier_count, context.options.batch_capacity);
      const size_t max_partition_size = static_cast<size_t>(
          (frontier_count + partition_count - 1) / partition_count);
      std::vector<PointVariablePath> next;
      auto next_lease = std::make_shared<QueryMemoryLease>(
          context.options.memory_account, 0);
      std::unique_ptr<QuerySpillFile> next_spill;
      uint64_t next_count = 0;
      size_t completed_this_hop = 0;
      const auto expand_path = [&](const PointVariablePath& path) -> Status {
        if (context.options.cancellation &&
            context.options.cancellation->IsCancelled()) {
          return Status::QueryCancelled(
              "physical runtime",
              "query cancelled while expanding frontier");
        }
        std::vector<PointVariableEdge> edges;
        std::shared_ptr<QueryMemoryLease> edge_lease;
        const Status loaded = LoadPointVariableEdges(
            path.current_id, &edges, &edge_lease);
        if (!loaded.ok()) return loaded;
        for (const PointVariableEdge& edge : edges) {
          const LogicalKey identity = LogicalKey::EdgeExistence(
              edge.source_id, edge.target_id, edge.edge_type, edge.edge_id,
              step.direction);
          if (path.visited_edges.count(identity) != 0) continue;
          auto visible = PointVariableTargetVisible(edge.target_id);
          if (!visible.ok()) return visible.status();
          if (!visible.ValueOrDie()) continue;
          PointVariablePath extended = path;
          extended.current_id = edge.target_id;
          extended.last_edge = edge;
          extended.path_edges.push_back(edge);
          extended.visited_edges.insert(identity);
          if (hop + 1 >= step.min_hops) {
            const Status retained = retain_state(
                extended, completed, completed_lease, completed_spill);
            if (!retained.ok()) return retained;
            ++completed_this_hop;
          }
          const Status retained = retain_state(
              extended, &next, &next_lease, &next_spill);
          if (!retained.ok()) return retained;
          ++next_count;
        }
        return Status::OK();
      };
      if (frontier_spill) {
        Status status = frontier_spill->Seal();
        if (!status.ok()) return status;
        status = frontier_spill->Rewind();
        if (!status.ok()) return status;
        for (;;) {
          ResultBatch batch;
          status = frontier_spill->Next(&batch);
          if (status.IsNotFound()) break;
          if (!status.ok()) return status;
          auto path = DecodePointVariablePath(batch);
          if (!path.ok()) return path.status();
          status = expand_path(path.ValueOrDie());
          if (!status.ok()) return status;
        }
      } else {
        for (const PointVariablePath& path : frontier) {
          const Status status = expand_path(path);
          if (!status.ok()) return status;
        }
      }
      if (next_spill) {
        const Status sealed = next_spill->Seal();
        if (!sealed.ok()) return sealed;
      }
      if (!next.empty()) {
        RecordOperatorOutput(PhysicalOperatorKind::kExpand, next.size());
      } else if (next_count != 0) {
        RecordOperatorOutput(PhysicalOperatorKind::kExpand, next_count);
      }
      RecordPathFrontierHop(
          context.options.execution_stats, frontier_count, next_count,
          completed_this_hop, partition_count,
          max_partition_size);
      frontier = std::move(next);
      frontier_lease = std::move(next_lease);
      frontier_spill = std::move(next_spill);
      frontier_count = next_count;
    }
    if (*completed_spill) {
      const Status sealed = (*completed_spill)->Seal();
      if (!sealed.ok()) return sealed;
    }
    return Status::OK();
  }

  Status PublishPointVariablePaths(std::vector<PointVariablePath> paths) {
    if (paths.empty() || !plan->expand().has_value()) return Status::OK();
    constexpr uint64_t kOutputColumns = 12;
    uint64_t output_bytes = sizeof(FlatVector) * kOutputColumns +
        static_cast<uint64_t>(paths.size()) * kOutputColumns *
            (sizeof(Value) + sizeof(bool));
    constexpr uint64_t kRelationshipFieldNameBytes =
        sizeof("source_id") - 1 + sizeof("target_id") - 1 +
        sizeof("edge_type") - 1 + sizeof("edge_id") - 1 +
        sizeof("valid_from") - 1 + sizeof("commit_seq") - 1;
    for (const PointVariablePath& path : paths) {
      output_bytes += sizeof(ListValue) +
          static_cast<uint64_t>(path.path_edges.size()) *
              (sizeof(StructValue) + 6 * sizeof(StructField) +
               kRelationshipFieldNameBytes);
    }
    if (context.options.memory_account) {
      const Status reserved =
          context.options.memory_account->Reserve(output_bytes);
      if (!reserved.ok()) return reserved;
    }
    auto output_lease = std::make_shared<RuntimeMemoryLease>(
        context.options.memory_account, output_bytes);
    std::vector<Value> entity_types;
    std::vector<Value> sources_values;
    std::vector<Value> targets_values;
    std::vector<Value> edge_ids;
    std::vector<Value> edge_types;
    std::vector<Value> column_ids;
    std::vector<Value> valid_from_values;
    std::vector<Value> commit_values;
    std::vector<Value> operation_values;
    std::vector<Value> system_time_values;
    std::vector<Value> valid_to_values;
    std::vector<ListValue> path_values;
    for (std::vector<Value>* values :
         {&entity_types, &sources_values, &targets_values, &edge_ids,
          &edge_types, &column_ids, &valid_from_values, &commit_values,
          &operation_values, &system_time_values, &valid_to_values}) {
      values->reserve(paths.size());
    }
    path_values.reserve(paths.size());
    for (const PointVariablePath& path : paths) {
      const PointVariableEdge& edge = path.last_edge;
      entity_types.push_back(Value::Int32(
          static_cast<int32_t>(plan->expand()->direction)));
      sources_values.push_back(Value::Int64(
          static_cast<int64_t>(path.root_id)));
      targets_values.push_back(Value::Int64(
          static_cast<int64_t>(path.current_id)));
      edge_ids.push_back(Value::Int64(static_cast<int64_t>(edge.edge_id)));
      edge_types.push_back(Value::Int32(static_cast<int32_t>(edge.edge_type)));
      column_ids.push_back(Value::Int32(0));
      valid_from_values.push_back(Value::Timestamp(edge.valid_from));
      commit_values.push_back(Value::Int64(
          static_cast<int64_t>(edge.commit_seq)));
      operation_values.push_back(Value::Int32(edge.operation));
      if (plan->include_system_time()) {
        if (!timeline || edge.commit_seq == 0 ||
            edge.commit_seq > timeline->physical_times.size()) {
          return Status::Corruption(
              "physical runtime",
              "variable point commit is absent from CommitTimeline");
        }
        system_time_values.push_back(Value::Timestamp(
            timeline->physical_times[edge.commit_seq - 1]));
      } else {
        system_time_values.push_back(Value::Timestamp(0));
      }
      if (plan->include_valid_to()) {
        TemporalScanSpec boundary_spec = scan_spec;
        boundary_spec.entity_type = plan->expand()->direction;
        boundary_spec.key_kind = LogicalKeyKind::kExistence;
        boundary_spec.edge_type = edge.edge_type;
        const LogicalKey key = LogicalKey::EdgeExistence(
            edge.source_id, edge.target_id, edge.edge_type, edge.edge_id,
            plan->expand()->direction);
        boundary_spec.exact_key = key;
        boundary_spec.allowed_candidate_entity_ids.reset();
        const auto boundary = FindNextPinnedValidBoundary(
            sources, boundary_spec, key, edge.valid_from);
        if (!boundary.ok()) return boundary.status();
        valid_to_values.push_back(Value::Timestamp(
            boundary.ValueOrDie().value_or(
                std::numeric_limits<uint64_t>::max())));
      } else {
        valid_to_values.push_back(Value::Timestamp(
            std::numeric_limits<uint64_t>::max()));
      }
      ListValue path_value;
      path_value.element_kind = ListElementKind::kStruct;
      path_value.structured_elements.reserve(path.path_edges.size());
      for (const PointVariableEdge& path_edge : path.path_edges) {
        path_value.structured_elements.push_back(
            PointVariableEdgeValue(path_edge));
      }
      path_values.push_back(std::move(path_value));
    }
    ColumnBatch expanded(context.options.batch_capacity);
    for (std::vector<Value>* values :
         {&entity_types, &sources_values, &targets_values, &edge_ids,
          &edge_types, &column_ids, &valid_from_values, &commit_values,
          &operation_values, &system_time_values, &valid_to_values}) {
      const Status added = expanded.AddVector(std::make_shared<FlatVector>(
          std::move(*values), std::vector<bool>{}, output_lease));
      if (!added.ok()) return added;
    }
    const Status path_added = expanded.AddVector(std::make_shared<ListVector>(
        std::move(path_values), std::vector<bool>{}, output_lease));
    if (!path_added.ok()) return path_added;
    std::map<SlotId, uint32_t> layout{
        {plan->expand()->source_slot, 1},
        {plan->expand()->target_slot, 2},
        {plan->expand()->edge_id_slot, 3},
        {plan->expand()->edge_type_slot, 4},
        {plan->expand()->valid_from_slot, 6},
        {plan->expand()->commit_seq_slot, 7},
        {plan->expand()->operation_slot, 8},
        {plan->expand()->system_time_slot, 9},
        {plan->expand()->valid_to_slot, 10}};
    if (plan->expand()->path_slot.value != 0) {
      layout.emplace(plan->expand()->path_slot, 11);
    }
    return ContinuePointExpandPredicates(
        std::move(expanded), std::move(layout));
  }

  Status BeginPointVariableExpand(const ColumnBatch& roots) {
    if (pending_point_variable_expand.has_value()) {
      return Status::Corruption(
          "physical runtime", "variable point frontier overlaps");
    }
    std::vector<PointVariablePath> completed;
    std::shared_ptr<QueryMemoryLease> completed_lease;
    std::unique_ptr<QuerySpillFile> completed_spill;
    const Status built = BuildPointVariablePaths(
        roots, &completed, &completed_lease, &completed_spill);
    if (!built.ok()) return built;
    pending_point_variable_expand.emplace(PendingPointVariableExpand{
        std::move(completed), std::move(completed_lease),
        std::move(completed_spill), 0, false});
    return ContinuePointVariableExpand();
  }

  Status ContinuePointVariableExpand() {
    if (!pending_point_variable_expand.has_value()) {
      return Status::Corruption(
          "physical runtime", "variable point frontier is absent");
    }
    PendingPointVariableExpand& pending = *pending_point_variable_expand;
    if (!pending.completed_spill &&
        pending.next == pending.completed.size()) {
      pending_point_variable_expand.reset();
      return Status::OK();
    }
    std::vector<PointVariablePath> output;
    output.reserve(context.options.batch_capacity);
    auto output_lease = std::make_shared<QueryMemoryLease>(
        context.options.memory_account, 0);
    if (pending.completed_spill) {
      if (!pending.spill_rewound) {
        const Status rewound = pending.completed_spill->Rewind();
        if (!rewound.ok()) return rewound;
        pending.spill_rewound = true;
      }
      while (output.size() < context.options.batch_capacity) {
        ResultBatch batch;
        const Status next = pending.completed_spill->Next(&batch);
        if (next.IsNotFound()) {
          pending.completed_spill.reset();
          break;
        }
        if (!next.ok()) return next;
        auto path = DecodePointVariablePath(batch);
        if (!path.ok()) return path.status();
        const Status reserved = output_lease->ReserveAdditional(
            PointVariablePathBytes(path.ValueOrDie()));
        if (!reserved.ok()) return reserved;
        output.push_back(std::move(path).ConsumeValueOrDie());
      }
    } else {
      const size_t count = std::min<size_t>(
          context.options.batch_capacity,
          pending.completed.size() - pending.next);
      for (size_t index = 0; index < count; ++index) {
        const PointVariablePath& path = pending.completed[pending.next + index];
        const Status reserved = output_lease->ReserveAdditional(
            PointVariablePathBytes(path));
        if (!reserved.ok()) return reserved;
        output.push_back(path);
      }
      pending.next += count;
    }
    if (output.empty()) {
      pending_point_variable_expand.reset();
      return Status::OK();
    }
    return PublishPointVariablePaths(std::move(output));
  }

  struct PendingPointExpand {
    TemporalScanCursor edge_scan;
  };

  struct PendingMultiHopExpand {
    TemporalScanCursor edge_scan;
    size_t hop = 0;
    ColumnBatch prefix;
    std::map<SlotId, uint32_t> layout;
    std::optional<ColumnBatch> edges;
    std::set<uint64_t> visible_targets;
    uint32_t edge_row = 0;
    uint32_t prefix_row = 0;
  };

  struct PendingMultiHopPropertyGather {
    ColumnBatch batch;
    std::map<SlotId, uint32_t> layout;
    std::vector<PhysicalPropertySlot> properties;
    PinnedPropertyGatherCursor cursor;
    bool predicate_phase = false;
  };

  Status BeginMultiHopPropertyGather(
      ColumnBatch batch, std::map<SlotId, uint32_t> layout,
      std::vector<PhysicalPropertySlot> properties, bool predicate_phase) {
    if (properties.empty()) {
      return Status::InvalidArgument("physical runtime", "multi-hop gather has no properties");
    }
    const BindingId binding = properties.front().binding;
    if (std::any_of(properties.begin(), properties.end(), [binding](const auto& property) {
          return property.binding != binding;
        })) {
      return Status::Corruption("physical runtime", "multi-hop gather mixes bindings");
    }
    ColumnBatch compacted;
    const Status compacted_status = CompactSelectedExpandBatch(
        std::move(batch), &compacted);
    if (!compacted_status.ok()) return compacted_status;
    batch = std::move(compacted);
    AddOperatorInput(PhysicalOperatorKind::kPropertyGather,
                     batch.row_count(), !predicate_phase);
    const PhysicalExpandSpec* relationship = nullptr;
    std::optional<SlotId> node_slot;
    for (const PhysicalExpandSpec& step : plan->expand_steps()) {
      if (step.relationship_binding == binding) relationship = &step;
      if (step.source_binding == binding) node_slot = step.source_slot;
      if (step.target_binding == binding) node_slot = step.target_slot;
    }
    if ((relationship == nullptr && !node_slot.has_value()) ||
        (relationship != nullptr && node_slot.has_value())) {
      return Status::Corruption("physical runtime", "multi-hop property binding is invalid");
    }
    ColumnBatch identities(batch.row_count());
    std::vector<Value> entity_types;
    std::vector<Value> entity_ids;
    std::vector<Value> target_ids;
    std::vector<Value> edge_ids;
    std::vector<Value> edge_types;
    entity_types.reserve(batch.row_count());
    entity_ids.reserve(batch.row_count());
    target_ids.reserve(batch.row_count());
    edge_ids.reserve(batch.row_count());
    edge_types.reserve(batch.row_count());
    for (uint32_t row = 0; row < batch.row_count(); ++row) {
      if (relationship != nullptr) {
        const auto source_column = layout.find(relationship->source_slot);
        const auto target_column = layout.find(relationship->target_slot);
        const auto edge_id_column = layout.find(relationship->edge_id_slot);
        const auto edge_type_column = layout.find(relationship->edge_type_slot);
        if (source_column == layout.end() || target_column == layout.end() ||
            edge_id_column == layout.end() || edge_type_column == layout.end()) {
          return Status::Corruption("physical runtime", "multi-hop edge property layout is incomplete");
        }
        const Value* source = batch.ValueRefAt(source_column->second, row);
        const Value* target = batch.ValueRefAt(target_column->second, row);
        const Value* edge_id = batch.ValueRefAt(edge_id_column->second, row);
        const Value* edge_type = batch.ValueRefAt(edge_type_column->second, row);
        if (source == nullptr || target == nullptr || edge_id == nullptr || edge_type == nullptr) {
          return Status::Corruption("physical runtime", "multi-hop edge property identity is null");
        }
        entity_types.push_back(Value::Int32(static_cast<int32_t>(relationship->direction)));
        entity_ids.push_back(*source);
        target_ids.push_back(*target);
        edge_ids.push_back(*edge_id);
        edge_types.push_back(*edge_type);
      } else {
        const auto id_column = layout.find(*node_slot);
        if (id_column == layout.end()) {
          return Status::Corruption("physical runtime", "multi-hop node property layout is incomplete");
        }
        const Value* id = batch.ValueRefAt(id_column->second, row);
        if (id == nullptr) {
          return Status::Corruption("physical runtime", "multi-hop node property identity is null");
        }
        entity_types.push_back(Value::Int32(static_cast<int32_t>(EntityType::Vertex)));
        entity_ids.push_back(*id);
        target_ids.push_back(Value::Int64(0));
        edge_ids.push_back(Value::Int64(0));
        edge_types.push_back(Value::Int32(0));
      }
    }
    for (std::vector<Value>* values :
         {&entity_types, &entity_ids, &target_ids, &edge_ids, &edge_types}) {
      const Status added = identities.AddVector(std::make_shared<FlatVector>(
          std::move(*values), std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    TemporalScanSpec property_spec{valid_time, snapshot_seq, context.options.batch_capacity};
    property_spec.cancellation = context.options.cancellation;
    property_spec.memory_account = context.options.memory_account;
    AttachBlobMaterialization(&property_spec, !predicate_phase);
    AttachScanStats(context.options.execution_stats, true, &property_spec);
    std::vector<uint32_t> column_ids;
    std::vector<uint32_t> epochs;
    column_ids.reserve(properties.size());
    epochs.reserve(properties.size());
    for (const PhysicalPropertySlot& property : properties) {
      column_ids.push_back(property.column.column_id);
      epochs.push_back(property.column.schema_epoch);
    }
    PropertyGatherSpec gather_spec{
        std::move(column_ids), valid_time, snapshot_seq, std::move(epochs)};
    if (predicate_phase) {
      auto probes = BuildBlobPredicateProbes(properties);
      if (!probes.ok()) return probes.status();
      gather_spec.blob_predicate_probes =
          std::move(probes).ConsumeValueOrDie();
    }
    if (!predicate_phase && context.options.execution_stats) {
      const auto stats = context.options.execution_stats;
      gather_spec.payload_copy_observer = [stats](uint64_t bytes) {
        stats->projection_gather_payload_bytes_copied += bytes;
      };
    }
    auto opened = OpenPinnedPropertyGather(
        std::move(identities), sources, std::move(property_spec), std::move(gather_spec));
    if (!opened.ok()) return opened.status();
    pending_multi_hop_property_gather.emplace(PendingMultiHopPropertyGather{
        std::move(batch), std::move(layout), std::move(properties),
        std::move(opened).ConsumeValueOrDie(), predicate_phase});
    return Status::OK();
  }

  Status ContinueMultiHopPropertyGather() {
    if (!pending_multi_hop_property_gather.has_value()) {
      return Status::Corruption("physical runtime", "multi-hop property gather is absent");
    }
    uint32_t completed = 0;
    const auto gather_start = std::chrono::steady_clock::now();
    const Status advanced = pending_multi_hop_property_gather->cursor.Advance(
        kPropertyLookupQuantum, &completed);
    AddOperatorBlocked(
        PhysicalOperatorKind::kPropertyGather, ElapsedNs(gather_start),
        !pending_multi_hop_property_gather->predicate_phase);
    if (!advanced.ok()) return advanced;
    if (!pending_multi_hop_property_gather->cursor.done()) return Status::OK();
    ColumnBatch gathered;
    const Status finished = pending_multi_hop_property_gather->cursor.Finish(&gathered);
    if (!finished.ok()) return finished;
    PendingMultiHopPropertyGather state = std::move(*pending_multi_hop_property_gather);
    pending_multi_hop_property_gather.reset();
    RecordOperatorOutput(PhysicalOperatorKind::kPropertyGather,
                         gathered.row_count(), !state.predicate_phase);
    const uint32_t base = state.batch.column_count();
    for (size_t index = 0; index < state.properties.size(); ++index) {
      const auto vector = gathered.VectorAt(5 + static_cast<uint32_t>(index));
      if (!vector) {
        return Status::Corruption("physical runtime", "multi-hop property vector is absent");
      }
      const Status appended = state.batch.AddVector(vector);
      if (!appended.ok()) return appended;
      state.layout.emplace(state.properties[index].slot,
                           base + static_cast<uint32_t>(index));
    }
    if (state.predicate_phase) {
      return ContinueMultiHopPredicateProperties(
          std::move(state.batch), std::move(state.layout));
    }
    return ContinueMultiHopProjectionProperties(
        std::move(state.batch), std::move(state.layout));
  }

  Status ContinueMultiHopPredicateProperties(
      ColumnBatch batch, std::map<SlotId, uint32_t> layout) {
    for (const PhysicalPropertySlot& first : plan->predicate_properties()) {
      if (layout.count(first.slot) != 0) continue;
      std::vector<PhysicalPropertySlot> properties;
      for (const PhysicalPropertySlot& property : plan->predicate_properties()) {
        if (property.binding == first.binding && layout.count(property.slot) == 0) {
          properties.push_back(property);
        }
      }
      return BeginMultiHopPropertyGather(
          std::move(batch), std::move(layout), std::move(properties), true);
    }
    if (!plan->predicates().empty()) {
      const uint64_t filter_input_rows = batch.row_count();
      ColumnBatch selected;
      const auto filter_start = std::chrono::steady_clock::now();
      const Status filtered = FilterColumnBatch(
          batch, [this, &layout](const ColumnBatch& input, uint32_t row) {
            for (const PhysicalPredicate& predicate : plan->predicates()) {
              const auto column = layout.find(predicate.slot);
              if (column == layout.end()) return false;
              const Value* value = input.ValueRefAt(column->second, row);
              if (value == nullptr || !MatchesPredicate(*value, predicate)) return false;
            }
            return true;
          }, &selected);
      AddOperatorCpu(PhysicalOperatorKind::kFilter,
                     ElapsedNs(filter_start));
      if (!filtered.ok()) return filtered;
      RecordOperatorBatch(PhysicalOperatorKind::kFilter, filter_input_rows,
                          selected.row_count());
      InvalidateHashResolvedProjectionSlots(&layout);
      return ContinueMultiHopProjectionProperties(
          std::move(selected), std::move(layout));
    }
    InvalidateHashResolvedProjectionSlots(&layout);
    return ContinueMultiHopProjectionProperties(std::move(batch), std::move(layout));
  }

  Status ContinueMultiHopProjectionProperties(
      ColumnBatch batch, std::map<SlotId, uint32_t> layout) {
    for (const PhysicalPropertySlot& first : plan->projection_properties()) {
      if (layout.count(first.slot) != 0) continue;
      std::vector<PhysicalPropertySlot> properties;
      for (const PhysicalPropertySlot& property : plan->projection_properties()) {
        if (property.binding == first.binding && layout.count(property.slot) == 0) {
          properties.push_back(property);
        }
      }
      return BeginMultiHopPropertyGather(
          std::move(batch), std::move(layout), std::move(properties), false);
    }
    return ProjectAndEnqueue(std::move(batch), layout);
  }

  Status BeginMultiHopExpand(ColumnBatch prefix, std::map<SlotId, uint32_t> layout,
                             size_t hop) {
    if (hop >= plan->expand_steps().size()) {
      return Status::Corruption("physical runtime", "multi-hop expand exceeds plan steps");
    }
    const PhysicalExpandSpec& step = plan->expand_steps()[hop];
    const auto source_column = layout.find(step.source_slot);
    if (source_column == layout.end()) {
      return Status::Corruption("physical runtime", "multi-hop source slot is absent");
    }
    AddOperatorInputAt(PhysicalOperatorKind::kExpand, hop,
                       prefix.row_count());
    auto source_ids = std::make_shared<std::set<uint64_t>>();
    for (uint32_t row = 0; row < prefix.row_count(); ++row) {
      const Value* source = prefix.ValueRefAt(source_column->second, row);
      if (source == nullptr || source->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(source->data()) < 0) {
        return Status::Corruption("physical runtime", "multi-hop source identity is invalid");
      }
      source_ids->insert(static_cast<uint64_t>(std::get<int64_t>(source->data())));
    }
    if (source_ids->empty()) return Status::OK();
    TemporalScanSpec edge_spec{valid_time, snapshot_seq, context.options.batch_capacity};
    edge_spec.entity_type = step.direction;
    edge_spec.key_kind = LogicalKeyKind::kExistence;
    edge_spec.edge_type = step.edge_type;
    edge_spec.allowed_candidate_entity_ids = std::move(source_ids);
    edge_spec.cancellation = context.options.cancellation;
    edge_spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &edge_spec);
    auto opened = OpenPinnedTemporalScan(sources, edge_spec);
    if (!opened.ok()) return opened.status();
    pending_multi_hop_expands.push_back(PendingMultiHopExpand{
        std::move(opened).ConsumeValueOrDie(), hop, std::move(prefix), std::move(layout)});
    return Status::OK();
  }

  Status BeginInitialMultiHopExpand(ColumnBatch roots) {
    if (plan->expand_steps().empty()) {
      return Status::Corruption("physical runtime", "multi-hop plan has no expand steps");
    }
    return BeginMultiHopExpand(
        std::move(roots), {{plan->expand_steps().front().source_slot, kEntityId}}, 0);
  }

  Status ApplyExpandDynamicFilter(ColumnBatch edges,
                                  const PhysicalExpandSpec& step,
                                  ColumnBatch* filtered) {
    if (filtered == nullptr) {
      return Status::InvalidArgument(
          "physical runtime", "expand dynamic-filter output is absent");
    }
    const auto target =
        advisory_candidates_by_binding.find(step.target_binding);
    const auto relationship =
        advisory_candidates_by_binding.find(step.relationship_binding);
    if (target == advisory_candidates_by_binding.end() &&
        relationship == advisory_candidates_by_binding.end()) {
      *filtered = std::move(edges);
      return Status::OK();
    }
    for (uint32_t row = 0; row < edges.row_count(); ++row) {
      if (target != advisory_candidates_by_binding.end()) {
        const Value* target_id = edges.ValueRefAt(kTargetId, row);
        if (target_id == nullptr || target_id->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(target_id->data()) < 0) {
          return Status::Corruption(
              "physical runtime",
              "expand dynamic-filter target identity is invalid");
        }
      }
      if (relationship != advisory_candidates_by_binding.end()) {
        const Value* edge_id = edges.ValueRefAt(kEdgeId, row);
        if (edge_id == nullptr || edge_id->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(edge_id->data()) < 0) {
          return Status::Corruption(
              "physical runtime",
              "expand dynamic-filter relationship identity is invalid");
        }
      }
    }
    const uint64_t input_rows = edges.row_count();
    const Status status = FilterColumnBatch(
        edges,
        [&target, &relationship, this](const ColumnBatch& batch,
                                       uint32_t row) {
          if (target != advisory_candidates_by_binding.end()) {
            const uint64_t target_id = static_cast<uint64_t>(
                std::get<int64_t>(batch.ValueRefAt(kTargetId, row)->data()));
            if (target->second->count(target_id) == 0) return false;
          }
          if (relationship != advisory_candidates_by_binding.end()) {
            const uint64_t edge_id = static_cast<uint64_t>(
                std::get<int64_t>(batch.ValueRefAt(kEdgeId, row)->data()));
            if (relationship->second->count(edge_id) == 0) return false;
          }
          return true;
        },
        filtered);
    if (!status.ok()) return status;
    if (context.options.execution_stats) {
      context.options.execution_stats->index_dynamic_filter_input_rows +=
          input_rows;
      context.options.execution_stats->index_dynamic_filter_output_rows +=
          filtered->row_count();
      context.options.execution_stats->index_dynamic_filter_rejected_rows +=
          input_rows - filtered->row_count();
    }
    return Status::OK();
  }

  Status LoadMultiHopEdges(PendingMultiHopExpand* pending,
                           ScopedOperatorRuntimeTimer* timer) {
    if (pending == nullptr) {
      return Status::Corruption("physical runtime", "multi-hop work is absent");
    }
    if (pending->edges.has_value()) return Status::OK();
    ColumnBatch edges;
    const auto edge_start = std::chrono::steady_clock::now();
    const Status next_edge = pending->edge_scan.NextMorsel(&edges);
    if (timer != nullptr) timer->RecordBlocked(ElapsedNs(edge_start));
    if (next_edge.IsNotFound()) return next_edge;
    if (!next_edge.ok()) return next_edge;
    ColumnBatch filtered_edges;
    const Status dynamic_filtered = ApplyExpandDynamicFilter(
        std::move(edges), plan->expand_steps()[pending->hop],
        &filtered_edges);
    if (!dynamic_filtered.ok()) return dynamic_filtered;
    edges = std::move(filtered_edges);
    auto target_ids = std::make_shared<std::set<uint64_t>>();
    for (uint32_t row = 0; row < edges.row_count(); ++row) {
      const Value* target = edges.ValueRefAt(kTargetId, row);
      if (target == nullptr || target->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(target->data()) < 0) {
        return Status::Corruption("physical runtime", "multi-hop edge target identity is invalid");
      }
      target_ids->insert(static_cast<uint64_t>(std::get<int64_t>(target->data())));
    }
    TemporalScanSpec target_spec{valid_time, snapshot_seq, context.options.batch_capacity};
    target_spec.entity_type = EntityType::Vertex;
    target_spec.key_kind = LogicalKeyKind::kExistence;
    target_spec.allowed_candidate_entity_ids = std::move(target_ids);
    target_spec.cancellation = context.options.cancellation;
    target_spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &target_spec);
    auto opened_targets = OpenPinnedTemporalScan(sources, target_spec);
    if (!opened_targets.ok()) return opened_targets.status();
    TemporalScanCursor targets = std::move(opened_targets).ConsumeValueOrDie();
    pending->visible_targets.clear();
    for (;;) {
      ColumnBatch target_batch;
      const auto target_start = std::chrono::steady_clock::now();
      const Status next_target = targets.NextMorsel(&target_batch);
      if (timer != nullptr) timer->RecordBlocked(ElapsedNs(target_start));
      if (next_target.IsNotFound()) break;
      if (!next_target.ok()) return next_target;
      for (uint32_t row = 0; row < target_batch.row_count(); ++row) {
        const Value* target = target_batch.ValueRefAt(kEntityId, row);
        if (target == nullptr || target->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(target->data()) < 0) {
          return Status::Corruption("physical runtime", "multi-hop target identity is invalid");
        }
        pending->visible_targets.insert(
            static_cast<uint64_t>(std::get<int64_t>(target->data())));
      }
    }
    pending->edges.emplace(std::move(edges));
    pending->edge_row = 0;
    pending->prefix_row = 0;
    return Status::OK();
  }

  Status ContinueMultiHopExpand() {
    if (pending_multi_hop_expands.empty()) {
      return Status::Corruption("physical runtime", "multi-hop queue is empty");
    }
    PendingMultiHopExpand& pending = pending_multi_hop_expands.front();
    const auto stats = context.options.execution_stats;
    ScopedOperatorRuntimeTimer timer(
        stats ? stats->operator_runtime : nullptr,
        OperatorKeyForOccurrence(PhysicalOperatorKind::kExpand, pending.hop));
    const Status loaded = LoadMultiHopEdges(&pending, &timer);
    if (loaded.IsNotFound()) {
      pending_multi_hop_expands.pop_front();
      return Status::OK();
    }
    if (!loaded.ok()) return loaded;
    const PhysicalExpandSpec& step = plan->expand_steps()[pending.hop];
    std::vector<std::pair<SlotId, uint32_t>> previous_slots(
        pending.layout.begin(), pending.layout.end());
    std::sort(previous_slots.begin(), previous_slots.end(),
              [](const auto& left, const auto& right) { return left.first < right.first; });
    const std::vector<SlotId> appended_slots{
        step.target_slot, step.edge_type_slot, step.edge_id_slot, step.valid_from_slot,
        step.commit_seq_slot, step.operation_slot, step.system_time_slot, step.valid_to_slot};
    std::vector<std::vector<Value>> columns(previous_slots.size() + appended_slots.size());
    for (auto& values : columns) values.reserve(context.options.batch_capacity);
    uint32_t output_rows = 0;
    while (output_rows < context.options.batch_capacity &&
           pending.edge_row < pending.edges->row_count()) {
      const Value* source = pending.edges->ValueRefAt(kEntityId, pending.edge_row);
      const Value* target = pending.edges->ValueRefAt(kTargetId, pending.edge_row);
      const Value* edge_type = pending.edges->ValueRefAt(kEdgeType, pending.edge_row);
      const Value* edge_id = pending.edges->ValueRefAt(kEdgeId, pending.edge_row);
      const Value* valid_from = pending.edges->ValueRefAt(kValidFrom, pending.edge_row);
      const Value* commit_seq = pending.edges->ValueRefAt(kCommitSeq, pending.edge_row);
      const Value* operation = pending.edges->ValueRefAt(kOperation, pending.edge_row);
      if (source == nullptr || target == nullptr || edge_type == nullptr || edge_id == nullptr ||
          valid_from == nullptr || commit_seq == nullptr || operation == nullptr ||
          source->type() != PhysicalType::kInt64 || target->type() != PhysicalType::kInt64 ||
          edge_type->type() != PhysicalType::kInt32 || edge_id->type() != PhysicalType::kInt64 ||
          valid_from->type() != PhysicalType::kTimestamp64 ||
          commit_seq->type() != PhysicalType::kInt64 || operation->type() != PhysicalType::kInt32 ||
          std::get<int64_t>(source->data()) < 0 || std::get<int64_t>(target->data()) < 0) {
        return Status::Corruption("physical runtime", "multi-hop edge identity is invalid");
      }
      if (pending.visible_targets.count(
              static_cast<uint64_t>(std::get<int64_t>(target->data()))) == 0) {
        ++pending.edge_row;
        pending.prefix_row = 0;
        continue;
      }
      Value system_time = Value::Timestamp(0);
      if (plan->include_system_time()) {
        if (!timeline || std::get<int64_t>(commit_seq->data()) <= 0 ||
            static_cast<uint64_t>(std::get<int64_t>(commit_seq->data())) >
                timeline->physical_times.size()) {
          return Status::Corruption(
              "physical runtime", "multi-hop commit is absent from CommitTimeline");
        }
        system_time = Value::Timestamp(timeline->physical_times[
            static_cast<size_t>(std::get<int64_t>(commit_seq->data()) - 1)]);
      }
      Value valid_to = Value::Timestamp(std::numeric_limits<uint64_t>::max());
      if (plan->include_valid_to()) {
        const int32_t type = std::get<int32_t>(edge_type->data());
        const int64_t id = std::get<int64_t>(edge_id->data());
        if (type < 0 || type > std::numeric_limits<uint16_t>::max() || id < 0) {
          return Status::Corruption("physical runtime", "multi-hop edge key is invalid");
        }
        const LogicalKey key = LogicalKey::EdgeExistence(
            static_cast<uint64_t>(std::get<int64_t>(source->data())),
            static_cast<uint64_t>(std::get<int64_t>(target->data())),
            static_cast<uint16_t>(type), static_cast<uint64_t>(id), step.direction);
        TemporalScanSpec boundary_spec = scan_spec;
        boundary_spec.entity_type = step.direction;
        boundary_spec.key_kind = LogicalKeyKind::kExistence;
        boundary_spec.edge_type = static_cast<uint16_t>(type);
        boundary_spec.exact_key = key;
        boundary_spec.allowed_candidate_entity_ids.reset();
        if (context.options.execution_stats) {
          const auto stats = context.options.execution_stats;
          boundary_spec.open_observer = [stats] { ++stats->pinned_boundary_point_scans; };
          boundary_spec.stats_observer = [stats](uint64_t, uint64_t blocks, uint64_t peak) {
            stats->boundary_sst_blocks_read += blocks;
            stats->max_sst_cursor_buffered_events = std::max(
                stats->max_sst_cursor_buffered_events, peak);
          };
          AttachStorageStats(stats, &boundary_spec);
        }
        const auto boundary = FindNextPinnedValidBoundary(
            sources, boundary_spec, key, std::get<uint64_t>(valid_from->data()));
        if (!boundary.ok()) return boundary.status();
        valid_to = Value::Timestamp(boundary.ValueOrDie().value_or(
            std::numeric_limits<uint64_t>::max()));
      }
      const uint32_t source_column = pending.layout.at(step.source_slot);
      while (output_rows < context.options.batch_capacity &&
             pending.prefix_row < pending.prefix.row_count()) {
        const uint32_t prefix_row = pending.prefix_row++;
        const Value* prefix_source = pending.prefix.ValueRefAt(source_column, prefix_row);
        if (prefix_source == nullptr || prefix_source->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(prefix_source->data()) != std::get<int64_t>(source->data())) {
          continue;
        }
        for (size_t column = 0; column < previous_slots.size(); ++column) {
          const Value* value = pending.prefix.ValueRefAt(previous_slots[column].second, prefix_row);
          if (value == nullptr) {
            return Status::Corruption("physical runtime", "multi-hop prefix slot is null");
          }
          columns[column].push_back(*value);
        }
        columns[previous_slots.size() + 0].push_back(*target);
        columns[previous_slots.size() + 1].push_back(*edge_type);
        columns[previous_slots.size() + 2].push_back(*edge_id);
        columns[previous_slots.size() + 3].push_back(*valid_from);
        columns[previous_slots.size() + 4].push_back(*commit_seq);
        columns[previous_slots.size() + 5].push_back(*operation);
        columns[previous_slots.size() + 6].push_back(system_time);
        columns[previous_slots.size() + 7].push_back(valid_to);
        ++output_rows;
      }
      if (pending.prefix_row == pending.prefix.row_count()) {
        ++pending.edge_row;
        pending.prefix_row = 0;
      }
    }
    if (pending.edge_row == pending.edges->row_count()) {
      pending.edges.reset();
      pending.visible_targets.clear();
    }
    if (output_rows == 0) return Status::OK();
    ColumnBatch expanded(context.options.batch_capacity);
    for (auto& values : columns) {
      const Status added = expanded.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    std::map<SlotId, uint32_t> layout;
    for (size_t index = 0; index < previous_slots.size(); ++index) {
      layout.emplace(previous_slots[index].first, static_cast<uint32_t>(index));
    }
    for (size_t index = 0; index < appended_slots.size(); ++index) {
      layout.emplace(appended_slots[index],
                     static_cast<uint32_t>(previous_slots.size() + index));
    }
    RecordOperatorOutputAt(PhysicalOperatorKind::kExpand, pending.hop,
                           expanded.row_count());
    if (pending.hop + 1 < plan->expand_steps().size()) {
      PendingMultiHopExpand parent = std::move(pending_multi_hop_expands.front());
      pending_multi_hop_expands.pop_front();
      timer.Finish();
      const Status started = BeginMultiHopExpand(
          std::move(expanded), std::move(layout), parent.hop + 1);
      pending_multi_hop_expands.push_back(std::move(parent));
      return started;
    }
    timer.Finish();
    return ContinueMultiHopPredicateProperties(std::move(expanded), std::move(layout));
  }

  struct PendingTargetPropertyGather {
    ColumnBatch edge_batch;
    std::map<SlotId, uint32_t> layout;
    std::vector<PhysicalPropertySlot> properties;
    PinnedPropertyGatherCursor cursor;
    bool predicate_phase = false;
    uint32_t gathered_property_base = 5;
  };

  Status CompactSelectedExpandBatch(ColumnBatch input, ColumnBatch* compacted) {
    if (compacted == nullptr) {
      return Status::InvalidArgument("physical runtime", "compact expand output is absent");
    }
    if (input.source_row_count() == input.row_count()) {
      *compacted = std::move(input);
      return Status::OK();
    }
    const uint64_t row_count = input.row_count();
    const uint64_t column_count = input.column_count();
    if (column_count != 0 &&
        row_count > (std::numeric_limits<uint64_t>::max() -
                     column_count * sizeof(FlatVector)) /
                        (column_count * (sizeof(Value) + sizeof(bool)))) {
      return Status::QueryMemoryLimit("physical runtime", "compact expand batch charge overflow");
    }
    uint64_t charge = column_count * sizeof(FlatVector) +
        column_count * row_count * (sizeof(Value) + sizeof(bool));
    for (uint32_t column = 0; column < input.column_count(); ++column) {
      for (uint32_t row = 0; row < input.row_count(); ++row) {
        const uint64_t payload = input.RetainedPayloadBytesAt(column, row);
        if (charge > std::numeric_limits<uint64_t>::max() - payload) {
          return Status::QueryMemoryLimit("physical runtime", "compact expand payload charge overflow");
        }
        charge += payload;
      }
    }
    if (context.options.memory_account) {
      const Status reserved = context.options.memory_account->Reserve(charge);
      if (!reserved.ok()) return reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(context.options.memory_account, charge);
    ColumnBatch result(input.row_count());
    for (uint32_t column = 0; column < input.column_count(); ++column) {
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(input.row_count());
      validity.reserve(input.row_count());
      for (uint32_t row = 0; row < input.row_count(); ++row) {
        const Value* value = input.ValueRefAt(column, row);
        validity.push_back(value != nullptr);
        values.push_back(value != nullptr ? *value : Value::Bool(false));
      }
      const Status added = result.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::move(validity), lease));
      if (!added.ok()) return added;
    }
    *compacted = std::move(result);
    return Status::OK();
  }

  Status BeginTargetPropertyGather(
      ColumnBatch edge_batch, std::map<SlotId, uint32_t> layout,
      std::vector<PhysicalPropertySlot> properties, bool predicate_phase) {
    if (properties.empty()) {
      return Status::InvalidArgument("physical runtime", "target gather has no properties");
    }
    ColumnBatch compacted;
    const Status compacted_status = CompactSelectedExpandBatch(
        std::move(edge_batch), &compacted);
    if (!compacted_status.ok()) return compacted_status;
    edge_batch = std::move(compacted);
    AddOperatorInput(PhysicalOperatorKind::kPropertyGather,
                     edge_batch.row_count(), !predicate_phase);
    ColumnBatch targets(edge_batch.row_count());
    std::vector<Value> types;
    std::vector<Value> ids;
    std::vector<Value> empty_targets;
    std::vector<Value> empty_edge_ids;
    std::vector<Value> empty_edge_types;
    std::vector<Value> row_valid_times;
    types.reserve(edge_batch.row_count());
    ids.reserve(edge_batch.row_count());
    empty_targets.reserve(edge_batch.row_count());
    empty_edge_ids.reserve(edge_batch.row_count());
    empty_edge_types.reserve(edge_batch.row_count());
    const bool event_time_gather =
        plan->temporal_mode() == PhysicalTemporalMode::kValidTimeChanges ||
        plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges;
    if (event_time_gather) row_valid_times.reserve(edge_batch.row_count());
    const BindingId binding = properties.front().binding;
    const uint32_t identity_column = binding == plan->expand()->source_binding
        ? kEntityId : binding == plan->expand()->target_binding ? kTargetId : UINT32_MAX;
    if (identity_column == UINT32_MAX || std::any_of(
            properties.begin(), properties.end(), [binding](const PhysicalPropertySlot& property) {
              return property.binding != binding;
            })) {
      return Status::Corruption("physical runtime", "node gather binding is invalid");
    }
    for (uint32_t row = 0; row < edge_batch.row_count(); ++row) {
      const Value* entity = edge_batch.ValueRefAt(identity_column, row);
      if (entity == nullptr || entity->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(entity->data()) < 0) {
        return Status::Corruption("physical runtime", "node gather identity is invalid");
      }
      types.push_back(Value::Int32(static_cast<int32_t>(EntityType::Vertex)));
      ids.push_back(*entity);
      empty_targets.push_back(Value::Int64(0));
      empty_edge_ids.push_back(Value::Int64(0));
      empty_edge_types.push_back(Value::Int32(0));
      if (event_time_gather) {
        const auto valid_column = layout.find(plan->expand()->valid_from_slot);
        if (valid_column == layout.end()) {
          return Status::Corruption(
              "physical runtime",
              "relationship change node gather lacks valid time");
        }
        const Value* row_valid =
            edge_batch.ValueRefAt(valid_column->second, row);
        if (row_valid == nullptr ||
            row_valid->type() != PhysicalType::kTimestamp64) {
          return Status::Corruption(
              "physical runtime",
              "relationship change node gather valid time is invalid");
        }
        row_valid_times.push_back(*row_valid);
      }
    }
    for (std::vector<Value>* values : {&types, &ids, &empty_targets,
                                       &empty_edge_ids, &empty_edge_types}) {
      const Status added = targets.AddVector(std::make_shared<FlatVector>(
          std::move(*values), std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    if (event_time_gather) {
      const Status added = targets.AddVector(std::make_shared<FlatVector>(
          std::move(row_valid_times), std::vector<bool>{}));
      if (!added.ok()) return added;
    }
    TemporalScanSpec property_spec{valid_time, snapshot_seq, context.options.batch_capacity};
    property_spec.cancellation = context.options.cancellation;
    property_spec.memory_account = context.options.memory_account;
    AttachBlobMaterialization(&property_spec, !predicate_phase);
    AttachScanStats(context.options.execution_stats, true, &property_spec);
    std::vector<uint32_t> column_ids;
    std::vector<uint32_t> epochs;
    column_ids.reserve(properties.size());
    epochs.reserve(properties.size());
    for (const PhysicalPropertySlot& property : properties) {
      column_ids.push_back(property.column.column_id);
      epochs.push_back(property.column.schema_epoch);
    }
    PropertyGatherSpec gather_spec{
        std::move(column_ids), valid_time, snapshot_seq, std::move(epochs)};
    if (predicate_phase) {
      auto probes = BuildBlobPredicateProbes(properties);
      if (!probes.ok()) return probes.status();
      gather_spec.blob_predicate_probes =
          std::move(probes).ConsumeValueOrDie();
    }
    if (!predicate_phase && context.options.execution_stats) {
      const auto stats = context.options.execution_stats;
      gather_spec.payload_copy_observer = [stats](uint64_t bytes) {
        stats->projection_gather_payload_bytes_copied += bytes;
      };
    }
    if (event_time_gather) gather_spec.valid_time_column = 5;
    const uint32_t gathered_property_base = targets.column_count();
    auto opened = OpenPinnedPropertyGather(
        std::move(targets), sources, std::move(property_spec),
        std::move(gather_spec));
    if (!opened.ok()) return opened.status();
    pending_target_gather.emplace(PendingTargetPropertyGather{
        std::move(edge_batch), std::move(layout), std::move(properties),
        std::move(opened).ConsumeValueOrDie(), predicate_phase,
        gathered_property_base});
    return Status::OK();
  }

  Status ContinueTargetPropertyGather() {
    if (!pending_target_gather.has_value()) {
      return Status::Corruption("physical runtime", "target gather state is absent");
    }
    uint32_t completed = 0;
    const auto gather_start = std::chrono::steady_clock::now();
    const Status advanced = pending_target_gather->cursor.Advance(
        kPropertyLookupQuantum, &completed);
    AddOperatorBlocked(
        PhysicalOperatorKind::kPropertyGather, ElapsedNs(gather_start),
        !pending_target_gather->predicate_phase);
    if (!advanced.ok()) return advanced;
    if (!pending_target_gather->cursor.done()) return Status::OK();
    ColumnBatch gathered;
    const Status finished = pending_target_gather->cursor.Finish(&gathered);
    if (!finished.ok()) return finished;
    PendingTargetPropertyGather state = std::move(*pending_target_gather);
    pending_target_gather.reset();
    RecordOperatorOutput(PhysicalOperatorKind::kPropertyGather,
                         gathered.row_count(), !state.predicate_phase);
    const uint32_t base = state.edge_batch.column_count();
    for (size_t index = 0; index < state.properties.size(); ++index) {
      const auto vector = gathered.VectorAt(
          state.gathered_property_base + static_cast<uint32_t>(index));
      if (!vector) {
        return Status::Corruption("physical runtime", "target gather value vector is absent");
      }
      const Status appended = state.edge_batch.AddVector(vector);
      if (!appended.ok()) return appended;
      state.layout[state.properties[index].slot] = base + static_cast<uint32_t>(index);
    }
    if (state.predicate_phase) {
      const uint64_t filter_input_rows = state.edge_batch.row_count();
      ColumnBatch selected;
      const auto filter_start = std::chrono::steady_clock::now();
      const Status filtered = FilterColumnBatch(
          state.edge_batch, [this, &state](const ColumnBatch& batch, uint32_t row) {
            for (const PhysicalPredicate& predicate : plan->predicates()) {
              const auto column = state.layout.find(predicate.slot);
              if (column == state.layout.end()) continue;
              const Value* value = batch.ValueRefAt(column->second, row);
              if (value == nullptr || !MatchesPredicate(*value, predicate)) return false;
            }
            return true;
          }, &selected);
      AddOperatorCpu(PhysicalOperatorKind::kFilter,
                     ElapsedNs(filter_start));
      if (!filtered.ok()) return filtered;
      RecordOperatorBatch(PhysicalOperatorKind::kFilter, filter_input_rows,
                          selected.row_count());
      return ContinuePointExpandPredicates(
          std::move(selected), std::move(state.layout));
    }
    return ContinuePointExpandProjections(
        std::move(state.edge_batch), std::move(state.layout));
  }

  Status ContinuePointExpandPredicates(
      ColumnBatch expanded, std::map<SlotId, uint32_t> layout) {
    if (!plan->expand().has_value()) {
      return Status::Corruption("physical runtime", "expand predicate continuation lacks expand plan");
    }
    const std::array<BindingId, 3> bindings{
        plan->expand()->source_binding, plan->expand()->relationship_binding,
        plan->expand()->target_binding};
    for (const BindingId binding : bindings) {
      std::vector<PhysicalPropertySlot> missing;
      for (const PhysicalPropertySlot& property : plan->predicate_properties()) {
        if (property.binding == binding && layout.count(property.slot) == 0) {
          missing.push_back(property);
        }
      }
      if (missing.empty()) continue;
      if (binding == plan->expand()->relationship_binding) {
        return BeginGather(std::move(expanded), std::move(missing),
                           GatherPhase::kPredicate, std::move(layout));
      }
      return BeginTargetPropertyGather(
          std::move(expanded), std::move(layout), std::move(missing), true);
    }
    InvalidateHashResolvedProjectionSlots(&layout);
    return ContinuePointExpandProjections(std::move(expanded), std::move(layout));
  }

  Status ContinuePointExpandProjections(
      ColumnBatch expanded, std::map<SlotId, uint32_t> layout) {
    if (!plan->expand().has_value()) {
      return Status::Corruption("physical runtime", "expand projection continuation lacks expand plan");
    }
    const std::array<BindingId, 3> bindings{
        plan->expand()->relationship_binding, plan->expand()->source_binding,
        plan->expand()->target_binding};
    for (const BindingId binding : bindings) {
      std::vector<PhysicalPropertySlot> missing;
      for (const PhysicalPropertySlot& property : plan->projection_properties()) {
        if (property.binding == binding && layout.count(property.slot) == 0) {
          missing.push_back(property);
        }
      }
      if (missing.empty()) continue;
      if (binding == plan->expand()->relationship_binding) {
        return BeginGather(std::move(expanded), std::move(missing),
                           GatherPhase::kProjection, std::move(layout));
      }
      return BeginTargetPropertyGather(
          std::move(expanded), std::move(layout), std::move(missing), false);
    }
    return ProjectAndEnqueue(std::move(expanded), std::move(layout));
  }

  Status BeginPointExpand(const ColumnBatch& sources_batch) {
    if (!plan->expand().has_value()) {
      return Status::Corruption("physical runtime", "expand state is absent from plan");
    }
    AddOperatorInput(PhysicalOperatorKind::kExpand, sources_batch.row_count());
    auto source_ids = std::make_shared<std::set<uint64_t>>();
    for (uint32_t row = 0; row < sources_batch.row_count(); ++row) {
      const Value* source = sources_batch.ValueRefAt(kEntityId, row);
      if (source == nullptr || source->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(source->data()) < 0) {
        return Status::Corruption("physical runtime", "expand source identity is invalid");
      }
      source_ids->insert(static_cast<uint64_t>(std::get<int64_t>(source->data())));
    }
    if (source_ids->empty()) return Status::OK();
    TemporalScanSpec edge_spec{valid_time, snapshot_seq, context.options.batch_capacity};
    edge_spec.entity_type = plan->expand()->direction;
    edge_spec.key_kind = LogicalKeyKind::kExistence;
    edge_spec.edge_type = plan->expand()->edge_type;
    edge_spec.allowed_candidate_entity_ids = std::move(source_ids);
    edge_spec.cancellation = context.options.cancellation;
    edge_spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &edge_spec);
    auto opened = OpenPinnedTemporalScan(sources, edge_spec);
    if (!opened.ok()) return opened.status();
    pending_expand.emplace(PendingPointExpand{std::move(opened).ConsumeValueOrDie()});
    return Status::OK();
  }

  Status ContinuePointExpand() {
    if (!pending_expand.has_value() || !plan->expand().has_value()) {
      return Status::Corruption("physical runtime", "expand continuation is incomplete");
    }
    const auto stats = context.options.execution_stats;
    ScopedOperatorRuntimeTimer timer(
        stats ? stats->operator_runtime : nullptr,
        OperatorKeyFor(PhysicalOperatorKind::kExpand));
    ColumnBatch edges;
    const auto edge_start = std::chrono::steady_clock::now();
    const Status next_edge = pending_expand->edge_scan.NextMorsel(&edges);
    timer.RecordBlocked(ElapsedNs(edge_start));
    if (next_edge.IsNotFound()) {
      pending_expand.reset();
      return Status::OK();
    }
    if (!next_edge.ok()) return next_edge;

    ColumnBatch filtered_edges;
    const Status dynamic_filtered = ApplyExpandDynamicFilter(
        std::move(edges), *plan->expand(), &filtered_edges);
    if (!dynamic_filtered.ok()) return dynamic_filtered;
    edges = std::move(filtered_edges);

    auto target_ids = std::make_shared<std::set<uint64_t>>();
    for (uint32_t row = 0; row < edges.row_count(); ++row) {
      const Value* target = edges.ValueRefAt(kTargetId, row);
      if (target == nullptr || target->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(target->data()) < 0) {
        return Status::Corruption("physical runtime", "expand edge target identity is invalid");
      }
      target_ids->insert(static_cast<uint64_t>(std::get<int64_t>(target->data())));
    }
    if (target_ids->empty()) return Status::OK();

    TemporalScanSpec target_spec{valid_time, snapshot_seq, context.options.batch_capacity};
    target_spec.entity_type = EntityType::Vertex;
    target_spec.key_kind = LogicalKeyKind::kExistence;
    target_spec.allowed_candidate_entity_ids = std::move(target_ids);
    target_spec.cancellation = context.options.cancellation;
    target_spec.memory_account = context.options.memory_account;
    AttachScanStats(context.options.execution_stats, false, &target_spec);
    auto opened_targets = OpenPinnedTemporalScan(sources, target_spec);
    if (!opened_targets.ok()) return opened_targets.status();
    TemporalScanCursor targets = std::move(opened_targets).ConsumeValueOrDie();
    std::set<uint64_t> visible_targets;
    for (;;) {
      ColumnBatch target_batch;
      const auto target_start = std::chrono::steady_clock::now();
      const Status next_target = targets.NextMorsel(&target_batch);
      timer.RecordBlocked(ElapsedNs(target_start));
      if (next_target.IsNotFound()) break;
      if (!next_target.ok()) return next_target;
      for (uint32_t row = 0; row < target_batch.row_count(); ++row) {
        const Value* target = target_batch.ValueRefAt(kEntityId, row);
        if (target == nullptr || target->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(target->data()) < 0) {
          return Status::Corruption("physical runtime", "target scan identity is invalid");
        }
        visible_targets.insert(static_cast<uint64_t>(std::get<int64_t>(target->data())));
      }
    }

    std::vector<Value> entity_type_values;
    std::vector<Value> source_values;
    std::vector<Value> target_values;
    std::vector<Value> edge_type_values;
    std::vector<Value> edge_id_values;
    std::vector<Value> valid_from_values;
    std::vector<Value> commit_seq_values;
    std::vector<Value> operation_values;
    std::vector<Value> system_time_values;
    std::vector<Value> valid_to_values;
    std::vector<Value> column_id_values;
    entity_type_values.reserve(edges.row_count());
    source_values.reserve(edges.row_count());
    target_values.reserve(edges.row_count());
    edge_type_values.reserve(edges.row_count());
    edge_id_values.reserve(edges.row_count());
    valid_from_values.reserve(edges.row_count());
    commit_seq_values.reserve(edges.row_count());
    operation_values.reserve(edges.row_count());
    system_time_values.reserve(edges.row_count());
    valid_to_values.reserve(edges.row_count());
    column_id_values.reserve(edges.row_count());
    for (uint32_t row = 0; row < edges.row_count(); ++row) {
      const Value* source = edges.ValueRefAt(kEntityId, row);
      const Value* target = edges.ValueRefAt(kTargetId, row);
      const Value* edge_type = edges.ValueRefAt(kEdgeType, row);
      const Value* edge_id = edges.ValueRefAt(kEdgeId, row);
      const Value* valid_from = edges.ValueRefAt(kValidFrom, row);
      const Value* commit_seq = edges.ValueRefAt(kCommitSeq, row);
      const Value* operation = edges.ValueRefAt(kOperation, row);
      if (source == nullptr || target == nullptr || source->type() != PhysicalType::kInt64 ||
          target->type() != PhysicalType::kInt64 || edge_type == nullptr || edge_id == nullptr ||
          valid_from == nullptr || commit_seq == nullptr || operation == nullptr ||
          edge_type->type() != PhysicalType::kInt32 || edge_id->type() != PhysicalType::kInt64 ||
          valid_from->type() != PhysicalType::kTimestamp64 ||
          commit_seq->type() != PhysicalType::kInt64 || operation->type() != PhysicalType::kInt32 ||
          std::get<int64_t>(source->data()) < 0 ||
          std::get<int64_t>(target->data()) < 0) {
        return Status::Corruption("physical runtime", "expand edge identity is invalid");
      }
      if (visible_targets.count(static_cast<uint64_t>(std::get<int64_t>(target->data()))) == 0) {
        continue;
      }
      entity_type_values.push_back(
          Value::Int32(static_cast<int32_t>(plan->expand()->direction)));
      source_values.push_back(*source);
      target_values.push_back(*target);
      edge_type_values.push_back(*edge_type);
      edge_id_values.push_back(*edge_id);
      valid_from_values.push_back(*valid_from);
      commit_seq_values.push_back(*commit_seq);
      operation_values.push_back(*operation);
      if (plan->include_system_time()) {
        if (!timeline || std::get<int64_t>(commit_seq->data()) <= 0 ||
            static_cast<uint64_t>(std::get<int64_t>(commit_seq->data())) >
                timeline->physical_times.size()) {
          return Status::Corruption(
              "physical runtime", "expand commit is absent from CommitTimeline");
        }
        system_time_values.push_back(Value::Timestamp(timeline->physical_times[
            static_cast<size_t>(std::get<int64_t>(commit_seq->data()) - 1)]));
      } else {
        system_time_values.push_back(Value::Timestamp(0));
      }
      if (plan->include_valid_to()) {
        const int32_t type = std::get<int32_t>(edge_type->data());
        const int64_t id = std::get<int64_t>(edge_id->data());
        if (type < 0 || type > std::numeric_limits<uint16_t>::max() || id < 0) {
          return Status::Corruption("physical runtime", "expand edge key is invalid");
        }
        const LogicalKey key = LogicalKey::EdgeExistence(
            static_cast<uint64_t>(std::get<int64_t>(source->data())),
            static_cast<uint64_t>(std::get<int64_t>(target->data())),
            static_cast<uint16_t>(type), static_cast<uint64_t>(id),
            plan->expand()->direction);
        TemporalScanSpec boundary_spec = scan_spec;
        boundary_spec.entity_type = plan->expand()->direction;
        boundary_spec.key_kind = LogicalKeyKind::kExistence;
        boundary_spec.edge_type = static_cast<uint16_t>(type);
        boundary_spec.exact_key = key;
        boundary_spec.allowed_candidate_entity_ids.reset();
        if (context.options.execution_stats) {
          const auto stats = context.options.execution_stats;
          boundary_spec.open_observer = [stats] { ++stats->pinned_boundary_point_scans; };
          boundary_spec.stats_observer = [stats](uint64_t, uint64_t blocks, uint64_t peak) {
            stats->boundary_sst_blocks_read += blocks;
            stats->max_sst_cursor_buffered_events = std::max(
                stats->max_sst_cursor_buffered_events, peak);
          };
          AttachStorageStats(stats, &boundary_spec);
        }
        const auto boundary = FindNextPinnedValidBoundary(
            sources, boundary_spec, key, std::get<uint64_t>(valid_from->data()));
        if (!boundary.ok()) return boundary.status();
        valid_to_values.push_back(Value::Timestamp(boundary.ValueOrDie().value_or(
            std::numeric_limits<uint64_t>::max())));
      } else {
      valid_to_values.push_back(Value::Timestamp(std::numeric_limits<uint64_t>::max()));
      }
      column_id_values.push_back(Value::Int32(0));
    }
    if (source_values.empty()) return Status::OK();
    ColumnBatch expanded(context.options.batch_capacity);
    Status added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(entity_type_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(source_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(target_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(edge_id_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(edge_type_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(column_id_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(valid_from_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(commit_seq_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(operation_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(system_time_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    added = expanded.AddVector(std::make_shared<FlatVector>(
        std::move(valid_to_values), std::vector<bool>{}));
    if (!added.ok()) return added;
    RecordOperatorOutput(PhysicalOperatorKind::kExpand, expanded.row_count());
    std::map<SlotId, uint32_t> layout{
        {plan->expand()->source_slot, 1}, {plan->expand()->target_slot, 2},
        {plan->expand()->edge_id_slot, 3}, {plan->expand()->edge_type_slot, 4},
        {plan->expand()->valid_from_slot, 6}, {plan->expand()->commit_seq_slot, 7},
        {plan->expand()->operation_slot, 8}, {plan->expand()->system_time_slot, 9},
        {plan->expand()->valid_to_slot, 10}};
    timer.Finish();
    return ContinuePointExpandPredicates(std::move(expanded), std::move(layout));
  }

  Status RunMorsel() {
    if (context.options.cancellation && context.options.cancellation->IsCancelled()) {
      ReleaseTerminalResources();
      return Status::QueryCancelled("physical runtime", "query cancelled before storage I/O");
    }
    if (pending_gather.has_value()) return ContinueGather();
    if (pending_target_gather.has_value()) return ContinueTargetPropertyGather();
    if (pending_multi_hop_property_gather.has_value()) return ContinueMultiHopPropertyGather();
    if (pending_range_expand.has_value()) return ContinueRangeExpand();
    if (pending_point_variable_expand.has_value()) {
      return ContinuePointVariableExpand();
    }
    if (pending_segmented_point_expand.has_value()) {
      return ContinueSegmentedPointExpand();
    }
    if (!pending_multi_hop_expands.empty()) return ContinueMultiHopExpand();
    if (pending_expand.has_value()) return ContinuePointExpand();
    if (!scan.has_value()) {
      if (!predicate_preparation_finished) {
        auto prepared = AdvancePredicatePreparationQuantum();
        if (!prepared.ok()) {
          if (prepared.status().IsQueryCancelled()) {
            ReleaseAllAdvisoryIndexState();
            return prepared.status();
          }
          return prepared.status();
        }
        if (prepared.ValueOrDie()) return Status::OK();
      }
      if (!index_preparation_finished) {
        auto prepared = AdvanceIndexPreparationQuantum();
        if (!prepared.ok()) {
          if (prepared.status().IsQueryCancelled()) {
            ReleaseAllAdvisoryIndexState();
            return prepared.status();
          }
          FallBackFromAdvisoryIndex();
          return Status::OK();
        }
        if (prepared.ValueOrDie()) return Status::OK();
        if (!index_advisory_bypassed) {
          auto candidates_ready = AdvanceCandidatePreparationQuantum();
          if (!candidates_ready.ok()) {
            if (candidates_ready.status().IsQueryCancelled()) {
              ReleaseAllAdvisoryIndexState();
              return candidates_ready.status();
            }
            FallBackFromAdvisoryIndex();
            return Status::OK();
          }
          if (candidates_ready.ValueOrDie()) return Status::OK();
          for (auto& [binding, candidates] : candidate_results_by_binding) {
            if (context.options.execution_stats) {
              context.options.execution_stats->index_candidate_entity_count +=
                  candidates.size();
            }
            advisory_candidates_by_binding.emplace(
                binding, std::make_shared<const std::set<uint64_t>>(
                             std::move(candidates)));
          }
          const auto root_candidates =
              advisory_candidates_by_binding.find(plan->binding_id());
          if (root_candidates != advisory_candidates_by_binding.end()) {
            scan_spec.allowed_candidate_entity_ids = root_candidates->second;
          }
          ReleasePreparedIndexSources();
          ReleaseCandidateScratch();
        }
        index_preparation_finished = true;
      }
      auto opened = OpenPinnedTemporalScan(sources, scan_spec);
      if (!opened.ok()) return opened.status();
      scan.emplace(std::move(opened).ConsumeValueOrDie());
    }
    ColumnBatch input;
    const auto scan_start = std::chrono::steady_clock::now();
    const Status scanned = scan->NextMorsel(&input);
    AddOperatorBlocked(plan->operators().front().kind,
                       ElapsedNs(scan_start));
    if (scanned.IsNotFound()) {
      if (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeRange &&
          !range_finalized) {
        range_finalized = true;
        const Status finalized = FinalizeRange();
        if (!finalized.ok()) return finalized;
        if (!queue.empty() || pending_gather.has_value() ||
            pending_range_expand.has_value()) {
          return Status::OK();
        }
      }
      eof = true;
      ReleaseExecutionResources();
      return Status::OK();
    }
    if (!scanned.ok()) return scanned;
    RecordOperatorBatch(plan->operators().front().kind, 0, input.row_count());
    if (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeRange) {
      return ProcessRangeBatch(input);
    }
    if (plan->expand().has_value() &&
        (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeChanges ||
         plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges)) {
      std::map<SlotId, uint32_t> layout{
          {plan->expand()->source_slot, kEntityId},
          {plan->expand()->target_slot, kTargetId},
          {plan->expand()->edge_id_slot, kEdgeId},
          {plan->expand()->edge_type_slot, kEdgeType},
          {plan->expand()->valid_from_slot, kValidFrom},
          {plan->expand()->commit_seq_slot, kCommitSeq},
          {plan->expand()->operation_slot, kOperation}};
      if (plan->include_system_time()) {
        const uint64_t derived_charge = sizeof(FlatVector) +
            static_cast<uint64_t>(input.row_count()) *
                (sizeof(Value) + sizeof(bool));
        if (context.options.memory_account) {
          const Status reserved =
              context.options.memory_account->Reserve(derived_charge);
          if (!reserved.ok()) return reserved;
        }
        auto derived_lease = std::make_shared<RuntimeMemoryLease>(
            context.options.memory_account, derived_charge);
        std::vector<Value> values;
        values.reserve(input.row_count());
        for (uint32_t row = 0; row < input.row_count(); ++row) {
          const Value* commit = input.ValueRefAt(kCommitSeq, row);
          if (commit == nullptr || commit->type() != PhysicalType::kInt64 ||
              std::get<int64_t>(commit->data()) <= 0 || !timeline ||
              static_cast<uint64_t>(std::get<int64_t>(commit->data())) >
                  timeline->physical_times.size()) {
            return Status::Corruption(
                "physical runtime",
                "relationship change commit is absent from CommitTimeline");
          }
          values.push_back(Value::Timestamp(timeline->physical_times[
              static_cast<size_t>(std::get<int64_t>(commit->data()) - 1)]));
        }
        const Status added = input.AddVector(std::make_shared<FlatVector>(
            std::move(values), std::vector<bool>{},
            std::move(derived_lease)));
        if (!added.ok()) return added;
        layout[plan->expand()->system_time_slot] = input.column_count() - 1;
      }
      RecordOperatorBatch(PhysicalOperatorKind::kExpand, input.row_count(),
                          input.row_count());
      return ContinuePointExpandPredicates(
          std::move(input), std::move(layout));
    }
    if (plan->expand().has_value()) {
      if (plan->expand_steps().size() > 1) {
        return BeginSegmentedPointExpand(input);
      }
      std::map<SlotId, uint32_t> layout{
          {plan->expand()->source_slot, kEntityId},
          {plan->expand()->valid_from_slot, kValidFrom},
          {plan->expand()->commit_seq_slot, kCommitSeq},
          {plan->expand()->operation_slot, kOperation}};
      std::vector<PhysicalPropertySlot> source_predicates;
      for (const PhysicalPropertySlot& property : plan->predicate_properties()) {
        if (property.binding == plan->expand()->source_binding) {
          source_predicates.push_back(property);
        }
      }
      if (!source_predicates.empty()) {
        return BeginGather(std::move(input), std::move(source_predicates),
                           GatherPhase::kPredicate, std::move(layout));
      }
      if (plan->expand()->max_hops > 1) {
        return BeginPointVariableExpand(input);
      }
      return BeginPointExpand(input);
    }
    std::map<SlotId, uint32_t> layout{
        {SlotId{1}, kEntityId}, {SlotId{2}, kValidFrom},
        {SlotId{3}, kCommitSeq}, {SlotId{4}, kOperation}};
    uint32_t next_derived_slot = 5;
    if (plan->include_valid_to()) {
      const uint64_t derived_charge = sizeof(FlatVector) +
          static_cast<uint64_t>(input.row_count()) * (sizeof(Value) + sizeof(bool));
      if (context.options.memory_account) {
        const Status reserved = context.options.memory_account->Reserve(derived_charge);
        if (!reserved.ok()) return reserved;
      }
      auto derived_lease = std::make_shared<RuntimeMemoryLease>(
          context.options.memory_account, derived_charge);
      if (context.options.execution_stats) {
        context.options.execution_stats->metadata_derived_bytes_reserved += derived_charge;
      }
      std::vector<Value> values;
      values.reserve(input.row_count());
      for (uint32_t row = 0; row < input.row_count(); ++row) {
        const auto id = input.ValueAt(kEntityId, row);
        const auto from = input.ValueAt(kValidFrom, row);
        if (!id.has_value() || !from.has_value()) {
          return Status::Corruption("physical runtime", "root fact lacks temporal identity");
        }
        const LogicalKey key = LogicalKey::VertexExistence(
            static_cast<uint64_t>(std::get<int64_t>(id->data())));
        TemporalScanSpec boundary_spec = scan_spec;
        if (context.options.execution_stats) {
          const auto stats = context.options.execution_stats;
          boundary_spec.open_observer = [stats] { ++stats->pinned_boundary_point_scans; };
          boundary_spec.stats_observer = [stats](uint64_t, uint64_t blocks, uint64_t peak) {
            stats->boundary_sst_blocks_read += blocks;
            stats->max_sst_cursor_buffered_events = std::max(
                stats->max_sst_cursor_buffered_events, peak);
          };
          AttachStorageStats(stats, &boundary_spec);
        }
        const auto boundary = FindNextPinnedValidBoundary(
            sources, boundary_spec, key, std::get<uint64_t>(from->data()));
        if (!boundary.ok()) return boundary.status();
        values.push_back(Value::Timestamp(boundary.ValueOrDie().value_or(
            std::numeric_limits<uint64_t>::max())));
      }
      const Status added = input.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::vector<bool>{}, std::move(derived_lease)));
      if (!added.ok()) return added;
      layout[SlotId{next_derived_slot++}] = input.column_count() - 1;
    }
    if (plan->include_system_time()) {
      const uint64_t derived_charge = sizeof(FlatVector) +
          static_cast<uint64_t>(input.row_count()) * (sizeof(Value) + sizeof(bool));
      if (context.options.memory_account) {
        const Status reserved = context.options.memory_account->Reserve(derived_charge);
        if (!reserved.ok()) return reserved;
      }
      auto derived_lease = std::make_shared<RuntimeMemoryLease>(
          context.options.memory_account, derived_charge);
      if (context.options.execution_stats) {
        context.options.execution_stats->metadata_derived_bytes_reserved += derived_charge;
      }
      std::vector<Value> values;
      values.reserve(input.row_count());
      for (uint32_t row = 0; row < input.row_count(); ++row) {
        const auto commit = input.ValueAt(kCommitSeq, row);
        if (!commit.has_value() || commit->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(commit->data()) <= 0 || !timeline ||
            static_cast<uint64_t>(std::get<int64_t>(commit->data())) >
                timeline->physical_times.size()) {
          return Status::Corruption("physical runtime", "commit is absent from CommitTimeline");
        }
        values.push_back(Value::Timestamp(timeline->physical_times[
            static_cast<size_t>(std::get<int64_t>(commit->data()) - 1)]));
      }
      const Status added = input.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::vector<bool>{}, std::move(derived_lease)));
      if (!added.ok()) return added;
      layout[SlotId{next_derived_slot++}] = input.column_count() - 1;
    }
    if (plan->include_valid_to() || plan->include_system_time()) {
      RecordOperatorBatch(PhysicalOperatorKind::kMetadataProject,
                          input.row_count(), input.row_count());
    }
    if (!plan->predicate_properties().empty()) {
      return BeginGather(std::move(input), plan->predicate_properties(),
                         GatherPhase::kPredicate, std::move(layout));
    }
    return AfterPredicate(std::move(input), std::move(layout));
  }

  enum class GatherPhase : uint8_t { kNone, kPredicate, kProjection };
  static constexpr uint32_t kPropertyLookupQuantum = 64;
  static constexpr uint32_t kDeltaIndexEventQuantum = 64;

  Status BeginGather(ColumnBatch batch,
                     const std::vector<PhysicalPropertySlot>& properties,
                     GatherPhase phase, std::map<SlotId, uint32_t> layout) {
    AddOperatorInput(PhysicalOperatorKind::kPropertyGather, batch.row_count(),
                     phase == GatherPhase::kProjection);
    const uint32_t base = batch.column_count();
    TemporalScanSpec property_spec{valid_time, snapshot_seq,
                                   context.options.batch_capacity};
    property_spec.cancellation = context.options.cancellation;
    property_spec.memory_account = context.options.memory_account;
    AttachBlobMaterialization(
        &property_spec, phase == GatherPhase::kProjection);
    AttachScanStats(context.options.execution_stats, true, &property_spec);
    std::vector<uint32_t> ids;
    std::vector<uint32_t> epochs;
    ids.reserve(properties.size());
    epochs.reserve(properties.size());
    for (size_t index = 0; index < properties.size(); ++index) {
      ids.push_back(properties[index].column.column_id);
      epochs.push_back(properties[index].column.schema_epoch);
      layout[properties[index].slot] = base + static_cast<uint32_t>(index);
    }
    PropertyGatherSpec gather_spec{
        std::move(ids), valid_time, snapshot_seq, std::move(epochs)};
    if (phase == GatherPhase::kPredicate) {
      auto probes = BuildBlobPredicateProbes(properties);
      if (!probes.ok()) return probes.status();
      gather_spec.blob_predicate_probes =
          std::move(probes).ConsumeValueOrDie();
    }
    if (plan->temporal_mode() == PhysicalTemporalMode::kValidTimeRange ||
        plan->temporal_mode() == PhysicalTemporalMode::kValidTimeChanges ||
        plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges) {
      const SlotId valid_time_slot = plan->expand().has_value()
          ? plan->expand()->valid_from_slot : SlotId{2};
      const auto valid_slot = layout.find(valid_time_slot);
      if (valid_slot == layout.end()) {
        return Status::Corruption(
            "physical runtime", "change gather lacks valid-time slot");
      }
      gather_spec.valid_time_column = valid_slot->second;
    }
    if (phase == GatherPhase::kProjection &&
        context.options.execution_stats) {
      const auto stats = context.options.execution_stats;
      gather_spec.payload_copy_observer = [stats](uint64_t bytes) {
        stats->projection_gather_payload_bytes_copied += bytes;
      };
    }
    auto opened = OpenPinnedPropertyGather(
        std::move(batch), sources, std::move(property_spec),
        std::move(gather_spec));
    if (!opened.ok()) return opened.status();
    pending_gather.emplace(std::move(opened).ConsumeValueOrDie());
    pending_gather_phase = phase;
    pending_layout = std::move(layout);
    return Status::OK();
  }

  Status ContinueGather() {
    uint32_t completed = 0;
    const GatherPhase active_phase = pending_gather_phase;
    const auto gather_start = std::chrono::steady_clock::now();
    const Status advanced = pending_gather->Advance(
        kPropertyLookupQuantum, &completed);
    AddOperatorBlocked(
        PhysicalOperatorKind::kPropertyGather, ElapsedNs(gather_start),
        active_phase == GatherPhase::kProjection);
    if (!advanced.ok()) return advanced;
    if (!pending_gather->done()) return Status::OK();
    ColumnBatch gathered;
    const Status finished = pending_gather->Finish(&gathered);
    if (!finished.ok()) return finished;
    const GatherPhase phase = pending_gather_phase;
    pending_gather.reset();
    pending_gather_phase = GatherPhase::kNone;
    std::map<SlotId, uint32_t> layout = std::move(pending_layout);
    pending_layout.clear();
    RecordOperatorOutput(PhysicalOperatorKind::kPropertyGather,
                         gathered.row_count(),
                         phase == GatherPhase::kProjection);
    if (phase == GatherPhase::kPredicate) {
      const uint64_t filter_input_rows = gathered.row_count();
      ColumnBatch selected;
      const auto filter_start = std::chrono::steady_clock::now();
      const Status selected_status = FilterColumnBatch(
          gathered,
          [this, &layout](const ColumnBatch& batch, uint32_t row) {
            for (const PhysicalPredicate& predicate : plan->predicates()) {
              const auto column = layout.find(predicate.slot);
              if (column == layout.end()) continue;
              const Value* value = batch.ValueRefAt(column->second, row);
              if (value == nullptr || !MatchesPredicate(*value, predicate)) {
                return false;
              }
            }
            return true;
          },
          &selected);
      AddOperatorCpu(PhysicalOperatorKind::kFilter,
                     ElapsedNs(filter_start));
      if (!selected_status.ok()) return selected_status;
      RecordOperatorBatch(PhysicalOperatorKind::kFilter, filter_input_rows,
                          selected.row_count());
      return AfterPredicate(std::move(selected), std::move(layout));
    }
    if (plan->expand().has_value()) {
      return ContinuePointExpandProjections(
          std::move(gathered), std::move(layout));
    }
    return ProjectAndEnqueue(std::move(gathered), std::move(layout));
  }

  Status AfterPredicate(ColumnBatch filtered,
                        std::map<SlotId, uint32_t> layout) {
    if (filtered.row_count() == 0) return Status::OK();
    if (plan->expand().has_value()) {
      if (layout.count(plan->expand()->target_slot) == 0) {
        if (plan->expand_steps().size() > 1) {
          return BeginSegmentedPointExpand(filtered);
        }
        if (plan->expand()->max_hops > 1) {
          return BeginPointVariableExpand(filtered);
        }
        return BeginPointExpand(filtered);
      }
      return ContinuePointExpandPredicates(
          std::move(filtered), std::move(layout));
    }
    if (!plan->projection_properties().empty()) {
      return BeginGather(std::move(filtered), plan->projection_properties(),
                         GatherPhase::kProjection, std::move(layout));
    }
    return ProjectAndEnqueue(std::move(filtered), std::move(layout));
  }

  Status ProjectAndEnqueue(ColumnBatch filtered,
                           const std::map<SlotId, uint32_t>& layout) {
    const auto project_start = std::chrono::steady_clock::now();
    uint64_t charge = 0;
    const auto add_charge = [&charge](uint64_t bytes,
                                      const char* detail) -> Status {
      if (charge > std::numeric_limits<uint64_t>::max() - bytes) {
        return Status::QueryMemoryLimit("physical runtime", detail);
      }
      charge += bytes;
      return Status::OK();
    };
    for (const PhysicalExpression& expression : plan->projections()) {
      const auto column = layout.find(expression.referenced_slot);
      if (column == layout.end()) {
        return Status::Corruption("physical runtime", "projection slot has no runtime column");
      }
      if (expression.kind == PhysicalExpressionKind::kRelationshipBinding) {
        if (expression.relationship_slots.size() != 6) {
          return Status::Corruption("physical runtime", "relationship projection slots are invalid");
        }
        const uint64_t per_row = sizeof(StructValue) +
            static_cast<uint64_t>(expression.relationship_slots.size()) * sizeof(StructField);
        if (filtered.row_count() != 0 &&
            per_row > (std::numeric_limits<uint64_t>::max() - charge) /
                filtered.row_count()) {
          return Status::QueryMemoryLimit("physical runtime", "relationship projection charge overflow");
        }
        const Status vector_charge = add_charge(
            sizeof(StructVector) +
                static_cast<uint64_t>(filtered.row_count()) * sizeof(bool),
            "relationship projection vector charge overflow");
        if (!vector_charge.ok()) return vector_charge;
        charge += per_row * filtered.row_count();
        continue;
      }
      if (expression.kind == PhysicalExpressionKind::kPathBinding) {
        if (!filtered.IsList(column->second)) {
          return Status::Corruption(
              "physical runtime", "path projection source is not a list");
        }
        const Status vector_charge = add_charge(
            sizeof(ListVector) +
                static_cast<uint64_t>(filtered.row_count()) *
                    (sizeof(ListValue) + sizeof(bool)),
            "path projection vector charge overflow");
        if (!vector_charge.ok()) return vector_charge;
        for (uint32_t row = 0; row < filtered.row_count(); ++row) {
          const ListValue* path = filtered.ListRefAt(column->second, row);
          if (path == nullptr) continue;
          if (path->element_kind != ListElementKind::kStruct ||
              !path->elements.empty()) {
            return Status::Corruption(
                "physical runtime", "path projection has an invalid element shape");
          }
          const Status payload_charge = add_charge(
              filtered.RetainedPayloadBytesAt(column->second, row),
              "path projection payload charge overflow");
          if (!payload_charge.ok()) return payload_charge;
        }
        continue;
      }
      const Status vector_charge = add_charge(
          sizeof(FlatVector) +
              static_cast<uint64_t>(filtered.row_count()) *
                  (sizeof(Value) + sizeof(bool)),
          "scalar projection vector charge overflow");
      if (!vector_charge.ok()) return vector_charge;
      for (uint32_t row = 0; row < filtered.row_count(); ++row) {
        const Value* value = filtered.ValueRefAt(column->second, row);
        if (value != nullptr) {
          const uint64_t payload =
              expression.kind == PhysicalExpressionKind::kOperationName
                  ? 3 : filtered.RetainedPayloadBytesAt(column->second, row);
          if (charge > std::numeric_limits<uint64_t>::max() - payload) {
            return Status::QueryMemoryLimit(
                "physical runtime", "project payload charge overflow");
          }
          charge += payload;
        }
      }
    }
    if (context.options.memory_account) {
      const Status reserved = context.options.memory_account->Reserve(charge);
      if (!reserved.ok()) return reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(context.options.memory_account, charge);
    ColumnBatch projected(filtered.row_count());
    std::vector<std::string> names;
    names.reserve(plan->projections().size());
    for (const PhysicalExpression& expression : plan->projections()) {
      if (expression.kind == PhysicalExpressionKind::kRelationshipBinding) {
        std::vector<uint32_t> columns;
        columns.reserve(expression.relationship_slots.size());
        for (SlotId slot : expression.relationship_slots) {
          const auto column = layout.find(slot);
          if (column == layout.end()) {
            return Status::Corruption(
                "physical runtime", "relationship projection slot has no runtime column");
          }
          columns.push_back(column->second);
        }
        std::vector<StructValue> relationships;
        relationships.reserve(filtered.row_count());
        for (uint32_t row = 0; row < filtered.row_count(); ++row) {
          std::vector<StructField> fields;
          fields.reserve(6);
          static constexpr const char* kFieldNames[] = {
              "source_id", "target_id", "edge_type", "edge_id", "valid_from", "commit_seq"};
          for (size_t field = 0; field < columns.size(); ++field) {
            const Value* value = filtered.ValueRefAt(columns[field], row);
            if (value == nullptr) {
              return Status::Corruption(
                  "physical runtime", "relationship projection has a null identity");
            }
            fields.push_back(StructField{kFieldNames[field], *value});
          }
          relationships.push_back(StructValue{std::move(fields)});
        }
        const Status added = projected.AddVector(std::make_shared<StructVector>(
            std::move(relationships), std::vector<bool>{}, lease));
        if (!added.ok()) return added;
        names.push_back(expression.output_name);
        continue;
      }
      if (expression.kind == PhysicalExpressionKind::kPathBinding) {
        const uint32_t column = layout.at(expression.referenced_slot);
        if (!filtered.IsList(column)) {
          return Status::Corruption(
              "physical runtime", "path projection source is not a list");
        }
        std::vector<ListValue> paths;
        std::vector<bool> validity;
        paths.reserve(filtered.row_count());
        validity.reserve(filtered.row_count());
        for (uint32_t row = 0; row < filtered.row_count(); ++row) {
          const ListValue* path = filtered.ListRefAt(column, row);
          validity.push_back(path != nullptr);
          if (path == nullptr) {
            paths.emplace_back();
            continue;
          }
          if (path->element_kind != ListElementKind::kStruct ||
              !path->elements.empty()) {
            return Status::Corruption(
                "physical runtime", "path projection has an invalid element shape");
          }
          paths.push_back(*path);
          if (context.options.execution_stats) {
            context.options.execution_stats->physical_project_payload_bytes_copied +=
                filtered.ValuePayloadBytesAt(column, row);
          }
        }
        const Status added = projected.AddVector(std::make_shared<ListVector>(
            std::move(paths), std::move(validity), lease));
        if (!added.ok()) return added;
        names.push_back(expression.output_name);
        continue;
      }
      const uint32_t column = layout.at(expression.referenced_slot);
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(filtered.row_count());
      validity.reserve(filtered.row_count());
      for (uint32_t row = 0; row < filtered.row_count(); ++row) {
        const Value* value = filtered.ValueRefAt(column, row);
        validity.push_back(value != nullptr);
        uint64_t copied_payload = 0;
        if (expression.kind == PhysicalExpressionKind::kOperationName &&
            value != nullptr) {
          const int32_t operation = std::get<int32_t>(value->data());
          if (operation != 0 && operation != 1) {
            return Status::Corruption("physical runtime", "unknown temporal operation code");
          }
          values.push_back(Value::String(operation == 0 ? "PUT" : "DELETE"));
          copied_payload = 3;
        } else {
          values.push_back(value != nullptr ? *value : Value::Binary(""));
          copied_payload = filtered.ValuePayloadBytesAt(column, row);
        }
        if (context.options.execution_stats && copied_payload != 0) {
          context.options.execution_stats->physical_project_payload_bytes_copied +=
              copied_payload;
        }
      }
      const Status added = projected.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::move(validity), lease));
      if (!added.ok()) return added;
      names.push_back(expression.output_name);
    }
    ResultBatch result(std::move(names), std::move(projected),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    const uint64_t rows = result.batch().row_count();
    RecordOperatorBatch(PhysicalOperatorKind::kProject, filtered.row_count(),
                        rows);
    if (queue.size() >= queue_capacity) {
      return Status::Corruption("physical runtime", "result queue overflow");
    }
    queue.push_back(std::move(result));
    RecordOperatorBatch(PhysicalOperatorKind::kResultSink, rows, rows);
    if (context.options.execution_stats) {
      context.options.execution_stats->physical_output_rows += rows;
      context.options.execution_stats->result_queue_high_water = std::max<uint64_t>(
          context.options.execution_stats->result_queue_high_water, queue.size());
    }
    AddOperatorCpu(PhysicalOperatorKind::kProject,
                   ElapsedNs(project_start));
    return Status::OK();
  }

  std::shared_ptr<const PhysicalPlan> plan;
  QuerySnapshot snapshot;
  TcypherExecutionContext context;
  PinnedTemporalScanSources sources;
  WorkExecutionService* execution_service = nullptr;
  TemporalScanSpec scan_spec;
  std::optional<TemporalScanCursor> scan;
  std::vector<std::shared_ptr<RuntimeMemoryLease>> index_preparation_leases;
  std::vector<std::shared_ptr<RuntimeMemoryLease>> index_candidate_leases;
  std::shared_ptr<RuntimeMemoryLease> index_metadata_lease;
  std::shared_ptr<RuntimeMemoryLease> canonical_predicate_lease;
  std::vector<uint64_t> selected_index_ids;
  std::vector<BindingId> candidate_binding_by_predicate;
  std::set<uint64_t> selected_index_id_set;
  std::map<IndexColumnKey, uint64_t> selected_definition_by_column;
  std::vector<size_t> relevant_sst_index_sources;
  std::vector<size_t> relevant_delta_index_sources;
  std::vector<size_t> lazy_sst_index_sources;
  std::vector<size_t> lazy_delta_index_sources;
  std::vector<size_t> prepared_sst_index_sources;
  std::vector<size_t> prepared_delta_index_sources;
  size_t next_sst_index_source = 0;
  size_t next_delta_index_source = 0;
  std::set<std::pair<uint64_t, uint64_t>> sst_index_coverage;
  std::map<uint64_t, size_t> delta_index_coverage;
  std::vector<bool> active_candidate_predicates;
  std::vector<size_t> active_candidate_predicate_indices;
  std::vector<size_t> active_candidate_ordinal_by_plan;
  std::map<BindingId, size_t> active_candidate_count_by_binding;
  std::optional<TemporalMemTableCursor> delta_index_cursor;
  std::optional<MemtableDeltaIndex::LookupCursor> delta_candidate_cursor;
  size_t predicate_preparation_predicate_index = 0;
  size_t predicate_preparation_value_index = 0;
  uint8_t predicate_preparation_bound_stage = 0;
  bool predicate_preparation_initialized = false;
  bool predicate_preparation_finished = false;
  uint64_t predicate_literal_comparison_count = 0;
  bool count_predicate_literal_comparisons = false;
  bool index_preparation_initialized = false;
  IndexPreparationPhase index_preparation_phase =
      IndexPreparationPhase::kSelectDefinitions;
  size_t index_metadata_predicate_index = 0;
  size_t index_metadata_definition_index = 0;
  size_t index_metadata_sst_source_index = 0;
  size_t index_metadata_delta_source_index = 0;
  size_t index_metadata_version_file_index = 0;
  size_t index_metadata_coverage_predicate_index = 0;
  uint32_t index_metadata_items_this_morsel = 0;
  bool index_preparation_finished = false;
  bool index_advisory_bypassed = false;
  std::vector<CanonicalPhysicalPredicate> canonical_predicates;
  size_t candidate_canonical_predicate_index = 0;
  size_t candidate_canonical_value_index = 0;
  uint8_t candidate_canonical_bound_stage = 0;
  bool count_candidate_literal_lookup_comparisons = false;
  std::map<std::pair<BindingId, uint64_t>, CandidateMatchProgress>
      candidate_matches;
  std::map<BindingId, std::set<uint64_t>> candidate_results_by_binding;
  std::map<std::pair<BindingId, uint64_t>,
           CandidateMatchProgress>::iterator
      candidate_finalize_iterator;
  std::map<BindingId, std::shared_ptr<const std::set<uint64_t>>>
      advisory_candidates_by_binding;
  size_t candidate_predicate_index = 0;
  size_t candidate_source_index = 0;
  size_t candidate_item_index = 0;
  CandidatePhase candidate_phase = CandidatePhase::kSst;
  uint64_t candidate_upper_bound = 0;
  uint64_t adaptive_sampled_candidates = 0;
  std::vector<uint64_t> adaptive_sampled_candidates_by_predicate;
  bool candidate_preparation_initialized = false;
  bool candidate_healthy_source = false;
  std::optional<PinnedPropertyGatherCursor> pending_gather;
  GatherPhase pending_gather_phase = GatherPhase::kNone;
  std::map<SlotId, uint32_t> pending_layout;
  std::optional<PendingPointExpand> pending_expand;
  std::optional<PendingPointVariableExpand> pending_point_variable_expand;
  std::optional<PendingSegmentedPointExpand> pending_segmented_point_expand;
  std::optional<PendingRangeExpand> pending_range_expand;
  std::deque<PendingMultiHopExpand> pending_multi_hop_expands;
  std::optional<PendingMultiHopPropertyGather> pending_multi_hop_property_gather;
  std::optional<PendingTargetPropertyGather> pending_target_gather;
  std::set<BindingId> completed_node_property_bindings;
  std::shared_ptr<RuntimeTimelineSnapshot> timeline;
  std::map<uint64_t, RangePendingEvent> range_previous_events;
  std::set<uint64_t> range_closed_entities;
  bool range_finalized = false;
  std::deque<ResultBatch> queue;
  static constexpr size_t queue_capacity = 2;
  Status terminal = Status::OK();
  uint64_t valid_time = 0;
  uint64_t snapshot_seq = 0;
  bool eof = false;
  bool abandoned = false;
};

class ScheduledQueryResultStream final : public QueryResultStream {
 public:
  ScheduledQueryResultStream(
      std::shared_ptr<QueryRuntimeState> state,
      std::shared_ptr<WorkExecutionService> execution_service)
      : execution_service_(std::move(execution_service)),
        state_(std::move(state)) {}
  ~ScheduledQueryResultStream() override {
    if (state_) state_->Abandon();
  }
  Status Next(ResultBatch* batch) override {
    return state_ ? state_->Pop(batch)
                  : Status::InvalidArgument("physical runtime", "missing query state");
  }
  Status terminal_status() const override {
    return state_ ? state_->terminal
                  : Status::InvalidArgument("physical runtime", "missing query state");
  }

 private:
  // ScheduleAndWait keeps callbacks synchronous with Next. Keeping the shared
  // service owner here prevents a callback-captured state from destroying and
  // joining the service on its own worker thread.
  std::shared_ptr<WorkExecutionService> execution_service_;
  std::shared_ptr<QueryRuntimeState> state_;
};

class PhysicalHashJoinResultStream final : public QueryResultStream {
 public:
  using StoredRow = std::vector<ResultValueCell>;

  struct BuildRow {
    StoredRow values;
    uint64_t ordinal = 0;
  };

  PhysicalHashJoinResultStream(std::unique_ptr<QueryResultStream> build_input,
                               std::unique_ptr<QueryResultStream> probe_input,
                               std::vector<std::string> names,
                               std::vector<PhysicalHashJoinPlan::Output> outputs,
                               std::vector<uint32_t> build_key_columns,
                               std::vector<uint32_t> probe_key_columns,
                               uint32_t capacity,
                               std::shared_ptr<QueryCancellation> cancellation,
                               std::shared_ptr<QueryMemoryAccount> memory_account,
                               std::string spill_directory,
                               std::shared_ptr<ResourceGovernorExtension> spill_resources,
                               std::shared_ptr<TcypherExecutionStats> stats,
                               uint64_t plan_id, uint32_t operator_id,
                               bool probe_is_left)
      : build_input_(std::move(build_input)), probe_input_(std::move(probe_input)),
        names_(std::move(names)), outputs_(std::move(outputs)),
        build_key_columns_(std::move(build_key_columns)),
        probe_key_columns_(std::move(probe_key_columns)),
        capacity_(capacity), cancellation_(std::move(cancellation)),
        memory_account_(std::move(memory_account)),
        spill_directory_(std::move(spill_directory)),
        spill_resources_(spill_resources), stats_(std::move(stats)),
        plan_id_(plan_id), operator_id_(operator_id),
        probe_is_left_(probe_is_left) {}
  ~PhysicalHashJoinResultStream() override {
    if (memory_account_ && reserved_bytes_ != 0) {
      memory_account_->Release(reserved_bytes_);
    }
    ReleaseDynamicFilter();
  }

  Status Next(ResultBatch* output) override {
    if (output == nullptr) return Status::InvalidArgument("physical hash join", "missing output");
    if (!initialized_) {
      const Status initialized = Initialize();
      if (initialized.IsQueryMemoryLimit()) {
        return terminal_ = Status::QueryMemoryLimit(
            "physical hash join", "initialization exceeds memory limit");
      }
      if (!initialized.ok()) return terminal_ = initialized;
    }
    if (!terminal_.ok()) return terminal_;
    if (spilling_) {
      return NextSpilled(output);
    }
    std::vector<std::vector<ResultValueCell>> values(outputs_.size());
    uint32_t output_rows = 0;
    while (output_rows < capacity_) {
      if (cancellation_ && cancellation_->IsCancelled()) {
        return terminal_ = Status::QueryCancelled("physical hash join", "query cancelled");
      }
      if (matches_ == nullptr) {
        if (!probe_.has_value() || probe_row_ == probe_.value().batch().row_count()) {
          ResultBatch input;
          const Status next = probe_input_->Next(&input);
          if (next.IsNotFound()) {
            const Status terminal =
                ResultStreamTerminalAtEnd(probe_input_.get());
            if (!terminal.ok()) return terminal_ = terminal;
            ReleaseDynamicFilter();
            break;
          }
          if (!next.ok()) return terminal_ = next;
          if (stats_) {
            stats_->hash_join_probe_input_rows += input.batch().row_count();
            if (stats_->operator_runtime) {
              stats_->operator_runtime->RecordHashJoinInput(
                  OperatorRuntimeKey{plan_id_, operator_id_}, false,
                  input.batch().row_count());
            }
          }
          if (std::any_of(probe_key_columns_.begin(), probe_key_columns_.end(),
                          [&](uint32_t column) {
                            return column >= input.batch().column_count();
                          })) {
            return terminal_ = Status::Corruption("physical hash join", "probe input shape");
          }
          probe_ = std::move(input);
          probe_row_ = 0;
        }
        const uint32_t row = probe_row_++;
        const auto key = KeyAt(
            probe_.value().batch(), row, probe_key_columns_);
        if (!key.ok()) return terminal_ = key.status();
        if (!ProbePassesDynamicFilter(key.ValueOrDie(), false)) continue;
        const auto found = build_.find(*key.ValueOrDie());
        if (found == build_.end()) continue;
        auto probe_values = StoredRowAt(
            probe_.value(), row,
            probe_.value().batch().column_count(), "probe");
        if (!probe_values.ok()) return terminal_ = probe_values.status();
        probe_values_ = std::move(probe_values).ConsumeValueOrDie();
        matches_ = &found->second;
        match_index_ = 0;
        continue;
      }
      const BuildRow& build_row = (*matches_)[match_index_++];
      for (size_t output_index = 0; output_index < outputs_.size(); ++output_index) {
        const PhysicalHashJoinPlan::Output& spec = outputs_[output_index];
        const bool from_probe = spec.from_left == probe_is_left_;
        const StoredRow& source = from_probe ? probe_values_ : build_row.values;
        if (spec.column >= source.size()) {
          return terminal_ = Status::Corruption(
              "physical hash join", "output column mapping exceeds child row");
        }
        const ResultValueCell& value = source[spec.column];
        if (!CellPresent(value) && !spec.nullable) {
          return terminal_ = Status::Corruption(
              "physical hash join", "non-nullable output is null");
        }
        values[output_index].push_back(value);
      }
      ++output_rows;
      if (match_index_ == matches_->size()) matches_ = nullptr;
    }
    if (output_rows == 0) return Status::NotFound("physical hash join", "end of stream");
    uint64_t output_bytes = sizeof(FlatVector) * outputs_.size();
    for (const auto& column : values) {
      output_bytes += static_cast<uint64_t>(column.size()) *
          (sizeof(ResultValueCell) + sizeof(bool));
      for (const ResultValueCell& value : column) {
        output_bytes += CellPayloadBytes(value);
      }
    }
    if (memory_account_) {
      const Status reserved = memory_account_->Reserve(output_bytes);
      if (!reserved.ok()) return terminal_ = reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(memory_account_, output_bytes);
    ObserveOperatorMemory(output_bytes);
    ColumnBatch batch(output_rows);
    for (size_t column = 0; column < values.size(); ++column) {
      const Status added = AddCellColumn(
          std::move(values[column]), outputs_[column].type, lease, &batch);
      if (!added.ok()) return terminal_ = added;
    }
    *output = ResultBatch(names_, std::move(batch));
    const Status valid = output->Validate();
    if (!valid.ok()) return terminal_ = valid;
    if (stats_) {
      stats_->last_physical_plan_id = plan_id_;
      stats_->executed_physical_plan_id = plan_id_;
      stats_->physical_output_rows += output->batch().row_count();
      if (stats_->operator_runtime) {
        stats_->operator_runtime->RecordHashJoinOutput(
            OperatorRuntimeKey{plan_id_, operator_id_},
            output->batch().row_count());
      }
    }
    return Status::OK();
  }
  Status terminal_status() const override {
    if (!terminal_.ok()) return terminal_;
    if (build_input_ && !build_input_->terminal_status().ok()) {
      return build_input_->terminal_status();
    }
    return probe_input_ ? probe_input_->terminal_status() : Status::OK();
  }

 private:
  static constexpr uint32_t kSpillPartitions = 16;
  static constexpr const char* kSpillOrdinalName = "__cedar_hash_join_ordinal";
  static constexpr const char* kRunProbeOrdinalName = "__cedar_hash_join_probe_ordinal";
  static constexpr const char* kRunBuildOrdinalName = "__cedar_hash_join_build_ordinal";

  struct SpillCursor {
    ResultBatch batch;
    uint32_t row = 0;
    bool has_row = false;
  };

  static ResultValueCell CellAt(
      const ColumnBatch& batch, uint32_t column, uint32_t row) {
    ResultValueCell cell;
    if (batch.IsStructured(column)) {
      cell.kind = ResultValueKind::kStruct;
      cell.structure = batch.StructAt(column, row);
    } else if (batch.IsList(column)) {
      cell.kind = ResultValueKind::kList;
      cell.list = batch.ListAt(column, row);
    } else {
      cell.kind = ResultValueKind::kScalar;
      cell.scalar = batch.ValueAt(column, row);
    }
    return cell;
  }

  static bool CellPresent(const ResultValueCell& cell) {
    switch (cell.kind) {
      case ResultValueKind::kScalar: return cell.scalar.has_value();
      case ResultValueKind::kStruct: return cell.structure.has_value();
      case ResultValueKind::kList: return cell.list.has_value();
    }
    return false;
  }

  static uint64_t CellPayloadBytes(const ResultValueCell& cell) {
    if (!CellPresent(cell)) return 0;
    if (cell.kind == ResultValueKind::kScalar) {
      return cell.scalar->Encode().size();
    }
    uint64_t bytes = 0;
    if (cell.kind == ResultValueKind::kStruct) {
      for (const StructField& field : cell.structure->fields) {
        bytes += field.name.size() + sizeof(field.value);
        if (field.value.has_value()) bytes += field.value->Encode().size();
      }
      return bytes;
    }
    for (const std::optional<Value>& element : cell.list->elements) {
      bytes += sizeof(element);
      if (element.has_value()) bytes += element->Encode().size();
    }
    return bytes;
  }

  static Status AddCellColumn(
      std::vector<ResultValueCell> cells, PhysicalType scalar_type,
      const std::shared_ptr<void>& retention, ColumnBatch* batch) {
    if (batch == nullptr || cells.empty()) {
      return Status::InvalidArgument(
          "physical hash join", "missing output cell column");
    }
    const ResultValueKind kind = cells.front().kind;
    if (std::any_of(cells.begin(), cells.end(), [&](const ResultValueCell& cell) {
          return cell.kind != kind;
        })) {
      return Status::Corruption(
          "physical hash join", "output column changes value shape");
    }
    std::vector<bool> validity;
    validity.reserve(cells.size());
    if (kind == ResultValueKind::kStruct) {
      std::vector<StructValue> values;
      values.reserve(cells.size());
      for (ResultValueCell& cell : cells) {
        validity.push_back(cell.structure.has_value());
        values.push_back(cell.structure.value_or(StructValue{}));
      }
      return batch->AddVector(std::make_shared<StructVector>(
          std::move(values), std::move(validity), retention));
    }
    if (kind == ResultValueKind::kList) {
      std::vector<ListValue> values;
      values.reserve(cells.size());
      for (ResultValueCell& cell : cells) {
        validity.push_back(cell.list.has_value());
        values.push_back(cell.list.value_or(ListValue{}));
      }
      return batch->AddVector(std::make_shared<ListVector>(
          std::move(values), std::move(validity), retention));
    }
    std::vector<Value> values;
    values.reserve(cells.size());
    for (ResultValueCell& cell : cells) {
      validity.push_back(cell.scalar.has_value());
      values.push_back(cell.scalar.value_or(NullPlaceholder(scalar_type)));
    }
    return batch->AddVector(std::make_shared<FlatVector>(
        std::move(values), std::move(validity), retention));
  }

  static Status AddSingleCell(
      const ResultValueCell& cell, PhysicalType scalar_type,
      ColumnBatch* batch) {
    return AddCellColumn({cell}, scalar_type, nullptr, batch);
  }

  Status CheckCancelled(const char* phase) const {
    return cancellation_ && cancellation_->IsCancelled()
        ? Status::QueryCancelled("physical hash join", phase) : Status::OK();
  }

  static uint64_t StableKeyHash(const std::string& key) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char byte : key) {
      hash ^= byte;
      hash *= 1099511628211ULL;
    }
    return hash;
  }

  uint32_t PartitionFor(const std::string& key) const {
    return static_cast<uint32_t>(StableKeyHash(key) % kSpillPartitions);
  }

  bool ProbePassesDynamicFilter(
      const std::optional<std::string>& key, bool spill_path) {
    if (stats_) ++stats_->hash_join_dynamic_filter_input_rows;
    const bool passes = key.has_value() &&
        (!dynamic_filter_enabled_ ||
         build_filter_hashes_.count(StableKeyHash(*key)) != 0);
    if (stats_) {
      if (passes) {
        ++stats_->hash_join_dynamic_filter_output_rows;
      } else {
        ++stats_->hash_join_dynamic_filter_rejected_rows;
        if (spill_path) {
          ++stats_->hash_join_dynamic_filter_spill_rows_avoided;
        }
      }
    }
    return passes;
  }

  void ReleaseDynamicFilter() {
    build_filter_hashes_.clear();
    if (memory_account_ && dynamic_filter_reserved_bytes_ != 0) {
      memory_account_->Release(dynamic_filter_reserved_bytes_);
    }
    dynamic_filter_reserved_bytes_ = 0;
  }

  void AddDynamicFilterSignature(const std::string& key) {
    if (!dynamic_filter_enabled_) return;
    const uint64_t signature = StableKeyHash(key);
    if (build_filter_hashes_.count(signature) != 0) return;
    constexpr uint64_t kSignatureBytes =
        sizeof(uint64_t) + 4 * sizeof(void*) + 32;
    if (memory_account_) {
      const Status reserved = memory_account_->Reserve(kSignatureBytes);
      if (!reserved.ok()) {
        ReleaseDynamicFilter();
        dynamic_filter_enabled_ = false;
        if (stats_) ++stats_->hash_join_dynamic_filter_memory_disables;
        return;
      }
    }
    dynamic_filter_reserved_bytes_ += kSignatureBytes;
    if (stats_) {
      stats_->hash_join_dynamic_filter_memory_bytes = std::max(
          stats_->hash_join_dynamic_filter_memory_bytes,
          dynamic_filter_reserved_bytes_);
    }
    build_filter_hashes_.insert(signature);
  }

  static StatusOr<uint64_t> ReadOrdinal(const ResultBatch& batch, uint32_t row,
                                        uint32_t column) {
    if (column >= batch.batch().column_count()) {
      return Status::Corruption("physical hash join", "spill ordinal column is absent");
    }
    const auto value = batch.batch().ValueAt(column, row);
    if (!value.has_value() || value->type() != PhysicalType::kInt64) {
      return Status::Corruption("physical hash join", "invalid spill ordinal");
    }
    const int64_t ordinal = std::get<int64_t>(value->data());
    if (ordinal < 0) return Status::Corruption("physical hash join", "negative spill ordinal");
    return static_cast<uint64_t>(ordinal);
  }

  static StatusOr<std::optional<std::string>> KeyFromStoredRow(
      const StoredRow& row, const std::vector<uint32_t>& columns) {
    if (columns.empty()) {
      return Status::Corruption("physical hash join", "join key has no columns");
    }
    std::string key;
    for (uint32_t column : columns) {
      if (column >= row.size()) {
        return Status::Corruption("physical hash join", "spill row shape");
      }
      if (row[column].kind != ResultValueKind::kScalar) {
        return Status::InvalidArgument(
            "physical hash join", "join key must be scalar");
      }
      if (!row[column].scalar.has_value()) return std::optional<std::string>{};
      const std::string encoded = row[column].scalar->Encode();
      if (encoded.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument(
            "physical hash join", "join key component exceeds length bound");
      }
      const uint32_t length = static_cast<uint32_t>(encoded.size());
      for (uint32_t byte = 0; byte < 4; ++byte) {
        key.push_back(static_cast<char>(length >> (byte * 8)));
      }
      key.append(encoded);
    }
    return std::optional<std::string>{std::move(key)};
  }

  static uint64_t StoredRowBytes(const StoredRow& stored, const std::string& key) {
    uint64_t bytes = sizeof(BuildRow) + sizeof(std::string) + key.size();
    for (const auto& value : stored) {
      bytes += sizeof(ResultValueCell) + CellPayloadBytes(value);
    }
    return bytes;
  }

  static uint64_t InputRowBytes(const ResultBatch& input, uint32_t row,
                                const std::string& key) {
    uint64_t bytes = sizeof(BuildRow) + sizeof(std::string) + key.size();
    for (uint32_t column = 0; column < input.batch().column_count(); ++column) {
      const ResultValueCell value = CellAt(input.batch(), column, row);
      bytes += sizeof(ResultValueCell) + CellPayloadBytes(value);
    }
    return bytes;
  }

  StatusOr<StoredRow> StoredRowAt(const ResultBatch& input, uint32_t row,
                                  uint32_t columns, const char* side) const {
    StoredRow stored;
    stored.reserve(columns);
    for (uint32_t column = 0; column < columns; ++column) {
      (void)side;
      stored.push_back(CellAt(input.batch(), column, row));
    }
    return stored;
  }

  Status AppendSpilledRow(PartitionedSpillSet* spill, uint32_t partition,
                          const std::vector<std::string>& names,
                          const StoredRow& row, uint64_t ordinal) {
    if (spill == nullptr || ordinal > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return Status::InvalidArgument("physical hash join", "invalid spill row");
    }
    ColumnBatch batch(1);
    for (const auto& value : row) {
      const Status added = AddSingleCell(value, PhysicalType::kBinary, &batch);
      if (!added.ok()) return added;
    }
    const Status ordinal_added = batch.AddVector(std::make_shared<FlatVector>(
        std::vector<Value>{Value::Int64(static_cast<int64_t>(ordinal))},
        std::vector<bool>{true}));
    if (!ordinal_added.ok()) return ordinal_added;
    std::vector<std::string> spill_names = names;
    spill_names.push_back(kSpillOrdinalName);
    ResultBatch record(std::move(spill_names), std::move(batch));
    const Status valid = record.Validate();
    if (!valid.ok()) return valid;
    const Status appended = spill->Append(partition, record);
    if (appended.ok()) ReportSpillBytes();
    return appended;
  }

  StatusOr<StoredRow> ReadSpilledRow(const ResultBatch& batch, uint32_t row,
                                     uint32_t expected_columns,
                                     uint64_t* ordinal) const {
    if (ordinal == nullptr || batch.batch().column_count() != expected_columns + 1) {
      return Status::Corruption("physical hash join", "invalid spill row shape");
    }
    auto decoded_ordinal = ReadOrdinal(batch, row, expected_columns);
    if (!decoded_ordinal.ok()) return decoded_ordinal.status();
    *ordinal = decoded_ordinal.ValueOrDie();
    return StoredRowAt(batch, row, expected_columns, "spill");
  }

  Status StartSpill() {
    if (spilling_) return Status::OK();
    const std::string directory = spill_directory_.empty() ? "/tmp" : spill_directory_;
    build_spill_ = std::make_unique<PartitionedSpillSet>(
        directory, kSpillPartitions, cancellation_, spill_resources_,
        memory_account_);
    probe_spill_ = std::make_unique<PartitionedSpillSet>(
        directory, kSpillPartitions, cancellation_, spill_resources_,
        memory_account_);
    Status status = build_spill_->Open();
    if (!status.ok()) return status;
    status = probe_spill_->Open();
    if (!status.ok()) return status;
    for (const auto& entry : build_) {
      const uint32_t partition = PartitionFor(entry.first);
      for (const BuildRow& row : entry.second) {
        status = CheckCancelled("query cancelled while spilling hash build");
        if (!status.ok()) return status;
        status = AppendSpilledRow(build_spill_.get(), partition, build_names_,
                                  row.values, row.ordinal);
        if (!status.ok()) return status;
      }
    }
    if (memory_account_ && reserved_bytes_ != 0) memory_account_->Release(reserved_bytes_);
    reserved_bytes_ = 0;
    build_.clear();
    spilling_ = true;
    if (stats_) {
      ++stats_->hash_join_spill_starts;
      if (stats_->operator_runtime) {
        stats_->operator_runtime->RecordHashJoinSpill(
            OperatorRuntimeKey{plan_id_, operator_id_});
      }
    }
    return Status::OK();
  }

  Status AddBuildRow(const ResultBatch& input, uint32_t row) {
    const auto key = KeyAt(input.batch(), row, build_key_columns_);
    if (!key.ok()) return key.status();
    if (!key.ValueOrDie().has_value()) return Status::OK();
    AddDynamicFilterSignature(*key.ValueOrDie());
    const uint64_t bytes = InputRowBytes(input, row, *key.ValueOrDie());
    if (!spilling_ && memory_account_) {
      const uint64_t used = memory_account_->used_bytes();
      const uint64_t soft = memory_account_->soft_limit_bytes();
      if (used >= soft || bytes >= soft - used) {
        const Status spilled = StartSpill();
        if (!spilled.ok()) return spilled;
      }
    }
    bool reserved = false;
    if (!spilling_ && memory_account_) {
      const Status status = memory_account_->Reserve(bytes);
      if (!status.ok()) {
        const Status spilled = StartSpill();
        if (!spilled.ok()) return spilled;
      } else {
        reserved_bytes_ += bytes;
        ObserveOperatorMemory(reserved_bytes_);
        reserved = true;
      }
    }
    auto stored = StoredRowAt(input, row, input.batch().column_count(), "build");
    if (!stored.ok()) {
      if (reserved && memory_account_) {
        memory_account_->Release(bytes);
        reserved_bytes_ -= bytes;
      }
      return stored.status();
    }
    const uint64_t ordinal = next_build_ordinal_++;
    if (spilling_) {
      return AppendSpilledRow(build_spill_.get(), PartitionFor(*key.ValueOrDie()),
                              build_names_, stored.ValueOrDie(), ordinal);
    }
    build_[*key.ValueOrDie()].push_back(BuildRow{std::move(stored).ConsumeValueOrDie(), ordinal});
    return Status::OK();
  }

  Status Initialize() {
    initialized_ = true;
    for (;;) {
      const Status cancelled = CheckCancelled("query cancelled while building hash table");
      if (!cancelled.ok()) return cancelled;
      ResultBatch input;
      const Status next = build_input_->Next(&input);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(build_input_.get());
        if (!terminal.ok()) return terminal;
        return spilling_ ? build_spill_->Seal() : Status::OK();
      }
      if (!next.ok()) return next;
      if (stats_) {
        stats_->hash_join_build_input_rows += input.batch().row_count();
        if (stats_->operator_runtime) {
          stats_->operator_runtime->RecordHashJoinInput(
              OperatorRuntimeKey{plan_id_, operator_id_}, true,
              input.batch().row_count());
        }
      }
      if (std::any_of(build_key_columns_.begin(), build_key_columns_.end(),
                      [&](uint32_t column) { return column >= input.batch().column_count(); })) {
        return Status::Corruption("physical hash join", "build input shape");
      }
      if (build_names_.empty()) build_names_ = input.column_names();
      for (uint32_t row = 0; row < input.batch().row_count(); ++row) {
        const Status row_cancelled = CheckCancelled("query cancelled while building hash table");
        if (!row_cancelled.ok()) return row_cancelled;
        const Status added = AddBuildRow(input, row);
        if (!added.ok()) return added;
      }
    }
  }

  Status PartitionProbe() {
    while (true) {
      const Status cancelled = CheckCancelled("query cancelled while spilling hash probe");
      if (!cancelled.ok()) return cancelled;
      ResultBatch input;
      const Status next = probe_input_->Next(&input);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(probe_input_.get());
        if (!terminal.ok()) return terminal;
        const Status sealed = probe_spill_->Seal();
        if (sealed.ok()) ReleaseDynamicFilter();
        return sealed;
      }
      if (!next.ok()) return next;
      if (stats_) {
        stats_->hash_join_probe_input_rows += input.batch().row_count();
        if (stats_->operator_runtime) {
          stats_->operator_runtime->RecordHashJoinInput(
              OperatorRuntimeKey{plan_id_, operator_id_}, false,
              input.batch().row_count());
        }
      }
      if (std::any_of(probe_key_columns_.begin(), probe_key_columns_.end(),
                      [&](uint32_t column) { return column >= input.batch().column_count(); })) {
        return Status::Corruption("physical hash join", "probe input shape");
      }
      if (probe_names_.empty()) probe_names_ = input.column_names();
      for (uint32_t row = 0; row < input.batch().row_count(); ++row) {
        const Status row_cancelled = CheckCancelled("query cancelled while spilling hash probe");
        if (!row_cancelled.ok()) return row_cancelled;
        const auto key = KeyAt(input.batch(), row, probe_key_columns_);
        if (!key.ok()) return key.status();
        const uint64_t ordinal = next_probe_ordinal_++;
        if (!ProbePassesDynamicFilter(key.ValueOrDie(), true)) continue;
        auto stored = StoredRowAt(input, row, input.batch().column_count(), "probe");
        if (!stored.ok()) return stored.status();
        const Status appended = AppendSpilledRow(
            probe_spill_.get(), PartitionFor(*key.ValueOrDie()), probe_names_,
            stored.ValueOrDie(), ordinal);
        if (!appended.ok()) return appended;
      }
    }
  }

  Status AppendOutputRunRow(uint32_t partition, const StoredRow& probe,
                            uint64_t probe_ordinal, const BuildRow& build) {
    if (probe_ordinal > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        build.ordinal > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return Status::InvalidArgument("physical hash join", "output ordinal exceeds Int64");
    }
    ColumnBatch batch(1);
    for (const auto& spec : outputs_) {
      const bool from_probe = spec.from_left == probe_is_left_;
      const StoredRow& source = from_probe ? probe : build.values;
      if (spec.column >= source.size()) {
        return Status::Corruption("physical hash join", "output column mapping exceeds child row");
      }
      const ResultValueCell& value = source[spec.column];
      if (!CellPresent(value) && !spec.nullable) {
        return Status::Corruption("physical hash join", "non-nullable output is null");
      }
      const Status added = AddSingleCell(value, spec.type, &batch);
      if (!added.ok()) return added;
    }
    for (uint64_t ordinal : {probe_ordinal, build.ordinal}) {
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::vector<Value>{Value::Int64(static_cast<int64_t>(ordinal))},
          std::vector<bool>{true}));
      if (!added.ok()) return added;
    }
    std::vector<std::string> names = names_;
    names.push_back(kRunProbeOrdinalName);
    names.push_back(kRunBuildOrdinalName);
    ResultBatch record(std::move(names), std::move(batch));
    const Status valid = record.Validate();
    if (!valid.ok()) return valid;
    const Status appended = output_spill_->Append(partition, record);
    if (appended.ok()) ReportSpillBytes();
    return appended;
  }

  Status BuildOutputRunNestedLoop(uint32_t partition) {
    Status status = probe_spill_->Rewind(partition);
    if (!status.ok()) return status;
    for (;;) {
      status = CheckCancelled(
          "query cancelled while replaying hot hash probe partition");
      if (!status.ok()) return status;
      ResultBatch probe_batch;
      status = probe_spill_->Next(partition, &probe_batch);
      if (status.IsNotFound()) break;
      if (status.IsQueryMemoryLimit()) {
        return Status::QueryMemoryLimit(
            "physical hash join", "hot probe batch exceeds memory limit");
      }
      if (!status.ok()) return status;
      for (uint32_t probe_row = 0;
           probe_row < probe_batch.batch().row_count(); ++probe_row) {
        uint64_t probe_ordinal = 0;
        auto probe = ReadSpilledRow(
            probe_batch, probe_row,
            static_cast<uint32_t>(probe_names_.size()), &probe_ordinal);
        if (!probe.ok()) return probe.status();
        const auto probe_key =
            KeyFromStoredRow(probe.ValueOrDie(), probe_key_columns_);
        if (!probe_key.ok() || !probe_key.ValueOrDie().has_value()) {
          return probe_key.ok()
              ? Status::Corruption(
                    "physical hash join", "null hot-partition probe key")
              : probe_key.status();
        }
        status = build_spill_->Rewind(partition);
        if (!status.ok()) return status;
        for (;;) {
          status = CheckCancelled(
              "query cancelled while scanning hot hash build partition");
          if (!status.ok()) return status;
          ResultBatch build_batch;
          status = build_spill_->Next(partition, &build_batch);
          if (status.IsNotFound()) break;
          if (status.IsQueryMemoryLimit()) {
            return Status::QueryMemoryLimit(
                "physical hash join", "hot build batch exceeds memory limit");
          }
          if (!status.ok()) return status;
          for (uint32_t build_row = 0;
               build_row < build_batch.batch().row_count(); ++build_row) {
            uint64_t build_ordinal = 0;
            auto build = ReadSpilledRow(
                build_batch, build_row,
                static_cast<uint32_t>(build_names_.size()), &build_ordinal);
            if (!build.ok()) return build.status();
            const auto build_key =
                KeyFromStoredRow(build.ValueOrDie(), build_key_columns_);
            if (!build_key.ok() || !build_key.ValueOrDie().has_value()) {
              return build_key.ok()
                  ? Status::Corruption(
                        "physical hash join", "null hot-partition build key")
                  : build_key.status();
            }
            if (*build_key.ValueOrDie() != *probe_key.ValueOrDie()) continue;
            const Status appended = AppendOutputRunRow(
                partition, probe.ValueOrDie(), probe_ordinal,
                BuildRow{std::move(build).ConsumeValueOrDie(), build_ordinal});
            if (appended.IsQueryMemoryLimit()) {
              return Status::QueryMemoryLimit(
                  "physical hash join", "hot output row exceeds memory limit");
            }
            if (!appended.ok()) return appended;
          }
        }
      }
    }
    status = probe_spill_->Seal(partition);
    if (!status.ok()) return status;
    status = build_spill_->Seal(partition);
    if (!status.ok()) return status;
    return output_spill_->Seal(partition);
  }

  Status BuildOutputRun(uint32_t partition) {
    if (!build_spill_->HasData(partition) || !probe_spill_->HasData(partition)) {
      return Status::OK();
    }
    std::map<std::string, std::vector<BuildRow>> local_build;
    uint64_t local_reserved = 0;
    const auto release = [&]() {
      if (memory_account_ && local_reserved != 0) memory_account_->Release(local_reserved);
    };
    Status status = build_spill_->Rewind(partition);
    if (!status.ok()) return status;
    bool use_nested_loop = false;
    for (;;) {
      const Status cancelled = CheckCancelled("query cancelled while replaying hash build");
      if (!cancelled.ok()) { release(); return cancelled; }
      ResultBatch batch;
      status = build_spill_->Next(partition, &batch);
      if (status.IsNotFound()) break;
      if (status.IsQueryMemoryLimit()) {
        use_nested_loop = true;
        break;
      }
      if (!status.ok()) { release(); return status; }
      for (uint32_t row = 0; row < batch.batch().row_count(); ++row) {
        uint64_t ordinal = 0;
        auto stored = ReadSpilledRow(batch, row, static_cast<uint32_t>(build_names_.size()), &ordinal);
        if (!stored.ok()) { release(); return stored.status(); }
        const auto key = KeyFromStoredRow(stored.ValueOrDie(), build_key_columns_);
        if (!key.ok() || !key.ValueOrDie().has_value()) {
          release();
          return key.ok() ? Status::Corruption("physical hash join", "null build spill key")
                          : key.status();
        }
        const uint64_t bytes = StoredRowBytes(stored.ValueOrDie(), *key.ValueOrDie());
        if (memory_account_) {
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) {
            use_nested_loop = true;
            break;
          }
          local_reserved += bytes;
          ObserveOperatorMemory(local_reserved);
        }
        local_build[*key.ValueOrDie()].push_back(
            BuildRow{std::move(stored).ConsumeValueOrDie(), ordinal});
      }
      if (use_nested_loop) break;
    }
    if (use_nested_loop) {
      release();
      local_reserved = 0;
      local_build.clear();
      return BuildOutputRunNestedLoop(partition);
    }
    status = build_spill_->Seal(partition);
    if (!status.ok()) { release(); return status; }
    status = probe_spill_->Rewind(partition);
    if (!status.ok()) { release(); return status; }
    for (;;) {
      const Status cancelled = CheckCancelled("query cancelled while replaying hash probe");
      if (!cancelled.ok()) { release(); return cancelled; }
      ResultBatch batch;
      status = probe_spill_->Next(partition, &batch);
      if (status.IsNotFound()) break;
      if (!status.ok()) { release(); return status; }
      for (uint32_t row = 0; row < batch.batch().row_count(); ++row) {
        uint64_t ordinal = 0;
        auto probe = ReadSpilledRow(batch, row, static_cast<uint32_t>(probe_names_.size()), &ordinal);
        if (!probe.ok()) { release(); return probe.status(); }
        const auto key = KeyFromStoredRow(probe.ValueOrDie(), probe_key_columns_);
        if (!key.ok() || !key.ValueOrDie().has_value()) {
          release();
          return key.ok() ? Status::Corruption("physical hash join", "null probe spill key")
                          : key.status();
        }
        const auto found = local_build.find(*key.ValueOrDie());
        if (found == local_build.end()) continue;
        for (const BuildRow& build : found->second) {
          const Status match_cancelled = CheckCancelled("query cancelled while writing hash output run");
          if (!match_cancelled.ok()) { release(); return match_cancelled; }
          const Status appended = AppendOutputRunRow(partition, probe.ValueOrDie(), ordinal, build);
          if (!appended.ok()) { release(); return appended; }
        }
      }
    }
    status = probe_spill_->Seal(partition);
    if (!status.ok()) { release(); return status; }
    status = output_spill_->Seal(partition);
    if (!status.ok()) { release(); return status; }
    release();
    return Status::OK();
  }

  Status LoadOutputCursor(uint32_t partition) {
    SpillCursor& cursor = output_cursors_[partition];
    cursor.has_row = false;
    while (true) {
      const Status cancelled = CheckCancelled("query cancelled while merging hash output");
      if (!cancelled.ok()) return cancelled;
      ResultBatch batch;
      const Status next = output_spill_->Next(partition, &batch);
      if (next.IsNotFound()) return Status::OK();
      if (!next.ok()) return next;
      if (batch.batch().row_count() == 0) continue;
      cursor.batch = std::move(batch);
      cursor.row = 0;
      cursor.has_row = true;
      return Status::OK();
    }
  }

  Status PrepareSpilledOutput() {
    if (spill_prepared_) return Status::OK();
    Status status = PartitionProbe();
    if (!status.ok()) return status;
    output_spill_ = std::make_unique<PartitionedSpillSet>(
        spill_directory_.empty() ? "/tmp" : spill_directory_,
        kSpillPartitions, cancellation_, spill_resources_, memory_account_);
    status = output_spill_->Open();
    if (!status.ok()) return status;
    for (uint32_t partition = 0; partition < kSpillPartitions; ++partition) {
      status = CheckCancelled("query cancelled while preparing hash output runs");
      if (!status.ok()) return status;
      status = BuildOutputRun(partition);
      if (!status.ok()) return status;
    }
    output_cursors_.resize(kSpillPartitions);
    for (uint32_t partition = 0; partition < kSpillPartitions; ++partition) {
      if (!output_spill_->HasData(partition)) continue;
      status = output_spill_->Rewind(partition);
      if (!status.ok()) return status;
      status = LoadOutputCursor(partition);
      if (!status.ok()) return status;
    }
    spill_prepared_ = true;
    return Status::OK();
  }

  Status NextSpilled(ResultBatch* output) {
    const Status prepared = PrepareSpilledOutput();
    if (prepared.IsQueryMemoryLimit()) {
      return terminal_ = Status::QueryMemoryLimit(
          "physical hash join", "spill output preparation exceeds memory limit");
    }
    if (!prepared.ok()) return terminal_ = prepared;
    std::vector<std::vector<ResultValueCell>> values(outputs_.size());
    uint32_t output_rows = 0;
    while (output_rows < capacity_) {
      const Status cancelled = CheckCancelled("query cancelled while merging hash output");
      if (!cancelled.ok()) return terminal_ = cancelled;
      std::optional<uint32_t> selected;
      uint64_t selected_probe = 0;
      uint64_t selected_build = 0;
      for (uint32_t partition = 0; partition < output_cursors_.size(); ++partition) {
        const SpillCursor& cursor = output_cursors_[partition];
        if (!cursor.has_row) continue;
        const uint32_t probe_column = static_cast<uint32_t>(outputs_.size());
        const auto probe = ReadOrdinal(cursor.batch, cursor.row, probe_column);
        const auto build = ReadOrdinal(cursor.batch, cursor.row, probe_column + 1);
        if (!probe.ok()) return terminal_ = probe.status();
        if (!build.ok()) return terminal_ = build.status();
        if (!selected.has_value() || std::tie(probe.ValueOrDie(), build.ValueOrDie()) <
                                     std::tie(selected_probe, selected_build)) {
          selected = partition;
          selected_probe = probe.ValueOrDie();
          selected_build = build.ValueOrDie();
        }
      }
      if (!selected.has_value()) break;
      SpillCursor& cursor = output_cursors_[*selected];
      for (size_t column = 0; column < outputs_.size(); ++column) {
        values[column].push_back(CellAt(
            cursor.batch.batch(), static_cast<uint32_t>(column), cursor.row));
      }
      ++output_rows;
      if (++cursor.row == cursor.batch.batch().row_count()) {
        const Status loaded = LoadOutputCursor(*selected);
        if (loaded.IsQueryMemoryLimit()) {
          return terminal_ = Status::QueryMemoryLimit(
              "physical hash join", "spill output cursor exceeds memory limit");
        }
        if (!loaded.ok()) return terminal_ = loaded;
      }
    }
    if (output_rows == 0) return Status::NotFound("physical hash join", "end of stream");
    uint64_t output_bytes = sizeof(FlatVector) * outputs_.size();
    for (const auto& column : values) {
      output_bytes += static_cast<uint64_t>(column.size()) *
          (sizeof(ResultValueCell) + sizeof(bool));
      for (const ResultValueCell& value : column) {
        output_bytes += CellPayloadBytes(value);
      }
    }
    if (memory_account_) {
      const Status reserved = memory_account_->Reserve(output_bytes);
      if (reserved.IsQueryMemoryLimit()) {
        return terminal_ = Status::QueryMemoryLimit(
            "physical hash join", "spill output batch exceeds memory limit");
      }
      if (!reserved.ok()) return terminal_ = reserved;
    }
    auto lease = std::make_shared<RuntimeMemoryLease>(memory_account_, output_bytes);
    ObserveOperatorMemory(output_bytes);
    ColumnBatch batch(output_rows);
    for (size_t column = 0; column < values.size(); ++column) {
      const Status added = AddCellColumn(
          std::move(values[column]), outputs_[column].type, lease, &batch);
      if (!added.ok()) return terminal_ = added;
    }
    *output = ResultBatch(names_, std::move(batch));
    const Status valid = output->Validate();
    if (!valid.ok()) return terminal_ = valid;
    if (stats_) {
      stats_->last_physical_plan_id = plan_id_;
      stats_->executed_physical_plan_id = plan_id_;
      stats_->physical_output_rows += output->batch().row_count();
      if (stats_->operator_runtime) {
        stats_->operator_runtime->RecordHashJoinOutput(
            OperatorRuntimeKey{plan_id_, operator_id_},
            output->batch().row_count());
      }
    }
    return Status::OK();
  }

  static StatusOr<std::optional<std::string>> KeyAt(
      const ColumnBatch& batch, uint32_t row,
      const std::vector<uint32_t>& columns) {
    if (columns.empty()) {
      return Status::Corruption("physical hash join", "join key has no columns");
    }
    std::string key;
    for (uint32_t column : columns) {
      const auto value = batch.ValueAt(column, row);
      if (!value.has_value()) return std::optional<std::string>{};
      const std::string encoded = value->Encode();
      if (encoded.size() > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument(
            "physical hash join", "join key component exceeds length bound");
      }
      const uint32_t length = static_cast<uint32_t>(encoded.size());
      for (uint32_t byte = 0; byte < 4; ++byte) {
        key.push_back(static_cast<char>(length >> (byte * 8)));
      }
      key.append(encoded);
    }
    return std::optional<std::string>{std::move(key)};
  }
  static Value NullPlaceholder(PhysicalType type) {
    switch (type) {
      case PhysicalType::kBool: return Value::Bool(false);
      case PhysicalType::kInt32: return Value::Int32(0);
      case PhysicalType::kInt64: return Value::Int64(0);
      case PhysicalType::kFloat32: return Value::Float32(0);
      case PhysicalType::kFloat64: return Value::Float64(0);
      case PhysicalType::kTimestamp64: return Value::Timestamp(0);
      case PhysicalType::kString: return Value::String("");
      case PhysicalType::kBinary: return Value::Binary("");
    }
    return Value::Binary("");
  }
  void ObserveOperatorMemory(uint64_t bytes) {
    if (stats_ && stats_->operator_runtime) {
      stats_->operator_runtime->ObserveMemory(
          OperatorRuntimeKey{plan_id_, operator_id_}, bytes);
    }
  }
  void ReportSpillBytes() {
    uint64_t total = 0;
    for (const PartitionedSpillSet* spill :
         {build_spill_.get(), probe_spill_.get(), output_spill_.get()}) {
      if (spill != nullptr) total += spill->bytes_written();
    }
    if (total <= reported_spill_bytes_) return;
    if (stats_ && stats_->operator_runtime) {
      stats_->operator_runtime->AddSpill(
          OperatorRuntimeKey{plan_id_, operator_id_},
          total - reported_spill_bytes_);
    }
    reported_spill_bytes_ = total;
  }
  std::unique_ptr<QueryResultStream> build_input_, probe_input_;
  std::map<std::string, std::vector<BuildRow>> build_;
  std::set<uint64_t> build_filter_hashes_;
  uint64_t dynamic_filter_reserved_bytes_ = 0;
  bool dynamic_filter_enabled_ = true;
  std::vector<std::string> names_;
  std::vector<PhysicalHashJoinPlan::Output> outputs_;
  std::vector<uint32_t> build_key_columns_;
  std::vector<uint32_t> probe_key_columns_;
  uint32_t capacity_;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  std::shared_ptr<TcypherExecutionStats> stats_;
  uint64_t plan_id_;
  uint32_t operator_id_ = 0;
  bool probe_is_left_ = true;
  uint64_t reserved_bytes_ = 0;
  uint64_t reported_spill_bytes_ = 0;
  uint64_t next_build_ordinal_ = 0;
  uint64_t next_probe_ordinal_ = 0;
  std::vector<std::string> build_names_;
  std::vector<std::string> probe_names_;
  std::unique_ptr<PartitionedSpillSet> build_spill_;
  std::unique_ptr<PartitionedSpillSet> probe_spill_;
  std::unique_ptr<PartitionedSpillSet> output_spill_;
  std::vector<SpillCursor> output_cursors_;
  std::optional<ResultBatch> probe_;
  const std::vector<BuildRow>* matches_ = nullptr;
  StoredRow probe_values_;
  uint32_t probe_row_ = 0;
  size_t match_index_ = 0;
  bool initialized_ = false;
  bool spilling_ = false;
  bool spill_prepared_ = false;
  Status terminal_ = Status::OK();
};

class PhysicalCrossJoinResultStream final : public QueryResultStream {
 public:
  using StoredRow = std::vector<ResultValueCell>;
  struct RetainedRow {
    StoredRow row;
    uint64_t row_charge = 0;
  };

  PhysicalCrossJoinResultStream(
      std::unique_ptr<QueryResultStream> build_input,
      std::unique_ptr<QueryResultStream> probe_input,
      std::vector<std::string> names,
      std::vector<PhysicalHashJoinPlan::Output> outputs,
      std::vector<PhysicalType> build_types, uint32_t capacity,
      std::shared_ptr<QueryCancellation> cancellation,
      std::shared_ptr<QueryMemoryAccount> memory_account,
      std::string spill_directory,
      std::shared_ptr<ResourceGovernorExtension> spill_resources,
      std::shared_ptr<TcypherExecutionStats> stats, uint64_t plan_id,
      uint32_t operator_id, bool probe_is_left)
      : build_input_(std::move(build_input)),
        probe_input_(std::move(probe_input)), names_(std::move(names)),
        outputs_(std::move(outputs)), build_types_(std::move(build_types)),
        capacity_(capacity),
        cancellation_(std::move(cancellation)),
        memory_account_(std::move(memory_account)),
        spill_directory_(std::move(spill_directory)),
        spill_resources_(std::move(spill_resources)), stats_(std::move(stats)),
        plan_id_(plan_id), operator_id_(operator_id),
        probe_is_left_(probe_is_left) {}

  ~PhysicalCrossJoinResultStream() override {
    std::vector<RetainedRow>().swap(build_rows_);
    if (memory_account_ && reserved_bytes_ != 0) {
      memory_account_->Release(reserved_bytes_);
    }
  }

  Status Next(ResultBatch* output) override {
    if (output == nullptr) {
      return Status::InvalidArgument("physical cross join", "missing output");
    }
    if (!terminal_.ok()) return terminal_;
    if (!initialized_) {
      const Status initialized = Initialize();
      if (!initialized.ok()) return terminal_ = initialized;
    }
    if ((!spilling_ && build_rows_.empty()) ||
        (spilling_ && !build_spill_->HasData(0))) {
      return Status::NotFound("physical cross join", "end of stream");
    }
    if (spilling_) return NextSpilled(output);

    auto output_lease = std::make_shared<RuntimeMemoryLease>(memory_account_, 0);
    uint64_t output_bytes = 0;
    std::vector<std::vector<ResultValueCell>> values;
    const Status prepared = PrepareOutputAccumulator(
        output_lease, &output_bytes, &values);
    if (!prepared.ok()) {
      if (!prepared.IsQueryMemoryLimit()) return terminal_ = prepared;
      spilled_build_rows_to_skip_ = build_index_;
      output_lease.reset();
      const Status spilled = StartSpill();
      if (!spilled.ok()) return terminal_ = spilled;
      return NextSpilled(output);
    }
    uint32_t output_rows = 0;
    while (output_rows < capacity_) {
      const Status cancelled = CheckCancelled("query cancelled while producing cross join");
      if (!cancelled.ok()) return terminal_ = cancelled;
      if (!have_probe_row_) {
        const Status loaded = LoadProbeRow();
        if (loaded.IsNotFound()) break;
        if (!loaded.ok()) return terminal_ = loaded;
      }
      const StoredRow& build = build_rows_[build_index_++].row;
      const RowView probe{nullptr, &*probe_batch_, probe_row_};
      const RowView build_view{&build, nullptr, 0};
      const Status reserved = ReserveOutputPayload(
          probe, build_view, output_lease, &output_bytes);
      if (!reserved.ok()) {
        if (!reserved.IsQueryMemoryLimit()) return terminal_ = reserved;
        --build_index_;
        if (output_rows != 0) break;
        spilled_build_rows_to_skip_ = build_index_;
        values.clear();
        output_lease.reset();
        const Status spilled = StartSpill();
        if (!spilled.ok()) return terminal_ = spilled;
        return NextSpilled(output);
      }
      const Status appended = AppendOutputRow(probe, build_view, &values);
      if (!appended.ok()) return terminal_ = appended;
      ++output_rows;
      if (build_index_ == build_rows_.size()) {
        build_index_ = 0;
        have_probe_row_ = false;
        ++probe_row_;
      }
    }
    if (output_rows == 0) {
      return Status::NotFound("physical cross join", "end of stream");
    }

    return FinishOutput(std::move(values), output_rows, output_lease,
                        output_bytes, output);
  }

  Status terminal_status() const override {
    if (!terminal_.ok()) return terminal_;
    if (build_input_ && !build_input_->terminal_status().ok()) {
      return build_input_->terminal_status();
    }
    return probe_input_ ? probe_input_->terminal_status() : Status::OK();
  }

  uint64_t retained_capacity_for_testing() const {
    return build_rows_.capacity();
  }

 private:
  struct RowView {
    const StoredRow* stored = nullptr;
    const ResultBatch* batch = nullptr;
    uint32_t row = 0;
  };

  class StoredCellViewVector final : public Vector {
   public:
    explicit StoredCellViewVector(const ResultValueCell* cell) : cell_(cell) {}
    uint32_t size() const override { return cell_ == nullptr ? 0 : 1; }
    std::optional<Value> ValueAt(uint32_t row) const override {
      return row == 0 && cell_ != nullptr &&
              cell_->kind == ResultValueKind::kScalar
          ? cell_->scalar : std::optional<Value>{};
    }
    const Value* ValueRefAt(uint32_t row) const override {
      return row == 0 && cell_ != nullptr &&
              cell_->kind == ResultValueKind::kScalar &&
              cell_->scalar.has_value()
          ? &*cell_->scalar : nullptr;
    }
    uint64_t ValuePayloadBytesAt(uint32_t row) const override {
      return row == 0 && cell_ != nullptr ? CellLogicalPayloadBytes(*cell_) : 0;
    }
    uint64_t RetainedPayloadBytesAt(uint32_t row) const override {
      return row == 0 && cell_ != nullptr ? CellPayloadBytes(*cell_) : 0;
    }
    const StructValue* StructRefAt(uint32_t row) const override {
      return row == 0 && cell_ != nullptr &&
              cell_->kind == ResultValueKind::kStruct &&
              cell_->structure.has_value()
          ? &*cell_->structure : nullptr;
    }
    const ListValue* ListRefAt(uint32_t row) const override {
      return row == 0 && cell_ != nullptr &&
              cell_->kind == ResultValueKind::kList && cell_->list.has_value()
          ? &*cell_->list : nullptr;
    }
    std::optional<StructValue> StructAt(uint32_t row) const override {
      const StructValue* value = StructRefAt(row);
      return value == nullptr ? std::optional<StructValue>{} : *value;
    }
    std::optional<ListValue> ListAt(uint32_t row) const override {
      const ListValue* value = ListRefAt(row);
      return value == nullptr ? std::optional<ListValue>{} : *value;
    }
    bool is_structured() const override {
      return cell_ != nullptr && cell_->kind == ResultValueKind::kStruct;
    }
    bool is_list() const override {
      return cell_ != nullptr && cell_->kind == ResultValueKind::kList;
    }

   private:
    const ResultValueCell* cell_ = nullptr;
  };

  static ResultValueCell CellAt(
      const ColumnBatch& batch, uint32_t column, uint32_t row) {
    ResultValueCell cell;
    if (batch.IsStructured(column)) {
      cell.kind = ResultValueKind::kStruct;
      cell.structure = batch.StructAt(column, row);
    } else if (batch.IsList(column)) {
      cell.kind = ResultValueKind::kList;
      cell.list = batch.ListAt(column, row);
    } else {
      cell.kind = ResultValueKind::kScalar;
      cell.scalar = batch.ValueAt(column, row);
    }
    return cell;
  }

  static bool CellPresent(const ResultValueCell& cell) {
    switch (cell.kind) {
      case ResultValueKind::kScalar: return cell.scalar.has_value();
      case ResultValueKind::kStruct: return cell.structure.has_value();
      case ResultValueKind::kList: return cell.list.has_value();
    }
    return false;
  }

  static uint64_t OwnedValuePayloadBytes(const Value& value) {
    switch (value.type()) {
      case PhysicalType::kBool: return 0;
      case PhysicalType::kInt32:
      case PhysicalType::kFloat32: return 0;
      case PhysicalType::kInt64:
      case PhysicalType::kFloat64:
      case PhysicalType::kTimestamp64: return 0;
      case PhysicalType::kString:
      case PhysicalType::kBinary:
        return std::get<std::string>(value.data()).size();
    }
    return 0;
  }

  static uint64_t CellLogicalPayloadBytes(const ResultValueCell& cell) {
    if (!CellPresent(cell)) return 0;
    if (cell.kind == ResultValueKind::kScalar) {
      return OwnedValuePayloadBytes(*cell.scalar);
    }
    uint64_t bytes = 0;
    if (cell.kind == ResultValueKind::kStruct) {
      bytes += static_cast<uint64_t>(cell.structure->fields.size()) *
          sizeof(StructField);
      for (const StructField& field : cell.structure->fields) {
        if (field.value.has_value()) bytes += OwnedValuePayloadBytes(*field.value);
      }
      return bytes;
    }
    if (cell.list->element_kind == ListElementKind::kScalar) {
      bytes += static_cast<uint64_t>(cell.list->elements.size()) *
          sizeof(std::optional<Value>);
      for (const auto& element : cell.list->elements) {
        if (element.has_value()) bytes += OwnedValuePayloadBytes(*element);
      }
    } else {
      bytes += static_cast<uint64_t>(cell.list->structured_elements.size()) *
          sizeof(StructValue);
      for (const StructValue& element : cell.list->structured_elements) {
        bytes += static_cast<uint64_t>(element.fields.size()) * sizeof(StructField);
        for (const StructField& field : element.fields) {
          if (field.value.has_value()) bytes += OwnedValuePayloadBytes(*field.value);
        }
      }
    }
    return bytes;
  }

  static uint64_t CellPayloadBytes(const ResultValueCell& cell) {
    if (!CellPresent(cell)) return 0;
    if (cell.kind == ResultValueKind::kScalar) {
      return OwnedValuePayloadBytes(*cell.scalar);
    }
    uint64_t bytes = 0;
    if (cell.kind == ResultValueKind::kStruct) {
      bytes += static_cast<uint64_t>(cell.structure->fields.capacity()) *
          sizeof(StructField);
      for (const StructField& field : cell.structure->fields) {
        bytes += field.name.capacity();
        if (field.value.has_value()) {
          bytes += OwnedValuePayloadBytes(*field.value);
        }
      }
      return bytes;
    }
    if (cell.list->element_kind == ListElementKind::kScalar) {
      bytes += static_cast<uint64_t>(cell.list->elements.capacity()) *
          sizeof(std::optional<Value>);
      for (const std::optional<Value>& element : cell.list->elements) {
        if (element.has_value()) {
          bytes += OwnedValuePayloadBytes(*element);
        }
      }
    } else {
      bytes += static_cast<uint64_t>(
          cell.list->structured_elements.capacity()) * sizeof(StructValue);
      for (const StructValue& element : cell.list->structured_elements) {
        bytes += static_cast<uint64_t>(element.fields.capacity()) *
            sizeof(StructField);
        for (const StructField& field : element.fields) {
          bytes += field.name.capacity();
          if (field.value.has_value()) {
            bytes += OwnedValuePayloadBytes(*field.value);
          }
        }
      }
    }
    return bytes;
  }

  static Value NullPlaceholder(PhysicalType type) {
    switch (type) {
      case PhysicalType::kBool: return Value::Bool(false);
      case PhysicalType::kInt32: return Value::Int32(0);
      case PhysicalType::kInt64: return Value::Int64(0);
      case PhysicalType::kFloat32: return Value::Float32(0);
      case PhysicalType::kFloat64: return Value::Float64(0);
      case PhysicalType::kTimestamp64: return Value::Timestamp(0);
      case PhysicalType::kString: return Value::String("");
      case PhysicalType::kBinary: return Value::Binary("");
    }
    return Value::Binary("");
  }

  static Status AddCellColumn(
      std::vector<ResultValueCell> cells, PhysicalType scalar_type,
      const std::shared_ptr<void>& retention, ColumnBatch* batch) {
    if (batch == nullptr || cells.empty()) {
      return Status::InvalidArgument(
          "physical cross join", "missing output cell column");
    }
    const ResultValueKind kind = cells.front().kind;
    if (std::any_of(cells.begin(), cells.end(), [&](const ResultValueCell& cell) {
          return cell.kind != kind;
        })) {
      return Status::Corruption(
          "physical cross join", "output column changes value shape");
    }
    std::vector<bool> validity;
    validity.reserve(cells.size());
    if (kind == ResultValueKind::kStruct) {
      std::vector<StructValue> typed;
      typed.reserve(cells.size());
      for (ResultValueCell& cell : cells) {
        validity.push_back(cell.structure.has_value());
        typed.push_back(cell.structure.has_value()
                            ? std::move(*cell.structure) : StructValue{});
      }
      return batch->AddVector(std::make_shared<StructVector>(
          std::move(typed), std::move(validity), retention));
    }
    if (kind == ResultValueKind::kList) {
      std::vector<ListValue> typed;
      typed.reserve(cells.size());
      for (ResultValueCell& cell : cells) {
        validity.push_back(cell.list.has_value());
        typed.push_back(cell.list.has_value()
                            ? std::move(*cell.list) : ListValue{});
      }
      return batch->AddVector(std::make_shared<ListVector>(
          std::move(typed), std::move(validity), retention));
    }
    std::vector<Value> typed;
    typed.reserve(cells.size());
    for (ResultValueCell& cell : cells) {
      validity.push_back(cell.scalar.has_value());
      typed.push_back(cell.scalar.has_value()
                          ? std::move(*cell.scalar)
                          : NullPlaceholder(scalar_type));
    }
    return batch->AddVector(std::make_shared<FlatVector>(
        std::move(typed), std::move(validity), retention));
  }

  Status CheckCancelled(const char* phase) const {
    return cancellation_ && cancellation_->IsCancelled()
        ? Status::QueryCancelled("physical cross join", phase)
        : Status::OK();
  }

  static uint64_t InputRowBytes(const ResultBatch& input, uint32_t row) {
    uint64_t bytes = static_cast<uint64_t>(input.batch().column_count()) *
        sizeof(ResultValueCell);
    for (uint32_t column = 0; column < input.batch().column_count(); ++column) {
      bytes += input.batch().RetainedPayloadBytesAt(column, row);
    }
    return bytes;
  }

  static StoredRow StoredRowAt(
      const ResultBatch& input, uint32_t row) {
    StoredRow stored;
    stored.reserve(input.batch().column_count());
    for (uint32_t column = 0; column < input.batch().column_count(); ++column) {
      stored.push_back(CellAt(input.batch(), column, row));
    }
    return stored;
  }

  static size_t RowSize(const RowView& row) {
    return row.stored != nullptr ? row.stored->size()
                                 : row.batch->batch().column_count();
  }

  static uint64_t RowCellPayloadBytes(
      const RowView& row, uint32_t column) {
    return row.stored != nullptr
        ? CellPayloadBytes((*row.stored)[column])
        : row.batch->batch().RetainedPayloadBytesAt(column, row.row);
  }

  static ResultValueCell RowCellAt(const RowView& row, uint32_t column) {
    return row.stored != nullptr ? (*row.stored)[column]
                                 : CellAt(row.batch->batch(), column, row.row);
  }

  Status PrepareOutputAccumulator(
      const std::shared_ptr<RuntimeMemoryLease>& lease,
      uint64_t* reserved_bytes,
      std::vector<std::vector<ResultValueCell>>* values) {
    if (!lease || reserved_bytes == nullptr || values == nullptr) {
      return Status::InvalidArgument(
          "physical cross join", "missing output reservation");
    }
    const uint64_t typed_element_bytes = std::max({
        sizeof(Value), sizeof(StructValue), sizeof(ListValue)});
    const uint64_t output_vector_bytes = std::max({
        sizeof(FlatVector), sizeof(StructVector), sizeof(ListVector)});
    const uint64_t fixed_bytes =
        static_cast<uint64_t>(outputs_.size()) *
        (output_vector_bytes +
         2U * sizeof(std::vector<ResultValueCell>) +
         sizeof(std::vector<Value>) + sizeof(std::vector<bool>) +
         static_cast<uint64_t>(capacity_) *
             (sizeof(ResultValueCell) + typed_element_bytes +
              2U * sizeof(bool)));
    ObserveOperatorMemory(reserved_bytes_);
    const Status reserved = lease->ReserveAdditional(fixed_bytes);
    if (!reserved.ok()) return reserved;
    *reserved_bytes = fixed_bytes;
    ObserveOperatorMemory(reserved_bytes_ + *reserved_bytes);
    values->resize(outputs_.size());
    for (auto& column : *values) column.reserve(capacity_);
    return Status::OK();
  }

  Status ReserveOutputPayload(
      const RowView& probe, const RowView& build,
      const std::shared_ptr<RuntimeMemoryLease>& lease,
      uint64_t* reserved_bytes) {
    uint64_t payload_bytes = 0;
    for (const PhysicalHashJoinPlan::Output& spec : outputs_) {
      const RowView& source =
          spec.from_left == probe_is_left_ ? probe : build;
      if (spec.column >= RowSize(source)) {
        return Status::Corruption(
            "physical cross join", "output column mapping exceeds child row");
      }
      payload_bytes += RowCellPayloadBytes(source, spec.column);
    }
    ObserveOperatorMemory(reserved_bytes_ + *reserved_bytes);
    const Status reserved = lease->ReserveAdditional(payload_bytes);
    if (!reserved.ok()) return reserved;
    *reserved_bytes += payload_bytes;
    ObserveOperatorMemory(reserved_bytes_ + *reserved_bytes);
    return Status::OK();
  }

  Status AppendOutputRow(
      const RowView& probe, const RowView& build,
      std::vector<std::vector<ResultValueCell>>* values) const {
    if (values == nullptr || values->size() != outputs_.size()) {
      return Status::InvalidArgument(
          "physical cross join", "missing output accumulator");
    }
    for (size_t output_index = 0; output_index < outputs_.size(); ++output_index) {
      const PhysicalHashJoinPlan::Output& spec = outputs_[output_index];
      const bool from_probe = spec.from_left == probe_is_left_;
      const RowView& source = from_probe ? probe : build;
      if (spec.column >= RowSize(source)) {
        return Status::Corruption(
            "physical cross join", "output column mapping exceeds child row");
      }
      ResultValueCell value = RowCellAt(source, spec.column);
      if (!CellPresent(value) && !spec.nullable) {
        return Status::Corruption(
            "physical cross join", "non-nullable output is null");
      }
      (*values)[output_index].push_back(std::move(value));
    }
    return Status::OK();
  }

  Status FinishOutput(
      std::vector<std::vector<ResultValueCell>> values,
      uint32_t output_rows,
      const std::shared_ptr<RuntimeMemoryLease>& lease,
      uint64_t output_bytes, ResultBatch* output) {
    ColumnBatch batch(output_rows);
    for (size_t column = 0; column < values.size(); ++column) {
      const Status added = AddCellColumn(
          std::move(values[column]), outputs_[column].type, lease, &batch);
      if (!added.ok()) return added;
    }
    *output = ResultBatch(names_, std::move(batch));
    const Status valid = output->Validate();
    if (!valid.ok()) return valid;
    if (stats_) {
      stats_->last_physical_plan_id = plan_id_;
      stats_->executed_physical_plan_id = plan_id_;
      stats_->physical_output_rows += output->batch().row_count();
      stats_->cross_join_output_rows += output->batch().row_count();
      if (stats_->operator_runtime) {
        ObserveOperatorMemory(reserved_bytes_ + output_bytes);
        stats_->operator_runtime->RecordCrossJoinOutput(
            OperatorRuntimeKey{plan_id_, operator_id_},
            output->batch().row_count());
      }
    }
    return Status::OK();
  }

  Status AppendStoredRow(const StoredRow& row) {
    if (!build_spill_ || row.size() != build_types_.size()) {
      return Status::Corruption(
          "physical cross join", "spill row shape differs from replay input");
    }
    ColumnBatch batch(1);
    for (size_t column = 0; column < row.size(); ++column) {
      const Status added = batch.AddVector(
          std::make_shared<StoredCellViewVector>(&row[column]));
      if (!added.ok()) return added;
    }
    ResultBatch record(build_names_, std::move(batch));
    const Status valid = record.Validate();
    if (!valid.ok()) return valid;
    return build_spill_->Append(0, record);
  }

  Status AppendInputRow(const ResultBatch& input, uint32_t row) {
    if (!build_spill_ || input.batch().column_count() != build_types_.size()) {
      return Status::Corruption(
          "physical cross join", "spill input shape differs from replay input");
    }
    const auto source_row = input.batch().SourceRowAt(row);
    if (!source_row.has_value()) {
      return Status::Corruption(
          "physical cross join", "spill input row is out of range");
    }
    ColumnBatch batch(1);
    for (uint32_t column = 0; column < input.batch().column_count(); ++column) {
      const Status added = batch.AddVector(std::make_shared<SliceVector>(
          input.batch().VectorAt(column), *source_row, 1));
      if (!added.ok()) return added;
    }
    ResultBatch record(build_names_, std::move(batch));
    const Status valid = record.Validate();
    if (!valid.ok()) return valid;
    return build_spill_->Append(0, record);
  }

  Status StartSpill() {
    if (spilling_) return Status::OK();
    const std::string directory =
        spill_directory_.empty() ? "/tmp" : spill_directory_;
    build_spill_ = std::make_unique<PartitionedSpillSet>(
        directory, 1, cancellation_, spill_resources_, memory_account_,
        [stats = stats_, plan_id = plan_id_, operator_id = operator_id_](
            uint64_t bytes) {
          if (!stats) return;
          stats->cross_join_spill_bytes += bytes;
          if (stats->operator_runtime) {
            stats->operator_runtime->AddSpill(
                OperatorRuntimeKey{plan_id, operator_id}, bytes);
          }
        });
    Status status = build_spill_->Open();
    if (!status.ok()) return status;
    for (RetainedRow& retained : build_rows_) {
      status = CheckCancelled(
          "query cancelled while spilling cross join replay side");
      if (!status.ok()) return status;
      status = AppendStoredRow(retained.row);
      if (!status.ok()) return status;
      const uint64_t released = retained.row_charge;
      StoredRow{}.swap(retained.row);
      retained.row_charge = 0;
      ObserveOperatorMemory(reserved_bytes_);
      if (memory_account_ && released != 0) memory_account_->Release(released);
      reserved_bytes_ -= std::min(reserved_bytes_, released);
    }
    std::vector<RetainedRow>().swap(build_rows_);
    const uint64_t released_container = retained_rows_reserved_bytes_;
    if (memory_account_ && released_container != 0) {
      memory_account_->Release(released_container);
    }
    reserved_bytes_ -= std::min(reserved_bytes_, released_container);
    retained_rows_reserved_bytes_ = 0;
    ObserveOperatorMemory(reserved_bytes_);
    spilling_ = true;
    if (stats_) {
      ++stats_->cross_join_spill_starts;
      if (stats_->operator_runtime) {
        stats_->operator_runtime->RecordCrossJoinSpill(
            OperatorRuntimeKey{plan_id_, operator_id_});
      }
    }
    return Status::OK();
  }

  uint64_t RetainedCapacityGrowthBytes() const {
    if (build_rows_.size() != build_rows_.capacity()) return 0;
    const size_t next_capacity =
        build_rows_.capacity() == 0 ? 1 : build_rows_.capacity() * 2;
    return static_cast<uint64_t>(next_capacity) * sizeof(RetainedRow);
  }

  Status ReserveRetainedCapacityForAppend() {
    if (build_rows_.size() != build_rows_.capacity()) return Status::OK();
    const size_t next_capacity =
        build_rows_.capacity() == 0 ? 1 : build_rows_.capacity() * 2;
    const uint64_t next_charge =
        static_cast<uint64_t>(next_capacity) * sizeof(RetainedRow);
    const uint64_t old_charge = retained_rows_reserved_bytes_;
    if (memory_account_) {
      ObserveOperatorMemory(reserved_bytes_);
      const Status reserved = memory_account_->Reserve(next_charge);
      if (!reserved.ok()) return reserved;
      reserved_bytes_ += next_charge;
      ObserveOperatorMemory(reserved_bytes_);
    }
    build_rows_.reserve(next_capacity);
    if (memory_account_ && old_charge != 0) {
      memory_account_->Release(old_charge);
      reserved_bytes_ -= std::min(reserved_bytes_, old_charge);
    }
    retained_rows_reserved_bytes_ = memory_account_ ? next_charge : 0;
    ObserveOperatorMemory(reserved_bytes_);
    return Status::OK();
  }

  Status AddBuildRow(const ResultBatch& input, uint32_t row) {
    const uint64_t bytes = InputRowBytes(input, row);
    const uint64_t capacity_growth = RetainedCapacityGrowthBytes();
    if (!spilling_ && memory_account_) {
      const uint64_t used = memory_account_->used_bytes();
      const uint64_t soft = memory_account_->soft_limit_bytes();
      if (used >= soft || bytes + capacity_growth >= soft - used) {
        const Status spilled = StartSpill();
        if (!spilled.ok()) return spilled;
      }
    }
    if (spilling_) return AppendInputRow(input, row);
    if (memory_account_) {
      ObserveOperatorMemory(reserved_bytes_);
      const Status reserved = memory_account_->Reserve(bytes);
      if (!reserved.ok()) {
        const Status spilled = StartSpill();
        if (!spilled.ok()) return spilled;
        return AppendInputRow(input, row);
      }
      reserved_bytes_ += bytes;
      ObserveOperatorMemory(reserved_bytes_);
      const Status capacity = ReserveRetainedCapacityForAppend();
      if (!capacity.ok()) {
        memory_account_->Release(bytes);
        reserved_bytes_ -= std::min(reserved_bytes_, bytes);
        const Status spilled = StartSpill();
        if (!spilled.ok()) return spilled;
        return AppendInputRow(input, row);
      }
    } else {
      reserved_bytes_ += bytes;
    }
    ObserveOperatorMemory(reserved_bytes_);
    build_rows_.push_back(RetainedRow{StoredRowAt(input, row), bytes});
    return Status::OK();
  }

  StatusOr<uint32_t> NextSpilledBuildRow() {
    for (;;) {
      if (spilled_build_batch_.has_value() &&
          spilled_build_row_ >= spilled_build_batch_->batch().row_count()) {
        spilled_build_batch_.reset();
        spilled_build_row_ = 0;
      }
      if (!spilled_build_batch_.has_value()) {
        ResultBatch batch;
        const Status next = build_spill_->Next(0, &batch);
        if (!next.ok()) return next;
        spilled_build_batch_ = std::move(batch);
        spilled_build_row_ = 0;
      }
      return spilled_build_row_++;
    }
  }

  Status NextSpilled(ResultBatch* output) {
    auto output_lease = std::make_shared<RuntimeMemoryLease>(memory_account_, 0);
    uint64_t output_bytes = 0;
    std::vector<std::vector<ResultValueCell>> values;
    const Status prepared = PrepareOutputAccumulator(
        output_lease, &output_bytes, &values);
    if (!prepared.ok()) return terminal_ = prepared;
    uint32_t output_rows = 0;
    while (output_rows < capacity_) {
      const Status cancelled = CheckCancelled(
          "query cancelled while replaying cross join spill");
      if (!cancelled.ok()) return terminal_ = cancelled;
      if (!have_probe_row_) {
        const Status loaded = LoadProbeRow();
        if (loaded.IsNotFound()) break;
        if (!loaded.ok()) return terminal_ = loaded;
      }
      if (!spill_rewound_for_probe_) {
        const Status rewound = build_spill_->Rewind(0);
        if (!rewound.ok()) return terminal_ = rewound;
        spilled_build_batch_.reset();
        spilled_build_row_ = 0;
        spill_rewound_for_probe_ = true;
        while (spilled_build_rows_to_skip_ != 0) {
          auto skipped = NextSpilledBuildRow();
          if (!skipped.ok()) {
            return terminal_ = skipped.status().IsNotFound()
                ? Status::Corruption(
                      "physical cross join", "spill resume prefix is absent")
                : skipped.status();
          }
          --spilled_build_rows_to_skip_;
        }
      }
      auto build = NextSpilledBuildRow();
      if (build.status().IsNotFound()) {
        spill_rewound_for_probe_ = false;
        have_probe_row_ = false;
        ++probe_row_;
        continue;
      }
      if (!build.ok()) return terminal_ = build.status();
      const RowView probe{nullptr, &*probe_batch_, probe_row_};
      const RowView build_view{
          nullptr, &*spilled_build_batch_, build.ValueOrDie()};
      const Status reserved = ReserveOutputPayload(
          probe, build_view, output_lease, &output_bytes);
      if (!reserved.ok()) return terminal_ = reserved;
      const Status appended = AppendOutputRow(probe, build_view, &values);
      if (!appended.ok()) return terminal_ = appended;
      ++output_rows;
    }
    if (output_rows == 0) {
      return Status::NotFound("physical cross join", "end of stream");
    }
    const Status finished = FinishOutput(
        std::move(values), output_rows, output_lease, output_bytes, output);
    if (!finished.ok()) return terminal_ = finished;
    return Status::OK();
  }

  Status Initialize() {
    initialized_ = true;
    for (;;) {
      const Status cancelled = CheckCancelled(
          "query cancelled while building cross join replay side");
      if (!cancelled.ok()) return cancelled;
      ResultBatch input;
      const Status next = build_input_->Next(&input);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(build_input_.get());
        if (!terminal.ok()) return terminal;
        return spilling_ ? build_spill_->Seal() : Status::OK();
      }
      if (!next.ok()) return next;
      if (build_names_.empty()) build_names_ = input.column_names();
      if (input.batch().column_count() != build_types_.size() ||
          build_names_.size() != build_types_.size()) {
        return Status::Corruption(
            "physical cross join", "replay input layout is invalid");
      }
      if (stats_) {
        stats_->cross_join_replay_input_rows += input.batch().row_count();
        if (stats_->operator_runtime) {
          stats_->operator_runtime->RecordCrossJoinInput(
              OperatorRuntimeKey{plan_id_, operator_id_}, true,
              input.batch().row_count());
        }
      }
      for (uint32_t row = 0; row < input.batch().row_count(); ++row) {
        const Status row_cancelled = CheckCancelled(
            "query cancelled while building cross join replay row");
        if (!row_cancelled.ok()) return row_cancelled;
        const Status added = AddBuildRow(input, row);
        if (!added.ok()) return added;
      }
    }
  }

  Status LoadProbeRow() {
    while (!probe_batch_.has_value() ||
           probe_row_ >= probe_batch_->batch().row_count()) {
      ResultBatch input;
      const Status next = probe_input_->Next(&input);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(probe_input_.get());
        return terminal.ok()
            ? Status::NotFound("physical cross join", "end of stream")
            : terminal;
      }
      if (!next.ok()) return next;
      probe_batch_ = std::move(input);
      probe_row_ = 0;
      if (stats_) {
        stats_->cross_join_stream_input_rows +=
            probe_batch_->batch().row_count();
        if (stats_->operator_runtime) {
          stats_->operator_runtime->RecordCrossJoinInput(
              OperatorRuntimeKey{plan_id_, operator_id_}, false,
              probe_batch_->batch().row_count());
        }
      }
    }
    have_probe_row_ = true;
    return Status::OK();
  }

  void ObserveOperatorMemory(uint64_t bytes) {
    if (stats_ && stats_->operator_runtime) {
      stats_->operator_runtime->ObserveMemory(
          OperatorRuntimeKey{plan_id_, operator_id_}, bytes);
    }
  }

  std::unique_ptr<QueryResultStream> build_input_;
  std::unique_ptr<QueryResultStream> probe_input_;
  std::vector<RetainedRow> build_rows_;
  uint64_t retained_rows_reserved_bytes_ = 0;
  std::vector<std::string> names_;
  std::vector<PhysicalHashJoinPlan::Output> outputs_;
  std::vector<PhysicalType> build_types_;
  uint32_t capacity_ = 0;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  std::shared_ptr<TcypherExecutionStats> stats_;
  uint64_t plan_id_ = 0;
  uint32_t operator_id_ = 0;
  bool probe_is_left_ = true;
  uint64_t reserved_bytes_ = 0;
  std::vector<std::string> build_names_;
  std::unique_ptr<PartitionedSpillSet> build_spill_;
  std::optional<ResultBatch> spilled_build_batch_;
  uint32_t spilled_build_row_ = 0;
  size_t spilled_build_rows_to_skip_ = 0;
  bool spill_rewound_for_probe_ = false;
  bool spilling_ = false;
  std::optional<ResultBatch> probe_batch_;
  uint32_t probe_row_ = 0;
  size_t build_index_ = 0;
  bool initialized_ = false;
  bool have_probe_row_ = false;
  Status terminal_ = Status::OK();
};

struct SampledResultBatch {
  ResultBatch batch;
  std::shared_ptr<QueryMemoryLease> metadata_lease;
};

class PrefixReplayResultStream final : public QueryResultStream {
 public:
  PrefixReplayResultStream(
      std::deque<SampledResultBatch> prefix,
      std::unique_ptr<QueryResultStream> input)
      : prefix_(std::move(prefix)), input_(std::move(input)) {}

  Status Next(ResultBatch* output) override {
    if (output == nullptr) {
      return Status::InvalidArgument(
          "pipeline reoptimization", "missing replay output");
    }
    if (!prefix_.empty()) {
      SampledResultBatch sampled = std::move(prefix_.front());
      prefix_.pop_front();
      *output = std::move(sampled.batch);
      return Status::OK();
    }
    return input_->Next(output);
  }

  Status terminal_status() const override {
    return input_ ? input_->terminal_status() : Status::OK();
  }

  QueryOperatorResourceStats operator_resource_stats() const override {
    return input_ ? input_->operator_resource_stats()
                  : QueryOperatorResourceStats{};
  }

 private:
  std::deque<SampledResultBatch> prefix_;
  std::unique_ptr<QueryResultStream> input_;
};

class AdaptivePhysicalHashJoinResultStream final : public QueryResultStream {
 public:
  AdaptivePhysicalHashJoinResultStream(
      std::unique_ptr<QueryResultStream> left_input,
      std::unique_ptr<QueryResultStream> right_input,
      std::vector<std::string> names,
      std::vector<PhysicalHashJoinPlan::Output> outputs,
      std::vector<uint32_t> left_key_columns,
      std::vector<uint32_t> right_key_columns,
      PhysicalHashJoinBuildSide planned_build_side, uint32_t capacity,
      std::shared_ptr<QueryCancellation> cancellation,
      std::shared_ptr<QueryMemoryAccount> memory_account,
      std::string spill_directory,
      std::shared_ptr<ResourceGovernorExtension> spill_resources,
      std::shared_ptr<TcypherExecutionStats> stats, uint64_t plan_id,
      uint32_t operator_id)
      : left_input_(std::move(left_input)),
        right_input_(std::move(right_input)),
        names_(std::move(names)),
        outputs_(std::move(outputs)),
        left_key_columns_(std::move(left_key_columns)),
        right_key_columns_(std::move(right_key_columns)),
        planned_build_side_(planned_build_side),
        capacity_(capacity),
        cancellation_(std::move(cancellation)),
        memory_account_(std::move(memory_account)),
        spill_directory_(std::move(spill_directory)),
        spill_resources_(std::move(spill_resources)),
        stats_(std::move(stats)),
        plan_id_(plan_id),
        operator_id_(operator_id) {}

  Status Next(ResultBatch* output) override {
    if (output == nullptr) {
      return Status::InvalidArgument(
          "pipeline reoptimization", "missing join output");
    }
    if (!delegate_) {
      const Status initialized = Initialize();
      if (!initialized.ok()) return terminal_ = initialized;
    }
    const Status next = delegate_->Next(output);
    if (!next.ok() && !next.IsNotFound()) terminal_ = next;
    return next;
  }

  Status terminal_status() const override {
    if (!terminal_.ok()) return terminal_;
    if (delegate_) return delegate_->terminal_status();
    if (left_input_ && !left_input_->terminal_status().ok()) {
      return left_input_->terminal_status();
    }
    return right_input_ ? right_input_->terminal_status() : Status::OK();
  }

  QueryOperatorResourceStats operator_resource_stats() const override {
    return delegate_ ? delegate_->operator_resource_stats()
                     : QueryOperatorResourceStats{};
  }

 private:
  static constexpr uint32_t kSampleBatchLimit = 2;

  struct PrefixSample {
    std::deque<SampledResultBatch> batches;
    uint64_t rows = 0;
    uint64_t batch_count = 0;
    bool exhausted = false;
  };

  Status Sample(std::unique_ptr<QueryResultStream>* input,
                PrefixSample* sample) {
    if (input == nullptr || !*input || sample == nullptr) {
      return Status::InvalidArgument(
          "pipeline reoptimization", "missing sample input");
    }
    for (uint32_t batch_index = 0;
         batch_index < kSampleBatchLimit; ++batch_index) {
      if (cancellation_ && cancellation_->IsCancelled()) {
        return Status::QueryCancelled(
            "pipeline reoptimization", "query cancelled while sampling");
      }
      ResultBatch batch;
      const Status next = (*input)->Next(&batch);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(input->get());
        if (!terminal.ok()) return terminal;
        sample->exhausted = true;
        return Status::OK();
      }
      if (next.IsQueryMemoryLimit()) {
        return Status::QueryMemoryLimit(
            "pipeline reoptimization", "input sample exceeds memory limit");
      }
      if (!next.ok()) return next;
      constexpr uint64_t kPrefixBatchMetadataBytes =
          sizeof(SampledResultBatch) + 128;
      auto metadata_lease =
          std::make_shared<QueryMemoryLease>(memory_account_, 0);
      const Status reserved = metadata_lease->ReserveAdditional(
          kPrefixBatchMetadataBytes);
      if (!reserved.ok()) {
        return Status::QueryMemoryLimit(
            "pipeline reoptimization", "prefix metadata exceeds memory limit");
      }
      prefix_memory_bytes_ += kPrefixBatchMetadataBytes;
      if (stats_) {
        stats_->pipeline_reoptimization_prefix_memory_bytes = std::max(
            stats_->pipeline_reoptimization_prefix_memory_bytes,
            prefix_memory_bytes_);
      }
      sample->rows += batch.batch().row_count();
      ++sample->batch_count;
      sample->batches.push_back(
          SampledResultBatch{std::move(batch), std::move(metadata_lease)});
    }
    return Status::OK();
  }

  Status Initialize() {
    PrefixSample left;
    PrefixSample right;
    Status sampled = Sample(&left_input_, &left);
    if (!sampled.ok()) return sampled;
    sampled = Sample(&right_input_, &right);
    if (!sampled.ok()) return sampled;
    if (stats_) {
      ++stats_->pipeline_reoptimization_checks;
      stats_->pipeline_reoptimization_sampled_rows += left.rows + right.rows;
      stats_->pipeline_reoptimization_sampled_batches +=
          left.batch_count + right.batch_count;
    }

    bool build_left =
        planned_build_side_ == PhysicalHashJoinBuildSide::kLeft;
    if (left.exhausted && right.exhausted) {
      if (left.rows < right.rows) {
        build_left = true;
      } else if (right.rows < left.rows) {
        build_left = false;
      }
    } else if (left.exhausted && left.rows <= right.rows) {
      build_left = true;
    } else if (right.exhausted && right.rows <= left.rows) {
      build_left = false;
    }
    const bool planned_left =
        planned_build_side_ == PhysicalHashJoinBuildSide::kLeft;
    if (stats_) {
      stats_->hash_join_build_side_left = build_left ? 1 : 0;
      if (build_left != planned_left) {
        ++stats_->pipeline_reoptimizations;
        ++stats_->hash_join_build_side_switches;
      }
    }

    std::unique_ptr<QueryResultStream> left_replay =
        std::make_unique<PrefixReplayResultStream>(
            std::move(left.batches), std::move(left_input_));
    std::unique_ptr<QueryResultStream> right_replay =
        std::make_unique<PrefixReplayResultStream>(
            std::move(right.batches), std::move(right_input_));
    std::unique_ptr<QueryResultStream> build;
    std::unique_ptr<QueryResultStream> probe;
    std::vector<uint32_t> build_keys;
    std::vector<uint32_t> probe_keys;
    if (build_left) {
      build = std::move(left_replay);
      probe = std::move(right_replay);
      build_keys = left_key_columns_;
      probe_keys = right_key_columns_;
    } else {
      build = std::move(right_replay);
      probe = std::move(left_replay);
      build_keys = right_key_columns_;
      probe_keys = left_key_columns_;
    }
    delegate_ = std::make_unique<PhysicalHashJoinResultStream>(
        std::move(build), std::move(probe), std::move(names_),
        std::move(outputs_), std::move(build_keys), std::move(probe_keys),
        capacity_, cancellation_, memory_account_, spill_directory_,
        spill_resources_, stats_, plan_id_, operator_id_, !build_left);
    return Status::OK();
  }

  std::unique_ptr<QueryResultStream> left_input_;
  std::unique_ptr<QueryResultStream> right_input_;
  std::vector<std::string> names_;
  std::vector<PhysicalHashJoinPlan::Output> outputs_;
  std::vector<uint32_t> left_key_columns_;
  std::vector<uint32_t> right_key_columns_;
  PhysicalHashJoinBuildSide planned_build_side_ =
      PhysicalHashJoinBuildSide::kRight;
  uint32_t capacity_ = 0;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  std::shared_ptr<TcypherExecutionStats> stats_;
  uint64_t plan_id_ = 0;
  uint32_t operator_id_ = 0;
  std::unique_ptr<QueryResultStream> delegate_;
  uint64_t prefix_memory_bytes_ = 0;
  Status terminal_ = Status::OK();
};

class AdaptivePhysicalCrossJoinResultStream final : public QueryResultStream {
 public:
  AdaptivePhysicalCrossJoinResultStream(
      std::unique_ptr<QueryResultStream> left_input,
      std::unique_ptr<QueryResultStream> right_input,
      std::vector<std::string> names,
      std::vector<PhysicalHashJoinPlan::Output> outputs,
      std::vector<PhysicalType> left_types,
      std::vector<PhysicalType> right_types,
      PhysicalHashJoinBuildSide planned_build_side, uint32_t capacity,
      std::shared_ptr<QueryCancellation> cancellation,
      std::shared_ptr<QueryMemoryAccount> memory_account,
      std::string spill_directory,
      std::shared_ptr<ResourceGovernorExtension> spill_resources,
      std::shared_ptr<TcypherExecutionStats> stats, uint64_t plan_id,
      uint32_t operator_id)
      : left_input_(std::move(left_input)),
        right_input_(std::move(right_input)), names_(std::move(names)),
        outputs_(std::move(outputs)), left_types_(std::move(left_types)),
        right_types_(std::move(right_types)),
        planned_build_side_(planned_build_side),
        capacity_(capacity), cancellation_(std::move(cancellation)),
        memory_account_(std::move(memory_account)),
        spill_directory_(std::move(spill_directory)),
        spill_resources_(std::move(spill_resources)), stats_(std::move(stats)),
        plan_id_(plan_id), operator_id_(operator_id) {}

  Status Next(ResultBatch* output) override {
    if (output == nullptr) {
      return Status::InvalidArgument(
          "pipeline reoptimization", "missing cross join output");
    }
    if (!delegate_) {
      const Status initialized = Initialize();
      if (!initialized.ok()) return terminal_ = initialized;
    }
    const Status next = delegate_->Next(output);
    if (!next.ok() && !next.IsNotFound()) terminal_ = next;
    return next;
  }

  Status terminal_status() const override {
    if (!terminal_.ok()) return terminal_;
    if (delegate_) return delegate_->terminal_status();
    if (left_input_ && !left_input_->terminal_status().ok()) {
      return left_input_->terminal_status();
    }
    return right_input_ ? right_input_->terminal_status() : Status::OK();
  }

  QueryOperatorResourceStats operator_resource_stats() const override {
    return delegate_ ? delegate_->operator_resource_stats()
                     : QueryOperatorResourceStats{};
  }

 private:
  static constexpr uint32_t kSampleBatchLimit = 2;

  struct PrefixSample {
    std::deque<SampledResultBatch> batches;
    uint64_t rows = 0;
    uint64_t batch_count = 0;
    bool exhausted = false;
  };

  Status Sample(std::unique_ptr<QueryResultStream>* input,
                PrefixSample* sample) {
    if (input == nullptr || !*input || sample == nullptr) {
      return Status::InvalidArgument(
          "pipeline reoptimization", "missing cross join sample input");
    }
    for (uint32_t batch_index = 0;
         batch_index < kSampleBatchLimit; ++batch_index) {
      if (cancellation_ && cancellation_->IsCancelled()) {
        return Status::QueryCancelled(
            "pipeline reoptimization", "query cancelled while sampling cross join");
      }
      ResultBatch batch;
      const Status next = (*input)->Next(&batch);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(input->get());
        if (!terminal.ok()) return terminal;
        sample->exhausted = true;
        return Status::OK();
      }
      if (next.IsQueryMemoryLimit()) {
        return Status::QueryMemoryLimit(
            "pipeline reoptimization", "cross join sample exceeds memory limit");
      }
      if (!next.ok()) return next;
      constexpr uint64_t kPrefixBatchMetadataBytes =
          sizeof(SampledResultBatch) + 128;
      auto metadata_lease =
          std::make_shared<QueryMemoryLease>(memory_account_, 0);
      const Status reserved = metadata_lease->ReserveAdditional(
          kPrefixBatchMetadataBytes);
      if (!reserved.ok()) return reserved;
      prefix_memory_bytes_ += kPrefixBatchMetadataBytes;
      if (stats_) {
        stats_->pipeline_reoptimization_prefix_memory_bytes = std::max(
            stats_->pipeline_reoptimization_prefix_memory_bytes,
            prefix_memory_bytes_);
      }
      sample->rows += batch.batch().row_count();
      ++sample->batch_count;
      sample->batches.push_back(
          SampledResultBatch{std::move(batch), std::move(metadata_lease)});
    }
    return Status::OK();
  }

  Status Initialize() {
    PrefixSample left;
    PrefixSample right;
    Status sampled = Sample(&left_input_, &left);
    if (!sampled.ok()) return sampled;
    sampled = Sample(&right_input_, &right);
    if (!sampled.ok()) return sampled;
    if (stats_) {
      ++stats_->pipeline_reoptimization_checks;
      stats_->pipeline_reoptimization_sampled_rows += left.rows + right.rows;
      stats_->pipeline_reoptimization_sampled_batches +=
          left.batch_count + right.batch_count;
    }

    bool build_left =
        planned_build_side_ == PhysicalHashJoinBuildSide::kLeft;
    if (left.exhausted && right.exhausted) {
      if (left.rows < right.rows) {
        build_left = true;
      } else if (right.rows < left.rows) {
        build_left = false;
      }
    } else if (left.exhausted && left.rows <= right.rows) {
      build_left = true;
    } else if (right.exhausted && right.rows <= left.rows) {
      build_left = false;
    }
    const bool planned_left =
        planned_build_side_ == PhysicalHashJoinBuildSide::kLeft;
    if (stats_ && build_left != planned_left) {
      ++stats_->pipeline_reoptimizations;
      ++stats_->cross_join_build_side_switches;
    }

    std::unique_ptr<QueryResultStream> left_replay =
        std::make_unique<PrefixReplayResultStream>(
            std::move(left.batches), std::move(left_input_));
    std::unique_ptr<QueryResultStream> right_replay =
        std::make_unique<PrefixReplayResultStream>(
            std::move(right.batches), std::move(right_input_));
    std::unique_ptr<QueryResultStream> build;
    std::unique_ptr<QueryResultStream> probe;
    std::vector<PhysicalType> build_types;
    if (build_left) {
      build = std::move(left_replay);
      probe = std::move(right_replay);
      build_types = left_types_;
    } else {
      build = std::move(right_replay);
      probe = std::move(left_replay);
      build_types = right_types_;
    }
    delegate_ = std::make_unique<PhysicalCrossJoinResultStream>(
        std::move(build), std::move(probe), std::move(names_),
        std::move(outputs_), std::move(build_types), capacity_, cancellation_,
        memory_account_, spill_directory_, spill_resources_, stats_, plan_id_,
        operator_id_, !build_left);
    return Status::OK();
  }

  std::unique_ptr<QueryResultStream> left_input_;
  std::unique_ptr<QueryResultStream> right_input_;
  std::vector<std::string> names_;
  std::vector<PhysicalHashJoinPlan::Output> outputs_;
  std::vector<PhysicalType> left_types_;
  std::vector<PhysicalType> right_types_;
  PhysicalHashJoinBuildSide planned_build_side_ =
      PhysicalHashJoinBuildSide::kRight;
  uint32_t capacity_ = 0;
  std::shared_ptr<QueryCancellation> cancellation_;
  std::shared_ptr<QueryMemoryAccount> memory_account_;
  std::string spill_directory_;
  std::shared_ptr<ResourceGovernorExtension> spill_resources_;
  std::shared_ptr<TcypherExecutionStats> stats_;
  uint64_t plan_id_ = 0;
  uint32_t operator_id_ = 0;
  std::unique_ptr<QueryResultStream> delegate_;
  uint64_t prefix_memory_bytes_ = 0;
  Status terminal_ = Status::OK();
};

}  // namespace

std::unique_ptr<QueryResultStream> OpenPhysicalCrossJoinForTesting(
    std::unique_ptr<QueryResultStream> build_input,
    std::unique_ptr<QueryResultStream> probe_input,
    std::vector<PhysicalHashJoinPlan::Output> outputs,
    std::vector<PhysicalType> build_types, uint32_t capacity,
    std::shared_ptr<QueryMemoryAccount> memory_account,
    std::string spill_directory,
    std::shared_ptr<TcypherExecutionStats> stats, bool probe_is_left) {
  std::vector<std::string> names;
  names.reserve(outputs.size());
  for (const PhysicalHashJoinPlan::Output& output : outputs) {
    names.push_back(output.name);
  }
  return std::make_unique<PhysicalCrossJoinResultStream>(
      std::move(build_input), std::move(probe_input), std::move(names),
      std::move(outputs), std::move(build_types), capacity, nullptr,
      std::move(memory_account), std::move(spill_directory), nullptr,
      std::move(stats), 1, 1, probe_is_left);
}

uint64_t PhysicalCrossJoinRetainedCapacityForTesting(
    QueryResultStream* stream) {
  auto* cross_join = dynamic_cast<PhysicalCrossJoinResultStream*>(stream);
  return cross_join == nullptr ? 0 : cross_join->retained_capacity_for_testing();
}

StatusOr<QuerySnapshot> BuildPhysicalQuerySnapshot(
    const BoundTcypherStatement& statement,
    const TcypherExecutionContext& context) {
  uint64_t valid_time = context.options.statement_start_valid_time;
  const uint64_t visible_prefix = context.pinned_visible_seq_ceiling.value_or(
      context.visible_seq_ceiling);
  uint64_t snapshot_seq = visible_prefix;
  bool has_system_cutoff = false;
  const auto apply = [&](const BoundTemporalScope& scope) {
    if (scope.axis == TemporalAxis::kValidTime) valid_time = scope.valid_time_start;
    else {
      snapshot_seq = scope.snapshot_seq;
      has_system_cutoff = true;
    }
  };
  for (const BoundTemporalScope& scope : statement.temporal_scopes) apply(scope);
  for (const BoundTemporalScope& scope : statement.primary_match_scopes) apply(scope);
  if (snapshot_seq > visible_prefix) {
    return Status::InvalidArgument("query snapshot", "temporal cutoff exceeds visible prefix");
  }
  std::shared_ptr<const VersionSnapshot> version = context.version_snapshot;
  if (!version) {
    version = std::make_shared<const VersionSnapshot>(VersionSnapshot{0, {}});
  }
  std::vector<ResolvedTemporalContext> temporal_contexts{
      ResolvedTemporalContext{TemporalAxis::kValidTime, snapshot_seq, valid_time}};
  if (has_system_cutoff) {
    temporal_contexts.push_back(
        ResolvedTemporalContext{TemporalAxis::kSystemTime, snapshot_seq, valid_time});
  }
  return CreateQuerySnapshot(
      visible_prefix, std::move(version), context.schema_snapshot,
      context.blob_reader_epoch, context.statement_start_hlc,
      std::move(temporal_contexts));
}

StatusOr<std::unique_ptr<QueryResultStream>> ApplyPhysicalAggregateSink(
    std::unique_ptr<QueryResultStream> stream,
    const PhysicalAggregateSinkSpec& sink, uint32_t capacity,
    const std::shared_ptr<QueryCancellation>& cancellation,
    const std::shared_ptr<QueryMemoryAccount>& memory_account,
    const std::string& spill_directory,
    std::shared_ptr<ResourceGovernorExtension> spill_resources) {
  if (!stream || sink.aggregates.empty()) {
    return Status::InvalidArgument(
        "physical aggregate", "aggregate input or specification is missing");
  }
  if (sink.aggregates.size() == 1 &&
      sink.aggregates.front().kind == PhysicalAggregateKind::kCollect) {
    const PhysicalAggregateExpression& collect = sink.aggregates.front();
    const ResultValueKind collect_kind =
        collect.input_kind == PhysicalAggregateValueKind::kStruct
            ? ResultValueKind::kStruct
            : collect.input_kind == PhysicalAggregateValueKind::kList
                ? ResultValueKind::kList : ResultValueKind::kScalar;
    if (sink.group_columns.empty()) {
      stream = std::make_unique<CollectResultStream>(
          std::move(stream), collect.output_name, cancellation, memory_account,
          collect_kind);
    } else {
      std::vector<ResultOutputSlot> outputs;
      outputs.reserve(sink.outputs.size());
      for (const PhysicalAggregateOutput& output : sink.outputs) {
        outputs.push_back(ResultOutputSlot{output.aggregate, output.index});
      }
      stream = std::make_unique<GroupedCollectResultStream>(
          std::move(stream), sink.group_columns, collect.input_column,
          collect.output_name, std::move(outputs), capacity,
          cancellation, memory_account, spill_directory, spill_resources,
          collect_kind);
    }
    return stream;
  }
  std::vector<ResultAggregateSpec> aggregates;
  aggregates.reserve(sink.aggregates.size());
  for (const PhysicalAggregateExpression& aggregate : sink.aggregates) {
    ResultAggregateKind kind;
    switch (aggregate.kind) {
      case PhysicalAggregateKind::kCount: kind = ResultAggregateKind::kCount; break;
      case PhysicalAggregateKind::kSum: kind = ResultAggregateKind::kSum; break;
      case PhysicalAggregateKind::kAvg: kind = ResultAggregateKind::kAvg; break;
      case PhysicalAggregateKind::kMin: kind = ResultAggregateKind::kMin; break;
      case PhysicalAggregateKind::kMax: kind = ResultAggregateKind::kMax; break;
      case PhysicalAggregateKind::kCollect:
        return Status::InvalidArgument(
            "physical aggregate", "COLLECT cannot be mixed with other aggregates");
    }
    aggregates.push_back(ResultAggregateSpec{
        kind, aggregate.input_column, aggregate.output_name});
  }
  if (sink.group_columns.empty()) {
    if (aggregates.size() == 1) {
      const ResultAggregateSpec aggregate = aggregates.front();
      switch (aggregate.kind) {
        case ResultAggregateKind::kCount:
          stream = std::make_unique<CountResultStream>(
              std::move(stream), aggregate.column_name);
          break;
        case ResultAggregateKind::kSum:
          stream = std::make_unique<SumResultStream>(
              std::move(stream), aggregate.column_name);
          break;
        case ResultAggregateKind::kAvg:
          stream = std::make_unique<AvgResultStream>(
              std::move(stream), aggregate.column_name);
          break;
        case ResultAggregateKind::kMin:
        case ResultAggregateKind::kMax:
          stream = std::make_unique<ExtremaResultStream>(
              std::move(stream), aggregate.column_name,
              aggregate.kind == ResultAggregateKind::kMin);
          break;
      }
    } else {
      stream = std::make_unique<MultiAggregateResultStream>(
          std::move(stream), std::move(aggregates));
    }
  } else {
    std::vector<ResultOutputSlot> outputs;
    outputs.reserve(sink.outputs.size());
    for (const PhysicalAggregateOutput& output : sink.outputs) {
      outputs.push_back(ResultOutputSlot{output.aggregate, output.index});
    }
    stream = std::make_unique<GroupedMultiAggregateResultStream>(
        std::move(stream), sink.group_columns, std::move(aggregates),
        std::move(outputs), capacity, cancellation, memory_account,
        spill_directory, spill_resources);
  }
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ApplyPhysicalBlockingSinks(
    std::unique_ptr<QueryResultStream> stream,
    const std::vector<PhysicalOperatorSpec>& operators,
    const std::optional<PhysicalAggregateSinkSpec>& aggregate_sink,
    const std::optional<PhysicalSortSinkSpec>& sort_sink,
    uint32_t capacity,
    const std::shared_ptr<QueryCancellation>& cancellation,
    const std::shared_ptr<QueryMemoryAccount>& memory_account,
    const std::string& spill_directory,
    std::shared_ptr<ResourceGovernorExtension> spill_resources,
    const std::string& runtime_name,
    const std::shared_ptr<TcypherExecutionStats>& stats,
    uint64_t source_plan_id) {
  for (const PhysicalOperatorSpec& op : operators) {
    const OperatorRuntimeKey key{source_plan_id, op.id.value};
    auto timing = std::make_shared<OperatorRuntimeTimingState>();
    if (stats && stats->operator_runtime) {
      stream = std::make_unique<OperatorRuntimeCountingResultStream>(
          std::move(stream), stats->operator_runtime, key,
          OperatorRuntimeCountingResultStream::Phase::kInput, timing);
    }
    if (op.kind == PhysicalOperatorKind::kAggregate) {
      if (!aggregate_sink.has_value() || aggregate_sink->op.id != op.id) {
        return Status::InvalidArgument(
            runtime_name, "aggregate sink specification is missing");
      }
      auto aggregated = ApplyPhysicalAggregateSink(
          std::move(stream), *aggregate_sink, capacity, cancellation,
          memory_account, spill_directory, spill_resources);
      if (!aggregated.ok()) return aggregated.status();
      stream = std::move(aggregated).ConsumeValueOrDie();
    } else if (op.kind == PhysicalOperatorKind::kDistinct) {
      stream = std::make_unique<DistinctResultStream>(
          std::move(stream), cancellation, memory_account, spill_directory,
          spill_resources);
    } else if (op.kind == PhysicalOperatorKind::kSort) {
      if (!sort_sink.has_value() || sort_sink->op.id != op.id) {
        return Status::InvalidArgument(
            runtime_name, "sort sink specification is missing");
      }
      stream = std::make_unique<SortResultStream>(
          std::move(stream), sort_sink->input_column,
          sort_sink->descending, capacity, cancellation, memory_account,
          spill_directory, spill_resources);
    } else {
      return Status::InvalidArgument(
          runtime_name, "unsupported post-result operator");
    }
    if (stats && stats->operator_runtime) {
      stream = std::make_unique<OperatorRuntimeCountingResultStream>(
          std::move(stream), stats->operator_runtime, key,
          OperatorRuntimeCountingResultStream::Phase::kOutput, timing);
    }
  }
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalRootPointRuntime(
    std::shared_ptr<const PhysicalPlan> plan, QuerySnapshot snapshot,
    TcypherExecutionContext context) {
  if (!plan) {
    return Status::InvalidArgument("physical runtime", "missing physical plan");
  }
  const Status valid_plan = ValidatePhysicalPlan(*plan);
  if (!valid_plan.ok()) return valid_plan;
  if (context.options.execution_stats && context.root_access_path.has_value()) {
    context.options.execution_stats->has_executed_access_path = true;
    context.options.execution_stats->executed_access_path =
        *context.root_access_path;
  }
  const uint32_t capacity = context.options.batch_capacity;
  const auto cancellation = context.options.cancellation;
  const auto memory_account = context.options.memory_account;
  const auto stats = context.options.execution_stats;
  const std::string spill_directory = context.options.spill_directory;
  const auto spill_resources = context.options.spill_resource_extensions;
  auto sources = BuildPinnedSources(context, snapshot);
  if (!sources.ok()) return sources.status();
  std::shared_ptr<WorkScheduler> inline_scheduler;
  std::shared_ptr<WorkExecutionService> inline_execution_service;
  if (!context.work_execution_service) {
    inline_scheduler = std::make_shared<WorkScheduler>();
    inline_execution_service =
        std::make_shared<WorkExecutionService>(inline_scheduler, 1);
    const Status started = inline_execution_service->Start();
    if (!started.ok()) return started;
  }
  std::shared_ptr<WorkExecutionService> execution_service =
      context.work_execution_service
          ? std::move(context.work_execution_service)
          : std::move(inline_execution_service);
  auto state = std::make_shared<QueryRuntimeState>(
      plan, std::move(snapshot), std::move(context),
      std::move(sources).ConsumeValueOrDie(),
      execution_service.get());
  const Status opened = state->Open();
  if (!opened.ok()) return opened;
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<ScheduledQueryResultStream>(
          std::move(state), std::move(execution_service));
  return ApplyPhysicalBlockingSinks(
      std::move(stream), plan->post_result_operators(),
      plan->aggregate_sink(), plan->sort_sink(), capacity, cancellation,
      memory_account, spill_directory, spill_resources, "physical runtime",
      stats, plan->plan_id());
}

StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalHashJoinRuntime(
    std::shared_ptr<const PhysicalHashJoinPlan> plan, QuerySnapshot snapshot,
    TcypherExecutionContext context) {
  if (!plan || context.options.batch_capacity == 0) {
    return Status::InvalidArgument("physical hash join", "invalid join plan");
  }
  const Status valid = ValidatePhysicalHashJoinPlan(*plan);
  if (!valid.ok()) return valid;
  const uint32_t capacity = context.options.batch_capacity;
  const auto cancellation = context.options.cancellation;
  const auto memory_account = context.options.memory_account;
  const std::string spill_directory = context.options.spill_directory;
  const auto spill_resources = context.options.spill_resource_extensions;
  const auto stats = context.options.execution_stats;
  if (stats) {
    ++stats->pipeline_builds;
    stats->last_physical_plan_id = plan->plan_id;
    stats->hash_join_build_side_left =
        plan->build_side == PhysicalHashJoinBuildSide::kLeft ? 1 : 0;
  }
  auto left_stream = OpenPhysicalRootPointRuntime(plan->left, snapshot, context);
  if (!left_stream.ok()) return left_stream.status();
  auto right_stream = OpenPhysicalRootPointRuntime(
      plan->right, std::move(snapshot), std::move(context));
  if (!right_stream.ok()) return right_stream.status();
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<AdaptivePhysicalHashJoinResultStream>(
          std::move(left_stream).ConsumeValueOrDie(),
          std::move(right_stream).ConsumeValueOrDie(), plan->output_names,
          plan->outputs, plan->left_key_columns, plan->right_key_columns,
          plan->build_side, capacity, cancellation, memory_account,
          spill_directory, spill_resources, stats, plan->plan_id,
          plan->join.id.value);
  if (stats) stats->last_physical_plan_id = plan->plan_id;
  auto applied = ApplyPhysicalBlockingSinks(
      std::move(stream), plan->post_join_operators, plan->aggregate_sink,
      plan->sort_sink, capacity, cancellation, memory_account,
      spill_directory, spill_resources, "physical hash join", stats,
      plan->plan_id);
  if (!applied.ok()) return applied.status();
  std::unique_ptr<QueryResultStream> identified =
      std::make_unique<PhysicalPlanIdentityResultStream>(
          std::move(applied).ConsumeValueOrDie(), stats, plan->plan_id);
  return identified;
}

StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalMultiHashJoinRuntime(
    std::shared_ptr<const PhysicalMultiHashJoinPlan> plan,
    QuerySnapshot snapshot, TcypherExecutionContext context) {
  const auto stats = context.options.execution_stats;
  PhysicalPlanOpenIdentityGuard identity_guard(
      stats, plan ? plan->plan_id : 0);
  if (!plan || context.options.batch_capacity == 0) {
    return Status::InvalidArgument(
        "physical multi hash join", "invalid join plan");
  }
  const Status valid = ValidatePhysicalMultiHashJoinPlan(*plan);
  if (!valid.ok()) return valid;
  if (stats) {
    stats->has_selected_graph_order = true;
    stats->selected_graph_order = plan->graph_order;
    stats->has_executed_graph_order = true;
    stats->executed_graph_order = plan->graph_order;
  }

  const uint32_t capacity = context.options.batch_capacity;
  const auto cancellation = context.options.cancellation;
  const auto memory_account = context.options.memory_account;
  const std::string spill_directory = context.options.spill_directory;
  const auto spill_resources = context.options.spill_resource_extensions;

  const auto snapshot_for_input = [&](uint32_t input_index)
      -> StatusOr<QuerySnapshot> {
    if (input_index >= plan->input_snapshot_seqs.size()) {
      return Status::Corruption(
          "physical multi hash join", "input snapshot index is invalid");
    }
    QuerySnapshot child = snapshot;
    const uint64_t snapshot_seq = plan->input_snapshot_seqs[input_index];
    if (snapshot_seq > child.visible_seq_ceiling) {
      return Status::InvalidArgument(
          "physical multi hash join", "input snapshot exceeds visible prefix");
    }
    for (ResolvedTemporalContext& temporal :
         child.resolved_temporal_contexts) {
      temporal.snapshot_seq = snapshot_seq;
    }
    return child;
  };

  auto accumulator_snapshot = snapshot_for_input(plan->join_order.front());
  if (!accumulator_snapshot.ok()) return accumulator_snapshot.status();
  auto opened_accumulator = OpenPhysicalRootPointRuntime(
      plan->inputs[plan->join_order.front()],
      std::move(accumulator_snapshot).ConsumeValueOrDie(), context);
  if (!opened_accumulator.ok()) return opened_accumulator.status();
  std::unique_ptr<QueryResultStream> accumulator =
      std::move(opened_accumulator).ConsumeValueOrDie();
  std::vector<PhysicalMultiJoinColumn> accumulator_layout =
      plan->input_layouts[plan->join_order.front()];

  for (const PhysicalMultiHashJoinStep& step : plan->steps) {
    auto input_snapshot = snapshot_for_input(step.input_index);
    if (!input_snapshot.ok()) return input_snapshot.status();
    auto opened_input = OpenPhysicalRootPointRuntime(
        plan->inputs[step.input_index],
        std::move(input_snapshot).ConsumeValueOrDie(), context);
    if (!opened_input.ok()) return opened_input.status();
    std::unique_ptr<QueryResultStream> input =
        std::move(opened_input).ConsumeValueOrDie();
    std::vector<std::string> names;
    names.reserve(step.output_layout.size());
    for (const PhysicalMultiJoinColumn& column : step.output_layout) {
      names.push_back(column.name);
    }
    if (stats) {
      stats->hash_join_build_side_left =
          step.build_side == PhysicalHashJoinBuildSide::kLeft ? 1 : 0;
    }
    if (step.join.kind == PhysicalOperatorKind::kHashJoin) {
      accumulator = std::make_unique<AdaptivePhysicalHashJoinResultStream>(
          std::move(accumulator), std::move(input), std::move(names),
          step.outputs, step.accumulated_key_columns, step.input_key_columns,
          step.build_side, capacity, cancellation, memory_account,
          spill_directory, spill_resources, stats, plan->plan_id,
          step.join.id.value);
    } else if (step.join.kind == PhysicalOperatorKind::kCrossJoin) {
      if (stats) ++stats->physical_cross_join_builds;
      std::vector<PhysicalType> accumulated_types;
      accumulated_types.reserve(accumulator_layout.size());
      for (const PhysicalMultiJoinColumn& column : accumulator_layout) {
        accumulated_types.push_back(column.type);
      }
      std::vector<PhysicalType> input_types;
      input_types.reserve(plan->input_layouts[step.input_index].size());
      for (const PhysicalMultiJoinColumn& column :
           plan->input_layouts[step.input_index]) {
        input_types.push_back(column.type);
      }
      accumulator = std::make_unique<AdaptivePhysicalCrossJoinResultStream>(
          std::move(accumulator), std::move(input), std::move(names),
          step.outputs, std::move(accumulated_types), std::move(input_types),
          step.build_side, capacity, cancellation, memory_account,
          spill_directory, spill_resources, stats, plan->plan_id,
          step.join.id.value);
    } else {
      return Status::Corruption(
          "physical multi join", "unknown physical join step");
    }
    accumulator_layout = step.output_layout;
    if (stats) ++stats->pipeline_builds;
  }
  if (stats) stats->last_physical_plan_id = plan->plan_id;

  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<ProjectColumnsResultStream>(
          std::move(accumulator), plan->final_output_columns,
          plan->output_names);
  auto applied = ApplyPhysicalBlockingSinks(
      std::move(stream), plan->post_join_operators, plan->aggregate_sink,
      plan->sort_sink, capacity, cancellation, memory_account,
      spill_directory, spill_resources, "physical multi hash join", stats,
      plan->plan_id);
  if (!applied.ok()) return applied.status();
  std::unique_ptr<QueryResultStream> identified =
      std::make_unique<PhysicalPlanIdentityResultStream>(
          std::move(applied).ConsumeValueOrDie(), stats, plan->plan_id);
  return identified;
}

}  // namespace cedar
