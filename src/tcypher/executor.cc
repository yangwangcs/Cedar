// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/executor.h"
#include "cedar/observability/explain_analyze_profile.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <limits>
#include <optional>
#include <set>
#include <utility>

#include "cedar/tcypher/binder.h"
#include "cedar/columnar/sst.h"
#include "cedar/index/index_sidecar.h"
#include "cedar/optimizer/cost_model.h"
#include "cedar/tcypher/logical_plan.h"
#include "cedar/tcypher/physical_plan.h"
#include "cedar/tcypher/runtime/interval_align.h"
#include "cedar/tcypher/runtime/interval_derive.h"
#include "cedar/tcypher/runtime/query_runtime.h"
#include "cedar/tcypher/runtime/query_spill.h"
#include "cedar/tcypher/runtime/temporal_coalesce.h"
#include "cedar/tcypher/runtime/transaction_sink.h"
#include "cedar/tcypher/runtime/vector_expand.h"
#include "cedar/tcypher/runtime/vector_pipeline.h"
#include "cedar/tcypher/session.h"
#include "cedar/tcypher/syntax/parser.h"
#include "cedar/tcypher/storage/temporal_scan.h"
#include "cedar/transaction/transaction_coordinator.h"

namespace cedar {
namespace {

std::optional<ColumnSchema> FindColumnByName(const SchemaSnapshot& snapshot,
                                              EntityType entity_type,
                                              const std::string& name) {
  for (const auto& entry : snapshot.schemas) {
    if (entry.first.first != static_cast<uint8_t>(entity_type)) continue;
    for (auto schema = entry.second.rbegin(); schema != entry.second.rend(); ++schema) {
      if (schema->logical_type == name) return *schema;
    }
  }
  return std::nullopt;
}

struct PhysicalJoinPlanningEstimates {
  uint64_t statistics_snapshot_id = 0;
  std::map<BindingId, PhysicalCardinalityEstimate> by_binding;
};

PhysicalJoinPlanningEstimates BuildPhysicalJoinPlanningEstimates(
    const BoundTcypherStatement& statement,
    const TcypherExecutionContext& context) {
  PhysicalJoinPlanningEstimates planning;
  if (context.statistics_snapshot) {
    planning.statistics_snapshot_id =
        context.statistics_snapshot->statistics_snapshot_id();
  }
  const auto find_pattern = [&](const std::string& variable)
      -> const MatchNodePattern* {
    if (statement.syntax.match.variable == variable) return &statement.syntax.match;
    for (const MatchClause& clause : statement.syntax.additional_matches) {
      if (clause.match.variable == variable) return &clause.match;
    }
    return nullptr;
  };
  const bool has_unpersisted_sources = !context.committed_events.empty() ||
      !context.memtable_event_sources.empty() ||
      !context.session_overlay_events.empty();
  for (const BoundVariable& variable : statement.variables) {
    PhysicalCardinalityEstimate result;
    if (context.index_catalog_snapshot && context.version_snapshot &&
        context.index_catalog_snapshot->catalog_generation ==
            context.version_snapshot->generation) {
      result.index_available = std::any_of(
          statement.predicates.begin(), statement.predicates.end(),
          [&](const BoundPredicateExpression& predicate) {
            if (predicate.binding_id != variable.binding_id) return false;
            return std::any_of(
                context.index_catalog_snapshot->definitions.begin(),
                context.index_catalog_snapshot->definitions.end(),
                [&](const IndexDefinition& definition) {
                  return definition.state == IndexState::kActive &&
                      definition.entity_type == predicate.column.entity_type &&
                      definition.column_id == predicate.column.column_id &&
                      definition.schema_epoch == predicate.column.schema_epoch &&
                      IsSupportedIndexCanonicalEncoding(
                          definition.canonical_encoding_id) &&
                      (definition.capabilities & kIndexEquality) != 0;
                });
          });
    }
    const MatchNodePattern* pattern = find_pattern(variable.variable);
    if (pattern != nullptr && pattern->entity_id.has_value()) {
      result.rows = 1;
      result.confidence_per_mille = 1000;
      result.conservative = false;
      planning.by_binding.emplace(variable.binding_id, result);
      continue;
    }
    if (!context.statistics_snapshot || !context.version_snapshot ||
        has_unpersisted_sources) {
      planning.by_binding.emplace(variable.binding_id, result);
      continue;
    }
    bool found_complete = false;
    bool compatible = true;
    uint64_t rows = std::numeric_limits<uint64_t>::max();
    for (const BoundJoinEquality& equality : statement.joins) {
      for (const BoundJoinInput* input : {&equality.left, &equality.right}) {
        if (input->variable != variable.variable || !input->column.has_value()) {
          continue;
        }
        const auto snapshot = context.statistics_snapshot->SnapshotFor(
            *context.version_snapshot, variable.entity_type,
            input->column->column_id);
        if (!snapshot.ok() || !snapshot.ValueOrDie().complete ||
            snapshot.ValueOrDie().conservative) {
          compatible = false;
          break;
        }
        found_complete = true;
        rows = std::min(rows, snapshot.ValueOrDie().aggregate.row_count);
      }
      if (!compatible) break;
    }
    if (compatible && found_complete) {
      result.rows = rows;
      result.confidence_per_mille = 700;
      result.conservative = false;
    }
    planning.by_binding.emplace(variable.binding_id, result);
  }
  return planning;
}

PhysicalHashJoinPlanningStats BuildPhysicalHashJoinPlanningStats(
    const BoundTcypherStatement& statement,
    const TcypherExecutionContext& context) {
  const PhysicalJoinPlanningEstimates estimates =
      BuildPhysicalJoinPlanningEstimates(statement, context);
  PhysicalHashJoinPlanningStats planning;
  planning.statistics_snapshot_id = estimates.statistics_snapshot_id;
  if (statement.joins.empty()) return planning;
  const auto binding_for = [&](const std::string& variable_name)
      -> std::optional<BindingId> {
    const auto found = std::find_if(
        statement.variables.begin(), statement.variables.end(),
        [&](const BoundVariable& variable) {
          return variable.variable == variable_name;
        });
    if (found == statement.variables.end()) return std::nullopt;
    return found->binding_id;
  };
  const auto estimate_for = [&](const std::string& variable_name) {
    const std::optional<BindingId> binding = binding_for(variable_name);
    if (!binding.has_value()) return PhysicalCardinalityEstimate{};
    const auto found = estimates.by_binding.find(*binding);
    return found == estimates.by_binding.end()
        ? PhysicalCardinalityEstimate{} : found->second;
  };
  planning.left = estimate_for(statement.joins.front().left.variable);
  planning.right = estimate_for(statement.joins.front().right.variable);
  return planning;
}

StatusOr<uint64_t> SystemTimeForCommit(const CommitTimeline& timeline, uint64_t commit_seq) {
  for (const CommitTimelineEntry& entry : timeline.entries()) {
    if (entry.commit_seq == commit_seq) {
      return entry.system_time_hlc.physical_us;
    }
  }
  return Status::Corruption("T-Cypher executor", "event commit is absent from CommitTimeline");
}

StatusOr<uint64_t> ResolveExactEntityId(const MatchNodePattern& pattern,
                                        const TcypherQueryOptions& options) {
  if (!pattern.entity_id.has_value()) {
    return Status::BindError("T-Cypher executor", "strict query requires an exact entity id");
  }
  if (pattern.entity_id->integer_literal.has_value()) return *pattern.entity_id->integer_literal;
  const auto parameter = options.timestamp_parameters.find(pattern.entity_id->parameter_name);
  if (parameter == options.timestamp_parameters.end()) {
    return Status::BindError("T-Cypher executor", "exact entity id parameter is missing");
  }
  return parameter->second;
}

Status CheckQueryCancelled(const TcypherExecutionContext& context) {
  if (context.options.cancellation && context.options.cancellation->IsCancelled()) {
    return Status::QueryCancelled("T-Cypher executor", "query cancelled");
  }
  return Status::OK();
}

bool PhysicalHashJoinOwnsOperator(
    const PhysicalHashJoinPlan& plan, PhysicalOperatorKind kind) {
  return std::any_of(
      plan.post_join_operators.begin(), plan.post_join_operators.end(),
      [kind](const PhysicalOperatorSpec& op) { return op.kind == kind; });
}

bool PhysicalMultiHashJoinOwnsOperator(
    const PhysicalMultiHashJoinPlan& plan, PhysicalOperatorKind kind) {
  return std::any_of(
      plan.post_join_operators.begin(), plan.post_join_operators.end(),
      [kind](const PhysicalOperatorSpec& op) { return op.kind == kind; });
}

StatusOr<PinnedTemporalScanSources> PinnedScanSourcesFor(
    const TcypherExecutionContext& context) {
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
    auto materialized = std::make_shared<TemporalMemTable>();
    for (const TemporalEvent& event : context.committed_events) {
      const Status inserted = materialized->Insert(event);
      if (!inserted.ok()) return inserted;
    }
    sources.memtables.push_back(std::move(materialized));
  }
  if (!context.session_overlay_events.empty()) {
    sources.session_overlay =
        std::make_shared<const std::vector<TemporalEvent>>(
            context.session_overlay_events);
  }
  return sources;
}

void AttachPinnedScanStats(const TcypherExecutionContext& context,
                           bool property_scan, TemporalScanSpec* spec) {
  if (!context.options.execution_stats) return;
  const std::shared_ptr<TcypherExecutionStats> execution_stats =
      context.options.execution_stats;
  spec->open_observer = [execution_stats, property_scan]() {
    if (property_scan) ++execution_stats->pinned_property_point_scans;
    else ++execution_stats->pinned_root_scan_opens;
  };
  spec->stats_observer =
      [execution_stats, property_scan](uint64_t event_delta, uint64_t block_delta,
                        uint64_t max_buffered_events) {
    execution_stats->base_events_visited += event_delta;
    execution_stats->sst_blocks_read += block_delta;
    if (!property_scan) execution_stats->root_sst_blocks_read += block_delta;
    execution_stats->max_sst_cursor_buffered_events = std::max(
        execution_stats->max_sst_cursor_buffered_events,
        max_buffered_events);
  };
}

struct AccountedTimelineSnapshot {
  ~AccountedTimelineSnapshot() {
    std::vector<uint64_t>().swap(physical_times);
    if (memory_account) memory_account->Release(retained_bytes);
  }

  std::shared_ptr<QueryMemoryAccount> memory_account;
  uint64_t retained_bytes = 0;
  std::vector<uint64_t> physical_times;
};

struct DerivedVectorMemoryLease {
  ~DerivedVectorMemoryLease() {
    if (memory_account) memory_account->Release(retained_bytes);
  }

  std::shared_ptr<QueryMemoryAccount> memory_account;
  uint64_t retained_bytes = 0;
};

StatusOr<VectorBatchTransform> BuildPointMetadataTransform(
    const TcypherExecutionContext& context,
    PinnedTemporalScanSources sources, TemporalScanSpec scan_spec,
    bool include_valid_to, bool include_system_time) {
  if (!include_valid_to && !include_system_time) {
    return VectorBatchTransform{};
  }
  std::shared_ptr<AccountedTimelineSnapshot> system_times;
  if (include_system_time) {
    const auto& entries = context.commit_timeline.entries();
    const size_t snapshot_entries = static_cast<size_t>(std::min<uint64_t>(
        scan_spec.snapshot_seq, entries.size()));
    for (size_t index = 0; index < snapshot_entries; ++index) {
      if (entries[index].commit_seq != static_cast<uint64_t>(index) + 1) {
        return Status::Corruption("point metadata",
                                  "CommitTimeline sequence is not contiguous");
      }
    }
    if (snapshot_entries >
        (std::numeric_limits<uint64_t>::max() -
         sizeof(AccountedTimelineSnapshot)) / sizeof(uint64_t)) {
      return Status::QueryMemoryLimit("point metadata",
                                      "timeline snapshot charge overflow");
    }
    const uint64_t timeline_charge = sizeof(AccountedTimelineSnapshot) +
        static_cast<uint64_t>(snapshot_entries) * sizeof(uint64_t);
    if (context.options.memory_account) {
      const Status reserved =
          context.options.memory_account->Reserve(timeline_charge);
      if (!reserved.ok()) return reserved;
    }
    system_times = std::make_shared<AccountedTimelineSnapshot>();
    system_times->memory_account = context.options.memory_account;
    system_times->retained_bytes = timeline_charge;
    system_times->physical_times.reserve(snapshot_entries);
    for (size_t index = 0; index < snapshot_entries; ++index) {
      system_times->physical_times.push_back(
          entries[index].system_time_hlc.physical_us);
    }
  }
  scan_spec.open_observer = nullptr;
  scan_spec.stats_observer = nullptr;
  if (context.options.execution_stats) {
    const std::shared_ptr<TcypherExecutionStats> execution_stats =
        context.options.execution_stats;
    scan_spec.open_observer = [execution_stats]() {
      ++execution_stats->pinned_boundary_point_scans;
    };
    scan_spec.stats_observer =
        [execution_stats](uint64_t, uint64_t block_delta,
                          uint64_t max_buffered_events) {
      execution_stats->boundary_sst_blocks_read += block_delta;
      execution_stats->max_sst_cursor_buffered_events = std::max(
          execution_stats->max_sst_cursor_buffered_events,
          max_buffered_events);
    };
  }
  const std::shared_ptr<TcypherExecutionStats> metadata_stats =
      context.options.execution_stats;
  return VectorBatchTransform(
      [sources = std::move(sources), scan_spec = std::move(scan_spec),
       system_times = std::move(system_times), include_valid_to,
       include_system_time, metadata_stats](ColumnBatch* batch) -> Status {
    if (batch == nullptr || batch->column_count() <= kCommitSeq) {
      return Status::InvalidArgument("point metadata", "invalid input batch");
    }
    const uint64_t derived_columns =
        (include_valid_to ? 1U : 0U) + (include_system_time ? 1U : 0U);
    const uint64_t row_charge = sizeof(Value) + sizeof(bool);
    const uint64_t per_vector_charge = sizeof(FlatVector) +
        static_cast<uint64_t>(batch->row_count()) * row_charge;
    if (derived_columns > std::numeric_limits<uint64_t>::max() /
                              per_vector_charge) {
      return Status::QueryMemoryLimit("point metadata",
                                      "derived vector charge overflow");
    }
    const uint64_t derived_charge = derived_columns * per_vector_charge;
    if (scan_spec.memory_account) {
      const Status reserved =
          scan_spec.memory_account->Reserve(derived_charge);
      if (!reserved.ok()) return reserved;
      if (metadata_stats) {
        metadata_stats->metadata_derived_bytes_reserved += derived_charge;
      }
    }
    auto derived_lease = std::make_shared<DerivedVectorMemoryLease>();
    derived_lease->memory_account = scan_spec.memory_account;
    derived_lease->retained_bytes = derived_charge;
    if (include_valid_to) {
      std::vector<Value> valid_to;
      valid_to.reserve(batch->row_count());
      for (uint32_t row = 0; row < batch->row_count(); ++row) {
        const auto entity_id = batch->ValueAt(kEntityId, row);
        const auto valid_from = batch->ValueAt(kValidFrom, row);
        if (!entity_id.has_value() || !valid_from.has_value() ||
            entity_id->type() != PhysicalType::kInt64 ||
            valid_from->type() != PhysicalType::kTimestamp64 ||
            std::get<int64_t>(entity_id->data()) < 0) {
          return Status::Corruption("point metadata",
                                    "scan fact identity is invalid");
        }
        const LogicalKey key = LogicalKey::VertexExistence(
            static_cast<uint64_t>(std::get<int64_t>(entity_id->data())));
        const auto boundary = FindNextPinnedValidBoundary(
            sources, scan_spec, key,
            std::get<uint64_t>(valid_from->data()));
        if (!boundary.ok()) return boundary.status();
        valid_to.push_back(Value::Timestamp(
            boundary.ValueOrDie().value_or(kTemporalInfinity)));
      }
      const Status added = batch->AddVector(std::make_shared<FlatVector>(
          std::move(valid_to), std::vector<bool>{}, derived_lease));
      if (!added.ok()) return added;
    }
    if (include_system_time) {
      std::vector<Value> values;
      values.reserve(batch->row_count());
      for (uint32_t row = 0; row < batch->row_count(); ++row) {
        const auto commit_seq = batch->ValueAt(kCommitSeq, row);
        if (!commit_seq.has_value() ||
            commit_seq->type() != PhysicalType::kInt64 ||
            std::get<int64_t>(commit_seq->data()) <= 0) {
          return Status::Corruption("point metadata",
                                    "scan commit sequence is invalid");
        }
        const uint64_t selected_commit = static_cast<uint64_t>(
            std::get<int64_t>(commit_seq->data()));
        if (!system_times ||
            selected_commit > system_times->physical_times.size()) {
          return Status::Corruption("point metadata",
                                    "commit is absent from CommitTimeline");
        }
        values.push_back(Value::Timestamp(
            system_times->physical_times[selected_commit - 1]));
      }
      const Status added = batch->AddVector(std::make_shared<FlatVector>(
          std::move(values), std::vector<bool>{}, derived_lease));
      if (!added.ok()) return added;
    }
    return Status::OK();
  });
}

Status VisitPinnedBaseEvents(
    const TcypherExecutionContext& context,
    const std::function<Status(const TemporalEvent&)>& visitor) {
  const std::optional<uint64_t> session_base_snapshot =
      context.session != nullptr && context.session->active()
          ? std::optional<uint64_t>(context.session->snapshot_seq())
          : std::nullopt;
  const auto counted_visitor = [&context, &visitor,
                                session_base_snapshot](const TemporalEvent& event) {
    if (context.options.execution_stats) {
      ++context.options.execution_stats->base_events_visited;
    }
    if (session_base_snapshot.has_value() &&
        event.commit_seq() > *session_base_snapshot) {
      return Status::OK();
    }
    return visitor(event);
  };
  for (const auto& source : context.memtable_event_sources) {
    const Status status = source->VisitEvents(counted_visitor);
    if (!status.ok()) return status;
  }
  for (const PinnedSstSource& source : context.sst_event_sources) {
    SstCursorStats cursor_stats;
    const Status status = VisitSstEvents(
        source.path, counted_visitor, &cursor_stats, context.io_governor,
        context.options.workload_class == TcypherWorkloadClass::kAnalytical);
    if (!status.ok()) return status;
    if (context.options.execution_stats) {
      context.options.execution_stats->sst_blocks_read += cursor_stats.blocks_read;
      context.options.execution_stats->max_sst_cursor_buffered_events = std::max(
          context.options.execution_stats->max_sst_cursor_buffered_events,
          cursor_stats.peak_buffered_events);
    }
  }
  return Status::OK();
}

Status VisitCommittedEvents(
    const TcypherExecutionContext& context,
    const std::function<Status(const TemporalEvent&)>& visitor) {
  for (const TemporalEvent& event : context.committed_events) {
    if (context.options.execution_stats) {
      ++context.options.execution_stats->base_events_visited;
    }
    const Status status = visitor(event);
    if (!status.ok()) return status;
  }
  for (const TemporalEvent& event : context.session_overlay_events) {
    const Status status = visitor(event);
    if (!status.ok()) return status;
  }
  return VisitPinnedBaseEvents(context, visitor);
}

Status VisitCommittedEventsForKey(
    const TcypherExecutionContext& context, const LogicalKey& key,
    const std::function<Status(const TemporalEvent&)>& visitor) {
  if (!visitor) {
    return Status::InvalidArgument("T-Cypher key cursor", "event visitor is required");
  }
  for (const TemporalEvent& event : context.committed_events) {
    if (event.logical_key() != key) continue;
    if (context.options.execution_stats) {
      ++context.options.execution_stats->base_events_visited;
    }
    const Status status = visitor(event);
    if (!status.ok()) return status;
  }
  for (const TemporalEvent& event : context.session_overlay_events) {
    if (event.logical_key() != key) continue;
    const Status status = visitor(event);
    if (!status.ok()) return status;
  }
  const std::optional<uint64_t> session_base_snapshot =
      context.session != nullptr && context.session->active()
          ? std::optional<uint64_t>(context.session->snapshot_seq())
          : std::nullopt;
  const auto counted_visitor = [&context, &visitor,
                                session_base_snapshot](const TemporalEvent& event) {
    if (context.options.execution_stats) {
      ++context.options.execution_stats->base_events_visited;
    }
    if (session_base_snapshot.has_value() &&
        event.commit_seq() > *session_base_snapshot) {
      return Status::OK();
    }
    return visitor(event);
  };
  for (const auto& source : context.memtable_event_sources) {
    const Status status = source->VisitKeyEvents(key, counted_visitor);
    if (!status.ok()) return status;
  }
  for (const PinnedSstSource& source : context.sst_event_sources) {
    SstCursorStats cursor_stats;
    const Status status = VisitSstEventsForKey(
        source.path, key, counted_visitor, &cursor_stats, nullptr,
        context.io_governor);
    if (context.options.execution_stats) {
      context.options.execution_stats->sst_blocks_read += cursor_stats.blocks_read;
      context.options.execution_stats->max_sst_cursor_buffered_events = std::max(
          context.options.execution_stats->max_sst_cursor_buffered_events,
          cursor_stats.peak_buffered_events);
    }
    if (!status.ok()) return status;
  }
  return Status::OK();
}

StatusOr<std::map<LogicalKey, std::vector<TemporalEvent>>> GroupCommittedEvents(
    const TcypherExecutionContext& context,
    const std::function<bool(const TemporalEvent&)>& include) {
  std::map<LogicalKey, std::vector<TemporalEvent>> grouped;
  const Status status = VisitCommittedEvents(context, [&](const TemporalEvent& event) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    if (include(event)) {
      grouped[event.logical_key()].push_back(event);
      if (context.options.execution_stats) {
        ++context.options.execution_stats->base_history_materialized_events;
        ++context.options.execution_stats->vector_materialized_root_events;
      }
    }
    return Status::OK();
  });
  if (!status.ok()) return status;
  return grouped;
}

StatusOr<std::map<LogicalKey, std::vector<TemporalEvent>>>
GroupCommittedEventsForKeys(
    const TcypherExecutionContext& context,
    const std::vector<LogicalKey>& keys) {
  std::map<LogicalKey, std::vector<TemporalEvent>> grouped;
  for (const LogicalKey& key : keys) {
    const Status status = VisitCommittedEventsForKey(
        context, key, [&context, &grouped](const TemporalEvent& event) {
          const Status cancelled = CheckQueryCancelled(context);
          if (!cancelled.ok()) return cancelled;
          grouped[event.logical_key()].push_back(event);
          if (context.options.execution_stats) {
            ++context.options.execution_stats->base_history_materialized_events;
            ++context.options.execution_stats->vector_materialized_root_events;
          }
          return Status::OK();
        });
    if (!status.ok()) return status;
  }
  return grouped;
}

StatusOr<std::optional<Value>> ResolveQueryValue(
    const std::map<LogicalKey, std::vector<TemporalEvent>>& events_by_key,
    const LogicalKey& key, uint64_t valid_time, uint64_t snapshot_seq,
    const TcypherExecutionContext& context) {
  const auto found = events_by_key.find(key);
  if (found == events_by_key.end()) return std::optional<Value>{};
  const std::optional<TemporalEvent> event =
      ResolveVisibleEvent(found->second, key, valid_time, snapshot_seq);
  if (!event.has_value() || event->is_delete()) {
    return std::optional<Value>{};
  }
  if (!event->is_blob_reference()) {
    return std::optional<Value>{event->value()};
  }
  if (context.options.execution_stats) {
    ++context.options.execution_stats->blob_refs_seen;
    ++context.options.execution_stats->blob_payload_reads;
  }
  if (context.transaction_coordinator == nullptr) {
    return Status::NotSupported(
        "T-Cypher executor",
        "Blob projection requires a transaction coordinator");
  }
  return context.transaction_coordinator->MaterializeBlobValue(*event);
}

StatusOr<std::vector<TemporalEvent>> CollectCommittedEvents(
    const TcypherExecutionContext& context,
    const std::function<bool(const TemporalEvent&)>& include) {
  std::vector<TemporalEvent> events;
  const Status status = VisitCommittedEvents(context, [&](const TemporalEvent& event) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    if (include(event)) {
      events.push_back(event);
      if (context.options.execution_stats) {
        ++context.options.execution_stats->base_history_materialized_events;
        ++context.options.execution_stats->vector_materialized_root_events;
      }
    }
    return Status::OK();
  });
  if (!status.ok()) return status;
  return events;
}

StatusOr<std::vector<TemporalEvent>> CollectCommittedEventsForKey(
    const TcypherExecutionContext& context, const LogicalKey& key) {
  std::vector<TemporalEvent> events;
  const Status status = VisitCommittedEventsForKey(
      context, key, [&context, &events](const TemporalEvent& event) {
        const Status cancelled = CheckQueryCancelled(context);
        if (!cancelled.ok()) return cancelled;
        events.push_back(event);
        if (context.options.execution_stats) {
          ++context.options.execution_stats->base_history_materialized_events;
          ++context.options.execution_stats->vector_materialized_root_events;
        }
        return Status::OK();
      });
  if (!status.ok()) return status;
  return events;
}

StatusOr<std::vector<TemporalEvent>> CollectCommittedEventsForKeys(
    const TcypherExecutionContext& context,
    const std::vector<LogicalKey>& keys) {
  std::vector<TemporalEvent> events;
  for (const LogicalKey& key : keys) {
    const Status status = VisitCommittedEventsForKey(
        context, key, [&context, &events](const TemporalEvent& event) {
          const Status cancelled = CheckQueryCancelled(context);
          if (!cancelled.ok()) return cancelled;
          events.push_back(event);
          if (context.options.execution_stats) {
            ++context.options.execution_stats->base_history_materialized_events;
            ++context.options.execution_stats->vector_materialized_root_events;
          }
          return Status::OK();
        });
    if (!status.ok()) return status;
  }
  return events;
}

bool MatchesStringPredicate(const Value& value, const StringEqualityPredicate& predicate) {
  if (value.type() != PhysicalType::kString) return false;
  const std::string& text = std::get<std::string>(value.data());
  switch (predicate.kind) {
    case StringPredicateKind::kEquality:
      return text == predicate.string_value;
    case StringPredicateKind::kIn:
      return std::find(predicate.in_values.begin(), predicate.in_values.end(), text) !=
             predicate.in_values.end();
    case StringPredicateKind::kPrefix:
      return text.size() >= predicate.string_value.size() &&
             text.compare(0, predicate.string_value.size(), predicate.string_value) == 0;
    case StringPredicateKind::kRange:
      if (predicate.lower_bound.has_value() &&
          (text < *predicate.lower_bound ||
           (text == *predicate.lower_bound && !predicate.lower_inclusive))) return false;
      if (predicate.upper_bound.has_value() &&
          (text > *predicate.upper_bound ||
           (text == *predicate.upper_bound && !predicate.upper_inclusive))) return false;
      return true;
  }
  return false;
}

bool UsesIntegerLiteral(const StringEqualityPredicate& predicate) {
  return predicate.integer_value.has_value() || !predicate.in_integer_values.empty() ||
      predicate.lower_integer_bound.has_value() || predicate.upper_integer_bound.has_value();
}

std::optional<int64_t> IntegerValueForPredicate(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kInt32:
      return static_cast<int64_t>(std::get<int32_t>(value.data()));
    case PhysicalType::kInt64:
      return std::get<int64_t>(value.data());
    case PhysicalType::kTimestamp64: {
      const uint64_t timestamp = std::get<uint64_t>(value.data());
      if (timestamp > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::nullopt;
      }
      return static_cast<int64_t>(timestamp);
    }
    default:
      return std::nullopt;
  }
}

bool MatchesTypedPredicate(const Value& value, const StringEqualityPredicate& predicate) {
  if (!UsesIntegerLiteral(predicate)) return MatchesStringPredicate(value, predicate);
  const auto candidate = IntegerValueForPredicate(value);
  if (!candidate.has_value()) return false;
  switch (predicate.kind) {
    case StringPredicateKind::kEquality:
      return predicate.integer_value.has_value() && *candidate == *predicate.integer_value;
    case StringPredicateKind::kIn:
      return std::find(predicate.in_integer_values.begin(), predicate.in_integer_values.end(),
                       *candidate) != predicate.in_integer_values.end();
    case StringPredicateKind::kRange:
      if (predicate.lower_integer_bound.has_value() &&
          (*candidate < *predicate.lower_integer_bound ||
           (*candidate == *predicate.lower_integer_bound && !predicate.lower_inclusive))) {
        return false;
      }
      if (predicate.upper_integer_bound.has_value() &&
          (*candidate > *predicate.upper_integer_bound ||
           (*candidate == *predicate.upper_integer_bound && !predicate.upper_inclusive))) {
        return false;
      }
      return true;
    case StringPredicateKind::kPrefix:
      return false;
  }
  return false;
}

StatusOr<std::optional<Value>> IntegerPredicateValue(
    const ColumnSchema& schema, const StringEqualityPredicate& predicate) {
  if (!predicate.integer_value.has_value()) {
    return Status::InvalidArgument("T-Cypher executor", "integer equality has no literal");
  }
  const int64_t value = *predicate.integer_value;
  switch (schema.physical_type) {
    case PhysicalType::kInt32:
      if (value < std::numeric_limits<int32_t>::min() ||
          value > std::numeric_limits<int32_t>::max()) {
        return Status::SchemaMismatch("T-Cypher executor", "integer literal exceeds Int32 range");
      }
      return std::optional<Value>{Value::Int32(static_cast<int32_t>(value))};
    case PhysicalType::kInt64:
      return std::optional<Value>{Value::Int64(value)};
    case PhysicalType::kTimestamp64:
      if (value < 0) {
        return Status::SchemaMismatch("T-Cypher executor", "timestamp predicate cannot be negative");
      }
      return std::optional<Value>{Value::Timestamp(static_cast<uint64_t>(value))};
    default:
      return Status::SchemaMismatch("T-Cypher executor", "property is not integer-indexable");
  }
}

Status ApplySessionOverlay(TcypherExecutionContext* context) {
  if (context == nullptr || context->session == nullptr || !context->session->active()) {
    return Status::OK();
  }
  const uint64_t snapshot_seq = context->session->snapshot_seq();
  context->session_overlay_events.clear();
  context->visible_seq_ceiling = snapshot_seq;
  context->committed_events.erase(
      std::remove_if(context->committed_events.begin(), context->committed_events.end(),
                     [snapshot_seq](const TemporalEvent& event) {
                       return event.commit_seq() > snapshot_seq;
                     }),
      context->committed_events.end());
  const std::vector<PendingEvent>& pending = context->session->pending_events();
  if (pending.empty()) return Status::OK();
  if (pending.size() > std::numeric_limits<uint64_t>::max() - context->visible_seq_ceiling) {
    return Status::InvalidArgument("T-Cypher executor", "session overlay sequence overflow");
  }
  context->session_overlay_events.reserve(pending.size());
  uint64_t temporary_seq = context->visible_seq_ceiling;
  for (const PendingEvent& event : pending) {
    ++temporary_seq;
    if (event.operation == TemporalOperation::kDelete) {
      context->session_overlay_events.push_back(TemporalEvent::Delete(
          event.logical_key, event.valid_from, temporary_seq, event.schema_epoch));
    } else if (event.blob_ref.has_value()) {
      context->session_overlay_events.push_back(TemporalEvent::PutBlob(
          event.logical_key, event.valid_from, temporary_seq, event.schema_epoch, *event.blob_ref));
    } else {
      context->session_overlay_events.push_back(TemporalEvent::Put(
          event.logical_key, event.valid_from, temporary_seq, event.schema_epoch, event.value));
    }
  }
  std::sort(context->session_overlay_events.begin(),
            context->session_overlay_events.end(),
            [](const TemporalEvent& left, const TemporalEvent& right) {
    if (left.logical_key() != right.logical_key()) {
      return left.logical_key() < right.logical_key();
    }
    if (left.valid_from() != right.valid_from()) {
      return left.valid_from() > right.valid_from();
    }
    return left.commit_seq() > right.commit_seq();
  });
  context->visible_seq_ceiling = temporary_seq;
  return Status::OK();
}

bool IsIndexedPropertyEvent(const TemporalEvent& event, const ColumnSchema& schema) {
  const LogicalKey& key = event.logical_key();
  return key.entity_type() == EntityType::Vertex && key.kind() == LogicalKeyKind::kProperty &&
      key.column_id() == schema.column_id && event.schema_epoch() == schema.schema_epoch;
}

StatusOr<IndexCanonicalValue> EncodeIndexedEventValue(
    const TemporalEvent& event) {
  return event.is_blob_reference()
      ? StatusOr<IndexCanonicalValue>(
            EncodeIndexBlobHash(*event.blob_ref()))
      : EncodeIndexCanonicalValue(event.value());
}

StatusOr<bool> IndexedEventEqualsValue(const TemporalEvent& event,
                                       const Value& expected) {
  const auto actual = EncodeIndexedEventValue(event);
  if (!actual.ok()) return actual.status();
  const auto wanted = event.is_blob_reference()
      ? EncodeIndexBlobHash(expected)
      : EncodeIndexCanonicalValue(expected);
  if (!wanted.ok()) return wanted.status();
  return CompareIndexCanonicalValues(
      actual.ValueOrDie(), wanted.ValueOrDie()) == 0;
}

bool SamePostingIdentity(const TemporalEvent& event, const IndexPosting& posting,
                         const ColumnSchema& schema) {
  if (!IsIndexedPropertyEvent(event, schema) || event.is_delete() ||
      event.valid_from() != posting.valid_from ||
      event.commit_seq() != posting.commit_seq) {
    return false;
  }
  const auto canonical = EncodeIndexedEventValue(event);
  return canonical.ok() &&
      CompareIndexCanonicalValues(canonical.ValueOrDie(), posting.value) == 0;
}

bool ContainsLogicalEvent(const std::vector<TcypherIndexSource>& sources,
                          uint64_t index_id, const std::set<uint64_t>& allowed_source_ids,
                          const TemporalEvent& sought) {
  for (const TcypherIndexSource& source : sources) {
    if (source.index_id != index_id || allowed_source_ids.count(source.source_sst_id) == 0) {
      continue;
    }
    for (const TemporalEvent& event : source.events) {
      if (event.logical_key() == sought.logical_key() &&
          event.valid_from() == sought.valid_from() &&
          event.commit_seq() == sought.commit_seq() &&
          event.schema_epoch() == sought.schema_epoch() &&
          event.operation() == sought.operation()) {
        return true;
      }
    }
  }
  return false;
}

bool ContainsLogicalEvent(const TcypherDeltaIndexSource& source,
                          const TemporalEvent& sought) {
  return std::any_of(source.events.begin(), source.events.end(),
                     [&sought](const TemporalEvent& event) {
    return event.logical_key() == sought.logical_key() &&
           event.valid_from() == sought.valid_from() &&
           event.commit_seq() == sought.commit_seq() &&
           event.schema_epoch() == sought.schema_epoch() &&
           event.operation() == sought.operation();
  });
}

bool DeltaOrdinalsAreComplete(
    const TcypherDeltaIndexSource& source, const ColumnSchema& schema,
    const std::vector<uint64_t>& ordinals,
    const std::function<bool(const TemporalEvent&)>& matches) {
  std::vector<bool> selected(source.events.size(), false);
  for (uint64_t ordinal : ordinals) {
    if (ordinal >= source.events.size() || selected[ordinal]) return false;
    const TemporalEvent& event = source.events[ordinal];
    if (!IsIndexedPropertyEvent(event, schema) || event.is_delete() ||
        !matches(event)) {
      return false;
    }
    selected[ordinal] = true;
  }
  for (uint64_t ordinal = 0; ordinal < source.events.size(); ++ordinal) {
    const TemporalEvent& event = source.events[ordinal];
    const bool expected = IsIndexedPropertyEvent(event, schema) &&
        !event.is_delete() && matches(event);
    if (selected[ordinal] != expected) return false;
  }
  return true;
}

// The index is an optional candidate source. It can only restrict the base
// scan when every relevant immutable source is represented by an explicitly
// usable fragment in the same pinned VersionSnapshot. Every selected entity
// is still checked against the complete property history before becoming a
// result.
StatusOr<std::optional<std::set<uint64_t>>> IndexEqualityCandidates(
    const TcypherExecutionContext& context, const ColumnSchema& schema,
    const Value& expected) {
  if (!context.index_catalog_snapshot || !context.version_snapshot ||
      (context.index_sources.empty() && context.delta_index_sources.empty()) ||
      context.index_catalog_snapshot->catalog_generation != context.version_snapshot->generation) {
    return std::optional<std::set<uint64_t>>{};
  }

  const IndexDefinition* selected = nullptr;
  for (const IndexDefinition& definition : context.index_catalog_snapshot->definitions) {
    if (definition.state != IndexState::kActive ||
        definition.entity_type != EntityType::Vertex ||
        definition.column_id != schema.column_id ||
        definition.schema_epoch != schema.schema_epoch ||
        (definition.capabilities & kIndexEquality) == 0 ||
        !IsSupportedIndexCanonicalEncoding(
            definition.canonical_encoding_id)) {
      continue;
    }
    selected = &definition;
    break;
  }
  if (selected == nullptr) return std::optional<std::set<uint64_t>>{};

  const TcypherDeltaIndexSource* delta_source = nullptr;
  for (const TcypherDeltaIndexSource& source : context.delta_index_sources) {
    if (source.index_id != selected->index_id ||
        source.source_generation != context.visible_seq_ceiling) {
      continue;
    }
    if (delta_source != nullptr) return std::optional<std::set<uint64_t>>{};
    delta_source = &source;
  }

  std::map<uint64_t, const TcypherIndexSource*> sources_by_id;
  for (const TcypherIndexSource& source : context.index_sources) {
    if (source.index_id != selected->index_id) continue;
    if (source.source_sst_id == 0 || source.sidecar.source_sst_id != source.source_sst_id ||
        !sources_by_id.emplace(source.source_sst_id, &source).second) {
      return std::optional<std::set<uint64_t>>{};
    }
  }
  std::set<uint64_t> covered_source_ids;
  for (const SstFileMeta& file : context.version_snapshot->files) {
    if (file.partition.entity_type != EntityType::Vertex ||
        file.partition.key_kind != LogicalKeyKind::kProperty ||
        file.partition.column_id != schema.column_id ||
        file.partition.schema_epoch != schema.schema_epoch) {
      continue;
    }
    const auto source = sources_by_id.find(file.file_number);
    const auto fragment = std::find_if(context.index_catalog_snapshot->fragments.begin(),
                                       context.index_catalog_snapshot->fragments.end(),
        [&file, selected](const IndexFragment& candidate) {
          return candidate.index_id == selected->index_id &&
              candidate.source_sst_id == file.file_number && candidate.usable;
        });
    if (source == sources_by_id.end() || fragment == context.index_catalog_snapshot->fragments.end() ||
        fragment->catalog_generation > context.index_catalog_snapshot->catalog_generation ||
        fragment->source_row_count != source->second->events.size()) {
      continue;
    }
    covered_source_ids.insert(file.file_number);
  }
  uint64_t base_rows = 0;
  std::map<LogicalKey, uint64_t> property_versions;
  Status visited = VisitCommittedEvents(context, [&](const TemporalEvent& event) {
    if (event.logical_key().entity_type() == EntityType::Vertex &&
        event.logical_key().kind() == LogicalKeyKind::kExistence) {
      ++base_rows;
    }
    if (event.logical_key().entity_type() == EntityType::Vertex &&
        event.logical_key().kind() == LogicalKeyKind::kProperty &&
        event.logical_key().column_id() == schema.column_id &&
        event.schema_epoch() == schema.schema_epoch) {
      ++property_versions[event.logical_key()];
    }
    return Status::OK();
  });
  if (!visited.ok()) return visited;
  uint64_t version_total = 0;
  for (const auto& entry : property_versions) version_total += entry.second;
  const uint64_t versions_per_candidate = property_versions.empty()
      ? 1 : std::max<uint64_t>(1, version_total / property_versions.size());
  std::set<uint64_t> entity_ids;
  uint64_t candidate_rows = 0;
  std::set<uint64_t> indexed_source_ids = covered_source_ids;
  for (uint64_t source_id : covered_source_ids) {
    const TcypherIndexSource& source = *sources_by_id.at(source_id);
    const auto postings = LookupIndexEquality(source.sidecar, expected);
    if (!postings.ok()) {
      indexed_source_ids.erase(source_id);
      continue;
    }
    bool valid_postings = true;
    for (const IndexPosting& posting : postings.ValueOrDie()) {
      if (posting.source_row_ordinal >= source.events.size() ||
          !SamePostingIdentity(source.events[posting.source_row_ordinal], posting, schema)) {
        valid_postings = false;
        break;
      }
    }
    if (!valid_postings) {
      indexed_source_ids.erase(source_id);
      continue;
    }
    for (const IndexPosting& posting : postings.ValueOrDie()) {
      ++candidate_rows;
      entity_ids.insert(source.events[posting.source_row_ordinal].logical_key().entity_id());
    }
  }

  bool delta_healthy = false;
  if (delta_source != nullptr) {
    if (context.options.execution_stats) {
      ++context.options.execution_stats->memtable_delta_probes;
    }
    const auto ordinals = delta_source->index.Lookup(
        delta_source->source_generation, expected);
    if (ordinals.ok()) {
      delta_healthy = DeltaOrdinalsAreComplete(
          *delta_source, schema, ordinals.ValueOrDie(), [&](const TemporalEvent& event) {
        const auto matches = IndexedEventEqualsValue(event, expected);
        return matches.ok() && matches.ValueOrDie();
      });
      if (delta_healthy) {
        for (uint64_t ordinal : ordinals.ValueOrDie()) {
          ++candidate_rows;
          entity_ids.insert(delta_source->events[ordinal].logical_key().entity_id());
        }
        if (context.options.execution_stats) {
          context.options.execution_stats->memtable_delta_candidates +=
              ordinals.ValueOrDie().size();
        }
      }
    }
  }
  if (covered_source_ids.empty() && !delta_healthy) {
    return std::optional<std::set<uint64_t>>{};
  }

  // Sources without a healthy sidecar, including MemTables and FrozenMemTables,
  // contribute ordinary base candidates. The union is only an accelerator;
  // the normal temporal property gather still validates visibility and deletes.
  uint64_t uncovered_base_rows = 0;
  bool fallback_to_base = false;
  visited = VisitCommittedEvents(context, [&](const TemporalEvent& event) {
    if (!IsIndexedPropertyEvent(event, schema)) return Status::OK();
    if (ContainsLogicalEvent(context.index_sources, selected->index_id, indexed_source_ids, event)) {
      return Status::OK();
    }
    if (delta_healthy && ContainsLogicalEvent(*delta_source, event)) return Status::OK();
    ++uncovered_base_rows;
    const auto matches = IndexedEventEqualsValue(event, expected);
    if (!matches.ok()) {
      fallback_to_base = true;
      return Status::OK();
    }
    if (matches.ValueOrDie()) {
      entity_ids.insert(event.logical_key().entity_id());
    }
    return Status::OK();
  });
  if (!visited.ok()) return visited;
  if (fallback_to_base) return std::optional<std::set<uint64_t>>{};
  const AccessPathDecision decision = ChooseAccessPathDecision(
      ScanCostEstimate{base_rows, candidate_rows, uncovered_base_rows,
                       versions_per_candidate},
      OptimizerBudget{4, false});
  if (decision.source != CandidateSource::kIndex && decision.source != CandidateSource::kHybrid) {
    return std::optional<std::set<uint64_t>>{};
  }
  return std::optional<std::set<uint64_t>>{std::move(entity_ids)};
}

StatusOr<std::optional<std::set<uint64_t>>> IndexStringPredicateCandidates(
    const TcypherExecutionContext& context, const ColumnSchema& schema,
    const StringEqualityPredicate& predicate) {
  if (predicate.kind == StringPredicateKind::kEquality) {
    return IndexEqualityCandidates(context, schema, Value::String(predicate.string_value));
  }
  if (!context.index_catalog_snapshot || !context.version_snapshot ||
      (context.index_sources.empty() && context.delta_index_sources.empty()) ||
      context.index_catalog_snapshot->catalog_generation != context.version_snapshot->generation) {
    return std::optional<std::set<uint64_t>>{};
  }
  const uint32_t required_capability = predicate.kind == StringPredicateKind::kPrefix
      ? kIndexPrefix : (predicate.kind == StringPredicateKind::kRange ? kIndexOrderedRange
                                                                        : kIndexEquality);
  const IndexDefinition* selected = nullptr;
  for (const IndexDefinition& definition : context.index_catalog_snapshot->definitions) {
    if (definition.state == IndexState::kActive && definition.entity_type == EntityType::Vertex &&
        definition.column_id == schema.column_id && definition.schema_epoch == schema.schema_epoch &&
        (definition.capabilities & required_capability) != 0 &&
        IsSupportedIndexCanonicalEncoding(
            definition.canonical_encoding_id)) {
      selected = &definition;
      break;
    }
  }
  if (selected == nullptr) return std::optional<std::set<uint64_t>>{};

  const TcypherDeltaIndexSource* delta_source = nullptr;
  for (const TcypherDeltaIndexSource& source : context.delta_index_sources) {
    if (source.index_id != selected->index_id ||
        source.source_generation != context.visible_seq_ceiling) {
      continue;
    }
    if (delta_source != nullptr) return std::optional<std::set<uint64_t>>{};
    delta_source = &source;
  }

  std::map<uint64_t, const TcypherIndexSource*> sources_by_id;
  for (const TcypherIndexSource& source : context.index_sources) {
    if (source.index_id != selected->index_id || source.source_sst_id == 0 ||
        source.sidecar.source_sst_id != source.source_sst_id ||
        !sources_by_id.emplace(source.source_sst_id, &source).second) {
      continue;
    }
  }
  std::set<uint64_t> covered_source_ids;
  for (const SstFileMeta& file : context.version_snapshot->files) {
    if (file.partition.entity_type != EntityType::Vertex ||
        file.partition.key_kind != LogicalKeyKind::kProperty ||
        file.partition.column_id != schema.column_id ||
        file.partition.schema_epoch != schema.schema_epoch) continue;
    const auto source = sources_by_id.find(file.file_number);
    const auto fragment = std::find_if(context.index_catalog_snapshot->fragments.begin(),
                                       context.index_catalog_snapshot->fragments.end(),
        [&file, selected](const IndexFragment& candidate) {
          return candidate.index_id == selected->index_id && candidate.source_sst_id == file.file_number &&
                 candidate.usable;
        });
    if (source != sources_by_id.end() && fragment != context.index_catalog_snapshot->fragments.end() &&
        fragment->catalog_generation <= context.index_catalog_snapshot->catalog_generation &&
        fragment->source_row_count == source->second->events.size()) {
      covered_source_ids.insert(file.file_number);
    }
  }
  uint64_t base_rows = 0;
  uint64_t version_total = 0;
  std::set<LogicalKey> property_keys;
  Status visited = VisitCommittedEvents(context, [&](const TemporalEvent& event) {
    if (event.logical_key().entity_type() == EntityType::Vertex &&
        event.logical_key().kind() == LogicalKeyKind::kExistence) ++base_rows;
    if (IsIndexedPropertyEvent(event, schema)) {
      ++version_total;
      property_keys.insert(event.logical_key());
    }
    return Status::OK();
  });
  if (!visited.ok()) return visited;
  const uint64_t versions_per_candidate = property_keys.empty()
      ? 1 : std::max<uint64_t>(1, version_total / property_keys.size());
  std::set<uint64_t> indexed_source_ids = covered_source_ids;
  std::set<uint64_t> entity_ids;
  uint64_t candidate_rows = 0;
  for (uint64_t source_id : covered_source_ids) {
    const TcypherIndexSource& source = *sources_by_id.at(source_id);
    StatusOr<std::vector<IndexPosting>> postings = Status::InvalidArgument("index probe", "invalid predicate");
    if (predicate.kind == StringPredicateKind::kIn) {
      std::vector<IndexPosting> combined;
      for (const std::string& value : predicate.in_values) {
        const auto found = LookupIndexEquality(source.sidecar, Value::String(value));
        if (!found.ok()) { postings = found.status(); break; }
        combined.insert(combined.end(), found.ValueOrDie().begin(), found.ValueOrDie().end());
      }
      std::sort(combined.begin(), combined.end(), [](const IndexPosting& left, const IndexPosting& right) {
        return left.source_row_ordinal < right.source_row_ordinal;
      });
      combined.erase(std::unique(combined.begin(), combined.end(), [](const IndexPosting& left,
                                                                        const IndexPosting& right) {
        return left.source_row_ordinal == right.source_row_ordinal;
      }), combined.end());
      postings = std::move(combined);
    } else if (predicate.kind == StringPredicateKind::kRange) {
      postings = LookupIndexRange(source.sidecar,
          predicate.lower_bound.has_value() ? std::optional<Value>(Value::String(*predicate.lower_bound))
                                             : std::nullopt,
          predicate.lower_inclusive,
          predicate.upper_bound.has_value() ? std::optional<Value>(Value::String(*predicate.upper_bound))
                                             : std::nullopt,
          predicate.upper_inclusive);
    } else {
      postings = LookupIndexPrefix(source.sidecar, Value::String(predicate.string_value));
    }
    if (!postings.ok()) { indexed_source_ids.erase(source_id); continue; }
    bool valid_postings = true;
    for (const IndexPosting& posting : postings.ValueOrDie()) {
      if (posting.source_row_ordinal >= source.events.size() ||
          !SamePostingIdentity(source.events[posting.source_row_ordinal], posting, schema)) {
        valid_postings = false;
        break;
      }
    }
    if (!valid_postings) { indexed_source_ids.erase(source_id); continue; }
    for (const IndexPosting& posting : postings.ValueOrDie()) {
      ++candidate_rows;
      entity_ids.insert(source.events[posting.source_row_ordinal].logical_key().entity_id());
    }
  }
  bool delta_healthy = false;
  if (delta_source != nullptr) {
    if (context.options.execution_stats) {
      ++context.options.execution_stats->memtable_delta_probes;
    }
    StatusOr<std::vector<uint64_t>> ordinals =
        Status::InvalidArgument("memtable delta index", "invalid string predicate");
    if (predicate.kind == StringPredicateKind::kIn) {
      std::vector<uint64_t> combined;
      bool valid = true;
      for (const std::string& value : predicate.in_values) {
        const auto found = delta_source->index.Lookup(
            delta_source->source_generation, Value::String(value));
        if (!found.ok()) {
          ordinals = found.status();
          valid = false;
          break;
        }
        combined.insert(combined.end(), found.ValueOrDie().begin(),
                        found.ValueOrDie().end());
      }
      if (valid) {
        std::sort(combined.begin(), combined.end());
        combined.erase(std::unique(combined.begin(), combined.end()), combined.end());
        ordinals = std::move(combined);
      }
    } else if (predicate.kind == StringPredicateKind::kRange) {
      ordinals = delta_source->index.LookupRange(
          delta_source->source_generation,
          predicate.lower_bound.has_value()
              ? std::optional<Value>(Value::String(*predicate.lower_bound))
              : std::nullopt,
          predicate.lower_inclusive,
          predicate.upper_bound.has_value()
              ? std::optional<Value>(Value::String(*predicate.upper_bound))
              : std::nullopt,
          predicate.upper_inclusive);
    } else {
      ordinals = delta_source->index.LookupPrefix(
          delta_source->source_generation, Value::String(predicate.string_value));
    }
    if (ordinals.ok()) {
      delta_healthy = DeltaOrdinalsAreComplete(
          *delta_source, schema, ordinals.ValueOrDie(),
          [&predicate](const TemporalEvent& event) {
            return MatchesStringPredicate(event.value(), predicate);
          });
      if (delta_healthy) {
        for (uint64_t ordinal : ordinals.ValueOrDie()) {
          ++candidate_rows;
          entity_ids.insert(
              delta_source->events[ordinal].logical_key().entity_id());
        }
        if (context.options.execution_stats) {
          context.options.execution_stats->memtable_delta_candidates +=
              ordinals.ValueOrDie().size();
        }
      }
    }
  }
  if (covered_source_ids.empty() && !delta_healthy) {
    return std::optional<std::set<uint64_t>>{};
  }
  uint64_t uncovered_base_rows = 0;
  bool fallback_to_base = false;
  visited = VisitCommittedEvents(context, [&](const TemporalEvent& event) {
    if (!IsIndexedPropertyEvent(event, schema)) return Status::OK();
    if (event.is_blob_reference()) {
      fallback_to_base = true;
      return Status::OK();
    }
    if (ContainsLogicalEvent(context.index_sources, selected->index_id, indexed_source_ids, event)) {
      return Status::OK();
    }
    if (delta_healthy && ContainsLogicalEvent(*delta_source, event)) return Status::OK();
    ++uncovered_base_rows;
    if (!event.is_delete() && MatchesStringPredicate(event.value(), predicate)) {
      entity_ids.insert(event.logical_key().entity_id());
    }
    return Status::OK();
  });
  if (!visited.ok()) return visited;
  if (fallback_to_base) return std::optional<std::set<uint64_t>>{};
  const AccessPathDecision decision = ChooseAccessPathDecision(
      ScanCostEstimate{base_rows, candidate_rows, uncovered_base_rows, versions_per_candidate},
      OptimizerBudget{4, false});
  if (decision.source != CandidateSource::kIndex && decision.source != CandidateSource::kHybrid) {
    return std::optional<std::set<uint64_t>>{};
  }
  return std::optional<std::set<uint64_t>>(std::move(entity_ids));
}

class QueryMemoryReservation {
 public:
  explicit QueryMemoryReservation(std::shared_ptr<QueryMemoryAccount> account)
      : account_(std::move(account)) {}
  ~QueryMemoryReservation() {
    if (account_ && reserved_bytes_ != 0) account_->Release(reserved_bytes_);
  }

  Status Reserve(uint64_t bytes) {
    if (!account_ || bytes == 0) return Status::OK();
    const Status reserved = account_->Reserve(bytes);
    if (!reserved.ok()) return reserved;
    reserved_bytes_ += bytes;
    return Status::OK();
  }

  std::pair<std::shared_ptr<QueryMemoryAccount>, uint64_t> ReleaseToResult() {
    auto result = std::make_pair(account_, reserved_bytes_);
    reserved_bytes_ = 0;
    return result;
  }

  void Release() {
    if (account_ && reserved_bytes_ != 0) account_->Release(reserved_bytes_);
    reserved_bytes_ = 0;
  }

 private:
  std::shared_ptr<QueryMemoryAccount> account_;
  uint64_t reserved_bytes_ = 0;
};

StatusOr<std::unique_ptr<QueryResultStream>> BuildPathResultStream(
    const TcypherExecutionContext& context, std::vector<ResultBatch> batches,
    QueryMemoryReservation* reservation) {
  if (reservation == nullptr) {
    return Status::InvalidArgument("T-Cypher executor", "missing path memory reservation");
  }
  if (context.options.memory_account && context.options.memory_account->ShouldSpill()) {
    const std::string directory = context.options.spill_directory.empty()
        ? "/tmp" : context.options.spill_directory;
    auto spill = std::make_unique<QuerySpillFile>(
        directory, context.options.cancellation,
        context.options.spill_resource_extensions,
        context.options.memory_account);
    Status status = spill->Open();
    if (!status.ok()) return status;
    for (const ResultBatch& batch : batches) {
      status = spill->Append(batch);
      if (!status.ok()) return status;
    }
    reservation->Release();
    std::unique_ptr<QueryResultStream> stream =
        std::make_unique<SpillResultStream>(std::move(spill));
    return stream;
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  auto held = reservation->ReleaseToResult();
  if (held.first && held.second != 0) {
    stream = std::make_unique<MemoryAccountedResultStream>(
        std::move(stream), std::move(held.first), held.second);
  }
  return stream;
}

const char* LogicalOperatorName(LogicalOperatorKind kind) {
  switch (kind) {
    case LogicalOperatorKind::kTemporalScan: return "TemporalScan";
    case LogicalOperatorKind::kEventScan: return "EventScan";
    case LogicalOperatorKind::kChangeScan: return "ChangeScan";
    case LogicalOperatorKind::kIntervalDerive: return "IntervalDerive";
    case LogicalOperatorKind::kIntervalAlign: return "IntervalAlign";
    case LogicalOperatorKind::kTemporalCoalesce: return "TemporalCoalesce";
    case LogicalOperatorKind::kPropertyGather: return "PropertyGather";
    case LogicalOperatorKind::kFilter: return "Filter";
    case LogicalOperatorKind::kDistinct: return "Distinct";
    case LogicalOperatorKind::kAggregate: return "Aggregate";
    case LogicalOperatorKind::kSort: return "Sort";
    case LogicalOperatorKind::kExpand: return "Expand";
    case LogicalOperatorKind::kVariableExpand: return "VariableExpand";
    case LogicalOperatorKind::kJoin: return "Join";
    case LogicalOperatorKind::kProduceResult: return "ProduceResult";
  }
  return "Unknown";
}

std::string FormatLogicalPlan(const LogicalPlan& plan) {
  std::string text;
  for (size_t index = 0; index < plan.nodes.size(); ++index) {
    if (index != 0) text.append(" -> ");
    text.append(LogicalOperatorName(plan.nodes[index].kind));
  }
  return text;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainTcypherPlan(const LogicalPlan& plan,
                                                                 uint64_t snapshot_seq) {
  ColumnBatch batch(1);
  const Status added = batch.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::String(FormatLogicalPlan(plan))}, std::vector<bool>{}));
  if (!added.ok()) return added;
  std::unique_ptr<QueryResultStream> stream = std::make_unique<InMemoryResultStream>(
      std::vector<ResultBatch>{ResultBatch({"plan"}, std::move(batch),
                                           ResultTemporalMetadata{snapshot_seq, true, true})},
      Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainPhysicalTcypherPlan(
    const PhysicalPlan& plan, uint64_t snapshot_seq) {
  ColumnBatch batch(1);
  const Status added = batch.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::String(FormatPhysicalPlan(plan))}, std::vector<bool>{}));
  if (!added.ok()) return added;
  std::unique_ptr<QueryResultStream> stream = std::make_unique<InMemoryResultStream>(
      std::vector<ResultBatch>{ResultBatch({"plan"}, std::move(batch),
                                           ResultTemporalMetadata{snapshot_seq, true, true})},
      Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainPhysicalHashJoinTcypherPlan(
    const PhysicalHashJoinPlan& plan, uint64_t snapshot_seq) {
  ColumnBatch batch(1);
  const Status added = batch.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::String(FormatPhysicalHashJoinPlan(plan))},
      std::vector<bool>{}));
  if (!added.ok()) return added;
  std::unique_ptr<QueryResultStream> stream = std::make_unique<InMemoryResultStream>(
      std::vector<ResultBatch>{ResultBatch(
          {"plan"}, std::move(batch),
          ResultTemporalMetadata{snapshot_seq, true, true})},
      Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainPhysicalMultiHashJoinTcypherPlan(
    const PhysicalMultiHashJoinPlan& plan, uint64_t snapshot_seq) {
  ColumnBatch batch(1);
  const Status added = batch.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::String(FormatPhysicalMultiHashJoinPlan(plan))},
      std::vector<bool>{}));
  if (!added.ok()) return added;
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(
          std::vector<ResultBatch>{ResultBatch(
              {"plan"}, std::move(batch),
              ResultTemporalMetadata{snapshot_seq, true, true})},
          Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainAnalyzeTcypherPlan(
    const LogicalPlan& plan, uint64_t snapshot_seq, std::unique_ptr<QueryResultStream> input) {
  if (!input) return Status::InvalidArgument("T-Cypher executor", "missing analyzed result stream");
  uint64_t output_rows = 0;
  uint64_t result_batches = 0;
  for (;;) {
    ResultBatch batch;
    const Status next = input->Next(&batch);
    if (next.IsNotFound()) break;
    if (!next.ok()) return next;
    output_rows += batch.batch().row_count();
    ++result_batches;
  }
  const Status terminal = input->terminal_status();
  if (!terminal.ok()) return terminal;
  const std::string profile =
      "plan=" + FormatLogicalPlan(plan) + "\n" +
      "snapshot_seq=" + std::to_string(snapshot_seq) + "\n" +
      "result_batches=" + std::to_string(result_batches) + "\n" +
      "output_rows=" + std::to_string(output_rows) + "\n" +
      "terminal_status=" + terminal.ToString();
  ColumnBatch batch(1);
  const Status added = batch.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::String(profile)}, std::vector<bool>{}));
  if (!added.ok()) return added;
  std::unique_ptr<QueryResultStream> stream = std::make_unique<InMemoryResultStream>(
      std::vector<ResultBatch>{ResultBatch({"profile"}, std::move(batch),
                                           ResultTemporalMetadata{snapshot_seq, true, true})},
      Status::OK());
  return stream;
}

ExplainAnalyzeRuntimeProfile CaptureExplainProfile(
    const TcypherExecutionContext& context, uint64_t snapshot_seq) {
  ExplainAnalyzeRuntimeProfile profile;
  profile.snapshot_seq = snapshot_seq;
  if (context.version_snapshot) {
    profile.version_set_generation = context.version_snapshot->generation;
  }
  if (context.index_catalog_snapshot) {
    profile.catalog_generation = context.index_catalog_snapshot->catalog_generation;
    profile.statistics_snapshot_id =
        context.index_catalog_snapshot->statistics_snapshot_id;
  }
  if (context.statistics_snapshot) {
    profile.statistics_snapshot_id =
        context.statistics_snapshot->statistics_snapshot_id();
  }
  if (context.options.memory_account) {
    profile.memory_used_bytes = context.options.memory_account->used_bytes();
    profile.memory_peak_bytes = context.options.memory_account->peak_bytes();
    profile.memory_soft_limit_bytes =
        context.options.memory_account->soft_limit_bytes();
    profile.memory_hard_limit_bytes =
        context.options.memory_account->hard_limit_bytes();
  }
  return profile;
}

void FinalizeExplainProfile(
    ExplainAnalyzeRuntimeProfile* profile,
    const std::shared_ptr<TcypherExecutionStats>& stats,
    const std::shared_ptr<QueryMemoryAccount>& memory) {
  if (stats) {
    profile->executed_physical_plan_id = stats->executed_physical_plan_id;
    profile->base_events_visited = stats->base_events_visited;
    profile->sst_blocks_read = stats->sst_blocks_read;
    profile->sst_physical_bytes_read = stats->sst_physical_bytes_read;
    profile->page_bytes_decoded = stats->page_bytes_decoded;
    profile->page_bytes_skipped = stats->page_bytes_skipped;
    profile->page_decode_count = stats->page_decode_count;
    profile->page_decode_latency_ns = stats->page_decode_latency_ns;
    profile->root_sst_blocks_read = stats->root_sst_blocks_read;
    profile->boundary_sst_blocks_read = stats->boundary_sst_blocks_read;
    profile->projection_payload_bytes_copied =
        stats->projection_gather_payload_bytes_copied +
        stats->physical_project_payload_bytes_copied;
    profile->memtable_delta_probes = stats->memtable_delta_probes;
    profile->memtable_delta_candidates = stats->memtable_delta_candidates;
    profile->index_candidate_items_processed =
        stats->index_candidate_items_processed;
    profile->index_advisory_fallbacks = stats->index_advisory_fallbacks;
    profile->index_sidecar_bytes_read = stats->index_advisory_sidecar_bytes_read;
    if (stats->runtime_feedback_key.has_value()) {
      profile->has_runtime_feedback = true;
      profile->runtime_feedback_bucket =
          stats->runtime_feedback_key->selectivity_bucket;
    }
    profile->runtime_feedback_observations =
        stats->runtime_feedback_observations;
    profile->runtime_feedback_applied = stats->runtime_feedback_applied;
    profile->runtime_feedback_source = stats->runtime_feedback_source;
    profile->runtime_feedback_base_rows = stats->runtime_feedback_base_rows;
    profile->runtime_feedback_candidate_rows =
        stats->runtime_feedback_candidate_rows;
    profile->index_adaptive_reoptimizations =
        stats->index_adaptive_reoptimizations;
    profile->index_adaptive_intersection_predicates_dropped =
        stats->index_adaptive_intersection_predicates_dropped;
    profile->index_adaptive_unopened_fragments_skipped =
        stats->index_adaptive_unopened_fragments_skipped;
    profile->index_adaptive_unopened_delta_sources_skipped =
        stats->index_adaptive_unopened_delta_sources_skipped;
    profile->index_adaptive_sampled_candidates =
        stats->index_adaptive_sampled_candidates;
    profile->index_dynamic_filter_input_rows =
        stats->index_dynamic_filter_input_rows;
    profile->index_dynamic_filter_rejected_rows =
        stats->index_dynamic_filter_rejected_rows;
    profile->index_dynamic_filter_output_rows =
        stats->index_dynamic_filter_output_rows;
    profile->has_selected_access_path = stats->has_selected_access_path;
    profile->selected_access_path = stats->selected_access_path;
    profile->has_executed_access_path = stats->has_executed_access_path;
    profile->executed_access_path = stats->executed_access_path;
    profile->selected_access_path_score = stats->selected_access_path_score;
    profile->selected_access_path_cost = stats->selected_access_path_cost;
    profile->selected_access_path_rationale =
        stats->selected_access_path_rationale;
    profile->access_path_fallback = stats->access_path_fallback;
    profile->has_selected_graph_order = stats->has_selected_graph_order;
    profile->selected_graph_order = stats->selected_graph_order;
    profile->has_executed_graph_order = stats->has_executed_graph_order;
    profile->executed_graph_order = stats->executed_graph_order;
    profile->pipelines_built = stats->pipeline_builds;
    profile->morsels_scheduled = stats->morsels_scheduled;
    profile->morsels_completed = stats->morsels_completed;
    profile->scheduler_dispatches = stats->scheduler_dispatches;
    profile->result_queue_high_water = stats->result_queue_high_water;
    profile->physical_output_rows = stats->physical_output_rows;
    profile->pipeline_reoptimization_checks =
        stats->pipeline_reoptimization_checks;
    profile->pipeline_reoptimizations = stats->pipeline_reoptimizations;
    profile->pipeline_reoptimization_sampled_rows =
        stats->pipeline_reoptimization_sampled_rows;
    profile->pipeline_reoptimization_sampled_batches =
        stats->pipeline_reoptimization_sampled_batches;
    profile->pipeline_reoptimization_prefix_memory_bytes =
        stats->pipeline_reoptimization_prefix_memory_bytes;
    profile->path_frontier_hops = stats->path_frontier_hops;
    profile->path_frontier_input_states =
        stats->path_frontier_input_states;
    profile->path_frontier_output_states =
        stats->path_frontier_output_states;
    profile->path_frontier_completed_paths =
        stats->path_frontier_completed_paths;
    profile->path_frontier_repartitions =
        stats->path_frontier_repartitions;
    profile->path_frontier_partitions = stats->path_frontier_partitions;
    profile->path_frontier_max_partition_size =
        stats->path_frontier_max_partition_size;
    profile->path_frontier_spill_starts =
        stats->path_frontier_spill_starts;
    profile->path_frontier_spill_bytes =
        stats->path_frontier_spill_bytes;
    profile->hash_join_build_input_rows = stats->hash_join_build_input_rows;
    profile->hash_join_probe_input_rows = stats->hash_join_probe_input_rows;
    profile->hash_join_spill_starts = stats->hash_join_spill_starts;
    profile->hash_join_dynamic_filter_input_rows =
        stats->hash_join_dynamic_filter_input_rows;
    profile->hash_join_dynamic_filter_rejected_rows =
        stats->hash_join_dynamic_filter_rejected_rows;
    profile->hash_join_dynamic_filter_output_rows =
        stats->hash_join_dynamic_filter_output_rows;
    profile->hash_join_dynamic_filter_spill_rows_avoided =
        stats->hash_join_dynamic_filter_spill_rows_avoided;
    profile->hash_join_dynamic_filter_memory_bytes =
        stats->hash_join_dynamic_filter_memory_bytes;
    profile->hash_join_dynamic_filter_memory_disables =
        stats->hash_join_dynamic_filter_memory_disables;
    profile->hash_join_build_side_switches =
        stats->hash_join_build_side_switches;
    if (stats->operator_runtime) {
      profile->operator_counters = stats->operator_runtime->Snapshot();
    }
  }
  if (memory) {
    profile->memory_used_bytes = memory->used_bytes();
    profile->memory_peak_bytes = memory->peak_bytes();
    profile->memory_soft_limit_bytes = memory->soft_limit_bytes();
    profile->memory_hard_limit_bytes = memory->hard_limit_bytes();
  }
}

template <typename Plan>
StatusOr<std::unique_ptr<QueryResultStream>> ExplainAnalyzePhysicalPlan(
    const Plan& plan, std::unique_ptr<QueryResultStream> input,
    std::shared_ptr<TcypherExecutionStats> stats,
    std::shared_ptr<QueryMemoryAccount> memory,
    ExplainAnalyzeRuntimeProfile profile,
    std::string (*serialize)(const Plan&, const ExplainAnalyzeRuntimeProfile&)) {
  if (!input) {
    return Status::InvalidArgument("T-Cypher executor", "missing analyzed result stream");
  }
  for (;;) {
    ResultBatch batch;
    const Status next = input->Next(&batch);
    if (next.IsNotFound()) break;
    if (!next.ok()) return next;
    profile.output_rows += batch.batch().row_count();
    ++profile.result_batches;
  }
  const Status terminal = input->terminal_status();
  profile.terminal_status = terminal.ToString();
  if (!terminal.ok()) return terminal;
  FinalizeExplainProfile(&profile, stats, memory);
  const std::string serialized = serialize(plan, profile);
  ColumnBatch batch(1);
  const Status added = batch.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::String(serialized)}, std::vector<bool>{}));
  if (!added.ok()) return added;
  std::unique_ptr<QueryResultStream> stream = std::make_unique<InMemoryResultStream>(
      std::vector<ResultBatch>{ResultBatch(
          {"profile"}, std::move(batch),
          ResultTemporalMetadata{profile.snapshot_seq, true, true})},
      Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainAnalyzePhysicalTcypherPlan(
    const PhysicalPlan& plan, uint64_t snapshot_seq,
    std::unique_ptr<QueryResultStream> input,
    const std::shared_ptr<TcypherExecutionStats>& stats,
    const std::shared_ptr<QueryMemoryAccount>& memory,
    ExplainAnalyzeRuntimeProfile profile) {
  return ExplainAnalyzePhysicalPlan<PhysicalPlan>(
      plan, std::move(input), stats, memory, std::move(profile),
      &SerializeExplainAnalyzeProfile);
}

StatusOr<std::unique_ptr<QueryResultStream>> ExplainAnalyzePhysicalHashJoinTcypherPlan(
    const PhysicalHashJoinPlan& plan, uint64_t snapshot_seq,
    std::unique_ptr<QueryResultStream> input,
    const std::shared_ptr<TcypherExecutionStats>& stats,
    const std::shared_ptr<QueryMemoryAccount>& memory,
    ExplainAnalyzeRuntimeProfile profile) {
  return ExplainAnalyzePhysicalPlan<PhysicalHashJoinPlan>(
      plan, std::move(input), stats, memory, std::move(profile),
      &SerializeExplainAnalyzeProfile);
}

StatusOr<std::unique_ptr<QueryResultStream>>
ExplainAnalyzePhysicalMultiHashJoinTcypherPlan(
    const PhysicalMultiHashJoinPlan& plan, uint64_t snapshot_seq,
    std::unique_ptr<QueryResultStream> input,
    const std::shared_ptr<TcypherExecutionStats>& stats,
    const std::shared_ptr<QueryMemoryAccount>& memory,
    ExplainAnalyzeRuntimeProfile profile) {
  return ExplainAnalyzePhysicalPlan<PhysicalMultiHashJoinPlan>(
      plan, std::move(input), stats, memory, std::move(profile),
      &SerializeExplainAnalyzeProfile);
}

StatusOr<std::unique_ptr<QueryResultStream>> ApplyResultLimit(
    const TcypherStatement& statement, std::unique_ptr<QueryResultStream> stream,
    uint32_t batch_capacity, std::shared_ptr<QueryCancellation> cancellation = nullptr,
    std::shared_ptr<QueryMemoryAccount> memory_account = nullptr,
    bool aggregate_already_applied = false,
    bool distinct_already_applied = false,
    bool sort_already_applied = false,
    std::string spill_directory = {},
    std::shared_ptr<ResourceGovernorExtension> spill_resources = nullptr) {
  if (!stream) return Status::InvalidArgument("T-Cypher executor", "missing result stream");
  const auto collect = std::find_if(statement.returns.begin(), statement.returns.end(),
      [](const ReturnExpression& expression) {
        return expression.kind == ReturnExpressionKind::kCollect;
  });
  if (collect != statement.returns.end() && !aggregate_already_applied) {
    const size_t collect_count = std::count_if(statement.returns.begin(), statement.returns.end(),
        [](const ReturnExpression& expression) {
          return expression.kind == ReturnExpressionKind::kCollect;
        });
    const bool has_other_aggregate = std::any_of(statement.returns.begin(), statement.returns.end(),
        [](const ReturnExpression& expression) {
          return expression.kind == ReturnExpressionKind::kCount ||
                 expression.kind == ReturnExpressionKind::kSum ||
                 expression.kind == ReturnExpressionKind::kAvg ||
                 expression.kind == ReturnExpressionKind::kMin ||
                 expression.kind == ReturnExpressionKind::kMax;
        });
    if (collect_count != 1 || has_other_aggregate) {
      return Status::NotSupported("T-Cypher executor",
                                  "COLLECT cannot be mixed with other aggregates yet");
    }
    const std::string name = "collect(" + collect->variable +
        (collect->property_name.empty() ? "" : "." + collect->property_name) + ")";
    ResultValueKind collect_kind = ResultValueKind::kScalar;
    if (collect->property_name.empty()) {
      const auto relationship = std::find_if(
          statement.relationships.begin(), statement.relationships.end(),
          [&collect](const MatchRelationshipPattern& candidate) {
            return candidate.variable == collect->variable;
          });
      if (relationship != statement.relationships.end()) {
        collect_kind = relationship->variable_length
            ? ResultValueKind::kList : ResultValueKind::kStruct;
      }
    }
    const uint32_t collect_column = static_cast<uint32_t>(collect - statement.returns.begin());
    if (statement.returns.size() == 1) {
      std::unique_ptr<QueryResultStream> collected =
          std::make_unique<CollectResultStream>(
              std::move(stream), name, cancellation, memory_account,
              collect_kind);
      return collected;
    }
    std::vector<uint32_t> group_columns;
    std::vector<ResultOutputSlot> output_slots;
    group_columns.reserve(statement.returns.size() - 1);
    output_slots.reserve(statement.returns.size());
    uint32_t group_index = 0;
    for (uint32_t column = 0; column < statement.returns.size(); ++column) {
      if (column == collect_column) {
        output_slots.push_back(ResultOutputSlot{true, 0});
      } else {
        group_columns.push_back(column);
        output_slots.push_back(ResultOutputSlot{false, group_index++});
      }
    }
    std::unique_ptr<QueryResultStream> collected = std::make_unique<GroupedCollectResultStream>(
        std::move(stream), std::move(group_columns), collect_column, name,
        std::move(output_slots), batch_capacity, cancellation, memory_account,
        spill_directory, spill_resources, collect_kind);
    return collected;
  }
  const auto aggregate = std::find_if(statement.returns.begin(), statement.returns.end(),
      [](const ReturnExpression& expression) {
        return expression.kind == ReturnExpressionKind::kCount ||
               expression.kind == ReturnExpressionKind::kSum ||
               expression.kind == ReturnExpressionKind::kAvg ||
               expression.kind == ReturnExpressionKind::kMin ||
               expression.kind == ReturnExpressionKind::kMax;
      });
  if (aggregate != statement.returns.end() && !aggregate_already_applied) {
    const size_t aggregate_count = std::count_if(statement.returns.begin(), statement.returns.end(),
        [](const ReturnExpression& expression) {
          return expression.kind == ReturnExpressionKind::kCount ||
                 expression.kind == ReturnExpressionKind::kSum ||
                 expression.kind == ReturnExpressionKind::kAvg ||
                 expression.kind == ReturnExpressionKind::kMin ||
                 expression.kind == ReturnExpressionKind::kMax;
        });
    if (aggregate_count != 1) {
      std::vector<ResultAggregateSpec> aggregates;
      aggregates.reserve(aggregate_count);
      for (uint32_t column = 0; column < statement.returns.size(); ++column) {
        const ReturnExpression& expression = statement.returns[column];
        if (expression.kind != ReturnExpressionKind::kCount &&
            expression.kind != ReturnExpressionKind::kSum &&
            expression.kind != ReturnExpressionKind::kAvg &&
            expression.kind != ReturnExpressionKind::kMin &&
            expression.kind != ReturnExpressionKind::kMax) {
          continue;
        }
        const std::string name = expression.kind == ReturnExpressionKind::kCount
            ? "count(" + expression.variable + ")"
            : std::string(expression.kind == ReturnExpressionKind::kSum ? "sum(" :
                          expression.kind == ReturnExpressionKind::kAvg ? "avg(" :
                          expression.kind == ReturnExpressionKind::kMin ? "min(" : "max(") +
              expression.variable + "." + expression.property_name + ")";
        ResultAggregateKind kind;
        switch (expression.kind) {
          case ReturnExpressionKind::kCount: kind = ResultAggregateKind::kCount; break;
          case ReturnExpressionKind::kSum: kind = ResultAggregateKind::kSum; break;
          case ReturnExpressionKind::kAvg: kind = ResultAggregateKind::kAvg; break;
          case ReturnExpressionKind::kMin: kind = ResultAggregateKind::kMin; break;
          case ReturnExpressionKind::kMax: kind = ResultAggregateKind::kMax; break;
          default:
            return Status::Corruption("T-Cypher executor", "non-aggregate multi-aggregate input");
        }
        aggregates.push_back(ResultAggregateSpec{kind, column, name});
      }
      if (aggregate_count == statement.returns.size()) {
        stream = std::make_unique<MultiAggregateResultStream>(std::move(stream),
                                                               std::move(aggregates));
      } else {
        std::vector<uint32_t> group_columns;
        std::vector<ResultOutputSlot> output_slots;
        group_columns.reserve(statement.returns.size() - aggregate_count);
        output_slots.reserve(statement.returns.size());
        uint32_t group_index = 0;
        uint32_t aggregate_index = 0;
        for (uint32_t column = 0; column < statement.returns.size(); ++column) {
          const ReturnExpressionKind kind = statement.returns[column].kind;
          const bool is_aggregate = kind == ReturnExpressionKind::kCount ||
              kind == ReturnExpressionKind::kSum || kind == ReturnExpressionKind::kAvg ||
              kind == ReturnExpressionKind::kMin || kind == ReturnExpressionKind::kMax;
          if (is_aggregate) {
            output_slots.push_back(ResultOutputSlot{true, aggregate_index++});
          } else {
            group_columns.push_back(column);
            output_slots.push_back(ResultOutputSlot{false, group_index++});
          }
        }
        stream = std::make_unique<GroupedMultiAggregateResultStream>(
            std::move(stream), std::move(group_columns), std::move(aggregates),
            std::move(output_slots), batch_capacity, cancellation,
            memory_account, spill_directory, spill_resources);
      }
    } else {
    const uint32_t aggregate_column = static_cast<uint32_t>(aggregate - statement.returns.begin());
    const ReturnExpression& expression = *aggregate;
    const std::string name = expression.kind == ReturnExpressionKind::kCount
        ? "count(" + expression.variable + ")"
        : std::string(expression.kind == ReturnExpressionKind::kSum ? "sum(" :
                      expression.kind == ReturnExpressionKind::kAvg ? "avg(" :
                      expression.kind == ReturnExpressionKind::kMin ? "min(" : "max(") +
          expression.variable + "." + expression.property_name + ")";
    if (statement.returns.size() == 1) {
      switch (expression.kind) {
        case ReturnExpressionKind::kCount:
          stream = std::make_unique<CountResultStream>(std::move(stream), name);
          break;
        case ReturnExpressionKind::kSum:
          stream = std::make_unique<SumResultStream>(std::move(stream), name);
          break;
        case ReturnExpressionKind::kAvg:
          stream = std::make_unique<AvgResultStream>(std::move(stream), name);
          break;
        case ReturnExpressionKind::kMin:
        case ReturnExpressionKind::kMax:
          stream = std::make_unique<ExtremaResultStream>(std::move(stream), name,
              expression.kind == ReturnExpressionKind::kMin);
          break;
        default:
          break;
      }
    } else {
      std::vector<uint32_t> group_columns;
      group_columns.reserve(statement.returns.size() - 1);
      std::vector<ResultOutputSlot> output_slots;
      output_slots.reserve(statement.returns.size());
      uint32_t group_index = 0;
      for (uint32_t column = 0; column < statement.returns.size(); ++column) {
        if (column == aggregate_column) {
          output_slots.push_back(ResultOutputSlot{true, 0});
        } else {
          group_columns.push_back(column);
          output_slots.push_back(ResultOutputSlot{false, group_index++});
        }
      }
      ResultAggregateKind aggregate_kind;
      switch (expression.kind) {
        case ReturnExpressionKind::kCount: aggregate_kind = ResultAggregateKind::kCount; break;
        case ReturnExpressionKind::kSum: aggregate_kind = ResultAggregateKind::kSum; break;
        case ReturnExpressionKind::kAvg: aggregate_kind = ResultAggregateKind::kAvg; break;
        case ReturnExpressionKind::kMin: aggregate_kind = ResultAggregateKind::kMin; break;
        case ReturnExpressionKind::kMax: aggregate_kind = ResultAggregateKind::kMax; break;
        default:
          return Status::Corruption("T-Cypher executor", "unknown grouped aggregate kind");
      }
      stream = std::make_unique<GroupedMultiAggregateResultStream>(
          std::move(stream), std::move(group_columns),
          std::vector<ResultAggregateSpec>{{aggregate_kind, aggregate_column, name}},
          std::move(output_slots), batch_capacity, cancellation, memory_account,
          spill_directory, spill_resources);
    }
    }
  }
  if (statement.distinct && !distinct_already_applied) {
    stream = std::make_unique<DistinctResultStream>(
        std::move(stream), cancellation, memory_account, spill_directory,
        spill_resources);
  }
  if (statement.order_by.has_value() && !sort_already_applied) {
    const OrderByTerm& order = *statement.order_by;
    uint32_t column = 0;
    while (column < statement.returns.size() &&
           (statement.returns[column].variable != order.variable ||
            statement.returns[column].property_name != order.property_name)) ++column;
    if (column == statement.returns.size()) {
      return Status::BindError("T-Cypher executor", "ORDER BY property must be projected");
    }
    stream = std::make_unique<SortResultStream>(std::move(stream), column, order.descending,
                                                batch_capacity, cancellation, memory_account,
                                                std::move(spill_directory), spill_resources);
  }
  if (statement.skip.has_value()) {
    stream = std::make_unique<SkipResultStream>(std::move(stream), *statement.skip);
  }
  if (statement.limit.has_value()) {
    stream = std::make_unique<LimitedResultStream>(std::move(stream), *statement.limit);
  }
  if (cancellation) {
    stream = std::make_unique<CancellableResultStream>(std::move(stream), std::move(cancellation));
  }
  return stream;
}

// Change scans emit raw bindings and values; the result pipeline owns aggregate reduction.
TcypherStatement AggregateInputStatement(const TcypherStatement& statement) {
  TcypherStatement input = statement;
  for (ReturnExpression& expression : input.returns) {
    if (expression.kind == ReturnExpressionKind::kCount) {
      expression.kind = ReturnExpressionKind::kBinding;
      expression.variable = input.match.variable;
      expression.property_name.clear();
    } else if (expression.kind == ReturnExpressionKind::kSum ||
               expression.kind == ReturnExpressionKind::kAvg ||
               expression.kind == ReturnExpressionKind::kMin ||
               expression.kind == ReturnExpressionKind::kMax) {
      expression.kind = ReturnExpressionKind::kProperty;
    } else if (expression.kind == ReturnExpressionKind::kCollect) {
      expression.kind = expression.property_name.empty()
          ? ReturnExpressionKind::kBinding
          : ReturnExpressionKind::kProperty;
    }
  }
  return input;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteVariableExpandAsOf(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t valid_time, uint64_t snapshot_seq) {
  if (statement.relationships.size() != 1 || statement.expanded_nodes.size() != 1) {
    return Status::NotSupported("T-Cypher executor", "only one variable relationship is available");
  }
  const MatchRelationshipPattern& relationship = statement.relationships.front();
  const MatchNodePattern& target = statement.expanded_nodes.front();
  if (!relationship.variable_length) {
    return Status::NotSupported("T-Cypher executor", "variable relationship is required");
  }
  if (!target.label.empty()) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->logical_type != target.label) {
      return Status::BindError("T-Cypher executor", "target vertex label is not registered");
    }
  }
  const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
                                   ? EntityType::EdgeOut
                                   : EntityType::EdgeIn;
  std::optional<uint16_t> edge_type;
  if (!relationship.type.empty()) {
    const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "relationship type is not registered");
    }
    edge_type = schema->column_id;
  }
  struct Projection {
    ReturnExpressionKind kind;
    std::string variable;
    uint16_t property_column = 0;
    std::string name;
  };
  std::vector<Projection> projections;
  projections.reserve(statement.returns.size());
  for (const ReturnExpression& expression : statement.returns) {
    const auto relationship = std::find_if(
        statement.relationships.begin(), statement.relationships.end(),
        [&expression](const MatchRelationshipPattern& candidate) {
          return candidate.variable == expression.variable;
        });
    if (relationship != statement.relationships.end() &&
        relationship->variable_length &&
        expression.kind == ReturnExpressionKind::kProperty) {
      return Status::NotSupported(
          "T-Cypher executor",
          "property access on a variable relationship path is not supported");
    }
    if (expression.variable != statement.match.variable && expression.variable != target.variable) {
      return Status::NotSupported("T-Cypher executor",
                                  "variable path projections are not available yet");
    }
    Projection projection{expression.kind, expression.variable, 0, expression.variable};
    if (expression.kind == ReturnExpressionKind::kProperty ||
        expression.kind == ReturnExpressionKind::kSum ||
        expression.kind == ReturnExpressionKind::kAvg ||
        expression.kind == ReturnExpressionKind::kMin ||
        expression.kind == ReturnExpressionKind::kMax) {
      const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                           expression.property_name);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "projected property is not registered");
      }
      projection.property_column = schema->column_id;
      projection.name = expression.kind == ReturnExpressionKind::kSum
          ? "sum(" + expression.variable + "." + expression.property_name + ")"
          : expression.kind == ReturnExpressionKind::kAvg
              ? "avg(" + expression.variable + "." + expression.property_name + ")"
              : expression.kind == ReturnExpressionKind::kMin
                  ? "min(" + expression.variable + "." + expression.property_name + ")"
                  : expression.kind == ReturnExpressionKind::kMax
                      ? "max(" + expression.variable + "." + expression.property_name + ")"
                      : expression.variable + "." + expression.property_name;
    } else if (expression.kind != ReturnExpressionKind::kBinding &&
               expression.kind != ReturnExpressionKind::kCount) {
      return Status::NotSupported("T-Cypher executor", "unsupported variable path projection");
    }
    projections.push_back(std::move(projection));
  }

  QueryMemoryReservation memory(context.options.memory_account);

  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  std::set<uint64_t> vertices;
  struct Edge {
    uint64_t source_id;
    uint64_t target_id;
    LogicalKey identity;
  };
  std::vector<Edge> edges;
  for (const auto& entry : events_by_key) {
    const LogicalKey& key = entry.first;
    const auto visible = ResolveVisibleEvent(entry.second, key, valid_time, snapshot_seq);
    if (!visible.has_value() || visible->is_delete()) continue;
    if (key.entity_type() == EntityType::Vertex && key.kind() == LogicalKeyKind::kExistence) {
      vertices.insert(key.entity_id());
    } else if (key.entity_type() == direction &&
               key.kind() == LogicalKeyKind::kExistence &&
               (!edge_type.has_value() || key.edge_type() == *edge_type)) {
      const Status reserved = memory.Reserve(sizeof(Edge));
      if (!reserved.ok()) return reserved;
      edges.push_back(Edge{key.entity_id(), key.target_id(), key});
    }
  }
  std::vector<Edge> visible_edges;
  for (const Edge& edge : edges) {
    if (vertices.count(edge.source_id) != 0 && vertices.count(edge.target_id) != 0) {
      const Status reserved = memory.Reserve(sizeof(Edge));
      if (!reserved.ok()) return reserved;
      visible_edges.push_back(edge);
    }
  }

  struct PathState {
    uint64_t start_id;
    uint64_t endpoint_id;
    uint32_t hops;
    std::set<LogicalKey> visited_edges;
  };
  std::vector<PathState> frontier;
  for (uint64_t vertex : vertices) {
    const Status reserved = memory.Reserve(sizeof(PathState));
    if (!reserved.ok()) return reserved;
    frontier.push_back(PathState{vertex, vertex, 0, {}});
  }
  std::vector<std::pair<uint64_t, uint64_t>> results;
  while (!frontier.empty()) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    std::vector<PathState> next_frontier;
    for (const PathState& state : frontier) {
      if (state.hops == relationship.max_hops) continue;
      for (const Edge& edge : visible_edges) {
        if (edge.source_id != state.endpoint_id || state.visited_edges.count(edge.identity) != 0) {
          continue;
        }
        PathState next = state;
        next.endpoint_id = edge.target_id;
        ++next.hops;
        next.visited_edges.insert(edge.identity);
        if (next.hops >= relationship.min_hops) {
          const Status reserved = memory.Reserve(sizeof(std::pair<uint64_t, uint64_t>));
          if (!reserved.ok()) return reserved;
          results.emplace_back(next.start_id, next.endpoint_id);
        }
        if (next.hops < relationship.max_hops) {
          const Status reserved = memory.Reserve(
              sizeof(PathState) + next.visited_edges.size() * sizeof(LogicalKey));
          if (!reserved.ok()) return reserved;
          next_frontier.push_back(std::move(next));
        }
      }
    }
    frontier = std::move(next_frontier);
  }
  std::sort(results.begin(), results.end());

  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < results.size(); begin += context.options.batch_capacity) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const size_t end = std::min(results.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(statement.returns.size());
    std::vector<std::vector<bool>> validity(statement.returns.size());
    for (std::vector<Value>& column : columns) column.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
      for (size_t output = 0; output < projections.size(); ++output) {
        const Projection& projection = projections[output];
        const uint64_t id = projection.variable == statement.match.variable
                                ? results[index].first
                                : results[index].second;
        if (projection.kind == ReturnExpressionKind::kBinding ||
            projection.kind == ReturnExpressionKind::kCount) {
          if (id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
          }
          columns[output].push_back(Value::Int64(static_cast<int64_t>(id)));
        } else {
          const LogicalKey key = LogicalKey::VertexProperty(id, projection.property_column);
          auto value = ResolveQueryValue(
              events_by_key, key, valid_time, snapshot_seq, context);
          if (!value.ok()) return value.status();
          columns[output].push_back(
              value.ValueOrDie().value_or(Value::Binary("")));
          validity[output].push_back(value.ValueOrDie().has_value());
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    std::vector<std::string> names;
    names.reserve(statement.returns.size());
    for (size_t output = 0; output < columns.size(); ++output) {
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(columns[output]), std::move(validity[output])));
      if (!added.ok()) return added;
      names.push_back(projections[output].name);
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  return BuildPathResultStream(context, std::move(batches), &memory);
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteFixedExpandChainAsOf(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t valid_time, uint64_t snapshot_seq) {
  if (statement.relationships.size() < 2 ||
      statement.relationships.size() != statement.expanded_nodes.size()) {
    return Status::InvalidArgument("T-Cypher executor", "invalid fixed relationship chain");
  }
  for (size_t index = 0; index < statement.relationships.size(); ++index) {
    if (statement.relationships[index].variable_length) {
      return Status::NotSupported("T-Cypher executor",
                                  "multi-hop fixed expansion does not support variable paths");
    }
    if (!statement.expanded_nodes[index].label.empty()) {
      const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
      if (!existence.has_value() || existence->logical_type != statement.expanded_nodes[index].label) {
        return Status::BindError("T-Cypher executor", "target vertex label is not registered");
      }
    }
  }
  struct EdgeBinding {
    uint64_t source_id = 0;
    uint64_t target_id = 0;
    uint64_t edge_id = 0;
    uint16_t edge_type = 0;
    uint64_t valid_from = 0;
    uint64_t commit_seq = 0;
    EntityType direction = EntityType::EdgeOut;
  };
  struct ChainRow {
    std::vector<uint64_t> nodes;
    std::vector<EdgeBinding> edges;
  };
  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  const auto vertex_visible = [&](uint64_t vertex_id) -> std::optional<TemporalEvent> {
    const LogicalKey key = LogicalKey::VertexExistence(vertex_id);
    const auto found = events_by_key.find(key);
    if (found == events_by_key.end()) return std::nullopt;
    const auto event = ResolveVisibleEvent(found->second, key, valid_time, snapshot_seq);
    return event.has_value() && !event->is_delete() ? event : std::nullopt;
  };
  std::optional<uint64_t> exact_root;
  if (statement.match.entity_id.has_value()) {
    const auto resolved = ResolveExactEntityId(statement.match, context.options);
    if (!resolved.ok()) return resolved.status();
    exact_root = resolved.ValueOrDie();
  }
  std::optional<uint16_t> predicate_column;
  if (statement.where.has_value()) {
    if (statement.where->variable != statement.match.variable) {
      return Status::NotSupported("T-Cypher executor",
                                  "multi-hop WHERE supports only root vertex properties");
    }
    const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                         statement.where->property_name);
    if (!schema.has_value() || schema->physical_type != PhysicalType::kString) {
      return Status::SchemaMismatch("T-Cypher executor", "WHERE property must be a string");
    }
    predicate_column = schema->column_id;
  }
  std::vector<ChainRow> rows;
  for (const auto& entry : events_by_key) {
    if (entry.first.entity_type() != EntityType::Vertex ||
        entry.first.kind() != LogicalKeyKind::kExistence ||
        (exact_root.has_value() && entry.first.entity_id() != *exact_root)) {
      continue;
    }
    const auto root = vertex_visible(entry.first.entity_id());
    if (!root.has_value()) continue;
    if (predicate_column.has_value()) {
      const LogicalKey key = LogicalKey::VertexProperty(entry.first.entity_id(), *predicate_column);
      auto value = ResolveQueryValue(
          events_by_key, key, valid_time, snapshot_seq, context);
      if (!value.ok()) return value.status();
      if (!value.ValueOrDie().has_value() ||
          *value.ValueOrDie() != Value::String(statement.where->string_value)) {
        continue;
      }
    }
    rows.push_back(ChainRow{{entry.first.entity_id()}, {}});
  }
  for (size_t hop = 0; hop < statement.relationships.size(); ++hop) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const MatchRelationshipPattern& relationship = statement.relationships[hop];
    const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
        ? EntityType::EdgeOut : EntityType::EdgeIn;
    std::optional<uint16_t> edge_type;
    if (!relationship.type.empty()) {
      const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "relationship type is not registered");
      }
      edge_type = schema->column_id;
    }
    std::vector<ChainRow> expanded;
    for (const ChainRow& row : rows) {
      const uint64_t source_id = row.nodes.back();
      for (const auto& edge_entry : events_by_key) {
        const LogicalKey& edge_key = edge_entry.first;
        if (edge_key.entity_type() != direction || edge_key.kind() != LogicalKeyKind::kExistence ||
            edge_key.entity_id() != source_id ||
            (edge_type.has_value() && edge_key.edge_type() != *edge_type)) {
          continue;
        }
        const auto edge = ResolveVisibleEvent(edge_entry.second, edge_key, valid_time, snapshot_seq);
        if (!edge.has_value() || edge->is_delete() || !vertex_visible(edge_key.target_id()).has_value()) {
          continue;
        }
        ChainRow next = row;
        next.nodes.push_back(edge_key.target_id());
        next.edges.push_back(EdgeBinding{edge_key.entity_id(), edge_key.target_id(), edge_key.edge_id(),
                                         edge_key.edge_type(), edge->valid_from(), edge->commit_seq(),
                                         direction});
        expanded.push_back(std::move(next));
      }
    }
    rows = std::move(expanded);
    if (rows.empty()) break;
  }
  std::map<std::string, size_t> node_positions;
  node_positions.emplace(statement.match.variable, 0);
  for (size_t index = 0; index < statement.expanded_nodes.size(); ++index) {
    if (!node_positions.emplace(statement.expanded_nodes[index].variable, index + 1).second) {
      return Status::BindError("T-Cypher executor", "duplicate node variable in relationship chain");
    }
  }
  std::map<std::string, size_t> edge_positions;
  for (size_t index = 0; index < statement.relationships.size(); ++index) {
    if (statement.relationships[index].variable.empty() ||
        !edge_positions.emplace(statement.relationships[index].variable, index).second) {
      return Status::BindError("T-Cypher executor", "each multi-hop relationship requires a unique variable");
    }
  }
  struct Projection {
    ReturnExpressionKind kind;
    std::string variable;
    std::optional<uint16_t> property_column;
    std::string name;
  };
  std::vector<Projection> projections;
  for (const ReturnExpression& expression : statement.returns) {
    const bool node = node_positions.count(expression.variable) != 0;
    const bool edge = edge_positions.count(expression.variable) != 0;
    if (!node && !edge) return Status::BindError("T-Cypher executor", "return variable is not owned by chain");
    Projection projection{expression.kind, expression.variable, std::nullopt, expression.variable};
    if (!expression.property_name.empty()) {
      const EntityType entity = edge ? statement.relationships[edge_positions.at(expression.variable)].direction ==
          RelationshipDirection::kOutgoing ? EntityType::EdgeOut : EntityType::EdgeIn : EntityType::Vertex;
      const auto schema = FindColumnByName(*context.schema_snapshot, entity, expression.property_name);
      if (!schema.has_value()) return Status::BindError("T-Cypher executor", "projected property is not registered");
      projection.property_column = schema->column_id;
      projection.name = expression.variable + "." + expression.property_name;
    } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
      projection.name = "valid_from(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
      projection.name = "commit_seq(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
      projection.name = "system_time(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kCount) {
      projection.name = "count(" + expression.variable + ")";
    } else if (expression.kind != ReturnExpressionKind::kBinding) {
      return Status::NotSupported("T-Cypher executor", "unsupported multi-hop fixed projection");
    }
    projections.push_back(std::move(projection));
  }
  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < rows.size(); begin += context.options.batch_capacity) {
    const size_t end = std::min(rows.size(), begin + context.options.batch_capacity);
    ColumnBatch batch(context.options.batch_capacity);
    for (const Projection& projection : projections) {
      const auto node = node_positions.find(projection.variable);
      const auto edge = edge_positions.find(projection.variable);
      if (projection.kind == ReturnExpressionKind::kBinding && edge != edge_positions.end()) {
        std::vector<StructValue> values;
        values.reserve(end - begin);
        for (size_t row = begin; row < end; ++row) {
          const EdgeBinding& binding = rows[row].edges[edge->second];
          values.push_back(StructValue{{
              StructField{"source_id", Value::Int64(static_cast<int64_t>(binding.source_id))},
              StructField{"target_id", Value::Int64(static_cast<int64_t>(binding.target_id))},
              StructField{"edge_type", Value::Int32(static_cast<int32_t>(binding.edge_type))},
              StructField{"edge_id", Value::Int64(static_cast<int64_t>(binding.edge_id))},
              StructField{"valid_from", Value::Timestamp(binding.valid_from)},
              StructField{"commit_seq", Value::Int64(static_cast<int64_t>(binding.commit_seq))},
          }});
        }
        const Status added = batch.AddVector(
            std::make_shared<StructVector>(std::move(values), std::vector<bool>{}));
        if (!added.ok()) return added;
        continue;
      }
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(end - begin);
      validity.reserve(end - begin);
      for (size_t row = begin; row < end; ++row) {
        const ChainRow& chain = rows[row];
        uint64_t entity_id = 0;
        std::optional<EdgeBinding> binding;
        if (node != node_positions.end()) entity_id = chain.nodes[node->second];
        if (edge != edge_positions.end()) binding = chain.edges[edge->second];
        if (projection.kind == ReturnExpressionKind::kBinding || projection.kind == ReturnExpressionKind::kCount) {
          const uint64_t value = binding.has_value() ? binding->edge_id : entity_id;
          if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "chain identity exceeds Int64");
          }
          values.push_back(Value::Int64(static_cast<int64_t>(value)));
          validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kValidFrom ||
                   projection.kind == ReturnExpressionKind::kCommitSeq ||
                   projection.kind == ReturnExpressionKind::kSystemTime) {
          std::optional<TemporalEvent> event;
          if (binding.has_value()) {
            const LogicalKey key = LogicalKey::EdgeExistence(binding->source_id, binding->target_id,
                binding->edge_type, binding->edge_id, binding->direction);
            event = ResolveVisibleEvent(events_by_key[key], key, valid_time, snapshot_seq);
          } else {
            const LogicalKey key = LogicalKey::VertexExistence(entity_id);
            event = ResolveVisibleEvent(events_by_key[key], key, valid_time, snapshot_seq);
          }
          if (!event.has_value()) return Status::Corruption("T-Cypher executor", "visible chain binding disappeared");
          if (projection.kind == ReturnExpressionKind::kValidFrom) {
            values.push_back(Value::Timestamp(event->valid_from()));
          } else if (projection.kind == ReturnExpressionKind::kCommitSeq) {
            values.push_back(Value::Int64(static_cast<int64_t>(event->commit_seq())));
          } else {
            const auto system_time = SystemTimeForCommit(context.commit_timeline, event->commit_seq());
            if (!system_time.ok()) return system_time.status();
            values.push_back(Value::Timestamp(system_time.ValueOrDie()));
          }
          validity.push_back(true);
        } else {
          const LogicalKey key = binding.has_value()
              ? LogicalKey::EdgeProperty(binding->source_id, binding->target_id, binding->edge_type,
                                         binding->edge_id, *projection.property_column, binding->direction)
              : LogicalKey::VertexProperty(entity_id, *projection.property_column);
          auto value = ResolveQueryValue(
              events_by_key, key, valid_time, snapshot_seq, context);
          if (!value.ok()) return value.status();
          values.push_back(value.ValueOrDie().value_or(Value::Binary("")));
          validity.push_back(value.ValueOrDie().has_value());
        }
      }
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(values), std::move(validity)));
      if (!added.ok()) return added;
    }
    ResultBatch result;
    std::vector<std::string> names;
    for (const Projection& projection : projections) names.push_back(projection.name);
    result = ResultBatch(std::move(names), std::move(batch),
                         ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteFixedExpandAsOf(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t valid_time, uint64_t snapshot_seq) {
  if (statement.relationships.size() != 1 || statement.expanded_nodes.size() != 1) {
    return Status::NotSupported("T-Cypher executor", "only one fixed relationship is available");
  }
  const MatchRelationshipPattern& relationship = statement.relationships.front();
  const MatchNodePattern& target = statement.expanded_nodes.front();
  if (relationship.variable_length) {
    return Status::NotSupported("T-Cypher executor",
                                "fixed relationship cannot use a variable-length pattern");
  }
  if (!target.label.empty()) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->logical_type != target.label) {
      return Status::BindError("T-Cypher executor", "target vertex label is not registered");
    }
  }
  const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
                                   ? EntityType::EdgeOut
                                   : EntityType::EdgeIn;
  std::optional<uint16_t> edge_type;
  if (!relationship.type.empty()) {
    const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "relationship type is not registered");
    }
    edge_type = schema->column_id;
  }
  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  std::vector<TemporalEvent> vertex_candidates;
  std::vector<TemporalEvent> expand_candidates;
  for (const auto& entry : events_by_key) {
    const LogicalKey& key = entry.first;
    const bool vertex_existence = key.entity_type() == EntityType::Vertex &&
                                  key.kind() == LogicalKeyKind::kExistence;
    const bool matching_edge = key.entity_type() == direction &&
                               key.kind() == LogicalKeyKind::kExistence &&
                               (!edge_type.has_value() || key.edge_type() == *edge_type);
    if (!vertex_existence && !matching_edge) continue;
    for (const TemporalEvent& event : entry.second) {
      if (vertex_existence) vertex_candidates.push_back(event);
      expand_candidates.push_back(event);
    }
  }
  auto scan = OpenTemporalScan(
      vertex_candidates, TemporalScanSpec{valid_time, snapshot_seq, context.options.batch_capacity});
  if (!scan.ok()) return scan.status();

  struct Projection {
    ReturnExpressionKind kind;
    std::string variable;
    uint16_t property_column = 0;
  };
  std::vector<std::string> names;
  std::vector<Projection> projections;
  for (const ReturnExpression& expression : statement.returns) {
    if (expression.variable != statement.match.variable && expression.variable != target.variable &&
        expression.variable != relationship.variable) {
      return Status::BindError("T-Cypher executor", "return variable is not owned by expand");
    }
    Projection projection{expression.kind, expression.variable};
    if (expression.kind == ReturnExpressionKind::kProperty ||
        expression.kind == ReturnExpressionKind::kSum ||
        expression.kind == ReturnExpressionKind::kAvg ||
        expression.kind == ReturnExpressionKind::kMin ||
        expression.kind == ReturnExpressionKind::kMax) {
      const EntityType property_entity = expression.variable == relationship.variable
          ? direction : EntityType::Vertex;
      const auto schema = FindColumnByName(*context.schema_snapshot, property_entity,
                                           expression.property_name);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "projected property is not registered");
      }
      projection.property_column = schema->column_id;
      names.push_back(expression.kind == ReturnExpressionKind::kSum
                          ? "sum(" + expression.variable + "." + expression.property_name + ")"
                          : expression.kind == ReturnExpressionKind::kAvg
                              ? "avg(" + expression.variable + "." + expression.property_name + ")"
                              : expression.kind == ReturnExpressionKind::kMin
                                  ? "min(" + expression.variable + "." + expression.property_name + ")"
                                  : expression.kind == ReturnExpressionKind::kMax
                                      ? "max(" + expression.variable + "." + expression.property_name + ")"
                                      : expression.variable + "." + expression.property_name);
    } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
      names.push_back("valid_from(" + expression.variable + ")");
    } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
      names.push_back("commit_seq(" + expression.variable + ")");
    } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
      names.push_back("system_time(" + expression.variable + ")");
    } else if (expression.kind == ReturnExpressionKind::kBinding ||
               expression.kind == ReturnExpressionKind::kCount) {
      names.push_back(expression.variable);
    } else {
      return Status::NotSupported("T-Cypher executor", "unsupported fixed expand projection");
    }
    projections.push_back(std::move(projection));
  }

  TemporalScanCursor cursor = std::move(scan).ConsumeValueOrDie();
  std::vector<ResultBatch> batches;
  for (;;) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    ColumnBatch sources;
    const Status scanned = cursor.NextMorsel(&sources);
    if (scanned.IsNotFound()) break;
    if (!scanned.ok()) return scanned;
    ColumnBatch expanded;
    const Status expanded_status = ExpandAsOfBatch(
        sources, expand_candidates,
        VectorExpandSpec{direction, valid_time, snapshot_seq, context.options.batch_capacity},
        &expanded);
    if (!expanded_status.ok()) return expanded_status;
    if (expanded.row_count() == 0) continue;
    ColumnBatch projected(context.options.batch_capacity);
    for (const Projection& projection : projections) {
      if (projection.kind == ReturnExpressionKind::kBinding &&
          projection.variable == relationship.variable) {
        std::vector<StructValue> relationships;
        relationships.reserve(expanded.row_count());
        for (uint32_t row = 0; row < expanded.row_count(); ++row) {
          const auto source = expanded.ValueAt(kExpandedSourceId, row);
          const auto target_id = expanded.ValueAt(kExpandedTargetId, row);
          const auto edge_id = expanded.ValueAt(kExpandedEdgeId, row);
          const auto edge_type_value = expanded.ValueAt(kExpandedEdgeType, row);
          const auto valid_from = expanded.ValueAt(kExpandedValidFrom, row);
          const auto commit_seq = expanded.ValueAt(kExpandedCommitSeq, row);
          if (!source.has_value() || !target_id.has_value() || !edge_id.has_value() ||
              !edge_type_value.has_value() || !valid_from.has_value() || !commit_seq.has_value()) {
            return Status::Corruption("T-Cypher executor", "expand result has a null identity");
          }
          relationships.push_back(StructValue{{
              StructField{"source_id", *source}, StructField{"target_id", *target_id},
              StructField{"edge_type", *edge_type_value}, StructField{"edge_id", *edge_id},
              StructField{"valid_from", *valid_from}, StructField{"commit_seq", *commit_seq},
          }});
        }
        const Status added = projected.AddVector(
            std::make_shared<StructVector>(std::move(relationships), std::vector<bool>{}));
        if (!added.ok()) return added;
        continue;
      }
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(expanded.row_count());
      validity.reserve(expanded.row_count());
      for (uint32_t row = 0; row < expanded.row_count(); ++row) {
        const auto source = expanded.ValueAt(kExpandedSourceId, row);
        const auto target_id = expanded.ValueAt(kExpandedTargetId, row);
        const auto edge_id = expanded.ValueAt(kExpandedEdgeId, row);
        const auto edge_type_value = expanded.ValueAt(kExpandedEdgeType, row);
        const auto valid_from = expanded.ValueAt(kExpandedValidFrom, row);
        const auto commit_seq = expanded.ValueAt(kExpandedCommitSeq, row);
        if (!source.has_value() || !target_id.has_value() || !edge_id.has_value() ||
            !edge_type_value.has_value() || !valid_from.has_value() || !commit_seq.has_value()) {
          return Status::Corruption("T-Cypher executor", "expand result has a null identity");
        }
        const uint64_t source_id = static_cast<uint64_t>(std::get<int64_t>(source->data()));
        const uint64_t destination_id = static_cast<uint64_t>(std::get<int64_t>(target_id->data()));
        const uint64_t relationship_id = static_cast<uint64_t>(std::get<int64_t>(edge_id->data()));
        const uint16_t relationship_type = static_cast<uint16_t>(std::get<int32_t>(edge_type_value->data()));
        if (projection.kind == ReturnExpressionKind::kBinding ||
            projection.kind == ReturnExpressionKind::kCount) {
          const Value& identity = projection.variable == statement.match.variable ? *source : *target_id;
          values.push_back(identity);
          validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kValidFrom) {
          values.push_back(*valid_from);
          validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kCommitSeq) {
          values.push_back(*commit_seq);
          validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kSystemTime) {
          const uint64_t sequence = static_cast<uint64_t>(std::get<int64_t>(commit_seq->data()));
          const auto system_time = SystemTimeForCommit(context.commit_timeline, sequence);
          if (!system_time.ok()) return system_time.status();
          values.push_back(Value::Timestamp(system_time.ValueOrDie()));
          validity.push_back(true);
        } else {
          const LogicalKey key = projection.variable == relationship.variable
              ? LogicalKey::EdgeProperty(source_id, destination_id, relationship_type, relationship_id,
                                         projection.property_column, direction)
              : LogicalKey::VertexProperty(projection.variable == statement.match.variable
                                               ? source_id : destination_id,
                                           projection.property_column);
          auto value = ResolveQueryValue(
              events_by_key, key, valid_time, snapshot_seq, context);
          if (!value.ok()) return value.status();
          values.push_back(value.ValueOrDie().value_or(Value::Binary("")));
          validity.push_back(value.ValueOrDie().has_value());
        }
      }
      const Status added = projected.AddVector(
          std::make_shared<FlatVector>(std::move(values), std::move(validity)));
      if (!added.ok()) return added;
    }
    ResultBatch batch(names, std::move(projected),
                      ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = batch.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(batch));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteRelationshipChanges(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq,
    bool system_time_axis,
    const std::optional<std::pair<uint64_t, uint64_t>>& valid_time_range,
    const std::optional<uint64_t>& valid_time_as_of) {
  const bool mixed_path = statement.relationships.size() > 1 &&
      std::any_of(statement.relationships.begin(), statement.relationships.end(),
                  [](const MatchRelationshipPattern& relationship) {
                    return relationship.variable_length;
                  });
  if (mixed_path) {
    return Status::NotSupported(
        "T-Cypher executor",
        "mixed variable/fixed paths are not supported in CHANGES");
  }
  if (statement.relationships.size() != 1 || statement.expanded_nodes.size() != 1) {
    return Status::NotSupported("T-Cypher executor",
                                "change execution supports one fixed relationship");
  }
  const MatchRelationshipPattern& relationship = statement.relationships.front();
  const MatchNodePattern& target = statement.expanded_nodes.front();
  if (relationship.variable_length) {
    return Status::NotSupported("T-Cypher executor",
                                "change execution does not support variable relationship paths");
  }
  if (!target.label.empty()) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->logical_type != target.label) {
      return Status::BindError("T-Cypher executor", "target vertex label is not registered");
    }
  }
  const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
      ? EntityType::EdgeOut : EntityType::EdgeIn;
  std::optional<uint16_t> edge_type;
  if (!relationship.type.empty()) {
    const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "relationship type is not registered");
    }
    edge_type = schema->column_id;
  }
  struct Projection {
    ReturnExpressionKind kind;
    std::string variable;
    uint16_t property_column = 0;
    std::string name;
  };
  std::vector<Projection> projections;
  projections.reserve(statement.returns.size());
  for (const ReturnExpression& expression : statement.returns) {
    if (expression.variable != statement.match.variable && expression.variable != target.variable &&
        expression.variable != relationship.variable) {
      return Status::BindError("T-Cypher executor", "return variable is not owned by relationship");
    }
    Projection projection{expression.kind, expression.variable, 0, expression.variable};
    if (expression.kind == ReturnExpressionKind::kProperty) {
      const EntityType entity = expression.variable == relationship.variable
          ? direction : EntityType::Vertex;
      const auto schema = FindColumnByName(*context.schema_snapshot, entity, expression.property_name);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "projected property is not registered");
      }
      projection.property_column = schema->column_id;
      projection.name = expression.variable + "." + expression.property_name;
    } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
      projection.name = "valid_from(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
      projection.name = "commit_seq(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kOperation) {
      projection.name = "operation(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
      projection.name = "system_time(" + expression.variable + ")";
    } else if (expression.kind != ReturnExpressionKind::kBinding) {
      return Status::NotSupported("T-Cypher executor", "unsupported relationship change projection");
    }
    projections.push_back(std::move(projection));
  }

  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  std::vector<TemporalEvent> changes;
  for (const auto& entry : events_by_key) {
    const LogicalKey& key = entry.first;
    if (key.entity_type() != direction || key.kind() != LogicalKeyKind::kExistence ||
        (edge_type.has_value() && key.edge_type() != *edge_type)) {
      continue;
    }
    for (const TemporalEvent& event : entry.second) {
      const Status cancelled = CheckQueryCancelled(context);
      if (!cancelled.ok()) return cancelled;
      if (event.commit_seq() > snapshot_seq) continue;
      if (!system_time_axis) {
        if (event.valid_from() < range_start || event.valid_from() >= range_end) continue;
      } else {
        if (valid_time_range.has_value() &&
            (event.valid_from() < valid_time_range->first ||
             event.valid_from() >= valid_time_range->second)) {
          continue;
        }
        if (valid_time_as_of.has_value()) {
          const auto visible = ResolveVisibleEvent(entry.second, key, *valid_time_as_of,
                                                    snapshot_seq);
          if (!visible.has_value() || visible->valid_from() != event.valid_from() ||
              visible->commit_seq() != event.commit_seq()) {
            continue;
          }
        }
        const auto system_time = SystemTimeForCommit(context.commit_timeline, event.commit_seq());
        if (!system_time.ok()) return system_time.status();
        if (system_time.ValueOrDie() < range_start || system_time.ValueOrDie() >= range_end) continue;
      }
      changes.push_back(event);
    }
  }
  std::sort(changes.begin(), changes.end(), [system_time_axis](const TemporalEvent& left,
                                                                 const TemporalEvent& right) {
    if (system_time_axis) {
      if (left.commit_seq() != right.commit_seq()) return left.commit_seq() < right.commit_seq();
    } else {
      if (left.valid_from() != right.valid_from()) return left.valid_from() < right.valid_from();
      if (left.commit_seq() != right.commit_seq()) return left.commit_seq() < right.commit_seq();
    }
    return left.logical_key() < right.logical_key();
  });
  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < changes.size(); begin += context.options.batch_capacity) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const size_t end = std::min(changes.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(projections.size());
    std::vector<std::vector<bool>> validity(projections.size());
    for (auto& column : columns) column.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
      const TemporalEvent& event = changes[index];
      const LogicalKey& key = event.logical_key();
      for (size_t output = 0; output < projections.size(); ++output) {
        const Projection& projection = projections[output];
        if (projection.kind == ReturnExpressionKind::kBinding &&
            projection.variable == relationship.variable) {
          continue;
        }
        const uint64_t entity_id = projection.variable == target.variable
            ? key.target_id() : key.entity_id();
        if (projection.kind == ReturnExpressionKind::kBinding) {
          if (entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
          }
          columns[output].push_back(Value::Int64(static_cast<int64_t>(entity_id)));
        } else if (projection.kind == ReturnExpressionKind::kValidFrom) {
          columns[output].push_back(Value::Timestamp(event.valid_from()));
        } else if (projection.kind == ReturnExpressionKind::kCommitSeq) {
          columns[output].push_back(Value::Int64(static_cast<int64_t>(event.commit_seq())));
        } else if (projection.kind == ReturnExpressionKind::kOperation) {
          columns[output].push_back(Value::String(event.is_delete() ? "DELETE" : "PUT"));
        } else if (projection.kind == ReturnExpressionKind::kSystemTime) {
          const auto system_time = SystemTimeForCommit(context.commit_timeline, event.commit_seq());
          if (!system_time.ok()) return system_time.status();
          columns[output].push_back(Value::Timestamp(system_time.ValueOrDie()));
        } else {
          const LogicalKey property_key = projection.variable == relationship.variable
              ? LogicalKey::EdgeProperty(key.entity_id(), key.target_id(), key.edge_type(), key.edge_id(),
                                         projection.property_column, direction)
              : LogicalKey::VertexProperty(entity_id, projection.property_column);
          auto value = ResolveQueryValue(
              events_by_key, property_key, event.valid_from(), snapshot_seq,
              context);
          if (!value.ok()) return value.status();
          columns[output].push_back(
              value.ValueOrDie().value_or(Value::Binary("")));
          validity[output].push_back(value.ValueOrDie().has_value());
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    std::vector<std::string> names;
    names.reserve(projections.size());
    for (size_t output = 0; output < projections.size(); ++output) {
      const Projection& projection = projections[output];
      Status added = Status::OK();
      if (projection.kind == ReturnExpressionKind::kBinding &&
          projection.variable == relationship.variable) {
        std::vector<StructValue> relationships;
        relationships.reserve(end - begin);
        for (size_t index = begin; index < end; ++index) {
          const TemporalEvent& event = changes[index];
          const LogicalKey& key = event.logical_key();
          if (key.entity_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
              key.target_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
              key.edge_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "relationship identity exceeds Int64");
          }
          relationships.push_back(StructValue{{
              StructField{"source_id", Value::Int64(static_cast<int64_t>(key.entity_id()))},
              StructField{"target_id", Value::Int64(static_cast<int64_t>(key.target_id()))},
              StructField{"edge_type", Value::Int32(static_cast<int32_t>(key.edge_type()))},
              StructField{"edge_id", Value::Int64(static_cast<int64_t>(key.edge_id()))},
              StructField{"valid_from", Value::Timestamp(event.valid_from())},
              StructField{"commit_seq", Value::Int64(static_cast<int64_t>(event.commit_seq()))},
              StructField{"operation", Value::String(event.is_delete() ? "DELETE" : "PUT")},
          }});
        }
        added = batch.AddVector(
            std::make_shared<StructVector>(std::move(relationships), std::vector<bool>{}));
      } else {
        added = batch.AddVector(std::make_shared<FlatVector>(
            std::move(columns[output]), std::move(validity[output])));
      }
      if (!added.ok()) return added;
      names.push_back(projection.name);
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteValidTimeChanges(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq) {
  if (!statement.relationships.empty()) {
    return ExecuteRelationshipChanges(statement, context, range_start, range_end, snapshot_seq,
                                      false, std::nullopt, std::nullopt);
  }
  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  std::map<size_t, uint16_t> property_columns;
  for (size_t index = 0; index < statement.returns.size(); ++index) {
    const ReturnExpression& expression = statement.returns[index];
    if (expression.variable != statement.match.variable ||
        expression.kind == ReturnExpressionKind::kValidTo) {
      return Status::NotSupported("T-Cypher executor", "change property projections are not available yet");
    }
    if (expression.kind == ReturnExpressionKind::kProperty) {
      const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                           expression.property_name);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "projected property is not registered");
      }
      property_columns.emplace(index, schema->column_id);
    }
  }
  std::vector<TemporalEvent> changes;
  for (const auto& entry : events_by_key) {
    const LogicalKey& key = entry.first;
    if (key.entity_type() != EntityType::Vertex || key.kind() != LogicalKeyKind::kExistence) {
      continue;
    }
    for (const TemporalEvent& event : entry.second) {
      if (event.commit_seq() <= snapshot_seq && event.valid_from() >= range_start &&
          event.valid_from() < range_end) {
        changes.push_back(event);
      }
    }
  }
  std::sort(changes.begin(), changes.end(), [](const TemporalEvent& left,
                                                const TemporalEvent& right) {
    if (left.valid_from() != right.valid_from()) return left.valid_from() < right.valid_from();
    if (left.commit_seq() != right.commit_seq()) return left.commit_seq() < right.commit_seq();
    return left.logical_key() < right.logical_key();
  });
  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < changes.size(); begin += context.options.batch_capacity) {
    const size_t end = std::min(changes.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(statement.returns.size());
    std::vector<std::vector<bool>> validity(statement.returns.size());
    for (std::vector<Value>& column : columns) column.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
      const TemporalEvent& event = changes[index];
      const uint64_t entity_id = event.logical_key().entity_id();
      if (entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
      }
      for (size_t output = 0; output < statement.returns.size(); ++output) {
        switch (statement.returns[output].kind) {
          case ReturnExpressionKind::kBinding:
            columns[output].push_back(Value::Int64(static_cast<int64_t>(entity_id)));
            break;
          case ReturnExpressionKind::kValidFrom:
            columns[output].push_back(Value::Timestamp(event.valid_from()));
            break;
          case ReturnExpressionKind::kCommitSeq:
            columns[output].push_back(Value::Int64(static_cast<int64_t>(event.commit_seq())));
            break;
          case ReturnExpressionKind::kOperation:
            columns[output].push_back(Value::String(event.is_delete() ? "DELETE" : "PUT"));
            break;
          case ReturnExpressionKind::kSystemTime: {
            const auto system_time = SystemTimeForCommit(context.commit_timeline, event.commit_seq());
            if (!system_time.ok()) return system_time.status();
            columns[output].push_back(Value::Timestamp(system_time.ValueOrDie()));
            break;
          }
          case ReturnExpressionKind::kProperty: {
            const LogicalKey property_key = LogicalKey::VertexProperty(
                entity_id, property_columns.at(output));
            auto value = ResolveQueryValue(
                events_by_key, property_key, event.valid_from(), snapshot_seq,
                context);
            if (!value.ok()) return value.status();
            columns[output].push_back(
                value.ValueOrDie().value_or(Value::Binary("")));
            validity[output].push_back(value.ValueOrDie().has_value());
            break;
          }
          case ReturnExpressionKind::kValidTo:
          case ReturnExpressionKind::kCount:
          case ReturnExpressionKind::kSum:
          case ReturnExpressionKind::kAvg:
          case ReturnExpressionKind::kMin:
          case ReturnExpressionKind::kMax:
          case ReturnExpressionKind::kCollect:
            return Status::NotSupported("T-Cypher executor", "unsupported change projection");
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    std::vector<std::string> names;
    names.reserve(statement.returns.size());
    for (size_t output = 0; output < statement.returns.size(); ++output) {
      const ReturnExpression& expression = statement.returns[output];
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(columns[output]), std::move(validity[output])));
      if (!added.ok()) return added;
      if (expression.kind == ReturnExpressionKind::kOperation) {
        names.push_back("operation(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
        names.push_back("commit_seq(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
        names.push_back("valid_from(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
        names.push_back("system_time(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kProperty) {
        names.push_back(expression.variable + "." + expression.property_name);
      } else {
        names.push_back(expression.variable);
      }
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteSystemTimeChanges(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq,
    const std::optional<std::pair<uint64_t, uint64_t>>& valid_time_range,
    const std::optional<uint64_t>& valid_time_as_of) {
  if (!statement.relationships.empty()) {
    return ExecuteRelationshipChanges(statement, context, range_start, range_end, snapshot_seq,
                                      true, valid_time_range, valid_time_as_of);
  }
  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  std::map<size_t, uint16_t> property_columns;
  for (size_t index = 0; index < statement.returns.size(); ++index) {
    const ReturnExpression& expression = statement.returns[index];
    if (expression.variable != statement.match.variable ||
        expression.kind == ReturnExpressionKind::kValidTo) {
      return Status::NotSupported("T-Cypher executor", "change property projections are not available yet");
    }
    if (expression.kind == ReturnExpressionKind::kProperty) {
      const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                           expression.property_name);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "projected property is not registered");
      }
      property_columns.emplace(index, schema->column_id);
    }
  }
  std::vector<TemporalEvent> changes;
  for (const auto& entry : events_by_key) {
    const LogicalKey& key = entry.first;
    if (key.entity_type() != EntityType::Vertex || key.kind() != LogicalKeyKind::kExistence) {
      continue;
    }
    for (const TemporalEvent& event : entry.second) {
      if (event.commit_seq() > snapshot_seq) continue;
      if (valid_time_range.has_value() &&
          (event.valid_from() < valid_time_range->first ||
           event.valid_from() >= valid_time_range->second)) {
        continue;
      }
      if (valid_time_as_of.has_value()) {
        const auto visible = ResolveVisibleEvent(entry.second, key, *valid_time_as_of,
                                                  snapshot_seq);
        if (!visible.has_value() || visible->valid_from() != event.valid_from() ||
            visible->commit_seq() != event.commit_seq()) {
          continue;
        }
      }
      bool in_system_range = false;
      for (const CommitTimelineEntry& timeline_entry : context.commit_timeline.entries()) {
        if (timeline_entry.commit_seq == event.commit_seq()) {
          in_system_range = timeline_entry.system_time_hlc.physical_us >= range_start &&
                            timeline_entry.system_time_hlc.physical_us < range_end;
          break;
        }
      }
      if (in_system_range) changes.push_back(event);
    }
  }
  std::sort(changes.begin(), changes.end(), [](const TemporalEvent& left,
                                                const TemporalEvent& right) {
    if (left.commit_seq() != right.commit_seq()) return left.commit_seq() < right.commit_seq();
    return left.logical_key() < right.logical_key();
  });
  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < changes.size(); begin += context.options.batch_capacity) {
    const size_t end = std::min(changes.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(statement.returns.size());
    std::vector<std::vector<bool>> validity(statement.returns.size());
    for (std::vector<Value>& column : columns) column.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
      const TemporalEvent& event = changes[index];
      const uint64_t entity_id = event.logical_key().entity_id();
      if (entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
      }
      for (size_t output = 0; output < statement.returns.size(); ++output) {
        switch (statement.returns[output].kind) {
          case ReturnExpressionKind::kBinding:
            columns[output].push_back(Value::Int64(static_cast<int64_t>(entity_id)));
            break;
          case ReturnExpressionKind::kValidFrom:
            columns[output].push_back(Value::Timestamp(event.valid_from()));
            break;
          case ReturnExpressionKind::kCommitSeq:
            columns[output].push_back(Value::Int64(static_cast<int64_t>(event.commit_seq())));
            break;
          case ReturnExpressionKind::kOperation:
            columns[output].push_back(Value::String(event.is_delete() ? "DELETE" : "PUT"));
            break;
          case ReturnExpressionKind::kSystemTime: {
            const auto system_time = SystemTimeForCommit(context.commit_timeline, event.commit_seq());
            if (!system_time.ok()) return system_time.status();
            columns[output].push_back(Value::Timestamp(system_time.ValueOrDie()));
            break;
          }
          case ReturnExpressionKind::kProperty: {
            const LogicalKey property_key = LogicalKey::VertexProperty(
                entity_id, property_columns.at(output));
            auto value = ResolveQueryValue(
                events_by_key, property_key, event.valid_from(), snapshot_seq,
                context);
            if (!value.ok()) return value.status();
            columns[output].push_back(
                value.ValueOrDie().value_or(Value::Binary("")));
            validity[output].push_back(value.ValueOrDie().has_value());
            break;
          }
          case ReturnExpressionKind::kValidTo:
          case ReturnExpressionKind::kCount:
          case ReturnExpressionKind::kSum:
          case ReturnExpressionKind::kAvg:
          case ReturnExpressionKind::kMin:
          case ReturnExpressionKind::kMax:
          case ReturnExpressionKind::kCollect:
            return Status::NotSupported("T-Cypher executor", "unsupported change projection");
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    std::vector<std::string> names;
    names.reserve(statement.returns.size());
    for (size_t output = 0; output < statement.returns.size(); ++output) {
      const ReturnExpression& expression = statement.returns[output];
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(columns[output]), std::move(validity[output])));
      if (!added.ok()) return added;
      if (expression.kind == ReturnExpressionKind::kOperation) {
        names.push_back("operation(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
        names.push_back("commit_seq(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
        names.push_back("valid_from(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
        names.push_back("system_time(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kProperty) {
        names.push_back(expression.variable + "." + expression.property_name);
      } else {
        names.push_back(expression.variable);
      }
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteFixedExpandChainRange(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq) {
  if (statement.relationships.size() < 2 ||
      statement.relationships.size() != statement.expanded_nodes.size()) {
    return Status::InvalidArgument("T-Cypher executor", "invalid fixed relationship range chain");
  }
  for (size_t index = 0; index < statement.relationships.size(); ++index) {
    if (statement.relationships[index].variable_length) {
      return Status::NotSupported("T-Cypher executor",
                                  "multi-hop range expansion does not support variable paths");
    }
    if (!statement.expanded_nodes[index].label.empty()) {
      const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
      if (!existence.has_value() || existence->logical_type != statement.expanded_nodes[index].label) {
        return Status::BindError("T-Cypher executor", "target vertex label is not registered");
      }
    }
  }

  struct NodeBinding {
    uint64_t entity_id = 0;
    uint64_t commit_seq = 0;
    TemporalOperation operation = TemporalOperation::kPut;
  };
  struct EdgeBinding {
    LogicalKey key = LogicalKey::VertexExistence(0);
    uint64_t source_id = 0;
    uint64_t target_id = 0;
    uint64_t commit_seq = 0;
    TemporalOperation operation = TemporalOperation::kPut;
    EntityType direction = EntityType::EdgeOut;
  };
  struct ChainRow {
    std::vector<NodeBinding> nodes;
    std::vector<EdgeBinding> edges;
    uint64_t valid_from = 0;
    uint64_t valid_to = 0;
    std::vector<std::optional<Value>> properties;
  };
  struct Projection {
    ReturnExpressionKind kind;
    std::string variable;
    std::optional<uint16_t> property_column;
    std::string name;
  };

  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  const auto intervals_for = [&](const LogicalKey& key) -> StatusOr<std::vector<TemporalInterval>> {
    const auto found = events_by_key.find(key);
    const std::vector<TemporalEvent> no_events;
    return DeriveVisibleIntervals(found == events_by_key.end() ? no_events : found->second,
                                  key, snapshot_seq, range_start, range_end);
  };

  std::map<std::string, size_t> node_positions;
  node_positions.emplace(statement.match.variable, 0);
  for (size_t index = 0; index < statement.expanded_nodes.size(); ++index) {
    if (!node_positions.emplace(statement.expanded_nodes[index].variable, index + 1).second) {
      return Status::BindError("T-Cypher executor", "duplicate node variable in relationship chain");
    }
  }
  std::map<std::string, size_t> edge_positions;
  for (size_t index = 0; index < statement.relationships.size(); ++index) {
    if (statement.relationships[index].variable.empty() ||
        !edge_positions.emplace(statement.relationships[index].variable, index).second) {
      return Status::BindError("T-Cypher executor", "each multi-hop relationship requires a unique variable");
    }
  }

  std::optional<uint64_t> exact_root;
  if (statement.match.entity_id.has_value()) {
    const auto resolved = ResolveExactEntityId(statement.match, context.options);
    if (!resolved.ok()) return resolved.status();
    exact_root = resolved.ValueOrDie();
  }
  std::optional<uint16_t> predicate_column;
  if (statement.where.has_value()) {
    if (statement.where->variable != statement.match.variable) {
      return Status::NotSupported("T-Cypher executor",
                                  "multi-hop range WHERE supports only root vertex properties");
    }
    const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                         statement.where->property_name);
    if (!schema.has_value() || schema->physical_type != PhysicalType::kString) {
      return Status::SchemaMismatch("T-Cypher executor", "WHERE property must be a string");
    }
    predicate_column = schema->column_id;
  }

  std::vector<ChainRow> rows;
  for (const auto& entry : events_by_key) {
    const LogicalKey& key = entry.first;
    if (key.entity_type() != EntityType::Vertex || key.kind() != LogicalKeyKind::kExistence ||
        (exact_root.has_value() && key.entity_id() != *exact_root)) continue;
    const auto root_intervals = intervals_for(key);
    if (!root_intervals.ok()) return root_intervals.status();
    std::vector<TemporalInterval> predicate_intervals;
    if (predicate_column.has_value()) {
      const auto derived = intervals_for(LogicalKey::VertexProperty(key.entity_id(), *predicate_column));
      if (!derived.ok()) return derived.status();
      predicate_intervals = derived.ValueOrDie();
    }
    for (const TemporalInterval& root : root_intervals.ValueOrDie()) {
      if (root.event.is_delete()) continue;
      if (!predicate_column.has_value()) {
        rows.push_back(ChainRow{{NodeBinding{key.entity_id(), root.event.commit_seq(), root.event.operation()}},
                                {}, root.valid_from, root.valid_to, {}});
        continue;
      }
      for (const TemporalInterval& predicate : predicate_intervals) {
        if (predicate.event.is_delete()) continue;
        const auto matches = IndexedEventEqualsValue(
            predicate.event, Value::String(statement.where->string_value));
        if (!matches.ok()) return matches.status();
        if (!matches.ValueOrDie()) continue;
        const uint64_t start = std::max(root.valid_from, predicate.valid_from);
        const uint64_t end = std::min(root.valid_to, predicate.valid_to);
        if (start < end) {
          rows.push_back(ChainRow{{NodeBinding{key.entity_id(), root.event.commit_seq(), root.event.operation()}},
                                  {}, start, end, {}});
        }
      }
    }
  }

  for (size_t hop = 0; hop < statement.relationships.size() && !rows.empty(); ++hop) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const MatchRelationshipPattern& relationship = statement.relationships[hop];
    const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
        ? EntityType::EdgeOut : EntityType::EdgeIn;
    std::optional<uint16_t> edge_type;
    if (!relationship.type.empty()) {
      const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
      if (!schema.has_value()) return Status::BindError("T-Cypher executor", "relationship type is not registered");
      edge_type = schema->column_id;
    }
    std::vector<ChainRow> expanded;
    for (const ChainRow& row : rows) {
      const uint64_t source_id = row.nodes.back().entity_id;
      for (const auto& edge_entry : events_by_key) {
        const LogicalKey& edge_key = edge_entry.first;
        if (edge_key.entity_type() != direction || edge_key.kind() != LogicalKeyKind::kExistence ||
            edge_key.entity_id() != source_id ||
            (edge_type.has_value() && edge_key.edge_type() != *edge_type)) continue;
        const auto edge_intervals = intervals_for(edge_key);
        const auto target_intervals = intervals_for(LogicalKey::VertexExistence(edge_key.target_id()));
        if (!edge_intervals.ok()) return edge_intervals.status();
        if (!target_intervals.ok()) return target_intervals.status();
        for (const TemporalInterval& edge : edge_intervals.ValueOrDie()) {
          if (edge.event.is_delete()) continue;
          for (const TemporalInterval& target : target_intervals.ValueOrDie()) {
            if (target.event.is_delete()) continue;
            const uint64_t start = std::max(row.valid_from, std::max(edge.valid_from, target.valid_from));
            const uint64_t end = std::min(row.valid_to, std::min(edge.valid_to, target.valid_to));
            if (start >= end) continue;
            ChainRow next = row;
            next.valid_from = start;
            next.valid_to = end;
            next.edges.push_back(EdgeBinding{edge_key, edge_key.entity_id(), edge_key.target_id(),
                                             edge.event.commit_seq(), edge.event.operation(), direction});
            next.nodes.push_back(NodeBinding{edge_key.target_id(), target.event.commit_seq(),
                                             target.event.operation()});
            expanded.push_back(std::move(next));
          }
        }
      }
    }
    rows = std::move(expanded);
  }

  std::vector<Projection> projections;
  std::vector<size_t> property_outputs;
  for (size_t output = 0; output < statement.returns.size(); ++output) {
    const ReturnExpression& expression = statement.returns[output];
    const bool node = node_positions.count(expression.variable) != 0;
    const bool edge = edge_positions.count(expression.variable) != 0;
    if (!node && !edge) return Status::BindError("T-Cypher executor", "return variable is not owned by chain");
    Projection projection{expression.kind, expression.variable, std::nullopt, expression.variable};
    if (!expression.property_name.empty()) {
      const EntityType entity = edge
          ? (statement.relationships[edge_positions.at(expression.variable)].direction ==
                     RelationshipDirection::kOutgoing ? EntityType::EdgeOut : EntityType::EdgeIn)
          : EntityType::Vertex;
      const auto schema = FindColumnByName(*context.schema_snapshot, entity, expression.property_name);
      if (!schema.has_value()) return Status::BindError("T-Cypher executor", "projected property is not registered");
      projection.property_column = schema->column_id;
      projection.name = expression.variable + "." + expression.property_name;
      property_outputs.push_back(output);
    } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
      projection.name = "valid_from(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kValidTo) {
      projection.name = "valid_to(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
      projection.name = "commit_seq(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kOperation) {
      projection.name = "operation(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
      projection.name = "system_time(" + expression.variable + ")";
    } else if (expression.kind == ReturnExpressionKind::kCount) {
      projection.name = "count(" + expression.variable + ")";
    } else if (expression.kind != ReturnExpressionKind::kBinding) {
      return Status::NotSupported("T-Cypher executor", "unsupported multi-hop range projection");
    }
    projections.push_back(std::move(projection));
  }

  if (!property_outputs.empty()) {
    std::vector<ChainRow> aligned_rows;
    for (const ChainRow& row : rows) {
      std::vector<std::vector<TemporalInterval>> streams;
      streams.push_back({TemporalInterval{TemporalEvent::Put(
          LogicalKey::VertexExistence(row.nodes.front().entity_id), row.valid_from, 0, 0,
          Value::Bool(true)), row.valid_from, row.valid_to}});
      for (size_t output : property_outputs) {
        const Projection& projection = projections[output];
        const auto node = node_positions.find(projection.variable);
        const auto edge = edge_positions.find(projection.variable);
        const LogicalKey property_key = edge != edge_positions.end()
            ? LogicalKey::EdgeProperty(row.edges[edge->second].source_id, row.edges[edge->second].target_id,
                                       row.edges[edge->second].key.edge_type(),
                                       row.edges[edge->second].key.edge_id(), *projection.property_column,
                                       row.edges[edge->second].direction)
            : LogicalKey::VertexProperty(row.nodes[node->second].entity_id, *projection.property_column);
        const auto derived = intervals_for(property_key);
        if (!derived.ok()) return derived.status();
        std::vector<TemporalInterval> property_intervals = derived.ValueOrDie();
        if (property_intervals.empty()) {
          property_intervals.push_back(
              TemporalInterval{TemporalEvent::Delete(property_key, 0, 0, 0), 0, kTemporalInfinity});
        } else if (property_intervals.front().valid_from > 0) {
          property_intervals.insert(property_intervals.begin(),
              TemporalInterval{TemporalEvent::Delete(property_key, 0, 0, 0), 0,
                               property_intervals.front().valid_from});
        }
        streams.push_back(std::move(property_intervals));
      }
      const auto aligned = AlignTemporalIntervals(streams, row.valid_from, row.valid_to);
      if (!aligned.ok()) return aligned.status();
      for (const AlignedTemporalInterval& interval : aligned.ValueOrDie()) {
        ChainRow projected = row;
        projected.valid_from = interval.valid_from;
        projected.valid_to = interval.valid_to;
        projected.properties.clear();
        for (size_t property = 0; property < property_outputs.size(); ++property) {
          const auto& fact = interval.facts[property + 1];
          projected.properties.push_back(!fact || fact->is_delete()
              ? std::nullopt : std::optional<Value>(fact->value()));
        }
        aligned_rows.push_back(std::move(projected));
      }
    }
    rows = std::move(aligned_rows);
  }
  std::sort(rows.begin(), rows.end(), [](const ChainRow& left, const ChainRow& right) {
    if (left.nodes.front().entity_id != right.nodes.front().entity_id) {
      return left.nodes.front().entity_id < right.nodes.front().entity_id;
    }
    return left.valid_from < right.valid_from;
  });

  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < rows.size(); begin += context.options.batch_capacity) {
    const size_t end = std::min(rows.size(), begin + context.options.batch_capacity);
    ColumnBatch batch(context.options.batch_capacity);
    for (const Projection& projection : projections) {
      const auto node = node_positions.find(projection.variable);
      const auto edge = edge_positions.find(projection.variable);
      if (projection.kind == ReturnExpressionKind::kBinding && edge != edge_positions.end()) {
        std::vector<StructValue> values;
        values.reserve(end - begin);
        for (size_t row = begin; row < end; ++row) {
          const EdgeBinding& binding = rows[row].edges[edge->second];
          values.push_back(StructValue{{
              StructField{"source_id", Value::Int64(static_cast<int64_t>(binding.source_id))},
              StructField{"target_id", Value::Int64(static_cast<int64_t>(binding.target_id))},
              StructField{"edge_type", Value::Int32(static_cast<int32_t>(binding.key.edge_type()))},
              StructField{"edge_id", Value::Int64(static_cast<int64_t>(binding.key.edge_id()))},
              StructField{"valid_from", Value::Timestamp(rows[row].valid_from)},
              StructField{"valid_to", Value::Timestamp(rows[row].valid_to)},
              StructField{"commit_seq", Value::Int64(static_cast<int64_t>(binding.commit_seq))},
          }});
        }
        const Status added = batch.AddVector(
            std::make_shared<StructVector>(std::move(values), std::vector<bool>{}));
        if (!added.ok()) return added;
        continue;
      }
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(end - begin);
      validity.reserve(end - begin);
      for (size_t row = begin; row < end; ++row) {
        const ChainRow& chain = rows[row];
        const uint64_t entity_id = node == node_positions.end() ? 0 : chain.nodes[node->second].entity_id;
        const EdgeBinding* binding = edge == edge_positions.end() ? nullptr : &chain.edges[edge->second];
        const uint64_t commit_seq = binding == nullptr ? chain.nodes[node->second].commit_seq : binding->commit_seq;
        const TemporalOperation operation = binding == nullptr ? chain.nodes[node->second].operation : binding->operation;
        if (projection.kind == ReturnExpressionKind::kBinding || projection.kind == ReturnExpressionKind::kCount) {
          const uint64_t id = binding == nullptr ? entity_id : binding->key.edge_id();
          if (id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "chain identity exceeds Int64");
          }
          values.push_back(Value::Int64(static_cast<int64_t>(id)));
          validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kProperty) {
          const auto property = std::find(property_outputs.begin(), property_outputs.end(),
                                          static_cast<size_t>(&projection - projections.data()));
          if (property == property_outputs.end()) return Status::Corruption("T-Cypher executor", "range property projection is absent");
          const std::optional<Value>& value = chain.properties[static_cast<size_t>(property - property_outputs.begin())];
          values.push_back(value.value_or(Value::Binary("")));
          validity.push_back(value.has_value());
        } else if (projection.kind == ReturnExpressionKind::kValidFrom) {
          values.push_back(Value::Timestamp(chain.valid_from)); validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kValidTo) {
          values.push_back(Value::Timestamp(chain.valid_to)); validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kCommitSeq) {
          values.push_back(Value::Int64(static_cast<int64_t>(commit_seq))); validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kOperation) {
          values.push_back(Value::String(operation == TemporalOperation::kDelete ? "DELETE" : "PUT"));
          validity.push_back(true);
        } else if (projection.kind == ReturnExpressionKind::kSystemTime) {
          const auto system_time = SystemTimeForCommit(context.commit_timeline, commit_seq);
          if (!system_time.ok()) return system_time.status();
          values.push_back(Value::Timestamp(system_time.ValueOrDie())); validity.push_back(true);
        }
      }
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(values), std::move(validity)));
      if (!added.ok()) return added;
    }
    std::vector<std::string> names;
    for (const Projection& projection : projections) names.push_back(projection.name);
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  return std::unique_ptr<QueryResultStream>(
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK()));
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteFixedExpandRange(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq) {
  if (statement.relationships.size() != 1 || statement.expanded_nodes.size() != 1) {
    return Status::NotSupported("T-Cypher executor",
                                "only one fixed range relationship is available");
  }
  const MatchRelationshipPattern& relationship = statement.relationships.front();
  const MatchNodePattern& target = statement.expanded_nodes.front();
  if (relationship.variable_length) {
    return Status::NotSupported("T-Cypher executor",
                                "fixed range relationship cannot use a variable-length pattern");
  }
  if (!target.label.empty()) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->logical_type != target.label) {
      return Status::BindError("T-Cypher executor", "target vertex label is not registered");
    }
  }
  const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
                                   ? EntityType::EdgeOut
                                   : EntityType::EdgeIn;
  std::optional<uint16_t> edge_type;
  if (!relationship.type.empty()) {
    const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "relationship type is not registered");
    }
    edge_type = schema->column_id;
  }

  struct PropertyProjection {
    size_t output;
    std::string variable;
    uint16_t column_id;
  };
  std::vector<PropertyProjection> property_projections;
  for (size_t output = 0; output < statement.returns.size(); ++output) {
    const ReturnExpression& expression = statement.returns[output];
    if (expression.kind != ReturnExpressionKind::kProperty) continue;
    const EntityType property_entity = expression.variable == relationship.variable
        ? direction : EntityType::Vertex;
    const auto schema = FindColumnByName(*context.schema_snapshot, property_entity,
                                         expression.property_name);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "projected property is not registered");
    }
    property_projections.push_back(PropertyProjection{output, expression.variable, schema->column_id});
  }

  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  struct RangeEdgeRow {
    LogicalKey edge_key;
    uint64_t source_id;
    uint64_t target_id;
    uint64_t valid_from;
    uint64_t valid_to;
    uint64_t commit_seq;
    TemporalOperation operation;
    std::vector<std::optional<Value>> properties;
  };
  std::vector<RangeEdgeRow> rows;
  for (const auto& entry : events_by_key) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const LogicalKey& edge_key = entry.first;
    if (edge_key.entity_type() != direction || edge_key.kind() != LogicalKeyKind::kExistence ||
        (edge_type.has_value() && edge_key.edge_type() != *edge_type)) {
      continue;
    }
    const auto edge_intervals = DeriveVisibleIntervals(entry.second, edge_key, snapshot_seq,
                                                       range_start, range_end);
    if (!edge_intervals.ok()) return edge_intervals.status();
    const LogicalKey source_key = LogicalKey::VertexExistence(edge_key.entity_id());
    const LogicalKey target_key = LogicalKey::VertexExistence(edge_key.target_id());
    const auto source_found = events_by_key.find(source_key);
    const auto target_found = events_by_key.find(target_key);
    if (source_found == events_by_key.end() || target_found == events_by_key.end()) continue;
    const auto source_intervals = DeriveVisibleIntervals(source_found->second, source_key,
                                                         snapshot_seq, range_start, range_end);
    const auto target_intervals = DeriveVisibleIntervals(target_found->second, target_key,
                                                         snapshot_seq, range_start, range_end);
    if (!source_intervals.ok()) return source_intervals.status();
    if (!target_intervals.ok()) return target_intervals.status();
    for (const TemporalInterval& edge : edge_intervals.ValueOrDie()) {
      if (edge.event.is_delete()) continue;
      for (const TemporalInterval& source : source_intervals.ValueOrDie()) {
        if (source.event.is_delete()) continue;
        for (const TemporalInterval& target_interval : target_intervals.ValueOrDie()) {
          if (target_interval.event.is_delete()) continue;
          const uint64_t valid_from = std::max(
              edge.valid_from, std::max(source.valid_from, target_interval.valid_from));
          const uint64_t valid_to = std::min(
              edge.valid_to, std::min(source.valid_to, target_interval.valid_to));
          if (valid_from < valid_to) {
            rows.push_back(RangeEdgeRow{edge_key, edge_key.entity_id(), edge_key.target_id(), valid_from,
                                        valid_to, edge.event.commit_seq(), edge.event.operation(), {}});
          }
        }
      }
    }
  }
  if (!property_projections.empty()) {
    std::vector<RangeEdgeRow> aligned_rows;
    for (const RangeEdgeRow& row : rows) {
      std::vector<std::vector<TemporalInterval>> streams;
      streams.push_back({TemporalInterval{
          TemporalEvent::Put(row.edge_key, row.valid_from, row.commit_seq, 0, Value::Binary("")),
          row.valid_from, row.valid_to}});
      std::vector<LogicalKey> property_keys;
      property_keys.reserve(property_projections.size());
      for (const PropertyProjection& projection : property_projections) {
        const LogicalKey key = projection.variable == relationship.variable
            ? LogicalKey::EdgeProperty(row.source_id, row.target_id, row.edge_key.edge_type(),
                                       row.edge_key.edge_id(), projection.column_id, direction)
            : LogicalKey::VertexProperty(projection.variable == statement.match.variable
                                             ? row.source_id : row.target_id, projection.column_id);
        property_keys.push_back(key);
        const auto found = events_by_key.find(key);
        const std::vector<TemporalEvent> no_events;
        auto intervals = DeriveVisibleIntervals(found == events_by_key.end() ? no_events : found->second,
                                                key, snapshot_seq, row.valid_from, row.valid_to);
        if (!intervals.ok()) return intervals.status();
        std::vector<TemporalInterval> property_intervals = intervals.ConsumeValueOrDie();
        if (property_intervals.empty()) {
          property_intervals.push_back(
              TemporalInterval{TemporalEvent::Delete(key, 0, 0, 0), 0, kTemporalInfinity});
        } else if (property_intervals.front().valid_from > 0) {
          property_intervals.insert(property_intervals.begin(),
              TemporalInterval{TemporalEvent::Delete(key, 0, 0, 0), 0,
                               property_intervals.front().valid_from});
        }
        streams.push_back(std::move(property_intervals));
      }
      const auto aligned = AlignTemporalIntervals(streams, row.valid_from, row.valid_to);
      if (!aligned.ok()) return aligned.status();
      for (const AlignedTemporalInterval& interval : aligned.ValueOrDie()) {
        RangeEdgeRow projected = row;
        projected.valid_from = interval.valid_from;
        projected.valid_to = interval.valid_to;
        projected.properties.clear();
        projected.properties.reserve(property_projections.size());
        for (size_t index = 0; index < property_projections.size(); ++index) {
          const auto& fact = interval.facts[index + 1];
          projected.properties.push_back(!fact || fact->is_delete()
              ? std::nullopt : std::optional<Value>(fact->value()));
        }
        aligned_rows.push_back(std::move(projected));
      }
    }
    rows = std::move(aligned_rows);
  }
  std::sort(rows.begin(), rows.end(), [](const RangeEdgeRow& left, const RangeEdgeRow& right) {
    if (left.source_id != right.source_id) return left.source_id < right.source_id;
    if (left.target_id != right.target_id) return left.target_id < right.target_id;
    return left.valid_from < right.valid_from;
  });

  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < rows.size(); begin += context.options.batch_capacity) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const size_t end = std::min(rows.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(statement.returns.size());
    std::vector<std::vector<bool>> validity(statement.returns.size());
    for (std::vector<Value>& column : columns) column.reserve(end - begin);
    std::vector<std::string> names;
    names.reserve(statement.returns.size());
    for (const ReturnExpression& expression : statement.returns) {
      if ((expression.kind == ReturnExpressionKind::kBinding &&
           expression.variable == relationship.variable)) {
        return Status::NotSupported("T-Cypher executor",
                                    "range relationship value projections are not available yet");
      }
      if (expression.variable != statement.match.variable && expression.variable != target.variable &&
          expression.variable != relationship.variable) {
        return Status::BindError("T-Cypher executor", "return variable is not owned by range expand");
      }
      if (expression.kind == ReturnExpressionKind::kValidFrom) {
        names.push_back("valid_from(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kValidTo) {
        names.push_back("valid_to(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
        names.push_back("commit_seq(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kOperation) {
        names.push_back("operation(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
        names.push_back("system_time(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kProperty) {
        names.push_back(expression.variable + "." + expression.property_name);
      } else {
        names.push_back(expression.variable);
      }
    }
    for (size_t index = begin; index < end; ++index) {
      const RangeEdgeRow& row = rows[index];
      for (size_t output = 0; output < statement.returns.size(); ++output) {
        const ReturnExpression& expression = statement.returns[output];
        if (expression.kind == ReturnExpressionKind::kBinding) {
          const uint64_t id = expression.variable == statement.match.variable
                                  ? row.source_id : row.target_id;
          if (id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
          }
          columns[output].push_back(Value::Int64(static_cast<int64_t>(id)));
        } else if (expression.kind == ReturnExpressionKind::kProperty) {
          const auto property = std::find_if(property_projections.begin(), property_projections.end(),
              [output](const PropertyProjection& projection) { return projection.output == output; });
          if (property == property_projections.end()) {
            return Status::Corruption("T-Cypher executor", "range property projection is absent");
          }
          const size_t property_index = static_cast<size_t>(property - property_projections.begin());
          const std::optional<Value>& value = row.properties[property_index];
          columns[output].push_back(value.value_or(Value::Binary("")));
          validity[output].push_back(value.has_value());
        } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
          columns[output].push_back(Value::Timestamp(row.valid_from));
        } else if (expression.kind == ReturnExpressionKind::kValidTo) {
          columns[output].push_back(Value::Timestamp(row.valid_to));
        } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
          columns[output].push_back(Value::Int64(static_cast<int64_t>(row.commit_seq)));
        } else if (expression.kind == ReturnExpressionKind::kOperation) {
          columns[output].push_back(
              Value::String(row.operation == TemporalOperation::kDelete ? "DELETE" : "PUT"));
        } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
          const auto system_time = SystemTimeForCommit(context.commit_timeline, row.commit_seq);
          if (!system_time.ok()) return system_time.status();
          columns[output].push_back(Value::Timestamp(system_time.ValueOrDie()));
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    for (size_t output = 0; output < columns.size(); ++output) {
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(columns[output]), std::move(validity[output])));
      if (!added.ok()) return added;
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteVariableExpandRange(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq) {
  if (statement.relationships.size() != 1 || statement.expanded_nodes.size() != 1) {
    return Status::NotSupported("T-Cypher executor",
                                "only one variable range relationship is available");
  }
  const MatchRelationshipPattern& relationship = statement.relationships.front();
  const MatchNodePattern& target = statement.expanded_nodes.front();
  if (!relationship.variable_length) {
    return Status::NotSupported("T-Cypher executor",
                                "variable range relationship is required");
  }
  if (!target.label.empty()) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->logical_type != target.label) {
      return Status::BindError("T-Cypher executor", "target vertex label is not registered");
    }
  }
  struct PropertyProjection {
    size_t output;
    std::string variable;
    uint16_t column_id;
  };
  std::vector<PropertyProjection> property_projections;
  for (size_t output = 0; output < statement.returns.size(); ++output) {
    const ReturnExpression& expression = statement.returns[output];
    if (expression.variable != statement.match.variable && expression.variable != target.variable) {
      return Status::NotSupported("T-Cypher executor",
                                  "variable range path projection is not available yet");
    }
    if (expression.kind == ReturnExpressionKind::kProperty ||
        expression.kind == ReturnExpressionKind::kSum ||
        expression.kind == ReturnExpressionKind::kAvg ||
        expression.kind == ReturnExpressionKind::kMin ||
        expression.kind == ReturnExpressionKind::kMax) {
      const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                           expression.property_name);
      if (!schema.has_value()) {
        return Status::BindError("T-Cypher executor", "projected property is not registered");
      }
      property_projections.push_back(PropertyProjection{output, expression.variable,
                                                        schema->column_id});
    } else if (expression.kind != ReturnExpressionKind::kBinding &&
               expression.kind != ReturnExpressionKind::kCount &&
               expression.kind != ReturnExpressionKind::kValidFrom &&
               expression.kind != ReturnExpressionKind::kValidTo) {
      return Status::NotSupported("T-Cypher executor",
                                  "variable range path projection is not available yet");
    }
  }
  const EntityType direction = relationship.direction == RelationshipDirection::kOutgoing
                                   ? EntityType::EdgeOut
                                   : EntityType::EdgeIn;
  std::optional<uint16_t> edge_type;
  if (!relationship.type.empty()) {
    const auto schema = FindColumnByName(*context.schema_snapshot, direction, relationship.type);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "relationship type is not registered");
    }
    edge_type = schema->column_id;
  }

  QueryMemoryReservation memory(context.options.memory_account);

  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent&) { return true; });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  struct EdgeInterval {
    uint64_t source_id;
    uint64_t target_id;
    LogicalKey identity;
    uint64_t valid_from;
    uint64_t valid_to;
  };
  std::vector<EdgeInterval> edges;
  for (const auto& entry : events_by_key) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const LogicalKey& edge_key = entry.first;
    if (edge_key.entity_type() != direction || edge_key.kind() != LogicalKeyKind::kExistence ||
        (edge_type.has_value() && edge_key.edge_type() != *edge_type)) {
      continue;
    }
    const LogicalKey source_key = LogicalKey::VertexExistence(edge_key.entity_id());
    const LogicalKey target_key = LogicalKey::VertexExistence(edge_key.target_id());
    const auto source_events = events_by_key.find(source_key);
    const auto target_events = events_by_key.find(target_key);
    if (source_events == events_by_key.end() || target_events == events_by_key.end()) continue;
    const auto edge_intervals = DeriveVisibleIntervals(entry.second, edge_key, snapshot_seq,
                                                       range_start, range_end);
    const auto source_intervals = DeriveVisibleIntervals(source_events->second, source_key,
                                                         snapshot_seq, range_start, range_end);
    const auto target_intervals = DeriveVisibleIntervals(target_events->second, target_key,
                                                         snapshot_seq, range_start, range_end);
    if (!edge_intervals.ok()) return edge_intervals.status();
    if (!source_intervals.ok()) return source_intervals.status();
    if (!target_intervals.ok()) return target_intervals.status();
    for (const TemporalInterval& edge : edge_intervals.ValueOrDie()) {
      if (edge.event.is_delete()) continue;
      for (const TemporalInterval& source : source_intervals.ValueOrDie()) {
        if (source.event.is_delete()) continue;
        for (const TemporalInterval& destination : target_intervals.ValueOrDie()) {
          if (destination.event.is_delete()) continue;
          const uint64_t valid_from = std::max(
              edge.valid_from, std::max(source.valid_from, destination.valid_from));
          const uint64_t valid_to = std::min(
              edge.valid_to, std::min(source.valid_to, destination.valid_to));
          if (valid_from < valid_to) {
            const Status reserved = memory.Reserve(sizeof(EdgeInterval));
            if (!reserved.ok()) return reserved;
            edges.push_back(EdgeInterval{edge_key.entity_id(), edge_key.target_id(), edge_key,
                                         valid_from, valid_to});
          }
        }
      }
    }
  }
  struct PathState {
    uint64_t start_id;
    uint64_t endpoint_id;
    uint32_t hops;
    uint64_t valid_from;
    uint64_t valid_to;
    std::set<LogicalKey> visited_edges;
  };
  struct PathRow {
    uint64_t start_id;
    uint64_t endpoint_id;
    uint64_t valid_from;
    uint64_t valid_to;
    std::vector<std::optional<Value>> properties;
  };
  std::vector<PathState> frontier;
  std::vector<PathRow> rows;
  for (const EdgeInterval& edge : edges) {
    PathState state{edge.source_id, edge.target_id, 1, edge.valid_from, edge.valid_to,
                    {edge.identity}};
    if (state.hops >= relationship.min_hops) {
      const Status reserved = memory.Reserve(sizeof(PathRow));
      if (!reserved.ok()) return reserved;
      rows.push_back(
          PathRow{state.start_id, state.endpoint_id, state.valid_from, state.valid_to, {}});
    }
    if (state.hops < relationship.max_hops) {
      const Status reserved = memory.Reserve(sizeof(PathState) + sizeof(LogicalKey));
      if (!reserved.ok()) return reserved;
      frontier.push_back(std::move(state));
    }
  }
  while (!frontier.empty()) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    std::vector<PathState> next_frontier;
    for (const PathState& state : frontier) {
      for (const EdgeInterval& edge : edges) {
        if (edge.source_id != state.endpoint_id || state.visited_edges.count(edge.identity) != 0) {
          continue;
        }
        const uint64_t valid_from = std::max(state.valid_from, edge.valid_from);
        const uint64_t valid_to = std::min(state.valid_to, edge.valid_to);
        if (valid_from >= valid_to) continue;
        PathState next = state;
        next.endpoint_id = edge.target_id;
        ++next.hops;
        next.valid_from = valid_from;
        next.valid_to = valid_to;
        next.visited_edges.insert(edge.identity);
        if (next.hops >= relationship.min_hops) {
          const Status reserved = memory.Reserve(sizeof(PathRow));
          if (!reserved.ok()) return reserved;
          rows.push_back(
              PathRow{next.start_id, next.endpoint_id, next.valid_from, next.valid_to, {}});
        }
        if (next.hops < relationship.max_hops) {
          const Status reserved = memory.Reserve(
              sizeof(PathState) + next.visited_edges.size() * sizeof(LogicalKey));
          if (!reserved.ok()) return reserved;
          next_frontier.push_back(std::move(next));
        }
      }
    }
    frontier = std::move(next_frontier);
  }
  if (!property_projections.empty()) {
    std::vector<PathRow> aligned_rows;
    for (const PathRow& row : rows) {
      std::vector<std::vector<TemporalInterval>> streams;
      streams.push_back({TemporalInterval{
          TemporalEvent::Put(LogicalKey::VertexExistence(row.start_id), row.valid_from, 0, 0,
                             Value::Binary("")), row.valid_from, row.valid_to}});
      for (const PropertyProjection& projection : property_projections) {
        const LogicalKey key = LogicalKey::VertexProperty(
            projection.variable == statement.match.variable ? row.start_id : row.endpoint_id,
            projection.column_id);
        const auto found = events_by_key.find(key);
        const std::vector<TemporalEvent> no_events;
        auto intervals = DeriveVisibleIntervals(found == events_by_key.end() ? no_events : found->second,
                                                key, snapshot_seq, row.valid_from, row.valid_to);
        if (!intervals.ok()) return intervals.status();
        std::vector<TemporalInterval> property_intervals = intervals.ConsumeValueOrDie();
        if (property_intervals.empty()) {
          property_intervals.push_back(
              TemporalInterval{TemporalEvent::Delete(key, 0, 0, 0), 0, kTemporalInfinity});
        } else if (property_intervals.front().valid_from > 0) {
          property_intervals.insert(property_intervals.begin(),
              TemporalInterval{TemporalEvent::Delete(key, 0, 0, 0), 0,
                               property_intervals.front().valid_from});
        }
        streams.push_back(std::move(property_intervals));
      }
      const auto aligned = AlignTemporalIntervals(streams, row.valid_from, row.valid_to);
      if (!aligned.ok()) return aligned.status();
      for (const AlignedTemporalInterval& interval : aligned.ValueOrDie()) {
        PathRow projected = row;
        projected.valid_from = interval.valid_from;
        projected.valid_to = interval.valid_to;
        projected.properties.clear();
        projected.properties.reserve(property_projections.size());
        for (size_t index = 0; index < property_projections.size(); ++index) {
          const auto& fact = interval.facts[index + 1];
          projected.properties.push_back(!fact || fact->is_delete()
              ? std::nullopt : std::optional<Value>(fact->value()));
        }
        aligned_rows.push_back(std::move(projected));
      }
    }
    rows = std::move(aligned_rows);
  }
  std::sort(rows.begin(), rows.end(), [](const PathRow& left, const PathRow& right) {
    if (left.start_id != right.start_id) return left.start_id < right.start_id;
    if (left.endpoint_id != right.endpoint_id) return left.endpoint_id < right.endpoint_id;
    if (left.valid_from != right.valid_from) return left.valid_from < right.valid_from;
    return left.valid_to < right.valid_to;
  });

  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < rows.size(); begin += context.options.batch_capacity) {
    const Status cancelled = CheckQueryCancelled(context);
    if (!cancelled.ok()) return cancelled;
    const size_t end = std::min(rows.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(statement.returns.size());
    std::vector<std::vector<bool>> validity(statement.returns.size());
    for (std::vector<Value>& column : columns) column.reserve(end - begin);
    std::vector<std::string> names;
    names.reserve(statement.returns.size());
    for (const ReturnExpression& expression : statement.returns) {
      if (expression.kind == ReturnExpressionKind::kValidFrom) {
        names.push_back("valid_from(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kValidTo) {
        names.push_back("valid_to(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kProperty) {
        names.push_back(expression.variable + "." + expression.property_name);
      } else if (expression.kind == ReturnExpressionKind::kSum) {
        names.push_back("sum(" + expression.variable + "." + expression.property_name + ")");
      } else if (expression.kind == ReturnExpressionKind::kAvg) {
        names.push_back("avg(" + expression.variable + "." + expression.property_name + ")");
      } else if (expression.kind == ReturnExpressionKind::kMin) {
        names.push_back("min(" + expression.variable + "." + expression.property_name + ")");
      } else if (expression.kind == ReturnExpressionKind::kMax) {
        names.push_back("max(" + expression.variable + "." + expression.property_name + ")");
      } else {
        names.push_back(expression.variable);
      }
    }
    for (size_t index = begin; index < end; ++index) {
      const PathRow& row = rows[index];
      for (size_t output = 0; output < statement.returns.size(); ++output) {
        const ReturnExpression& expression = statement.returns[output];
        if (expression.kind == ReturnExpressionKind::kBinding ||
            expression.kind == ReturnExpressionKind::kCount) {
          const uint64_t id = expression.variable == statement.match.variable
                                  ? row.start_id : row.endpoint_id;
          if (id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
          }
          columns[output].push_back(Value::Int64(static_cast<int64_t>(id)));
        } else if (expression.kind == ReturnExpressionKind::kProperty ||
                   expression.kind == ReturnExpressionKind::kSum ||
                   expression.kind == ReturnExpressionKind::kAvg ||
                   expression.kind == ReturnExpressionKind::kMin ||
                   expression.kind == ReturnExpressionKind::kMax) {
          const auto property = std::find_if(property_projections.begin(), property_projections.end(),
              [output](const PropertyProjection& projection) { return projection.output == output; });
          if (property == property_projections.end()) {
            return Status::Corruption("T-Cypher executor", "path property projection is absent");
          }
          const size_t property_index = static_cast<size_t>(property - property_projections.begin());
          const std::optional<Value>& value = row.properties[property_index];
          columns[output].push_back(value.value_or(Value::Binary("")));
          validity[output].push_back(value.has_value());
        } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
          columns[output].push_back(Value::Timestamp(row.valid_from));
        } else {
          columns[output].push_back(Value::Timestamp(row.valid_to));
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    for (size_t output = 0; output < columns.size(); ++output) {
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(columns[output]), std::move(validity[output])));
      if (!added.ok()) return added;
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  return BuildPathResultStream(context, std::move(batches), &memory);
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteValidTimeStateRange(
    const TcypherStatement& statement, const TcypherExecutionContext& context,
    uint64_t range_start, uint64_t range_end, uint64_t snapshot_seq) {
  if (!statement.relationships.empty()) {
    return Status::NotSupported("T-Cypher executor",
                                "range relationship execution is not available yet");
  }
  auto grouped = GroupCommittedEvents(context, [](const TemporalEvent& event) {
    return event.logical_key().entity_type() == EntityType::Vertex;
  });
  if (!grouped.ok()) return grouped.status();
  auto events_by_key = std::move(grouped).ConsumeValueOrDie();
  std::vector<uint16_t> property_ids;
  std::map<uint16_t, size_t> property_streams;
  for (const ReturnExpression& expression : statement.returns) {
    if (expression.variable != statement.match.variable) {
      return Status::NotSupported("T-Cypher executor", "only root vertex range projections are available yet");
    }
    if (expression.kind != ReturnExpressionKind::kProperty) continue;
    const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                         expression.property_name);
    if (!schema.has_value()) {
      return Status::BindError("T-Cypher executor", "projected property is not registered");
    }
    if (property_streams.emplace(schema->column_id, property_ids.size() + 1).second) {
      property_ids.push_back(schema->column_id);
    }
  }

  struct RangeRow {
    uint64_t entity_id;
    uint64_t valid_from;
    uint64_t valid_to;
    uint64_t commit_seq;
    TemporalOperation operation;
    std::map<uint16_t, std::optional<Value>> properties;
  };
  std::vector<RangeRow> rows;
  for (const auto& entry : events_by_key) {
    if (entry.first.kind() != LogicalKeyKind::kExistence) continue;
    const auto intervals = DeriveVisibleIntervals(entry.second, entry.first, snapshot_seq,
                                                  range_start, range_end);
    if (!intervals.ok()) return intervals.status();
    std::vector<std::vector<TemporalInterval>> streams;
    streams.push_back(intervals.ValueOrDie());
    for (uint16_t property_id : property_ids) {
      const LogicalKey property_key = LogicalKey::VertexProperty(entry.first.entity_id(), property_id);
      const auto property_events = events_by_key.find(property_key);
      const std::vector<TemporalEvent> no_events;
      const auto derived_property_intervals = DeriveVisibleIntervals(
          property_events == events_by_key.end() ? no_events : property_events->second,
          property_key, snapshot_seq, range_start, range_end);
      if (!derived_property_intervals.ok()) return derived_property_intervals.status();
      std::vector<TemporalInterval> property_intervals =
          derived_property_intervals.ValueOrDie();
      if (property_intervals.empty() || property_intervals.front().valid_from > 0) {
        const uint64_t absent_until = property_intervals.empty()
                                          ? kTemporalInfinity
                                          : property_intervals.front().valid_from;
        property_intervals.insert(
            property_intervals.begin(),
            TemporalInterval{TemporalEvent::Delete(property_key, 0, 0, 0), 0, absent_until});
      }
      streams.push_back(std::move(property_intervals));
    }
    const auto aligned = AlignTemporalIntervals(streams, range_start, range_end);
    if (!aligned.ok()) return aligned.status();
    const auto coalesced = CoalesceTemporalIntervals(aligned.ValueOrDie(), true);
    if (!coalesced.ok()) return coalesced.status();
    for (const AlignedTemporalInterval& interval : coalesced.ValueOrDie()) {
      if (!interval.facts.front() || interval.facts.front()->is_delete()) continue;
      RangeRow row{entry.first.entity_id(), interval.valid_from, interval.valid_to,
                   interval.facts.front()->commit_seq(), interval.facts.front()->operation(), {}};
      for (size_t index = 0; index < property_ids.size(); ++index) {
        const auto& property_fact = interval.facts[index + 1];
        row.properties.emplace(property_ids[index],
                               property_fact->is_delete()
                                   ? std::optional<Value>{}
                                   : std::optional<Value>{property_fact->value()});
      }
      rows.push_back(std::move(row));
    }
  }
  std::sort(rows.begin(), rows.end(), [](const RangeRow& left, const RangeRow& right) {
    if (left.entity_id != right.entity_id) return left.entity_id < right.entity_id;
    return left.valid_from < right.valid_from;
  });

  std::vector<ResultBatch> batches;
  for (size_t begin = 0; begin < rows.size(); begin += context.options.batch_capacity) {
    const size_t end = std::min(rows.size(), begin + context.options.batch_capacity);
    std::vector<std::vector<Value>> columns(statement.returns.size());
    std::vector<std::vector<bool>> validity(statement.returns.size());
    for (std::vector<Value>& column : columns) column.reserve(end - begin);
    for (std::vector<bool>& column : validity) column.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
      const RangeRow& row = rows[index];
      if (row.entity_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument("T-Cypher executor", "entity id exceeds Int64");
      }
      for (size_t output = 0; output < statement.returns.size(); ++output) {
        const ReturnExpression& expression = statement.returns[output];
        switch (expression.kind) {
          case ReturnExpressionKind::kBinding:
            columns[output].push_back(Value::Int64(static_cast<int64_t>(row.entity_id)));
            validity[output].push_back(true);
            break;
          case ReturnExpressionKind::kProperty: {
            const auto schema = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                                 expression.property_name);
            const std::optional<Value>& value = row.properties.at(schema->column_id);
            columns[output].push_back(value.has_value() ? *value : Value::Binary(""));
            validity[output].push_back(value.has_value());
            break;
          }
          case ReturnExpressionKind::kValidFrom:
            columns[output].push_back(Value::Timestamp(row.valid_from));
            validity[output].push_back(true);
            break;
          case ReturnExpressionKind::kValidTo:
            columns[output].push_back(Value::Timestamp(row.valid_to));
            validity[output].push_back(true);
            break;
          case ReturnExpressionKind::kCommitSeq:
            if (row.commit_seq > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
              return Status::InvalidArgument("T-Cypher executor", "commit sequence exceeds Int64");
            }
            columns[output].push_back(Value::Int64(static_cast<int64_t>(row.commit_seq)));
            validity[output].push_back(true);
            break;
          case ReturnExpressionKind::kOperation:
            columns[output].push_back(
                Value::String(row.operation == TemporalOperation::kDelete ? "DELETE" : "PUT"));
            validity[output].push_back(true);
            break;
          case ReturnExpressionKind::kSystemTime: {
            const auto system_time = SystemTimeForCommit(context.commit_timeline, row.commit_seq);
            if (!system_time.ok()) return system_time.status();
            columns[output].push_back(Value::Timestamp(system_time.ValueOrDie()));
            validity[output].push_back(true);
            break;
          }
          case ReturnExpressionKind::kCount:
          case ReturnExpressionKind::kSum:
          case ReturnExpressionKind::kAvg:
          case ReturnExpressionKind::kMin:
          case ReturnExpressionKind::kMax:
          case ReturnExpressionKind::kCollect:
            return Status::NotSupported("T-Cypher executor",
                                        "range numeric aggregation is not available yet");
        }
      }
    }
    ColumnBatch batch(context.options.batch_capacity);
    std::vector<std::string> names;
    names.reserve(statement.returns.size());
    for (size_t output = 0; output < statement.returns.size(); ++output) {
      const ReturnExpression& expression = statement.returns[output];
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(std::move(columns[output]), std::move(validity[output])));
      if (!added.ok()) return added;
      if (expression.kind == ReturnExpressionKind::kProperty) {
        names.push_back(expression.variable + "." + expression.property_name);
      } else if (expression.kind == ReturnExpressionKind::kOperation) {
        names.push_back("operation(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
        names.push_back("commit_seq(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
        names.push_back("valid_from(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kValidTo) {
        names.push_back("valid_to(" + expression.variable + ")");
      } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
        names.push_back("system_time(" + expression.variable + ")");
      } else {
        names.push_back(expression.variable);
      }
    }
    ResultBatch result(std::move(names), std::move(batch),
                       ResultTemporalMetadata{snapshot_seq, true, true});
    const Status valid = result.Validate();
    if (!valid.ok()) return valid;
    batches.push_back(std::move(result));
  }
  std::unique_ptr<QueryResultStream> stream =
      std::make_unique<InMemoryResultStream>(std::move(batches), Status::OK());
  return stream;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteCreate(
    const BoundTcypherStatement& bound, const TcypherExecutionContext& context) {
  if (context.transaction_coordinator == nullptr || !bound.syntax.mutation.has_value() ||
      !bound.mutation_valid_from.has_value()) {
    return Status::InvalidArgument("T-Cypher executor", "missing CREATE execution context");
  }
  const TemporalMutation& mutation = *bound.syntax.mutation;
  const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
  const auto property = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                         mutation.property_name);
  if (!existence.has_value() || existence->column_id != 0 ||
      existence->physical_type != PhysicalType::kBinary || !property.has_value() ||
      property->physical_type != PhysicalType::kString) {
    return Status::SchemaMismatch("T-Cypher executor", "CREATE schema is not registered");
  }
  const auto vertex_id = context.transaction_coordinator->AllocateVertexId();
  if (!vertex_id.ok()) return vertex_id.status();
  TcypherSession* session = context.session != nullptr && context.session->active()
      ? context.session : nullptr;
  const uint64_t snapshot_seq = context.visible_seq_ceiling;
  TransactionSink sink(context.transaction_coordinator, snapshot_seq,
                       context.options.cancellation, session);
  uint64_t commit_seq = 0;
  const Status submitted = sink.Submit({
      TypedMutation::Put(LogicalKey::VertexExistence(vertex_id.ValueOrDie()),
                         *bound.mutation_valid_from, existence->schema_epoch, Value::Binary("")),
      TypedMutation::Put(LogicalKey::VertexProperty(vertex_id.ValueOrDie(), property->column_id),
                         *bound.mutation_valid_from, property->schema_epoch,
                         Value::String(mutation.string_value)),
  }, &commit_seq);
  if (!submitted.ok()) return submitted;
  std::unique_ptr<QueryResultStream> result =
      std::make_unique<InMemoryResultStream>(std::vector<ResultBatch>{}, Status::OK());
  return result;
}

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteExactMutation(
    const BoundTcypherStatement& bound, const TcypherExecutionContext& context) {
  if (context.transaction_coordinator == nullptr || !bound.syntax.mutation.has_value() ||
      !bound.mutation_valid_from.has_value() || !bound.mutation_target_entity_id.has_value()) {
    return Status::InvalidArgument("T-Cypher executor", "missing exact mutation context");
  }
  const uint64_t entity_id = *bound.mutation_target_entity_id;
  const uint64_t valid_from = *bound.mutation_valid_from;
  TcypherSession* session = context.session != nullptr && context.session->active()
      ? context.session : nullptr;
  const uint64_t snapshot_seq = context.visible_seq_ceiling;
  const LogicalKey existence_key = LogicalKey::VertexExistence(entity_id);
  std::optional<TemporalEvent> visible_event;
  if (session != nullptr) {
    const auto existence_events = CollectCommittedEventsForKey(context, existence_key);
    if (!existence_events.ok()) return existence_events.status();
    visible_event = ResolveVisibleEvent(existence_events.ValueOrDie(), existence_key,
                                        valid_from, snapshot_seq);
  } else {
    const auto visible = context.transaction_coordinator->GetChecked(
        existence_key, valid_from, snapshot_seq);
    if (!visible.ok()) return visible.status();
    if (visible.ValueOrDie().has_value()) {
      visible_event = TemporalEvent::Put(existence_key, valid_from, snapshot_seq, 0,
                                         *visible.ValueOrDie());
    }
  }
  if (session != nullptr && session->mode() == TcypherSessionMode::kStrict) {
    const Status read_status = session->RecordRead(
        TransactionCoordinator::StrictReadPoint{
            LogicalKey::VertexExistence(entity_id), valid_from});
    if (!read_status.ok()) return read_status;
  }
  if (!visible_event.has_value() || visible_event->is_delete()) {
    return Status::Conflict("T-Cypher executor", "mutation target is not visible at VALID FROM");
  }

  const TemporalMutation& mutation = *bound.syntax.mutation;
  TypedMutation typed_mutation = TypedMutation::Delete(
      LogicalKey::VertexExistence(entity_id), valid_from, 0);
  if (bound.syntax.kind == TcypherStatementKind::kSet) {
    const auto property = FindColumnByName(*context.schema_snapshot, EntityType::Vertex,
                                           mutation.property_name);
    if (!property.has_value()) {
      return Status::BindError("T-Cypher executor", "updated property is not registered");
    }
    if (property->physical_type != PhysicalType::kString) {
      return Status::SchemaMismatch("T-Cypher executor", "updated string property has wrong type");
    }
    typed_mutation = TypedMutation::Put(
        LogicalKey::VertexProperty(entity_id, property->column_id), valid_from,
        property->schema_epoch, Value::String(mutation.string_value));
  } else if (bound.syntax.kind == TcypherStatementKind::kDelete) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->physical_type != PhysicalType::kBinary) {
      return Status::SchemaMismatch("T-Cypher executor", "vertex existence schema is not registered");
    }
    typed_mutation = TypedMutation::Delete(LogicalKey::VertexExistence(entity_id), valid_from,
                                           existence->schema_epoch);
  } else {
    return Status::InvalidArgument("T-Cypher executor", "unknown exact mutation kind");
  }

  TransactionSink sink(context.transaction_coordinator, snapshot_seq,
                       context.options.cancellation, session);
  uint64_t commit_seq = 0;
  const Status submitted = sink.Submit({std::move(typed_mutation)}, &commit_seq);
  if (!submitted.ok()) return submitted;
  std::unique_ptr<QueryResultStream> result =
      std::make_unique<InMemoryResultStream>(std::vector<ResultBatch>{}, Status::OK());
  return result;
}

uint64_t RuntimeFeedbackPlanShape(const PhysicalPlan& plan) {
  uint64_t hash = 1469598103934665603ULL;
  const auto mix = [&hash](uint64_t value) {
    for (uint32_t byte = 0; byte < 8; ++byte) {
      hash ^= static_cast<uint8_t>(value >> (byte * 8));
      hash *= 1099511628211ULL;
    }
  };
  mix(static_cast<uint8_t>(plan.temporal_mode()));
  for (const PhysicalOperatorSpec& op : plan.operators()) {
    mix(static_cast<uint8_t>(op.kind));
  }
  for (const PhysicalPredicate& predicate : plan.predicates()) {
    mix(predicate.slot.value);
    mix(static_cast<uint8_t>(predicate.kind));
    mix(static_cast<uint8_t>(predicate.type));
    mix(static_cast<uint8_t>(predicate.column.entity_type));
    mix(predicate.column.column_id);
  }
  return hash == 0 ? 1 : hash;
}

uint64_t RuntimeFeedbackSchemaEpochs(const PhysicalPlan& plan) {
  uint64_t hash = 1469598103934665603ULL;
  for (const PhysicalPredicate& predicate : plan.predicates()) {
    hash ^= predicate.column.schema_epoch;
    hash *= 1099511628211ULL;
  }
  return hash == 0 ? 1 : hash;
}

std::optional<uint64_t> EstimateIndexedCandidateRows(
    const PhysicalPlan& plan, TcypherExecutionContext* context,
    const IndexDefinition& definition, size_t predicate_index) {
  if (context == nullptr || predicate_index >= plan.predicates().size()) {
    return std::nullopt;
  }
  const PhysicalPredicate& predicate = plan.predicates()[predicate_index];
  if ((predicate.kind != PhysicalPredicateKind::kEquality &&
       predicate.kind != PhysicalPredicateKind::kIn) ||
      predicate.values.empty()) {
    return std::nullopt;
  }
  uint64_t candidates = 0;
  bool observed_source = false;
  std::vector<IndexCanonicalValue> canonical_values;
  canonical_values.reserve(predicate.values.size() * 2);
  for (const Value& value : predicate.values) {
    const auto canonical = EncodeIndexCanonicalValue(value);
    if (!canonical.ok()) return std::nullopt;
    canonical_values.push_back(canonical.ValueOrDie());
    if (value.type() == PhysicalType::kString ||
        value.type() == PhysicalType::kBinary) {
      const auto hash = EncodeIndexBlobHash(value);
      if (!hash.ok()) return std::nullopt;
      canonical_values.push_back(hash.ValueOrDie());
    }
  }
  const auto add = [&candidates](uint64_t count) {
    candidates = count > std::numeric_limits<uint64_t>::max() - candidates
        ? std::numeric_limits<uint64_t>::max()
        : candidates + count;
  };
  constexpr uint64_t kFeedbackSidecarByteLimit = 256U << 10;
  constexpr uint64_t kFeedbackSidecarPostingLimit = 4096;
  for (TcypherIndexSource& source : context->index_sources) {
    if (source.index_id != definition.index_id) continue;
    if (source.sidecar.source_sst_id != source.source_sst_id) {
      if (!source.pinned_sst_source.has_value() ||
          !source.definition.has_value() || !source.fragment.has_value() ||
          source.definition->index_id != source.index_id ||
          source.fragment->index_id != source.index_id ||
          source.fragment->source_sst_id != source.source_sst_id ||
          source.pinned_sst_source->metadata.file_number !=
              source.source_sst_id ||
          !source.fragment->usable || source.sidecar_path.empty()) {
        return std::nullopt;
      }
      if (context->options.cancellation &&
          context->options.cancellation->IsCancelled()) {
        return std::nullopt;
      }
      std::error_code sidecar_error;
      const uint64_t sidecar_bytes = std::filesystem::file_size(
          source.sidecar_path, sidecar_error);
      const uint64_t posting_count = source.fragment->indexed_put_count;
      if (sidecar_error || sidecar_bytes > kFeedbackSidecarByteLimit ||
          posting_count > kFeedbackSidecarPostingLimit ||
          sidecar_bytes > std::numeric_limits<uint64_t>::max() / 2) {
        return std::nullopt;
      }
      uint64_t charge = 2 * sidecar_bytes;
      if (posting_count >
          (std::numeric_limits<uint64_t>::max() - charge) /
              (2 * sizeof(IndexPosting))) {
        return std::nullopt;
      }
      charge += posting_count * 2 * sizeof(IndexPosting);
      std::shared_ptr<QueryMemoryLease> lease;
      if (context->options.memory_account && charge != 0) {
        const Status reserved = context->options.memory_account->Reserve(charge);
        if (!reserved.ok()) return std::nullopt;
        lease = std::make_shared<QueryMemoryLease>(
            context->options.memory_account, charge);
      }
      auto materialized = ReadVerifiedIndexSidecarFile(
          source.sidecar_path, *source.definition, source.source_sst_id,
          source.fragment->identity_checksum);
      if (!materialized.ok()) {
        source.fragment->usable = false;
        if (context->transaction_coordinator != nullptr) {
          context->transaction_coordinator->ReportIndexHealthEvent(
              source.index_id, source.source_sst_id,
              context->index_catalog_snapshot->catalog_generation,
              IndexHealthFailureClass::kCorruptSidecar).IgnoreError();
        }
        return std::nullopt;
      }
      if (materialized.ValueOrDie().postings.size() != posting_count) {
        source.fragment->usable = false;
        if (context->transaction_coordinator != nullptr) {
          context->transaction_coordinator->ReportIndexHealthEvent(
              source.index_id, source.source_sst_id,
              context->index_catalog_snapshot->catalog_generation,
              IndexHealthFailureClass::kPostingCountMismatch).IgnoreError();
        }
        return std::nullopt;
      }
      source.sidecar = std::move(materialized).ConsumeValueOrDie();
      if (lease) context->runtime_feedback_index_leases.push_back(std::move(lease));
      if (context->options.execution_stats) {
        context->options.execution_stats->index_advisory_sidecar_bytes_read +=
            sidecar_bytes;
      }
    }
    for (const Value& value : predicate.values) {
      const auto postings = LookupIndexEquality(source.sidecar, value);
      if (!postings.ok()) return std::nullopt;
      add(postings.ValueOrDie().size());
    }
    observed_source = true;
  }
  for (const TcypherDeltaIndexSource& source : context->delta_index_sources) {
    if (source.index_id != definition.index_id) continue;
    bool indexed_source = true;
    for (const Value& value : predicate.values) {
      const auto ordinals = source.index.Lookup(
          source.source_generation, value);
      if (!ordinals.ok()) {
        indexed_source = false;
        break;
      }
      add(ordinals.ValueOrDie().size());
    }
    if (indexed_source) {
      observed_source = true;
      continue;
    }
    if (!source.pinned_memtable) return std::nullopt;
    const Status visited = source.pinned_memtable->VisitEvents(
        [&](const TemporalEvent& event) {
          if (!IsIndexedPropertyEvent(event, predicate.column) ||
              event.is_delete()) {
            return Status::OK();
          }
          const auto event_value = event.is_blob_reference()
              ? StatusOr<IndexCanonicalValue>(
                    EncodeIndexBlobHash(*event.blob_ref()))
              : EncodeIndexCanonicalValue(event.value());
          if (!event_value.ok()) return event_value.status();
          for (const IndexCanonicalValue& wanted : canonical_values) {
            if (CompareIndexCanonicalValues(event_value.ValueOrDie(), wanted) ==
                0) {
              add(1);
              break;
            }
          }
          return Status::OK();
        });
    if (!visited.ok()) return std::nullopt;
    observed_source = true;
  }
  return observed_source ? std::optional<uint64_t>(candidates)
                         : std::nullopt;
}

void PrepareRootRuntimeFeedback(const PhysicalPlan& plan,
                                TcypherExecutionContext* context) {
  if (context == nullptr ||
      plan.temporal_mode() != PhysicalTemporalMode::kPoint ||
      plan.predicate_properties().empty() || plan.predicates().empty() ||
      !context->index_catalog_snapshot || !context->version_snapshot ||
      !context->statistics_snapshot) {
    return;
  }
  struct PredicateEstimate {
    size_t predicate_index = 0;
    uint64_t index_id = 0;
    uint64_t candidate_rows = 0;
    uint64_t base_rows = 0;
    uint64_t uncovered_base_rows = 0;
  };
  std::vector<PredicateEstimate> predicate_estimates;
  for (size_t predicate_index = 0;
       predicate_index < plan.predicates().size(); ++predicate_index) {
    const PhysicalPredicate& predicate = plan.predicates()[predicate_index];
    const auto property = std::find_if(
        plan.predicate_properties().begin(),
        plan.predicate_properties().end(),
        [&predicate](const PhysicalPropertySlot& candidate) {
          return candidate.slot == predicate.slot;
        });
    const BindingId predicate_binding =
        property == plan.predicate_properties().end() ||
                property->binding.value == 0
            ? plan.binding_id()
            : property->binding;
    if (predicate_binding != plan.binding_id()) continue;
    if ((predicate.kind != PhysicalPredicateKind::kEquality &&
         predicate.kind != PhysicalPredicateKind::kIn) ||
        predicate.values.empty()) {
      continue;
    }
    const auto legal_index = std::find_if(
        context->index_catalog_snapshot->definitions.begin(),
        context->index_catalog_snapshot->definitions.end(),
        [&predicate](const IndexDefinition& definition) {
          return definition.state == IndexState::kActive &&
              definition.entity_type == predicate.column.entity_type &&
              definition.column_id == predicate.column.column_id &&
              definition.schema_epoch == predicate.column.schema_epoch &&
              IsSupportedIndexCanonicalEncoding(
                  definition.canonical_encoding_id) &&
              (definition.capabilities & kIndexEquality) != 0;
        });
    if (legal_index ==
        context->index_catalog_snapshot->definitions.end()) {
      continue;
    }
    const auto snapshot = context->statistics_snapshot->SnapshotFor(
        *context->version_snapshot, predicate.column.entity_type,
        predicate.column.column_id);
    if (!snapshot.ok() || !snapshot.ValueOrDie().complete ||
        snapshot.ValueOrDie().aggregate.row_count == 0) {
      continue;
    }
    const auto candidate_rows = EstimateIndexedCandidateRows(
        plan, context, *legal_index, predicate_index);
    if (!candidate_rows.has_value()) continue;
    uint64_t uncovered_base_rows = 0;
    const auto add_uncovered = [&](uint64_t rows) {
      uncovered_base_rows = rows >
              std::numeric_limits<uint64_t>::max() - uncovered_base_rows
          ? std::numeric_limits<uint64_t>::max()
          : uncovered_base_rows + rows;
    };
    for (const SstFileMeta& file : context->version_snapshot->files) {
      if (file.partition.entity_type != predicate.column.entity_type ||
          file.partition.column_id != predicate.column.column_id ||
          file.partition.schema_epoch != predicate.column.schema_epoch) {
        continue;
      }
      const auto fragment = std::find_if(
          context->index_catalog_snapshot->fragments.begin(),
          context->index_catalog_snapshot->fragments.end(),
          [&](const IndexFragment& candidate) {
            return candidate.index_id == legal_index->index_id &&
                candidate.source_sst_id == file.file_number &&
                candidate.usable &&
                candidate.catalog_generation <=
                    context->index_catalog_snapshot->catalog_generation &&
                (file.statistics.row_count == 0 ||
                 candidate.source_row_count ==
                     file.statistics.row_count);
          });
      if (fragment == context->index_catalog_snapshot->fragments.end()) {
        add_uncovered(file.statistics.row_count == 0
                          ? snapshot.ValueOrDie().aggregate.row_count
                          : file.statistics.row_count);
      }
    }
    const auto count_uncovered_events = [&](const std::vector<TemporalEvent>& events) {
      for (const TemporalEvent& event : events) {
        if (event.logical_key().entity_type() == predicate.column.entity_type &&
            event.logical_key().kind() == LogicalKeyKind::kProperty &&
            event.logical_key().column_id() == predicate.column.column_id &&
            event.schema_epoch() == predicate.column.schema_epoch) {
          add_uncovered(1);
        }
      }
    };
    count_uncovered_events(context->committed_events);
    count_uncovered_events(context->session_overlay_events);
    for (const std::shared_ptr<const TemporalMemTable>& memtable :
         context->memtable_event_sources) {
      if (!memtable) continue;
      const bool covered = std::any_of(
          context->delta_index_sources.begin(),
          context->delta_index_sources.end(),
          [&](const TcypherDeltaIndexSource& source) {
            return source.index_id == legal_index->index_id &&
                source.pinned_memtable == memtable;
          });
      if (!covered) add_uncovered(memtable->event_count());
    }
    predicate_estimates.push_back(PredicateEstimate{
        predicate_index, legal_index->index_id, *candidate_rows,
        snapshot.ValueOrDie().aggregate.row_count, uncovered_base_rows});
  }
  if (predicate_estimates.empty()) return;
  std::stable_sort(
      predicate_estimates.begin(), predicate_estimates.end(),
      [](const PredicateEstimate& left, const PredicateEstimate& right) {
        const unsigned __int128 left_ratio =
            static_cast<unsigned __int128>(left.candidate_rows) *
            right.base_rows;
        const unsigned __int128 right_ratio =
            static_cast<unsigned __int128>(right.candidate_rows) *
            left.base_rows;
        if (left_ratio != right_ratio) return left_ratio < right_ratio;
        return left.predicate_index < right.predicate_index;
      });
  const PredicateEstimate& best = predicate_estimates.front();
  ScanCostEstimate estimate;
  estimate.base_rows = best.base_rows;
  estimate.index_candidate_rows = best.candidate_rows;
  estimate.uncovered_base_rows = best.uncovered_base_rows;
  estimate.residual_predicate_count = static_cast<uint32_t>(
      std::min<size_t>(predicate_estimates.size() - 1,
                       std::numeric_limits<uint32_t>::max()));
  if (predicate_estimates.size() >= 2 &&
      predicate_estimates[0].uncovered_base_rows == 0 &&
      predicate_estimates[1].uncovered_base_rows == 0) {
    estimate.intersection_available = true;
    estimate.left_index_candidate_rows = predicate_estimates[0].candidate_rows;
    estimate.right_index_candidate_rows = predicate_estimates[1].candidate_rows;
    if (estimate.left_index_candidate_rows == 0 ||
        estimate.right_index_candidate_rows == 0) {
      estimate.intersection_candidate_rows = 0;
    } else {
      const unsigned __int128 product =
          static_cast<unsigned __int128>(
              estimate.left_index_candidate_rows) *
          estimate.right_index_candidate_rows;
      const uint64_t denominator = std::max<uint64_t>(1, estimate.base_rows);
      const unsigned __int128 ceiling =
          (product + denominator - 1) / denominator;
      estimate.intersection_candidate_rows = static_cast<uint64_t>(
          std::min<unsigned __int128>(
              ceiling,
              std::min(estimate.left_index_candidate_rows,
                       estimate.right_index_candidate_rows)));
    }
  }
  // StatsFragment distinct values are value-cardinality statistics, not the
  // number of versions per logical property key. Until version-chain
  // cardinality is published explicitly, retain the cost model's conservative
  // single-version default rather than inventing temporal churn.
  estimate.validation_versions_per_candidate = 1;
  const RuntimeFeedbackKey key{
      RuntimeFeedbackPlanShape(plan), RuntimeFeedbackSchemaEpochs(plan),
      context->index_catalog_snapshot->catalog_generation,
      context->statistics_snapshot->statistics_snapshot_id(),
      ClassifySelectivity(estimate.index_candidate_rows, estimate.base_rows)};
  if (!context->options.execution_stats) {
    context->options.execution_stats =
        std::make_shared<TcypherExecutionStats>();
  }
  const std::optional<RuntimeFeedbackAggregate> feedback =
      context->runtime_feedback ? context->runtime_feedback->Lookup(key)
                                : std::nullopt;
  TcypherExecutionStats& stats = *context->options.execution_stats;
  stats.runtime_feedback_key = key;
  stats.runtime_feedback_base_rows = estimate.base_rows;
  stats.runtime_feedback_candidate_rows = estimate.index_candidate_rows;
  stats.runtime_feedback_observations =
      feedback.has_value() ? feedback->observations : 0;
  const auto select_access_path = [&](const ScanCostEstimate& candidate) {
    const AccessPathDecision decision = ChooseAccessPathDecision(
        candidate, OptimizerBudget{4, false});
    stats.has_selected_access_path = true;
    stats.selected_access_path = decision.source;
    stats.selected_access_path_score = decision.score;
    stats.selected_access_path_cost = decision.cost;
    stats.selected_access_path_rationale = decision.rationale;
    context->root_access_path = decision.source;
    context->root_access_predicate_indices.clear();
    if (decision.source == CandidateSource::kIntersection) {
      context->root_access_predicate_indices.push_back(
          predicate_estimates[0].predicate_index);
      context->root_access_predicate_indices.push_back(
          predicate_estimates[1].predicate_index);
    } else if (decision.source == CandidateSource::kIndex ||
               decision.source == CandidateSource::kHybrid) {
      context->root_access_predicate_indices.push_back(
          predicate_estimates[0].predicate_index);
    }
  };
  select_access_path(estimate);
  if (!context->runtime_feedback) return;
  const ScanCostEstimate corrected =
      context->runtime_feedback->ApplyToEstimate(key, estimate);
  if (!corrected.feedback_applied) return;
  stats.runtime_feedback_candidate_rows = corrected.index_candidate_rows;
  stats.runtime_feedback_applied = true;
  select_access_path(corrected);
  stats.runtime_feedback_source = stats.selected_access_path;
}

}  // namespace

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteTcypher(
    const std::string& query, TcypherExecutionContext context) {
  const Status cancelled = CheckQueryCancelled(context);
  if (!cancelled.ok()) return cancelled;
  if (context.options.batch_capacity == 0 ||
      context.options.batch_capacity > kTcypherStandardBatchCapacity ||
      !context.schema_snapshot) {
    return Status::InvalidArgument("T-Cypher executor", "invalid query snapshot or batch capacity");
  }
  const auto parsed = ParseTcypher(query);
  if (!parsed.ok()) return parsed.status();
  const TcypherStatementKind parsed_kind = parsed.ValueOrDie().kind;
  if (parsed_kind == TcypherStatementKind::kBeginSnapshot ||
      parsed_kind == TcypherStatementKind::kBeginStrict ||
      parsed_kind == TcypherStatementKind::kCommit ||
      parsed_kind == TcypherStatementKind::kRollback) {
    if (context.session == nullptr) {
      return Status::NotSupported("T-Cypher executor", "transaction control requires a session");
    }
    Status status = Status::OK();
    if (parsed_kind == TcypherStatementKind::kBeginSnapshot) {
      status = context.session->Begin(TcypherSessionMode::kSnapshot);
    } else if (parsed_kind == TcypherStatementKind::kBeginStrict) {
      status = context.session->Begin(TcypherSessionMode::kStrict);
    } else if (parsed_kind == TcypherStatementKind::kCommit) {
      uint64_t commit_seq = 0;
      status = context.session->Commit(&commit_seq);
    } else {
      status = context.session->Rollback();
    }
    if (!status.ok()) return status;
    std::unique_ptr<QueryResultStream> result =
        std::make_unique<InMemoryResultStream>(std::vector<ResultBatch>{}, Status::OK());
    return result;
  }
  context.pinned_visible_seq_ceiling = context.visible_seq_ceiling;
  const Status overlay_status = ApplySessionOverlay(&context);
  if (!overlay_status.ok()) return overlay_status;
  const uint64_t session_snapshot = context.visible_seq_ceiling;
  const auto bound = BindTcypher(parsed.ValueOrDie(),
      TcypherBindingContext{context.commit_timeline, session_snapshot,
                            context.options.statement_start_valid_time,
                            context.options.timestamp_parameters,
                            context.schema_snapshot});
  if (!bound.ok()) return bound.status();
  if (context.session != nullptr && context.session->active() &&
      !context.session->pending_events().empty()) {
    const auto contains_system_time = [](const auto& scopes) {
      return std::any_of(
          scopes.begin(), scopes.end(), [](const BoundTemporalScope& scope) {
            return scope.axis == TemporalAxis::kSystemTime;
          });
    };
    const bool has_system_time_scope =
        contains_system_time(bound.ValueOrDie().temporal_scopes) ||
        contains_system_time(bound.ValueOrDie().primary_match_scopes) ||
        std::any_of(
            bound.ValueOrDie().additional_match_scopes.begin(),
            bound.ValueOrDie().additional_match_scopes.end(),
            [&](const std::vector<BoundTemporalScope>& scopes) {
              return contains_system_time(scopes);
            });
    const bool projects_system_time = std::any_of(
        bound.ValueOrDie().syntax.returns.begin(), bound.ValueOrDie().syntax.returns.end(),
        [](const ReturnExpression& expression) {
          return expression.kind == ReturnExpressionKind::kSystemTime;
        });
    if (has_system_time_scope || projects_system_time) {
      return Status::NotSupported("T-Cypher executor",
                                  "SYSTEM_TIME is unavailable while a session has staged writes");
    }
  }
  if (bound.ValueOrDie().syntax.kind == TcypherStatementKind::kCreate) {
    return ExecuteCreate(bound.ValueOrDie(), context);
  }
  if (bound.ValueOrDie().syntax.kind == TcypherStatementKind::kSet ||
      bound.ValueOrDie().syntax.kind == TcypherStatementKind::kDelete) {
    return ExecuteExactMutation(bound.ValueOrDie(), context);
  }
  if (session_snapshot == 0 && context.session_overlay_events.empty()) {
    return Status::InvalidArgument("T-Cypher executor", "query snapshot is empty");
  }
  const auto plan = LowerTcypher(bound.ValueOrDie());
  if (!plan.ok()) return plan.status();
  std::shared_ptr<const PhysicalPlan> physical_plan;
  std::optional<BoundTcypherStatement> physical_root_bound;
  std::shared_ptr<const PhysicalHashJoinPlan> physical_hash_join_plan;
  std::optional<BoundTcypherStatement> physical_hash_join_bound;
  std::shared_ptr<const PhysicalMultiHashJoinPlan> physical_multi_join_plan;
  std::optional<BoundTcypherStatement> physical_multi_join_bound;
  if (CanPlanPhysicalRootPoint(bound.ValueOrDie())) {
    auto built = PlanPhysicalRootPoint(bound.ValueOrDie(), plan.ValueOrDie());
    if (!built.ok()) return built.status();
    physical_plan = std::move(built).ConsumeValueOrDie();
    physical_root_bound = bound.ValueOrDie();
    if (context.options.execution_stats) {
      ++context.options.execution_stats->physical_plan_builds;
      context.options.execution_stats->last_physical_plan_id = physical_plan->plan_id();
    }
  }
  if (CanPlanPhysicalHashJoin(bound.ValueOrDie())) {
    auto built = PlanPhysicalHashJoin(
        bound.ValueOrDie(), plan.ValueOrDie(), nullptr,
        BuildPhysicalHashJoinPlanningStats(bound.ValueOrDie(), context));
    if (!built.ok()) return built.status();
    physical_hash_join_plan = std::move(built).ConsumeValueOrDie();
    physical_hash_join_bound = bound.ValueOrDie();
    if (context.options.execution_stats) {
      ++context.options.execution_stats->physical_plan_builds;
      context.options.execution_stats->last_physical_plan_id =
          physical_hash_join_plan->plan_id;
    }
  }
  if (CanPlanPhysicalMultiHashJoin(bound.ValueOrDie())) {
    const PhysicalJoinPlanningEstimates estimates =
        BuildPhysicalJoinPlanningEstimates(bound.ValueOrDie(), context);
    const uint64_t pinned_snapshot =
        context.pinned_visible_seq_ceiling.value_or(session_snapshot);
    auto built = PlanPhysicalMultiHashJoin(
        bound.ValueOrDie(), plan.ValueOrDie(), nullptr, estimates.by_binding,
        estimates.statistics_snapshot_id, pinned_snapshot);
    if (!built.ok()) return built.status();
    physical_multi_join_plan = std::move(built).ConsumeValueOrDie();
    physical_multi_join_bound = bound.ValueOrDie();
    if (context.options.execution_stats) {
      ++context.options.execution_stats->physical_plan_builds;
      ++context.options.execution_stats->physical_multi_join_builds;
      context.options.execution_stats->last_physical_plan_id =
          physical_multi_join_plan->plan_id;
    }
  }
  if (!physical_hash_join_plan && !physical_multi_join_plan &&
      !bound.ValueOrDie().syntax.additional_matches.empty() &&
      (bound.ValueOrDie().syntax.distinct ||
       bound.ValueOrDie().syntax.order_by.has_value() ||
       std::any_of(bound.ValueOrDie().syntax.returns.begin(),
                   bound.ValueOrDie().syntax.returns.end(),
                   [](const ReturnExpression& expression) {
                     return expression.kind == ReturnExpressionKind::kCount ||
                         expression.kind == ReturnExpressionKind::kSum ||
                         expression.kind == ReturnExpressionKind::kAvg ||
                         expression.kind == ReturnExpressionKind::kMin ||
                         expression.kind == ReturnExpressionKind::kMax ||
                         expression.kind == ReturnExpressionKind::kCollect;
                   }))) {
    TcypherStatement input_syntax =
        AggregateInputStatement(bound.ValueOrDie().syntax);
    input_syntax.distinct = false;
    input_syntax.order_by.reset();
    auto input_bound = BindTcypher(
        input_syntax,
        TcypherBindingContext{
            context.commit_timeline, session_snapshot,
            context.options.statement_start_valid_time,
            context.options.timestamp_parameters, context.schema_snapshot});
    if (!input_bound.ok()) return input_bound.status();
    const auto input_logical = LowerTcypher(input_bound.ValueOrDie());
    if (!input_logical.ok()) return input_logical.status();
    if (CanPlanPhysicalHashJoin(input_bound.ValueOrDie())) {
      auto built = PlanPhysicalHashJoin(
          input_bound.ValueOrDie(), input_logical.ValueOrDie(),
          &bound.ValueOrDie().syntax,
          BuildPhysicalHashJoinPlanningStats(input_bound.ValueOrDie(), context));
      if (!built.ok()) return built.status();
      physical_hash_join_plan = std::move(built).ConsumeValueOrDie();
      physical_hash_join_bound = std::move(input_bound).ConsumeValueOrDie();
      if (context.options.execution_stats) {
        ++context.options.execution_stats->physical_plan_builds;
        context.options.execution_stats->last_physical_plan_id =
            physical_hash_join_plan->plan_id;
      }
    } else if (CanPlanPhysicalMultiHashJoin(input_bound.ValueOrDie())) {
      const PhysicalJoinPlanningEstimates estimates =
          BuildPhysicalJoinPlanningEstimates(input_bound.ValueOrDie(), context);
      const uint64_t pinned_snapshot =
          context.pinned_visible_seq_ceiling.value_or(session_snapshot);
      auto built = PlanPhysicalMultiHashJoin(
          input_bound.ValueOrDie(), input_logical.ValueOrDie(),
          &bound.ValueOrDie().syntax, estimates.by_binding,
          estimates.statistics_snapshot_id, pinned_snapshot);
      if (!built.ok()) return built.status();
      physical_multi_join_plan = std::move(built).ConsumeValueOrDie();
      physical_multi_join_bound = std::move(input_bound).ConsumeValueOrDie();
      if (context.options.execution_stats) {
        ++context.options.execution_stats->physical_plan_builds;
        ++context.options.execution_stats->physical_multi_join_builds;
        context.options.execution_stats->last_physical_plan_id =
            physical_multi_join_plan->plan_id;
      }
    }
  }
  if (!physical_plan && !physical_hash_join_plan &&
      !physical_multi_join_plan &&
      bound.ValueOrDie().syntax.additional_matches.empty() &&
      (bound.ValueOrDie().syntax.distinct ||
       bound.ValueOrDie().syntax.order_by.has_value() ||
       std::any_of(bound.ValueOrDie().syntax.returns.begin(),
                   bound.ValueOrDie().syntax.returns.end(),
                   [](const ReturnExpression& expression) {
                     return expression.kind == ReturnExpressionKind::kCount ||
                         expression.kind == ReturnExpressionKind::kSum ||
                         expression.kind == ReturnExpressionKind::kAvg ||
                         expression.kind == ReturnExpressionKind::kMin ||
                         expression.kind == ReturnExpressionKind::kMax ||
                         expression.kind == ReturnExpressionKind::kCollect;
                   }))) {
    TcypherStatement input_syntax =
        AggregateInputStatement(bound.ValueOrDie().syntax);
    input_syntax.distinct = false;
    input_syntax.order_by.reset();
    auto input_bound = BindTcypher(
        input_syntax,
        TcypherBindingContext{
            context.commit_timeline, session_snapshot,
            context.options.statement_start_valid_time,
            context.options.timestamp_parameters, context.schema_snapshot});
    if (!input_bound.ok()) return input_bound.status();
    const auto input_logical = LowerTcypher(input_bound.ValueOrDie());
    if (!input_logical.ok()) return input_logical.status();
    if (CanPlanPhysicalRootPoint(input_bound.ValueOrDie())) {
      auto built = PlanPhysicalRootPoint(
          input_bound.ValueOrDie(), input_logical.ValueOrDie(),
          &bound.ValueOrDie().syntax);
      if (!built.ok()) return built.status();
      physical_plan = std::move(built).ConsumeValueOrDie();
      physical_root_bound = std::move(input_bound).ConsumeValueOrDie();
      if (context.options.execution_stats) {
        ++context.options.execution_stats->physical_plan_builds;
        context.options.execution_stats->last_physical_plan_id =
            physical_plan->plan_id();
      }
    }
  }
  const BoundTcypherStatement& root_runtime_bound =
      physical_root_bound.has_value() ? *physical_root_bound
                                      : bound.ValueOrDie();
  const bool physical_change_runtime_candidate = physical_plan != nullptr &&
      (physical_plan->temporal_mode() == PhysicalTemporalMode::kValidTimeChanges ||
       physical_plan->temporal_mode() == PhysicalTemporalMode::kSystemTimeChanges);
  const bool physical_range_runtime_candidate = physical_plan != nullptr &&
      physical_plan->temporal_mode() == PhysicalTemporalMode::kValidTimeRange;
  const bool physical_expand_runtime_candidate = physical_plan != nullptr &&
      physical_plan->expand().has_value() &&
      (root_runtime_bound.fixed_expand_point_candidate ||
       root_runtime_bound.fixed_expand_range_candidate);
  const bool physical_runtime_candidate = physical_plan != nullptr &&
      (root_runtime_bound.root_point_candidate || physical_change_runtime_candidate ||
       physical_range_runtime_candidate || physical_expand_runtime_candidate);
  const TcypherStatement& statement = bound.ValueOrDie().syntax;
  const bool strict_session = context.session != nullptr && context.session->active() &&
      context.session->mode() == TcypherSessionMode::kStrict;
  if (strict_session) {
    const bool state_point_only = std::all_of(statement.temporal_scopes.begin(),
        statement.temporal_scopes.end(), [](const TemporalScope& scope) {
          return scope.mode == TemporalScopeMode::kStateAsOf;
        });
    if (!state_point_only || statement.relationships.size() != 0 ||
        statement.where.has_value() || !statement.and_predicates.empty()) {
      return Status::NotSupported("T-Cypher executor",
                                  "strict sessions reject predicate, range, and graph scans");
    }
    const auto entity_id = ResolveExactEntityId(statement.match, context.options);
    if (!entity_id.ok()) return entity_id.status();
  }
  if (physical_plan != nullptr) {
    PrepareRootRuntimeFeedback(*physical_plan, &context);
  }
  if (bound.ValueOrDie().syntax.explain) {
    if (physical_multi_join_plan) {
      if (!bound.ValueOrDie().syntax.explain_analyze) {
        return ExplainPhysicalMultiHashJoinTcypherPlan(
            *physical_multi_join_plan, session_snapshot);
      }
      std::shared_ptr<TcypherExecutionStats> stats =
          context.options.execution_stats;
      if (!stats) {
        stats = std::make_shared<TcypherExecutionStats>();
        context.options.execution_stats = stats;
        ++stats->physical_plan_builds;
        ++stats->physical_multi_join_builds;
        stats->last_physical_plan_id = physical_multi_join_plan->plan_id;
      }
      ExplainAnalyzeRuntimeProfile explain_profile =
          CaptureExplainProfile(context, session_snapshot);
      const std::shared_ptr<QueryMemoryAccount> explain_memory =
          context.options.memory_account;
      auto snapshot = BuildPhysicalQuerySnapshot(
          physical_multi_join_bound.value(), context);
      if (!snapshot.ok()) return snapshot.status();
      const TcypherQueryOptions options = context.options;
      auto executed = OpenPhysicalMultiHashJoinRuntime(
          physical_multi_join_plan,
          std::move(snapshot).ConsumeValueOrDie(), std::move(context));
      if (!executed.ok()) return executed.status();
      auto limited = ApplyResultLimit(
          bound.ValueOrDie().syntax,
          std::move(executed).ConsumeValueOrDie(), options.batch_capacity,
          options.cancellation, options.memory_account,
          physical_multi_join_plan->aggregate_sink.has_value(),
          PhysicalMultiHashJoinOwnsOperator(
              *physical_multi_join_plan, PhysicalOperatorKind::kDistinct),
          physical_multi_join_plan->sort_sink.has_value(),
          options.spill_directory, options.spill_resource_extensions);
      if (!limited.ok()) return limited.status();
      return ExplainAnalyzePhysicalMultiHashJoinTcypherPlan(
          *physical_multi_join_plan, session_snapshot,
          std::move(limited).ConsumeValueOrDie(), stats, explain_memory,
          std::move(explain_profile));
    }
    if (physical_hash_join_plan) {
      if (!bound.ValueOrDie().syntax.explain_analyze) {
        return ExplainPhysicalHashJoinTcypherPlan(
            *physical_hash_join_plan, session_snapshot);
      }
      std::shared_ptr<TcypherExecutionStats> stats =
          context.options.execution_stats;
      if (!stats) {
        stats = std::make_shared<TcypherExecutionStats>();
        context.options.execution_stats = stats;
        ++stats->physical_plan_builds;
        stats->last_physical_plan_id = physical_hash_join_plan->plan_id;
      }
      ExplainAnalyzeRuntimeProfile explain_profile =
          CaptureExplainProfile(context, session_snapshot);
      const std::shared_ptr<QueryMemoryAccount> explain_memory =
          context.options.memory_account;
      auto snapshot = BuildPhysicalQuerySnapshot(
          physical_hash_join_bound.value(), context);
      if (!snapshot.ok()) return snapshot.status();
      const TcypherQueryOptions options = context.options;
      auto executed = OpenPhysicalHashJoinRuntime(
          physical_hash_join_plan,
          std::move(snapshot).ConsumeValueOrDie(), std::move(context));
      if (!executed.ok()) return executed.status();
      auto limited = ApplyResultLimit(
          bound.ValueOrDie().syntax,
          std::move(executed).ConsumeValueOrDie(), options.batch_capacity,
          options.cancellation, options.memory_account,
          physical_hash_join_plan->aggregate_sink.has_value(),
          PhysicalHashJoinOwnsOperator(
              *physical_hash_join_plan, PhysicalOperatorKind::kDistinct),
          physical_hash_join_plan->sort_sink.has_value(),
          options.spill_directory, options.spill_resource_extensions);
      if (!limited.ok()) return limited.status();
      return ExplainAnalyzePhysicalHashJoinTcypherPlan(
          *physical_hash_join_plan, session_snapshot,
          std::move(limited).ConsumeValueOrDie(), stats, explain_memory,
          std::move(explain_profile));
    }
    if (physical_plan) {
      auto snapshot = BuildPhysicalQuerySnapshot(root_runtime_bound, context);
      if (!snapshot.ok()) return snapshot.status();
      if (!bound.ValueOrDie().syntax.explain_analyze) {
        return ExplainPhysicalTcypherPlan(*physical_plan, session_snapshot);
      }
      if (!physical_runtime_candidate) {
        return Status::Corruption(
            "T-Cypher executor",
            "physical plan has no matching runtime dispatcher");
      }
      std::shared_ptr<TcypherExecutionStats> stats = context.options.execution_stats;
      if (!stats) {
        stats = std::make_shared<TcypherExecutionStats>();
        context.options.execution_stats = stats;
      }
      ExplainAnalyzeRuntimeProfile explain_profile =
          CaptureExplainProfile(context, session_snapshot);
      const std::shared_ptr<QueryMemoryAccount> explain_memory =
          context.options.memory_account;
      const TcypherQueryOptions options = context.options;
      auto executed = OpenPhysicalRootPointRuntime(
          physical_plan, std::move(snapshot).ConsumeValueOrDie(), std::move(context));
      if (!executed.ok()) return executed.status();
      auto limited = ApplyResultLimit(
          bound.ValueOrDie().syntax, std::move(executed).ConsumeValueOrDie(),
          options.batch_capacity, options.cancellation, options.memory_account,
          physical_plan->aggregate_sink().has_value(),
          std::any_of(physical_plan->post_result_operators().begin(),
                      physical_plan->post_result_operators().end(),
                      [](const PhysicalOperatorSpec& op) {
                        return op.kind == PhysicalOperatorKind::kDistinct;
                      }),
          physical_plan->sort_sink().has_value(), options.spill_directory,
          options.spill_resource_extensions);
      if (!limited.ok()) return limited.status();
      return ExplainAnalyzePhysicalTcypherPlan(
          *physical_plan, session_snapshot, std::move(limited).ConsumeValueOrDie(), stats,
          explain_memory, std::move(explain_profile));
    }
    if (bound.ValueOrDie().syntax.explain_analyze) {
      return Status::NotSupported(
          "T-Cypher executor",
          "EXPLAIN ANALYZE is available for the physical root point family");
    }
    return ExplainTcypherPlan(plan.ValueOrDie(), session_snapshot);
  }
  if (physical_runtime_candidate) {
    auto snapshot = BuildPhysicalQuerySnapshot(root_runtime_bound, context);
    if (!snapshot.ok()) return snapshot.status();
    const TcypherQueryOptions options = context.options;
    auto stream = OpenPhysicalRootPointRuntime(
        physical_plan, std::move(snapshot).ConsumeValueOrDie(), std::move(context));
    if (!stream.ok()) return stream.status();
    return ApplyResultLimit(
        statement, std::move(stream).ConsumeValueOrDie(), options.batch_capacity,
        options.cancellation, options.memory_account,
        physical_plan->aggregate_sink().has_value(),
        std::any_of(physical_plan->post_result_operators().begin(),
                    physical_plan->post_result_operators().end(),
                    [](const PhysicalOperatorSpec& op) {
                      return op.kind == PhysicalOperatorKind::kDistinct;
                    }),
        physical_plan->sort_sink().has_value(), options.spill_directory,
        options.spill_resource_extensions);
  }
  if (!statement.match.label.empty()) {
    const auto existence = context.schema_snapshot->Lookup(EntityType::Vertex, 0);
    if (!existence.has_value() || existence->logical_type != statement.match.label) {
      return Status::BindError("T-Cypher executor", "vertex label is not registered");
    }
  }
  uint64_t valid_time = context.options.statement_start_valid_time;
  uint64_t snapshot_seq = session_snapshot;
  bool valid_time_changes = false;
  bool system_time_changes = false;
  bool valid_time_state_range = false;
  uint64_t valid_time_change_start = 0;
  uint64_t valid_time_change_end = 0;
  uint64_t valid_time_state_range_start = 0;
  uint64_t valid_time_state_range_end = 0;
  uint64_t system_time_change_start = 0;
  uint64_t system_time_change_end = 0;
  std::optional<std::pair<uint64_t, uint64_t>> system_change_valid_time_range;
  std::optional<uint64_t> system_change_valid_time_as_of;
  const bool has_system_time_change = std::any_of(
      bound.ValueOrDie().temporal_scopes.begin(), bound.ValueOrDie().temporal_scopes.end(),
      [](const BoundTemporalScope& scope) {
        return scope.axis == TemporalAxis::kSystemTime &&
               scope.mode == TemporalScopeMode::kChangesBetween;
      });
  for (const BoundTemporalScope& scope : bound.ValueOrDie().temporal_scopes) {
    if (scope.mode == TemporalScopeMode::kChangesBetween) {
      if (scope.axis == TemporalAxis::kValidTime) {
        valid_time_changes = true;
        valid_time_change_start = scope.valid_time_start;
        valid_time_change_end = scope.valid_time_end;
      } else {
        system_time_changes = true;
        system_time_change_start = scope.system_time_start;
        system_time_change_end = scope.system_time_end;
      }
      continue;
    }
    if (scope.mode == TemporalScopeMode::kStateBetween &&
        scope.axis == TemporalAxis::kValidTime) {
      if (has_system_time_change) {
        system_change_valid_time_range =
            std::make_pair(scope.valid_time_start, scope.valid_time_end);
      } else {
        valid_time_state_range = true;
        valid_time_state_range_start = scope.valid_time_start;
        valid_time_state_range_end = scope.valid_time_end;
      }
      continue;
    }
    if (scope.mode != TemporalScopeMode::kStateAsOf) {
      return Status::NotSupported("T-Cypher executor", "state-range execution is not available yet");
    }
    if (scope.axis == TemporalAxis::kValidTime) {
      if (has_system_time_change) {
        system_change_valid_time_as_of = scope.valid_time_start;
      } else {
        valid_time = scope.valid_time_start;
      }
    } else {
      snapshot_seq = scope.snapshot_seq;
    }
  }
  for (const BoundTemporalScope& scope : bound.ValueOrDie().primary_match_scopes) {
    if (scope.axis == TemporalAxis::kSystemTime) {
      if (scope.mode == TemporalScopeMode::kStateAsOf) {
        snapshot_seq = scope.snapshot_seq;
      } else if (scope.mode == TemporalScopeMode::kChangesBetween) {
        system_time_changes = true;
        system_time_change_start = scope.system_time_start;
        system_time_change_end = scope.system_time_end;
      } else {
        return Status::NotSupported("T-Cypher executor", "MATCH SYSTEM_TIME requires AS OF or CHANGES BETWEEN");
      }
      continue;
    }
    if (scope.mode == TemporalScopeMode::kStateAsOf) {
      valid_time = scope.valid_time_start;
    } else if (scope.mode == TemporalScopeMode::kStateBetween) {
      valid_time_state_range = true;
      valid_time_state_range_start = scope.valid_time_start;
      valid_time_state_range_end = scope.valid_time_end;
    } else if (scope.mode == TemporalScopeMode::kChangesBetween) {
      if (scope.axis == TemporalAxis::kValidTime) {
        valid_time_changes = true;
        valid_time_change_start = scope.valid_time_start;
        valid_time_change_end = scope.valid_time_end;
      } else {
        system_time_changes = true;
        system_time_change_start = scope.system_time_start;
        system_time_change_end = scope.system_time_end;
      }
    } else {
      return Status::Corruption("T-Cypher executor", "unknown MATCH temporal override");
    }
  }
  if (system_time_changes) {
    const auto apply_valid_time_restriction =
        [&](const std::vector<BoundTemporalScope>& scopes) {
          for (const BoundTemporalScope& scope : scopes) {
            if (scope.axis != TemporalAxis::kValidTime) continue;
            if (scope.mode == TemporalScopeMode::kStateAsOf) {
              system_change_valid_time_as_of = scope.valid_time_start;
              system_change_valid_time_range.reset();
            } else if (scope.mode == TemporalScopeMode::kStateBetween) {
              system_change_valid_time_range =
                  std::make_pair(scope.valid_time_start, scope.valid_time_end);
              system_change_valid_time_as_of.reset();
            }
          }
        };
    apply_valid_time_restriction(bound.ValueOrDie().temporal_scopes);
    apply_valid_time_restriction(bound.ValueOrDie().primary_match_scopes);
  }

  const bool non_equality_predicate =
      (statement.where.has_value() && statement.where->kind != StringPredicateKind::kEquality) ||
      std::any_of(statement.and_predicates.begin(), statement.and_predicates.end(),
                  [](const StringEqualityPredicate& predicate) {
                    return predicate.kind != StringPredicateKind::kEquality;
                  });
  if (non_equality_predicate &&
      (valid_time_changes || system_time_changes || valid_time_state_range ||
       !statement.relationships.empty() ||
       std::any_of(statement.returns.begin(), statement.returns.end(),
                   [](const ReturnExpression& expression) {
                     return expression.kind == ReturnExpressionKind::kValidTo ||
                            expression.kind == ReturnExpressionKind::kSystemTime;
                   }))) {
    return Status::NotSupported("T-Cypher executor",
                                "non-equality WHERE is currently available for root point queries");
  }
  const bool integer_predicate =
      (statement.where.has_value() && UsesIntegerLiteral(*statement.where)) ||
      std::any_of(statement.and_predicates.begin(), statement.and_predicates.end(),
                  [](const StringEqualityPredicate& predicate) {
                    return UsesIntegerLiteral(predicate);
                  });
  if (integer_predicate &&
      (valid_time_changes || system_time_changes || valid_time_state_range ||
       !statement.relationships.empty() ||
       std::any_of(statement.returns.begin(), statement.returns.end(),
                   [](const ReturnExpression& expression) {
                     return expression.kind == ReturnExpressionKind::kValidTo ||
                            expression.kind == ReturnExpressionKind::kSystemTime;
                   }))) {
    return Status::NotSupported("T-Cypher executor",
                                "integer WHERE is currently available for root point queries");
  }
  if (!statement.and_predicates.empty() &&
      (valid_time_changes || system_time_changes || valid_time_state_range ||
       !statement.relationships.empty())) {
    return Status::NotSupported("T-Cypher executor",
                                "multiple WHERE predicates are currently available for root point queries");
  }

  if (valid_time_changes) {
    const TcypherStatement input = AggregateInputStatement(statement);
    auto stream = ExecuteValidTimeChanges(input, context, valid_time_change_start,
                                          valid_time_change_end, snapshot_seq);
    if (!stream.ok()) return stream.status();
    return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                            context.options.cancellation, context.options.memory_account,
                            false, false, false, context.options.spill_directory,
                            context.options.spill_resource_extensions);
  }
  if (system_time_changes) {
    const TcypherStatement input = AggregateInputStatement(statement);
    auto stream = ExecuteSystemTimeChanges(input, context, system_time_change_start,
                                           system_time_change_end, snapshot_seq,
                                           system_change_valid_time_range,
                                           system_change_valid_time_as_of);
    if (!stream.ok()) return stream.status();
    return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                            context.options.cancellation, context.options.memory_account,
                            false, false, false, context.options.spill_directory,
                            context.options.spill_resource_extensions);
  }
  if (valid_time_state_range) {
    TcypherStatement range_statement = statement;
    for (ReturnExpression& expression : range_statement.returns) {
      if (expression.kind == ReturnExpressionKind::kCount) {
        expression.kind = ReturnExpressionKind::kBinding;
        expression.property_name.clear();
      } else if (expression.kind == ReturnExpressionKind::kSum ||
                 expression.kind == ReturnExpressionKind::kAvg ||
                 expression.kind == ReturnExpressionKind::kMin ||
                 expression.kind == ReturnExpressionKind::kMax) {
        expression.kind = ReturnExpressionKind::kProperty;
      }
    }
    if (!statement.relationships.empty()) {
      StatusOr<std::unique_ptr<QueryResultStream>> stream =
          statement.relationships.size() > 1
              ? ExecuteFixedExpandChainRange(range_statement, context,
                                             valid_time_state_range_start,
                                             valid_time_state_range_end, snapshot_seq)
              : (statement.relationships.front().variable_length
                     ? ExecuteVariableExpandRange(range_statement, context,
                                                  valid_time_state_range_start,
                                                  valid_time_state_range_end, snapshot_seq)
                     : ExecuteFixedExpandRange(range_statement, context,
                                               valid_time_state_range_start,
                                               valid_time_state_range_end, snapshot_seq));
      if (!stream.ok()) return stream.status();
      return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                              context.options.cancellation, context.options.memory_account,
                              false, false, false, context.options.spill_directory,
                              context.options.spill_resource_extensions);
    }
    auto stream = ExecuteValidTimeStateRange(range_statement, context, valid_time_state_range_start,
                                             valid_time_state_range_end, snapshot_seq);
    if (!stream.ok()) return stream.status();
    return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                            context.options.cancellation, context.options.memory_account,
                            false, false, false, context.options.spill_directory,
                            context.options.spill_resource_extensions);
  }

  if (!statement.additional_matches.empty()) {
    if (strict_session) {
      return Status::NotSupported("T-Cypher executor",
                                  "strict sessions reject multi-MATCH scans");
    }
    if (physical_multi_join_plan) {
      auto snapshot = BuildPhysicalQuerySnapshot(
          physical_multi_join_bound.value(), context);
      if (!snapshot.ok()) return snapshot.status();
      const TcypherQueryOptions options = context.options;
      auto stream = OpenPhysicalMultiHashJoinRuntime(
          physical_multi_join_plan,
          std::move(snapshot).ConsumeValueOrDie(), std::move(context));
      if (!stream.ok()) return stream.status();
      return ApplyResultLimit(
          statement, std::move(stream).ConsumeValueOrDie(),
          options.batch_capacity, options.cancellation, options.memory_account,
          physical_multi_join_plan->aggregate_sink.has_value(),
          PhysicalMultiHashJoinOwnsOperator(
              *physical_multi_join_plan, PhysicalOperatorKind::kDistinct),
          physical_multi_join_plan->sort_sink.has_value(),
          options.spill_directory, options.spill_resource_extensions);
    }
    if (physical_hash_join_plan) {
      auto snapshot = BuildPhysicalQuerySnapshot(
          physical_hash_join_bound.value(), context);
      if (!snapshot.ok()) return snapshot.status();
      const TcypherQueryOptions options = context.options;
      auto stream = OpenPhysicalHashJoinRuntime(
          physical_hash_join_plan,
          std::move(snapshot).ConsumeValueOrDie(), std::move(context));
      if (!stream.ok()) return stream.status();
      return ApplyResultLimit(
          statement, std::move(stream).ConsumeValueOrDie(),
          options.batch_capacity, options.cancellation, options.memory_account,
          physical_hash_join_plan->aggregate_sink.has_value(),
          PhysicalHashJoinOwnsOperator(
              *physical_hash_join_plan, PhysicalOperatorKind::kDistinct),
          physical_hash_join_plan->sort_sink.has_value(), options.spill_directory,
          options.spill_resource_extensions);
    }
    return Status::NotSupported("T-Cypher executor",
                                "multi-MATCH query has no physical execution plan");
  }

  if (!statement.relationships.empty()) {
    if (statement.relationships.size() > 1) {
      if (std::any_of(statement.relationships.begin(), statement.relationships.end(),
                      [](const MatchRelationshipPattern& relationship) {
                        return relationship.variable_length;
                      })) {
        return Status::NotSupported("T-Cypher executor",
                                    "multi-hop chains with variable relationships are not available yet");
      }
      auto stream = ExecuteFixedExpandChainAsOf(statement, context, valid_time, snapshot_seq);
      if (!stream.ok()) return stream.status();
      return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                              context.options.cancellation, context.options.memory_account,
                              false, false, false, context.options.spill_directory,
                              context.options.spill_resource_extensions);
    }
    if (statement.relationships.front().variable_length) {
      auto stream = ExecuteVariableExpandAsOf(statement, context, valid_time, snapshot_seq);
      if (!stream.ok()) return stream.status();
      return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                              context.options.cancellation, context.options.memory_account,
                              false, false, false, context.options.spill_directory,
                              context.options.spill_resource_extensions);
    }
    auto stream = ExecuteFixedExpandAsOf(statement, context, valid_time, snapshot_seq);
    if (!stream.ok()) return stream.status();
    return ApplyResultLimit(statement, stream.ConsumeValueOrDie(), context.options.batch_capacity,
                            context.options.cancellation, context.options.memory_account,
                            false, false, false, context.options.spill_directory,
                            context.options.spill_resource_extensions);
  }

  return Status::Corruption("T-Cypher executor",
                            "root point query bypassed physical planning");
}

}  // namespace cedar
