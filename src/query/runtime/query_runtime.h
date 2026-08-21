// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
#define CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_

#include <limits>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/schema.h"
#include "cedar/snapshot.h"
#include "query/logical/expression.h"
#include "query/logical/logical_plan.h"
#include "query/runtime/relational.h"
#include "query/planner/query_planner.h"
#include "query/projection/projection_format.h"
#include "query/projection/projection_store.h"
#include "query/resource/query_resource_pool.h"

namespace cedar::internal {

struct PreparedPropertyBinding {
  SlotId source;
  PropertyId property;
  RowColumn output;
  PropertyEntityKind entity_kind;
  std::optional<PropertyDefinition> definition;
};

// Private pull-runtime boundary between logical relational nodes and physical
// vector operators. It is intentionally unavailable from the public Query API.
struct RuntimeRelationalInput {
  BatchStream left;
  BatchStream right;
  size_t left_key = 0;
  size_t right_key = 0;
  size_t estimated_rows = 0;
  bool sorted_keys = false;
  bool temporal = false;
  std::vector<SortKey> sort_keys;
  size_t offset = 0;
  size_t count = 0;
  std::vector<size_t> group_by;
  std::vector<AggregateSpec> aggregates;
};

struct RuntimeRelationalResult {
  BatchStream stream;
  std::optional<JoinAlgorithm> join_algorithm;
};

struct PreparedQueryPlan {
  PreparedQueryPlan() = default;
  PreparedQueryPlan(const PreparedQueryPlan& other)
      : canonical_temporal(other.canonical_temporal),
        entity_family(other.entity_family),
        entity_slot(other.entity_slot),
        scope(other.scope),
        property_bindings(other.property_bindings),
        predicate(other.predicate),
        output_columns(other.output_columns),
        effective_output_slot(other.effective_output_slot),
        referenced_properties(other.referenced_properties),
        physical_plan(other.physical_plan),
        projection_generation(other.projection_generation),
        projection_reader(other.projection_reader),
        delta_reader(other.delta_reader),
        bound_delta_view(other.bound_delta_view),
        graph_expand(other.graph_expand),
        graph_source_slot(other.graph_source_slot),
        graph_edge_slot(other.graph_edge_slot),
        graph_destination_slot(other.graph_destination_slot),
        graph_k_hops(other.graph_k_hops) {}
  PreparedQueryPlan& operator=(const PreparedQueryPlan& other) {
    if (this == &other) return *this;
    canonical_temporal = other.canonical_temporal;
    entity_family = other.entity_family;
    entity_slot = other.entity_slot;
    scope = other.scope;
    property_bindings = other.property_bindings;
    predicate = other.predicate;
    output_columns = other.output_columns;
    effective_output_slot = other.effective_output_slot;
    referenced_properties = other.referenced_properties;
    relational_kind.reset();
    relational_input.reset();
    physical_plan = other.physical_plan;
    projection_generation = other.projection_generation;
    projection_reader = other.projection_reader;
    delta_reader = other.delta_reader;
    bound_delta_view = other.bound_delta_view;
    graph_expand = other.graph_expand;
    graph_source_slot = other.graph_source_slot;
    graph_edge_slot = other.graph_edge_slot;
    graph_destination_slot = other.graph_destination_slot;
    graph_k_hops = other.graph_k_hops;
    return *this;
  }
  PreparedQueryPlan(PreparedQueryPlan&&) noexcept = default;
  PreparedQueryPlan& operator=(PreparedQueryPlan&&) noexcept = default;

  bool canonical_temporal = false;
  FactFamily entity_family = FactFamily::kVertexState;
  SlotId entity_slot;
  TemporalScope scope = At{ValidTime{0}};
  std::vector<PreparedPropertyBinding> property_bindings;
  std::shared_ptr<const ExpressionNode> predicate;
  std::vector<RowColumn> output_columns;
  std::optional<SlotId> effective_output_slot;
  std::vector<PropertyId> referenced_properties;
  std::optional<LogicalOpKind> relational_kind;
  std::optional<RuntimeRelationalInput> relational_input;
  std::shared_ptr<const PhysicalPlan> physical_plan;
  std::optional<ProjectionGeneration> projection_generation;
  std::function<StatusOr<std::vector<ProjectionChain>>(const CoverageSlice&)>
      projection_reader;
  std::function<StatusOr<QueryDeltaView>()> delta_reader;
  std::shared_ptr<const QueryDeltaView> bound_delta_view;
  std::optional<ExpandSpec> graph_expand;
  std::optional<SlotId> graph_source_slot;
  std::optional<SlotId> graph_edge_slot;
  std::optional<SlotId> graph_destination_slot;
  uint32_t graph_k_hops = 1;
};

StatusOr<PreparedQueryPlan> AnalyzeQuery(const Query& query);

StatusOr<RuntimeRelationalResult> ExecuteRelationalPlanNode(
    LogicalOpKind kind, RuntimeRelationalInput input,
    QueryReservation* reservation,
    FragmentBudget* fragment_budget = nullptr,
    size_t max_output_rows = std::numeric_limits<size_t>::max(),
    QueryScratch* scratch = nullptr);

class QueryRuntime {
 public:
  static StatusOr<QueryCursor> Execute(const PreparedQueryPlan& plan,
                                       Snapshot snapshot,
                                       const Bindings& bindings,
                                       const QueryOptions& options,
                                       QueryResourcePool* resource_pool = nullptr);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
