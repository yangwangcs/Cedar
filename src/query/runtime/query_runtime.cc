// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/query_runtime.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "query/logical/logical_plan.h"
#include "query/runtime/canonical_source.h"

namespace cedar {

class QueryCursor::State {
 public:
  State(internal::PreparedQueryPlan plan, Snapshot snapshot,
        QueryOptions options)
      : plan(std::move(plan)), snapshot(std::move(snapshot)),
        options(std::move(options)) {}

  internal::PreparedQueryPlan plan;
  std::optional<Snapshot> snapshot;
  QueryOptions options;
  std::vector<QueryBatch> batches;
  size_t next_batch = 0;
  bool initialized = false;
  bool clean_terminal = false;
  std::optional<Status> terminal_error;
};

QueryCursor::QueryCursor(std::unique_ptr<State> state)
    : state_(std::move(state)) {}
QueryCursor::~QueryCursor() = default;
QueryCursor::QueryCursor(QueryCursor&&) noexcept = default;
QueryCursor& QueryCursor::operator=(QueryCursor&&) noexcept = default;

StatusOr<std::optional<QueryBatch>> QueryCursor::Next() {
  if (!state_) {
    return Status::InvalidArgument("query cursor", "moved-from cursor");
  }
  if (state_->terminal_error.has_value()) return *state_->terminal_error;
  if (state_->clean_terminal) return std::optional<QueryBatch>{};

  if (!state_->initialized) {
    state_->initialized = true;
    auto vertices = internal::CanonicalSource::ReadVerticesAt(
        *state_->snapshot, state_->plan.valid_time);
    if (!vertices.ok()) {
      state_->terminal_error = vertices.status();
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    if (vertices.ValueOrDie().size() > state_->options.budget.output_rows) {
      state_->terminal_error = Status::ResourceExhausted(
          "query", "output row budget exceeded");
      state_->snapshot.reset();
      return *state_->terminal_error;
    }

    constexpr size_t kBatchRows = 1024;
    for (size_t offset = 0; offset < vertices.ValueOrDie().size();
         offset += kBatchRows) {
      const size_t count =
          std::min(kBatchRows, vertices.ValueOrDie().size() - offset);
      std::vector<VertexRef> values(
          vertices.ValueOrDie().begin() + static_cast<ptrdiff_t>(offset),
          vertices.ValueOrDie().begin() +
              static_cast<ptrdiff_t>(offset + count));
      QueryColumn column{state_->plan.vertex_slot, QueryType::kVertexRef,
                         std::move(values),
                         std::vector<uint8_t>(count, uint8_t{1})};
      state_->batches.emplace_back(
          QueryBatch(count, std::vector<QueryColumn>{std::move(column)}));
    }
  }

  if (state_->next_batch < state_->batches.size()) {
    return std::optional<QueryBatch>{
        std::move(state_->batches[state_->next_batch++])};
  }
  state_->batches.clear();
  state_->snapshot.reset();
  state_->clean_terminal = true;
  return std::optional<QueryBatch>{};
}

Status QueryCursor::Close() {
  if (!state_) {
    return Status::InvalidArgument("query cursor", "moved-from cursor");
  }
  state_->batches.clear();
  state_->snapshot.reset();
  state_->terminal_error.reset();
  state_->clean_terminal = true;
  return Status::OK();
}

namespace internal {
namespace {

void CollectReferencedProperties(const LogicalPlanNode& node,
                                 std::vector<PropertyId>* properties) {
  if (node.property_binding().has_value()) {
    const PropertyId property = node.property_binding()->property;
    if (std::none_of(properties->begin(), properties->end(),
                     [property](PropertyId candidate) {
                       return candidate == property;
                     })) {
      properties->push_back(property);
    }
  }
  for (const auto& input : node.inputs()) {
    CollectReferencedProperties(*input, properties);
  }
}

bool IsVertexProjection(const LogicalPlanNode& project, SlotId vertex_slot) {
  return !project.schema().columns().empty() &&
         std::all_of(project.schema().columns().begin(),
                     project.schema().columns().end(),
                     [vertex_slot](const RowColumn& column) {
                       return column.slot == vertex_slot &&
                              column.type == QueryType::kVertexRef &&
                              !column.optional;
                     });
}

}  // namespace

StatusOr<PreparedQueryPlan> AnalyzeQuery(const Query& query) {
  const LogicalPlanNode* root = LogicalPlanInspector::Inspect(query);
  if (root == nullptr) {
    return Status::InvalidArgument("query", "missing logical plan");
  }

  PreparedQueryPlan plan;
  plan.output_columns = root->schema().columns();
  CollectReferencedProperties(*root, &plan.referenced_properties);
  if (root->kind() != LogicalOpKind::kProject || root->inputs().size() != 1) {
    return plan;
  }
  const LogicalPlanNode& state_at = *root->inputs().front();
  if (state_at.kind() != LogicalOpKind::kStateAt ||
      state_at.inputs().size() != 1 || !state_at.scope().has_value() ||
      !std::holds_alternative<At>(*state_at.scope())) {
    return plan;
  }
  const LogicalPlanNode& scan = *state_at.inputs().front();
  if (scan.kind() != LogicalOpKind::kVertexScan || !scan.inputs().empty() ||
      scan.schema().columns().size() != 1) {
    return plan;
  }
  const RowColumn& vertex = scan.schema().columns().front();
  if (vertex.type != QueryType::kVertexRef || vertex.optional ||
      !IsVertexProjection(*root, vertex.slot)) {
    return plan;
  }
  plan.canonical_vertex_state_at = true;
  plan.vertex_slot = vertex.slot;
  plan.valid_time = std::get<At>(*state_at.scope()).time;
  return plan;
}

StatusOr<QueryCursor> QueryRuntime::Execute(const PreparedQueryPlan& plan,
                                            Snapshot snapshot,
                                            const Bindings&,
                                            const QueryOptions& options) {
  return QueryCursor(std::make_unique<QueryCursor::State>(
      plan, std::move(snapshot), options));
}

}  // namespace internal
}  // namespace cedar
