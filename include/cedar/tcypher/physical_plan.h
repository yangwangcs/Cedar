// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_PHYSICAL_PLAN_H_
#define CEDAR_TCYPHER_PHYSICAL_PLAN_H_

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/optimizer/cost_model.h"
#include "cedar/tcypher/binder.h"
#include "cedar/tcypher/logical_plan.h"
#include "cedar/types/value.h"

namespace cedar {

#define CEDAR_STRONG_PLAN_ID(name)                                             \
  struct name {                                                               \
    uint32_t value = 0;                                                       \
    friend bool operator==(name left, name right) { return left.value == right.value; } \
    friend bool operator!=(name left, name right) { return !(left == right); } \
    friend bool operator<(name left, name right) { return left.value < right.value; } \
  }

CEDAR_STRONG_PLAN_ID(SlotId);
CEDAR_STRONG_PLAN_ID(OperatorId);
CEDAR_STRONG_PLAN_ID(PipelineId);
CEDAR_STRONG_PLAN_ID(TemporalContextId);

#undef CEDAR_STRONG_PLAN_ID

struct SlotDescriptor {
  SlotId id;
  BindingId binding;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;

  friend bool operator==(const SlotDescriptor& left, const SlotDescriptor& right) {
    return left.id == right.id && left.binding == right.binding &&
        left.type == right.type && left.nullable == right.nullable;
  }
};

enum class PhysicalOperatorKind : uint8_t {
  kTemporalPointScan,
  kTemporalRangeScan,
  kChangeScan,
  kIntervalDerive,
  kIntervalAlign,
  kTemporalCoalesce,
  kExpand,
  kHashJoin,
  kCrossJoin,
  kDistinct,
  kAggregate,
  kSort,
  kPropertyGather,
  kFilter,
  kMetadataProject,
  kProject,
  kResultSink,
};

const char* PhysicalOperatorKindName(PhysicalOperatorKind kind);

enum class PhysicalTemporalMode : uint8_t {
  kPoint,
  kValidTimeRange,
  kValidTimeChanges,
  kSystemTimeChanges,
};

struct PhysicalOperatorSpec {
  OperatorId id;
  PhysicalOperatorKind kind = PhysicalOperatorKind::kTemporalPointScan;
  std::vector<SlotId> required_slots;
  std::vector<SlotId> produced_slots;

  friend bool operator==(const PhysicalOperatorSpec& left,
                         const PhysicalOperatorSpec& right) {
    return left.id == right.id && left.kind == right.kind &&
        left.required_slots == right.required_slots &&
        left.produced_slots == right.produced_slots;
  }
};

struct PipelineDescriptor {
  PipelineId id;
  std::vector<OperatorId> operators;
  std::vector<PipelineId> dependencies;

  friend bool operator==(const PipelineDescriptor& left,
                         const PipelineDescriptor& right) {
    return left.id == right.id && left.operators == right.operators &&
        left.dependencies == right.dependencies;
  }
};

enum class PhysicalPredicateKind : uint8_t { kEquality, kIn, kRange, kPrefix };

struct PhysicalPredicate {
  SlotId slot;
  ColumnSchema column;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
  PhysicalPredicateKind kind = PhysicalPredicateKind::kEquality;
  std::vector<Value> values;
  std::optional<Value> lower_bound;
  std::optional<Value> upper_bound;
  bool lower_inclusive = true;
  bool upper_inclusive = true;
};

enum class PhysicalExpressionKind : uint8_t {
  kSlot,
  kOperationName,
  kRelationshipBinding,
  kPathBinding,
};

struct PhysicalExpression {
  PhysicalExpressionKind kind = PhysicalExpressionKind::kSlot;
  SlotId referenced_slot;
  SlotId output_slot;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
  std::string output_name;
  std::vector<SlotId> relationship_slots;
  BindingId binding_id;
  ReturnExpressionKind result_kind = ReturnExpressionKind::kBinding;
  std::optional<BoundPropertyId> property_id;
  bool relationship_identity = false;
};

struct PhysicalPropertySlot {
  SlotId slot;
  ColumnSchema column;
  BindingId binding;
};

struct PhysicalExpandSpec {
  BindingId source_binding;
  BindingId relationship_binding;
  BindingId target_binding;
  SlotId source_slot;
  SlotId target_slot;
  SlotId edge_type_slot;
  SlotId edge_id_slot;
  SlotId valid_from_slot;
  SlotId commit_seq_slot;
  SlotId operation_slot;
  SlotId system_time_slot;
  SlotId valid_to_slot;
  EntityType direction = EntityType::EdgeOut;
  std::optional<uint16_t> edge_type;
  uint32_t min_hops = 1;
  uint32_t max_hops = 1;
  SlotId path_slot;
};

enum class PhysicalAggregateKind : uint8_t {
  kCount,
  kSum,
  kAvg,
  kMin,
  kMax,
  kCollect,
};

enum class PhysicalAggregateValueKind : uint8_t {
  kScalar,
  kStruct,
  kList,
};

struct PhysicalAggregateExpression {
  PhysicalAggregateKind kind = PhysicalAggregateKind::kCount;
  uint32_t input_column = 0;
  std::string output_name;
  PhysicalAggregateValueKind input_kind =
      PhysicalAggregateValueKind::kScalar;
};

struct PhysicalAggregateOutput {
  bool aggregate = false;
  uint32_t index = 0;
};

struct PhysicalAggregateSinkSpec {
  PhysicalOperatorSpec op;
  std::vector<uint32_t> group_columns;
  std::vector<PhysicalAggregateExpression> aggregates;
  std::vector<PhysicalAggregateOutput> outputs;
};

struct PhysicalSortSinkSpec {
  PhysicalOperatorSpec op;
  uint32_t input_column = 0;
  bool descending = false;
};

class PhysicalPlan {
 public:
  PhysicalPlan(uint64_t plan_id, BindingId binding_id,
               std::vector<SlotDescriptor> slots,
               std::vector<PhysicalOperatorSpec> operators,
               std::vector<PipelineDescriptor> pipelines,
               std::vector<PhysicalPredicate> predicates,
               std::vector<PhysicalExpression> projections,
               std::vector<PhysicalPropertySlot> predicate_properties = {},
               std::vector<PhysicalPropertySlot> projection_properties = {},
               std::optional<uint64_t> exact_entity_id = std::nullopt,
               std::string exact_entity_parameter = {},
               bool include_valid_to = false, bool include_system_time = false,
               TemporalContextId temporal_context = TemporalContextId{1},
               PhysicalTemporalMode temporal_mode = PhysicalTemporalMode::kPoint,
               std::optional<std::pair<uint64_t, uint64_t>> valid_time_range = std::nullopt,
               std::optional<std::pair<uint64_t, uint64_t>> system_time_range = std::nullopt,
               std::optional<uint64_t> valid_time_as_of = std::nullopt,
               std::optional<PhysicalExpandSpec> expand = std::nullopt,
               std::vector<PhysicalExpandSpec> expand_steps = {},
               std::optional<PhysicalAggregateSinkSpec> aggregate_sink = std::nullopt,
               std::optional<PhysicalSortSinkSpec> sort_sink = std::nullopt,
               std::vector<PhysicalOperatorSpec> post_result_operators = {})
      : plan_id_(plan_id), binding_id_(binding_id), slots_(std::move(slots)),
        operators_(std::move(operators)), pipelines_(std::move(pipelines)),
        predicates_(std::move(predicates)), projections_(std::move(projections)),
        predicate_properties_(std::move(predicate_properties)),
        projection_properties_(std::move(projection_properties)),
        exact_entity_id_(exact_entity_id),
        exact_entity_parameter_(std::move(exact_entity_parameter)),
        include_valid_to_(include_valid_to), include_system_time_(include_system_time),
        temporal_context_(temporal_context), temporal_mode_(temporal_mode),
        valid_time_range_(valid_time_range), system_time_range_(system_time_range),
        valid_time_as_of_(valid_time_as_of), expand_(std::move(expand)),
        expand_steps_(std::move(expand_steps)),
        aggregate_sink_(std::move(aggregate_sink)),
        sort_sink_(std::move(sort_sink)),
        post_result_operators_(std::move(post_result_operators)) {
    if (expand_steps_.empty() && expand_.has_value()) expand_steps_.push_back(*expand_);
    if (!expand_.has_value() && !expand_steps_.empty()) expand_ = expand_steps_.front();
  }

  uint64_t plan_id() const { return plan_id_; }
  BindingId binding_id() const { return binding_id_; }
  const std::vector<SlotDescriptor>& slots() const { return slots_; }
  const std::vector<PhysicalOperatorSpec>& operators() const { return operators_; }
  const std::vector<PipelineDescriptor>& pipelines() const { return pipelines_; }
  const std::vector<PhysicalPredicate>& predicates() const { return predicates_; }
  const std::vector<PhysicalExpression>& projections() const { return projections_; }
  const std::vector<PhysicalPropertySlot>& predicate_properties() const {
    return predicate_properties_;
  }
  const std::vector<PhysicalPropertySlot>& projection_properties() const {
    return projection_properties_;
  }
  std::optional<uint64_t> exact_entity_id() const { return exact_entity_id_; }
  const std::string& exact_entity_parameter() const { return exact_entity_parameter_; }
  bool include_valid_to() const { return include_valid_to_; }
  bool include_system_time() const { return include_system_time_; }
  TemporalContextId temporal_context() const { return temporal_context_; }
  PhysicalTemporalMode temporal_mode() const { return temporal_mode_; }
  std::optional<std::pair<uint64_t, uint64_t>> valid_time_range() const {
    return valid_time_range_;
  }
  std::optional<std::pair<uint64_t, uint64_t>> system_time_range() const {
    return system_time_range_;
  }
  std::optional<uint64_t> valid_time_as_of() const { return valid_time_as_of_; }
  const std::optional<PhysicalExpandSpec>& expand() const { return expand_; }
  const std::vector<PhysicalExpandSpec>& expand_steps() const { return expand_steps_; }
  const std::optional<PhysicalAggregateSinkSpec>& aggregate_sink() const {
    return aggregate_sink_;
  }
  const std::optional<PhysicalSortSinkSpec>& sort_sink() const {
    return sort_sink_;
  }
  const std::vector<PhysicalOperatorSpec>& post_result_operators() const {
    return post_result_operators_;
  }

 private:
  uint64_t plan_id_;
  BindingId binding_id_;
  std::vector<SlotDescriptor> slots_;
  std::vector<PhysicalOperatorSpec> operators_;
  std::vector<PipelineDescriptor> pipelines_;
  std::vector<PhysicalPredicate> predicates_;
  std::vector<PhysicalExpression> projections_;
  std::vector<PhysicalPropertySlot> predicate_properties_;
  std::vector<PhysicalPropertySlot> projection_properties_;
  std::optional<uint64_t> exact_entity_id_;
  std::string exact_entity_parameter_;
  bool include_valid_to_;
  bool include_system_time_;
  TemporalContextId temporal_context_;
  PhysicalTemporalMode temporal_mode_;
  std::optional<std::pair<uint64_t, uint64_t>> valid_time_range_;
  std::optional<std::pair<uint64_t, uint64_t>> system_time_range_;
  std::optional<uint64_t> valid_time_as_of_;
  std::optional<PhysicalExpandSpec> expand_;
  std::vector<PhysicalExpandSpec> expand_steps_;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink_;
  std::optional<PhysicalSortSinkSpec> sort_sink_;
  std::vector<PhysicalOperatorSpec> post_result_operators_;
};

struct PhysicalCardinalityEstimate {
  uint64_t rows = 1000000;
  uint32_t confidence_per_mille = 0;
  bool conservative = true;
  bool index_available = false;
};

struct PhysicalHashJoinPlanningStats {
  uint64_t statistics_snapshot_id = 0;
  PhysicalCardinalityEstimate left;
  PhysicalCardinalityEstimate right;
};

enum class PhysicalHashJoinBuildSide : uint8_t { kLeft, kRight };

struct PhysicalHashJoinPlan {
  struct Output {
    bool from_left = true;
    uint32_t column = 0;
    PhysicalType type = PhysicalType::kBinary;
    bool nullable = true;
    std::string name;
  };

  uint64_t plan_id = 0;
  std::shared_ptr<const PhysicalPlan> left;
  std::shared_ptr<const PhysicalPlan> right;
  PhysicalOperatorSpec join;
  uint32_t left_binding_column = 0;
  uint32_t left_key_column = 1;
  uint32_t right_binding_column = 0;
  uint32_t right_key_column = 1;
  std::vector<uint32_t> left_key_columns;
  std::vector<uint32_t> right_key_columns;
  std::vector<std::string> output_names;
  std::vector<Output> outputs;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink;
  std::optional<PhysicalSortSinkSpec> sort_sink;
  std::vector<PhysicalOperatorSpec> post_join_operators;
  std::vector<PipelineDescriptor> pipelines;
  PhysicalHashJoinBuildSide build_side = PhysicalHashJoinBuildSide::kRight;
  PhysicalCardinalityEstimate left_estimate;
  PhysicalCardinalityEstimate right_estimate;
  uint64_t statistics_snapshot_id = 0;
};

struct PhysicalMultiJoinColumn {
  BindingId binding_id;
  ReturnExpressionKind kind = ReturnExpressionKind::kBinding;
  std::optional<BoundPropertyId> property_id;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
  std::string name;
  bool relationship_identity = false;
};

struct PhysicalMultiHashJoinStep {
  PhysicalOperatorSpec join;
  uint32_t input_index = 0;
  std::vector<uint32_t> accumulated_key_columns;
  std::vector<uint32_t> input_key_columns;
  std::vector<PhysicalHashJoinPlan::Output> outputs;
  std::vector<PhysicalMultiJoinColumn> output_layout;
  PhysicalHashJoinBuildSide build_side = PhysicalHashJoinBuildSide::kRight;
  PhysicalCardinalityEstimate accumulated_estimate;
  PhysicalCardinalityEstimate input_estimate;
};

struct PhysicalMultiHashJoinPlan {
  uint64_t plan_id = 0;
  uint64_t statistics_snapshot_id = 0;
  GraphOrder graph_order = GraphOrder::kAdjacencyFirst;
  std::vector<std::shared_ptr<const PhysicalPlan>> inputs;
  std::vector<std::vector<PhysicalMultiJoinColumn>> input_layouts;
  std::vector<uint64_t> input_snapshot_seqs;
  std::vector<uint32_t> join_order;
  std::vector<PhysicalMultiHashJoinStep> steps;
  std::vector<std::string> output_names;
  std::vector<uint32_t> final_output_columns;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink;
  std::optional<PhysicalSortSinkSpec> sort_sink;
  std::vector<PhysicalOperatorSpec> post_join_operators;
  std::vector<PipelineDescriptor> pipelines;
};

bool CanPlanPhysicalRootPoint(const BoundTcypherStatement& statement);
bool CanPlanPhysicalHashJoin(const BoundTcypherStatement& statement);
bool CanPlanPhysicalMultiHashJoin(const BoundTcypherStatement& statement);
StatusOr<std::shared_ptr<const PhysicalPlan>> PlanPhysicalRootPoint(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement = nullptr);
StatusOr<std::shared_ptr<const PhysicalHashJoinPlan>> PlanPhysicalHashJoin(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement = nullptr,
    PhysicalHashJoinPlanningStats planning_stats = {});
StatusOr<std::shared_ptr<const PhysicalMultiHashJoinPlan>>
PlanPhysicalMultiHashJoin(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement,
    const std::map<BindingId, PhysicalCardinalityEstimate>& estimates,
    uint64_t statistics_snapshot_id, uint64_t visible_seq_ceiling = 0);
Status ValidatePhysicalPlan(const PhysicalPlan& plan);
Status ValidatePhysicalHashJoinPlan(const PhysicalHashJoinPlan& plan);
Status ValidatePhysicalMultiHashJoinPlan(const PhysicalMultiHashJoinPlan& plan);
std::string FormatPhysicalPlan(const PhysicalPlan& plan);
std::string FormatPhysicalHashJoinPlan(const PhysicalHashJoinPlan& plan);
std::string FormatPhysicalMultiHashJoinPlan(const PhysicalMultiHashJoinPlan& plan);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_PHYSICAL_PLAN_H_
