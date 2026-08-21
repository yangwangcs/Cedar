// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>

#include "cedar/query.h"
#include "query/logical/expression.h"
#include "query/logical/logical_plan.h"

namespace cedar {
namespace {

struct TemporalScopeCase {
  TemporalScope scope;
  internal::LogicalOpKind kind;
};

void ExpectSameScope(const TemporalScope& actual,
                     const TemporalScope& expected) {
  ASSERT_EQ(actual.index(), expected.index());
  std::visit(
      [](const auto& actual_scope, const auto& expected_scope) {
        using Actual = std::decay_t<decltype(actual_scope)>;
        using Expected = std::decay_t<decltype(expected_scope)>;
        if constexpr (!std::is_same_v<Actual, Expected>) {
          ADD_FAILURE() << "temporal scope alternatives differ";
        } else if constexpr (std::is_same_v<Actual, At>) {
          EXPECT_EQ(actual_scope.time, expected_scope.time);
        } else {
          EXPECT_EQ(actual_scope.interval, expected_scope.interval);
        }
      },
      actual, expected);
}

void ExpectTemporalScopePlan(
    const StatusOr<Query>& query, internal::LogicalOpKind temporal_kind,
    internal::LogicalOpKind scan_kind, const TemporalScope& expected_scope) {
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  const internal::LogicalPlanNode* temporal =
      internal::LogicalPlanInspector::Inspect(query.ValueOrDie());
  ASSERT_NE(temporal, nullptr);
  EXPECT_EQ(temporal->kind(), temporal_kind);
  ASSERT_TRUE(temporal->scope().has_value());
  ExpectSameScope(*temporal->scope(), expected_scope);
  ASSERT_EQ(temporal->inputs().size(), 1U);

  const auto& scan = temporal->inputs()[0];
  ASSERT_NE(scan, nullptr);
  EXPECT_EQ(scan->kind(), scan_kind);
  EXPECT_FALSE(scan->scope().has_value());
  EXPECT_TRUE(scan->inputs().empty());
}

TEST(LogicalPlanTest, BuildsTypedVertexPropertyPredicate) {
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  OptionalSlot<int64_t> age = OptionalSlot<int64_t>::Named("age");
  auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto bound = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, age);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto filtered = bound.ValueOrDie().Where(
      IsPresent(age) && GreaterThan(ValueOf(age), Literal<int64_t>(18)));
  ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();
  auto selected = filtered.ValueOrDie().Select(
      {Project(vertex), Project(age)});
  ASSERT_TRUE(selected.ok()) << selected.status().ToString();
  EXPECT_EQ(selected.ValueOrDie().schema().columns().size(), 2U);
}

TEST(LogicalPlanTest, MaterializesTemporalScopesAboveSourceScans) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const Slot<EdgeRef> edge = Slot<EdgeRef>::Named("e");
  const ValidTimeInterval interval{ValidTime{10}, ValidTime{20}};
  const std::vector<TemporalScopeCase> cases{
      {At{ValidTime{1}}, internal::LogicalOpKind::kStateAt},
      {Events{interval}, internal::LogicalOpKind::kEventsBetween},
      {Changes{interval}, internal::LogicalOpKind::kChangesBetween},
      {Overlaps{interval}, internal::LogicalOpKind::kStateOverlaps},
      {Throughout{interval}, internal::LogicalOpKind::kStateThroughout},
      {History{interval}, internal::LogicalOpKind::kHistory},
  };

  for (const TemporalScopeCase& test_case : cases) {
    ExpectTemporalScopePlan(Query::Vertices(vertex, test_case.scope),
                            test_case.kind,
                            internal::LogicalOpKind::kVertexScan,
                            test_case.scope);
    ExpectTemporalScopePlan(Query::Edges(edge, test_case.scope),
                            test_case.kind, internal::LogicalOpKind::kEdgeScan,
                            test_case.scope);
  }
}

template <typename Left, typename Right>
concept CanCompareEqual = requires(Left left, Right right) {
  Equal(ValueOf(left), Literal<Right>(right));
};
static_assert(!CanCompareEqual<OptionalSlot<int64_t>, std::string>);
static_assert(std::is_same_v<
              decltype(ValueOf(std::declval<OptionalSlot<int32_t>>())),
              OptionalExpr<int32_t>>);

TEST(LogicalPlanTest, PreservesNotOverOptionalEquality) {
  const OptionalSlot<int64_t> age = OptionalSlot<int64_t>::Named("age");
  const Expr<bool> expression = Not(Equal(ValueOf(age), Literal<int64_t>(18)));

  const internal::ExpressionNode* node =
      internal::ExpressionInspector::Inspect(expression);
  ASSERT_NE(node, nullptr);
  ASSERT_EQ(node->kind(), internal::ExpressionKind::kNot);
  ASSERT_EQ(node->children().size(), 1U);
  EXPECT_EQ(node->children()[0]->kind(),
            internal::ExpressionKind::kEqual);
}

TEST(LogicalPlanTest, RejectsDuplicateSlotIds) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();

  const auto selected = source.ValueOrDie().Select(
      {Project(vertex), Project(vertex)});
  EXPECT_TRUE(selected.status().IsInvalidArgument());
}

TEST(LogicalPlanTest, RejectsPredicatesWithSlotsOutsideInputSchema) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const Slot<int64_t> missing = Slot<int64_t>::Named("missing");
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();

  const auto filtered = source.ValueOrDie().Where(
      Not(Equal(ValueOf(missing), Literal<int64_t>(18))));
  EXPECT_TRUE(filtered.status().IsInvalidArgument());
}

TEST(LogicalPlanTest, RejectsPredicatesWithSlotsOfWrongInputType) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const Slot<int64_t> wrong_type = Slot<int64_t>::WithId(vertex.id());
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();

  const auto filtered = source.ValueOrDie().Where(
      Not(Equal(ValueOf(wrong_type), Literal<int64_t>(18))));
  EXPECT_TRUE(filtered.status().IsInvalidArgument());
}

TEST(LogicalPlanTest, RejectsExpandSlotsDuplicatingInputSchema) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const OptionalSlot<int64_t> age = OptionalSlot<int64_t>::Named("age");
  const Slot<VertexRef> destination = Slot<VertexRef>::Named("dst");
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  const auto bound = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, age);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();

  const ExpandSpec duplicate_edge{
      vertex, Slot<EdgeRef>::WithId(age.id()), destination,
      ExpandDirection::kOut};
  const auto duplicate_edge_result = bound.ValueOrDie().Expand(duplicate_edge);
  EXPECT_TRUE(duplicate_edge_result.status().IsInvalidArgument());

  const ExpandSpec duplicate_destination{
      vertex, Slot<EdgeRef>::Named("e"), Slot<VertexRef>::WithId(age.id()),
      ExpandDirection::kOut};
  const auto duplicate_destination_result =
      bound.ValueOrDie().Expand(duplicate_destination);
  EXPECT_TRUE(duplicate_destination_result.status().IsInvalidArgument());
}

TEST(LogicalPlanTest, RetainsFilterExpandAndPropertyBindingSemantics) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const Slot<EdgeRef> edge = Slot<EdgeRef>::Named("e");
  const Slot<VertexRef> destination = Slot<VertexRef>::Named("dst");
  const OptionalSlot<int64_t> age = OptionalSlot<int64_t>::Named("age");
  const ExpandSpec expand{vertex, edge, destination, ExpandDirection::kIn};

  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  const auto expanded = source.ValueOrDie().Expand(expand);
  ASSERT_TRUE(expanded.ok()) << expanded.status().ToString();
  const auto bound = expanded.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, age);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto filtered = bound.ValueOrDie().Where(
      IsPresent(ValueOf(age)) &&
      GreaterThan(ValueOf(age), Literal<int64_t>(18)));
  ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();

  const internal::LogicalPlanNode* filter =
      internal::LogicalPlanInspector::Inspect(filtered.ValueOrDie());
  ASSERT_NE(filter, nullptr);
  ASSERT_EQ(filter->kind(), internal::LogicalOpKind::kFilter);
  ASSERT_NE(filter->predicate(), nullptr);
  EXPECT_EQ(filter->predicate()->kind(), internal::ExpressionKind::kAnd);
  ASSERT_EQ(filter->predicate()->children().size(), 2U);
  const auto& greater_than = filter->predicate()->children()[1];
  ASSERT_EQ(greater_than->kind(), internal::ExpressionKind::kGreaterThan);
  ASSERT_EQ(greater_than->children().size(), 2U);
  EXPECT_EQ(greater_than->children()[0]->slot(), age.id());
  ASSERT_TRUE(greater_than->children()[1]->literal().has_value());
  EXPECT_EQ(std::get<int64_t>(*greater_than->children()[1]->literal()),
            int64_t{18});

  const auto& property = filter->inputs()[0];
  ASSERT_TRUE(property->property_binding().has_value());
  EXPECT_EQ(property->property_binding()->source, vertex.id());
  EXPECT_EQ(property->property_binding()->property, PropertyId{7});
  EXPECT_EQ(property->property_binding()->output.slot, age.id());

  const auto& expansion = property->inputs()[0];
  ASSERT_TRUE(expansion->expand_spec().has_value());
  EXPECT_EQ(expansion->expand_spec()->source.id(), vertex.id());
  EXPECT_EQ(expansion->expand_spec()->edge.id(), edge.id());
  EXPECT_EQ(expansion->expand_spec()->destination.id(), destination.id());
  EXPECT_EQ(expansion->expand_spec()->direction, ExpandDirection::kIn);
}

TEST(LogicalPlanTest, OptionalExpressionsCanBeTestedAndCompared) {
  const OptionalSlot<int32_t> rank = OptionalSlot<int32_t>::Named("rank");
  const OptionalExpr<int32_t> optional_rank = ValueOf(rank);

  EXPECT_TRUE(optional_rank.valid());
  EXPECT_TRUE(IsPresent(optional_rank).valid());
  EXPECT_TRUE(GreaterThan(optional_rank, Literal<int32_t>(3)).valid());
}

TEST(LogicalPlanTest, StringExpressionBuildersAreLinkable) {
  const Slot<std::string> name = Slot<std::string>::Named("name");
  const Expr<bool> expression = Equal(
      ValueOf(name), Literal<std::string>("cedar"));

  EXPECT_TRUE(expression.valid());
  const internal::ExpressionNode* node =
      internal::ExpressionInspector::Inspect(expression);
  ASSERT_NE(node, nullptr);
  ASSERT_EQ(node->kind(), internal::ExpressionKind::kEqual);
  EXPECT_EQ(node->children()[0]->slot(), name.id());
  ASSERT_TRUE(node->children()[1]->literal().has_value());
  EXPECT_EQ(std::get<std::string>(*node->children()[1]->literal()),
            std::string("cedar"));
}

TEST(LogicalPlanTest, RetainsDisplayNamesAcrossSchemaTransformations) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("vertex");
  const Slot<EdgeRef> edge = Slot<EdgeRef>::Named("edge");
  const Slot<VertexRef> destination = Slot<VertexRef>::Named("destination");
  const OptionalSlot<Binary> payload = OptionalSlot<Binary>::Named("payload");

  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  ASSERT_EQ(source.ValueOrDie().schema().columns().size(), 1U);
  EXPECT_EQ(source.ValueOrDie().schema().columns()[0].name, "vertex");
  const auto expanded = source.ValueOrDie().Expand(
      ExpandSpec{vertex, edge, destination, ExpandDirection::kOut});
  ASSERT_TRUE(expanded.ok()) << expanded.status().ToString();
  ASSERT_EQ(expanded.ValueOrDie().schema().columns().size(), 3U);
  EXPECT_EQ(expanded.ValueOrDie().schema().columns()[1].name, "edge");
  EXPECT_EQ(expanded.ValueOrDie().schema().columns()[2].name, "destination");
  const auto bound = expanded.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, payload);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  ASSERT_EQ(bound.ValueOrDie().schema().columns().size(), 4U);
  EXPECT_EQ(bound.ValueOrDie().schema().columns()[3].name, "payload");
  const internal::LogicalPlanNode* binding =
      internal::LogicalPlanInspector::Inspect(bound.ValueOrDie());
  ASSERT_NE(binding, nullptr);
  ASSERT_TRUE(binding->property_binding().has_value());
  EXPECT_EQ(binding->property_binding()->output.name, "payload");
  const auto projected = bound.ValueOrDie().Select(
      {Project(vertex), Project(edge), Project(destination), Project(payload)});
  ASSERT_TRUE(projected.ok()) << projected.status().ToString();

  const std::vector<RowColumn>& columns = projected.ValueOrDie().schema().columns();
  ASSERT_EQ(columns.size(), 4U);
  EXPECT_EQ(columns[0].name, "vertex");
  EXPECT_EQ(columns[1].name, "edge");
  EXPECT_EQ(columns[2].name, "destination");
  EXPECT_EQ(columns[3].name, "payload");
}

TEST(LogicalPlanTest, RepresentsParametersAsTypedExpressionLeaves) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const OptionalSlot<int64_t> age = OptionalSlot<int64_t>::Named("age");
  const Parameter<int64_t> minimum = Parameter<int64_t>::Named("minimum");
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  const auto bound = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, age);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto filtered = bound.ValueOrDie().Where(
      Equal(ValueOf(age), ValueOf(minimum)));
  ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();

  const internal::ExpressionNode* predicate =
      internal::LogicalPlanInspector::Inspect(filtered.ValueOrDie())->predicate().get();
  ASSERT_NE(predicate, nullptr);
  ASSERT_EQ(predicate->kind(), internal::ExpressionKind::kEqual);
  ASSERT_EQ(predicate->children().size(), 2U);
  const auto& parameter = predicate->children()[1];
  EXPECT_EQ(parameter->kind(), internal::ExpressionKind::kParameter);
  EXPECT_EQ(parameter->parameter(), minimum.id());
  EXPECT_EQ(parameter->type(), QueryType::kInt64);
}

TEST(LogicalPlanTest, MapsEveryScalarPhysicalTypeToADistinctPublicType) {
  static_assert(QueryTypeOf<bool>() == QueryType::kBool);
  static_assert(QueryTypeOf<int32_t>() == QueryType::kInt32);
  static_assert(QueryTypeOf<int64_t>() == QueryType::kInt64);
  static_assert(QueryTypeOf<float>() == QueryType::kFloat32);
  static_assert(QueryTypeOf<double>() == QueryType::kFloat64);
  static_assert(QueryTypeOf<Timestamp64>() == QueryType::kTimestamp64);
  static_assert(QueryTypeOf<std::string>() == QueryType::kString);
  static_assert(QueryTypeOf<Binary>() == QueryType::kBinary);
  static_assert(!std::is_convertible_v<uint64_t, Timestamp64>);
  static_assert(!std::is_convertible_v<std::string, Binary>);

  EXPECT_NE(QueryTypeOf<Timestamp64>(), QueryType::kInt64);
  EXPECT_NE(QueryTypeOf<Binary>(), QueryType::kString);
}

TEST(LogicalPlanTest, RejectsPredicatesWithSlotsOfWrongPresenceCapability) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const OptionalSlot<VertexRef> optional_vertex =
      OptionalSlot<VertexRef>::WithId(vertex.id());
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();

  const auto filtered = source.ValueOrDie().Where(IsPresent(optional_vertex));
  EXPECT_TRUE(filtered.status().IsInvalidArgument());
}

TEST(LogicalPlanTest, RejectsInvalidExpandDirection) {
  const Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  const Slot<EdgeRef> edge = Slot<EdgeRef>::Named("e");
  const Slot<VertexRef> destination = Slot<VertexRef>::Named("dst");
  const auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();

  const auto expanded = source.ValueOrDie().Expand(
      ExpandSpec{vertex, edge, destination, static_cast<ExpandDirection>(99)});
  EXPECT_TRUE(expanded.status().IsInvalidArgument());
}

}  // namespace
}  // namespace cedar
