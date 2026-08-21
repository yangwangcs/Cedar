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

}  // namespace
}  // namespace cedar
