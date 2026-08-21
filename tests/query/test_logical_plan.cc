// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "cedar/query.h"
#include "query/logical/expression.h"

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

}  // namespace
}  // namespace cedar
