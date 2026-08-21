// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <type_traits>

#include "cedar/query/types.h"

namespace cedar {

TEST(QueryTypesTest, ValidatesHalfOpenIntervals) {
  EXPECT_TRUE(
      (ValidTimeInterval{ValidTime{2}, ValidTime{5}}.Validate().ok()));
  EXPECT_TRUE(
      (ValidTimeInterval{ValidTime{2}, std::nullopt}.Validate().ok()));
  EXPECT_TRUE((ValidTimeInterval{ValidTime{2}, ValidTime{2}}
                   .Validate()
                   .IsInvalidArgument()));
  EXPECT_TRUE((ValidTimeInterval{ValidTime{5}, ValidTime{2}}
                   .Validate()
                   .IsInvalidArgument()));
}

TEST(QueryTypesTest, ExposesDistinctTerminalStatuses) {
  EXPECT_TRUE(Status::DeadlineExceeded("query").IsDeadlineExceeded());
  EXPECT_TRUE(Status::NumericOverflow("valid time").IsNumericOverflow());
}

TEST(QueryTypesTest, RejectsDuplicateParameterBindings) {
  Bindings bindings;
  EXPECT_TRUE(bindings
                  .Bind(ParameterId{1}, QueryType::kInt64, Value::Int64(7))
                  .ok());
  EXPECT_TRUE(bindings
                  .Bind(ParameterId{1}, QueryType::kInt64, Value::Int64(8))
                  .IsBindError());
}

TEST(QueryTypesTest, RejectsBindingsWhoseTypesDiffer) {
  Bindings bindings;
  EXPECT_TRUE(bindings
                  .Bind(ParameterId{1}, QueryType::kInt64, Value::Int32(7))
                  .IsBindError());
}

static_assert(
    std::is_same_v<decltype(QueryOptions{}.mode), QueryExecutionMode>);

template <typename T>
concept HasObsoleteQueryMemoryLimit = requires {
  T::QueryMemoryLimit("obsolete");
};

static_assert(!HasObsoleteQueryMemoryLimit<Status>);

}  // namespace cedar
