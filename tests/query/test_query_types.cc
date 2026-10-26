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

TEST(QueryTypesTest, BorrowedLookupPreservesPayloadAndTypedErrors) {
  Bindings bindings;
  ASSERT_TRUE(bindings.Bind(ParameterId{1}, QueryType::kString,
                            Value::String(std::string(4096, 's'))).ok());
  ASSERT_TRUE(bindings.Bind(ParameterId{2}, QueryType::kBinary,
                            Value::Binary(std::string("a\0b", 3))).ok());
  auto first = bindings.LookupBorrowed(ParameterId{1}, QueryType::kString);
  auto again = bindings.LookupBorrowed(ParameterId{1}, QueryType::kString);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(again.ok());
  EXPECT_EQ(first.ValueOrDie(), again.ValueOrDie());
  const auto& text = std::get<std::string>(std::get<Value>(*first.ValueOrDie()).data());
  auto copied = bindings.Lookup(ParameterId{1}, QueryType::kString);
  ASSERT_TRUE(copied.ok());
  const auto& copy = std::get<std::string>(std::get<Value>(copied.ValueOrDie()).data());
  EXPECT_EQ(text, copy);
  EXPECT_NE(text.data(), copy.data());
  auto binary = bindings.LookupBorrowed(ParameterId{2}, QueryType::kBinary);
  ASSERT_TRUE(binary.ok());
  EXPECT_EQ(std::get<std::string>(std::get<Value>(*binary.ValueOrDie()).data()),
            std::string("a\0b", 3));
  EXPECT_TRUE(bindings.LookupBorrowed(ParameterId{1}, QueryType::kBinary).status().IsBindError());
  EXPECT_TRUE(bindings.LookupBorrowed(ParameterId{3}, QueryType::kString).status().IsBindError());
}

static_assert(
    std::is_same_v<decltype(QueryOptions{}.mode), QueryExecutionMode>);

template <typename T>
concept HasObsoleteQueryMemoryLimit = requires {
  T::QueryMemoryLimit("obsolete");
};

static_assert(!HasObsoleteQueryMemoryLimit<Status>);

}  // namespace cedar
