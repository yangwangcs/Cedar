// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/query/types.h"

#include <algorithm>
#include <utility>

namespace cedar {
namespace {

std::optional<QueryType> QueryTypeFor(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
      return QueryType::kBool;
    case PhysicalType::kInt32:
      return QueryType::kInt32;
    case PhysicalType::kInt64:
      return QueryType::kInt64;
    case PhysicalType::kFloat32:
      return QueryType::kFloat32;
    case PhysicalType::kFloat64:
      return QueryType::kFloat64;
    case PhysicalType::kTimestamp64:
      return QueryType::kTimestamp64;
    case PhysicalType::kString:
      return QueryType::kString;
    case PhysicalType::kBinary:
      return QueryType::kBinary;
  }
  return std::nullopt;
}

template <typename T>
Status BindTypedValue(Bindings* bindings, ParameterId parameter,
                      QueryType expected, QueryType actual, T value) {
  if (expected != actual) {
    return Status::BindError("parameter value type differs from parameter type");
  }
  return bindings->Bind(parameter, expected, std::move(value));
}

}  // namespace

Status ValidTimeInterval::Validate() const {
  if (to.has_value() && from.value >= to->value) {
    return Status::InvalidArgument("valid time interval must satisfy from < to");
  }
  return Status::OK();
}

Status Bindings::Bind(ParameterId parameter, QueryType type, Value value) {
  const std::optional<QueryType> value_type = QueryTypeFor(value);
  if (!value_type.has_value() || type != *value_type) {
    return Status::BindError("parameter value type differs from parameter type");
  }
  return Bind(parameter, type, BoundValue(std::move(value)));
}

Status Bindings::Bind(ParameterId parameter, QueryType type, VertexRef value) {
  return BindTypedValue(this, parameter, type, QueryType::kVertexRef,
                        std::move(value));
}

Status Bindings::Bind(ParameterId parameter, QueryType type, EdgeRef value) {
  return BindTypedValue(this, parameter, type, QueryType::kEdgeRef,
                        std::move(value));
}

Status Bindings::Bind(ParameterId parameter, QueryType type, ValidTime value) {
  return BindTypedValue(this, parameter, type, QueryType::kValidTime,
                        std::move(value));
}

Status Bindings::Bind(ParameterId parameter, QueryType type,
                      ValidDuration value) {
  return BindTypedValue(this, parameter, type, QueryType::kValidDuration,
                        std::move(value));
}

Status Bindings::Bind(ParameterId parameter, QueryType type, CommitSeq value) {
  return BindTypedValue(this, parameter, type, QueryType::kCommitSeq,
                        std::move(value));
}

Status Bindings::Bind(ParameterId parameter, QueryType type,
                      ValidTimeInterval value) {
  return BindTypedValue(this, parameter, type, QueryType::kValidTimeInterval,
                        std::move(value));
}

Status Bindings::Bind(ParameterId parameter, QueryType type, BoundValue value) {
  const auto existing = std::find_if(
      bindings_.begin(), bindings_.end(), [parameter](const Binding& binding) {
        return binding.parameter == parameter;
      });
  if (existing != bindings_.end()) {
    return Status::BindError("parameter is bound more than once");
  }
  bindings_.push_back(Binding{parameter, type, std::move(value)});
  return Status::OK();
}

}  // namespace cedar
