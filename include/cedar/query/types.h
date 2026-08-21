// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_TYPES_H_
#define CEDAR_QUERY_TYPES_H_

#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

#include "cedar/fact/fact.h"

namespace cedar {

struct ValidTimeInterval {
  ValidTime from;
  std::optional<ValidTime> to;

  Status Validate() const;
  bool operator==(const ValidTimeInterval&) const = default;
};

struct ValidDuration {
  uint64_t value = 0;
  constexpr bool operator==(const ValidDuration&) const = default;
};

struct At {
  ValidTime time;
};
struct Events {
  ValidTimeInterval interval;
};
struct Changes {
  ValidTimeInterval interval;
};
struct Overlaps {
  ValidTimeInterval interval;
};
struct Throughout {
  ValidTimeInterval interval;
};
struct History {
  std::optional<ValidTimeInterval> interval;
};
using TemporalScope =
    std::variant<At, Events, Changes, Overlaps, Throughout, History>;

enum class QueryType : uint8_t {
  kBool,
  kInt32,
  kInt64,
  kFloat32,
  kFloat64,
  kTimestamp64,
  kString,
  kBinary,
  kVertexRef,
  kEdgeRef,
  kValidTime,
  kValidDuration,
  kCommitSeq,
  kValidTimeInterval,
  kPath,
  kJourney,
};

struct SlotId {
  uint32_t value = 0;
  constexpr bool operator==(const SlotId&) const = default;
};

struct ParameterId {
  uint32_t value = 0;
  constexpr bool operator==(const ParameterId&) const = default;
};

class Bindings {
 public:
  Status Bind(ParameterId parameter, QueryType type, Value value);
  Status Bind(ParameterId parameter, QueryType type, VertexRef value);
  Status Bind(ParameterId parameter, QueryType type, EdgeRef value);
  Status Bind(ParameterId parameter, QueryType type, ValidTime value);
  Status Bind(ParameterId parameter, QueryType type, ValidDuration value);
  Status Bind(ParameterId parameter, QueryType type, CommitSeq value);
  Status Bind(ParameterId parameter, QueryType type, ValidTimeInterval value);

 private:
  using BoundValue =
      std::variant<Value, VertexRef, EdgeRef, ValidTime, ValidDuration,
                   CommitSeq, ValidTimeInterval>;

  struct Binding {
    ParameterId parameter;
    QueryType type;
    BoundValue value;
  };

  Status Bind(ParameterId parameter, QueryType type, BoundValue value);

  std::vector<Binding> bindings_;
};

enum class QueryExecutionMode : uint8_t { kAuto, kInteractive, kAnalytical };

struct QueryBudget {
  uint64_t memory_bytes = 8ULL * 1024ULL * 1024ULL;
  uint64_t scratch_bytes = 0;
  uint64_t read_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t prefetch_bytes = 8ULL * 1024ULL * 1024ULL;
  uint64_t decoded_rows = 1'000'000;
  uint64_t output_rows = 1'000'000;
  uint64_t output_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t interval_fragments = 1'000'000;
  uint64_t graph_labels = 1'000'000;
  uint64_t visited_vertices = 1'000'000;
  uint64_t cpu_us = 0;
  uint64_t deadline_us = 0;
  uint32_t max_parallelism = 1;
  uint32_t max_hops = 4;
  uint32_t retained_output_batches = 2;
};

struct QueryOptions {
  QueryExecutionMode mode = QueryExecutionMode::kAuto;
  QueryBudget budget;
  bool capture_profile = false;
};

}  // namespace cedar

#endif  // CEDAR_QUERY_TYPES_H_
