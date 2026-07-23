// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_LDBC_ADAPTER_H_
#define CEDAR_BENCHMARK_LDBC_ADAPTER_H_

#include <cstdint>
#include <string>

#include "cedar/benchmark/cedar_tg.h"

namespace cedar {

// Metadata is kept with the adapted dataset and must be copied into the run
// manifest. Cedar does not claim this derived input is an official temporal
// LDBC benchmark.
struct LdbcAdapterConfig {
  std::string source_name = "LDBC-SNB-derived";
  std::string source_license = "";
  std::string transform_policy = "";
  uint16_t vertex_property_column = 1;
  uint16_t edge_type = 1;
  uint32_t schema_epoch = 1;
};

struct LdbcTemporalDataset {
  CedarTgDataset dataset;
  std::string source_name;
  std::string source_license;
  std::string transform_policy;
};

// Adapt the deliberately small interchange subset used by paper runs. Nodes
// use `id,valid_from,commit_seq,name,operation`; edges use
// `source_id,target_id,edge_id,edge_type,valid_from,commit_seq,operation`.
// PUT/DELETE are case-insensitive. Input order is not significant; output is
// sorted by the canonical event ordering and hashed deterministically.
StatusOr<LdbcTemporalDataset> AdaptLdbcCsv(
    const std::string& nodes_csv, const std::string& edges_csv,
    const LdbcAdapterConfig& config = {});

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_LDBC_ADAPTER_H_
