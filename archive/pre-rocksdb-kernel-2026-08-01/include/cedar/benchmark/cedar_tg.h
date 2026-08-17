// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_CEDAR_TG_H_
#define CEDAR_BENCHMARK_CEDAR_TG_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"

namespace cedar {

// The CI, workstation, and paper profiles are all projections of this same
// deterministic event model. Counts may scale, but event semantics do not.
struct CedarTgConfig {
  uint64_t seed = 1;
  uint64_t vertex_count = 0;
  uint64_t edge_count = 0;
  uint32_t property_events_per_vertex = 1;
  uint64_t valid_time_span = 1;
};

struct CedarTgDataset {
  CedarTgConfig config;
  std::vector<TemporalEvent> events;
  uint64_t vertex_events = 0;
  uint64_t edge_events = 0;
  uint64_t property_events = 0;
  std::string dataset_hash;
};

StatusOr<CedarTgDataset> GenerateCedarTg(const CedarTgConfig& config);

// Returns the canonical content hash used by Cedar-TG artifacts. Adapters
// that produce the same event model use this helper so provenance remains
// comparable across generators.
std::string HashCedarTgDataset(const CedarTgDataset& dataset);

// A stable binary artifact for benchmark provenance and oracle inputs. The
// writer publishes atomically and does not alter a database under measurement.
Status WriteCedarTgCanonicalFile(const std::string& path, const CedarTgDataset& dataset);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_CEDAR_TG_H_
