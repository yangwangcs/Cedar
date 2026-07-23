// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_METRIC_EXPORTER_H_
#define CEDAR_OBSERVABILITY_METRIC_EXPORTER_H_

#include <string>

#include "cedar/observability/metric_registry.h"

namespace cedar {

// Deterministic local snapshot for diagnostics and benchmark artifacts.
// Export failures are intentionally outside all durability paths.
std::string ExportMetricsJson(const MetricRegistry& registry);

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_METRIC_EXPORTER_H_
