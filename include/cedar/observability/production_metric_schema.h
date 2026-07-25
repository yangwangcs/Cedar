// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_PRODUCTION_METRIC_SCHEMA_H_
#define CEDAR_OBSERVABILITY_PRODUCTION_METRIC_SCHEMA_H_

#include <vector>

#include "cedar/observability/metric_registry.h"

namespace cedar {

const std::vector<MetricDefinition>& ProductionMetricDefinitions();
Status RegisterProductionMetricSchema(MetricRegistry* registry);

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_PRODUCTION_METRIC_SCHEMA_H_
