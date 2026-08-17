// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/observability/metric_exporter.h"

#include <sstream>

namespace cedar {
namespace {

std::string EscapeJson(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (unsigned char character : input) {
    switch (character) {
      case '"': output.append("\\\""); break;
      case '\\': output.append("\\\\"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default: output.push_back(static_cast<char>(character)); break;
    }
  }
  return output;
}

}  // namespace

std::string ExportMetricsJson(const MetricRegistry& registry) {
  const std::vector<MetricPoint> metrics = registry.Snapshot();
  const std::vector<HistogramMetricPoint> histograms = registry.HistogramSnapshot();
  std::ostringstream output;
  output << "{\"metric_schema_version\":1,\"metrics\":[";
  for (size_t index = 0; index < metrics.size(); ++index) {
    if (index != 0) output << ',';
    const MetricPoint& metric = metrics[index];
    output << "{\"name\":\"" << EscapeJson(metric.definition.name)
           << "\",\"type\":\""
           << (metric.definition.type == MetricType::kCounter ? "counter" : "gauge")
           << "\",\"unit\":\"" << EscapeJson(metric.definition.unit)
           << "\",\"schema_version\":" << metric.definition.schema_version
           << ",\"values\":{";
    size_t value_index = 0;
    for (const auto& value : metric.values) {
      if (value_index++ != 0) output << ',';
      output << '"' << EscapeJson(value.first) << "\":" << value.second;
    }
    output << "}}";
  }
  output << "],\"histograms\":[";
  for (size_t index = 0; index < histograms.size(); ++index) {
    if (index != 0) output << ',';
    const HistogramMetricPoint& metric = histograms[index];
    output << "{\"name\":\"" << EscapeJson(metric.definition.name)
           << "\",\"type\":\"histogram\",\"unit\":\""
           << EscapeJson(metric.definition.unit) << "\",\"schema_version\":"
           << metric.definition.schema_version << ",\"bounds\":[";
    for (size_t bound = 0; bound < metric.definition.histogram_bounds.size();
         ++bound) {
      if (bound != 0) output << ',';
      output << metric.definition.histogram_bounds[bound];
    }
    output << "],\"values\":{";
    size_t value_index = 0;
    for (const auto& value : metric.values) {
      if (value_index++ != 0) output << ',';
      const Histogram& histogram = value.second;
      output << '"' << EscapeJson(value.first) << "\":{\"count\":"
             << histogram.count() << ",\"min\":" << histogram.min()
             << ",\"p50\":" << histogram.Quantile(0.50)
             << ",\"p95\":" << histogram.Quantile(0.95)
             << ",\"p99\":" << histogram.Quantile(0.99)
             << ",\"p999\":" << histogram.Quantile(0.999)
             << ",\"max\":" << histogram.max() << ",\"sum\":"
             << histogram.sum() << '}';
    }
    output << "}}";
  }
  output << "]}";
  return output.str();
}

}  // namespace cedar
