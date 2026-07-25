// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_METRIC_REGISTRY_H_
#define CEDAR_OBSERVABILITY_METRIC_REGISTRY_H_

#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/observability/histogram.h"

namespace cedar {

enum class MetricType : uint8_t { kCounter, kGauge, kHistogram };

struct MetricDefinition {
  std::string name;
  MetricType type = MetricType::kCounter;
  std::string unit;
  uint32_t schema_version = 1;
  std::vector<uint64_t> histogram_bounds;
};

struct MetricPoint {
  MetricDefinition definition;
  std::map<std::string, uint64_t> values;
};

struct HistogramMetricPoint {
  MetricDefinition definition;
  std::map<std::string, Histogram> values;
};

enum class MetricDefinitionIssueKind : uint8_t {
  kMissing,
  kSchemaConflict,
};

struct MetricDefinitionIssue {
  std::string name;
  MetricDefinitionIssueKind kind = MetricDefinitionIssueKind::kMissing;
};

struct MetricDefinitionAudit {
  size_t required_count = 0;
  size_t matched_count = 0;
  std::vector<MetricDefinitionIssue> issues;

  bool complete() const {
    return matched_count == required_count && issues.empty();
  }
};

struct MetricActivityRequirement {
  std::string name;
  std::string label;
  uint64_t minimum = 1;
};

enum class MetricActivityIssueKind : uint8_t {
  kMetricMissing,
  kLabelMissing,
  kBelowMinimum,
};

struct MetricActivityIssue {
  std::string name;
  std::string label;
  MetricActivityIssueKind kind = MetricActivityIssueKind::kMetricMissing;
  uint64_t observed = 0;
  uint64_t minimum = 0;
};

struct MetricActivityAudit {
  size_t required_count = 0;
  size_t matched_count = 0;
  std::vector<MetricActivityIssue> issues;

  bool complete() const {
    return matched_count == required_count && issues.empty();
  }
};

// Stable-name metric registry. Labels are already normalized bounded buckets
// at this boundary; arbitrary identifiers and query text are rejected by the
// caller's schema rather than becoming unbounded map keys.
class MetricRegistry {
 public:
  explicit MetricRegistry(size_t max_labels) : max_labels_(max_labels) {}

  Status Register(const MetricDefinition& definition) {
    if (definition.name.empty() || definition.schema_version == 0) {
      return Status::InvalidArgument("metrics", "invalid metric definition");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto existing = definitions_.find(definition.name);
    if (existing != definitions_.end() &&
        (existing->second.type != definition.type || existing->second.unit != definition.unit ||
         existing->second.schema_version != definition.schema_version ||
         existing->second.histogram_bounds != definition.histogram_bounds)) {
      return Status::InvalidArgument("metrics", "metric definition conflicts with registered schema");
    }
    definitions_[definition.name] = definition;
    return Status::OK();
  }

  Status AddCounter(const std::string& name, const std::string& label, uint64_t delta) {
    return Update(name, label, delta, MetricType::kCounter, false);
  }
  Status SetGauge(const std::string& name, const std::string& label, uint64_t value) {
    return Update(name, label, value, MetricType::kGauge, true);
  }
  Status ObserveHistogram(const std::string& name, const std::string& label,
                          uint64_t value) {
    if (name.empty()) return Status::InvalidArgument("metrics", "empty metric name");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto definition = definitions_.find(name);
    if (definition == definitions_.end()) {
      return Status::NotFound("metrics", "metric is not registered");
    }
    if (definition->second.type != MetricType::kHistogram) {
      return Status::InvalidArgument("metrics", "metric update type mismatches schema");
    }
    auto& labels = histograms_[name];
    auto found = labels.find(label);
    if (found == labels.end()) {
      if (labels.size() >= max_labels_) {
        return Status::InvalidArgument("metrics", "label cardinality limit");
      }
      found = labels.emplace(label, Histogram(definition->second.histogram_bounds)).first;
    }
    found->second.Observe(value);
    return Status::OK();
  }

  Status MergeHistogram(const std::string& name, const std::string& label,
                        const Histogram& value) {
    if (name.empty()) {
      return Status::InvalidArgument("metrics", "empty metric name");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto definition = definitions_.find(name);
    if (definition == definitions_.end()) {
      return Status::NotFound("metrics", "metric is not registered");
    }
    if (definition->second.type != MetricType::kHistogram) {
      return Status::InvalidArgument(
          "metrics", "metric update type mismatches schema");
    }
    auto& labels = histograms_[name];
    auto found = labels.find(label);
    if (found == labels.end()) {
      if (labels.size() >= max_labels_) {
        return Status::InvalidArgument("metrics", "label cardinality limit");
      }
      found = labels
                  .emplace(label,
                           Histogram(definition->second.histogram_bounds))
                  .first;
    }
    return found->second.Merge(value);
  }

  std::optional<uint64_t> Counter(const std::string& name, const std::string& label) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto metric = metrics_.find(name);
    if (metric == metrics_.end()) return {};
    const auto value = metric->second.find(label);
    return value == metric->second.end() ? std::optional<uint64_t>{}
                                         : std::optional<uint64_t>{value->second};
  }

  // Include registered-but-empty metric families so an empty window retains its schema.
  std::vector<MetricPoint> Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<MetricPoint> snapshot;
    snapshot.reserve(definitions_.size());
    for (const auto& definition : definitions_) {
      if (definition.second.type == MetricType::kHistogram) continue;
      const auto metric = metrics_.find(definition.first);
      snapshot.push_back(MetricPoint{
          definition.second, metric == metrics_.end() ? std::map<std::string, uint64_t>{}
                                                       : metric->second});
    }
    return snapshot;
  }

  std::vector<HistogramMetricPoint> HistogramSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<HistogramMetricPoint> snapshot;
    snapshot.reserve(definitions_.size());
    for (const auto& definition : definitions_) {
      if (definition.second.type != MetricType::kHistogram) continue;
      const auto metric = histograms_.find(definition.first);
      snapshot.push_back(HistogramMetricPoint{
          definition.second, metric == histograms_.end()
              ? std::map<std::string, Histogram>{} : metric->second});
    }
    return snapshot;
  }

  MetricDefinitionAudit AuditDefinitions(
      const std::vector<MetricDefinition>& required) const {
    std::lock_guard<std::mutex> lock(mutex_);
    MetricDefinitionAudit audit;
    audit.required_count = required.size();
    for (const MetricDefinition& expected : required) {
      const auto existing = definitions_.find(expected.name);
      if (existing == definitions_.end()) {
        audit.issues.push_back(
            MetricDefinitionIssue{expected.name,
                                  MetricDefinitionIssueKind::kMissing});
        continue;
      }
      const MetricDefinition& actual = existing->second;
      if (actual.type != expected.type || actual.unit != expected.unit ||
          actual.schema_version != expected.schema_version ||
          actual.histogram_bounds != expected.histogram_bounds) {
        audit.issues.push_back(MetricDefinitionIssue{
            expected.name, MetricDefinitionIssueKind::kSchemaConflict});
        continue;
      }
      ++audit.matched_count;
    }
    std::sort(audit.issues.begin(), audit.issues.end(),
              [](const MetricDefinitionIssue& left,
                 const MetricDefinitionIssue& right) {
                if (left.name != right.name) return left.name < right.name;
                return left.kind < right.kind;
              });
    return audit;
  }

  MetricActivityAudit AuditActivity(
      const std::vector<MetricActivityRequirement>& required) const {
    std::lock_guard<std::mutex> lock(mutex_);
    MetricActivityAudit audit;
    audit.required_count = required.size();
    for (const MetricActivityRequirement& expected : required) {
      const auto definition = definitions_.find(expected.name);
      if (definition == definitions_.end()) {
        audit.issues.push_back(MetricActivityIssue{
            expected.name, expected.label,
            MetricActivityIssueKind::kMetricMissing, 0, expected.minimum});
        continue;
      }
      uint64_t observed = 0;
      bool has_label = false;
      if (definition->second.type == MetricType::kHistogram) {
        const auto metric = histograms_.find(expected.name);
        if (metric != histograms_.end()) {
          const auto label = metric->second.find(expected.label);
          if (label != metric->second.end()) {
            has_label = true;
            observed = label->second.count();
          }
        }
      } else {
        const auto metric = metrics_.find(expected.name);
        if (metric != metrics_.end()) {
          const auto label = metric->second.find(expected.label);
          if (label != metric->second.end()) {
            has_label = true;
            observed = label->second;
          }
        }
      }
      if (!has_label) {
        audit.issues.push_back(MetricActivityIssue{
            expected.name, expected.label,
            MetricActivityIssueKind::kLabelMissing, 0, expected.minimum});
      } else if (observed < expected.minimum) {
        audit.issues.push_back(MetricActivityIssue{
            expected.name, expected.label,
            MetricActivityIssueKind::kBelowMinimum, observed,
            expected.minimum});
      } else {
        ++audit.matched_count;
      }
    }
    std::sort(audit.issues.begin(), audit.issues.end(),
              [](const MetricActivityIssue& left,
                 const MetricActivityIssue& right) {
                if (left.name != right.name) return left.name < right.name;
                if (left.label != right.label) return left.label < right.label;
                return left.kind < right.kind;
              });
    return audit;
  }

 private:
  Status Update(const std::string& name, const std::string& label, uint64_t value,
                MetricType type, bool replace) {
    if (name.empty()) return Status::InvalidArgument("metrics", "empty metric name");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto definition = definitions_.find(name);
    if (definition == definitions_.end()) {
      return Status::NotFound("metrics", "metric is not registered");
    }
    if (definition->second.type != type) {
      return Status::InvalidArgument("metrics", "metric update type mismatches schema");
    }
    auto& labels = metrics_[name];
    if (labels.count(label) == 0 && labels.size() >= max_labels_) {
      return Status::InvalidArgument("metrics", "label cardinality limit");
    }
    if (replace) {
      labels[label] = value;
    } else if (value > UINT64_MAX - labels[label]) {
      labels[label] = UINT64_MAX;
    } else {
      labels[label] += value;
    }
    return Status::OK();
  }

  size_t max_labels_;
  mutable std::mutex mutex_;
  std::map<std::string, MetricDefinition> definitions_;
  std::map<std::string, std::map<std::string, uint64_t>> metrics_;
  std::map<std::string, std::map<std::string, Histogram>> histograms_;
};

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_METRIC_REGISTRY_H_
