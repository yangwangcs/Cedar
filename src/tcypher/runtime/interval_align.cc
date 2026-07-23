// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/interval_align.h"

#include <algorithm>

namespace cedar {

StatusOr<std::vector<AlignedTemporalInterval>> AlignTemporalIntervals(
    const std::vector<std::vector<TemporalInterval>>& streams,
    uint64_t range_start, uint64_t range_end) {
  if (streams.empty() || range_start >= range_end) {
    return Status::InvalidArgument("interval align", "empty streams or invalid range");
  }
  std::vector<uint64_t> boundaries;
  for (const auto& stream : streams) {
    if (stream.empty()) return std::vector<AlignedTemporalInterval>{};
    for (const TemporalInterval& interval : stream) {
      if (interval.valid_from >= interval.valid_to) {
        return Status::InvalidArgument("interval align", "empty source interval");
      }
      boundaries.push_back(interval.valid_from);
      if (interval.valid_to != kTemporalInfinity) boundaries.push_back(interval.valid_to);
    }
  }
  std::sort(boundaries.begin(), boundaries.end());
  boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
  std::vector<AlignedTemporalInterval> aligned;
  for (size_t index = 0; index < boundaries.size(); ++index) {
    const uint64_t start = boundaries[index];
    const uint64_t end = index + 1 == boundaries.size() ? kTemporalInfinity : boundaries[index + 1];
    if (start >= range_end || end <= range_start) continue;
    std::vector<std::shared_ptr<const TemporalEvent>> facts;
    facts.reserve(streams.size());
    for (const auto& stream : streams) {
      const TemporalInterval* selected = nullptr;
      for (const TemporalInterval& interval : stream) {
        if (interval.valid_from <= start && interval.valid_to > start) {
          if (selected != nullptr) {
            return Status::Corruption("interval align", "overlapping source intervals");
          }
          selected = &interval;
        }
      }
      if (selected == nullptr) {
        facts.clear();
        break;
      }
      facts.push_back(std::make_shared<TemporalEvent>(selected->event));
    }
    if (!facts.empty()) aligned.push_back(AlignedTemporalInterval{start, end, std::move(facts)});
  }
  return aligned;
}

StatusOr<std::vector<AlignedRawTemporalInterval>> AlignRawTemporalIntervals(
    const std::vector<std::vector<RawTemporalInterval>>& streams,
    uint64_t range_start, uint64_t range_end) {
  if (streams.empty() || range_start >= range_end) {
    return Status::InvalidArgument("interval align", "empty streams or invalid range");
  }
  std::vector<uint64_t> boundaries;
  for (const auto& stream : streams) {
    if (stream.empty()) return std::vector<AlignedRawTemporalInterval>{};
    for (const RawTemporalInterval& interval : stream) {
      if (interval.valid_from >= interval.valid_to) {
        return Status::InvalidArgument("interval align", "empty raw source interval");
      }
      boundaries.push_back(interval.valid_from);
      if (interval.valid_to != kTemporalInfinity) boundaries.push_back(interval.valid_to);
    }
  }
  std::sort(boundaries.begin(), boundaries.end());
  boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
  std::vector<AlignedRawTemporalInterval> aligned;
  for (size_t index = 0; index < boundaries.size(); ++index) {
    const uint64_t start = boundaries[index];
    const uint64_t end = index + 1 == boundaries.size()
        ? kTemporalInfinity : boundaries[index + 1];
    if (start >= range_end || end <= range_start) continue;
    std::vector<RawTemporalFact> facts;
    facts.reserve(streams.size());
    for (const auto& stream : streams) {
      const RawTemporalInterval* selected = nullptr;
      for (const RawTemporalInterval& interval : stream) {
        if (interval.valid_from <= start && interval.valid_to > start) {
          if (selected != nullptr) {
            return Status::Corruption("interval align", "overlapping raw source intervals");
          }
          selected = &interval;
        }
      }
      if (selected == nullptr) {
        facts.clear();
        break;
      }
      facts.push_back(selected->fact);
    }
    if (!facts.empty()) {
      aligned.push_back(AlignedRawTemporalInterval{start, end, std::move(facts)});
    }
  }
  return aligned;
}

StatusOr<std::vector<RawRangeExpandRow>> ExpandRawIntervalHop(
    const std::vector<RawTemporalInterval>& source_intervals,
    const std::vector<RawTemporalInterval>& edge_intervals,
    const std::vector<RawTemporalInterval>& target_intervals,
    uint64_t range_start, uint64_t range_end) {
  if (range_start >= range_end) {
    return Status::InvalidArgument("interval expand", "empty or reversed valid-time range");
  }
  std::vector<RawRangeExpandRow> expanded;
  for (const RawTemporalInterval& source : source_intervals) {
    if (source.valid_from >= source.valid_to ||
        source.fact.entity_type != EntityType::Vertex ||
        source.fact.key_kind != LogicalKeyKind::kExistence ||
        source.fact.operation == TemporalOperation::kDelete) {
      continue;
    }
    for (const RawTemporalInterval& edge : edge_intervals) {
      if (edge.valid_from >= edge.valid_to ||
          (edge.fact.entity_type != EntityType::EdgeOut &&
           edge.fact.entity_type != EntityType::EdgeIn) ||
          edge.fact.key_kind != LogicalKeyKind::kExistence ||
          edge.fact.operation == TemporalOperation::kDelete ||
          edge.fact.entity_id != source.fact.entity_id) {
        continue;
      }
      for (const RawTemporalInterval& target : target_intervals) {
        if (target.valid_from >= target.valid_to ||
            target.fact.entity_type != EntityType::Vertex ||
            target.fact.key_kind != LogicalKeyKind::kExistence ||
            target.fact.operation == TemporalOperation::kDelete ||
            target.fact.entity_id != edge.fact.target_id) {
          continue;
        }
        const uint64_t start = std::max(
            source.valid_from, std::max(edge.valid_from, target.valid_from));
        const uint64_t end = std::min(
            source.valid_to, std::min(edge.valid_to, target.valid_to));
        if (start < end && start < range_end && end > range_start) {
          expanded.push_back(RawRangeExpandRow{
              start, end, source.fact, edge.fact, target.fact});
        }
      }
    }
  }
  return expanded;
}

}  // namespace cedar
