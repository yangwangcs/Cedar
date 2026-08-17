// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/index/hybrid_scan.h"

#include <algorithm>

namespace cedar {
namespace {

StatusOr<std::vector<uint64_t>> BaseCandidates(const std::vector<TemporalEvent>& events,
                                                const Value& predicate_value) {
  const auto wanted = EncodeIndexCanonicalValue(predicate_value);
  if (!wanted.ok()) return wanted.status();
  std::vector<uint64_t> candidates;
  for (uint64_t ordinal = 0; ordinal < events.size(); ++ordinal) {
    const TemporalEvent& event = events[ordinal];
    if (event.operation() != TemporalOperation::kPut ||
        event.logical_key().kind() != LogicalKeyKind::kProperty) {
      continue;
    }
    const auto candidate = EncodeIndexCanonicalValue(event.value());
    if (!candidate.ok()) return candidate.status();
    if (CompareIndexCanonicalValues(candidate.ValueOrDie(), wanted.ValueOrDie()) == 0) {
      candidates.push_back(ordinal);
    }
  }
  return candidates;
}

}  // namespace

StatusOr<std::vector<uint64_t>> SelectHybridIndexCandidates(
    const std::vector<TemporalEvent>& source_events, const IndexCoverage& coverage,
    const Value& predicate_value) {
  if (!coverage.complete || !coverage.sidecar.has_value() ||
      coverage.sidecar->source_sst_id != coverage.source_sst_id) {
    return BaseCandidates(source_events, predicate_value);
  }
  const auto postings = LookupIndexEquality(*coverage.sidecar, predicate_value);
  if (!postings.ok()) return BaseCandidates(source_events, predicate_value);
  std::vector<uint64_t> candidates;
  candidates.reserve(postings.ValueOrDie().size());
  for (const IndexPosting& posting : postings.ValueOrDie()) {
    if (posting.source_row_ordinal >= source_events.size()) {
      return BaseCandidates(source_events, predicate_value);
    }
    candidates.push_back(posting.source_row_ordinal);
  }
  std::sort(candidates.begin(), candidates.end());
  candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());
  return candidates;
}

}  // namespace cedar
