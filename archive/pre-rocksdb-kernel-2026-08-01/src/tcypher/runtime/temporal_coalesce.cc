// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/temporal_coalesce.h"

namespace cedar {
namespace {

bool SameBlobReference(const BlobRef& left, const BlobRef& right,
                       bool provenance_demanded) {
  if (left.content_hash != right.content_hash || left.raw_length != right.raw_length) {
    return false;
  }
  return !provenance_demanded ||
      (left.hint.shard_id == right.hint.shard_id &&
       left.hint.segment_id == right.hint.segment_id && left.hint.offset == right.hint.offset);
}

bool SameDemandedFact(const TemporalEvent& left, const TemporalEvent& right,
                      bool provenance_demanded) {
  if (left.logical_key() != right.logical_key() || left.operation() != right.operation() ||
      left.is_blob_reference() != right.is_blob_reference()) {
    return false;
  }
  if (provenance_demanded &&
      (left.commit_seq() != right.commit_seq() || left.schema_epoch() != right.schema_epoch())) {
    return false;
  }
  if (left.is_blob_reference()) {
    return SameBlobReference(*left.blob_ref(), *right.blob_ref(), provenance_demanded);
  }
  return left.value() == right.value();
}

bool SameFactVector(const AlignedTemporalInterval& left,
                    const AlignedTemporalInterval& right, bool provenance_demanded) {
  if (left.facts.size() != right.facts.size()) return false;
  for (size_t index = 0; index < left.facts.size(); ++index) {
    if (!left.facts[index] || !right.facts[index] ||
        !SameDemandedFact(*left.facts[index], *right.facts[index], provenance_demanded)) {
      return false;
    }
  }
  return true;
}

}  // namespace

StatusOr<std::vector<AlignedTemporalInterval>> CoalesceTemporalIntervals(
    const std::vector<AlignedTemporalInterval>& intervals, bool provenance_demanded) {
  std::vector<AlignedTemporalInterval> result;
  for (const AlignedTemporalInterval& interval : intervals) {
    if (interval.valid_from >= interval.valid_to) {
      return Status::InvalidArgument("temporal coalesce", "empty interval");
    }
    if (result.empty()) {
      result.push_back(interval);
      continue;
    }
    AlignedTemporalInterval& previous = result.back();
    if (interval.valid_from < previous.valid_to) {
      return Status::Corruption("temporal coalesce", "overlapping aligned intervals");
    }
    if (interval.valid_from == previous.valid_to &&
        SameFactVector(previous, interval, provenance_demanded)) {
      previous.valid_to = interval.valid_to;
    } else {
      result.push_back(interval);
    }
  }
  return result;
}

}  // namespace cedar
