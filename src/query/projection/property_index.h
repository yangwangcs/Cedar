#ifndef CEDAR_QUERY_PROJECTION_PROPERTY_INDEX_H_
#define CEDAR_QUERY_PROJECTION_PROPERTY_INDEX_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/query/types.h"
#include "query/read/read_catalog.h"

namespace cedar::internal {

// A compact, Cedar-owned posting segment. It is deliberately independent of
// RocksDB handles and can be published through QueryProjectionStore's existing
// generation/manifest protocol.
struct PropertyIndexSegment {
  uint64_t generation_id = 0;
  CommitSeq base_seq;
  CommitSeq built_through;
  PropertyId property;
  PartId part_id;
  uint32_t schema_epoch = 0;
  std::vector<PropertyIndexPosting> postings;
};

Status ValidatePropertyIndexSegment(const PropertyIndexSegment& segment);
StatusOr<std::string> EncodePropertyIndexSegment(
    const PropertyIndexSegment& segment);
StatusOr<PropertyIndexSegment> DecodePropertyIndexSegment(
    const std::string& bytes,
    size_t allocation_limit = 64ULL * 1024ULL * 1024ULL);

}  // namespace cedar::internal

#endif
