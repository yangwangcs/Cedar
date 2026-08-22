#ifndef CEDAR_QUERY_PROJECTION_MANIFEST_H_
#define CEDAR_QUERY_PROJECTION_MANIFEST_H_

#include <optional>
#include <string>
#include <vector>

#include "query/projection/projection_format.h"

namespace cedar::internal {

// A segment is referenced by an immutable manifest.  The file name is always
// relative to the projection directory; accepting absolute paths would let a
// corrupt manifest escape the database directory.
struct SegmentDescriptor {
  std::string segment_id;
  std::string filename;
  ProjectionHeader header;
  uint64_t file_bytes = 0;
  uint32_t checksum = 0;
  bool operator==(const SegmentDescriptor&) const = default;
};

struct CoverageRegion {
  ProjectionKind kind = ProjectionKind::kState;
  PartId part_id;
  std::optional<PropertyId> property_id;
  uint32_t schema_epoch = 0;
  uint64_t entity_min = 0;
  uint64_t entity_max_exclusive = 0;
  ValidTimeInterval valid_time;
  std::vector<SegmentDescriptor> segments;
  bool operator==(const CoverageRegion&) const = default;
};

struct StatisticsReference {
  std::string filename;
  uint64_t generation_id = 0;
  CommitSeq base_seq;
  uint32_t checksum = 0;
  bool complete = false;
  bool operator==(const StatisticsReference&) const = default;
};

struct ProjectionManifest {
  std::string database_identity;
  uint64_t generation_id = 0;
  CommitSeq base_seq;
  std::vector<std::string> schema_fingerprints;
  std::vector<CoverageRegion> regions;
  std::optional<StatisticsReference> statistics;
  bool operator==(const ProjectionManifest&) const = default;
};

Status ValidateProjectionManifest(const ProjectionManifest&, const std::string&);
StatusOr<std::string> EncodeProjectionManifest(const ProjectionManifest&);
StatusOr<ProjectionManifest> DecodeProjectionManifest(const std::string&,
                                                       const std::string&);

}  // namespace cedar::internal

#endif
