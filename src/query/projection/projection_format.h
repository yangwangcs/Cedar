#ifndef CEDAR_QUERY_PROJECTION_FORMAT_H_
#define CEDAR_QUERY_PROJECTION_FORMAT_H_
#include <optional>
#include <string>
#include <vector>

#include "cedar/fact/fact.h"
#include "cedar/query/types.h"
#include "query/projection/projection_compression.h"
namespace cedar::internal {
enum class ProjectionKind : uint8_t {
  kState = 1,
  kAdjacency = 2,
  kProperty = 3,
  kStatistics = 4
};
struct ProjectionHeader {
  ProjectionKind kind = ProjectionKind::kState;
  uint64_t generation_id = 0;
  CommitSeq base_seq;
  PartId part_id;
  PropertyId property_id;
  uint32_t schema_epoch = 0;
  uint64_t entity_min = 0;
  uint64_t entity_max_exclusive = 0;
  ValidTime valid_from_min;
  std::optional<ValidTime> valid_to_max;
  bool operator==(const ProjectionHeader&) const = default;
};
struct ProjectionPageDirectoryEntry {
  uint64_t offset = 0;
  uint32_t compressed_bytes = 0;
  uint32_t uncompressed_bytes = 0;
  uint32_t row_count = 0;
  uint64_t entity_min = 0;
  uint64_t entity_max_exclusive = 0;
  ValidTime valid_from_min;
  std::optional<ValidTime> valid_to_max;
  std::optional<uint64_t> edge_type_min;
  std::optional<uint64_t> edge_type_max;
  uint32_t payload_crc32c = 0;
  bool operator==(const ProjectionPageDirectoryEntry&) const = default;
};
struct ProjectionInterval {
  ValidTimeInterval effective;
  Value value;
  bool operator==(const ProjectionInterval&) const = default;
};
struct ProjectionBoundary {
  ValidTime time;
  FactOperation operation;
  Value value;
  bool operator==(const ProjectionBoundary&) const = default;
};
struct ProjectionChain {
  ProjectionHeader header;
  std::vector<ProjectionInterval> intervals;
  std::vector<ProjectionBoundary> boundaries;
  std::vector<ProjectionPageDirectoryEntry> page_directory;
  bool operator==(const ProjectionChain&) const = default;
};
StatusOr<std::string> EncodeProjectionPage(const ProjectionChain&,
                                           CompressionCodec);
StatusOr<ProjectionChain> DecodeProjectionPage(const std::string&,
                                               size_t allocation_limit =
                                                   64ULL * 1024ULL * 1024ULL);
}  // namespace cedar::internal
#endif
