#ifndef CEDAR_QUERY_PROJECTION_PROPERTY_INDEX_H_
#define CEDAR_QUERY_PROJECTION_PROPERTY_INDEX_H_

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/query/types.h"
#include "query/read/read_catalog.h"
#include "storage/facts/read_memory_scope.h"

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

  // CPI1 keeps postings in one logical segment but records fixed-size page
  // boundaries so readers can discard whole ranges before touching rows.
  // `first_posting`/`row_count` address the decoded posting vector; on disk
  // they describe the same contiguous page payload.
  struct PageDirectoryEntry {
    uint32_t first_posting = 0;
    uint32_t row_count = 0;
    Value min_value = Value::Int64(0);
    Value max_value = Value::Int64(0);
    uint64_t entity_min = 0;
    uint64_t entity_max_exclusive = 0;
    ValidTime valid_from_min;
    std::optional<ValidTime> valid_to_max;
    uint64_t bloom_mask = 0;
    uint32_t payload_crc32c = 0;
    bool operator==(const PageDirectoryEntry&) const = default;
  };
  std::vector<PageDirectoryEntry> pages;
};

struct PropertyIndexReadResult {
  PropertyIndexReadResult() = default;
  PropertyIndexReadResult(const PropertyIndexReadResult&) = delete;
  PropertyIndexReadResult& operator=(const PropertyIndexReadResult&) = delete;
  PropertyIndexReadResult(PropertyIndexReadResult&&) noexcept = default;
  PropertyIndexReadResult& operator=(PropertyIndexReadResult&& other) noexcept {
    PropertyIndexReadResult previous(std::move(other));
    memory.swap(previous.memory);
    std::swap(segment, previous.segment);
    return *this;
  }
  std::shared_ptr<const void> memory;
  PropertyIndexSegment segment;
};

// Rebuilds deterministic page bounds after a segment is assembled from one
// or more immutable files. This does not mutate canonical facts.
Status BuildPropertyIndexPages(PropertyIndexSegment* segment,
    FactReadMemoryScope* output = nullptr, FactReadMemory* scratch_memory = nullptr);

// Returns only postings in the typed predicate range. Postings must be sorted
// by the schema value order; the seek uses a logarithmic lower bound and then
// visits the matching run.
std::vector<PropertyIndexPosting> SeekPropertyIndexRange(
    const PropertyIndexSegment& segment, PropertyIndexOperator op,
    const Value& lower, const std::optional<Value>& upper = std::nullopt);

Status VisitPropertyIndexRange(
    const PropertyIndexSegment& segment, PropertyIndexOperator op,
    const Value& lower,
    const std::function<Status(const PropertyIndexPosting&)>& visitor,
    const std::optional<Value>& upper = std::nullopt);

Status ValidatePropertyIndexSegment(const PropertyIndexSegment& segment,
                                   FactReadMemory* scratch_memory = nullptr);
StatusOr<std::string> EncodePropertyIndexSegment(
    const PropertyIndexSegment& segment);
StatusOr<PropertyIndexSegment> DecodePropertyIndexSegment(
    const std::string& bytes,
    size_t allocation_limit = 64ULL * 1024ULL * 1024ULL);
StatusOr<PropertyIndexReadResult> DecodeBudgetedPropertyIndexSegment(
    const std::string& bytes, std::shared_ptr<FactReadMemory> memory);

}  // namespace cedar::internal

#endif
