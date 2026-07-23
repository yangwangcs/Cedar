#ifndef CEDAR_INDEX_HYBRID_SCAN_H_
#define CEDAR_INDEX_HYBRID_SCAN_H_
#include <cstdint>
#include <optional>
#include <vector>
#include "cedar/index/index_sidecar.h"
namespace cedar {
struct IndexCoverage { uint64_t source_sst_id; bool complete; std::optional<IndexSidecar> sidecar; };
StatusOr<std::vector<uint64_t>> SelectHybridIndexCandidates(
    const std::vector<TemporalEvent>& events, const IndexCoverage& coverage, const Value& value);
}  // namespace cedar
#endif
