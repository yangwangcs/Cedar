#ifndef CEDAR_INDEX_INDEX_SIDECAR_H_
#define CEDAR_INDEX_INDEX_SIDECAR_H_
#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "cedar/core/status.h"
#include "cedar/columnar/sst.h"
#include "cedar/index/canonical_value.h"
#include "cedar/index/index_definition.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/runtime/work_cancellation.h"
#include "cedar/storage/temporal_event.h"
namespace cedar {
struct IndexPosting { IndexCanonicalValue value; uint64_t source_row_ordinal; uint64_t valid_from; uint64_t commit_seq; };
struct IndexSidecar { uint64_t source_sst_id; std::vector<IndexPosting> postings; };
enum class IndexSidecarPublicationFaultPoint : uint8_t {
  kAfterFileFsync,
  kAfterRename,
  kAfterDirectoryFsync,
};
StatusOr<std::string> BuildIndexSidecar(const IndexDefinition&, uint64_t, std::vector<IndexPosting>);
StatusOr<IndexSidecar> ReadIndexSidecar(const std::string&, const IndexDefinition&, uint64_t);
// Publish the immutable file before attaching its fragment through IndexCatalog.
Status WriteIndexSidecarFile(const std::string& path, const IndexDefinition& definition,
                             uint64_t source_sst_id,
                             const std::vector<IndexPosting>& postings,
                             std::function<Status(
                                 IndexSidecarPublicationFaultPoint)>
                                 fault_injector = {});
StatusOr<IndexSidecar> ReadIndexSidecarFile(const std::string& path,
                                            const IndexDefinition& definition,
                                            uint64_t source_sst_id);
StatusOr<IndexSidecar> ReadVerifiedIndexSidecarFile(
    const std::string& path, const IndexDefinition& definition,
    uint64_t source_sst_id,
    const std::array<uint8_t, 32>& expected_identity,
    uint64_t max_bytes = 1ULL << 30);
StatusOr<uint64_t> EstimateIndexSidecarEncodedBytes(
    const IndexDefinition& definition,
    const SstFileStatistics& source_statistics,
    uint64_t source_file_bytes, const ColumnSchema& schema);
StatusOr<IndexSidecar> BuildIndexCandidateSidecar(uint64_t source_sst_id,
                                                   const IndexDefinition& definition,
                                                   const std::vector<TemporalEvent>& source_events,
                                                   std::shared_ptr<WorkCancellation> cancellation = nullptr);
StatusOr<std::vector<IndexPosting>> LookupIndexEquality(const IndexSidecar& sidecar,
                                                         const Value& value);
StatusOr<std::vector<IndexPosting>> LookupIndexRange(
    const IndexSidecar& sidecar, const std::optional<Value>& lower,
    bool lower_inclusive, const std::optional<Value>& upper, bool upper_inclusive);
StatusOr<std::vector<IndexPosting>> LookupIndexPrefix(const IndexSidecar& sidecar,
                                                       const Value& prefix);
}  // namespace cedar
#endif
