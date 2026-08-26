#ifndef CEDAR_QUERY_PROJECTION_PROPERTY_INDEX_BUILDER_H_
#define CEDAR_QUERY_PROJECTION_PROPERTY_INDEX_BUILDER_H_

#include <vector>

#include "cedar/fact/canonical_reader.h"
#include "cedar/schema.h"
#include "query/projection/projection_store.h"

namespace cedar::internal {

// Builds immutable property-index segments from the canonical snapshot. The
// builder is deliberately read-only with respect to the fact store; callers
// publish the returned build through QueryProjectionStore::Build.
StatusOr<ProjectionBuild> BuildPropertyIndexProjection(
    const CanonicalFactReader& reader,
    const std::vector<PropertyDefinition>& definitions,
    PartScope part_scope, ValidTimeInterval valid_time,
    CommitSeq base_seq, CommitSeq built_through, uint64_t generation_id,
    std::string database_identity);

}  // namespace cedar::internal

#endif
