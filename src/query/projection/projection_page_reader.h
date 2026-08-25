#ifndef CEDAR_QUERY_PROJECTION_PAGE_READER_H_
#define CEDAR_QUERY_PROJECTION_PAGE_READER_H_

#include <string>
#include <vector>

#include "query/projection/projection_store.h"

namespace cedar::internal {

struct ProjectionPageSelection {
  std::vector<size_t> page_indexes;
  uint64_t pages_skipped = 0;
};

class ProjectionPageReader {
 public:
  StatusOr<ProjectionChain> ReadDirectory(
      const std::string& filename,
      size_t allocation_limit = 64ULL * 1024ULL * 1024ULL) const;
  StatusOr<ProjectionPageSelection> Select(
      const ProjectionChain& directory, const CoverageRequest& request) const;
  StatusOr<std::vector<ProjectionChain>> ReadSelected(
      const std::string& filename, const ProjectionChain& directory,
      const ProjectionPageSelection& selection,
      size_t allocation_limit = 64ULL * 1024ULL * 1024ULL) const;
};

}  // namespace cedar::internal

#endif
