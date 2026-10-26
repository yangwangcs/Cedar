#ifndef CEDAR_QUERY_PROJECTION_PAGE_READER_H_
#define CEDAR_QUERY_PROJECTION_PAGE_READER_H_

#include <string>
#include <vector>

#include "query/projection/projection_store.h"

namespace cedar::internal {

class ProjectionPageCache;
struct ProjectionPageReadCache {
  std::shared_ptr<ProjectionPageCache> pages;
  std::string namespace_key;
  std::shared_ptr<const void> generation;
  bool admit = false;
  ProjectionReadStats* stats = nullptr;
};

struct ProjectionPageSelection {
  std::vector<size_t> page_indexes;
  uint64_t pages_skipped = 0;
};

class ProjectionPageReader {
 public:
  Status VisitSelected(
      const std::string& filename, const ProjectionChain& directory,
      const ProjectionPageSelection& selection, QueryReservation& reservation,
      const ProjectionChainVisitor& visitor,
      const ProjectionPageReadCache* cache = nullptr) const;
  StatusOr<ProjectionChain> ReadDirectory(
      const std::string& filename,
      size_t allocation_limit = 64ULL * 1024ULL * 1024ULL,
      const std::function<bool(size_t)>& reserve = {}) const;
  StatusOr<ProjectionPageSelection> Select(
      const ProjectionChain& directory, const CoverageRequest& request,
      const std::function<bool(size_t)>& reserve = {}) const;
  StatusOr<std::vector<ProjectionChain>> ReadSelected(
      const std::string& filename, const ProjectionChain& directory,
      const ProjectionPageSelection& selection,
      size_t allocation_limit = 64ULL * 1024ULL * 1024ULL) const;
};

}  // namespace cedar::internal

#endif
