#ifndef CEDAR_QUERY_PROJECTION_PAGE_CACHE_H_
#define CEDAR_QUERY_PROJECTION_PAGE_CACHE_H_

#include <cstdint>
#include <memory>
#include <string>

#include "query/projection/projection_format.h"

namespace cedar::internal {

class QueryReservation;

class ProjectionPageCache {
 public:
  explicit ProjectionPageCache(uint64_t capacity);
  ~ProjectionPageCache();
  StatusOr<std::shared_ptr<const ProjectionChain>> Lookup(
      const std::string& key, QueryReservation& query,
      std::shared_ptr<const void> generation = {}) const;
  // Moves the decoded payload only when the resident cache accepts it.
  bool Admit(const std::string& key, ProjectionChain* page);
  uint64_t resident_bytes() const;
  uint64_t hits() const;
  uint64_t misses() const;

 private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace cedar::internal

#endif
