// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_INDEX_INDEX_CATALOG_H_
#define CEDAR_INDEX_INDEX_CATALOG_H_

#include <cstdint>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/index/index_definition.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/storage/version_set.h"

namespace cedar {

struct IndexCatalogSnapshot {
  uint64_t catalog_generation = 0;
  std::vector<IndexDefinition> definitions;
  std::vector<IndexFragment> fragments;
  uint64_t coverage_generation = 0;
  uint64_t statistics_snapshot_id = 0;
};

class IndexCatalog {
 public:
  IndexCatalog(VersionSet* versions, const SchemaRegistry* schemas)
      : versions_(versions), schemas_(schemas) {}

  Status RegisterIndex(IndexDefinition definition, uint64_t* index_id);
  Status SetIndexState(uint64_t index_id, IndexState state);
  // Removes the definition and all fragments in one Manifest generation.
  // Physical files remain owned by any pre-drop VersionSnapshot pins.
  Status DropIndex(uint64_t index_id);
  Status AttachFragment(IndexFragment fragment);
  Status DetachFragment(IndexFragmentKey key);
  IndexCatalogSnapshot Snapshot() const;

 private:
  VersionSet* versions_;
  const SchemaRegistry* schemas_;
};

}  // namespace cedar

#endif  // CEDAR_INDEX_INDEX_CATALOG_H_
