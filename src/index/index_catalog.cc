// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/index/index_catalog.h"

#include <algorithm>
#include <limits>

namespace cedar {
namespace {

bool IsAllowedStateTransition(IndexState from, IndexState to) {
  switch (from) {
    case IndexState::kDeclared:
      return to == IndexState::kBuilding;
    case IndexState::kBuilding:
      return to == IndexState::kActive || to == IndexState::kFailed;
    case IndexState::kActive:
    case IndexState::kFailed:
      return false;
  }
  return false;
}

bool ValidIndexEntityType(EntityType entity_type) {
  return static_cast<uint8_t>(entity_type) <=
      static_cast<uint8_t>(EntityType::EdgeIn);
}

}  // namespace

Status IndexCatalog::RegisterIndex(IndexDefinition definition, uint64_t* index_id) {
  if (versions_ == nullptr || schemas_ == nullptr || index_id == nullptr ||
      !ValidIndexEntityType(definition.entity_type) ||
      definition.schema_epoch == 0 ||
      definition.capabilities == 0 ||
      !IsSupportedIndexCanonicalEncoding(definition.canonical_encoding_id)) {
    return Status::InvalidArgument("index catalog", "invalid index registration");
  }
  if (!schemas_->Lookup(definition.entity_type, definition.column_id,
                        definition.schema_epoch).has_value()) {
    return Status::SchemaMismatch("index catalog", "indexed schema epoch is not registered");
  }
  const auto snapshot = versions_->Snapshot();
  if (snapshot->next_index_id == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted("index catalog",
                                     "index identity space exhausted");
  }
  for (const IndexDefinition& existing : snapshot->index_definitions) {
    if (existing.entity_type == definition.entity_type &&
        existing.column_id == definition.column_id &&
        existing.schema_epoch == definition.schema_epoch) {
      return Status::InvalidArgument("index catalog", "schema column already has an index definition");
    }
  }
  definition.index_id = snapshot->next_index_id;
  definition.state = IndexState::kDeclared;
  definition.generation = snapshot->generation + 1;
  VersionEdit edit;
  edit.expected_generation = snapshot->generation;
  edit.index_adds.push_back(definition);
  const Status applied = versions_->ApplyEdit(edit);
  if (!applied.ok()) return applied;
  *index_id = definition.index_id;
  return Status::OK();
}

Status IndexCatalog::SetIndexState(uint64_t index_id, IndexState state) {
  if (versions_ == nullptr || index_id == 0) {
    return Status::InvalidArgument("index catalog", "invalid index state update");
  }
  const auto snapshot = versions_->Snapshot();
  const auto found = std::find_if(snapshot->index_definitions.begin(),
                                  snapshot->index_definitions.end(),
                                  [index_id](const IndexDefinition& definition) {
                                    return definition.index_id == index_id;
                                  });
  if (found == snapshot->index_definitions.end()) {
    return Status::NotFound("index catalog", "index definition is not present");
  }
  if (!IsAllowedStateTransition(found->state, state)) {
    return Status::InvalidArgument("index catalog", "illegal index state transition");
  }
  IndexDefinition updated = *found;
  updated.state = state;
  updated.generation = snapshot->generation + 1;
  VersionEdit edit;
  edit.index_updates.push_back(updated);
  return versions_->ApplyEdit(edit);
}

Status IndexCatalog::DropIndex(uint64_t index_id) {
  if (versions_ == nullptr || index_id == 0) {
    return Status::InvalidArgument("index catalog", "invalid index drop");
  }
  const auto snapshot = versions_->Snapshot();
  const auto found = std::find_if(snapshot->index_definitions.begin(),
                                  snapshot->index_definitions.end(),
                                  [index_id](const IndexDefinition& definition) {
                                    return definition.index_id == index_id;
                                  });
  if (found == snapshot->index_definitions.end()) {
    return Status::NotFound("index catalog", "index definition is not present");
  }
  VersionEdit edit;
  edit.expected_generation = snapshot->generation;
  edit.index_deletes.push_back(index_id);
  for (const IndexFragment& fragment : snapshot->index_fragments) {
    if (fragment.index_id == index_id) {
      edit.index_fragment_deletes.push_back(
          IndexFragmentKey{fragment.index_id, fragment.source_sst_id});
    }
  }
  return versions_->ApplyEdit(edit);
}

Status IndexCatalog::AttachFragment(IndexFragment fragment) {
  if (versions_ == nullptr || fragment.index_id == 0 || fragment.source_sst_id == 0 ||
      fragment.relative_path.empty() || fragment.format_version == 0) {
    return Status::InvalidArgument("index catalog", "invalid index fragment attachment");
  }
  const auto snapshot = versions_->Snapshot();
  const auto definition = std::find_if(snapshot->index_definitions.begin(),
                                       snapshot->index_definitions.end(),
                                       [&fragment](const IndexDefinition& index) {
                                         return index.index_id == fragment.index_id;
                                       });
  if (definition == snapshot->index_definitions.end()) {
    return Status::NotFound("index catalog", "index definition is not present");
  }
  if (definition->state != IndexState::kBuilding && definition->state != IndexState::kActive) {
    return Status::InvalidArgument("index catalog", "index is not accepting fragments");
  }
  if (!std::any_of(snapshot->files.begin(), snapshot->files.end(), [&fragment](const auto& file) {
        return file.file_number == fragment.source_sst_id;
      })) {
    return Status::NotFound("index catalog", "source SST is not present");
  }
  fragment.catalog_generation = snapshot->generation + 1;
  VersionEdit edit;
  edit.index_fragment_adds.push_back(std::move(fragment));
  return versions_->ApplyEdit(edit);
}

Status IndexCatalog::DetachFragment(IndexFragmentKey key) {
  if (versions_ == nullptr || key.index_id == 0 || key.source_sst_id == 0) {
    return Status::InvalidArgument("index catalog", "invalid index fragment detachment");
  }
  const auto snapshot = versions_->Snapshot();
  if (!std::any_of(snapshot->index_fragments.begin(), snapshot->index_fragments.end(),
                   [&key](const IndexFragment& fragment) {
                     return fragment.index_id == key.index_id &&
                            fragment.source_sst_id == key.source_sst_id;
                   })) {
    return Status::NotFound("index catalog", "index fragment is not present");
  }
  VersionEdit edit;
  edit.index_fragment_deletes.push_back(key);
  return versions_->ApplyEdit(edit);
}

IndexCatalogSnapshot IndexCatalog::Snapshot() const {
  if (versions_ == nullptr) return IndexCatalogSnapshot{};
  const auto snapshot = versions_->Snapshot();
  uint64_t coverage_generation = 0;
  for (const IndexFragment& fragment : snapshot->index_fragments) {
    coverage_generation = std::max(coverage_generation, fragment.catalog_generation);
  }
  return IndexCatalogSnapshot{snapshot->generation, snapshot->index_definitions,
                              snapshot->index_fragments, coverage_generation, 0};
}

}  // namespace cedar
