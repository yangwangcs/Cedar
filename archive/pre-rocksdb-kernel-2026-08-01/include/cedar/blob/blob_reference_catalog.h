// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BLOB_BLOB_REFERENCE_CATALOG_H_
#define CEDAR_BLOB_BLOB_REFERENCE_CATALOG_H_

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/blob/blob_store.h"

namespace cedar {

struct VersionSnapshot;

// Runtime protection for references held by committed MemTables, published or
// pinned SSTs, and unpublished flush/compaction outputs. Durable state is
// rebuilt from WAL and Manifest metadata rather than this in-memory catalog.
class BlobReferenceCatalog {
 public:
  void ReplaceSource(std::string source, const std::vector<BlobHash>& references);
  void ReplaceVersionSnapshot(std::string source, const VersionSnapshot& snapshot);
  void RebuildLiveSstSources(const VersionSnapshot& snapshot);
  void RemoveSource(const std::string& source);
  bool IsLive(const BlobHash& hash) const;
  std::vector<BlobHash> LiveHashes() const;

 private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::unordered_set<BlobHash, BlobHashHasher>> sources_;
};

}  // namespace cedar

#endif  // CEDAR_BLOB_BLOB_REFERENCE_CATALOG_H_
