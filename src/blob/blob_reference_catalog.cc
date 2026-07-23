// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/blob/blob_reference_catalog.h"

#include <algorithm>
#include <iterator>

#include "cedar/storage/version_set.h"

namespace cedar {

void BlobReferenceCatalog::ReplaceSource(
    std::string source, const std::vector<BlobHash>& references) {
  std::unordered_set<BlobHash, BlobHashHasher> unique(references.begin(), references.end());
  std::lock_guard<std::mutex> lock(mutex_);
  sources_[std::move(source)] = std::move(unique);
}

void BlobReferenceCatalog::ReplaceVersionSnapshot(
    std::string source, const VersionSnapshot& snapshot) {
  std::vector<BlobHash> references;
  for (const SstFileMeta& file : snapshot.files) {
    references.insert(references.end(), file.blob_refs.begin(), file.blob_refs.end());
  }
  ReplaceSource(std::move(source), references);
}

void BlobReferenceCatalog::RebuildLiveSstSources(const VersionSnapshot& snapshot) {
  std::unordered_map<std::string, std::unordered_set<BlobHash, BlobHashHasher>> rebuilt;
  for (const SstFileMeta& file : snapshot.files) {
    rebuilt.emplace("live-sst:" + std::to_string(file.file_number),
                    std::unordered_set<BlobHash, BlobHashHasher>(
                        file.blob_refs.begin(), file.blob_refs.end()));
  }
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto it = sources_.begin(); it != sources_.end();) {
    if (it->first.compare(0, 9, "live-sst:") == 0) {
      it = sources_.erase(it);
    } else {
      ++it;
    }
  }
  sources_.insert(std::make_move_iterator(rebuilt.begin()),
                  std::make_move_iterator(rebuilt.end()));
}

void BlobReferenceCatalog::RemoveSource(const std::string& source) {
  std::lock_guard<std::mutex> lock(mutex_);
  sources_.erase(source);
}

bool BlobReferenceCatalog::IsLive(const BlobHash& hash) const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& source : sources_) {
    if (source.second.count(hash) != 0) return true;
  }
  return false;
}

std::vector<BlobHash> BlobReferenceCatalog::LiveHashes() const {
  std::unordered_set<BlobHash, BlobHashHasher> unique;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& source : sources_) {
      unique.insert(source.second.begin(), source.second.end());
    }
  }
  std::vector<BlobHash> hashes(unique.begin(), unique.end());
  std::sort(hashes.begin(), hashes.end(), [](const BlobHash& left, const BlobHash& right) {
    return left.bytes < right.bytes;
  });
  return hashes;
}

}  // namespace cedar
