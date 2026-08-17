// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/blob/blob_gc.h"

namespace cedar {

Status BlobGarbageCollector::RelocateLive(const BlobReferenceCatalog& catalog) {
  if (store_ == nullptr) return Status::InvalidArgument("blob gc", "missing BlobStore");
  return store_->RelocateLiveHashes(catalog.LiveHashes());
}

StatusOr<std::vector<BlobSegmentId>> BlobGarbageCollector::PrepareCollection(
    const BlobReferenceCatalog& catalog) {
  if (store_ == nullptr) return Status::InvalidArgument("blob gc", "missing BlobStore");
  Status status = store_->RelocateLiveHashes(catalog.LiveHashes());
  if (!status.ok()) return status;
  return store_->RetireUnreferencedSealedSegments(catalog.LiveHashes());
}

Status BlobGarbageCollector::FinishCollection(
    const std::vector<BlobSegmentId>& retired_segments) {
  if (store_ == nullptr) return Status::InvalidArgument("blob gc", "missing BlobStore");
  return store_->DeleteRetiredSegments(retired_segments);
}

Status BlobGarbageCollector::Collect(const BlobReferenceCatalog& catalog) {
  const auto retired = PrepareCollection(catalog);
  if (!retired.ok()) return retired.status();
  return FinishCollection(retired.ValueOrDie());
}

}  // namespace cedar
