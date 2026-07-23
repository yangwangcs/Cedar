// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BLOB_BLOB_GC_H_
#define CEDAR_BLOB_BLOB_GC_H_

#include "cedar/blob/blob_reference_catalog.h"
#include "cedar/blob/blob_store.h"

namespace cedar {

// Relocation phase of Blob GC. Segment retirement remains separate because it
// must wait for Manifest and reader-epoch ownership before unlinking files.
class BlobGarbageCollector {
 public:
  explicit BlobGarbageCollector(BlobStore* store) : store_(store) {}
  Status RelocateLive(const BlobReferenceCatalog& catalog);
  StatusOr<std::vector<BlobSegmentId>> PrepareCollection(
      const BlobReferenceCatalog& catalog);
  Status FinishCollection(const std::vector<BlobSegmentId>& retired_segments);
  Status Collect(const BlobReferenceCatalog& catalog);

 private:
  BlobStore* store_;
};

}  // namespace cedar

#endif  // CEDAR_BLOB_BLOB_GC_H_
