// Copyright (c) 2024 Cedar Storage Engine Authors.
// WriteBatch with Cedar extensions for temporal data.

#ifndef FERN_WRITE_BATCH_H_
#define FERN_WRITE_BATCH_H_

#include <string>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

class WriteBatch {
 public:
  class Handler;

  WriteBatch();

  // Intentionally copyable.
  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // Cedar-specific: Store with CedarKey and Descriptor
  void Put(const CedarKey& key, const Descriptor& value);

  // Cedar-specific: Store vertex with timestamp
  void PutVertex(uint64_t entity_id, uint16_t col_id,
                 Timestamp timestamp, const Descriptor& value);

  // Cedar-specific: Store edge with timestamp
  void PutEdge(uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
               Timestamp timestamp, const Descriptor& value);

  // If the database contains a mapping for "key", erase it.
  void Delete(const Slice& key);

  // Cedar-specific: Delete with CedarKey
  void Delete(const CedarKey& key);

  // Clear all updates buffered in this batch.
  void Clear();

  // Support for iterating over the contents of a batch.
  Status Iterate(Handler* handler) const;

  // Retrieve the serialized version of this batch.
  std::string Encode() const;

  // Load a serialized version of a batch.
  Status Decode(const Slice& data);

  // Retrieve data size of the batch.
  size_t GetDataSize() const;

  // Returns the number of updates in the batch.
  int Count() const;

  // Returns true if PutVertex/PutEdge are used (Cedar-specific)
  bool HasTemporalOps() const;

  // Approximate size of this batch in bytes.
  size_t ApproximateSize() const;

  // Cedar extension: Get temporal operations only
  std::vector<std::pair<CedarKey, Descriptor>> GetTemporalOps() const;

 private:
  friend class WriteBatchInternal;

  std::string rep_;  // See comment in write_batch.cc for the format
};

// WriteBatch::Handler is the interface for iterating over the contents of
// a batch.
class WriteBatch::Handler {
 public:
  virtual ~Handler();
  virtual void Put(const Slice& key, const Slice& value) = 0;
  virtual void Delete(const Slice& key) = 0;

  // Cedar-specific handlers
  virtual void PutVertex(uint64_t entity_id, uint16_t col_id,
                         Timestamp timestamp, const Descriptor& value) {
    (void)entity_id;
    (void)col_id;
    (void)timestamp;
    (void)value;
  }
  virtual void PutEdge(uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
                       Timestamp timestamp, const Descriptor& value) {
    (void)src_id;
    (void)dst_id;
    (void)edge_type;
    (void)timestamp;
    (void)value;
  }
};

}  // namespace cedar

#endif  // FERN_WRITE_BATCH_H_
