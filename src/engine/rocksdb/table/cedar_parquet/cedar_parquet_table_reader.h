// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <memory>
#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "file/random_access_file_reader.h"
#include "rocksdb/table_properties.h"
#include "table/cedar_parquet/cedar_parquet_format.h"
#include "table/cedar_parquet/parquet_metadata.h"
#include "table/cedar_parquet/parquet_bloom_filter.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

class CedarParquetTableIterator;
class CedarParquetProjectedCursor;

class CedarParquetTableReader final : public TableReader {
 public:
  static Status Open(const InternalKeyComparator& internal_comparator,
                     std::unique_ptr<RandomAccessFileReader>&& file,
                     uint64_t file_size, CedarParquetTableOptions options,
                     std::unique_ptr<CedarParquetTableReader>* reader);

  InternalIterator* NewIterator(const ReadOptions& read_options,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;
  Status Get(const ReadOptions& read_options, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;
  void MultiGet(const ReadOptions& read_options,
                const MultiGetContext::Range* mget_range,
                const SliceTransform* prefix_extractor,
                bool skip_filters = false) override;
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options, const Slice& key,
                               TableReaderCaller caller) override;
  uint64_t ApproximateSize(const ReadOptions& read_options, const Slice& start,
                           const Slice& end, TableReaderCaller caller) override;
  void SetupForCompaction() override {}
  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }
  size_t ApproximateMemoryUsage() const override {
    return sizeof(*this) + footer_bytes_ + bloom_bytes_;
  }
  Status VerifyChecksum(const ReadOptions& read_options, TableReaderCaller caller,
                        bool meta_blocks_only = false) override;

  // Streams bounded, typed Cedar-owned vectors from the pinned immutable table.
  // The canonical internal key is always present; encoded values are decoded
  // only when kEncodedValue is requested.
  Status ScanProjected(const CedarParquetScanSpec& spec,
                       const CedarParquetColumnarBatchVisitor& visitor) const;

  // Creates a pull cursor that retains one projected page at a time. This is
  // the table-side source used by the SuperVersion columnar merge; callers
  // consume rows in exact internal-comparator order without opening canonical
  // value pages unless kEncodedValue is projected.
  Status NewProjectedCursor(const CedarParquetScanSpec& spec,
                            std::unique_ptr<CedarParquetProjectedCursor>* cursor) const;

  void ResetPageDecodeCountForTesting() const { page_decode_count_ = 0; }
  uint64_t PageDecodeCountForTesting() const { return page_decode_count_.load(); }

 private:
  friend class CedarParquetTableIterator;
  friend class CedarParquetProjectedCursor;
  struct DecodedRowGroup {
    std::vector<std::string> sort_keys;
    std::vector<std::string> internal_keys;
    std::vector<std::string> values;
  };

  struct DecodedCanonicalPage {
    std::vector<std::string> sort_keys;
    std::vector<std::string> internal_keys;
    std::vector<std::string> values;
  };

  CedarParquetTableReader(const InternalKeyComparator& internal_comparator,
                          std::unique_ptr<RandomAccessFileReader>&& file,
                          uint64_t file_size, CedarParquetTableOptions options,
                          CedarParquetFooter footer,
                          std::shared_ptr<TableProperties> table_properties,
                          size_t footer_bytes,
                          std::vector<std::optional<CedarSplitBlockBloomFilter>> blooms);

  Status DecodeRowGroup(size_t row_group_index, DecodedRowGroup* decoded) const;
  Status DecodeCanonicalPage(size_t row_group_index, size_t page_index,
                             DecodedCanonicalPage* decoded) const;
  Status LoadCanonicalPage(
      size_t row_group_index, size_t page_index,
      std::shared_ptr<const DecodedCanonicalPage>* decoded) const;
  Status DecodeColumnPage(size_t row_group_index, size_t column_index,
                          size_t page_index,
                          std::vector<std::string>* decoded) const;
  Status DecodePrimitiveColumnPage(
      size_t row_group_index, size_t column_index, size_t page_index,
      std::vector<std::optional<std::string>>* decoded) const;
  Status FindRowGroup(const std::string& sort_key,
                      size_t* row_group_index) const;

  const InternalKeyComparator* internal_comparator_;
  std::unique_ptr<RandomAccessFileReader> file_;
  uint64_t file_size_;
  CedarParquetTableOptions options_;
  CedarParquetFooter footer_;
  std::shared_ptr<TableProperties> table_properties_;
  size_t footer_bytes_;
  std::vector<std::optional<CedarSplitBlockBloomFilter>> user_key_blooms_;
  size_t bloom_bytes_ = 0;
  mutable std::atomic<uint64_t> page_decode_count_{0};
  struct CachedCanonicalPage {
    size_t row_group_index = 0;
    size_t page_index = 0;
    size_t bytes = 0;
    std::shared_ptr<const DecodedCanonicalPage> decoded;
  };
  mutable std::mutex page_cache_mutex_;
  mutable std::optional<CachedCanonicalPage> page_cache_;
};

class CedarParquetProjectedCursor final {
 public:
  CedarParquetProjectedCursor(const CedarParquetProjectedCursor&) = delete;
  CedarParquetProjectedCursor& operator=(const CedarParquetProjectedCursor&) = delete;

  bool Valid() const;
  void Next();
  Slice internal_key() const;
  const CedarParquetColumnarBatch& batch() const { return batch_; }
  size_t row_index() const { return row_index_; }
  Status status() const { return status_; }

 private:
  friend class CedarParquetTableReader;
  CedarParquetProjectedCursor(const CedarParquetTableReader* reader,
                              CedarParquetScanSpec spec,
                              bool request_encoded_values,
                              std::vector<CedarParquetColumnVector> columns);

  Status LoadNextPage();

  const CedarParquetTableReader* reader_;
  CedarParquetScanSpec spec_;
  bool request_encoded_values_ = false;
  std::vector<CedarParquetColumnVector> columns_;
  CedarParquetColumnarBatch batch_;
  size_t row_group_index_ = 0;
  size_t page_index_ = 0;
  size_t row_index_ = 0;
  Status status_;
};

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
