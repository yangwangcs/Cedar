// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/io_status.h"
#include "rocksdb/table_properties.h"
#include "table/cedar_parquet/cedar_parquet_format.h"
#include "table/cedar_parquet/parquet_metadata.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

namespace cedar_parquet {

class CedarParquetTableBuilder final : public TableBuilder {
 public:
  CedarParquetTableBuilder(const TableBuilderOptions& table_builder_options,
                           CedarParquetTableOptions parquet_options,
                           WritableFileWriter* file);
  ~CedarParquetTableBuilder() override;

  CedarParquetTableBuilder(const CedarParquetTableBuilder&) = delete;
  CedarParquetTableBuilder& operator=(const CedarParquetTableBuilder&) = delete;

  void Add(const Slice& key, const Slice& value) override;
  Status status() const override { return status_; }
  IOStatus io_status() const override { return io_status_; }
  Status Finish() override;
  void Abandon() override;
  uint64_t NumEntries() const override { return properties_.num_entries; }
  uint64_t PreCompressionSize() const override { return pre_compression_size_; }
  uint64_t FileSize() const override;
  TableProperties GetTableProperties() const override { return properties_; }
  std::string GetFileChecksum() const override;
  const char* GetFileChecksumFuncName() const override;

 private:
  void SetStatus(Status status);
  void Write(const Slice& bytes);
  void WriteOffsetIndexes();
  void WriteColumnIndexes();
  void FlushRowGroup();

  const InternalKeyComparator* internal_comparator_ = nullptr;
  CedarParquetTableOptions parquet_options_;
  WritableFileWriter* file_;
  CedarParquetRowGroupBuilder row_group_builder_;
  CedarParquetFooter footer_;
  TableProperties properties_;
  Status status_;
  IOStatus io_status_;
  std::string last_internal_key_;
  uint64_t pre_compression_size_ = 0;
  bool closed_ = false;
};

}  // namespace cedar_parquet
}  // namespace ROCKSDB_NAMESPACE
