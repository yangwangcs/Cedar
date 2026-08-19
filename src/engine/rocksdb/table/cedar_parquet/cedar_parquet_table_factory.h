// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "rocksdb/table.h"
#include "table/cedar_parquet/cedar_parquet_format.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

class CedarParquetFactTableFactory final : public TableFactory {
 public:
  explicit CedarParquetFactTableFactory(CedarParquetTableOptions options);

  const char* Name() const override { return "CedarParquetFactTable"; }
  using TableFactory::NewTableReader;
  Status NewTableReader(const ReadOptions& read_options,
                        const TableReaderOptions& table_reader_options,
                        std::unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>* table_reader,
                        bool prefetch_index_and_filter_in_cache = true) const override;
  TableBuilder* NewTableBuilder(const TableBuilderOptions& table_builder_options,
                                WritableFileWriter* file) const override;
  Status ValidateOptions(const DBOptions& db_options,
                         const ColumnFamilyOptions& cf_options) const override;
  std::string GetPrintableOptions() const override;
  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<CedarParquetFactTableFactory>(*this);
  }

 private:
  CedarParquetTableOptions options_;
};

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet

namespace ROCKSDB_NAMESPACE {

TableFactory* NewCedarParquetFactTableFactory(
    const cedar_parquet::CedarParquetTableOptions& table_options);

}  // namespace ROCKSDB_NAMESPACE
