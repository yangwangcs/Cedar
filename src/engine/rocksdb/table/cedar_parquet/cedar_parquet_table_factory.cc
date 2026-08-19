// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/cedar_parquet_table_factory.h"

#include <utility>

#include "file/random_access_file_reader.h"
#include "rocksdb/options.h"
#include "table/cedar_parquet/cedar_parquet_table_builder.h"
#include "table/cedar_parquet/cedar_parquet_table_reader.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

constexpr char kParquetMagic[] = "PAR1";

const char* CompressionCodecName(CedarParquetCompressionCodec codec) {
  switch (codec) {
    case CedarParquetCompressionCodec::kUncompressed: return "UNCOMPRESSED";
    case CedarParquetCompressionCodec::kLz4Raw: return "LZ4_RAW";
    case CedarParquetCompressionCodec::kZstd: return "ZSTD";
  }
  return "UNKNOWN";
}

}  // namespace

CedarParquetFactTableFactory::CedarParquetFactTableFactory(
    CedarParquetTableOptions options)
    : options_(std::move(options)) {}

Status CedarParquetFactTableFactory::NewTableReader(
    const ReadOptions& read_options, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool prefetch_index_and_filter_in_cache) const {
  if (file_size < 4) return Status::Corruption("truncated facts table");
  char header_bytes[4];
  Slice header;
  IOStatus io_status = file->Read(IOOptions(), 0, sizeof(header_bytes), &header,
                                  header_bytes, nullptr);
  if (!io_status.ok()) return Status(io_status);
  if (header.size() != sizeof(header_bytes)) return Status::Corruption("truncated facts table");
  if (header == Slice(kParquetMagic, sizeof(header_bytes))) {
    std::unique_ptr<CedarParquetTableReader> cedar_reader;
    Status status = CedarParquetTableReader::Open(
        table_reader_options.internal_comparator, std::move(file), file_size,
        options_, &cedar_reader);
    if (status.ok()) table_reader->reset(cedar_reader.release());
    return status;
  }
  return Status::Corruption("facts table is not Cedar Parquet v2");
}

TableBuilder* CedarParquetFactTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, WritableFileWriter* file) const {
  CedarParquetTableOptions builder_options = options_;
  if (table_builder_options.is_bottommost &&
      builder_options.page_compression == CedarParquetCompressionCodec::kLz4Raw) {
    builder_options.page_compression = CedarParquetCompressionCodec::kZstd;
  }
  return new CedarParquetTableBuilder(table_builder_options,
                                      std::move(builder_options), file);
}

Status CedarParquetFactTableFactory::ValidateOptions(
    const DBOptions&, const ColumnFamilyOptions& cf_options) const {
  Status status = options_.Validate();
  if (!status.ok()) return status;
  if (cf_options.comparator == nullptr ||
      cf_options.comparator->Name() != options_.comparator_name) {
    return Status::InvalidArgument("Cedar Parquet facts require BytewiseComparator");
  }
  if (cf_options.prefix_extractor != nullptr) {
    return Status::InvalidArgument("Cedar Parquet facts do not support a prefix extractor");
  }
  if (cf_options.compression != kNoCompression) {
    return Status::InvalidArgument("Cedar Parquet facts own page compression");
  }
  if (cf_options.enable_blob_files) {
    return Status::InvalidArgument("Cedar Parquet facts cannot use RocksDB blobs");
  }
  return Status::OK();
}

std::string CedarParquetFactTableFactory::GetPrintableOptions() const {
  return "row_group_max_rows=" + std::to_string(options_.row_group_max_rows) +
         "; row_group_max_bytes=" + std::to_string(options_.row_group_max_bytes) +
         "; max_row_bytes=" + std::to_string(options_.max_row_bytes) +
         "; max_footer_bytes=" + std::to_string(options_.max_footer_bytes) +
         "; max_index_bytes=" + std::to_string(options_.max_index_bytes) +
         "; page_compression=" + CompressionCodecName(options_.page_compression) +
         "; comparator=" + options_.comparator_name;
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet

namespace ROCKSDB_NAMESPACE {

TableFactory* NewCedarParquetFactTableFactory(
    const cedar_parquet::CedarParquetTableOptions& table_options) {
  return new cedar_parquet::CedarParquetFactTableFactory(table_options);
}

}  // namespace ROCKSDB_NAMESPACE
