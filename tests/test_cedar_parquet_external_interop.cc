// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <parquet/column_reader.h>
#include <parquet/file_reader.h>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "table/cedar_parquet/cedar_parquet_table_builder.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

void StoreBigEndian64(std::string* destination, size_t offset, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    (*destination)[offset++] = static_cast<char>(value >> shift);
  }
}

std::string V2UserKey(uint64_t entity_id) {
  std::string key(32, '\0');
  key[0] = 2;
  key[5] = 1;
  StoreBigEndian64(&key, 8, entity_id);
  return key;
}

std::string InternalKeyFor(uint64_t entity_id, SequenceNumber sequence) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(V2UserKey(entity_id), sequence, kTypeValue));
  return internal_key;
}

void AppendBigEndian32(std::string* destination, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    destination->push_back(static_cast<char>(value >> shift));
  }
}

std::string StateFactValue() {
  std::string value;
  value.push_back(1);
  value.push_back(1);
  AppendBigEndian32(&value, 0);
  value.push_back(0);
  AppendBigEndian32(&value, 0);
  AppendBigEndian32(&value, crc32c::Value(value.data(), value.size()));
  return value;
}

std::string TestPath() {
  static uint64_t next_file = 0;
  return "/tmp/CedarParquetExternalInterop-" + std::to_string(next_file++) + ".sst";
}

TableBuilderOptions BuilderOptions(const ImmutableOptions& immutable_options,
                                   const MutableCFOptions& mutable_options,
                                   const InternalKeyComparator& comparator) {
  static const ReadOptions read_options;
  static const WriteOptions write_options;
  static const CompressionOptions compression_options;
  static const std::string column_family_name = "facts";
  return TableBuilderOptions(immutable_options, mutable_options, read_options,
                             write_options, comparator, nullptr, kNoCompression,
                             compression_options, 7, column_family_name, 0,
                             0);
}

parquet::Compression::type ApacheCompressionCodec(
    CedarParquetCompressionCodec codec) {
  switch (codec) {
    case CedarParquetCompressionCodec::kUncompressed:
      return parquet::Compression::UNCOMPRESSED;
    case CedarParquetCompressionCodec::kLz4Raw:
      return parquet::Compression::LZ4;
    case CedarParquetCompressionCodec::kZstd:
      return parquet::Compression::ZSTD;
  }
  return parquet::Compression::UNCOMPRESSED;
}

void VerifyApacheParquetReadsCanonicalPages(
    CedarParquetCompressionCodec page_compression) {
  const std::string path = TestPath();
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableOptions parquet_options;
  parquet_options.row_group_max_rows = 2;
  parquet_options.row_group_max_bytes = 1024;
  parquet_options.max_row_bytes = 512;
  parquet_options.page_compression = page_compression;
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   parquet_options, file.get());
  const std::vector<std::string> keys = {InternalKeyFor(8, 2),
                                         InternalKeyFor(9, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  for (size_t index = 0; index < keys.size(); ++index) {
    builder.Add(keys[index], values[index]);
    ASSERT_TRUE(builder.status().ok()) << builder.status().ToString();
  }
  ASSERT_TRUE(builder.Finish().ok()) << builder.status().ToString();
  ASSERT_TRUE(file->Close(IOOptions()).ok());

  std::unique_ptr<parquet::ParquetFileReader> reader;
  ASSERT_NO_THROW(reader = parquet::ParquetFileReader::OpenFile(path, false));
  ASSERT_NE(reader, nullptr);
  const std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
  ASSERT_NE(metadata, nullptr);
  EXPECT_EQ(metadata->num_rows(), 2);
  EXPECT_EQ(metadata->num_row_groups(), 1);
  ASSERT_NE(metadata->schema(), nullptr);
  EXPECT_EQ(metadata->schema()->num_columns(), 25);
  EXPECT_EQ(metadata->schema()->Column(0)->name(), "sort_key");
  EXPECT_EQ(metadata->schema()->Column(0)->physical_type(),
            parquet::Type::FIXED_LEN_BYTE_ARRAY);
  EXPECT_EQ(metadata->schema()->Column(1)->name(), "internal_key");
  EXPECT_EQ(metadata->schema()->Column(1)->physical_type(), parquet::Type::BYTE_ARRAY);
  EXPECT_EQ(metadata->RowGroup(0)->ColumnChunk(1)->compression(),
            ApacheCompressionCodec(page_compression));

  std::shared_ptr<parquet::ColumnReader> column = reader->RowGroup(0)->Column(1);
  auto* keys_reader = static_cast<parquet::ByteArrayReader*>(column.get());
  std::vector<parquet::ByteArray> decoded(keys.size());
  int64_t values_read = 0;
  ASSERT_EQ(keys_reader->ReadBatch(static_cast<int64_t>(decoded.size()), nullptr,
                                   nullptr, decoded.data(), &values_read),
            static_cast<int64_t>(decoded.size()));
  ASSERT_EQ(values_read, static_cast<int64_t>(decoded.size()));
  for (size_t index = 0; index < decoded.size(); ++index) {
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(decoded[index].ptr),
                          decoded[index].len),
              keys[index]);
  }
}

TEST(CedarParquetExternalInteropTest,
     ApacheParquetReaderReadsUncompressedCanonicalPages) {
  VerifyApacheParquetReadsCanonicalPages(
      CedarParquetCompressionCodec::kUncompressed);
}

TEST(CedarParquetExternalInteropTest,
     ApacheParquetReaderReadsLz4RawCanonicalPages) {
  VerifyApacheParquetReadsCanonicalPages(
      CedarParquetCompressionCodec::kLz4Raw);
}

TEST(CedarParquetExternalInteropTest,
     ApacheParquetReaderReadsZstdCanonicalPages) {
  VerifyApacheParquetReadsCanonicalPages(CedarParquetCompressionCodec::kZstd);
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
