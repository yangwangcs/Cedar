// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "table/cedar_parquet/cedar_parquet_table_builder.h"
#include "table/cedar_parquet/parquet_metadata.h"
#include "table/cedar_parquet/parquet_plain_page.h"
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

std::string InternalKeyFor(uint64_t entity_id, SequenceNumber sequence,
                           ValueType type = kTypeValue) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(V2UserKey(entity_id), sequence, type));
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

std::string TestPath(const char* name) {
  static uint64_t next_file = 0;
  return std::string("/tmp/") + name + "-" + std::to_string(next_file++) + ".sst";
}

TableBuilderOptions BuilderOptions(const ImmutableOptions& immutable_options,
                                   const MutableCFOptions& mutable_options,
                                   const InternalKeyComparator& comparator) {
  static const ReadOptions read_options;
  static const WriteOptions write_options;
  static const CompressionOptions compression_options;
  static const std::string column_family_name = "facts";
  return TableBuilderOptions(immutable_options, mutable_options, read_options,
                             write_options, comparator, nullptr,
                             kNoCompression, compression_options, 7,
                             column_family_name,
                             0, 0);
}

std::string ReadAll(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

class AppendFailingWritableFile final : public FSWritableFileOwnerWrapper {
 public:
  explicit AppendFailingWritableFile(std::unique_ptr<FSWritableFile>&& target)
      : FSWritableFileOwnerWrapper(std::move(target)) {}

  IOStatus Append(const Slice&, const IOOptions&, IODebugContext*) override {
    return IOStatus::IOError("injected Cedar Parquet write failure");
  }
};

class AppendFailingFileSystem final : public FileSystemWrapper {
 public:
  explicit AppendFailingFileSystem(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  const char* Name() const override { return "AppendFailingFileSystem"; }

  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* debug_context) override {
    std::unique_ptr<FSWritableFile> target;
    IOStatus status = FileSystemWrapper::NewWritableFile(
        path, options, &target, debug_context);
    if (status.ok()) {
      file->reset(new AppendFailingWritableFile(std::move(target)));
    }
    return status;
  }
};

TEST(CedarParquetTableBuilderTest, WritesBoundedRowGroupsAndCanonicalColumns) {
  const std::string path = TestPath("CedarParquetTableBuilder");
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
  parquet_options.page_compression = CedarParquetCompressionCodec::kLz4Raw;
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   parquet_options, file.get());

  const std::vector<std::string> keys = {
      InternalKeyFor(1, 3), InternalKeyFor(2, 2, kTypeDeletion),
      InternalKeyFor(3, 1)};
  const std::vector<std::string> values = {StateFactValue(), "", StateFactValue()};
  for (size_t index = 0; index < keys.size(); ++index) {
    builder.Add(keys[index], values[index]);
    ASSERT_TRUE(builder.status().ok()) << builder.status().ToString();
  }
  ASSERT_TRUE(builder.Finish().ok()) << builder.status().ToString();
  EXPECT_EQ(file->GetFlushedSize(), file->GetFileSize());
  ASSERT_TRUE(file->Close(IOOptions()).ok());

  const std::string encoded = ReadAll(path);
  ASSERT_GE(encoded.size(), 12U);
  EXPECT_EQ(encoded.substr(0, 4), "PAR1");
  EXPECT_EQ(encoded.substr(encoded.size() - 4), "PAR1");
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(encoded, &footer, nullptr).ok());
  ASSERT_EQ(footer.num_rows, 3);
  for (const auto& row_group : footer.row_groups) {
    for (const auto& column : row_group.columns) {
      EXPECT_EQ(column.compression_codec, CedarParquetCompressionCodec::kLz4Raw);
    }
  }
  bool reports_distinct_compressed_and_uncompressed_sizes = false;
  for (const auto& row_group : footer.row_groups) {
    for (const auto& column : row_group.columns) {
      reports_distinct_compressed_and_uncompressed_sizes =
          reports_distinct_compressed_and_uncompressed_sizes ||
          column.total_compressed_size != column.total_uncompressed_size;
    }
  }
  EXPECT_TRUE(reports_distinct_compressed_and_uncompressed_sizes);
  ASSERT_EQ(footer.row_groups.size(), 2U);
  ASSERT_EQ(footer.row_groups[0].num_rows, 2);
  ASSERT_EQ(footer.row_groups[1].num_rows, 1);
  std::string first_sort_key;
  std::string last_sort_key;
  ASSERT_TRUE(EncodeCedarParquetSortKey(keys[0], &first_sort_key).ok());
  ASSERT_TRUE(EncodeCedarParquetSortKey(keys[1], &last_sort_key).ok());
  EXPECT_EQ(footer.row_groups[0].columns[0].min_value, first_sort_key);
  EXPECT_EQ(footer.row_groups[0].columns[0].max_value, last_sort_key);

  std::vector<std::string> decoded_keys;
  std::vector<std::string> decoded_values;
  for (const auto& row_group : footer.row_groups) {
    std::vector<std::string> sort_keys;
    std::vector<std::optional<std::string>> optional_sort_keys;
    size_t consumed = 0;
    const auto& sort_column = row_group.columns[0];
    ASSERT_TRUE(DecodePlainPrimitiveDataPage(
                    encoded.substr(static_cast<size_t>(sort_column.data_page_offset),
                                   static_cast<size_t>(sort_column.total_compressed_size)),
                    sort_column.physical_type, false, &optional_sort_keys, &consumed,
                    sort_column.type_length, sort_column.compression_codec)
                    .ok());
    ASSERT_EQ(consumed, static_cast<size_t>(sort_column.total_compressed_size));
    for (const auto& value : optional_sort_keys) {
      ASSERT_TRUE(value.has_value());
      sort_keys.push_back(*value);
    }

    std::vector<std::string> group_keys;
    const auto& key_column = row_group.columns[1];
    ASSERT_TRUE(DecodePlainByteArrayDataPage(
                    encoded.substr(static_cast<size_t>(key_column.data_page_offset),
                                   static_cast<size_t>(key_column.total_compressed_size)),
                    &group_keys, nullptr, key_column.compression_codec)
                    .ok());
    std::vector<std::string> group_values;
    const auto& value_column = row_group.columns[2];
    ASSERT_TRUE(DecodePlainByteArrayDataPage(
                    encoded.substr(static_cast<size_t>(value_column.data_page_offset),
                                   static_cast<size_t>(value_column.total_compressed_size)),
                    &group_values, nullptr, value_column.compression_codec)
                    .ok());
    ASSERT_EQ(sort_keys.size(), group_keys.size());
    ASSERT_EQ(group_keys.size(), group_values.size());
    decoded_keys.insert(decoded_keys.end(), group_keys.begin(), group_keys.end());
    decoded_values.insert(decoded_values.end(), group_values.begin(), group_values.end());
  }
  EXPECT_EQ(decoded_keys, keys);
  EXPECT_EQ(decoded_values, values);
  EXPECT_EQ(builder.GetTableProperties().num_entries, 3U);
  EXPECT_EQ(builder.GetTableProperties().num_deletions, 1U);
  EXPECT_EQ(builder.GetTableProperties().key_largest_seqno, 3U);
  EXPECT_EQ(builder.GetTableProperties().key_smallest_seqno, 1U);
}

TEST(CedarParquetTableBuilderTest, PropagatesWritableFileFlushFailure) {
  const std::string path = TestPath("CedarParquetTableBuilderAppendFailure");
  auto file_system = std::make_shared<AppendFailingFileSystem>(
      Env::Default()->GetFileSystem());
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(file_system, path, FileOptions(),
                                         &file, nullptr)
                  .ok());

  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options,
                                                   mutable_options, comparator),
                                   CedarParquetTableOptions(), file.get());
  builder.Add(InternalKeyFor(1, 1), StateFactValue());
  ASSERT_TRUE(builder.status().ok()) << builder.status().ToString();

  const Status status = builder.Finish();
  EXPECT_TRUE(status.IsIOError()) << status.ToString();
  EXPECT_TRUE(builder.io_status().IsIOError()) << builder.io_status().ToString();
  EXPECT_NE(status.ToString().find("injected Cedar Parquet write failure"),
            std::string::npos);
}

TEST(CedarParquetTableBuilderTest, RejectsComparatorRegressionAndOversizedRow) {
  const std::string path = TestPath("CedarParquetTableBuilderRejects");
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableOptions parquet_options;
  parquet_options.row_group_max_bytes = 1024;
  parquet_options.max_row_bytes = 1024;
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   parquet_options, file.get());

  builder.Add(InternalKeyFor(2, 2), StateFactValue());
  ASSERT_TRUE(builder.status().ok());
  builder.Add(InternalKeyFor(1, 1), StateFactValue());
  EXPECT_TRUE(builder.status().IsInvalidArgument());
  builder.Abandon();

  CedarParquetTableBuilder oversized(BuilderOptions(immutable_options, mutable_options,
                                                     comparator),
                                     parquet_options, file.get());
  oversized.Add(InternalKeyFor(3, 1), std::string(1024, 'x'));
  EXPECT_TRUE(oversized.status().IsMemoryLimit());
  oversized.Abandon();
}

TEST(CedarParquetTableBuilderTest, WritesAlignedBoundedPagesAndOffsetIndexes) {
  const std::string path = TestPath("CedarParquetTableBuilderPages");
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableOptions parquet_options;
  parquet_options.row_group_max_rows = 4;
  parquet_options.row_group_max_bytes = 4096;
  parquet_options.page_max_rows = 2;
  parquet_options.page_max_bytes = 4096;
  parquet_options.max_row_bytes = 512;
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   parquet_options, file.get());
  for (uint64_t entity_id = 1; entity_id <= 4; ++entity_id) {
    builder.Add(InternalKeyFor(entity_id, 5 - entity_id), StateFactValue());
  }
  ASSERT_TRUE(builder.Finish().ok()) << builder.status().ToString();
  ASSERT_TRUE(file->Close(IOOptions()).ok());

  const std::string encoded = ReadAll(path);
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(encoded, &footer, nullptr).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  const auto& row_group = footer.row_groups.front();
  for (const auto& column : row_group.columns) {
    ASSERT_GT(column.offset_index_offset, 0);
    ASSERT_GT(column.offset_index_length, 0);
    ASSERT_GT(column.column_index_offset, 0);
    ASSERT_GT(column.column_index_length, 0);
    std::vector<CedarParquetFooter::ColumnChunk::PageLocation> locations;
    ASSERT_TRUE(DecodeOffsetIndex(
                    encoded.substr(static_cast<size_t>(column.offset_index_offset),
                                   static_cast<size_t>(column.offset_index_length)),
                    &locations)
                    .ok());
    ASSERT_EQ(locations.size(), 2U);
    EXPECT_EQ(locations[0].first_row_index, 0);
    EXPECT_EQ(locations[1].first_row_index, 2);
    size_t values = 0;
    for (const auto& location : locations) {
      std::vector<std::optional<std::string>> page;
      ASSERT_TRUE(DecodePlainPrimitiveDataPage(
                      encoded.substr(static_cast<size_t>(location.offset),
                                     static_cast<size_t>(location.compressed_page_size)),
                      column.physical_type,
                      footer.schema[&column - row_group.columns.data() + 1].repetition_type ==
                          kParquetOptional,
                      &page, nullptr,
                      column.type_length)
                      .ok());
      values += page.size();
    }
    EXPECT_EQ(values, 4U);
    std::vector<CedarParquetFooter::ColumnChunk::PageIndex> indexes;
    ASSERT_TRUE(DecodeColumnIndex(
                    encoded.substr(static_cast<size_t>(column.column_index_offset),
                                   static_cast<size_t>(column.column_index_length)),
                    &indexes)
                    .ok());
    EXPECT_EQ(indexes.size(), 2U);
  }
  const auto& sort_column = row_group.columns.front();
  ASSERT_GT(sort_column.column_index_offset, 0);
  ASSERT_GT(sort_column.column_index_length, 0);
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> page_indexes;
  ASSERT_TRUE(DecodeColumnIndex(
                  encoded.substr(static_cast<size_t>(sort_column.column_index_offset),
                                 static_cast<size_t>(sort_column.column_index_length)),
                  &page_indexes)
                  .ok());
  ASSERT_EQ(page_indexes.size(), 2U);
  std::string first_sort_key;
  std::string second_sort_key;
  std::string third_sort_key;
  std::string fourth_sort_key;
  ASSERT_TRUE(EncodeCedarParquetSortKey(InternalKeyFor(1, 4), &first_sort_key).ok());
  ASSERT_TRUE(EncodeCedarParquetSortKey(InternalKeyFor(2, 3), &second_sort_key).ok());
  ASSERT_TRUE(EncodeCedarParquetSortKey(InternalKeyFor(3, 2), &third_sort_key).ok());
  ASSERT_TRUE(EncodeCedarParquetSortKey(InternalKeyFor(4, 1), &fourth_sort_key).ok());
  EXPECT_EQ(page_indexes[0].min_value, first_sort_key);
  EXPECT_EQ(page_indexes[0].max_value, second_sort_key);
  EXPECT_EQ(page_indexes[1].min_value, third_sort_key);
  EXPECT_EQ(page_indexes[1].max_value, fourth_sort_key);
}

TEST(CedarParquetTableBuilderTest, RejectsRangeDeletionEntries) {
  const std::string path = TestPath("CedarParquetTableBuilderRangeDeletion");
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   CedarParquetTableOptions(), file.get());

  builder.Add(InternalKeyFor(1, 1, kTypeRangeDeletion), "z");
  EXPECT_TRUE(builder.status().IsNotSupported());
  builder.Abandon();
}

TEST(CedarParquetTableBuilderTest, RejectsNoncanonicalFactValue) {
  const std::string path = TestPath("CedarParquetTableBuilderRejectsValue");
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   CedarParquetTableOptions(), file.get());

  builder.Add(InternalKeyFor(1, 1), "not-a-cedar-fact");
  EXPECT_TRUE(builder.status().IsCorruption());
  builder.Abandon();
}

TEST(CedarParquetTableBuilderTest,
     WritesMaterializedColumnsUsingOptionalNativePhysicalPages) {
  const std::string path = TestPath("CedarParquetTableBuilderMaterialized");
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   CedarParquetTableOptions(), file.get());
  builder.Add(InternalKeyFor(7, 1), StateFactValue());
  ASSERT_TRUE(builder.Finish().ok()) << builder.status().ToString();
  ASSERT_TRUE(file->Close(IOOptions()).ok());

  CedarParquetFooter footer;
  const std::string encoded = ReadAll(path);
  ASSERT_TRUE(ParseParquetFooter(encoded, &footer, nullptr).ok());
  ASSERT_EQ(footer.schema.size(), 26U);
  ASSERT_EQ(footer.schema[0].num_children, 25);
  ASSERT_EQ(footer.schema[4].name, "part_id");
  EXPECT_EQ(footer.schema[4].physical_type, kParquetInt32);
  EXPECT_EQ(footer.schema[4].repetition_type, kParquetOptional);
  ASSERT_EQ(footer.row_groups.size(), 1U);
  ASSERT_EQ(footer.row_groups[0].columns.size(), 25U);
  const auto& part_id = footer.row_groups[0].columns[3];
  EXPECT_EQ(part_id.path, "part_id");
  EXPECT_EQ(part_id.physical_type, kParquetInt32);
  std::vector<std::optional<std::string>> values;
  ASSERT_TRUE(DecodePlainPrimitiveDataPage(
                  encoded.substr(static_cast<size_t>(part_id.data_page_offset),
                                 static_cast<size_t>(part_id.total_compressed_size)),
                  part_id.physical_type, true, &values, nullptr)
                  .ok());
  ASSERT_EQ(values.size(), 1U);
  ASSERT_TRUE(values[0].has_value());
  ASSERT_EQ(values[0]->size(), 4U);
  EXPECT_EQ(static_cast<unsigned char>((*values[0])[0]), 0U);
  EXPECT_EQ(static_cast<unsigned char>((*values[0])[1]), 0U);
  EXPECT_EQ(static_cast<unsigned char>((*values[0])[2]), 0U);
  EXPECT_EQ(static_cast<unsigned char>((*values[0])[3]), 0U);

  const auto& bool_value = footer.row_groups[0].columns[13];
  ASSERT_EQ(bool_value.path, "bool_value");
  ASSERT_TRUE(DecodePlainPrimitiveDataPage(
                  encoded.substr(static_cast<size_t>(bool_value.data_page_offset),
                                 static_cast<size_t>(bool_value.total_compressed_size)),
                  bool_value.physical_type, true, &values, nullptr)
                  .ok());
  ASSERT_EQ(values.size(), 1U);
  EXPECT_FALSE(values[0].has_value());
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
