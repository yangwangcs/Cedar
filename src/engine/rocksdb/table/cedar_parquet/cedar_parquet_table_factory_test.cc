// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/cedar_parquet/cedar_parquet_table_builder.h"
#include "table/cedar_parquet/cedar_parquet_table_factory.h"
#include "table/cedar_parquet/parquet_metadata.h"
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

std::string InternalKeyFor(uint64_t entity_id, SequenceNumber sequence) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(V2UserKey(entity_id), sequence, kTypeValue));
  return internal_key;
}

class CedarParquetFactTableFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char path[] = "/tmp/CedarParquetFactoryXXXXXX";
    ASSERT_NE(mkdtemp(path), nullptr);
    path_ = path;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::string path_;
};

std::vector<ColumnFamilyDescriptor> Descriptors(
    const std::shared_ptr<TableFactory>& facts_factory) {
  ColumnFamilyOptions facts_options;
  facts_options.compression = kNoCompression;
  facts_options.bottommost_compression = kNoCompression;
  facts_options.enable_blob_files = false;
  facts_options.table_factory = facts_factory;
  return {{kDefaultColumnFamilyName, ColumnFamilyOptions()},
          {"facts", std::move(facts_options)}};
}

TEST(CedarParquetFactTableFactoryOptionsTest, RejectsUnsupportedFactPolicies) {
  CedarParquetTableOptions table_options;
  CedarParquetFactTableFactory factory(table_options);
  DBOptions db_options;
  ColumnFamilyOptions cf_options;

  cf_options.comparator = BytewiseComparator();
  cf_options.compression = kNoCompression;
  cf_options.enable_blob_files = false;
  ASSERT_TRUE(factory.ValidateOptions(db_options, cf_options).ok());

  cf_options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  EXPECT_TRUE(factory.ValidateOptions(db_options, cf_options).IsInvalidArgument());
  cf_options.prefix_extractor.reset();

  cf_options.compression = kLZ4Compression;
  EXPECT_TRUE(factory.ValidateOptions(db_options, cf_options).IsInvalidArgument());
  cf_options.compression = kNoCompression;

  cf_options.enable_blob_files = true;
  EXPECT_TRUE(factory.ValidateOptions(db_options, cf_options).IsInvalidArgument());
}

TEST_F(CedarParquetFactTableFactoryTest,
       UsesZstdForVerifiedBottommostOutput) {
  CedarParquetTableOptions table_options;
  table_options.page_compression = CedarParquetCompressionCodec::kLz4Raw;
  CedarParquetFactTableFactory factory(table_options);
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  static const ReadOptions read_options;
  static const WriteOptions write_options;
  static const CompressionOptions compression_options;
  static const std::string column_family_name = "facts";
  TableBuilderOptions builder_options(
      immutable_options, mutable_options, read_options, write_options, comparator,
      nullptr, kNoCompression, compression_options, 1, column_family_name, 1,
      0, true);

  const std::string table_path = path_ + "/bottommost.sst";
  std::unique_ptr<WritableFileWriter> file;
  ASSERT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), table_path,
                                         FileOptions(), &file, nullptr)
                  .ok());
  std::unique_ptr<TableBuilder> builder(
      factory.NewTableBuilder(builder_options, file.get()));
  ASSERT_NE(builder, nullptr);
  builder->Add(InternalKeyFor(1, 1), StateFactValue());
  ASSERT_TRUE(builder->Finish().ok()) << builder->status().ToString();
  ASSERT_TRUE(file->Close(IOOptions()).ok());

  std::ifstream input(table_path, std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string encoded((std::istreambuf_iterator<char>(input)),
                            std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(encoded, &footer, nullptr).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  ASSERT_FALSE(footer.row_groups[0].columns.empty());
  EXPECT_EQ(footer.row_groups[0].columns[0].compression_codec,
            CedarParquetCompressionCodec::kZstd);
}

TEST_F(CedarParquetFactTableFactoryTest, WritesAndReopensParquetV2FactsTable) {
  auto facts_factory = std::shared_ptr<TableFactory>(
      NewCedarParquetFactTableFactory(CedarParquetTableOptions()));
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> database;
  ASSERT_TRUE(DB::Open(options, path_, Descriptors(facts_factory), &handles, &database).ok());
  ASSERT_EQ(handles.size(), 2U);
  const std::string fact_key = V2UserKey(7);
  const std::string fact_value = StateFactValue();
  ASSERT_TRUE(database->Put(WriteOptions(), handles[1], fact_key, fact_value).ok());
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(database->Flush(flush_options, handles[1]).ok());
  for (ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(database->DestroyColumnFamilyHandle(handle).ok());
  }
  handles.clear();
  database.reset();

  bool found_parquet_file = false;
  for (const auto& entry : std::filesystem::directory_iterator(path_)) {
    if (entry.path().extension() != ".sst") continue;
    std::ifstream input(entry.path(), std::ios::binary);
    std::string magic(4, '\0');
    input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
    found_parquet_file = found_parquet_file || magic == "PAR1";
  }
  ASSERT_TRUE(found_parquet_file);

  ASSERT_TRUE(DB::Open(options, path_, Descriptors(facts_factory), &handles, &database).ok());
  std::string value;
  ASSERT_TRUE(database->Get(ReadOptions(), handles[1], fact_key, &value).ok());
  EXPECT_EQ(value, fact_value);
  std::unique_ptr<Iterator> iterator(database->NewIterator(ReadOptions(), handles[1]));
  iterator->Seek(Slice(fact_key.data(), 8));
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  EXPECT_EQ(iterator->key(), fact_key);
  for (ColumnFamilyHandle* handle : handles) {
    EXPECT_TRUE(database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(CedarParquetFactTableFactoryTest, RejectsLegacyBlockBasedFactsTable) {
  const std::string legacy_key = "legacy-key";
  const std::string legacy_value = "legacy-value";

  // Create a facts SST with the default block-based table factory.
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> database;
  ASSERT_TRUE(DB::Open(options, path_, Descriptors(options.table_factory), &handles, &database)
                  .ok());
  ASSERT_TRUE(database->Put(WriteOptions(), handles[1], legacy_key, legacy_value).ok());
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(database->Flush(flush_options, handles[1]).ok());
  for (ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(database->DestroyColumnFamilyHandle(handle).ok());
  }
  handles.clear();
  database.reset();

  // The Cedar Parquet fact factory opens readers on demand, so DB::Open does not
  // inspect the legacy SST and succeeds. The legacy block-based file is still
  // rejected: the first read opens it through NewTableReader, whose magic check
  // fails with "facts table is not Cedar Parquet v2".
  auto facts_factory = std::shared_ptr<TableFactory>(
      NewCedarParquetFactTableFactory(CedarParquetTableOptions()));
  const Status open_status =
      DB::Open(options, path_, Descriptors(facts_factory), &handles, &database);
  ASSERT_TRUE(open_status.ok()) << open_status.ToString();

  // Keep using EXPECT below so that the column family handles are always
  // destroyed; leaking them trips ColumnFamilySet's last_ref assertion.
  std::cout << "DB::Open status: " << open_status.ToString() << std::endl;
  std::string value;
  const Status get_status = database->Get(ReadOptions(), handles[1], legacy_key, &value);
  std::cout << "first Get status: " << get_status.ToString() << std::endl;
  EXPECT_TRUE(get_status.IsCorruption()) << get_status.ToString();
  EXPECT_NE(get_status.ToString().find("facts table"), std::string::npos)
      << get_status.ToString();
  EXPECT_NE(get_status.ToString().find("Cedar Parquet"), std::string::npos)
      << get_status.ToString();

  std::unique_ptr<Iterator> iterator(database->NewIterator(ReadOptions(), handles[1]));
  if (iterator != nullptr) {
    iterator->SeekToFirst();
    std::cout << "iterator status: " << iterator->status().ToString() << std::endl;
    EXPECT_TRUE(iterator->status().IsCorruption()) << iterator->status().ToString();
    EXPECT_NE(iterator->status().ToString().find("facts table"), std::string::npos)
        << iterator->status().ToString();
    iterator->Seek(legacy_key);
    EXPECT_TRUE(iterator->status().IsCorruption()) << iterator->status().ToString();
    iterator.reset();
  } else {
    ADD_FAILURE() << "NewIterator returned null";
  }

  for (ColumnFamilyHandle* handle : handles) {
    EXPECT_TRUE(database->DestroyColumnFamilyHandle(handle).ok());
  }
  handles.clear();
  database.reset();
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
