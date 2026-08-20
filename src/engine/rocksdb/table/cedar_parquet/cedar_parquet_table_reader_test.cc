// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <string>
#include <fstream>
#include <limits>
#include <optional>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cedar_commit.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "table/cedar_parquet/cedar_parquet_table_builder.h"
#include "table/cedar_parquet/cedar_parquet_table_reader.h"
#include "table/cedar_parquet/parquet_plain_page.h"
#include "table/multiget_context.h"
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

std::string V2FactUserKey(uint64_t entity_id, uint64_t valid_from,
                          uint64_t cedar_commit_seq) {
  std::string key = V2UserKey(entity_id);
  StoreBigEndian64(&key, 16, ~valid_from);
  StoreBigEndian64(&key, 24, ~cedar_commit_seq);
  return key;
}

std::string InternalKeyFor(uint64_t entity_id, SequenceNumber sequence,
                           ValueType value_type = kTypeValue) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(V2UserKey(entity_id), sequence, value_type));
  return internal_key;
}

std::string InternalFactKeyFor(uint64_t entity_id, uint64_t valid_from,
                               uint64_t cedar_commit_seq,
                               SequenceNumber sequence) {
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(
                        V2FactUserKey(entity_id, valid_from, cedar_commit_seq),
                        sequence, kTypeValue));
  return internal_key;
}

std::string SortKeyFor(const std::string& internal_key) {
  std::string sort_key;
  EXPECT_TRUE(EncodeCedarParquetSortKey(internal_key, &sort_key).ok());
  return sort_key;
}

void AppendBigEndian32(std::string* destination, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    destination->push_back(static_cast<char>(value >> shift));
  }
}

void AppendLittleEndian32(std::string* destination, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
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
  static const std::string path =
      "/tmp/CedarParquetTableReader-" + std::to_string(getpid()) + ".sst";
  return path;
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

std::unique_ptr<CedarParquetTableReader> BuildAndOpen(
    const std::vector<std::string>& keys, const std::vector<std::string>& values,
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_options,
    const InternalKeyComparator& comparator, uint32_t row_group_max_rows = 2,
    uint32_t page_max_rows = 1, uint64_t row_group_max_bytes = 4096) {
  std::unique_ptr<WritableFileWriter> writer;
  EXPECT_TRUE(WritableFileWriter::Create(Env::Default()->GetFileSystem(), TestPath(),
                                         FileOptions(), &writer, nullptr)
                  .ok());
  CedarParquetTableOptions options;
  options.row_group_max_rows = row_group_max_rows;
  options.page_max_rows = page_max_rows;
  options.row_group_max_bytes = row_group_max_bytes;
  options.max_row_bytes = 512;
  CedarParquetTableBuilder builder(BuilderOptions(immutable_options, mutable_options,
                                                   comparator),
                                   options, writer.get());
  for (size_t index = 0; index < keys.size(); ++index) builder.Add(keys[index], values[index]);
  const Status finish_status = builder.Finish();
  EXPECT_TRUE(finish_status.ok()) << finish_status.ToString();
  EXPECT_TRUE(writer->Close(IOOptions()).ok());

  uint64_t file_size = 0;
  EXPECT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  EXPECT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  std::unique_ptr<CedarParquetTableReader> reader;
  Status open_status = CedarParquetTableReader::Open(comparator, std::move(file), file_size,
                                                      options, &reader);
  EXPECT_TRUE(open_status.ok()) << open_status.ToString();
  return reader;
}

std::unique_ptr<CedarParquetTableReader> OpenExisting(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_options,
    const InternalKeyComparator& comparator) {
  CedarParquetTableOptions options;
  options.row_group_max_rows = 2;
  options.page_max_rows = 1;
  options.row_group_max_bytes = 4096;
  options.max_row_bytes = 512;
  uint64_t file_size = 0;
  EXPECT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  EXPECT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, options, &reader);
  EXPECT_TRUE(status.ok()) << status.ToString();
  return reader;
}

TEST(CedarParquetTableReaderTest, IteratesAndSeeksAcrossBoundedRowGroups) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3),
                                         InternalKeyFor(3, 2), InternalKeyFor(4, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  const ReadOptions read_options;
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      read_options, nullptr, nullptr, false, TableReaderCaller::kUserGet));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key(), keys[0]);
  EXPECT_EQ(iterator->value(), values[0]);
  iterator->Next();
  iterator->Next();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key(), keys[2]);
  EXPECT_EQ(iterator->value(), values[2]);
  iterator->Seek(keys[1]);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key(), keys[1]);
  std::string identity_seek;
  AppendInternalKey(&identity_seek,
                    ParsedInternalKey(Slice(keys[2].data(), 8),
                                      kMaxSequenceNumber, kValueTypeForSeek));
  iterator->Seek(identity_seek);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key(), keys[0]);
  iterator->SeekForPrev(keys[2]);
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key(), keys[2]);
  iterator->SeekToLast();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key(), keys[3]);
  EXPECT_TRUE(iterator->IsKeyPinned());
  EXPECT_TRUE(iterator->IsValuePinned());
  iterator->Prev();
  EXPECT_EQ(iterator->key(), keys[2]);
  EXPECT_TRUE(iterator->status().ok());
  EXPECT_EQ(reader->GetTableProperties()->num_entries, keys.size());
}

TEST(CedarParquetTableReaderTest, GetsNewestVisibleVersionAcrossRowGroups) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 5), InternalKeyFor(1, 4),
                                         InternalKeyFor(1, 3), InternalKeyFor(2, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);

  PinnableSlice value;
  const std::string lookup = InternalKeyFor(1, 4);
  const std::string user_key = V2UserKey(1);
  GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, user_key, &value, nullptr, nullptr,
                         nullptr, true, nullptr, nullptr);
  ASSERT_TRUE(reader->Get(ReadOptions(), lookup, &get_context, nullptr).ok());
  EXPECT_EQ(get_context.State(), GetContext::kFound);
  EXPECT_EQ(value.ToString(), values[1]);
  EXPECT_TRUE(reader->VerifyChecksum(ReadOptions(), TableReaderCaller::kUserGet).ok());
}

TEST(CedarParquetTableReaderTest, IteratorSeekDecodesOnlyTheTargetPage) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3),
                                         InternalKeyFor(3, 2), InternalKeyFor(4, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);

  reader->ResetPageDecodeCountForTesting();
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserGet));
  iterator->Seek(keys[3]);

  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  EXPECT_EQ(iterator->key().ToString(), keys[3]);
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 1U);
}

TEST(CedarParquetTableReaderTest,
     ScansProjectedTypedVectorsWithoutOpeningCanonicalValuePages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3),
                                         InternalKeyFor(3, 2), InternalKeyFor(4, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  CedarParquetScanSpec spec;
  spec.sort_key_lower = SortKeyFor(keys[1]);
  spec.sort_key_upper = SortKeyFor(keys[2]);
  spec.batch_row_limit = 1;
  spec.projection = {CedarParquetColumnId::kEntityId,
                     CedarParquetColumnId::kFactFamily};
  reader->ResetPageDecodeCountForTesting();

  std::vector<uint64_t> entities;
  std::vector<uint32_t> families;
  ASSERT_TRUE(reader->ScanProjected(
      spec, [&entities, &families](const CedarParquetColumnarBatch& batch) -> Status {
        EXPECT_EQ(batch.row_count(), 1U);
        EXPECT_EQ(batch.columns.size(), 2U);
        if (batch.columns.size() != 2U) return Status::Corruption("test batch column count");
        const auto& entity = batch.columns[0];
        const auto& family = batch.columns[1];
        EXPECT_EQ(entity.id, CedarParquetColumnId::kEntityId);
        EXPECT_EQ(family.id, CedarParquetColumnId::kFactFamily);
        EXPECT_EQ(entity.present, std::vector<uint8_t>({1}));
        EXPECT_EQ(family.present, std::vector<uint8_t>({1}));
        entities.push_back(std::get<std::vector<uint64_t>>(entity.values)[0]);
        families.push_back(std::get<std::vector<uint32_t>>(family.values)[0]);
        return Status::OK();
      }).ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({2, 3}));
  EXPECT_EQ(families, std::vector<uint32_t>({1, 1}));
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 0U);
}

TEST(CedarParquetTableReaderTest, ReportsMetadataPruningAndBytesRead) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3),
                                         InternalKeyFor(3, 2), InternalKeyFor(4, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);

  CedarParquetScanStats stats;
  CedarParquetScanSpec spec;
  spec.sort_key_lower = SortKeyFor(keys[1]);
  spec.sort_key_upper = SortKeyFor(keys[2]);
  spec.batch_row_limit = 8;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.stats = &stats;
  ASSERT_TRUE(reader->ScanProjected(spec, [](const CedarParquetColumnarBatch&) {
                         return Status::OK();
                       })
                  .ok());
  EXPECT_EQ(stats.row_groups_skipped, 0U);
  EXPECT_EQ(stats.pages_skipped, 2U);
  EXPECT_EQ(stats.pages_read, 2U);
  EXPECT_GT(stats.bytes_read, 0U);
  EXPECT_EQ(stats.rows_emitted, 2U);
}

TEST(CedarParquetTableReaderTest, PrunesNumericCommitRangeBeforeReadingPages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {
      InternalFactKeyFor(1, 10, 1, 4), InternalFactKeyFor(2, 20, 2, 3),
      InternalFactKeyFor(3, 30, 3, 2), InternalFactKeyFor(4, 40, 4, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);

  CedarParquetScanStats stats;
  CedarParquetScanSpec spec;
  spec.cedar_commit_seq_min = std::numeric_limits<uint64_t>::max();
  spec.batch_row_limit = 8;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.stats = &stats;
  ASSERT_TRUE(reader->ScanProjected(spec, [](const CedarParquetColumnarBatch&) {
                         return Status::OK();
                       })
                  .ok());
  EXPECT_EQ(stats.pages_read, 0U);
  EXPECT_EQ(stats.pages_skipped, 4U);
  EXPECT_EQ(stats.rows_emitted, 0U);
}

TEST(CedarParquetTableReaderTest,
     NumericCommitRangeUsesNumericBoundsAcrossLittleEndianByteBoundary) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {
      InternalFactKeyFor(1, 10, 1, 4), InternalFactKeyFor(2, 20, 256, 3),
      InternalFactKeyFor(3, 30, 2, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator,
                   8, 8);

  CedarParquetScanSpec spec;
  spec.cedar_commit_seq_min = 1;
  spec.cedar_commit_seq_max = 1;
  spec.batch_row_limit = 8;
  spec.projection = {CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  ASSERT_TRUE(reader->ScanProjected(
                           spec, [&entities](const CedarParquetColumnarBatch& batch) {
                             const auto& values = std::get<std::vector<uint64_t>>(
                                 batch.columns[0].values);
                             entities.insert(entities.end(), values.begin(), values.end());
                             return Status::OK();
                           })
                  .ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({1}));
}

TEST(CedarParquetTableReaderTest,
     ParallelProjectedScanMatchesSerialRowsInDeterministicOrder) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (uint64_t entity = 1; entity <= 8; ++entity) {
    keys.push_back(InternalKeyFor(entity, 100 - entity));
    values.push_back(StateFactValue());
  }
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator,
      /*row_group_max_rows=*/2, /*page_max_rows=*/1);

  const auto collect = [](CedarParquetTableReader* table,
                          uint32_t workers,
                          std::vector<std::string>* observed_keys,
                          std::vector<uint64_t>* observed_entities) {
    CedarParquetScanSpec spec;
    spec.max_parallel_row_groups = workers;
    spec.batch_row_limit = 3;
    spec.projection = {CedarParquetColumnId::kEntityId};
    return table->ScanProjected(
        spec, [observed_keys, observed_entities](
                 const CedarParquetColumnarBatch& batch) {
          const auto& entity = std::get<std::vector<uint64_t>>(
              batch.columns.front().values);
          for (size_t row = 0; row < batch.row_count(); ++row) {
            observed_keys->push_back(batch.internal_keys[row]);
            observed_entities->push_back(entity[row]);
          }
          return Status::OK();
        });
  };

  std::vector<std::string> serial_keys;
  std::vector<uint64_t> serial_entities;
  ASSERT_TRUE(collect(reader.get(), 1, &serial_keys, &serial_entities).ok());
  std::vector<std::string> parallel_keys;
  std::vector<uint64_t> parallel_entities;
  ASSERT_TRUE(collect(reader.get(), 2, &parallel_keys, &parallel_entities).ok());
  EXPECT_EQ(parallel_keys, serial_keys);
  EXPECT_EQ(parallel_entities, serial_entities);
}

TEST(CedarParquetTableReaderTest, ParallelProjectedScanReportsBoundedWorkers) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (uint64_t entity = 1; entity <= 16; ++entity) {
    keys.push_back(InternalKeyFor(entity, 100 - entity));
    values.push_back(StateFactValue());
  }
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator,
      /*row_group_max_rows=*/2, /*page_max_rows=*/1);
  CedarParquetScanStats stats;
  CedarParquetScanSpec spec;
  spec.max_parallel_row_groups = 3;
  spec.batch_row_limit = 2;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.stats = &stats;
  ASSERT_TRUE(reader->ScanProjected(spec, [](const CedarParquetColumnarBatch&) {
                       return Status::OK();
                     })
                  .ok());
  EXPECT_GT(stats.max_parallel_row_groups_observed, 0U);
  EXPECT_LE(stats.max_parallel_row_groups_observed, 3U);
}

TEST(CedarParquetTableReaderTest, RejectsUnboundedParallelWorkerRequest) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 1)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator);
  CedarParquetScanSpec spec;
  spec.max_parallel_row_groups = kCedarParquetMaximumParallelRowGroups + 1;
  Status status = reader->ScanProjected(
      spec, [](const CedarParquetColumnarBatch&) { return Status::OK(); });
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(CedarParquetTableReaderTest, ParallelProjectedScanJoinsWorkersAfterCancellation) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (uint64_t entity = 1; entity <= 12; ++entity) {
    keys.push_back(InternalKeyFor(entity, 100 - entity));
    values.push_back(StateFactValue());
  }
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator,
      /*row_group_max_rows=*/2, /*page_max_rows=*/1);
  CedarParquetScanStats stats;
  CedarParquetScanSpec spec;
  spec.max_parallel_row_groups = 3;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.stats = &stats;
  size_t batches = 0;
  Status status = reader->ScanProjected(
      spec, [&batches](const CedarParquetColumnarBatch&) {
        ++batches;
        return Status::Incomplete("test", "stop after first batch");
      });
  EXPECT_TRUE(status.IsIncomplete()) << status.ToString();
  EXPECT_EQ(batches, 1U);
  EXPECT_GT(stats.pages_read, 0U);
  EXPECT_GT(stats.bytes_read, 0U);
  EXPECT_GT(stats.rows_emitted, 0U);
  EXPECT_GT(stats.max_parallel_row_groups_observed, 0U);
  EXPECT_LE(stats.max_parallel_row_groups_observed, 3U);
}

TEST(CedarParquetTableReaderTest,
     RetainsNumericValidTimeRangeAcrossLittleEndianByteBoundary) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {
      InternalFactKeyFor(1, 1, 1, 3), InternalFactKeyFor(2, 256, 2, 2),
      InternalFactKeyFor(3, 2, 3, 1)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator,
      /*row_group_max_rows=*/3, /*page_max_rows=*/3);

  CedarParquetScanSpec spec;
  spec.valid_from_min = 1;
  spec.valid_from_max = 1;
  spec.batch_row_limit = 8;
  spec.projection = {CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  ASSERT_TRUE(reader->ScanProjected(
                         spec, [&entities](const CedarParquetColumnarBatch& batch) {
                           const auto& values =
                               std::get<std::vector<uint64_t>>(batch.columns[0].values);
                           entities.insert(entities.end(), values.begin(), values.end());
                           return Status::OK();
                         })
                  .ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({1}));
}

TEST(CedarParquetTableReaderTest, ProjectedCursorRejectsReversedNumericRange) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalFactKeyFor(1, 1, 1, 1)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);

  CedarParquetScanSpec spec;
  spec.valid_from_min = 2;
  spec.valid_from_max = 1;
  spec.projection = {CedarParquetColumnId::kEntityId};
  std::unique_ptr<CedarParquetProjectedCursor> cursor;
  EXPECT_TRUE(reader->NewProjectedCursor(spec, &cursor).IsInvalidArgument());
  EXPECT_EQ(cursor, nullptr);
}

TEST(CedarParquetTableReaderTest,
     RejectsMalformedNumericColumnIndexDuringNumericScan) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {
      InternalFactKeyFor(1, 1, 1, 1), InternalFactKeyFor(2, 2, 2, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  auto& commit_column = footer.row_groups[0].columns[8];
  std::string malformed_index;
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> malformed_pages;
  ASSERT_TRUE(DecodeColumnIndex(
                  file.substr(static_cast<size_t>(commit_column.column_index_offset),
                              static_cast<size_t>(commit_column.column_index_length)),
                  &malformed_pages)
                  .ok());
  ASSERT_FALSE(malformed_pages.empty());
  malformed_pages[0].min_value = "short";
  ASSERT_TRUE(EncodeColumnIndex(malformed_pages, &malformed_index).ok());
  commit_column.column_index_offset = static_cast<int64_t>(footer_offset);
  commit_column.column_index_length = static_cast<int32_t>(malformed_index.size());
  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.append(malformed_index);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status open_status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(open_status.IsCorruption()) << open_status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest,
     RejectsEncodedValuePageWhoseValueCountDoesNotMatchCanonicalRows) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  reader.reset();

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.is_open());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, nullptr).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  ASSERT_GE(footer.row_groups[0].columns.size(), 3U);
  const auto& encoded_value_column = footer.row_groups[0].columns[2];
  ASSERT_GT(encoded_value_column.offset_index_offset, 0);
  ASSERT_GT(encoded_value_column.offset_index_length, 0);
  std::vector<CedarParquetFooter::ColumnChunk::PageLocation> page_locations;
  ASSERT_TRUE(DecodeOffsetIndex(
                  file.substr(static_cast<size_t>(encoded_value_column.offset_index_offset),
                              static_cast<size_t>(encoded_value_column.offset_index_length)),
                  &page_locations)
                  .ok());
  ASSERT_EQ(page_locations.size(), 1U);
  const auto& encoded_value_page = page_locations.front();
  std::string empty_page;
  ASSERT_TRUE(EncodePlainByteArrayDataPage({}, &empty_page).ok());
  ASSERT_LE(empty_page.size(),
            static_cast<size_t>(encoded_value_page.compressed_page_size));

  std::fstream output(TestPath(), std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(output.is_open());
  output.seekp(encoded_value_page.offset);
  output.write(empty_page.data(), static_cast<std::streamsize>(empty_page.size()));
  const std::string padding(
      static_cast<size_t>(encoded_value_page.compressed_page_size) - empty_page.size(), '\0');
  output.write(padding.data(), static_cast<std::streamsize>(padding.size()));
  ASSERT_TRUE(output.good());
  output.close();

  reader = OpenExisting(immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEncodedValue};
  const Status status = reader->ScanProjected(
      spec, [](const CedarParquetColumnarBatch&) { return Status::OK(); });
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
}

TEST(CedarParquetTableReaderTest,
     RejectsCanonicalPagesWhoseSharedValueCountDisagreesWithOffsetIndex) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  reader.reset();

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.is_open());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, nullptr).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  ASSERT_GE(footer.row_groups[0].columns.size(), 2U);

  std::fstream output(TestPath(), std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(output.is_open());
  for (size_t column_index : {0U, 1U}) {
    const auto& column = footer.row_groups[0].columns[column_index];
    std::vector<CedarParquetFooter::ColumnChunk::PageLocation> locations;
    ASSERT_TRUE(DecodeOffsetIndex(
                    file.substr(static_cast<size_t>(column.offset_index_offset),
                                static_cast<size_t>(column.offset_index_length)),
                    &locations)
                    .ok());
    ASSERT_EQ(locations.size(), 1U);
    std::string empty_page;
    ASSERT_TRUE(EncodePlainPrimitiveDataPage(
                    {}, column.physical_type, false, &empty_page, column.type_length,
                    column.compression_codec)
                    .ok());
    ASSERT_LE(empty_page.size(), static_cast<size_t>(locations[0].compressed_page_size));
    output.seekp(locations[0].offset);
    output.write(empty_page.data(), static_cast<std::streamsize>(empty_page.size()));
    const std::string padding(
        static_cast<size_t>(locations[0].compressed_page_size) - empty_page.size(), '\0');
    output.write(padding.data(), static_cast<std::streamsize>(padding.size()));
  }
  ASSERT_TRUE(output.good());
  output.close();

  reader = OpenExisting(immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  CedarParquetScanSpec spec;
  const Status status = reader->ScanProjected(
      spec, [](const CedarParquetColumnarBatch&) { return Status::OK(); });
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
}

TEST(CedarParquetTableReaderTest, RejectsNonNormalizedProjectedScanBounds) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  CedarParquetScanSpec spec;
  spec.sort_key_lower = "not a normalized Cedar Parquet sort key";
  const Status status = reader->ScanProjected(
      spec, [](const CedarParquetColumnarBatch&) { return Status::OK(); });
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(CedarParquetTableReaderTest, ProjectedCursorStreamsOneBoundedPageAtATime) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 5), InternalKeyFor(2, 4),
                                         InternalKeyFor(3, 3)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId};
  std::unique_ptr<CedarParquetProjectedCursor> cursor;
  ASSERT_TRUE(reader->NewProjectedCursor(spec, &cursor).ok());
  ASSERT_NE(cursor, nullptr);

  std::vector<uint64_t> entities;
  std::vector<std::string> internal_keys;
  while (cursor->Valid()) {
    const auto& batch = cursor->batch();
    const size_t row = cursor->row_index();
    ASSERT_EQ(batch.columns.size(), 1U);
    const auto& values = std::get<std::vector<uint64_t>>(batch.columns[0].values);
    ASSERT_LT(row, values.size());
    entities.push_back(values[row]);
    internal_keys.push_back(cursor->internal_key().ToString());
    cursor->Next();
  }
  ASSERT_TRUE(cursor->status().ok()) << cursor->status().ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({1, 2, 3}));
  EXPECT_EQ(internal_keys, keys);
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 0U);
}

TEST(CedarParquetTableReaderTest,
     ProjectedCursorIncludesTheFirstRowAtTheStandardSeekLowerBound) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {
      InternalKeyFor(1, kMaxSequenceNumber),
      InternalKeyFor(2, 4),
  };
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  CedarParquetScanSpec spec;
  ASSERT_TRUE(MakeCedarParquetSortLowerBound(
                  Slice(V2UserKey(1)), &spec.sort_key_lower.emplace())
                  .ok());
  spec.projection = {CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  ASSERT_TRUE(reader->ScanProjected(
      spec, [&entities](const CedarParquetColumnarBatch& batch) {
        const auto& values = std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), values.begin(), values.end());
        return Status::OK();
      }).ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({1, 2}));
}

TEST(CedarParquetTableReaderTest, MultiGetReusesCanonicalPages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 5), InternalKeyFor(1, 4),
                                         InternalKeyFor(2, 3), InternalKeyFor(3, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  const std::array<std::string, 3> user_key_bytes = {V2UserKey(1), V2UserKey(1),
                                                       V2UserKey(2)};
  const std::array<Slice, 3> user_keys = {Slice(user_key_bytes[0]),
                                          Slice(user_key_bytes[1]),
                                          Slice(user_key_bytes[2])};
  std::array<PinnableSlice, 3> output_values;
  std::array<Status, 3> statuses;
  std::vector<GetContext> contexts;
  contexts.reserve(user_keys.size());
  for (const Slice& user_key : user_keys) {
    contexts.emplace_back(options.comparator, nullptr, nullptr, nullptr,
                          GetContext::kNotFound, user_key, &output_values[contexts.size()],
                          nullptr, nullptr, nullptr, true, nullptr, nullptr);
  }
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_contexts;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  for (size_t index = 0; index < user_keys.size(); ++index) {
    key_contexts.emplace_back(nullptr, user_keys[index], &output_values[index], nullptr,
                              nullptr, &statuses[index]);
    key_contexts[index].ukey_without_ts = user_keys[index];
    key_contexts[index].get_context = &contexts[index];
    sorted_keys.push_back(&key_contexts[index]);
  }
  MultiGetContext multi_get_context(&sorted_keys, 0, sorted_keys.size(), 100,
                                    ReadOptions(), options.env->GetFileSystem().get(),
                                    options.statistics.get());
  MultiGetContext::Range range = multi_get_context.GetMultiGetRange();

  reader->ResetPageDecodeCountForTesting();
  reader->MultiGet(ReadOptions(), &range, nullptr);

  for (size_t index = 0; index < user_keys.size(); ++index) {
    ASSERT_TRUE(statuses[index].ok()) << statuses[index].ToString();
    EXPECT_EQ(contexts[index].State(), GetContext::kFound);
  }
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 2U);
}

TEST(CedarParquetTableReaderTest, ReusesCanonicalPageAcrossGets) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  reader->ResetPageDecodeCountForTesting();
  for (size_t index = 0; index < 2; ++index) {
    PinnableSlice value;
    const std::string user_key = V2UserKey(1);
    GetContext context(options.comparator, nullptr, nullptr, nullptr,
                       GetContext::kNotFound, user_key, &value, nullptr, nullptr,
                       nullptr, true, nullptr, nullptr);
    ASSERT_TRUE(reader->Get(ReadOptions(), keys[0], &context, nullptr).ok());
    ASSERT_EQ(context.State(), GetContext::kFound);
    EXPECT_EQ(value.ToString(), values[0]);
  }
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 1U);
}

TEST(CedarParquetTableReaderTest, BloomRejectsMissingUserKeyBeforePageDecode) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(3, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  PinnableSlice value;
  const std::string user_key = V2UserKey(2);
  const std::string lookup = InternalKeyFor(2, 3);
  GetContext context(options.comparator, nullptr, nullptr, nullptr,
                     GetContext::kNotFound, user_key, &value, nullptr, nullptr,
                     nullptr, true, nullptr, nullptr);
  reader->ResetPageDecodeCountForTesting();

  ASSERT_TRUE(reader->Get(ReadOptions(), lookup, &context, nullptr).ok());
  EXPECT_EQ(context.State(), GetContext::kNotFound);
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 0U);
}

TEST(CedarParquetTableReaderTest, BloomHasNoFalseNegativesAcrossPersistedFixture) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(64);
  values.reserve(64);
  for (uint64_t entity_id = 1; entity_id <= 64; ++entity_id) {
    keys.push_back(InternalKeyFor(entity_id, 100 - entity_id));
    values.push_back(StateFactValue());
  }
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  for (size_t index = 0; index < keys.size(); ++index) {
    PinnableSlice value;
    const std::string user_key = V2UserKey(index + 1);
    GetContext context(options.comparator, nullptr, nullptr, nullptr,
                       GetContext::kNotFound, user_key, &value, nullptr, nullptr,
                       nullptr, true, nullptr, nullptr);
    ASSERT_TRUE(reader->Get(ReadOptions(), keys[index], &context, nullptr).ok());
    EXPECT_EQ(context.State(), GetContext::kFound) << "entity " << index + 1;
    EXPECT_EQ(value.ToString(), values[index]);
  }
}

TEST(CedarParquetTableReaderTest, RejectsFooterLargerThanConfiguredMemoryLimit) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  parquet_options.max_footer_bytes = 1;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsIndexLargerThanConfiguredMemoryLimit) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  parquet_options.max_index_bytes = 1;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsPageLargerThanConfiguredMemoryLimit) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  parquet_options.page_max_bytes = 1;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest,
     VerifyChecksumRejectsValueLargerThanConfiguredRowLimit) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  parquet_options.max_row_bytes = 106;
  std::unique_ptr<CedarParquetTableReader> reader;
  ASSERT_TRUE(CedarParquetTableReader::Open(
                  comparator, std::move(reader_file), file_size, parquet_options, &reader)
                  .ok());
  EXPECT_TRUE(reader->VerifyChecksum(ReadOptions(), TableReaderCaller::kUserGet)
                  .IsCorruption());
}

TEST(CedarParquetTableReaderTest, RejectsBloomLargerThanConfiguredMemoryLimit) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (uint64_t entity_id = 1; entity_id <= 64; ++entity_id) {
    keys.push_back(InternalKeyFor(entity_id, 100 - entity_id));
    values.push_back(StateFactValue());
  }
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator,
                         64, 64, 64 * 1024), nullptr);

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string bytes((std::istreambuf_iterator<char>(input)),
                          std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(bytes, &footer, nullptr).ok());
  uint64_t index_cap = 0;
  for (const auto& column : footer.row_groups[0].columns) {
    index_cap = std::max<uint64_t>(
        index_cap, std::max<int32_t>(column.offset_index_length,
                                     column.column_index_length));
  }
  ASSERT_GT(footer.row_groups[0].columns[0].bloom_filter_length, index_cap);
  CedarParquetTableOptions parquet_options;
  parquet_options.max_index_bytes = index_cap;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, MultiGetBloomRejectsMissingUserKeyBeforePageDecode) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(3, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  const std::string user_key = V2UserKey(2);
  const Slice user_key_slice(user_key);
  PinnableSlice value;
  Status status;
  GetContext context(options.comparator, nullptr, nullptr, nullptr,
                     GetContext::kNotFound, user_key_slice, &value, nullptr, nullptr,
                     nullptr, true, nullptr, nullptr);
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_contexts;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  key_contexts.emplace_back(nullptr, user_key_slice, &value, nullptr, nullptr, &status);
  key_contexts[0].ukey_without_ts = user_key_slice;
  key_contexts[0].get_context = &context;
  sorted_keys.push_back(&key_contexts[0]);
  MultiGetContext multi_get_context(&sorted_keys, 0, sorted_keys.size(), 100,
                                    ReadOptions(), options.env->GetFileSystem().get(),
                                    options.statistics.get());
  MultiGetContext::Range range = multi_get_context.GetMultiGetRange();

  reader->ResetPageDecodeCountForTesting();
  reader->MultiGet(ReadOptions(), &range, nullptr);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(context.State(), GetContext::kNotFound);
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 0U);
}

TEST(CedarParquetTableReaderTest, RejectsCorruptBloomHeaderWhenOpeningTable) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string bytes((std::istreambuf_iterator<char>(input)),
                          std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(bytes, &footer, nullptr).ok());
  const auto& bloom = footer.row_groups[0].columns[0];
  ASSERT_GT(bloom.bloom_filter_offset, 0);
  std::fstream corrupt(TestPath(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(corrupt.good());
  corrupt.seekp(bloom.bloom_filter_offset);
  corrupt.put(static_cast<char>(0));
  corrupt.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  parquet_options.row_group_max_rows = 2;
  parquet_options.page_max_rows = 1;
  parquet_options.row_group_max_bytes = 4096;
  parquet_options.max_row_bytes = 512;
  std::unique_ptr<CedarParquetTableReader> rejected;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, parquet_options, &rejected);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(rejected, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsOverlappingOffsetIndexPages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file_bytes((std::istreambuf_iterator<char>(input)),
                               std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file_bytes, &footer, nullptr).ok());
  const auto& sort_column = footer.row_groups[0].columns[0];
  std::vector<CedarParquetFooter::ColumnChunk::PageLocation> locations;
  const std::string encoded_index = file_bytes.substr(
      static_cast<size_t>(sort_column.offset_index_offset),
      static_cast<size_t>(sort_column.offset_index_length));
  ASSERT_TRUE(DecodeOffsetIndex(encoded_index, &locations).ok());
  ASSERT_EQ(locations.size(), 2U);
  ASSERT_GT(locations[0].compressed_page_size, 1);
  locations[1].offset = locations[0].offset + locations[0].compressed_page_size - 1;
  std::string overlapping_index;
  ASSERT_TRUE(EncodeOffsetIndex(locations, &overlapping_index).ok());
  ASSERT_EQ(overlapping_index.size(), encoded_index.size());
  std::fstream output(TestPath(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(output.good());
  output.seekp(sort_column.offset_index_offset);
  output.write(overlapping_index.data(),
               static_cast<std::streamsize>(overlapping_index.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsOffsetIndexOverlappingDataPage) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  auto& sort_column = footer.row_groups[0].columns[0];
  const std::string offset_index = file.substr(
      static_cast<size_t>(sort_column.offset_index_offset),
      static_cast<size_t>(sort_column.offset_index_length));
  ASSERT_LE(offset_index.size(),
            static_cast<size_t>(sort_column.total_compressed_size));
  const uint64_t data_page_offset =
      static_cast<uint64_t>(sort_column.data_page_offset);
  sort_column.offset_index_offset = sort_column.data_page_offset;

  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.replace(static_cast<size_t>(data_page_offset), offset_index.size(), offset_index);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsFooterWithoutCedarFileIdentity) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  footer.key_value_metadata.clear();
  footer.created_by.clear();
  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsFooterWithEmptyRowGroups) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  footer.row_groups.clear();
  footer.num_rows = 0;
  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsFooterWithRowGroupFileOffsetOutsideData) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  ASSERT_FALSE(footer.row_groups[0].columns.empty());
  footer.row_groups[0].file_offset =
      footer.row_groups[0].columns[0].offset_index_offset;
  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsOutOfOrderRowGroupMetadata) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator,
                         /*row_group_max_rows=*/1),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  ASSERT_EQ(footer.row_groups.size(), 2U);
  std::swap(footer.row_groups[0], footer.row_groups[1]);
  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsOutOfOrderSortKeyPageIndex) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, nullptr).ok());
  const auto& sort_column = footer.row_groups[0].columns[0];
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> page_indexes;
  ASSERT_TRUE(DecodeColumnIndex(
                  file.substr(static_cast<size_t>(sort_column.column_index_offset),
                              static_cast<size_t>(sort_column.column_index_length)),
                  &page_indexes)
                  .ok());
  ASSERT_EQ(page_indexes.size(), 2U);
  std::swap(page_indexes[0], page_indexes[1]);
  std::string corrupt_index;
  ASSERT_TRUE(EncodeColumnIndex(page_indexes, &corrupt_index).ok());
  ASSERT_EQ(corrupt_index.size(),
            static_cast<size_t>(sort_column.column_index_length));
  std::fstream output(TestPath(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(output.good());
  output.seekp(sort_column.column_index_offset);
  output.write(corrupt_index.data(), static_cast<std::streamsize>(corrupt_index.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest, RejectsFooterWithMismatchedSchemaDigest) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  bool changed = false;
  for (auto& metadata : footer.key_value_metadata) {
    if (metadata.key == "cedar.canonical_schema") {
      metadata.value = std::string(64, '0');
      changed = true;
    }
  }
  ASSERT_TRUE(changed);
  std::string rewritten_footer;
  ASSERT_TRUE(EncodeCompactFooter(footer, &rewritten_footer).ok());
  std::string corrupt = file.substr(0, footer_offset);
  corrupt.append(rewritten_footer);
  AppendLittleEndian32(&corrupt, static_cast<uint32_t>(rewritten_footer.size()));
  corrupt.append("PAR1");
  std::ofstream output(TestPath(), std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.good());
  output.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  const Status status = CedarParquetTableReader::Open(
      comparator, std::move(reader_file), file_size, parquet_options, &reader);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_EQ(reader, nullptr);
}

TEST(CedarParquetTableReaderTest,
     VerifyChecksumRejectsMaterializedColumnIndexThatDisagreesWithData) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  ASSERT_NE(BuildAndOpen(keys, values, immutable_options, mutable_options, comparator),
            nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, nullptr).ok());
  const auto& part_id = footer.row_groups[0].columns[3];
  const std::string encoded_index = file.substr(
      static_cast<size_t>(part_id.column_index_offset),
      static_cast<size_t>(part_id.column_index_length));
  std::vector<CedarParquetFooter::ColumnChunk::PageIndex> page_indexes;
  ASSERT_TRUE(DecodeColumnIndex(encoded_index, &page_indexes).ok());
  ASSERT_EQ(page_indexes.size(), 1U);
  ASSERT_FALSE(page_indexes[0].all_null);
  page_indexes[0].min_value = std::string(4, static_cast<char>(0xff));
  page_indexes[0].max_value = page_indexes[0].min_value;
  std::string corrupt_index;
  ASSERT_TRUE(EncodeColumnIndex(page_indexes, &corrupt_index).ok());
  ASSERT_EQ(corrupt_index.size(), encoded_index.size());
  std::fstream output(TestPath(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(output.good());
  output.seekp(part_id.column_index_offset);
  output.write(corrupt_index.data(), static_cast<std::streamsize>(corrupt_index.size()));
  output.close();

  uint64_t file_size = 0;
  ASSERT_TRUE(Env::Default()->GetFileSize(TestPath(), &file_size).ok());
  std::unique_ptr<RandomAccessFileReader> reader_file;
  ASSERT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                              FileOptions(), &reader_file, nullptr)
                  .ok());
  CedarParquetTableOptions parquet_options;
  std::unique_ptr<CedarParquetTableReader> reader;
  ASSERT_TRUE(CedarParquetTableReader::Open(
                  comparator, std::move(reader_file), file_size, parquet_options, &reader)
                  .ok());
  EXPECT_TRUE(reader->VerifyChecksum(ReadOptions(), TableReaderCaller::kUserGet)
                  .IsCorruption());
}

TEST(CedarParquetTableReaderTest, GetsTargetWithoutDecodingUnrelatedKeyPage) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4), InternalKeyFor(2, 3)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, nullptr).ok());
  const auto& sort_column = footer.row_groups[0].columns[0];
  std::vector<CedarParquetFooter::ColumnChunk::PageLocation> page_locations;
  ASSERT_TRUE(DecodeOffsetIndex(
                  file.substr(static_cast<size_t>(sort_column.offset_index_offset),
                              static_cast<size_t>(sort_column.offset_index_length)),
                  &page_locations)
                  .ok());
  ASSERT_EQ(page_locations.size(), 2U);
  const auto& second_page = page_locations[1];
  std::fstream output(TestPath(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(output.good());
  output.seekg(second_page.offset + second_page.compressed_page_size - 1);
  char byte = 0;
  output.get(byte);
  output.seekp(second_page.offset + second_page.compressed_page_size - 1);
  output.put(static_cast<char>(byte ^ 1));
  output.close();

  PinnableSlice value;
  const std::string lookup = InternalKeyFor(1, 4);
  const std::string user_key = V2UserKey(1);
  GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, user_key, &value, nullptr, nullptr,
                         nullptr, true, nullptr, nullptr);
  ASSERT_TRUE(reader->Get(ReadOptions(), lookup, &get_context, nullptr).ok());
  EXPECT_EQ(get_context.State(), GetContext::kFound);
  EXPECT_EQ(value.ToString(), values[0]);
}

TEST(CedarParquetTableReaderTest,
     VerifyChecksumRejectsMaterializedValueThatDisagreesWithCanonicalFact) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4)};
  const std::vector<std::string> values = {StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string file((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, nullptr).ok());
  const auto& part_id = footer.row_groups[0].columns[3];
  std::vector<CedarParquetFooter::ColumnChunk::PageLocation> page_locations;
  ASSERT_TRUE(DecodeOffsetIndex(
                  file.substr(static_cast<size_t>(part_id.offset_index_offset),
                              static_cast<size_t>(part_id.offset_index_length)),
                  &page_locations)
                  .ok());
  ASSERT_EQ(page_locations.size(), 1U);
  const auto& page_location = page_locations[0];
  std::vector<std::optional<std::string>> decoded;
  ASSERT_TRUE(DecodePlainPrimitiveDataPage(
                  file.substr(static_cast<size_t>(page_location.offset),
                              static_cast<size_t>(page_location.compressed_page_size)),
                  part_id.physical_type, true, &decoded, nullptr)
                  .ok());
  ASSERT_EQ(decoded.size(), 1U);
  ASSERT_TRUE(decoded[0].has_value());
  decoded[0] = std::string(4, '\0');
  (*decoded[0])[0] = 1;
  std::string replacement;
  ASSERT_TRUE(EncodePlainPrimitiveDataPage(decoded, part_id.physical_type, true,
                                           &replacement)
                  .ok());
  ASSERT_EQ(replacement.size(), static_cast<size_t>(page_location.compressed_page_size));
  std::fstream output(TestPath(), std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(output.good());
  output.seekp(page_location.offset);
  output.write(replacement.data(), static_cast<std::streamsize>(replacement.size()));
  output.close();

  EXPECT_TRUE(reader->VerifyChecksum(ReadOptions(), TableReaderCaller::kUserGet)
                  .IsCorruption());
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
