// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <string>
#include <fstream>
#include <limits>
#include <optional>
#include <mutex>
#include <unistd.h>
#include <vector>
#include <thread>

#include <gtest/gtest.h>

#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cedar_commit.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "table/cedar_parquet/cedar_parquet_table_builder.h"
#include "table/cedar_parquet/cedar_parquet_table_reader.h"
#include "table/cedar_parquet/parquet_plain_page.h"
#include "table/multiget_context.h"
#include "util/crc32c.h"
#include "util/random.h"

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

struct FileReadTrace {
  std::mutex mutex;
  std::vector<std::pair<uint64_t, size_t>> requests;
  void Reset() {
    std::lock_guard<std::mutex> lock(mutex);
    requests.clear();
  }
};

class TracedRandomAccessFile final : public FSRandomAccessFileWrapper {
 public:
  TracedRandomAccessFile(std::unique_ptr<FSRandomAccessFile> file,
                         std::shared_ptr<FileReadTrace> trace)
      : FSRandomAccessFileWrapper(file.get()), file_(std::move(file)),
        trace_(std::move(trace)) {}
  IOStatus Read(uint64_t offset, size_t bytes, const IOOptions& options,
                Slice* result, char* scratch, IODebugContext* debug) const override {
    {
      std::lock_guard<std::mutex> lock(trace_->mutex);
      trace_->requests.emplace_back(offset, bytes);
    }
    return file_->Read(offset, bytes, options, result, scratch, debug);
  }
 private:
  std::unique_ptr<FSRandomAccessFile> file_;
  std::shared_ptr<FileReadTrace> trace_;
};

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
    uint32_t page_max_rows = 1, uint64_t row_group_max_bytes = 4096,
    CedarParquetReadResources resources = {NewLRUCache(1<<20),
        std::make_shared<CanonicalPageCacheMetrics>()},
    std::shared_ptr<FileReadTrace> trace = {}) {
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
  if (trace) {
    std::unique_ptr<FSRandomAccessFile> raw;
    EXPECT_TRUE(Env::Default()->GetFileSystem()->NewRandomAccessFile(
        TestPath(), FileOptions(), &raw, nullptr).ok());
    std::unique_ptr<FSRandomAccessFile> traced =
        std::make_unique<TracedRandomAccessFile>(std::move(raw), std::move(trace));
    file = std::make_unique<RandomAccessFileReader>(std::move(traced), TestPath());
  } else {
    EXPECT_TRUE(RandomAccessFileReader::Create(Env::Default()->GetFileSystem(), TestPath(),
                                                FileOptions(), &file, nullptr).ok());
  }
  std::unique_ptr<CedarParquetTableReader> reader;
  Status open_status = CedarParquetTableReader::Open(comparator, std::move(file), file_size,
                                                      options, &reader, std::move(resources));
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

void PutBigEndian32(std::string* destination, size_t offset, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    (*destination)[offset++] = static_cast<char>(value >> shift);
  }
}

// State-family fact key: [version=2][part][family][property=0][entity]
// [~valid_from][~commit_seq]. Entity ids in tests are unique, so bytewise user
// key order matches creation order after sorting.
std::string V2FactInternalKey(uint32_t part, uint8_t family, uint64_t entity,
                              uint64_t valid_from, uint64_t cedar_commit_seq,
                              SequenceNumber sequence) {
  std::string user_key(32, '\0');
  user_key[0] = 2;
  PutBigEndian32(&user_key, 1, part);
  user_key[5] = static_cast<char>(family);
  StoreBigEndian64(&user_key, 8, entity);
  StoreBigEndian64(&user_key, 16, ~valid_from);
  StoreBigEndian64(&user_key, 24, ~cedar_commit_seq);
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(user_key, sequence, kTypeValue));
  return internal_key;
}

// Builds the internal key the database layer hands to the table iterator for a
// user-key seek: the encoded prefix-seek user key plus the usual 8-byte
// internal-key trailer.
std::string PrefixSeekInternalKey(std::string_view prefix,
                                  SequenceNumber sequence = kMaxSequenceNumber) {
  const std::string encoded = EncodeCedarParquetPrefixSeekKey(prefix);
  EXPECT_EQ(encoded.size(), kCedarParquetV2UserKeyBytes);
  std::string internal_key;
  AppendInternalKey(&internal_key,
                    ParsedInternalKey(Slice(encoded), sequence,
                                      kValueTypeForSeek));
  return internal_key;
}

// Position a prefix seek must reach: the first key whose user key is at or
// after the zero-padded minimal user key of the prefix range. Returns
// keys.size() when every key sorts before the prefix.
size_t ExpectedPrefixSeekIndex(const std::vector<std::string>& keys,
                               std::string_view prefix) {
  std::string minimal(prefix);
  minimal.resize(kCedarParquetV2UserKeyBytes, '\0');
  for (size_t index = 0; index < keys.size(); ++index) {
    if (keys[index].compare(0, kCedarParquetV2UserKeyBytes, minimal) >= 0) {
      return index;
    }
  }
  return keys.size();
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
  // Keys are slices into the decoded page, which a later reposition can
  // replace, so callers must copy them. Values keep their page alive.
  EXPECT_FALSE(iterator->IsKeyPinned());
  EXPECT_TRUE(iterator->IsValuePinned());
  iterator->Prev();
  EXPECT_EQ(iterator->key(), keys[2]);
  EXPECT_TRUE(iterator->status().ok());
  EXPECT_EQ(reader->GetTableProperties()->num_entries, keys.size());
}

TEST(CedarParquetTableReaderTest,
     SeekOrdersSameUserKeyVersionsByBytewiseSortKey) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  // One user key with four versions in one page. InternalKeyComparator order is
  // descending sequence, then descending type number (kTypeValue=1 before
  // kTypeDeletion=0 at the same sequence); the sort-key trailer is the inverted
  // packed (sequence,type), so its bytewise order must be the same.
  const std::vector<std::string> keys = {
      InternalKeyFor(9, 30, kTypeValue), InternalKeyFor(9, 20, kTypeValue),
      InternalKeyFor(9, 20, kTypeDeletion), InternalKeyFor(9, 10, kTypeValue)};
  std::vector<std::string> values(keys.size(), StateFactValue());
  values[2].clear();  // deletion rows carry no encoded value
  std::vector<std::string> sort_keys;
  sort_keys.reserve(keys.size());
  for (const std::string& key : keys) sort_keys.push_back(SortKeyFor(key));

  // Premise of the F7 Seek implementation: bytewise sort-key order equals
  // InternalKeyComparator order, including versions that differ only by
  // sequence/type. A one-row-per-page fixture cannot observe this because every
  // page search is trivial there.
  for (size_t left = 0; left < keys.size(); ++left) {
    for (size_t right = 0; right < keys.size(); ++right) {
      if (left == right) continue;
      EXPECT_EQ(comparator.Compare(keys[left], keys[right]) < 0,
                sort_keys[left] < sort_keys[right])
          << left << " vs " << right;
    }
  }

  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator,
      /*row_group_max_rows=*/4, /*page_max_rows=*/4);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  reader->ResetPageDecodeCountForTesting();
  for (size_t index = 0; index < keys.size(); ++index) {
    iterator->Seek(keys[index]);
    ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
    ASSERT_TRUE(iterator->Valid()) << index;
    EXPECT_EQ(iterator->key().ToString(), keys[index]) << "seek " << index;
    EXPECT_EQ(iterator->value().ToString(), values[index]) << "value " << index;
    // The first Seek decodes the single four-row page; every later Seek must
    // locate its row in that cached page, so the search really is intra-page.
    EXPECT_EQ(reader->PageDecodeCountForTesting(), 1U) << "seek " << index;
  }
  for (size_t index = 0; index < keys.size(); ++index) {
    iterator->SeekForPrev(keys[index]);
    ASSERT_TRUE(iterator->Valid()) << index;
    EXPECT_EQ(iterator->key().ToString(), keys[index]) << "seek-for-prev " << index;
  }
}

TEST(CedarParquetTableReaderTest, FileAndMetadataMetricsFollowOwnedResources) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto trace = std::make_shared<FileReadTrace>();
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096,
      {NewLRUCache(1 << 20), metrics}, trace);
  ASSERT_NE(reader, nullptr);
  EXPECT_EQ(metrics->reader_metadata_bytes.load(), reader->ApproximateMemoryUsage());
  EXPECT_GT(metrics->reader_metadata_bytes.load(), sizeof(CedarParquetTableReader));
  const auto first_charge = reader->ApproximateMemoryUsage();
  EXPECT_EQ(metrics->reader_metadata_peak_bytes.load(), first_charge);
  auto second_reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096,
      {NewLRUCache(1 << 20), metrics});
  ASSERT_NE(second_reader, nullptr);
  const auto combined = first_charge + second_reader->ApproximateMemoryUsage();
  EXPECT_EQ(metrics->reader_metadata_bytes.load(), combined);
  EXPECT_EQ(metrics->reader_metadata_peak_bytes.load(), combined);
  second_reader.reset();
  EXPECT_EQ(metrics->reader_metadata_bytes.load(), first_charge);
  EXPECT_EQ(metrics->reader_metadata_peak_bytes.load(), combined);
  trace->Reset();
  const auto calls_before = metrics->file_read_calls.load();
  const auto bytes_before = metrics->file_read_bytes.load();
  CedarParquetScanSpec spec;
  spec.policy.admit = true;
  spec.projection = {CedarParquetColumnId::kEntityId};
  ASSERT_TRUE(reader->ScanProjected(spec, [](const auto&) { return Status::OK(); }).ok());
  uint64_t requested_bytes = 0;
  for (const auto& request : trace->requests) requested_bytes += request.second;
  EXPECT_EQ(metrics->file_read_bytes.load() - bytes_before, requested_bytes);
  EXPECT_GT(metrics->file_read_calls.load(), calls_before);
  const auto bytes_warm = metrics->file_read_bytes.load();
  ASSERT_TRUE(reader->ScanProjected(spec, [](const auto&) { return Status::OK(); }).ok());
  EXPECT_EQ(metrics->file_read_bytes.load(), bytes_warm);
  reader.reset();
  EXPECT_EQ(metrics->reader_metadata_bytes.load(), 0U);
  EXPECT_EQ(metrics->reader_metadata_peak_bytes.load(), combined);
}

TEST(CedarParquetTableReaderTest, MetadataAdmissionOwnsOpenScratchAndReaderLifetime) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto measured = std::make_shared<CedarParquetReadBudget>(
      1 << 20, CedarParquetReadBudgetKind::kMetadata);
  auto working = std::make_shared<CedarParquetReadBudget>(1);
  auto first = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096,
      {{}, {}, working, measured});
  ASSERT_NE(first, nullptr);
  EXPECT_GT(measured->current_bytes(), first->ApproximateMemoryUsage());
  EXPECT_GT(measured->peak_bytes(), measured->current_bytes());
  EXPECT_EQ(working->current_bytes(), 0U);
  const uint64_t peak = measured->peak_bytes();
  first.reset();
  EXPECT_EQ(measured->current_bytes(), 0U);
  auto budget = std::make_shared<CedarParquetReadBudget>(
      peak, CedarParquetReadBudgetKind::kMetadata);
  const auto open = [&](std::unique_ptr<CedarParquetTableReader>* result) {
    uint64_t size = 0;
    Status status = Env::Default()->GetFileSize(TestPath(), &size);
    if (!status.ok()) return status;
    std::unique_ptr<RandomAccessFileReader> file;
    status = RandomAccessFileReader::Create(Env::Default()->GetFileSystem(),
        TestPath(), FileOptions(), &file, nullptr);
    if (!status.ok()) return status;
    CedarParquetTableOptions table_options;
    return CedarParquetTableReader::Open(comparator, std::move(file), size,
        table_options, result, {{}, {}, working, budget});
  };
  ASSERT_TRUE(open(&first).ok());
  const auto retained = budget->current_bytes();
  std::unique_ptr<CedarParquetTableReader> second;
  EXPECT_TRUE(open(&second).IsMemoryLimit());
  EXPECT_EQ(second, nullptr);
  EXPECT_EQ(budget->current_bytes(), retained);
  EXPECT_LE(budget->peak_bytes(), budget->limit_bytes());
  EXPECT_GT(budget->rejections(), 0U);
  first.reset();
  EXPECT_EQ(budget->current_bytes(), 0U);
  ASSERT_TRUE(open(&second).ok());
  second.reset();
  EXPECT_EQ(budget->current_bytes(), 0U);
  EXPECT_EQ(working->current_bytes(), 0U);
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

TEST(CedarParquetTableReaderTest, RepeatedAdmittedColumnScanReusesDecodedPages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto reader = BuildAndOpen({InternalKeyFor(1, 4), InternalKeyFor(2, 3)},
      {StateFactValue(), StateFactValue()}, immutable_options, mutable_options,
      comparator, 2, 1, 4096, {cache, metrics});
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.policy.admit = true;
  std::vector<uint64_t> entities;
  const auto consume = [&](const CedarParquetColumnarBatch& batch) {
    const auto& column = std::get<std::vector<uint64_t>>(batch.columns[0].values);
    entities.insert(entities.end(), column.begin(), column.end());
    return Status::OK();
  };
  ASSERT_TRUE(reader->ScanProjected(spec, consume).ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({1, 2}));
  EXPECT_GT(cache->GetUsage(), 0U);
  EXPECT_GT(metrics->resident_charge_bytes.load(), 0U);
  const uint64_t hits = metrics->hits.load();
  const uint64_t misses = metrics->misses.load();
  const uint64_t decode_ns = metrics->decode_nanoseconds.load();
  uint64_t admitted = 0;
  for (const auto& column : metrics->columns) admitted += column.admissions.load();
  EXPECT_GT(admitted, 0U);
  spec.policy.options.read_tier = kBlockCacheTier;
  entities.clear();
  const Status warm = reader->ScanProjected(spec, consume);
  EXPECT_TRUE(warm.ok()) << warm.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({1, 2}));
  EXPECT_GT(metrics->hits.load(), hits);
  EXPECT_EQ(metrics->misses.load(), misses);
  EXPECT_EQ(metrics->decode_nanoseconds.load(), decode_ns);
  uint64_t column_hits = 0;
  for (const auto& column : metrics->columns) column_hits += column.hits.load();
  EXPECT_EQ(column_hits, metrics->hits.load());
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 0U);
}

TEST(CedarParquetTableReaderTest, WarmProjectedCursorUsesAdmittedColumnPages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto reader = BuildAndOpen({InternalKeyFor(1, 4), InternalKeyFor(2, 3)},
      {StateFactValue(), StateFactValue()}, immutable_options, mutable_options,
      comparator, 2, 1, 4096, {cache, metrics});
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.policy.admit = true;
  ASSERT_TRUE(reader->ScanProjected(spec, [](const CedarParquetColumnarBatch&) {
    return Status::OK();
  }).ok());
  spec.policy.options.read_tier = kBlockCacheTier;
  std::unique_ptr<CedarParquetProjectedCursor> cursor;
  const Status status = reader->NewProjectedCursor(spec, &cursor);
  ASSERT_TRUE(status.ok()) << status.ToString();
  size_t rows = 0;
  while (cursor->Valid()) {
    ++rows;
    cursor->Next();
  }
  EXPECT_TRUE(cursor->status().ok()) << cursor->status().ToString();
  EXPECT_EQ(rows, 2U);
}

TEST(CedarParquetTableReaderTest, CanonicalAndColumnScanSharePayloadOwners) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  const auto key = InternalKeyFor(1, 4);
  auto reader = BuildAndOpen({key}, {StateFactValue()}, immutable_options,
      mutable_options, comparator, 2, 1, 4096, {cache, metrics});
  std::unique_ptr<InternalIterator> canonical(reader->NewIterator(ReadOptions(),
      nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  canonical->SeekToFirst();
  ASSERT_TRUE(canonical->Valid());
  const auto resident = metrics->resident_charge_bytes.load();
  const auto usage = cache->GetUsage();
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEncodedValue};
  spec.policy.admit = true;
  spec.policy.options.read_tier = kBlockCacheTier;
  size_t rows = 0;
  const auto status = reader->ScanProjected(spec, [&](const CedarParquetColumnarBatch& batch) {
    rows += batch.row_count();
    EXPECT_EQ(batch.encoded_values, std::vector<std::string>({StateFactValue()}));
    return Status::OK();
  });
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(rows, 1U);
  EXPECT_EQ(metrics->resident_charge_bytes.load(), resident);
  EXPECT_EQ(cache->GetUsage(), usage);
  EXPECT_EQ(canonical->value().ToString(), StateFactValue());
}

TEST(CedarParquetTableReaderTest, PredicateProjectionLoadsColumnOnce) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto reader = BuildAndOpen({InternalFactKeyFor(1, 10, 20, 4)},
      {StateFactValue()}, immutable_options, mutable_options, comparator,
      2, 1, 4096, {{}, metrics});
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kValidFrom,
                     CedarParquetColumnId::kCedarCommitSeq};
  spec.valid_from_min = 10;
  spec.cedar_commit_seq_max = 20;
  size_t rows = 0;
  auto status = reader->ScanProjected(spec, [&](const CedarParquetColumnarBatch& batch) {
    rows += batch.row_count();
    EXPECT_EQ(std::get<std::vector<uint64_t>>(batch.columns[0].values),
              std::vector<uint64_t>({10}));
    EXPECT_EQ(std::get<std::vector<uint64_t>>(batch.columns[1].values),
              std::vector<uint64_t>({20}));
    return Status::OK();
  });
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(rows, 1U);
  // Sort-key, valid-time and commit-sequence pages each load once even without
  // admission; stored internal keys are temporary validation, not cached pages.
  EXPECT_EQ(metrics->misses.load(), 3U);
  uint64_t total_ns = 0;
  for (size_t column = 0; column < metrics->columns.size(); ++column) {
    const auto& counters = metrics->columns[column];
    total_ns += counters.decode_nanoseconds.load();
    const bool loaded = column == 0 || column == 7 || column == 8;
    EXPECT_EQ(counters.misses.load(), loaded ? 1U : 0U);
    EXPECT_EQ(counters.decoded_pages.load(), loaded ? 1U : 0U);
    EXPECT_EQ(counters.decode_attempts.load(), loaded || column == 1 ? 1U : 0U);
    EXPECT_EQ(counters.admissions.load(), 0U);
    EXPECT_EQ(counters.admission_rejections.load(), 0U);
    if (loaded) EXPECT_GT(counters.decoded_bytes.load(), 0U);
  }
  EXPECT_GT(total_ns, 0U);
  EXPECT_EQ(metrics->decode_nanoseconds.load(), total_ns);
  spec.valid_from_min = 11;
  rows = 0;
  EXPECT_TRUE(reader->ScanProjected(spec, [&](const CedarParquetColumnarBatch& batch) {
    rows += batch.row_count();
    return Status::OK();
  }).ok());
  EXPECT_EQ(rows, 0U);
}

TEST(CedarParquetTableReaderTest, FileBytesCountOnlyActualRequestedColumns) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto trace = std::make_shared<FileReadTrace>();
  auto reader = BuildAndOpen({InternalFactKeyFor(1, 10, 20, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096, {cache, {}}, trace);
  std::ifstream input(TestPath(), std::ios::binary);
  const std::string file((std::istreambuf_iterator<char>(input)), {});
  CedarParquetFooter footer;
  size_t footer_offset = 0;
  ASSERT_TRUE(ParseParquetFooter(file, &footer, &footer_offset).ok());
  ASSERT_EQ(footer.row_groups.size(), 1U);
  for (auto& column : footer.row_groups[0].columns) {
    ASSERT_TRUE(DecodeOffsetIndex(file.substr(static_cast<size_t>(column.offset_index_offset),
        static_cast<size_t>(column.offset_index_length)), &column.page_locations).ok());
    ASSERT_EQ(column.page_locations.size(), 1U);
  }
  trace->Reset();
  CedarParquetScanStats stats;
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId, CedarParquetColumnId::kValidFrom};
  spec.valid_from_min = 10;
  spec.policy.admit = true;
  spec.stats = &stats;
  const auto consume = [](const CedarParquetColumnarBatch&) { return Status::OK(); };
  ASSERT_TRUE(reader->ScanProjected(spec, consume).ok());
  uint64_t bytes = 0;
  ASSERT_EQ(trace->requests.size(), 4U);
  for (const auto& request : trace->requests) bytes += request.second;
  EXPECT_EQ(stats.bytes_read, bytes);
  for (size_t column : {size_t{0}, size_t{1},
                        static_cast<size_t>(CedarParquetColumnId::kEntityId), size_t{7}}) {
    const auto& location = footer.row_groups[0].columns[column].page_locations[0];
    EXPECT_EQ(std::count(trace->requests.begin(), trace->requests.end(),
        std::make_pair(static_cast<uint64_t>(location.offset),
                       static_cast<size_t>(location.compressed_page_size))), 1);
  }
  const auto& value_location = footer.row_groups[0].columns[2].page_locations[0];
  for (const auto& request : trace->requests)
    EXPECT_NE(request.first, static_cast<uint64_t>(value_location.offset));
  trace->Reset();
  stats = {};
  ASSERT_TRUE(reader->ScanProjected(spec, consume).ok());
  EXPECT_TRUE(trace->requests.empty());
  EXPECT_EQ(stats.bytes_read, 0U);
}

TEST(CedarParquetTableReaderTest, PartialCacheOnlyProjectionNeverReadsMissingColumn) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto trace = std::make_shared<FileReadTrace>();
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096, {cache, {}}, trace);
  std::unique_ptr<InternalIterator> canonical(reader->NewIterator(ReadOptions(),
      nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  canonical->SeekToFirst();
  ASSERT_TRUE(canonical->Valid());
  trace->Reset();
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.policy.options.read_tier = kBlockCacheTier;
  size_t rows = 0;
  const auto status = reader->ScanProjected(spec, [&](const CedarParquetColumnarBatch& batch) {
    rows += batch.row_count();
    return Status::OK();
  });
  EXPECT_TRUE(status.IsIncomplete()) << status.ToString();
  EXPECT_EQ(rows, 0U);
  EXPECT_TRUE(trace->requests.empty());
}

TEST(CedarParquetTableReaderTest, WarmProjectedScanReportsNoFileBytes) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096, {cache, {}});
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.policy.admit = true;
  const auto consume = [](const CedarParquetColumnarBatch&) { return Status::OK(); };
  ASSERT_TRUE(reader->ScanProjected(spec, consume).ok());
  CedarParquetScanStats stats;
  spec.stats = &stats;
  ASSERT_TRUE(reader->ScanProjected(spec, consume).ok());
  EXPECT_EQ(stats.bytes_read, 0U);
}

TEST(CedarParquetTableReaderTest, ChecksumRejectsBeforeReadWhenBudgetIsFull) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto budget = std::make_shared<CedarParquetReadBudget>(1);
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096, {{}, {}, budget});
  const auto status = reader->VerifyChecksum(ReadOptions(), TableReaderCaller::kUserIterator);
  EXPECT_TRUE(status.IsMemoryLimit()) << status.ToString();
  EXPECT_EQ(budget->current_bytes(), 0U);
  EXPECT_GT(budget->rejections(), 0U);
}

TEST(CedarParquetTableReaderTest, EmptyProjectedCursorReleasesEachFilteredPage) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys, values;
  for (uint64_t entity = 1; entity <= 32; ++entity) {
    keys.push_back(InternalFactKeyFor(entity, entity % 2 ? 1 : 2,
                                     entity % 2 ? 2 : 1, entity));
    values.push_back(StateFactValue());
  }
  auto budget = std::make_shared<CedarParquetReadBudget>(16 << 10);
  auto reader = BuildAndOpen(keys, values, immutable_options, mutable_options,
      comparator, 32, 2, 64 << 10, {{}, {}, budget});
  CedarParquetScanSpec spec;
  spec.projection = {CedarParquetColumnId::kEntityId};
  spec.valid_from_min = 2;
  spec.cedar_commit_seq_min = 2;
  std::unique_ptr<CedarParquetProjectedCursor> cursor;
  const auto status = reader->NewProjectedCursor(spec, &cursor);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_FALSE(cursor->Valid());
  EXPECT_TRUE(cursor->status().ok()) << cursor->status().ToString();
  EXPECT_EQ(budget->current_bytes(), 0U);
  EXPECT_LE(budget->peak_bytes(), budget->limit_bytes());
  cursor.reset();
  size_t callbacks = 0;
  const auto scanned = reader->ScanProjected(spec, [&](const auto&) {
    ++callbacks;
    return Status::OK();
  });
  EXPECT_TRUE(scanned.ok()) << scanned.ToString();
  EXPECT_EQ(callbacks, 0U);
  EXPECT_EQ(budget->current_bytes(), 0U);
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
  // Two rows per page: request 2 must skip row 0 inside page 0 and derive its
  // internal key from the cached sort-key column (F7 no longer caches the
  // internal-key column), and request 3 must reuse the second decoded page.
  const std::vector<std::string> keys = {InternalKeyFor(1, 5), InternalKeyFor(2, 4),
                                         InternalKeyFor(3, 3), InternalKeyFor(4, 2)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue(),
                                           StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator,
                   /*row_group_max_rows=*/4, /*page_max_rows=*/2);
  ASSERT_NE(reader, nullptr);

  const std::array<std::string, 3> user_key_bytes = {V2UserKey(1), V2UserKey(2),
                                                       V2UserKey(3)};
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
    // The page-cache path must return the value of the matching row, not just
    // report a hit: each request maps to the same-index stored row here.
    EXPECT_EQ(output_values[index].ToString(), values[index]);
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

TEST(CedarParquetTableReaderTest, AlternatingTenPagesReusesSharedCache) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys, values;
  for (uint64_t id = 1; id <= 10; ++id) {
    keys.push_back(InternalKeyFor(id, 11-id));
    values.push_back(StateFactValue());
  }
  auto reader = BuildAndOpen(keys, values, immutable_options, mutable_options,
                             comparator, 10, 1);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  for (const std::string& key : keys) {
    iterator->Seek(key); ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ(iterator->key().ToString(), key);
  }
  reader->ResetPageDecodeCountForTesting();
  for (size_t i = 0; i < 640; ++i) {
    const size_t index = i % keys.size();
    iterator->Seek(keys[index]); ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ(iterator->value().ToString(), values[index]);
  }
  EXPECT_EQ(reader->PageDecodeCountForTesting(), 0U);
}

TEST(CedarParquetTableReaderTest, NoFillReadsWithoutAdmitting) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  const auto key = InternalKeyFor(1, 4);
  auto reader = BuildAndOpen({key}, {StateFactValue()}, immutable_options,
      mutable_options, comparator, 2, 1, 4096, {cache, metrics});
  ReadOptions read;
  read.fill_cache = false;
  {
    std::unique_ptr<InternalIterator> it(reader->NewIterator(
        read, nullptr, nullptr, false, TableReaderCaller::kUserIterator));
    it->Seek(key);
    ASSERT_TRUE(it->status().ok());
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->value().ToString(), StateFactValue());
    EXPECT_EQ(metrics->resident_charge_bytes.load(), 0U);
  }
  EXPECT_EQ(cache->GetUsage(), 0U);
  EXPECT_EQ(metrics->uncached_live_bytes.load(), 0U);
  {
    std::unique_ptr<InternalIterator> warm(reader->NewIterator(
        ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
    warm->Seek(key);
    ASSERT_TRUE(warm->Valid());
  }
  const auto decodes = metrics->decodes.load();
  std::unique_ptr<InternalIterator> hit(reader->NewIterator(
      read, nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  hit->Seek(key);
  ASSERT_TRUE(hit->Valid());
  EXPECT_EQ(metrics->decodes.load(), decodes);
  EXPECT_GT(metrics->hits.load(), 0U);
}

TEST(CedarParquetTableReaderTest, CanonicalReadRejectsBeforeDecodeWhenBudgetIsFull) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto budget = std::make_shared<CedarParquetReadBudget>(1);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096,
      {{}, metrics, budget});
  std::unique_ptr<InternalIterator> it(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  it->SeekToFirst();
  EXPECT_FALSE(it->Valid());
  EXPECT_TRUE(it->status().IsMemoryLimit()) << it->status().ToString();
  EXPECT_EQ(metrics->decodes.load(), 0U);
  EXPECT_EQ(budget->current_bytes(), 0U);
  EXPECT_GT(budget->rejections(), 0U);
}

TEST(CedarParquetTableReaderTest, CanonicalValidationReleasesPriorDecodeScratch) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys, values;
  for (uint64_t id = 1; id <= 1024; ++id) {
    keys.push_back(InternalKeyFor(id, 1));
    values.push_back(StateFactValue());
  }
  auto budget = std::make_shared<CedarParquetReadBudget>(512 << 10);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto reader = BuildAndOpen(keys, values, immutable_options, mutable_options,
      comparator, 1024, 1024, 1 << 20, {{}, metrics, budget});
  ASSERT_NE(reader, nullptr);
  {
    ReadOptions read;
    read.fill_cache = false;
    std::unique_ptr<InternalIterator> it(reader->NewIterator(
        read, nullptr, nullptr, false, TableReaderCaller::kCompaction));
    it->SeekToFirst();
    size_t rows = 0;
    for (; it->Valid(); it->Next()) {
      ASSERT_LT(rows, keys.size());
      EXPECT_EQ(it->key().ToString(), keys[rows]);
      EXPECT_EQ(it->value().ToString(), values[rows]);
      ++rows;
    }
    ASSERT_TRUE(it->status().ok()) << it->status().ToString();
    EXPECT_EQ(rows, 1024U);
    EXPECT_EQ(metrics->resident_charge_bytes.load(), 0U);
    EXPECT_LE(budget->peak_bytes(), 512U << 10);
  }
  EXPECT_EQ(budget->current_bytes(), 0U);
}

TEST(CedarParquetTableReaderTest, UncachedCanonicalOwnerRetainsWorkingLease) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto budget = std::make_shared<CedarParquetReadBudget>(1 << 20);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto cache = NewLRUCache(1, 0, true);
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096,
      {cache, metrics, budget});
  std::unique_ptr<InternalIterator> it(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid()) << it->status().ToString();
  EXPECT_GT(budget->current_bytes(), 0U);
  EXPECT_GE(budget->peak_bytes(), budget->current_bytes());
  EXPECT_EQ(cache->GetUsage(), 0U);
  EXPECT_EQ(it->value().ToString(), StateFactValue());
  it.reset();
  EXPECT_EQ(budget->current_bytes(), 0U);
}

TEST(CedarParquetTableReaderTest, CacheOnlyColdPartialAndWarmIterators) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  const std::vector<std::string> keys{InternalKeyFor(1, 4), InternalKeyFor(2, 4)};
  auto reader = BuildAndOpen(keys, {StateFactValue(), StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096, {cache, metrics});
  ReadOptions read;
  read.read_tier = kBlockCacheTier;
  for (bool reverse : {false, true}) {
    std::unique_ptr<InternalIterator> cold(reader->NewIterator(
        read, nullptr, nullptr, false, TableReaderCaller::kUserIterator));
    if (reverse) cold->SeekToLast(); else cold->SeekToFirst();
    EXPECT_FALSE(cold->Valid());
    EXPECT_TRUE(cold->status().IsIncomplete());
  }
  EXPECT_EQ(metrics->decodes.load(), 0U);
  {
    std::unique_ptr<InternalIterator> warm(reader->NewIterator(
        ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
    warm->Seek(keys.front());
    ASSERT_TRUE(warm->Valid());
  }
  std::unique_ptr<InternalIterator> partial(reader->NewIterator(
      read, nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  partial->SeekToFirst();
  ASSERT_TRUE(partial->Valid());
  partial->Next();
  EXPECT_FALSE(partial->Valid());
  EXPECT_TRUE(partial->status().IsIncomplete());
  EXPECT_EQ(metrics->decodes.load(), 1U);
  {
    std::unique_ptr<InternalIterator> warm(reader->NewIterator(
        ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
    warm->SeekToLast();
    ASSERT_TRUE(warm->Valid());
  }
  partial->SeekToLast();
  ASSERT_TRUE(partial->Valid());
  partial->Prev();
  ASSERT_TRUE(partial->Valid());
  EXPECT_EQ(metrics->decodes.load(), 2U);
}

TEST(CedarParquetTableReaderTest, CompactionDoesNotAdmitCanonicalPages) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  auto reader = BuildAndOpen({InternalKeyFor(1, 4)}, {StateFactValue()},
      immutable_options, mutable_options, comparator, 2, 1, 4096, {cache, metrics});
  for (auto caller : {TableReaderCaller::kCompaction,
                      TableReaderCaller::kCompactionRefill}) {
    std::unique_ptr<InternalIterator> it(reader->NewIterator(
        ReadOptions(), nullptr, nullptr, false, caller));
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid()) << it->status().ToString();
    EXPECT_EQ(it->value().ToString(), StateFactValue());
  }
  EXPECT_EQ(cache->GetUsage(), 0U);
  EXPECT_EQ(metrics->resident_charge_bytes.load(), 0U);
}

TEST(CedarParquetTableReaderTest, GetRespectsCacheOnlyAndNoFill) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1 << 20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  const auto key = InternalKeyFor(1, 4);
  const auto user_key = V2UserKey(1);
  auto reader = BuildAndOpen({key}, {StateFactValue()}, immutable_options,
      mutable_options, comparator, 2, 1, 4096, {cache, metrics});
  const auto get = [&](ReadOptions read) {
    PinnableSlice value;
    GetContext context(options.comparator, nullptr, nullptr, nullptr,
        GetContext::kNotFound, user_key, &value, nullptr, nullptr, nullptr,
        true, nullptr, nullptr);
    auto status = reader->Get(read, key, &context, nullptr);
    if (status.ok()) {
      EXPECT_EQ(context.State(), GetContext::kFound);
      EXPECT_EQ(value.ToString(), StateFactValue());
    }
    return status;
  };
  ReadOptions read;
  read.read_tier = kBlockCacheTier;
  EXPECT_TRUE(get(read).IsIncomplete());
  EXPECT_EQ(metrics->decodes.load(), 0U);
  read.read_tier = kReadAllTier;
  read.fill_cache = false;
  EXPECT_TRUE(get(read).ok());
  EXPECT_EQ(cache->GetUsage(), 0U);
  read.fill_cache = true;
  EXPECT_TRUE(get(read).ok());
  const auto decodes = metrics->decodes.load();
  read.read_tier = kBlockCacheTier;
  EXPECT_TRUE(get(read).ok());
  EXPECT_EQ(metrics->decodes.load(), decodes);
}

TEST(CedarParquetTableReaderTest, PinnedPageRemainsChargedUntilIteratorRelease) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache = NewLRUCache(1<<20, 0, true);
  auto metrics = std::make_shared<CanonicalPageCacheMetrics>();
  const std::vector<std::string> keys = {InternalKeyFor(1,4)};
  auto reader=BuildAndOpen(keys,{StateFactValue()},immutable_options,mutable_options,
                          comparator,2,1,4096,{cache,metrics});
  std::unique_ptr<InternalIterator> it(reader->NewIterator(
      ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
  it->Seek(keys[0]); ASSERT_TRUE(it->Valid());
  const uint64_t bytes=metrics->resident_charge_bytes.load();
  ASSERT_GT(bytes,0U); EXPECT_EQ(metrics->pinned_charge_bytes.load(),bytes);
  cache->SetCapacity(1); cache->EraseUnRefEntries();
  EXPECT_EQ(metrics->resident_charge_bytes.load(),bytes);
  EXPECT_EQ(it->value().ToString(),StateFactValue());
  it.reset(); reader.reset(); cache->EraseUnRefEntries();
  EXPECT_EQ(metrics->resident_charge_bytes.load(),0U);
  EXPECT_EQ(metrics->pinned_charge_bytes.load(),0U);
  EXPECT_EQ(metrics->uncached_live_bytes.load(),0U);
}

TEST(CedarParquetTableReaderTest, OversizeAndNoCachePagesHaveBoundedLifetime) {
  for (bool cache_enabled : {false,true}) {
    Options options;
    ImmutableOptions immutable_options(options);
    MutableCFOptions mutable_options(options);
    InternalKeyComparator comparator(BytewiseComparator());
    auto metrics=std::make_shared<CanonicalPageCacheMetrics>();
    auto cache=cache_enabled?NewLRUCache(1,0,true):std::shared_ptr<Cache>{};
    const std::vector<std::string> keys={InternalKeyFor(1,4)};
    auto reader=BuildAndOpen(keys,{StateFactValue()},immutable_options,mutable_options,
                            comparator,2,1,4096,{cache,metrics});
    std::unique_ptr<InternalIterator> it(reader->NewIterator(
        ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
    it->Seek(keys[0]);ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->value().ToString(),StateFactValue());
    EXPECT_GT(metrics->uncached_live_bytes.load(),0U);
    EXPECT_EQ(metrics->resident_charge_bytes.load(),0U);
    it.reset();reader.reset();
    EXPECT_EQ(metrics->uncached_live_bytes.load(),0U);
  }
}

TEST(CedarParquetTableReaderTest, ReaderNamespacesDoNotMixSamePageCoordinates) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache=NewLRUCache(1<<20,0,true);
  auto metrics=std::make_shared<CanonicalPageCacheMetrics>();
  const std::string first_key=InternalKeyFor(1,4),second_key=InternalKeyFor(99,4);
  auto first=BuildAndOpen({first_key},{StateFactValue()},immutable_options,mutable_options,
                          comparator,2,1,4096,{cache,metrics});
  std::unique_ptr<InternalIterator> a(first->NewIterator(
      ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
  a->Seek(first_key);ASSERT_TRUE(a->Valid());
  // Detach the first immutable inode before the fixture creates a second SST.
  ASSERT_EQ(std::rename(TestPath().c_str(),(TestPath()+".first").c_str()),0);
  auto second=BuildAndOpen({second_key},{StateFactValue()},immutable_options,mutable_options,
                           comparator,2,1,4096,{cache,metrics});
  std::unique_ptr<InternalIterator> b(second->NewIterator(
      ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
  b->Seek(second_key);ASSERT_TRUE(b->Valid());
  EXPECT_EQ(b->key().ToString(),second_key);
  EXPECT_EQ(a->key().ToString(),first_key);
  EXPECT_EQ(metrics->decodes.load(),2U);
  a.reset();b.reset();first.reset();second.reset();cache->EraseUnRefEntries();
  EXPECT_EQ(metrics->resident_charge_bytes.load(),0U);
  std::remove((TestPath()+".first").c_str());
}

TEST(CedarParquetTableReaderTest, ConcurrentMissesPreserveValuesAndReleaseCharges) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache=NewLRUCache(1<<20,0,true);
  auto metrics=std::make_shared<CanonicalPageCacheMetrics>();
  const std::vector<std::string> keys={InternalKeyFor(1,4),InternalKeyFor(2,3)};
  auto reader=BuildAndOpen(keys,{StateFactValue(),StateFactValue()},immutable_options,
                          mutable_options,comparator,2,1,4096,{cache,metrics});
  std::atomic<bool> valid{true};std::vector<std::thread> threads;
  for (unsigned w=0;w<8;++w) threads.emplace_back([&]{
    std::unique_ptr<InternalIterator> it(reader->NewIterator(
        ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
    for(unsigned n=0;n<128;++n) {
      it->Seek(keys[n%2]);
      if(!it->Valid()||it->value().ToString()!=StateFactValue())valid=false;
    }
  });
  for(auto& t:threads)t.join();EXPECT_TRUE(valid.load());
  reader.reset();cache->EraseUnRefEntries();
  EXPECT_EQ(metrics->resident_charge_bytes.load(),0U);
  EXPECT_EQ(metrics->pinned_charge_bytes.load(),0U);
  EXPECT_EQ(metrics->uncached_live_bytes.load(),0U);
}

TEST(CedarParquetTableReaderTest, FullPinnedCacheInsertFailureKeepsReadCorrect) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  auto cache=NewLRUCache(1<<20,0,true);
  auto metrics=std::make_shared<CanonicalPageCacheMetrics>();
  const std::vector<std::string> keys={InternalKeyFor(1,4),InternalKeyFor(2,3)};
  auto reader=BuildAndOpen(keys,{StateFactValue(),StateFactValue()},immutable_options,
                          mutable_options,comparator,2,1,4096,{cache,metrics});
  std::unique_ptr<InternalIterator> a(reader->NewIterator(
      ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
  a->Seek(keys[0]);ASSERT_TRUE(a->Valid());
  cache->SetCapacity(cache->GetUsage()+1);
  std::unique_ptr<InternalIterator> b(reader->NewIterator(
      ReadOptions(),nullptr,nullptr,false,TableReaderCaller::kUserIterator));
  b->Seek(keys[1]);ASSERT_TRUE(b->Valid());
  EXPECT_EQ(b->value().ToString(),StateFactValue());
  // Both the sort-key and encoded-value page fail strict admission.
  EXPECT_EQ(metrics->insert_failures.load(),2U);
  EXPECT_GT(metrics->uncached_live_bytes.load(),0U);
  b.reset();EXPECT_EQ(metrics->uncached_live_bytes.load(),0U);
  a.reset();reader.reset();cache->EraseUnRefEntries();
  EXPECT_EQ(metrics->resident_charge_bytes.load(),0U);
  EXPECT_EQ(metrics->pinned_charge_bytes.load(),0U);
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

// F7: the page cache no longer keeps the derivable internal-key column, but a
// page is still validated when it is decoded: the stored column is decoded
// transiently and compared with DecodeCedarParquetSortKey(sort_key). Corrupt one
// stored internal key and prove the on-demand derivation cannot mask the
// mismatch - the first read of that page must fail closed with Corruption.
TEST(CedarParquetTableReaderTest,
     RejectsStoredInternalKeyThatDisagreesWithSortKey) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {InternalKeyFor(1, 4),
                                         InternalKeyFor(2, 4)};
  const std::vector<std::string> values = {StateFactValue(), StateFactValue()};
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  reader.reset();

  std::ifstream input(TestPath(), std::ios::binary);
  ASSERT_TRUE(input.good());
  const std::string bytes((std::istreambuf_iterator<char>(input)),
                          std::istreambuf_iterator<char>());
  CedarParquetFooter footer;
  ASSERT_TRUE(ParseParquetFooter(bytes, &footer, nullptr).ok());
  const auto& internal_key_column = footer.row_groups[0].columns[1];
  ASSERT_GT(internal_key_column.offset_index_length, 0);
  const std::string encoded_index = bytes.substr(
      static_cast<size_t>(internal_key_column.offset_index_offset),
      static_cast<size_t>(internal_key_column.offset_index_length));
  std::vector<CedarParquetFooter::ColumnChunk::PageLocation> locations;
  ASSERT_TRUE(DecodeOffsetIndex(encoded_index, &locations).ok());
  ASSERT_FALSE(locations.empty());
  const auto& location = locations[0];
  std::string wrong_internal_key = keys[0];
  wrong_internal_key.back() ^= 0x01;
  std::string replacement_page;
  ASSERT_TRUE(
      EncodePlainByteArrayDataPage({Slice(wrong_internal_key)}, &replacement_page)
          .ok());
  ASSERT_LE(replacement_page.size(),
            static_cast<size_t>(location.compressed_page_size));
  std::fstream output(TestPath(),
                      std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(output.good());
  output.seekp(static_cast<std::streamoff>(location.offset));
  output.write(replacement_page.data(),
               static_cast<std::streamsize>(replacement_page.size()));
  const std::string padding(
      static_cast<size_t>(location.compressed_page_size) - replacement_page.size(),
      '\0');
  output.write(padding.data(), static_cast<std::streamsize>(padding.size()));
  ASSERT_TRUE(output.good());
  output.close();

  reader = OpenExisting(immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));
  iterator->Seek(keys[0]);
  EXPECT_TRUE(iterator->status().IsCorruption())
      << iterator->status().ToString();
  EXPECT_FALSE(iterator->Valid());
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

TEST(CedarParquetTableReaderTest, ReverseIterationReturnsMatchingValues) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (uint64_t row = 0; row < 6; ++row) {
    keys.push_back(InternalKeyFor(row + 1, 6 - row));
    // Valid state-fact values alternate between put and delete so a value
    // read for the wrong row is observable.
    std::string value;
    value.push_back(1);
    value.push_back(row % 2 == 0 ? 1 : 2);
    AppendBigEndian32(&value, 0);
    value.push_back(0);
    AppendBigEndian32(&value, 0);
    AppendBigEndian32(&value, crc32c::Value(value.data(), value.size()));
    values.push_back(value);
  }
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserGet));
  iterator->SeekToLast();
  for (size_t index = keys.size(); index-- > 0;) {
    ASSERT_TRUE(iterator->Valid());
    EXPECT_EQ(iterator->key(), keys[index]) << "row " << index;
    EXPECT_EQ(iterator->value(), values[index]) << "row " << index;
    iterator->Prev();
  }
  EXPECT_FALSE(iterator->Valid());
}

TEST(CedarParquetTableReaderTest, PrefixSeekStartsAtEachPrefixRange) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  SequenceNumber sequence = 1;
  for (uint32_t part : {0U, 2U, 7U}) {
    for (uint8_t family : {1U, 4U}) {
      for (uint64_t entity : {1ULL, 3ULL, 5ULL}) {
        keys.push_back(V2FactInternalKey(part, family, entity,
                                         /*valid_from=*/0,
                                         /*cedar_commit_seq=*/1, sequence++));
      }
    }
  }
  std::sort(keys.begin(), keys.end());
  const std::vector<std::string> values(keys.size(), StateFactValue());
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));

  // Prefix lengths one through twelve cover the version byte, the partition,
  // the family, the property word, and the high entity bytes.
  for (size_t length = 1; length <= kCedarParquetMaximumPrefixSeekBytes;
       ++length) {
    std::vector<std::string> prefixes;
    for (const std::string& key : keys) {
      std::string prefix = key.substr(0, length);
      if (std::find(prefixes.begin(), prefixes.end(), prefix) ==
          prefixes.end()) {
        prefixes.push_back(std::move(prefix));
      }
    }
    ASSERT_FALSE(prefixes.empty()) << "length " << length;
    for (const std::string& prefix : prefixes) {
      const size_t expected = ExpectedPrefixSeekIndex(keys, prefix);
      ASSERT_LT(expected, keys.size()) << "length " << length;
      iterator->Seek(PrefixSeekInternalKey(prefix));
      ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
      ASSERT_TRUE(iterator->Valid()) << "length " << length;
      EXPECT_EQ(iterator->key().ToString(), keys[expected])
          << "length " << length;
      EXPECT_EQ(iterator->value().ToString(), values[expected]);
    }
  }
}

TEST(CedarParquetTableReaderTest, PrefixSeekWithoutMatchesPositionsAtNextRange) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  std::vector<std::string> keys;
  SequenceNumber sequence = 1;
  for (uint32_t part : {0U, 2U}) {
    for (uint64_t entity : {1ULL, 2ULL}) {
      keys.push_back(V2FactInternalKey(part, /*family=*/1, entity,
                                       /*valid_from=*/0,
                                       /*cedar_commit_seq=*/1, sequence++));
    }
  }
  std::sort(keys.begin(), keys.end());
  const std::vector<std::string> values(keys.size(), StateFactValue());
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));

  // Partition 1 holds no rows, so the seek has to land on partition 2's first
  // key rather than at the end of the table.
  std::string missing(5, '\0');
  missing[0] = 2;
  PutBigEndian32(&missing, 1, 1);
  const size_t expected = ExpectedPrefixSeekIndex(keys, missing);
  ASSERT_LT(expected, keys.size());
  EXPECT_EQ(static_cast<uint8_t>(keys[expected][1]), 0);
  EXPECT_EQ(static_cast<uint8_t>(keys[expected][4]), 2);
  iterator->Seek(PrefixSeekInternalKey(missing));
  ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().ToString(), keys[expected]);

  // A prefix past the last partition has no match and leaves a healthy,
  // invalid iterator at the end of the table.
  std::string beyond(5, '\0');
  beyond[0] = 2;
  PutBigEndian32(&beyond, 1, 9);
  EXPECT_EQ(ExpectedPrefixSeekIndex(keys, beyond), keys.size());
  iterator->Seek(PrefixSeekInternalKey(beyond));
  EXPECT_TRUE(iterator->status().ok()) << iterator->status().ToString();
  EXPECT_FALSE(iterator->Valid());
}

TEST(CedarParquetTableReaderTest, PrefixSeekRejectsMalformedEncoding) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  const std::vector<std::string> keys = {V2FactInternalKey(
      0, 1, 1, 0, 1, 1)};
  const std::vector<std::string> values(keys.size(), StateFactValue());
  std::unique_ptr<CedarParquetTableReader> reader = BuildAndOpen(
      keys, values, immutable_options, mutable_options, comparator);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));

  std::string encoded = EncodeCedarParquetPrefixSeekKey("ab");
  ASSERT_EQ(encoded.size(), kCedarParquetV2UserKeyBytes);
  encoded[1] = '\0';  // Prefix seek marker with a zero-length prefix.
  std::string target;
  AppendInternalKey(&target,
                    ParsedInternalKey(Slice(encoded), kMaxSequenceNumber,
                                      kValueTypeForSeek));
  iterator->Seek(target);
  EXPECT_TRUE(iterator->status().IsCorruption())
      << iterator->status().ToString();
  EXPECT_FALSE(iterator->Valid());
}

TEST(CedarParquetTableReaderTest,
     PrefixSeekMatchesFullScanOnRandomizedFacts) {
  Options options;
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_options(options);
  InternalKeyComparator comparator(BytewiseComparator());
  constexpr size_t kFactCount = 10000;
  constexpr size_t kQueryCount = 128;
  Random random(0x5eed5eed);
  std::vector<std::string> keys;
  keys.reserve(kFactCount);
  for (size_t index = 0; index < kFactCount; ++index) {
    // Even partitions only, so odd-partition prefixes have no match and land
    // on the following range.
    const uint32_t part = 2U * static_cast<uint32_t>(random.Uniform(3));
    const uint8_t family = random.OneIn(2) ? 1 : 4;
    const uint64_t valid_from = random.Next() % 4096;
    const uint64_t commit_seq = 1 + random.Next() % 1000000;
    keys.push_back(V2FactInternalKey(part, family, /*entity=*/index + 1,
                                     valid_from, commit_seq,
                                     static_cast<SequenceNumber>(index + 1)));
  }
  std::sort(keys.begin(), keys.end());
  const std::vector<std::string> values(kFactCount, StateFactValue());
  std::unique_ptr<CedarParquetTableReader> reader =
      BuildAndOpen(keys, values, immutable_options, mutable_options, comparator,
                   /*row_group_max_rows=*/512, /*page_max_rows=*/128,
                   /*row_group_max_bytes=*/64ULL * 1024ULL * 1024ULL);
  ASSERT_NE(reader, nullptr);
  std::unique_ptr<InternalIterator> iterator(reader->NewIterator(
      ReadOptions(), nullptr, nullptr, false, TableReaderCaller::kUserIterator));

  // The reference is a complete table scan.
  size_t scanned = 0;
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ASSERT_LT(scanned, keys.size());
    ASSERT_EQ(iterator->key().ToString(), keys[scanned]) << "scan " << scanned;
    ++scanned;
  }
  ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
  ASSERT_EQ(scanned, keys.size());

  for (size_t query = 0; query < kQueryCount; ++query) {
    const std::string& source = keys[random.Uniform(kFactCount)];
    const size_t length =
        1 + random.Uniform(kCedarParquetMaximumPrefixSeekBytes);
    std::string prefix = source.substr(0, length);
    if (query % 4 == 3) {
      // Exercise positions that are not the start of an existing prefix:
      // odd partitions have no rows, and 0xff is past the last partition.
      const uint32_t part =
          query % 8 == 7 ? 0xffU : 1U + 2U * static_cast<uint32_t>(random.Uniform(2));
      prefix.assign(5, '\0');
      prefix[0] = 2;
      PutBigEndian32(&prefix, 1, part);
      if (random.OneIn(2)) prefix.push_back(static_cast<char>(1));
    }
    const size_t expected = ExpectedPrefixSeekIndex(keys, prefix);
    iterator->Seek(PrefixSeekInternalKey(prefix));
    ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
    if (expected == keys.size()) {
      EXPECT_FALSE(iterator->Valid()) << "query " << query;
      continue;
    }
    ASSERT_TRUE(iterator->Valid()) << "query " << query;
    EXPECT_EQ(iterator->key().ToString(), keys[expected]) << "query " << query;
    // Continuing the bounded scan yields the same rows as the full scan.
    for (size_t offset = 1; offset <= 4 && expected + offset < keys.size();
         ++offset) {
      iterator->Next();
      ASSERT_TRUE(iterator->Valid())
          << "query " << query << " offset " << offset;
      EXPECT_EQ(iterator->key().ToString(), keys[expected + offset]);
    }
  }
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
