// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "cedar/fact/meta_v2_reclaim.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar {
namespace {

// MetaRecordKind is private to meta_codec.cc; these tests pin the on-disk kind
// bytes the same way the codec and marker tests do.
constexpr uint8_t kRawSequenceRecordKindV1 = 7;
constexpr uint8_t kRawSequenceRecordKindV2 = 9;
constexpr uint8_t kSequenceFormatMarkerKind = 10;

uint8_t RawMetaRecordKind(const std::string& encoded) {
  EXPECT_GE(encoded.size(), 2U);
  return encoded.size() < 2U ? 0 : static_cast<uint8_t>(encoded[1]);
}

struct RawDatabase {
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  ~RawDatabase() {
    if (!database) return;
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      database->DestroyColumnFamilyHandle(handle);
    }
  }
};

// Mirrors the raw inspection helper the reopen tests use: Cedar's storage
// configuration is needed for a facts column family that may already contain
// Cedar Parquet SSTs, and the flush-suppression flags keep this offline view
// from rewriting what the next Cedar open sees.
bool OpenRawDatabase(const std::string& path, RawDatabase* out) {
  FactStoreOptions store_options;
  store_options.path = path;
  std::shared_ptr<rocksdb::Cache> cache;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      store_options, /*is_new_database=*/false, /*resolved=*/nullptr, &cache);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  auto page_metrics =
      std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      internal::MakeRocksDbColumnFamilyDescriptors(
          store_options, options, /*resolved=*/nullptr, {cache, page_metrics});
  const rocksdb::Status opened = rocksdb::DB::Open(options, path, descriptors,
                                                   &out->handles, &out->database);
  if (!opened.ok()) {
    ADD_FAILURE() << "raw open failed: " << opened.ToString();
    return false;
  }
  return true;
}

std::optional<std::string> ReadMetaRecordIfPresent(const std::string& path,
                                                   const std::string& key) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return std::nullopt;
  std::string encoded;
  const rocksdb::Status got = raw.database->Get(
      rocksdb::ReadOptions(), raw.handles[2], key, &encoded);
  if (got.IsNotFound()) return std::nullopt;
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return std::nullopt;
  }
  return encoded;
}

std::string ReadMetaRecord(const std::string& path, const std::string& key) {
  const auto encoded = ReadMetaRecordIfPresent(path, key);
  EXPECT_TRUE(encoded.has_value()) << "missing meta record: " << key;
  return encoded.value_or(std::string());
}

void PutMetaRecord(const std::string& path, const std::string& key,
                   const std::string& value) {
  RawDatabase raw;
  ASSERT_TRUE(OpenRawDatabase(path, &raw));
  const rocksdb::Status put = raw.database->Put(
      rocksdb::WriteOptions(), raw.handles[2], key, value);
  ASSERT_TRUE(put.ok()) << put.ToString();
}

std::vector<std::string> ReadSequenceValues(const std::string& path,
                                            size_t count) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return {};
  std::vector<std::string> values;
  values.reserve(count);
  for (size_t sequence = 1; sequence <= count; ++sequence) {
    const auto key =
        EncodeSequenceMetaKey(CommitSeq{static_cast<uint64_t>(sequence)});
    if (!key.ok()) {
      ADD_FAILURE() << key.status().ToString();
      return {};
    }
    std::string value;
    const rocksdb::Status got = raw.database->Get(
        rocksdb::ReadOptions(), raw.handles[2], key.ValueOrDie(), &value);
    if (!got.ok()) {
      ADD_FAILURE() << "missing sequence " << sequence << ": "
                    << got.ToString();
      return {};
    }
    values.push_back(std::move(value));
  }
  return values;
}

std::vector<SequenceRecord> DecodeRawRecords(
    const std::vector<std::string>& values) {
  std::vector<SequenceRecord> records;
  records.reserve(values.size());
  for (size_t index = 0; index < values.size(); ++index) {
    const CommitSeq commit_seq{static_cast<uint64_t>(index + 1)};
    const auto decoded = DecodeSequenceRecord(values[index], commit_seq);
    if (!decoded.ok()) {
      ADD_FAILURE() << "decode sequence " << (index + 1) << ": "
                    << decoded.status().ToString();
      return {};
    }
    records.push_back(decoded.ValueOrDie());
  }
  return records;
}

StoreCommitBatch VertexStateBatch(TxnId txn_id, VertexId vertex_id,
                                  uint64_t valid_from) {
  return StoreCommitBatch{
      txn_id,
      valid_from,
      {{EntityFact::Vertex(VertexRef{PartId{0}, vertex_id}).ref(),
        ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt}},
      {},
      {},
      {},
      false};
}

FactStoreOptions StoreOptionsAt(const std::string& path, uint32_t format) {
  FactStoreOptions options;
  options.path = path;
  options.meta_sequence_format = format;
  return options;
}

// Writes `count` committed v1 sequence records (one vertex fact each).
void CreateV1History(const std::string& path, size_t count) {
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  for (size_t sequence = 1; sequence <= count; ++sequence) {
    const auto committed = store.Commit(VertexStateBatch(
        TxnId{1000 + sequence}, VertexId{static_cast<uint64_t>(sequence)},
        static_cast<uint64_t>(10 + sequence)));
    ASSERT_TRUE(committed.ok()) << "commit " << sequence << ": "
                                << committed.status().ToString();
  }
  ASSERT_TRUE(store.Close().ok());
}

// The supported opt-in path: opening with format 2 durably writes the marker
// before any commit is accepted, then closes without writing new records.
void EnableV2Writer(const std::string& path) {
  FactStore store(StoreOptionsAt(path, kMetaSequenceFormatV2));
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());
}

void ExpectSequencesReadable(const std::string& path,
                             const std::vector<SequenceRecord>& expected) {
  ASSERT_FALSE(expected.empty());
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  {
    const auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    for (size_t index = 0; index < expected.size(); ++index) {
      const auto read = store.ReadSequence(
          snapshot.ValueOrDie(), CommitSeq{static_cast<uint64_t>(index + 1)});
      ASSERT_TRUE(read.ok()) << "sequence " << (index + 1) << ": "
                             << read.status().ToString();
      EXPECT_EQ(read.ValueOrDie(), expected[index]);
    }
    const auto range = store.ReadSequenceRange(
        snapshot.ValueOrDie(), CommitSeq{1},
        CommitSeq{static_cast<uint64_t>(expected.size())});
    ASSERT_TRUE(range.ok()) << range.status().ToString();
    ASSERT_EQ(range.ValueOrDie().size(), expected.size());
    for (size_t index = 0; index < expected.size(); ++index) {
      EXPECT_EQ(range.ValueOrDie()[index], expected[index]);
    }
  }
  ASSERT_TRUE(store.Close().ok());
}

class MetaV2ReclaimTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_meta_v2_reclaim_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::string path_;
};

TEST_F(MetaV2ReclaimTest, ReclaimsEveryV1RecordAndIsIdempotent) {
  const std::string path = path_ + "/all_v1";
  CreateV1History(path, 8);
  EnableV2Writer(path);

  const std::vector<std::string> before = ReadSequenceValues(path, 8);
  ASSERT_EQ(before.size(), 8U);
  uint64_t bytes_before = 0;
  for (size_t index = 0; index < before.size(); ++index) {
    EXPECT_EQ(RawMetaRecordKind(before[index]), kRawSequenceRecordKindV1)
        << "sequence " << (index + 1);
    bytes_before += before[index].size();
  }
  const std::vector<SequenceRecord> expected = DecodeRawRecords(before);
  ASSERT_EQ(expected.size(), 8U);

  MetaV2ReclaimOptions options;
  MetaV2ReclaimStats stats;
  const Status reclaimed = ReclaimSequenceRecordsV2(path, options, &stats);
  ASSERT_TRUE(reclaimed.ok()) << reclaimed.ToString();
  EXPECT_EQ(stats.scanned, 8U);
  EXPECT_EQ(stats.rewritten, 8U);
  EXPECT_EQ(stats.already_v2, 0U);
  EXPECT_EQ(stats.bytes_before, bytes_before);

  const std::vector<std::string> after = ReadSequenceValues(path, 8);
  ASSERT_EQ(after.size(), 8U);
  uint64_t bytes_after = 0;
  for (size_t index = 0; index < after.size(); ++index) {
    EXPECT_EQ(RawMetaRecordKind(after[index]), kRawSequenceRecordKindV2)
        << "sequence " << (index + 1);
    bytes_after += after[index].size();
    // v2 drops 8 bytes of embedded commit_seq and adds two 1-byte version
    // fields (-6), then saves 9 bytes of every 32-byte fact key.
    EXPECT_EQ(before[index].size() - after[index].size(), 6U + 9U * 1U)
        << "sequence " << (index + 1);
  }
  EXPECT_EQ(stats.bytes_after, bytes_after);
  EXPECT_LT(stats.bytes_after, stats.bytes_before);
  EXPECT_EQ(stats.bytes_before - stats.bytes_after, 8U * (6U + 9U * 1U));
  ExpectSequencesReadable(path, expected);

  // A second run must skip every record and change nothing.
  MetaV2ReclaimStats second_stats;
  const Status second = ReclaimSequenceRecordsV2(path, options, &second_stats);
  ASSERT_TRUE(second.ok()) << second.ToString();
  EXPECT_EQ(second_stats.scanned, 8U);
  EXPECT_EQ(second_stats.rewritten, 0U);
  EXPECT_EQ(second_stats.already_v2, 8U);
  EXPECT_EQ(second_stats.bytes_before, stats.bytes_after);
  EXPECT_EQ(second_stats.bytes_after, stats.bytes_after);
  EXPECT_EQ(ReadSequenceValues(path, 8), after);
  ExpectSequencesReadable(path, expected);
}

TEST_F(MetaV2ReclaimTest, CrashAfterSecondBatchLeavesPrefixAndResumes) {
  const std::string path = path_ + "/crash_resume";
  CreateV1History(path, 8);
  EnableV2Writer(path);
  const std::vector<SequenceRecord> expected =
      DecodeRawRecords(ReadSequenceValues(path, 8));
  ASSERT_EQ(expected.size(), 8U);

  MetaV2ReclaimOptions options;
  options.batch_records = 2;
  options.fault_for_testing = [](size_t batch_index) {
    if (batch_index == 1) {
      throw std::runtime_error("simulated crash after the second batch");
    }
  };
  MetaV2ReclaimStats crashed_stats;
  EXPECT_THROW(
      ReclaimSequenceRecordsV2(path, options, &crashed_stats),
      std::runtime_error);

  // Batches 1-2 (four records) committed atomically; the remaining records are
  // still v1 and the database reopens cleanly.
  const std::vector<std::string> partial = ReadSequenceValues(path, 8);
  ASSERT_EQ(partial.size(), 8U);
  for (size_t index = 0; index < partial.size(); ++index) {
    const uint8_t expected_kind = index < 4 ? kRawSequenceRecordKindV2
                                            : kRawSequenceRecordKindV1;
    EXPECT_EQ(RawMetaRecordKind(partial[index]), expected_kind)
        << "sequence " << (index + 1);
  }

  MetaV2ReclaimOptions resume_options;
  resume_options.batch_records = 2;
  MetaV2ReclaimStats resumed_stats;
  const Status resumed =
      ReclaimSequenceRecordsV2(path, resume_options, &resumed_stats);
  ASSERT_TRUE(resumed.ok()) << resumed.ToString();
  EXPECT_EQ(resumed_stats.scanned, 8U);
  EXPECT_EQ(resumed_stats.rewritten, 4U);
  EXPECT_EQ(resumed_stats.already_v2, 4U);
  EXPECT_EQ(resumed_stats.bytes_before - resumed_stats.bytes_after,
            4U * (6U + 9U * 1U));

  const std::vector<std::string> complete = ReadSequenceValues(path, 8);
  ASSERT_EQ(complete.size(), 8U);
  for (size_t index = 0; index < complete.size(); ++index) {
    EXPECT_EQ(RawMetaRecordKind(complete[index]), kRawSequenceRecordKindV2)
        << "sequence " << (index + 1);
  }
  ExpectSequencesReadable(path, expected);
}

TEST_F(MetaV2ReclaimTest, RefusesMissingOrV1FormatMarkerWithoutWriting) {
  const std::string marker_key = EncodeSequenceFormatKey();

  // Missing marker: a plain v1 database that was never upgraded.
  {
    const std::string path = path_ + "/missing_marker";
    CreateV1History(path, 4);
    const std::vector<std::string> before = ReadSequenceValues(path, 4);
    ASSERT_EQ(before.size(), 4U);
    MetaV2ReclaimStats stats;
    const Status refused = ReclaimSequenceRecordsV2(path, {}, &stats);
    EXPECT_FALSE(refused.ok());
    EXPECT_TRUE(refused.IsInvalidArgument()) << refused.ToString();
    EXPECT_EQ(stats.scanned, 0U);
    EXPECT_EQ(stats.rewritten, 0U);
    EXPECT_EQ(stats.already_v2, 0U);
    EXPECT_EQ(stats.bytes_before, 0U);
    EXPECT_EQ(stats.bytes_after, 0U);
    EXPECT_EQ(ReadSequenceValues(path, 4), before);
    EXPECT_FALSE(ReadMetaRecordIfPresent(path, marker_key).has_value());
  }

  // Marker value 1: still the v1 writer format, so reclamation must refuse.
  {
    const std::string path = path_ + "/v1_marker";
    CreateV1History(path, 4);
    const auto marker = EncodeSequenceFormat(kMetaSequenceFormatV1);
    ASSERT_TRUE(marker.ok()) << marker.status().ToString();
    PutMetaRecord(path, marker_key, marker.ValueOrDie());
    const std::vector<std::string> before = ReadSequenceValues(path, 4);
    ASSERT_EQ(before.size(), 4U);
    MetaV2ReclaimStats stats;
    const Status refused = ReclaimSequenceRecordsV2(path, {}, &stats);
    EXPECT_FALSE(refused.ok());
    EXPECT_TRUE(refused.IsInvalidArgument()) << refused.ToString();
    EXPECT_EQ(stats.scanned, 0U);
    EXPECT_EQ(ReadSequenceValues(path, 4), before);
    const std::string stored = ReadMetaRecord(path, marker_key);
    EXPECT_EQ(RawMetaRecordKind(stored), kSequenceFormatMarkerKind);
    const auto decoded = DecodeSequenceFormat(stored);
    ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
    EXPECT_EQ(decoded.ValueOrDie(), kMetaSequenceFormatV1);
  }
}

TEST_F(MetaV2ReclaimTest, ReclaimsOnlyV1RecordsInMixedDatabase) {
  const std::string path = path_ + "/mixed";
  CreateV1History(path, 4);
  {
    FactStore store(StoreOptionsAt(path, kMetaSequenceFormatV2));
    ASSERT_TRUE(store.Open().ok());
    for (size_t sequence = 5; sequence <= 8; ++sequence) {
      const auto committed = store.Commit(VertexStateBatch(
          TxnId{2000 + sequence}, VertexId{static_cast<uint64_t>(sequence)},
          static_cast<uint64_t>(10 + sequence)));
      ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    }
    ASSERT_TRUE(store.Close().ok());
  }

  const std::vector<std::string> before = ReadSequenceValues(path, 8);
  ASSERT_EQ(before.size(), 8U);
  uint64_t bytes_before = 0;
  for (size_t index = 0; index < before.size(); ++index) {
    bytes_before += before[index].size();
    EXPECT_EQ(RawMetaRecordKind(before[index]),
              index < 4 ? kRawSequenceRecordKindV1 : kRawSequenceRecordKindV2)
        << "sequence " << (index + 1);
  }
  const std::vector<SequenceRecord> expected = DecodeRawRecords(before);
  ASSERT_EQ(expected.size(), 8U);

  MetaV2ReclaimOptions options;
  MetaV2ReclaimStats stats;
  const Status reclaimed = ReclaimSequenceRecordsV2(path, options, &stats);
  ASSERT_TRUE(reclaimed.ok()) << reclaimed.ToString();
  EXPECT_EQ(stats.scanned, 8U);
  EXPECT_EQ(stats.rewritten, 4U);
  EXPECT_EQ(stats.already_v2, 4U);
  EXPECT_EQ(stats.bytes_before, bytes_before);
  EXPECT_EQ(stats.bytes_before - stats.bytes_after,
            4U * (6U + 9U * 1U));

  const std::vector<std::string> after = ReadSequenceValues(path, 8);
  ASSERT_EQ(after.size(), 8U);
  for (size_t index = 0; index < after.size(); ++index) {
    EXPECT_EQ(RawMetaRecordKind(after[index]), kRawSequenceRecordKindV2)
        << "sequence " << (index + 1);
    if (index >= 4) {
      // Already-v2 records must be byte-identical after the run.
      EXPECT_EQ(after[index], before[index]);
    } else {
      EXPECT_EQ(before[index].size() - after[index].size(), 6U + 9U * 1U);
    }
  }
  ExpectSequencesReadable(path, expected);
}

}  // namespace
}  // namespace cedar
