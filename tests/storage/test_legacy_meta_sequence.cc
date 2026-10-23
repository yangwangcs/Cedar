// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

// v2-only meta sequence contract: a database written by this release always
// records marker 2 and kind-9 (`kSequenceV2`) records, while any database that
// still carries the retired v1 format (missing/1 marker or a kind-7 record) is
// rejected at Open as unsupported (v1 is unsupported; there is no migration or
// downgrade). The v1 record is built locally from the documented on-disk
// framing because this release has no v1 encoder.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "cedar/core/crc32c.h"
#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar {
namespace {

constexpr uint8_t kLegacySequenceRecordKind = 7;
constexpr uint8_t kSequenceV2RecordKind = 9;
constexpr uint8_t kSequenceFormatMarkerKind = 10;

void ExpectContains(const std::string& text, const std::string& needle) {
  EXPECT_NE(text.find(needle), std::string::npos)
      << "missing '" << needle << "' in: " << text;
}

// v1 is unsupported: the rejection must say so, state that no migration or
// downgrade exists, and tell the operator to delete and recreate the database
// (or restore a format-2 backup).
void ExpectV1UnsupportedMessage(const std::string& text) {
  ExpectContains(text, "v1");
  ExpectContains(text, "unsupported");
  ExpectContains(text, "migration/downgrade is not supported");
  ExpectContains(text, "Delete and recreate the database");
}

void AppendU32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

void AppendU64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

// Builds a framed meta record exactly like the retired v1 encoder did: meta
// version(1) + kind(1) + payload length(u32 BE) + payload + CRC32C(u32 BE).
std::string FrameMetaRecord(uint8_t kind, const std::string& payload) {
  std::string encoded;
  encoded.push_back(static_cast<char>(1));
  encoded.push_back(static_cast<char>(kind));
  AppendU32(&encoded, static_cast<uint32_t>(payload.size()));
  encoded.append(payload);
  AppendU32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

// The retired v1 `kSequence` payload: commit_seq(8) + txn_id(8) +
// system_hlc(8) + count(4) + N x 32-byte fact key.
std::string EncodeLegacyV1SequenceRecord(const SequenceRecord& record) {
  EXPECT_TRUE(record.Validate().ok()) << record.Validate().ToString();
  std::string payload;
  AppendU64(&payload, record.commit_seq.value);
  AppendU64(&payload, record.txn_id.value);
  AppendU64(&payload, record.system_hlc);
  AppendU32(&payload, static_cast<uint32_t>(record.fact_keys.size()));
  for (const std::string& key : record.fact_keys) payload.append(key);
  return FrameMetaRecord(kLegacySequenceRecordKind, payload);
}

std::string EncodeSequenceFormatMarker(uint32_t format) {
  std::string payload;
  AppendU32(&payload, format);
  return FrameMetaRecord(kSequenceFormatMarkerKind, payload);
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

// Offline view of the database that installs Cedar's column-family resources
// (the facts CF may already contain Cedar Parquet SSTs). The flush-suppression
// flags keep this inspection from rewriting what the next Cedar open sees.
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
  const rocksdb::Status opened = rocksdb::DB::Open(
      options, path, descriptors, &out->handles, &out->database);
  if (!opened.ok()) {
    ADD_FAILURE() << "raw open failed: " << opened.ToString();
    return false;
  }
  return true;
}

std::optional<std::string> ReadRawMetaRecordIfPresent(const std::string& path,
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

void PutRawMetaRecord(const std::string& path, const std::string& key,
                      const std::string& value) {
  RawDatabase raw;
  ASSERT_TRUE(OpenRawDatabase(path, &raw));
  ASSERT_TRUE(raw.database
                  ->Put(rocksdb::WriteOptions(), raw.handles[2], key, value)
                  .ok());
}

void DeleteRawMetaRecord(const std::string& path, const std::string& key) {
  RawDatabase raw;
  ASSERT_TRUE(OpenRawDatabase(path, &raw));
  ASSERT_TRUE(
      raw.database->Delete(rocksdb::WriteOptions(), raw.handles[2], key).ok());
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

class LegacyMetaSequenceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_legacy_meta_sequence_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  // Creates a valid v2 database with one committed transaction through the
  // public storage API. Tests then tamper with raw meta records.
  std::string CreateV2Database(const std::string& name) {
    const std::string path = path_ + "/" + name;
    FactStore store(FactStoreOptions{path});
    EXPECT_TRUE(store.Open().ok());
    EXPECT_TRUE(store.Commit(VertexStateBatch(TxnId{1}, VertexId{1}, 10)).ok());
    EXPECT_TRUE(store.Close().ok());
    return path;
  }

  std::string path_;
};

TEST_F(LegacyMetaSequenceTest, FreshDatabaseWritesMarker2AndKind9Records) {
  const std::string path = CreateV2Database("fresh_v2");

  const auto marker = ReadRawMetaRecordIfPresent(path, EncodeSequenceFormatKey());
  ASSERT_TRUE(marker.has_value());
  ASSERT_GE(marker->size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>((*marker)[1]), kSequenceFormatMarkerKind);
  const auto decoded_marker = DecodeSequenceFormat(*marker);
  ASSERT_TRUE(decoded_marker.ok()) << decoded_marker.status().ToString();
  EXPECT_EQ(decoded_marker.ValueOrDie(), kMetaSequenceFormatV2);

  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const auto record = ReadRawMetaRecordIfPresent(path, sequence_key.ValueOrDie());
  ASSERT_TRUE(record.has_value());
  ASSERT_GE(record->size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>((*record)[1]), kSequenceV2RecordKind);
}

TEST_F(LegacyMetaSequenceTest, OpenRejectsMissingMarkerAsLegacyV1Database) {
  const std::string path = CreateV2Database("missing_marker");
  DeleteRawMetaRecord(path, EncodeSequenceFormatKey());

  FactStore store(FactStoreOptions{path});
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();
  ExpectContains(opened.ToString(), "no durable meta sequence format marker");
  ExpectV1UnsupportedMessage(opened.ToString());
  if (opened.ok()) EXPECT_TRUE(store.Close().ok());
}

TEST_F(LegacyMetaSequenceTest, OpenRejectsFormat1MarkerAsLegacyV1Database) {
  const std::string path = CreateV2Database("marker_v1");
  PutRawMetaRecord(path, EncodeSequenceFormatKey(),
                   EncodeSequenceFormatMarker(kMetaSequenceFormatV1));

  FactStore store(FactStoreOptions{path});
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();
  ExpectContains(opened.ToString(), "format 1 (v1)");
  ExpectV1UnsupportedMessage(opened.ToString());
  if (opened.ok()) EXPECT_TRUE(store.Close().ok());
}

TEST_F(LegacyMetaSequenceTest, OpenRejectsLegacyKind7SequenceRecord) {
  const std::string path = CreateV2Database("kind7_record");
  // Replace sequence/1 (a valid kind-9 record) with the retired v1 encoding.
  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  const std::string fact_key = EncodeFactKey(ref, ValidTime{10}, CommitSeq{1});
  const std::string legacy = EncodeLegacyV1SequenceRecord(
      SequenceRecord{CommitSeq{1}, TxnId{1}, 10, {fact_key}});
  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  PutRawMetaRecord(path, sequence_key.ValueOrDie(), legacy);

  FactStore store(FactStoreOptions{path});
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok()) << "a kind-7 record must never be decoded";
  EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();
  ExpectContains(opened.ToString(), "legacy v1 meta sequence record");
  ExpectV1UnsupportedMessage(opened.ToString());
  if (opened.ok()) EXPECT_TRUE(store.Close().ok());

  // The rejected open must not have rewritten or removed the legacy record.
  const auto stored =
      ReadRawMetaRecordIfPresent(path, sequence_key.ValueOrDie());
  ASSERT_TRUE(stored.has_value());
  EXPECT_EQ(*stored, legacy);
}

TEST_F(LegacyMetaSequenceTest, DecodersRejectLegacyKind7RecordAsUnsupported) {
  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref();
  const std::string fact_key = EncodeFactKey(ref, ValidTime{10}, CommitSeq{3});
  const std::string legacy = EncodeLegacyV1SequenceRecord(
      SequenceRecord{CommitSeq{3}, TxnId{9}, 10, {fact_key}});

  const auto record = DecodeSequenceRecord(legacy, CommitSeq{3});
  ASSERT_FALSE(record.ok());
  EXPECT_TRUE(record.status().IsNotSupportedError())
      << record.status().ToString();
  ExpectV1UnsupportedMessage(record.status().ToString());

  const auto view = DecodeSequenceRecordView(legacy, CommitSeq{3});
  ASSERT_FALSE(view.ok());
  EXPECT_TRUE(view.status().IsNotSupportedError()) << view.status().ToString();
  ExpectContains(view.status().ToString(), "legacy v1 meta sequence record");
}

}  // namespace
}  // namespace cedar
