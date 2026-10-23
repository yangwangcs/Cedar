// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

// End-to-end coverage for the meta-sequence one-way gate and the
// CEDAR_META_SEQUENCE_V1_ONLY compatibility build. The production defect this
// guards against is a pre-v2 binary opening a format-2 database and either
// failing with an opaque framing error or, worse, silently misreading records.
//
// A normally configured build (no macro) runs these tests by enabling the
// v1-only decoder through the internal test hook, so the exact production gate
// code is exercised. A build configured with
// -DCEDAR_META_SEQUENCE_V1_ONLY=ON defaults the gate to on (and cannot unlock
// it); the same tests then validate the real compatibility binary. In that
// configuration the "supported binary" cross-checks are compiled out.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/facts/meta_sequence_compat.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar {
namespace {

void ExpectContains(const std::string& text, const std::string& needle) {
  EXPECT_NE(text.find(needle), std::string::npos)
      << "missing '" << needle << "' in: " << text;
}

constexpr uint8_t kRawSequenceRecordKindV1 = 7;
constexpr uint8_t kRawSequenceRecordKindV2 = 9;

uint8_t RawMetaRecordKind(const std::string& encoded) {
  EXPECT_GE(encoded.size(), 2U);
  return encoded.size() < 2U ? 0 : static_cast<uint8_t>(encoded[1]);
}

// Scoped view of the v1-only decoder flag. The setter restores the compiled
// default, so tests never leak the simulated mode into another test.
class ScopedV1OnlyMode {
 public:
  explicit ScopedV1OnlyMode(bool disabled)
      : previous_(internal::MetaSequenceV2ReadDisabled()) {
    internal::SetMetaSequenceV2ReadDisabledForTesting(disabled);
  }
  ~ScopedV1OnlyMode() {
    internal::SetMetaSequenceV2ReadDisabledForTesting(previous_);
  }
  ScopedV1OnlyMode(const ScopedV1OnlyMode&) = delete;
  ScopedV1OnlyMode& operator=(const ScopedV1OnlyMode&) = delete;

 private:
  bool previous_;
};

struct RawDatabase {
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::ColumnFamilyHandle* meta = nullptr;

  ~RawDatabase() {
    if (!database) return;
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      database->DestroyColumnFamilyHandle(handle);
    }
  }
};

// Raw view used only to assemble fixtures a newer binary could have produced
// (the format marker and a v2 record). It never calls the format-2 decoder.
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
  if (out->handles.size() != descriptors.size()) {
    ADD_FAILURE() << "raw open returned a mismatched handle count";
    return false;
  }
  for (size_t index = 0; index < descriptors.size(); ++index) {
    if (descriptors[index].name == "meta") out->meta = out->handles[index];
  }
  if (out->meta == nullptr) {
    ADD_FAILURE() << "raw open found no meta column family";
    return false;
  }
  return true;
}

void PutRawRecord(const std::string& path, const std::string& key,
                  const std::string& value) {
  RawDatabase raw;
  ASSERT_TRUE(OpenRawDatabase(path, &raw));
  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  const rocksdb::Status put =
      raw.database->Put(write_options, raw.meta, key, value);
  ASSERT_TRUE(put.ok()) << put.ToString();
}

std::optional<std::string> ReadRawRecordIfPresent(const std::string& path,
                                                  const std::string& key) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return std::nullopt;
  std::string value;
  const rocksdb::Status got =
      raw.database->Get(rocksdb::ReadOptions(), raw.meta, key, &value);
  if (got.IsNotFound()) return std::nullopt;
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return std::nullopt;
  }
  return value;
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

FactStoreOptions V1Options(const std::string& path) {
  FactStoreOptions options;
  options.path = path;
  // F19 made v2 the default; this fixture is an explicit pre-v2 database.
  options.meta_sequence_format = kMetaSequenceFormatV1;
  return options;
}

std::vector<SequenceRecord> CreateV1History(const std::string& path,
                                            size_t count) {
  std::vector<SequenceRecord> records;
  records.reserve(count);
  FactStore store(V1Options(path));
  EXPECT_TRUE(store.Open().ok());
  for (size_t sequence = 1; sequence <= count; ++sequence) {
    const auto committed = store.Commit(VertexStateBatch(
        TxnId{1000 + sequence}, VertexId{static_cast<uint64_t>(sequence)},
        static_cast<uint64_t>(10 + sequence)));
    EXPECT_TRUE(committed.ok())
        << "commit " << sequence << ": " << committed.status().ToString();
  }
  {
    const auto snapshot = store.BeginSnapshot();
    EXPECT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    for (size_t index = 0; index < count; ++index) {
      const auto read = store.ReadSequence(
          snapshot.ValueOrDie(), CommitSeq{static_cast<uint64_t>(index + 1)});
      EXPECT_TRUE(read.ok()) << read.status().ToString();
      if (read.ok()) records.push_back(read.ValueOrDie());
    }
  }
  EXPECT_TRUE(store.Close().ok());
  return records;
}

// Writes the durable format marker directly. A format-2 marker can exist next
// to v1 records: the opt-in writes the marker before the first v2 commit.
void WriteFormatMarker(const std::string& path, uint32_t format) {
  const auto encoded = EncodeSequenceFormat(format);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  PutRawRecord(path, EncodeSequenceFormatKey(), encoded.ValueOrDie());
}

// Replaces sequence/1 with its v2 encoding while leaving the format marker
// absent (the stray-record case a partially restored or hand-edited database
// can expose).
void RewriteFirstSequenceAsV2(const std::string& path) {
  const auto key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(key.ok()) << key.status().ToString();
  const auto encoded_v1 = ReadRawRecordIfPresent(path, key.ValueOrDie());
  ASSERT_TRUE(encoded_v1.has_value());
  EXPECT_EQ(RawMetaRecordKind(*encoded_v1), kRawSequenceRecordKindV1);
  const auto record = DecodeSequenceRecord(*encoded_v1);
  ASSERT_TRUE(record.ok()) << record.status().ToString();
  const auto encoded_v2 = EncodeSequenceRecordV2(record.ValueOrDie());
  ASSERT_TRUE(encoded_v2.ok()) << encoded_v2.status().ToString();
  EXPECT_EQ(RawMetaRecordKind(encoded_v2.ValueOrDie()), kRawSequenceRecordKindV2);
  PutRawRecord(path, key.ValueOrDie(), encoded_v2.ValueOrDie());
}

class MetaCompatTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_meta_compat_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::string path_;
};

TEST(MetaCompatBuildSwitchTest, DefaultReadModeMatchesCompileSwitch) {
#if defined(CEDAR_META_SEQUENCE_V1_ONLY)
  EXPECT_TRUE(internal::MetaSequenceV2ReadDisabled());
#else
  EXPECT_FALSE(internal::MetaSequenceV2ReadDisabled());
#endif
}

TEST_F(MetaCompatTest, V1DatabaseOpensAndAcceptsV1CommitsInV1OnlyMode) {
  const std::string path = path_ + "/v1_positive";
  CreateV1History(path, 3);

  ScopedV1OnlyMode v1_only(true);
#if defined(CEDAR_META_SEQUENCE_V1_ONLY)
  // The compatibility build simulates a pre-v2 binary, so its default options
  // must be the v1 writer. A normal build enables the same gate at runtime and
  // therefore has to opt out explicitly to keep the fixture a v1 database.
  FactStore store(FactStoreOptions{path});
#else
  FactStore store(V1Options(path));
#endif
  ASSERT_TRUE(store.Open().ok()) << "a v1 database must stay readable";
  {
    const auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto range = store.ReadSequenceRange(snapshot.ValueOrDie(),
                                               CommitSeq{1}, CommitSeq{3});
    ASSERT_TRUE(range.ok()) << range.status().ToString();
    EXPECT_EQ(range.ValueOrDie().size(), 3U);
  }
  const auto committed = store.Commit(VertexStateBatch(
      TxnId{2000}, VertexId{99}, 99));
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_TRUE(store.Close().ok());

  // The new v1 record stays readable in the same one-way mode.
  FactStore reopened(V1Options(path));
  ASSERT_TRUE(reopened.Open().ok());
  {
    const auto snapshot = reopened.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto range = reopened.ReadSequenceRange(snapshot.ValueOrDie(),
                                                  CommitSeq{1}, CommitSeq{4});
    ASSERT_TRUE(range.ok()) << range.status().ToString();
    EXPECT_EQ(range.ValueOrDie().size(), 4U);
  }
  ASSERT_TRUE(reopened.Close().ok());
}

TEST_F(MetaCompatTest, V1OnlyDecoderRejectsFormat2MarkerWithActionableCorruption) {
  const std::string path = path_ + "/v2_marker";
  CreateV1History(path, 3);
  WriteFormatMarker(path, kMetaSequenceFormatV2);

  ScopedV1OnlyMode v1_only(true);
  FactStore store(V1Options(path));
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok()) << "a v1-only decoder must refuse a format-2 database";
  EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
  ExpectContains(opened.ToString(), "meta sequence format 2");
  ExpectContains(opened.ToString(), "CEDAR_META_SEQUENCE_V1_ONLY");
  ExpectContains(opened.ToString(), "Reopen");
  if (opened.ok()) EXPECT_TRUE(store.Close().ok());

#if !defined(CEDAR_META_SEQUENCE_V1_ONLY)
  // The refused open must not have changed the database. A compiled v1-only
  // build cannot unlock the read gate, so this cross-check is v2-capable-only;
  // the v1-only build validates the same fixture in the positive test above
  // and through the one-way behavior itself.
  ScopedV1OnlyMode supported(false);
  FactStore supported_store(FactStoreOptions{path});
  ASSERT_TRUE(supported_store.Open().ok())
      << "the fixture must stay a valid format-2 database";
  ASSERT_TRUE(supported_store.Close().ok());
#endif
}

TEST_F(MetaCompatTest, V1OnlyDecoderRejectsStrayV2RecordWithoutMarker) {
  const std::string path = path_ + "/stray_v2";
  CreateV1History(path, 2);
  RewriteFirstSequenceAsV2(path);

  ScopedV1OnlyMode v1_only(true);
  {
    FactStore store(V1Options(path));
    const Status opened = store.Open();
    ASSERT_FALSE(opened.ok())
        << "a v2 record must not be misread when the marker is missing";
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    ExpectContains(opened.ToString(), "meta sequence v2 record");
    ExpectContains(opened.ToString(), "CEDAR_META_SEQUENCE_V1_ONLY");
    if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  }
#if !defined(CEDAR_META_SEQUENCE_V1_ONLY)
  // Cross-check: the fixture really is readable by a v2-capable binary.
  ScopedV1OnlyMode supported(false);
  FactStore supported_store(FactStoreOptions{path});
  ASSERT_TRUE(supported_store.Open().ok());
  {
    const auto snapshot = supported_store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto sequence =
        supported_store.ReadSequence(snapshot.ValueOrDie(), CommitSeq{1});
    ASSERT_TRUE(sequence.ok()) << sequence.status().ToString();
  }
  ASSERT_TRUE(supported_store.Close().ok());
#endif
}

TEST_F(MetaCompatTest, V1OnlyDecoderRefusesToEnableTheFormat2Writer) {
  const std::string path = path_ + "/writer_option";
  CreateV1History(path, 1);

  ScopedV1OnlyMode v1_only(true);
  FactStoreOptions options{path};
  options.meta_sequence_format = kMetaSequenceFormatV2;
  FactStore store(std::move(options));
  const Status opened = store.Open();
  ASSERT_FALSE(opened.ok()) << "a v1-only binary must not upgrade a database";
  EXPECT_TRUE(opened.IsInvalidArgument()) << opened.ToString();
  ExpectContains(opened.ToString(), "CEDAR_META_SEQUENCE_V1_ONLY");
  if (opened.ok()) EXPECT_TRUE(store.Close().ok());
  EXPECT_FALSE(
      ReadRawRecordIfPresent(path, EncodeSequenceFormatKey()).has_value());

  // The database remains a normal v1 database for an explicit v1 opt-out.
  FactStore v1_store(V1Options(path));
  ASSERT_TRUE(v1_store.Open().ok());
  ASSERT_TRUE(v1_store.Close().ok());
}

#if !defined(CEDAR_META_SEQUENCE_V1_ONLY)
TEST_F(MetaCompatTest, SupportedBinaryStillOpensAndReadsAV2Database) {
  const std::string path = path_ + "/v2_supported";
  {
    FactStoreOptions options{path};
    options.meta_sequence_format = kMetaSequenceFormatV2;
    FactStore store(std::move(options));
    ASSERT_TRUE(store.Open().ok());
    for (size_t sequence = 1; sequence <= 2; ++sequence) {
      const auto committed = store.Commit(VertexStateBatch(
          TxnId{3000 + sequence}, VertexId{static_cast<uint64_t>(sequence)},
          static_cast<uint64_t>(50 + sequence)));
      ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    }
    ASSERT_TRUE(store.Close().ok());
  }
  const auto encoded = ReadRawRecordIfPresent(
      path, EncodeSequenceMetaKey(CommitSeq{1}).ValueOrDie());
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(RawMetaRecordKind(*encoded), kRawSequenceRecordKindV2);

  // The marker is authoritative: a default-option open keeps the v2 writer and
  // dual-reads the history.
  ScopedV1OnlyMode supported(false);
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  EXPECT_EQ(store.EffectiveSequenceRecordFormat(), kMetaSequenceFormatV2);
  {
    const auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto range = store.ReadSequenceRange(snapshot.ValueOrDie(),
                                               CommitSeq{1}, CommitSeq{2});
    ASSERT_TRUE(range.ok()) << range.status().ToString();
    EXPECT_EQ(range.ValueOrDie().size(), 2U);
  }
  ASSERT_TRUE(store.Close().ok());
}
#endif

}  // namespace
}  // namespace cedar
