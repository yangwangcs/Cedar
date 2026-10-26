// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "cedar/database.h"
#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>

namespace cedar {
namespace {

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define CEDAR_THREAD_SANITIZER_ENABLED 1
#endif
#endif
#ifndef CEDAR_THREAD_SANITIZER_ENABLED
#define CEDAR_THREAD_SANITIZER_ENABLED 0
#endif

// F9 Task 6: the same three durable commit kill points are replayed under the
// two reopen-validation policies. The policy only affects FactStore::Open, so
// the crash itself always runs through the public Database API and the persisted
// store is then reopened directly with the chosen FactStoreOptions. A final
// Database::Open proves the kernel still recovers the same durable state.
struct ReopenValidationPolicy {
  const char* name;
  bool deep_recovery_validation;
  uint64_t recovery_validation_recent_sequences;
};

constexpr ReopenValidationPolicy kReopenValidationPolicies[] = {
    {"deep", true, 4096},
    {"bounded", false, 2},
};

enum class CommitCrashPoint {
  // commit_prewrite_fault_injector_for_testing: the batch has been assembled
  // but no durable write has been issued.
  kBeforeFsync,
  // commit_fault_injector_for_testing in the synchronous commit path: the
  // Write (including fsync) returned, but the visible watermark was not
  // published to readers in this process.
  kAfterFsync,
  // commit_fault_injector_for_testing in the kernel group-commit path:
  // CommitAsync's epoch is durably accepted, and the process dies before the
  // terminal result is published.
  kAfterDurableAcceptance,
};

constexpr int kCrashExitCode = 91;
constexpr int kAsyncCrashExitCode = 92;

const char* CrashPointName(CommitCrashPoint point) {
  switch (point) {
    case CommitCrashPoint::kBeforeFsync:
      return "before_fsync";
    case CommitCrashPoint::kAfterFsync:
      return "after_fsync";
    case CommitCrashPoint::kAfterDurableAcceptance:
      return "after_durable_acceptance";
  }
  return "unknown";
}

std::string VertexFactKey(uint64_t vertex_id, uint64_t valid_time,
                          uint64_t commit_seq) {
  return EncodeFactKey(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
      ValidTime{valid_time}, CommitSeq{commit_seq});
}

// Deletes one fact row through a vanilla RocksDB handle. The close flags mirror
// the fact-store corruption tests: they keep the recovered facts memtable from
// being flushed into a block-based SST that the Cedar table factory would later
// reject. The test target links cedar_core, so this only mutates the same CFs
// Cedar opened.
void DeleteFactRecord(const std::string& path, const std::string& key) {
  rocksdb::Options options;
  options.create_if_missing = false;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                           rocksdb::ColumnFamilyOptions(options));
  rocksdb::ColumnFamilyOptions facts_options(options);
  facts_options.prefix_extractor = std::shared_ptr<const rocksdb::SliceTransform>(
      rocksdb::NewFixedPrefixTransform(12));
  descriptors.emplace_back("facts", std::move(facts_options));
  descriptors.emplace_back("meta", rocksdb::ColumnFamilyOptions(options));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  const rocksdb::Status opened =
      rocksdb::DB::Open(options, path, descriptors, &handles, &database);
  ASSERT_TRUE(opened.ok()) << opened.ToString();
  if (!opened.ok()) return;
  ASSERT_TRUE(database->Delete(rocksdb::WriteOptions(), handles[1], key).ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
}

// Reads one meta-CF record through a secondary RocksDB handle so recovery tests
// can inspect the durable sequence-format marker and the raw record kind.
// Cedar's facts SSTs use the Cedar Parquet table factory, so this inspection
// uses the same column-family options as FactStore::Open; the flush-suppression
// flags keep the inspection from rewriting the files recovery will replay.
std::optional<std::string> ReadRawMetaRecord(const std::string& path,
                                             const std::string& key) {
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
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  const rocksdb::Status opened =
      rocksdb::DB::Open(options, path, descriptors, &handles, &database);
  if (!opened.ok()) {
    ADD_FAILURE() << opened.ToString();
    return std::nullopt;
  }
  std::string encoded;
  const rocksdb::Status got = database->Get(rocksdb::ReadOptions(), handles[2],
                                            key, &encoded);
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
  if (got.IsNotFound()) return std::nullopt;
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return std::nullopt;
  }
  return encoded;
}

// The crash scenario commits through the storage API directly (no kernel
// transaction) so the meta sequence v2 writer is the writer under test.
StoreCommitBatch VertexStateStoreBatch(TxnId txn_id, uint64_t system_hlc,
                                       uint64_t vertex_id, uint64_t valid_time) {
  return StoreCommitBatch{
      txn_id,
      system_hlc,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
        ValidTime{valid_time}, FactOperation::kPut, 0, std::nullopt}},
      {},
      {},
      {},
      false};
}

// Kills one committing child process at the requested durable boundary. The
// parent then observes its exact exit code so a scenario that did not reach its
// kill point fails the matrix instead of silently recovering a complete commit.
void CrashCommittedVertex(const std::string& path, CommitCrashPoint point,
                          uint64_t vertex_id, uint64_t valid_time) {
  const bool async = point == CommitCrashPoint::kAfterDurableAcceptance;
  const int crash_code = async ? kAsyncCrashExitCode : kCrashExitCode;
  const pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    DatabaseOptions options;
    options.path = path;
    if (async) {
      options.group_commit_max_batch_size = 1;
      options.group_commit_window_us = 0;
    }
    const auto crash = [crash_code]() -> Status { _exit(crash_code); };
    if (point == CommitCrashPoint::kBeforeFsync) {
      options.commit_prewrite_fault_injector_for_testing = crash;
    } else {
      options.commit_fault_injector_for_testing = crash;
    }
    auto opened = Database::Open(std::move(options));
    if (!opened.ok()) _exit(2);
    std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
    auto begun = database->BeginTransaction();
    if (!begun.ok()) _exit(3);
    std::unique_ptr<Transaction> transaction =
        std::move(begun).ConsumeValueOrDie();
    if (!transaction
             ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
                      ValidTime{valid_time})
             .ok()) {
      _exit(4);
    }
    if (async) {
      const auto accepted = transaction->CommitAsync();
      if (!accepted.ok()) _exit(5);
      const auto completed = accepted.ValueOrDie().Wait();
      static_cast<void>(completed);
      _exit(6);
    }
    const auto committed = transaction->Commit();
    static_cast<void>(committed);
    _exit(5);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), crash_code)
      << "crash point " << CrashPointName(point);
}

// Reopens the persisted store with the selected validation policy and checks the
// recovered storage-level state: visible sequence, transaction resolution,
// point-read value and the exact fact count in the family.
void ExpectFactStoreRecovered(const std::string& path,
                              const ReopenValidationPolicy& policy,
                              TxnId txn_id, uint64_t vertex_id,
                              uint64_t valid_time, bool committed) {
  FactStoreOptions options;
  options.path = path;
  options.deep_recovery_validation = policy.deep_recovery_validation;
  options.recovery_validation_recent_sequences =
      policy.recovery_validation_recent_sequences;
  FactStore store(options);
  const Status opened = store.Open();
  ASSERT_TRUE(opened.ok()) << policy.name << ": " << opened.ToString();
  const CommitSeq expected_seq = committed ? CommitSeq{1} : CommitSeq{0};
  EXPECT_EQ(store.visible_seq(), expected_seq) << policy.name;
  const auto resolved = store.ResolveTransaction(txn_id);
  ASSERT_TRUE(resolved.ok()) << policy.name << ": "
                             << resolved.status().ToString();
  ASSERT_EQ(resolved.ValueOrDie().has_value(), committed) << policy.name;
  if (committed) {
    EXPECT_EQ(resolved.ValueOrDie()->commit_seq, expected_seq) << policy.name;
  }
  {
    auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << policy.name << ": "
                               << snapshot.status().ToString();
    StoreSnapshot pinned = std::move(snapshot).ConsumeValueOrDie();
    EXPECT_EQ(pinned.commit_seq(), expected_seq) << policy.name;
    const auto event = store.Read(
        pinned,
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
        ValidTime{valid_time});
    ASSERT_TRUE(event.ok()) << policy.name << ": " << event.status().ToString();
    ASSERT_EQ(event.ValueOrDie().has_value(), committed) << policy.name;
    if (committed) {
      EXPECT_EQ(event.ValueOrDie()->commit_seq, expected_seq) << policy.name;
      EXPECT_EQ(event.ValueOrDie()->valid_from.value, valid_time) << policy.name;
      EXPECT_EQ(event.ValueOrDie()->operation, FactOperation::kPut)
          << policy.name;
    }
    size_t facts = 0;
    const Status scanned = store.Scan(
        pinned,
        FactPrefix::Family(PartId{0}, FactFamily::kVertexState, PropertyId{}),
        [&facts](const FactEvent&) {
          ++facts;
          return Status::OK();
        });
    ASSERT_TRUE(scanned.ok()) << policy.name << ": " << scanned.ToString();
    EXPECT_EQ(facts, committed ? 1U : 0U) << policy.name;
  }
  ASSERT_TRUE(store.Close().ok()) << policy.name;
}

// Kernel-level check after the policy-filtered reopen: the public Database
// reopen still resolves the transaction and serves the same snapshot state.
void ExpectDatabaseRecovered(const std::string& path, TxnId txn_id,
                             uint64_t vertex_id, uint64_t valid_time,
                             bool committed) {
  DatabaseOptions options;
  options.path = path;
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  const CommitSeq expected_seq = committed ? CommitSeq{1} : CommitSeq{0};
  const auto resolved = database->ResolveTransaction(txn_id);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_EQ(resolved.ValueOrDie().has_value(), committed);
  if (committed) {
    EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  }
  {
    auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    Snapshot pinned = std::move(snapshot).ConsumeValueOrDie();
    EXPECT_EQ(pinned.commit_seq(), expected_seq);
    const auto exists = pinned.Exists(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
        ValidTime{valid_time});
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_EQ(exists.ValueOrDie(), committed);
    size_t facts = 0;
    ASSERT_TRUE(pinned
                    .Scan(FactFamily::kVertexState, PropertyId{},
                          [&facts](const FactEvent&) {
                            ++facts;
                            return Status::OK();
                          })
                    .ok());
    EXPECT_EQ(facts, committed ? 1U : 0U);
  }
  // The snapshot must be released before Close (an outliving handle makes a
  // successful close report SnapshotPinned).
  ASSERT_TRUE(database->Close().ok());
}

// A successful open verifies the bounded policy really skipped (or deep really
// rejected) a deliberately deleted fact; a corruption expectation also pins the
// stable "missing fact" diagnostic.
void ExpectFactStoreOpenStatus(const std::string& path,
                               const ReopenValidationPolicy& policy,
                               bool expect_corruption,
                               std::string_view message = {}) {
  FactStoreOptions options;
  options.path = path;
  options.deep_recovery_validation = policy.deep_recovery_validation;
  options.recovery_validation_recent_sequences =
      policy.recovery_validation_recent_sequences;
  FactStore store(options);
  const Status status = store.Open();
  if (!expect_corruption) {
    ASSERT_TRUE(status.ok()) << policy.name << ": " << status.ToString();
    ASSERT_TRUE(store.Close().ok()) << policy.name;
    return;
  }
  EXPECT_TRUE(status.IsCorruption()) << policy.name << ": " << status.ToString();
  if (!message.empty()) {
    EXPECT_NE(status.ToString().find(message), std::string::npos)
        << policy.name << ": " << status.ToString();
  }
  if (status.ok()) EXPECT_TRUE(store.Close().ok()) << policy.name;
}

class RecoveryCrashMatrixTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_recovery_matrix_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::unique_ptr<Database> OpenAt(const std::string& path,
                                   DatabaseOptions options = {}) {
    options.path = path;
    auto opened = Database::Open(std::move(options));
    EXPECT_TRUE(opened.ok()) << opened.status().ToString();
    return opened.ok() ? std::move(opened).ConsumeValueOrDie() : nullptr;
  }

  std::unique_ptr<Database> Open(DatabaseOptions options = {}) {
    return OpenAt(path_, std::move(options));
  }

  CommitResult CommitVertex(Database& database, uint64_t vertex_id,
                            uint64_t valid_time) {
    auto begun = database.BeginTransaction();
    EXPECT_TRUE(begun.ok()) << begun.status().ToString();
    if (!begun.ok()) return {};
    std::unique_ptr<Transaction> transaction = std::move(begun).ConsumeValueOrDie();
    EXPECT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
                                    ValidTime{valid_time})
                    .ok());
    const auto committed = transaction->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    return committed.ok() ? committed.ValueOrDie() : CommitResult{};
  }

  std::string path_;
};

TEST_F(RecoveryCrashMatrixTest, ReopenDistinguishesPrewriteAndPostwriteBoundaries) {
  auto prewrite = Open(DatabaseOptions{
      .commit_prewrite_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "prewrite crash");
      }});
  ASSERT_TRUE(prewrite);
  const CommitResult before = CommitVertex(*prewrite, 1, 10);
  EXPECT_EQ(before.outcome, CommitOutcome::kIndeterminate);
  const TxnId before_id = before.txn_id;
  prewrite.reset();

  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto before_resolution = reopened->ResolveTransaction(before_id);
  ASSERT_TRUE(before_resolution.ok()) << before_resolution.status().ToString();
  EXPECT_FALSE(before_resolution.ValueOrDie().has_value());
  EXPECT_FALSE(reopened->BeginSnapshot().ValueOrDie()
                   .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                   .ValueOrDie());
  reopened.reset();

  auto postwrite = Open(DatabaseOptions{
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "postwrite crash");
      }});
  ASSERT_TRUE(postwrite);
  const CommitResult after = CommitVertex(*postwrite, 2, 20);
  EXPECT_EQ(after.outcome, CommitOutcome::kIndeterminate);
  const TxnId after_id = after.txn_id;
  postwrite.reset();

  reopened = Open();
  ASSERT_TRUE(reopened);
  const auto after_resolution = reopened->ResolveTransaction(after_id);
  ASSERT_TRUE(after_resolution.ok()) << after_resolution.status().ToString();
  ASSERT_TRUE(after_resolution.ValueOrDie().has_value());
  EXPECT_EQ(after_resolution.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest,
       KernelReopenRecoversKernelWriteAcrossPrewriteAndPostwriteBoundaries) {
  const auto kernel_options = [] {
    DatabaseOptions options;
    options.storage_profile = StorageProfile::kProductionAppend;
    options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
    options.production.kernel_mode = true;
    options.storage_read_working_bytes = 32ULL << 20;
    options.storage_reader_metadata_bytes = 16ULL << 20;
    options.query_runtime.query_memory_bytes = 32ULL * 1024ULL * 1024ULL;
    options.query_runtime.projection_cache_bytes = 32ULL * 1024ULL * 1024ULL;
    options.query_runtime.query_delta_bytes = 32ULL * 1024ULL * 1024ULL;
    options.runtime_pressure_override_for_testing = [](PressureSample* sample) {
      // This recovery test exercises the write-boundary fault contract, not
      // host-dependent disk admission.
      sample->free_disk_bytes = UINT64_MAX;
      sample->free_disk_percent = 100;
    };
    return options;
  };

  DatabaseOptions prewrite_options = kernel_options();
  prewrite_options.commit_prewrite_fault_injector_for_testing = [] {
    return Status::Indeterminate("test", "kernel prewrite crash");
  };
  auto prewrite = Open(std::move(prewrite_options));
  ASSERT_TRUE(prewrite);
  const CommitResult before = CommitVertex(*prewrite, 11, 110);
  EXPECT_EQ(before.outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(before.status.IsIndeterminate()) << before.status.ToString();
  const TxnId before_id = before.txn_id;
  prewrite.reset();

  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto before_resolution = reopened->ResolveTransaction(before_id);
  ASSERT_TRUE(before_resolution.ok()) << before_resolution.status().ToString();
  EXPECT_FALSE(before_resolution.ValueOrDie().has_value());
  EXPECT_FALSE(reopened->BeginSnapshot().ValueOrDie()
                   .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{11}}),
                           ValidTime{110})
                   .ValueOrDie());
  reopened.reset();

  DatabaseOptions postwrite_options = kernel_options();
  postwrite_options.commit_fault_injector_for_testing = [] {
    return Status::Indeterminate("test", "kernel postwrite crash");
  };
  auto postwrite = Open(std::move(postwrite_options));
  ASSERT_TRUE(postwrite);
  const CommitResult after = CommitVertex(*postwrite, 12, 120);
  EXPECT_EQ(after.outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(after.status.IsIndeterminate()) << after.status.ToString();
  const TxnId after_id = after.txn_id;
  postwrite.reset();

  reopened = Open();
  ASSERT_TRUE(reopened);
  const auto after_resolution = reopened->ResolveTransaction(after_id);
  ASSERT_TRUE(after_resolution.ok()) << after_resolution.status().ToString();
  ASSERT_TRUE(after_resolution.ValueOrDie().has_value());
  EXPECT_EQ(after_resolution.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{12}}),
                          ValidTime{120})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest, ReopenRecoversActualProcessCrashesAtCommitBoundaries) {
#if CEDAR_THREAD_SANITIZER_ENABLED
  GTEST_SKIP() << "ThreadSanitizer cannot create threads after a multithreaded fork";
#endif
  const auto crash_commit = [this](uint64_t vertex_id, uint64_t valid_time,
                                   bool after_write) {
    const pid_t child = fork();
    ASSERT_NE(child, -1);
    if (child == 0) {
      DatabaseOptions options;
      options.path = path_;
      const auto crash = []() -> Status { _exit(91); };
      if (after_write) {
        options.commit_fault_injector_for_testing = crash;
      } else {
        options.commit_prewrite_fault_injector_for_testing = crash;
      }
      auto opened = Database::Open(std::move(options));
      if (!opened.ok()) _exit(2);
      std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
      auto begun = database->BeginTransaction();
      if (!begun.ok()) _exit(3);
      std::unique_ptr<Transaction> transaction = std::move(begun).ConsumeValueOrDie();
      if (!transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
                               ValidTime{valid_time})
               .ok()) {
        _exit(4);
      }
      const auto committed = transaction->Commit();
      static_cast<void>(committed);
      _exit(5);
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 91);
  };

  crash_commit(3, 30, false);
  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto before = reopened->ResolveTransaction(TxnId{1});
  ASSERT_TRUE(before.ok()) << before.status().ToString();
  EXPECT_FALSE(before.ValueOrDie().has_value());
  EXPECT_FALSE(reopened->BeginSnapshot().ValueOrDie()
                   .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}), ValidTime{30})
                   .ValueOrDie());
  reopened.reset();

  crash_commit(4, 40, true);
  reopened = Open();
  ASSERT_TRUE(reopened);
  // The first transaction durably extended the default 65,536-ID lease, so the
  // next transaction after the crash starts at 65,537 (IDs are opaque; the gap
  // is the unused remainder of that durable lease).
  const auto after = reopened->ResolveTransaction(TxnId{65537});
  ASSERT_TRUE(after.ok()) << after.status().ToString();
  ASSERT_TRUE(after.ValueOrDie().has_value());
  EXPECT_EQ(after.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{4}}), ValidTime{40})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest, ReopenRecoversAsyncFinalPublishAfterDurableAcceptance) {
  const pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    DatabaseOptions options;
    options.path = path_;
    options.group_commit_max_batch_size = 1;
    options.group_commit_window_us = 0;
    options.commit_fault_injector_for_testing = []() -> Status { _exit(92); };
    auto opened = Database::Open(std::move(options));
    if (!opened.ok()) _exit(2);
    std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
    auto begun = database->BeginTransaction();
    if (!begun.ok()) _exit(3);
    std::unique_ptr<Transaction> transaction = std::move(begun).ConsumeValueOrDie();
    if (!transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{5}}), ValidTime{50}).ok()) {
      _exit(4);
    }
    const auto accepted = transaction->CommitAsync();
    if (!accepted.ok()) _exit(5);
    const auto completed = accepted.ValueOrDie().Wait();
    static_cast<void>(completed);
    _exit(6);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 92);

  auto reopened = Open();
  ASSERT_TRUE(reopened);
  const auto resolved = reopened->ResolveTransaction(TxnId{1});
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(reopened->BeginSnapshot().ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{5}}), ValidTime{50})
                  .ValueOrDie());
}

TEST_F(RecoveryCrashMatrixTest, ReopenSkipsDurableLeaseRemainders) {
  auto database = Open();
  ASSERT_TRUE(database);
  EXPECT_EQ(database->AllocateVertexId().ValueOrDie(), VertexId{1});
  EXPECT_EQ(database->AllocateEdgeId().ValueOrDie(), EdgeId{1});
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  database = Open();
  ASSERT_TRUE(database);
  EXPECT_EQ(database->AllocateVertexId().ValueOrDie(), VertexId{4097});
  EXPECT_EQ(database->AllocateEdgeId().ValueOrDie(), EdgeId{4097});
}

TEST_F(RecoveryCrashMatrixTest,
       ReopenCrashMatrixUnderBothValidationPolicies) {
#if CEDAR_THREAD_SANITIZER_ENABLED
  GTEST_SKIP() << "ThreadSanitizer cannot create threads after a multithreaded fork";
#endif
  constexpr uint64_t kVertexId = 100;
  constexpr uint64_t kValidTime = 1000;
  constexpr CommitCrashPoint kCrashPoints[] = {
      CommitCrashPoint::kBeforeFsync,
      CommitCrashPoint::kAfterFsync,
      CommitCrashPoint::kAfterDurableAcceptance,
  };
  for (const ReopenValidationPolicy& policy : kReopenValidationPolicies) {
    for (CommitCrashPoint point : kCrashPoints) {
      SCOPED_TRACE(std::string(policy.name) + "/" + CrashPointName(point));
      const bool committed = point != CommitCrashPoint::kBeforeFsync;
      const std::string path =
          path_ + "/matrix_" + policy.name + "_" + CrashPointName(point);
      ASSERT_TRUE(std::filesystem::create_directories(path));
      CrashCommittedVertex(path, point, kVertexId, kValidTime);
      ExpectFactStoreRecovered(path, policy, TxnId{1}, kVertexId, kValidTime,
                               committed);
      ExpectDatabaseRecovered(path, TxnId{1}, kVertexId, kValidTime, committed);
    }
  }
}

TEST_F(RecoveryCrashMatrixTest,
       ValidationPoliciesStillDetectRemovedFactsInsideTheirWindow) {
  struct MutationCase {
    const char* name;
    uint64_t removed_sequence;
    bool bounded_accepts;
  };
  constexpr MutationCase kMutations[] = {
      // Inside the K=2 window of visible sequence 3: both policies reject.
      {"removed_recent_fact", 3, false},
      // Outside the bounded window: bounded skips it, deep still rejects.
      {"removed_old_fact", 1, true},
  };
  for (const MutationCase& mutation : kMutations) {
    SCOPED_TRACE(mutation.name);
    const std::string path = path_ + "/" + mutation.name;
    ASSERT_TRUE(std::filesystem::create_directories(path));
    auto database = OpenAt(path);
    ASSERT_TRUE(database);
    constexpr uint64_t kTransactions = 3;
    for (uint64_t sequence = 1; sequence <= kTransactions; ++sequence) {
      ASSERT_EQ(CommitVertex(*database, sequence, 10 + sequence).outcome,
                CommitOutcome::kCommitted);
    }
    ASSERT_TRUE(database->Close().ok());
    database.reset();

    DeleteFactRecord(path, VertexFactKey(mutation.removed_sequence,
                                         10 + mutation.removed_sequence,
                                         mutation.removed_sequence));
    // deep=true cross-checks every fact referenced by a committed sequence.
    ExpectFactStoreOpenStatus(path, kReopenValidationPolicies[0], true,
                              "missing fact");
    // bounded=true with K=2 and visible=3 has floor=2: the recent mutation is
    // still checked, while the old one is intentionally left to read-time
    // detection.
    ExpectFactStoreOpenStatus(
        path, kReopenValidationPolicies[1], !mutation.bounded_accepts,
        mutation.bounded_accepts ? "" : "missing fact");
  }
}

TEST_F(RecoveryCrashMatrixTest, ReopenResumesVacuumWithoutAnIndependentFactsStore) {
  auto database = Open();
  ASSERT_TRUE(database);
  ASSERT_EQ(CommitVertex(*database, 1, 10).outcome, CommitOutcome::kCommitted);
  ASSERT_EQ(CommitVertex(*database, 1, 20).outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto faulted = Open(DatabaseOptions{
      .vacuum_fault_injector_for_testing = [](VacuumFaultPoint point) {
        return point == VacuumFaultPoint::kAfterBoundaryWrite
                   ? Status::Indeterminate("test", "vacuum crash")
                   : Status::OK();
      }});
  ASSERT_TRUE(faulted);
  EXPECT_TRUE(faulted->Vacuum(CommitSeq{2}).IsIndeterminate());
  faulted.reset();

  database = Open();
  ASSERT_TRUE(database);
  EXPECT_TRUE(database->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status()
                  .IsSnapshotExpired());
  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  EXPECT_TRUE(latest.ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{20})
                  .ValueOrDie());
  size_t events = 0;
  ASSERT_TRUE(latest.ValueOrDie()
                  .Scan(FactFamily::kVertexState, PropertyId{},
                        [&events](const FactEvent&) {
                          ++events;
                          return Status::OK();
                        })
                  .ok());
  EXPECT_EQ(events, 2U);
}

// F10 Task 10 / F20: the v2 writer (marker + kSequenceV2 records) must survive
// the same post-fsync crash boundary as any durable commit. The child opens a
// fresh v2 database (the only supported format), commits one transaction, and
// dies inside the post-write injector; the parent reopens with default options
// and must recover, validate, and keep writing v2.
TEST_F(RecoveryCrashMatrixTest,
       V2WriterRecoversPostFsyncCrashAndKeepsMarkerAuthoritative) {
#if CEDAR_THREAD_SANITIZER_ENABLED
  GTEST_SKIP() << "ThreadSanitizer cannot create threads after a multithreaded fork";
#endif
  const std::string path = path_ + "/v2_post_fsync";
  ASSERT_TRUE(std::filesystem::create_directories(path));
  constexpr int kCrashExitCode = 93;
  constexpr uint64_t kVertexId = 100;
  constexpr uint64_t kValidTime = 10;
  const pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    FactStoreOptions options;
    options.path = path;
    options.meta_sequence_format = kMetaSequenceFormatV2;
    // Fires after the WAL write/fsync returned: the marker written by Open and
    // the commit batch are durable, but nothing was published in this process.
    options.commit_fault_injector_for_testing = [kCrashExitCode]() -> Status {
      _exit(kCrashExitCode);
    };
    FactStore store(options);
    if (!store.Open().ok()) _exit(2);
    const auto committed = store.Commit(
        VertexStateStoreBatch(TxnId{1}, kValidTime, kVertexId, kValidTime));
    static_cast<void>(committed);
    _exit(5);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), kCrashExitCode);

  // Default options recover and validate the v2 record: the marker written with
  // the fresh-database initialization batch is authoritative.
  ExpectFactStoreRecovered(path, kReopenValidationPolicies[0], TxnId{1},
                           kVertexId, kValidTime, true);
  const auto marker = ReadRawMetaRecord(path, EncodeSequenceFormatKey());
  ASSERT_TRUE(marker.has_value());
  const auto decoded_marker = DecodeSequenceFormat(*marker);
  ASSERT_TRUE(decoded_marker.ok()) << decoded_marker.status().ToString();
  EXPECT_EQ(decoded_marker.ValueOrDie(), kMetaSequenceFormatV2);
  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  const auto record = ReadRawMetaRecord(path, sequence_key.ValueOrDie());
  ASSERT_TRUE(record.has_value());
  ASSERT_GE(record->size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>((*record)[1]), 9);  // kSequenceV2

  // A post-crash commit continues v2 through the recovered marker, and the next
  // reopen validates both records.
  {
    FactStoreOptions options;
    options.path = path;
    FactStore store(options);
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Commit(VertexStateStoreBatch(TxnId{2}, 20, 101, 20)).ok());
    ASSERT_TRUE(store.Close().ok());
  }
  const auto seq2_key = EncodeSequenceMetaKey(CommitSeq{2});
  ASSERT_TRUE(seq2_key.ok()) << seq2_key.status().ToString();
  const auto record2 = ReadRawMetaRecord(path, seq2_key.ValueOrDie());
  ASSERT_TRUE(record2.has_value());
  ASSERT_GE(record2->size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>((*record2)[1]), 9);  // kSequenceV2

  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    EXPECT_EQ(store.visible_seq(), CommitSeq{2});
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
}

}  // namespace
}  // namespace cedar
