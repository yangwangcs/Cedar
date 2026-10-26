// L0 reader-metadata boundary regression (W1.4).
//
// Reopen validation pins a Cedar Parquet table reader for every facts L0 SST it
// scans. The kernel-test profile allows up to 24 L0 files before write
// slowdown, so the reader-metadata pool must cover more than one or two
// concurrent readers. The 2026-10-25 audit saw a reopen fail with
// "working budget exhausted" while the working pool was empty: the metadata
// pool (automatic 64 KiB) was the real limit and the rejection text named the
// wrong pool.
//
// This test builds exactly 6, 8 and 12 facts L0 SSTs under the kernel-test
// profile without changing engine internals:
//   * Cedar kernel mode schedules a flush only for an explicit maintenance
//     grant, and a compaction only for its own grant, so granting flushes
//     accumulates one L0 SST per switch and never compacts them away.
//   * Each iteration commits small fact batches until one memtable switches,
//     then drains queued flush debt with kFlush grants.
// The store is then closed and reopened with the budgets below, and the reopen
// metadata peak/rejections are recorded.
//
// Measured boundary (Debug, 2026-10-25): one retained reader reserves about
// 19-22 KiB (reader object + 8 KiB open overhead + footer), and closing the
// store flushes the partially filled active MemTable, so a constructed run of N
// L0 SSTs reopens with N+1 (the recorded XML reports both counts). The 160 KiB
// split of the 1 MiB profile barely covers seven reopened readers
// (peak 158900 B of 163840 B) and rejects nine. Eight and twelve therefore
// re-split the same 672 KiB read-pool envelope into 256 KiB metadata plus
// 416 KiB working, and twelve (256/416 or 288/384) still exceeds it. Those two
// rejections are asserted explicitly so the limits are tracked boundaries
// rather than surprises, and the profile budget is never raised.
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>

#include <unistd.h>

#include "storage/facts/fact_store.h"

namespace cedar {
namespace {

constexpr uint64_t kFactsPerCommit = 64;

struct BoundaryCase {
  uint64_t l0_files;
  uint64_t reader_metadata_bytes;
  uint64_t read_working_bytes;
};

StoreCommitBatch VertexPutBatch(TxnId txn_id, uint64_t first_vertex) {
  StoreCommitBatch batch{txn_id, 1, {}, {}, {}, {}, false};
  batch.mutations.reserve(kFactsPerCommit);
  for (uint64_t index = 0; index < kFactsPerCommit; ++index) {
    batch.mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{first_vertex + index}})
            .ref(),
        ValidTime{1}, FactOperation::kPut, 0, std::nullopt});
  }
  return batch;
}

uint64_t FactsL0Files(const RocksDbRuntimeMetrics& metrics) {
  for (const auto& family : metrics.column_families) {
    if (family.role == RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts) {
      return family.l0_files;
    }
  }
  return 0;
}

class L0ReaderMetadataBoundaryTest : public testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_l0_metadata_boundary_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  FactStoreOptions StoreOptions(const BoundaryCase& test_case) const {
    FactStoreOptions options{path_};
    options.storage_profile = StorageProfile::kKernelTest;
    options.storage_read_working_bytes = test_case.read_working_bytes;
    options.storage_reader_metadata_bytes = test_case.reader_metadata_bytes;
    return options;
  }

  // Commits until one memtable switches, then drains every queued flush with
  // explicit kFlush grants. No compaction grant is issued, so the facts L0
  // count grows by one per iteration.
  void AddOneFactsL0(FactStore* store, TxnId* next_txn, uint64_t* next_vertex) {
    bool switched = false;
    for (int guard = 0; guard < 8192 && !switched; ++guard) {
      const auto metrics = store->SampleRuntimeMetrics();
      ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
      if (metrics.ValueOrDie().total_immutable_memtable_count > 0) {
        switched = true;
        break;
      }
      const auto committed =
          store->Commit(VertexPutBatch(*next_txn, *next_vertex));
      ASSERT_TRUE(committed.ok()) << committed.status().ToString();
      *next_txn = TxnId{next_txn->value + 1};
      *next_vertex += kFactsPerCommit;
    }
    ASSERT_TRUE(switched) << "no memtable switch after 8192 commits";
    DrainQueuedFlushes(store);
  }

  void DrainQueuedFlushes(FactStore* store) {
    for (int guard = 0; guard < 256; ++guard) {
      const auto metrics = store->SampleRuntimeMetrics();
      ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
      const auto& current = metrics.ValueOrDie();
      if (current.total_immutable_memtable_count == 0 &&
          current.flush_queue_depth == 0 && current.scheduled_flushes == 0 &&
          current.running_flushes == 0) {
        return;
      }
      FactStoreMaintenanceRequest request;
      request.kind = FactStoreMaintenanceKind::kFlush;
      request.snapshot_generation = current.maintenance_generation;
      request.max_input_bytes = 4ULL << 20;
      request.max_output_bytes = 4ULL << 20;
      request.deadline_us = 5'000'000;
      const auto flushed = store->RunNativeMaintenance(request, nullptr);
      if (flushed.ok()) continue;
      ASSERT_TRUE(flushed.status().IsMaintenanceBackoff())
          << flushed.status().ToString();
    }
    FAIL() << "flush grants did not drain the queued debt";
  }

  // Builds the requested facts L0 count, closes the store and reports the
  // highest vertex written (used to verify the reopened store's data).
  void BuildAndClose(const BoundaryCase& test_case, uint64_t* last_vertex_out) {
    FactStore store(StoreOptions(test_case));
    const Status opened = store.Open();
    ASSERT_TRUE(opened.ok()) << opened.ToString();
    TxnId next_txn{1};
    uint64_t next_vertex = 1;
    uint64_t l0_files = 0;
    for (uint64_t round = 0; round < test_case.l0_files + 32; ++round) {
      const auto metrics = store.SampleRuntimeMetrics();
      ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
      l0_files = FactsL0Files(metrics.ValueOrDie());
      if (l0_files >= test_case.l0_files) break;
      AddOneFactsL0(&store, &next_txn, &next_vertex);
    }
    ASSERT_EQ(l0_files, test_case.l0_files)
        << "facts L0 construction did not reach the target";
    const Status closed = store.Close();
    ASSERT_TRUE(closed.ok()) << closed.ToString();
    *last_vertex_out = next_vertex;
  }

  void RecordObservation(const BoundaryCase& test_case, uint64_t reopened_l0,
                         const ValidationCacheMetrics& metrics) {
    // Recorded so a gate can capture the values with --gtest_output=xml.
    const auto record = [this](const char* name, uint64_t value) {
      RecordProperty(name, static_cast<int64_t>(value));
    };
    record("target_l0_files", test_case.l0_files);
    record("reopened_l0_files", reopened_l0);
    record("metadata_limit_bytes", metrics.storage_reader_metadata_limit_bytes);
    record("metadata_peak_bytes", metrics.storage_reader_metadata_peak_bytes);
    record("metadata_reserved_bytes",
           metrics.storage_reader_metadata_reserved_bytes);
    record("metadata_rejections", metrics.storage_reader_metadata_rejections);
    record("parquet_metadata_peak_bytes",
           metrics.parquet_reader_metadata_peak_bytes);
    record("working_limit_bytes", test_case.read_working_bytes);
    record("working_peak_bytes", metrics.storage_read_working_peak_bytes);
  }

  void RunPassingBoundary(const BoundaryCase& test_case) {
    uint64_t last_vertex = 0;
    BuildAndClose(test_case, &last_vertex);

    // Reopen with a fresh reader-metadata pool. Deep recovery validation scans
    // every retained fact, so every L0 reader is opened and pinned.
    FactStore reopened{StoreOptions(test_case)};
    const auto opened = reopened.Open();
    ASSERT_TRUE(opened.ok()) << opened.ToString()
                             << " l0=" << test_case.l0_files;
    const auto reopened_metrics = reopened.SampleRuntimeMetrics();
    ASSERT_TRUE(reopened_metrics.ok())
        << reopened_metrics.status().ToString();
    const uint64_t reopened_l0 = FactsL0Files(reopened_metrics.ValueOrDie());
    // Closing may flush the partially filled active facts MemTable, adding at
    // most one L0 SST. The boundary under test is "at least target L0 readers".
    EXPECT_GE(reopened_l0, test_case.l0_files);
    EXPECT_LE(reopened_l0, test_case.l0_files + 1);

    const auto validation = reopened.SampleValidationCacheMetrics();
    ASSERT_TRUE(validation.ok()) << validation.status().ToString();
    const auto& metrics = validation.ValueOrDie();
    EXPECT_EQ(metrics.storage_reader_metadata_limit_bytes,
              test_case.reader_metadata_bytes);
    EXPECT_EQ(metrics.storage_reader_metadata_rejections, 0U)
        << "l0=" << test_case.l0_files
        << " peak=" << metrics.storage_reader_metadata_peak_bytes;
    EXPECT_GT(metrics.storage_reader_metadata_peak_bytes, 0U);
    EXPECT_LE(metrics.storage_reader_metadata_peak_bytes,
              metrics.storage_reader_metadata_limit_bytes);
    EXPECT_GT(metrics.parquet_reader_metadata_peak_bytes, 0U);
    EXPECT_EQ(metrics.storage_read_working_rejections, 0U);
    RecordObservation(test_case, reopened_l0, metrics);

    {
      // The reopened store still returns the retained facts. Release the
      // snapshot before Close: a live snapshot pins an epoch by design.
      auto snapshot = reopened.BeginSnapshot({});
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      const FactRef ref =
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{last_vertex - 1}})
              .ref();
      const auto read = reopened.Read(snapshot.ValueOrDie(), ref, ValidTime{1});
      ASSERT_TRUE(read.ok()) << read.status().ToString();
      ASSERT_TRUE(read.ValueOrDie().has_value())
          << "l0=" << test_case.l0_files;
    }
    const Status closed = reopened.Close();
    EXPECT_TRUE(closed.ok()) << closed.ToString();
  }

  std::string path_;
};

TEST_F(L0ReaderMetadataBoundaryTest,
       ReopenAfterSixFactsL0SstUnder160KiBMetadata) {
  RunPassingBoundary({6, 160ULL << 10, 512ULL << 10});
}

TEST_F(L0ReaderMetadataBoundaryTest,
       ReopenAfterEightFactsL0SstUnder256KiBMetadata) {
  RunPassingBoundary({8, 256ULL << 10, 416ULL << 10});
}

// Twelve constructed L0 SSTs (thirteen reopened readers) do not fit the
// kernel-test read-pool envelope. The 2026-10-25 measurement (Debug) shows the
// largest single working reservation exceeding ~157 KiB on top of about
// 231 KiB already reserved, while metadata reaches about 272 KiB: together they
// exceed the 672 KiB left for both pools after the fixed 352 KiB of the 1 MiB
// profile. The boundary is asserted as an explicit budget rejection so a future
// capacity change cannot silently turn it into a corruption or a hang.
TEST_F(L0ReaderMetadataBoundaryTest,
       TwelveFactsL0ExceedTheOneMiBReadPoolEnvelope) {
  const BoundaryCase test_case{12, 288ULL << 10, 384ULL << 10};
  uint64_t last_vertex = 0;
  BuildAndClose(test_case, &last_vertex);
  FactStore reopened{StoreOptions(test_case)};
  const Status opened = reopened.Open();
  ASSERT_FALSE(opened.ok())
      << "twelve L0 readers unexpectedly fit the 672 KiB read-pool envelope";
  const std::string message = opened.ToString();
  EXPECT_TRUE(opened.IsResourceExhausted()) << message;
  const bool names_metadata =
      message.find("reader metadata budget exhausted") != std::string::npos;
  const bool names_working =
      message.find("read working budget exhausted") != std::string::npos;
  EXPECT_TRUE(names_metadata || names_working) << message;
  EXPECT_FALSE(names_metadata && names_working) << message;
}

// Documents the measured 160 KiB boundary instead of leaving it implicit: with
// eight constructed L0 SSTs (nine reopened readers) the reopen must fail, and
// the rejection must name the metadata pool (the audit's misleading message
// said read working).
TEST_F(L0ReaderMetadataBoundaryTest,
       EightFactsL0Rejects160KiBMetadataAndNamesTheMetadataPool) {
  const BoundaryCase test_case{8, 160ULL << 10, 512ULL << 10};
  uint64_t last_vertex = 0;
  BuildAndClose(test_case, &last_vertex);
  FactStore reopened{StoreOptions(test_case)};
  const Status opened = reopened.Open();
  ASSERT_FALSE(opened.ok()) << "160 KiB unexpectedly covered eight L0 readers";
  const std::string message = opened.ToString();
  EXPECT_TRUE(opened.IsResourceExhausted()) << message;
  EXPECT_NE(message.find("reader metadata budget exhausted"), std::string::npos)
      << message;
  EXPECT_EQ(message.find("read working budget exhausted"), std::string::npos)
      << message;
  EXPECT_NE(message.find("rejected=0"), std::string::npos)
      << "the working pool was not the limiting pool: " << message;
}

}  // namespace
}  // namespace cedar
