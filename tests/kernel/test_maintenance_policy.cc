#include <gtest/gtest.h>

#include "kernel/maintenance_policy.h"

namespace cedar {
namespace {

CedarRuntimeSnapshot SnapshotWithDebt(
    RocksDbRuntimeMetrics::ColumnFamilyRole role, uint64_t active,
    uint64_t immutable, uint64_t l0_files, uint64_t pending_compaction_bytes) {
  CedarRuntimeSnapshot snapshot;
  snapshot.rocksdb.write_buffer_manager_limit_bytes = 1ULL << 30;
  snapshot.rocksdb.write_buffer_manager_bytes = active + immutable;
  snapshot.rocksdb.total_active_memtable_bytes = active;
  snapshot.rocksdb.total_immutable_memtable_bytes = immutable;
  snapshot.rocksdb.total_l0_files = l0_files;
  snapshot.rocksdb.total_pending_compaction_bytes = pending_compaction_bytes;
  snapshot.rocksdb.column_families.push_back(
      RocksDbRuntimeMetrics::ColumnFamilyMetrics{
          .role = role,
          .active_memtable_bytes = active,
          .immutable_memtable_bytes = immutable,
          .l0_files = l0_files,
          .pending_compaction_bytes = pending_compaction_bytes,
      });
  snapshot.rocksdb.column_families.push_back(
      RocksDbRuntimeMetrics::ColumnFamilyMetrics{
          .role = RocksDbRuntimeMetrics::ColumnFamilyRole::kDefault,
      });
  snapshot.rocksdb.column_families.push_back(
      RocksDbRuntimeMetrics::ColumnFamilyMetrics{
          .role = RocksDbRuntimeMetrics::ColumnFamilyRole::kOther,
      });
  return snapshot;
}

TEST(CedarMaintenancePolicyTest, FlushGrantCoversTheWholeImmutableMemtableUnit) {
  const CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 16ULL << 20,
      128ULL << 20, 0, 0);

  const CedarMaintenancePlan plan =
      SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false);

  ASSERT_TRUE(plan.flush.has_value());
  EXPECT_EQ(plan.flush->priority, CedarMaintenancePriority::kEmergency);
  EXPECT_GE(plan.flush->max_input_bytes, 144ULL << 20);
}

TEST(CedarMaintenancePolicyTest,
     WriteStoppedWithActiveMemtableRequestsEmergencyFlush) {
  CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 16ULL << 20, 0, 0, 0);
  snapshot.rocksdb.write_stopped = 1;

  const CedarMaintenancePlan plan =
      SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false);

  ASSERT_TRUE(plan.flush.has_value());
  EXPECT_FALSE(plan.compaction.has_value());
  EXPECT_EQ(plan.flush->priority, CedarMaintenancePriority::kEmergency);
  EXPECT_FALSE(plan.flush->yield_for_wal_sync);
  EXPECT_GE(plan.flush->max_input_bytes, 16ULL << 20);
}

TEST(CedarMaintenancePolicyTest, FlushesMetaDebtWithoutFactsDebt) {
  const CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kMeta, 0, 1ULL << 20, 0, 0);

  const CedarMaintenancePlan plan =
      SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false);

  ASSERT_TRUE(plan.flush.has_value());
  EXPECT_EQ(plan.flush->kind, CedarMaintenanceKind::kFlush);
  EXPECT_EQ(plan.flush->priority, CedarMaintenancePriority::kNormal);
  EXPECT_FALSE(plan.compaction.has_value());
}

TEST(CedarMaintenancePolicyTest, EmergencyWbmFlushAndCompactionUseSeparateLanes) {
  CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 10, 80, 4, 1);
  snapshot.rocksdb.write_buffer_manager_limit_bytes = 100;

  const CedarMaintenancePlan plan =
      SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false);

  ASSERT_TRUE(plan.flush.has_value());
  EXPECT_EQ(plan.flush->priority, CedarMaintenancePriority::kEmergency);
  ASSERT_TRUE(plan.compaction.has_value());
  EXPECT_EQ(plan.compaction->priority, CedarMaintenancePriority::kNormal);
}

TEST(CedarMaintenancePolicyTest,
     EmergencyWbmFlushDoesNotYieldForWalCriticalWorkBeforeWriteStop) {
  CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 10, 80, 0, 0);
  snapshot.rocksdb.write_buffer_manager_limit_bytes = 100;

  const CedarMaintenancePlan plan =
      SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, true);

  ASSERT_TRUE(plan.flush.has_value());
  EXPECT_EQ(plan.flush->priority, CedarMaintenancePriority::kEmergency);
  EXPECT_FALSE(plan.flush->yield_for_wal_sync);
}

TEST(CedarMaintenancePolicyTest, WalCriticalSuppressesNormalCompaction) {
  const CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 0, 0, 4, 1);

  const CedarMaintenancePlan plan =
      SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, true);

  EXPECT_FALSE(plan.flush.has_value());
  EXPECT_FALSE(plan.compaction.has_value());
}

TEST(CedarMaintenancePolicyTest, ManualRecoveryAndShutdownSuppressAllGrants) {
  CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 10, 80, 4, 1);
  snapshot.rocksdb.write_buffer_manager_limit_bytes = 100;
  snapshot.rocksdb.manual_conflict = true;
  EXPECT_FALSE(SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false)
                   .flush.has_value());

  snapshot.rocksdb.manual_conflict = false;
  snapshot.rocksdb.recovery_in_progress = true;
  EXPECT_FALSE(SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false)
                   .compaction.has_value());

  snapshot.rocksdb.recovery_in_progress = false;
  snapshot.rocksdb.shutting_down = true;
  EXPECT_FALSE(SelectCedarMaintenance(snapshot, CedarMaintenanceHistory{}, false)
                   .flush.has_value());
}

TEST(CedarMaintenancePolicyTest, BudgetFeedbackCoversNextCompleteUnit) {
  const CedarRuntimeSnapshot snapshot = SnapshotWithDebt(
      RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts, 0, 0, 4, 1);
  CedarMaintenanceHistory history;
  history.last_compaction = CedarMaintenanceCompletion{
      .kind = CedarMaintenanceKind::kCompaction,
      .yield = CedarMaintenanceYield::kInputBudget,
      .remaining_smallest_complete_unit_bytes = 32ULL << 20,
  };

  const CedarMaintenancePlan plan = SelectCedarMaintenance(snapshot, history, false);

  ASSERT_TRUE(plan.compaction.has_value());
  EXPECT_EQ(plan.compaction->max_input_bytes, 32ULL << 20);
}

}  // namespace
}  // namespace cedar
