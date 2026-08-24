#include <gtest/gtest.h>

#include "cedar/runtime/pressure_controller.h"

namespace cedar {
namespace {

TEST(PressureControllerTest, RampsBusyTargetAcrossConfiguredSteps) {
  PressureController controller;
  EXPECT_EQ(controller.target_count(), 128U);
  PressureSample busy;
  busy.arrival_rate = 1000;
  busy.queue_depth = 128;
  busy.bytes_per_txn = 1024;
  busy.wal_sync_us = 1000;
  busy.memtable_us = 1000;
  busy.queue_age_us = 1000;
  for (int i = 0; i < 8; ++i) {
    controller.Observe(busy);
  }
  EXPECT_EQ(controller.target_count(), 256U);
  PressureSample idle;
  idle.arrival_rate = 1;
  controller.Observe(idle);
  EXPECT_EQ(controller.target_count(), 128U);
}

TEST(PressureControllerTest, HardPressureRequiresThreeHealthyRecoverySamples) {
  PressureController controller;
  controller.Observe(PressureSample{0, 24, 16ULL << 30, 0, 0, 0});
  EXPECT_EQ(controller.state(), PressureState::kHard);
  const PressureSample recovered{0, 12, 4ULL << 30, 0, 0, 0};
  controller.Observe(recovered);
  EXPECT_EQ(controller.state(), PressureState::kHard);
  controller.Observe(recovered);
  EXPECT_EQ(controller.state(), PressureState::kHard);
  controller.Observe(recovered);
  EXPECT_EQ(controller.state(), PressureState::kNormal);
}

TEST(PressureControllerTest, SustainedColumnarFlushDeficitEntersSoftPressure) {
  PressureController controller;
  PressureSample deficit;
  deficit.sample_interval_us = 1'000'000;
  deficit.admitted_facts_bytes_per_sec = 2ULL << 30;
  deficit.completed_background_bytes_per_sec = 0;
  for (int i = 0; i < 2; ++i) {
    controller.Observe(deficit);
    EXPECT_EQ(controller.state(), PressureState::kNormal);
  }
  controller.Observe(deficit);
  EXPECT_EQ(controller.state(), PressureState::kSoft);
  EXPECT_GE(controller.projected_debt_bytes(), 8ULL << 30);
}

TEST(PressureControllerTest,
     ShortPredictedDebtDoesNotStopAdmissionWithoutActualHardPressure) {
  PressureController controller;
  PressureSample deficit;
  deficit.sample_interval_us = 1'000'000;
  deficit.admitted_facts_bytes_per_sec = 8ULL << 30;
  deficit.completed_background_bytes_per_sec = 0;
  for (int i = 0; i < 6; ++i) controller.Observe(deficit);

  EXPECT_NE(controller.state(), PressureState::kHard);
  EXPECT_TRUE(controller.DecideAdmission(0, 0, 1).admit);
}

TEST(PressureControllerTest, ZeroDurationSampleDoesNotCreateThroughputPressure) {
  PressureController controller;
  PressureSample invalid_rate;
  invalid_rate.admitted_facts_bytes_per_sec = 100ULL << 30;
  for (int i = 0; i < 4; ++i) controller.Observe(invalid_rate);
  EXPECT_EQ(controller.state(), PressureState::kNormal);
  EXPECT_EQ(controller.admitted_rate_ewma(), 0U);
}

TEST(PressureControllerTest, AdmissionDecisionHonorsSoftAndHardPressure) {
  PressureController controller;
  controller.Observe(PressureSample{0, 16, 8ULL << 30, 0, 0, 0});
  auto soft = controller.DecideAdmission(1, 100, 1000);
  EXPECT_TRUE(soft.admit);
  EXPECT_LT(soft.max_count, 128U);
  controller.Observe(PressureSample{1, 24, 16ULL << 30, 0, 0, 0});
  auto hard = controller.DecideAdmission(1, 100, 1000);
  EXPECT_FALSE(hard.admit);
}

TEST(PressureControllerTest, DiskHeadroomEntersAndLeavesSoftPressure) {
  PressureController controller;
  PressureSample low_space;
  low_space.free_disk_bytes = 9ULL << 30;
  low_space.free_disk_percent = 20;
  controller.Observe(low_space);
  EXPECT_EQ(controller.state(), PressureState::kSoft);

  PressureSample recovered;
  recovered.free_disk_bytes = 12ULL << 30;
  recovered.free_disk_percent = 20;
  for (int i = 0; i < 3; ++i) controller.Observe(recovered);
  EXPECT_EQ(controller.state(), PressureState::kNormal);
}

TEST(PressureControllerTest, CriticalDiskHeadroomStopsAdmission) {
  PressureController controller;
  PressureSample critical;
  critical.free_disk_bytes = 3ULL << 30;
  critical.free_disk_percent = 20;
  controller.Observe(critical);
  EXPECT_EQ(controller.state(), PressureState::kHard);
  EXPECT_FALSE(controller.DecideAdmission(0, 0, 1).admit);
}

TEST(PressureControllerTest, RetainedWalHardLimitStopsAdmissionUntilHysteresisRecovers) {
  PressureController controller;
  PressureSample retained_wal;
  retained_wal.retained_wal_bytes = 1ULL << 30;
  controller.Observe(retained_wal);
  EXPECT_EQ(controller.state(), PressureState::kHard);
  EXPECT_FALSE(controller.DecideAdmission(0, 0, 1).admit);

  retained_wal.retained_wal_bytes = 512ULL << 20;
  controller.Observe(retained_wal);
  EXPECT_EQ(controller.state(), PressureState::kHard);
  controller.Observe(retained_wal);
  EXPECT_EQ(controller.state(), PressureState::kHard);
  controller.Observe(retained_wal);
  EXPECT_EQ(controller.state(), PressureState::kNormal);
  EXPECT_TRUE(controller.DecideAdmission(0, 0, 1).admit);
}

}  // namespace
}  // namespace cedar
