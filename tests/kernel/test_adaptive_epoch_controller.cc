#include <gtest/gtest.h>

#include "kernel/adaptive_epoch_controller.h"

namespace cedar::internal {
namespace {

AdaptiveEpochController::Options Options() {
  return {.max_transactions = 128,
          .max_encoded_bytes = 2ULL * 1024ULL * 1024ULL,
          .latency_slo_us = 5'000,
          .maximum_collection_age_us = 200};
}

TEST(AdaptiveEpochControllerTest, LoneRequestWaitsOnlyForTheConfiguredAge) {
  AdaptiveEpochController controller(Options());
  const EpochLimits limits = controller.NextLimits(
      EpochQueueSnapshot{.depth = 1, .oldest_age_us = 0,
                         .pressure_state = PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 1U);
  EXPECT_EQ(limits.max_age_us, 200U);
}

TEST(AdaptiveEpochControllerTest, CollectionNeverRestartsAnOldRequestsWindow) {
  AdaptiveEpochController controller(Options());
  EXPECT_EQ(controller.NextLimits({1, 150, PressureState::kNormal}).max_age_us, 50U);
  EXPECT_EQ(controller.NextLimits({1, 200, PressureState::kNormal}).max_age_us, 0U);
  EXPECT_EQ(controller.NextLimits({1, 300, PressureState::kNormal}).max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, AGroupCappedAtOneDoesNotCollectAnotherTransaction) {
  auto options = Options();
  options.max_transactions = 1;
  AdaptiveEpochController controller(options);
  EXPECT_EQ(controller.NextLimits({1, 0, PressureState::kNormal}).max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, ObservedZeroSyncCostDispatchesLoneWorkPromptly) {
  AdaptiveEpochController controller(Options());
  controller.Observe({0, 0, 1, 1024});
  EXPECT_EQ(controller.NextLimits({1, 0, PressureState::kNormal}).max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SyncCostRetainsCollectionForLoneWork) {
  AdaptiveEpochController controller(Options());
  controller.Observe({1000, 0, 1, 1024});
  EXPECT_EQ(controller.NextLimits({1, 150, PressureState::kNormal}).max_age_us, 50U);
}

TEST(AdaptiveEpochControllerTest, MissingSyncMetricsDoNotMeanZeroSyncCost) {
  AdaptiveEpochController controller({});
  controller.Observe({0, 0, 1, 1024, false});
  EXPECT_EQ(controller.NextLimits({1, 0, PressureState::kNormal}).max_age_us, 200U);
}

TEST(AdaptiveEpochControllerTest, DeepHealthyQueueUsesTheBoundedMaximum) {
  AdaptiveEpochController controller(Options());
  const EpochLimits limits = controller.NextLimits(
      EpochQueueSnapshot{.depth = 128, .oldest_age_us = 10,
                         .pressure_state = PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 128U);
  EXPECT_EQ(limits.max_encoded_bytes, 2ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SlowWalSyncKeepsADeepYoungQueueIntact) {
  AdaptiveEpochController controller(Options());
  for (uint32_t sample = 0; sample < 4; ++sample) {
    controller.Observe(EpochObservation{.wal_sync_us = 10'000,
                                        .queue_p99_us = 1'000,
                                        .transactions = 128,
                                        .encoded_bytes = 2ULL * 1024ULL * 1024ULL});
  }
  const EpochLimits limits = controller.NextLimits(
      EpochQueueSnapshot{.depth = 128, .oldest_age_us = 10,
                         .pressure_state = PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 128U);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SlowWalWithDeepQueueKeepsAmortizingGroup) {
  AdaptiveEpochController controller({128, 2ULL << 20, 5'000, 200, 16, 16});
  for (int i = 0; i < 4; ++i) {
    controller.Observe({8'000, 1'000, 64, 64 * 1024});
  }
  const auto limits = controller.NextLimits({64, 100, PressureState::kNormal});
  EXPECT_GE(limits.max_transactions, 64U);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SettleAgeFollowsObservedSlowSyncCost) {
  AdaptiveEpochController controller(
      {128, 2ULL << 20, 5'000, 200, 2, 16, 5'000, 8'000, 8});
  controller.Observe({60'000, 0, 1, 1024});
  EXPECT_EQ(controller.NextLimits({2, 0, PressureState::kNormal}).settle_age_us,
            7'500U);
}

TEST(AdaptiveEpochControllerTest, SettleAgeIsCappedAndGated) {
  AdaptiveEpochController capped(
      {128, 2ULL << 20, 5'000, 200, 2, 16, 5'000, 8'000, 8});
  capped.Observe({200'000, 0, 1, 1024});
  EXPECT_EQ(capped.NextLimits({2, 0, PressureState::kNormal}).settle_age_us,
            8'000U);
  AdaptiveEpochController fast(
      {128, 2ULL << 20, 5'000, 200, 2, 16, 5'000, 8'000, 8});
  fast.Observe({1'000, 0, 1, 1024});
  EXPECT_EQ(fast.NextLimits({2, 0, PressureState::kNormal}).settle_age_us, 0U);
  AdaptiveEpochController disabled(
      {128, 2ULL << 20, 5'000, 200, 2, 16, 0, 8'000, 8});
  disabled.Observe({60'000, 0, 1, 1024});
  EXPECT_EQ(disabled.NextLimits({2, 0, PressureState::kNormal}).settle_age_us,
            0U);
}

TEST(AdaptiveEpochControllerTest, SettleDoesNotApplyUnderPressure) {
  AdaptiveEpochController controller(
      {128, 2ULL << 20, 5'000, 200, 2, 16, 5'000, 8'000, 8});
  controller.Observe({60'000, 0, 1, 1024});
  EXPECT_EQ(controller.NextLimits({64, 10, PressureState::kSoft}).settle_age_us,
            0U);
  EXPECT_EQ(controller.NextLimits({64, 10, PressureState::kHard}).settle_age_us,
            0U);
}

TEST(AdaptiveEpochControllerTest, ShallowQueueStillHonorsLatencyWindow) {
  AdaptiveEpochController controller(Options());
  const auto limits = controller.NextLimits({1, 100, PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 1U);
  EXPECT_GT(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SoftPressureConstrainsBytesAndAge) {
  AdaptiveEpochController controller(Options());
  const EpochLimits limits = controller.NextLimits(
      EpochQueueSnapshot{.depth = 64, .oldest_age_us = 10,
                         .pressure_state = PressureState::kSoft});
  EXPECT_EQ(limits.max_transactions, 64U);
  EXPECT_EQ(limits.max_encoded_bytes, 1ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(limits.max_age_us, 40U);
}

TEST(AdaptiveEpochControllerTest, SlowWalKeepsAnAgedShallowGroupIntact) {
  AdaptiveEpochController controller(Options());
  controller.Observe({50'000, 0, 8, 8 * 1024});
  const auto limits = controller.NextLimits({8, 50'000, PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 8U);
  EXPECT_EQ(limits.max_encoded_bytes, 8 * 1024U);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SlowWalKeepsAnAgedDeepGroupBounded) {
  AdaptiveEpochController controller({32, 64 * 1024, 5'000, 200});
  controller.Observe({5'000, 0, 8, 8 * 8192});
  const auto limits = controller.NextLimits({256, 50'000, PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 32U);
  EXPECT_EQ(limits.max_encoded_bytes, 64 * 1024U);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, FastWalStillShrinksAnAgedGroup) {
  AdaptiveEpochController controller(Options());
  controller.Observe({100, 0, 8, 8 * 1024});
  const auto limits = controller.NextLimits({8, 50'000, PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 4U);
  EXPECT_EQ(limits.max_encoded_bytes, 4 * 1024U);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, UnobservedWalStillShrinksAnAgedGroup) {
  AdaptiveEpochController controller(Options());
  const auto limits = controller.NextLimits({8, 50'000, PressureState::kNormal});
  EXPECT_EQ(limits.max_transactions, 4U);
  EXPECT_EQ(limits.max_encoded_bytes, 1ULL << 20);
  EXPECT_EQ(limits.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, SlowWalRetainsPressureProtection) {
  AdaptiveEpochController controller(Options());
  controller.Observe({50'000, 0, 8, 8 * 1024});
  const auto soft = controller.NextLimits({256, 50'000, PressureState::kSoft});
  EXPECT_EQ(soft.max_transactions, 64U);
  EXPECT_EQ(soft.max_encoded_bytes, 1ULL << 20);
  EXPECT_EQ(soft.max_age_us, 0U);
  const auto hard = controller.NextLimits({256, 50'000, PressureState::kHard});
  EXPECT_EQ(hard.max_transactions, 1U);
  EXPECT_EQ(hard.max_encoded_bytes, 512ULL << 10);
  EXPECT_EQ(hard.max_age_us, 0U);
}

TEST(AdaptiveEpochControllerTest, WalEstimateAdaptsAfterStorageRecovers) {
  AdaptiveEpochController controller(Options());
  controller.Observe({50'000, 0, 8, 8 * 1024});
  EXPECT_EQ(controller.NextLimits({8, 50'000, PressureState::kNormal})
                .max_transactions, 8U);
  for (int i = 0; i < 32; ++i) {
    controller.Observe({100, 0, 8, 8 * 1024});
  }
  EXPECT_EQ(controller.NextLimits({8, 50'000, PressureState::kNormal})
                .max_transactions, 4U);
}

}  // namespace
}  // namespace cedar::internal
