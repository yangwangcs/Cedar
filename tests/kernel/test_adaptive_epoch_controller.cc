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
  EXPECT_EQ(limits.max_age_us, 50U);
}

}  // namespace
}  // namespace cedar::internal
