#include <gtest/gtest.h>

#include "benchmarks/cedar_kernel_bench_options.h"

namespace cedar::benchmark {

TEST(BenchmarkQualificationTest, SuccessfulThirtySecondsIsWarmOnly) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 30;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 30;
  sample.reopen_verified = true;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample), "warm_not_sustained");
}

TEST(BenchmarkQualificationTest, SustainedRunDoesNotUseThroughputFloor) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 1'800;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 1'800;
  sample.reopen_verified = true;
  sample.operations_per_second = 1.0;
  sample.n_plus_one_eligible_epochs = 1;
  sample.n_plus_one_promoted_epochs = 1;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample), "sustained_local_gates_passed");
}

TEST(BenchmarkQualificationTest, UnexplainedAutonomousJobFailsClosed) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 1'800;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 1'800;
  sample.reopen_verified = true;
  sample.n_plus_one_eligible_epochs = 1;
  sample.n_plus_one_promoted_epochs = 1;
  sample.unexplained_autonomous_jobs = 1;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample),
            "sustained_unexplained_autonomous_maintenance");
}

TEST(BenchmarkQualificationTest, WriterFailureFailsClosed) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 1'800;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 1'800;
  sample.reopen_verified = true;
  sample.n_plus_one_eligible_epochs = 1;
  sample.n_plus_one_promoted_epochs = 1;
  sample.writer_failures = 1;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample),
            "sustained_writer_failure");
}

TEST(BenchmarkOptionsTest, RequiresAbsolutePath) {
  EXPECT_FALSE(ParseKernelBenchmarkOptions({"--path", "relative"}).ok());
  const auto parsed = ParseKernelBenchmarkOptions({"--path", "/tmp/cedar"});
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
}

TEST(BenchmarkOptionsTest, RejectsRemovedExecutionProfileOption) {
  EXPECT_FALSE(ParseKernelBenchmarkOptions(
                   {"--path", "/tmp/cedar", "--profile", "lean"})
                   .ok());
  EXPECT_FALSE(ParseKernelBenchmarkOptions(
                   {"--path", "/tmp/cedar", "--profile", "kernel"})
                   .ok());
}

TEST(BenchmarkOptionsTest, ParsesKernelWorkloadNames) {
  const auto parsed = ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--workload", "projected-event-scan"});
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.ValueOrDie().workload, KernelWorkload::kProjectedEventScan);
  EXPECT_STREQ(KernelWorkloadName(parsed.ValueOrDie().workload), "projected-event-scan");
}

TEST(BenchmarkOptionsTest, RejectsUnknownKernelWorkload) {
  EXPECT_FALSE(ParseKernelBenchmarkOptions(
                   {"--path", "/tmp/cedar", "--workload", "legacy"})
                   .ok());
}

TEST(BenchmarkOptionsTest, ValidatesCampaignDurations) {
  EXPECT_TRUE(ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--campaign", "smoke", "--operations", "2048"})
                  .ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--campaign", "warm", "--duration-seconds", "30"})
                  .ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--campaign", "preflight", "--duration-seconds", "300"})
                  .ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--campaign", "preflight", "--duration-seconds", "60"})
                  .ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--campaign", "sustained", "--duration-seconds", "1800"})
                  .ok());
  EXPECT_FALSE(ParseKernelBenchmarkOptions(
                   {"--path", "/tmp/cedar", "--campaign", "sustained",
                    "--duration-seconds", "1799"})
                   .ok());
}

TEST(BenchmarkQualificationTest, PreflightFailureStopsTheCampaign) {
  KernelBenchmarkOptions options;
  options.campaign = CampaignKind::kPreflight;
  options.duration_seconds = 300;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 300;
  sample.reopen_verified = true;
  sample.background_errors = 1;
  EXPECT_NE(CampaignExitCode(options, sample), 0);
}

TEST(BenchmarkQualificationTest, WarmSuccessDoesNotClaimSustainedButCanProceed) {
  KernelBenchmarkOptions options;
  options.campaign = CampaignKind::kWarm;
  options.duration_seconds = 30;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 30;
  sample.reopen_verified = true;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample), "warm_not_sustained");
  EXPECT_EQ(CampaignExitCode(options, sample), 0);
}

TEST(BenchmarkQualificationTest, ReopenVerificationCanBeDisabled) {
  KernelBenchmarkOptions options;
  options.campaign = CampaignKind::kWarm;
  options.duration_seconds = 30;
  options.verify_reopen = false;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 30;
  EXPECT_EQ(CampaignExitCode(options, sample), 0);
}

TEST(BenchmarkQualificationTest, MaintenanceDebtFailsClosed) {
  KernelBenchmarkOptions options;
  options.campaign = CampaignKind::kPreflight;
  options.duration_seconds = 300;
  KernelBenchmarkSample sample;
  sample.elapsed_seconds = 300;
  sample.reopen_verified = true;
  sample.pending_compaction_bytes = 32ULL * 1024ULL * 1024ULL * 1024ULL;
  EXPECT_NE(CampaignExitCode(options, sample), 0);
}

TEST(BenchmarkOptionsTest, ParsesBoundedWriterClients) {
  const auto parsed = ParseKernelBenchmarkOptions(
      {"--path", "/tmp/cedar", "--writer-clients", "32"});
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.ValueOrDie().writer_clients, 32U);
}

TEST(BenchmarkOptionsTest, RejectsUnboundedWriterClients) {
  EXPECT_FALSE(ParseKernelBenchmarkOptions(
                   {"--path", "/tmp/cedar", "--writer-clients", "0"})
                   .ok());
  EXPECT_FALSE(ParseKernelBenchmarkOptions(
                   {"--path", "/tmp/cedar", "--writer-clients", "33"})
                   .ok());
}

TEST(BenchmarkOptionsTest, RejectsSeedAndDestinationAlias) {
  EXPECT_FALSE(ParseKernelBenchmarkOptions({
                   "--seed-db", "/tmp/cedar-seed", "--database-path",
                   "/tmp/cedar-seed", "--prepare-seed", "true"})
                   .ok());
}

}  // namespace cedar::benchmark
