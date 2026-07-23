// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/regression_compare.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <numeric>
#include <sstream>

#include "cedar/observability/instrumentation_profile.h"

#include "cedar/blob/blob_store.h"

namespace cedar {
namespace {

class SplitMix64 {
 public:
  explicit SplitMix64(uint64_t seed) : state_(seed) {}
  uint64_t Next() {
    uint64_t value = (state_ += 0x9e3779b97f4a7c15ULL);
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  }
 private:
  uint64_t state_;
};

double Median(std::vector<double> values) {
  if (values.empty()) return 0.0;
  std::sort(values.begin(), values.end());
  const size_t middle = values.size() / 2;
  return values.size() % 2 == 0 ? (values[middle - 1] + values[middle]) / 2.0 : values[middle];
}

double CoefficientOfVariation(const std::vector<BenchmarkRegressionSample>& samples,
                              bool throughput) {
  if (samples.empty()) return 0.0;
  double sum = 0.0;
  for (const auto& sample : samples) sum += throughput ? sample.throughput : sample.p99_latency_ns;
  const double mean = sum / static_cast<double>(samples.size());
  if (mean <= 0.0) return std::numeric_limits<double>::infinity();
  double squared = 0.0;
  for (const auto& sample : samples) {
    const double delta = (throughput ? sample.throughput : sample.p99_latency_ns) - mean;
    squared += delta * delta;
  }
  return std::sqrt(squared / static_cast<double>(samples.size())) / mean;
}

struct Interval {
  double median = 0.0;
  double low = 0.0;
  double high = 0.0;
};

Interval BootstrapMedian(const std::vector<double>& paired_values, uint32_t resamples,
                         uint64_t seed) {
  Interval interval;
  interval.median = Median(paired_values);
  if (paired_values.empty() || resamples == 0) return interval;
  SplitMix64 random(seed);
  std::vector<double> bootstrapped;
  bootstrapped.reserve(resamples);
  std::vector<double> drawn(paired_values.size());
  for (uint32_t iteration = 0; iteration < resamples; ++iteration) {
    for (size_t index = 0; index < paired_values.size(); ++index) {
      drawn[index] = paired_values[random.Next() % paired_values.size()];
    }
    bootstrapped.push_back(Median(drawn));
  }
  std::sort(bootstrapped.begin(), bootstrapped.end());
  const size_t low_index = static_cast<size_t>(0.025 * static_cast<double>(resamples - 1));
  const size_t high_index = static_cast<size_t>(0.975 * static_cast<double>(resamples - 1));
  interval.low = bootstrapped[low_index];
  interval.high = bootstrapped[high_index];
  return interval;
}

bool HasResourceRegression(const std::vector<double>& baseline, const std::vector<double>& candidate,
                           double threshold, uint32_t resamples, uint64_t seed) {
  std::vector<double> relative;
  relative.reserve(baseline.size());
  for (size_t index = 0; index < baseline.size(); ++index) {
    if (baseline[index] <= 0.0) return true;
    relative.push_back(candidate[index] / baseline[index] - 1.0);
  }
  return BootstrapMedian(relative, resamples, seed).low > threshold;
}

std::string Number(double value) {
  std::ostringstream output;
  output << std::fixed << std::setprecision(4) << value;
  return output.str();
}

}  // namespace

std::string BenchmarkBaselineKeyId(const BenchmarkBaselineKey& key) {
  std::ostringstream payload;
  payload << key.protocol_version << '\n' << key.hardware_profile << '\n' << key.dataset_hash
          << '\n' << key.workload_hash << '\n' << key.durability_mode << '\n'
          << key.cache_mode << '\n' << key.resource_profile_id << '\n'
          << key.database_format_version;
  return BlobHashHex(Blake3Hash(payload.str()));
}

std::vector<BenchmarkRunArm> BuildAlternatingPairedOrder(uint32_t pair_count, uint64_t seed) {
  SplitMix64 random(seed);
  std::vector<BenchmarkRunArm> order;
  order.reserve(static_cast<size_t>(pair_count) * 2);
  for (uint32_t index = 0; index < pair_count; ++index) {
    const bool candidate_first = (random.Next() & 1U) != 0;
    order.push_back(candidate_first ? BenchmarkRunArm::kCandidate : BenchmarkRunArm::kBaseline);
    order.push_back(candidate_first ? BenchmarkRunArm::kBaseline : BenchmarkRunArm::kCandidate);
  }
  return order;
}

BenchmarkRegressionResult ComparePairedBenchmarkRuns(
    const BenchmarkBaselineKey& expected_key, const BenchmarkBaselineKey& candidate_key,
    const std::vector<BenchmarkRegressionSample>& baseline,
    const std::vector<BenchmarkRegressionSample>& candidate,
    const BenchmarkRegressionPolicy& policy) {
  BenchmarkRegressionResult result;
  if (BenchmarkBaselineKeyId(expected_key) != BenchmarkBaselineKeyId(candidate_key)) {
    result.status = BenchmarkRegressionStatus::kIncompatible;
    result.detail = "baseline key differs (hardware, data, workload, durability, cache, resource, or format)";
    return result;
  }
  if (baseline.empty() || baseline.size() != candidate.size() ||
      policy.minimum_pair_count == 0 || policy.bootstrap_resamples == 0 ||
      policy.throughput_regression_fraction < 0.0 || policy.p99_regression_fraction < 0.0 ||
      policy.maximum_coefficient_of_variation < 0.0) {
    result.detail = "paired samples and a positive regression policy are required";
    return result;
  }
  std::vector<double> throughput;
  std::vector<double> p99;
  std::vector<double> memory_baseline;
  std::vector<double> memory_candidate;
  std::vector<double> amplification_baseline;
  std::vector<double> amplification_candidate;
  throughput.reserve(baseline.size());
  p99.reserve(baseline.size());
  for (size_t index = 0; index < baseline.size(); ++index) {
    const auto& base = baseline[index];
    const auto& next = candidate[index];
    if (!base.verification_passed || !next.verification_passed || base.throughput <= 0.0 ||
        next.throughput <= 0.0 || base.p99_latency_ns <= 0.0 || next.p99_latency_ns <= 0.0) {
      result.detail = "every paired sample must have passed verification and positive metrics";
      return result;
    }
    throughput.push_back(next.throughput / base.throughput - 1.0);
    p99.push_back(next.p99_latency_ns / base.p99_latency_ns - 1.0);
    memory_baseline.push_back(base.memory_bytes);
    memory_candidate.push_back(next.memory_bytes);
    amplification_baseline.push_back(base.amplification);
    amplification_candidate.push_back(next.amplification);
  }
  if (baseline.size() < policy.minimum_pair_count) {
    result.detail = "paired samples require at least " +
        std::to_string(policy.minimum_pair_count) + " valid repetitions";
    return result;
  }
  result.baseline_cv = std::max(CoefficientOfVariation(baseline, true),
                                CoefficientOfVariation(baseline, false));
  result.candidate_cv = std::max(CoefficientOfVariation(candidate, true),
                                 CoefficientOfVariation(candidate, false));
  if (result.baseline_cv > policy.maximum_coefficient_of_variation ||
      result.candidate_cv > policy.maximum_coefficient_of_variation) {
    result.status = BenchmarkRegressionStatus::kNoisy;
    result.detail = "coefficient of variation exceeds policy: baseline=" + Number(result.baseline_cv) +
        " candidate=" + Number(result.candidate_cv);
    return result;
  }
  const Interval throughput_interval = BootstrapMedian(throughput, policy.bootstrap_resamples,
                                                        policy.bootstrap_seed);
  const Interval p99_interval = BootstrapMedian(p99, policy.bootstrap_resamples,
                                                policy.bootstrap_seed ^ 0x9e3779b97f4a7c15ULL);
  result.throughput_relative_median = throughput_interval.median;
  result.throughput_ci_low = throughput_interval.low;
  result.throughput_ci_high = throughput_interval.high;
  result.p99_relative_median = p99_interval.median;
  result.p99_ci_low = p99_interval.low;
  result.p99_ci_high = p99_interval.high;
  const bool throughput_regressed = throughput_interval.high < -policy.throughput_regression_fraction;
  const bool p99_regressed = p99_interval.low > policy.p99_regression_fraction;
  const bool memory_regressed = policy.gate_memory && HasResourceRegression(
      memory_baseline, memory_candidate, policy.resource_regression_fraction,
      policy.bootstrap_resamples, policy.bootstrap_seed ^ 0x243f6a8885a308d3ULL);
  const bool amplification_regressed = policy.gate_amplification && HasResourceRegression(
      amplification_baseline, amplification_candidate, policy.resource_regression_fraction,
      policy.bootstrap_resamples, policy.bootstrap_seed ^ 0x13198a2e03707344ULL);
  result.status = (throughput_regressed || p99_regressed || memory_regressed || amplification_regressed)
      ? BenchmarkRegressionStatus::kFail : BenchmarkRegressionStatus::kPass;
  result.detail = "throughput median=" + Number(throughput_interval.median) +
      " ci=[" + Number(throughput_interval.low) + "," + Number(throughput_interval.high) +
      "] p99 median=" + Number(p99_interval.median) + " ci=[" + Number(p99_interval.low) +
      "," + Number(p99_interval.high) + "]";
  return result;
}

BenchmarkRegressionResult CompareInstrumentationOverheadRuns(
    const BenchmarkBaselineKey& minimal_key,
    const BenchmarkBaselineKey& instrumented_key,
    const std::string& minimal_profile_id,
    const std::string& instrumented_profile_id,
    const std::vector<BenchmarkRegressionSample>& minimal,
    const std::vector<BenchmarkRegressionSample>& instrumented,
    const InstrumentationOverheadPolicy& policy) {
  BenchmarkRegressionResult result;
  if (minimal_profile_id != kInstrumentationProfileTier0Minimal ||
      instrumented_profile_id != kInstrumentationProfileTier0Tier1) {
    result.status = BenchmarkRegressionStatus::kIncompatible;
    result.detail =
        "instrumentation overhead requires tier0-minimal baseline and "
        "tier0-tier1 candidate";
    return result;
  }
  BenchmarkRegressionPolicy comparison;
  comparison.minimum_pair_count = policy.minimum_pair_count;
  comparison.throughput_regression_fraction =
      policy.maximum_throughput_overhead_fraction;
  comparison.p99_regression_fraction =
      policy.maximum_p99_overhead_fraction;
  comparison.maximum_coefficient_of_variation =
      policy.maximum_coefficient_of_variation;
  comparison.bootstrap_resamples = policy.bootstrap_resamples;
  comparison.bootstrap_seed = policy.bootstrap_seed;
  result = ComparePairedBenchmarkRuns(
      minimal_key, instrumented_key, minimal, instrumented, comparison);
  if (result.status != BenchmarkRegressionStatus::kPass &&
      result.status != BenchmarkRegressionStatus::kFail) {
    return result;
  }
  const bool throughput_over =
      result.throughput_relative_median <
      -policy.maximum_throughput_overhead_fraction;
  const bool p99_over =
      result.p99_relative_median > policy.maximum_p99_overhead_fraction;
  result.status = throughput_over || p99_over
      ? BenchmarkRegressionStatus::kFail
      : BenchmarkRegressionStatus::kPass;
  result.detail = "instrumentation overhead: " + result.detail;
  return result;
}

}  // namespace cedar
