# Cedar HTAP Transaction Measurement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce format-1, boundary-owned HTAP transaction measurements in metrics and benchmark artifacts without interpreting missing samples as zero.

**Architecture:** `TransactionCoordinator` owns cumulative, bounded measurements and emits optional event-sink notifications. `DecisionLog` reports its exact fsync interval as part of an append result. `CedarDatabase` maps events to the registry, and `RunBenchmarkWorkload` derives a validated before/after measurement window carried by artifact schema 3.

**Tech Stack:** C++17, Cedar `Status`, `Histogram`, GoogleTest, CMake/CTest, JSON artifact writer/reader.

## Global Constraints

- Database format remains numeric `1`; this change has no database migration or legacy reader.
- Artifact summary schema is exactly `3`; schema `2` is rejected under the clean-break policy.
- All labels are closed vocabulary; transaction IDs, status messages, keys, schemas, shards, and query text are prohibited labels.
- All telemetry and sink failures are ignored and cannot change a transaction result or recovery behavior.
- `tier0-minimal` declares its transaction-measurement window unavailable; release measurement claims use `tier0-tier1`.
- Use only focused normal build/tests during implementation, with `-j1`; do not run sanitizer matrices.
- Preserve the dirty worktree: never reset, clean, stage, commit, or push.

---

## File Structure

- Create `include/cedar/transaction/transaction_measurements.h`: snapshot, event, bounded distributions, and window-delta API.
- Create `src/transaction/transaction_measurements.cc`: saturating aggregation and exact window validation.
- Modify `include/cedar/transaction/decision_log.h` and `src/transaction/decision_log.cc`: expose `fsync_attempted` and measured fsync duration on decision appends.
- Modify `include/cedar/transaction/transaction_coordinator.h` and `src/transaction/transaction_coordinator.cc`: own canonical measurements and publish phase/outcome events.
- Modify `include/cedar/db/cedar_database.h` and `src/db/cedar_database.cc`: configure the optional metric sink and expose snapshots to benchmarks.
- Modify `include/cedar/benchmark/artifact_writer.h`, `include/cedar/benchmark/workload_driver.h`, `src/benchmark/artifact_writer.cc`, `src/benchmark/artifact_reader.cc`, `src/benchmark/workload_driver.cc`, and `src/benchmark/report_builder.cc`: schema-3 representation, strict validation, window propagation, and report output.
- Modify `CMakeLists.txt`: compile the measurement implementation.
- Modify `tests/test_correctness_kernel.cc`: focused unit, coordinator, DecisionLog, workload, and strict-reader tests.

## Task 1: Measurement Model and DecisionLog fsync Result

**Files:**
- Create: `include/cedar/transaction/transaction_measurements.h`
- Create: `src/transaction/transaction_measurements.cc`
- Modify: `include/cedar/transaction/decision_log.h`
- Modify: `src/transaction/decision_log.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces `TransactionMeasurementSnapshot`, `TransactionMeasurementWindow`, and `BuildTransactionMeasurementWindow(before, after)`.
- Produces `DecisionAppendResult::{fsync_attempted, fsync_latency_ns}`.

- [ ] **Step 1: Write failing measurement-window and fsync-result tests**

```cpp
TEST(TransactionMeasurementTest, WindowRejectsCounterRegressionAndMarksEmptyHistogramUndefined) {
  TransactionMeasurementSnapshot before;
  TransactionMeasurementSnapshot after;
  after.started = 1;
  const auto window = BuildTransactionMeasurementWindow(before, after);
  ASSERT_TRUE(window.ok()) << window.status().ToString();
  EXPECT_EQ(window.ValueOrDie().commit_latency.sample_count, 0U);
  EXPECT_FALSE(window.ValueOrDie().commit_latency.defined);
  EXPECT_TRUE(BuildTransactionMeasurementWindow(after, before).status().IsCorruption());
}

TEST_F(DurableLogTest, DecisionAppendReportsAttemptedFsyncDuration) {
  ASSERT_TRUE(decision_log_.Open().ok());
  const auto append = decision_log_.AppendCommitWithResult(9, {PrepareReference{0, 1, 2}}, {});
  ASSERT_TRUE(append.status.ok()) << append.status.ToString();
  EXPECT_TRUE(append.fsync_attempted);
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='TransactionMeasurementTest.*:DurableLogTest.DecisionAppendReportsAttemptedFsyncDuration'`

Expected: compilation failure because the measurement types and fsync fields do not exist.

- [ ] **Step 3: Implement the bounded data model and precise fsync timing**

```cpp
struct DecisionAppendResult {
  Status status = Status::OK();
  bool may_be_durable = false;
  bool requires_reopen = false;
  uint64_t commit_seq = 0;
  bool fsync_attempted = false;
  uint64_t fsync_latency_ns = 0;
};

struct TransactionDistribution {
  bool defined = false;
  uint64_t sample_count = 0;
  uint64_t min_ns = 0;
  uint64_t p50_ns = 0;
  uint64_t p95_ns = 0;
  uint64_t p99_ns = 0;
  uint64_t p999_ns = 0;
  uint64_t max_ns = 0;
  uint64_t sum_ns = 0;
};
```

Time only the `::fsync(fd)` call in `AppendRecord`; carry its attempted bit and duration through `AppendRecordResult` and then `DecisionAppendResult`.  Use the existing bounded latency histogram definition for snapshot aggregation.  Return corruption for a regressed counter or mismatched histogram bounds.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='TransactionMeasurementTest.*:DurableLogTest.DecisionAppendReportsAttemptedFsyncDuration'`

Expected: all selected tests pass.

- [ ] **Step 5: Record completion without a commit**

Do not stage or commit. Mark this task complete in this plan after the focused tests pass.

## Task 2: Coordinator-Owned Events and Registry Bridge

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `include/cedar/db/cedar_database.h`
- Modify: `src/db/cedar_database.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes `TransactionMeasurementSnapshot` and `DecisionAppendResult` from Task 1.
- Produces `TransactionCoordinator::transaction_measurements()` and `SetTransactionMeasurementSink(...)`.
- Produces `CedarDatabase::transaction_measurements()`.

- [ ] **Step 1: Write failing coordinator behavior tests**

```cpp
TEST_F(DurableLogTest, CoordinatorRecordsTypedConflictWithoutDecisionSample) {
  // Establish a strict read/write conflict, then inspect the coordinator snapshot.
  const TransactionMeasurementSnapshot snapshot = coordinator_.transaction_measurements();
  EXPECT_EQ(snapshot.aborted, 1U);
  EXPECT_EQ(snapshot.conflicts, 1U);
  EXPECT_EQ(snapshot.decision_latency.count(), 0U);
}

TEST_F(DurableLogTest, CoordinatorRecordsDecisionFsyncAndVisiblePrefixWait) {
  // Use the existing install hook to delay visibility, commit once, then inspect samples.
  const TransactionMeasurementSnapshot snapshot = coordinator_.transaction_measurements();
  EXPECT_EQ(snapshot.committed, 1U);
  EXPECT_EQ(snapshot.decision_fsync_latency.count(), 1U);
  EXPECT_EQ(snapshot.visible_prefix_wait_success.count(), 1U);
}
```

- [ ] **Step 2: Run the focused coordinator tests and verify RED**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='DurableLogTest.CoordinatorRecords*'`

Expected: compilation failure because coordinator measurement accessors do not exist.

- [ ] **Step 3: Implement terminal and phase event publication**

Add an optional `std::function<void(const TransactionMeasurementEvent&)>` sink and a measurement mutex to `TransactionCoordinator`.  Use one terminal-scope guard in `CommitInternal` so every entered call increments exactly one of committed/aborted/indeterminate and records total commit duration.  Around prepare execution, decision append, and visible-prefix wait, record phase-specific duration and bounded outcome.  Map `Status::SerializationConflict` only to conflict.  Register the exact metric families from the design and have `CedarDatabase` sink calls update them with only closed labels.  Remove transaction writes to `cedar_txn_failed_total`.

- [ ] **Step 4: Run focused transaction and metric tests and verify GREEN**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='DurableLogTest.CoordinatorRecords*:MetricRegistryTest.*'`

Expected: all selected tests pass; no assertion depends on a status-message label.

- [ ] **Step 5: Record completion without a commit**

Do not stage or commit. Mark this task complete in this plan after the focused tests pass.

## Task 3: Schema-3 Artifact Writer, Reader, and Report

**Files:**
- Modify: `include/cedar/benchmark/artifact_writer.h`
- Modify: `src/benchmark/artifact_writer.cc`
- Modify: `src/benchmark/artifact_reader.cc`
- Modify: `src/benchmark/report_builder.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes `TransactionMeasurementWindow` from Task 1.
- Produces `BenchmarkArtifactSummary::transaction_measurements` and strict schema-3 JSON.

- [ ] **Step 1: Write failing schema-3 serialization and rejection tests**

```cpp
TEST(BenchmarkArtifactTest, TransactionMeasurementsSerializeUndefinedWithoutZeroPercentiles) {
  BenchmarkArtifactSummary summary;
  summary.transaction_measurements.availability_reason = "minimal_instrumentation";
  const std::string json = SerializeBenchmarkArtifactSummary(summary);
  EXPECT_NE(json.find("\"artifact_schema_version\":3"), std::string::npos);
  EXPECT_NE(json.find("\"defined\":false"), std::string::npos);
  EXPECT_NE(json.find("\"p99_ns\":null"), std::string::npos);
}

TEST(BenchmarkArtifactTest, ReaderRejectsDefinedDistributionWithZeroSamples) {
  // Archive a valid run, replace defined/sample_count with a contradictory pair,
  // then require ReadBenchmarkArtifact to return Corruption.
}
```

- [ ] **Step 2: Run the artifact tests and verify RED**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='BenchmarkArtifactTest.TransactionMeasurements*:BenchmarkArtifactTest.ReaderRejectsDefinedDistributionWithZeroSamples'`

Expected: compilation failure or assertion failure because schema remains 2 and transaction measurement JSON is absent.

- [ ] **Step 3: Implement schema-3 strict encoding and rendering**

Set the default summary version to `3` and make the reader accept only `3`.  Serialize every distribution as `{defined,sample_count,min_ns,p50_ns,p95_ns,p99_ns,p999_ns,max_ns,sum_ns}` using JSON `null` for every scalar of an undefined distribution.  Serialize counters, `conflict_abort_rate`, visible-prefix success/failure distributions, `max_lag_seq`, and `availability_reason`.  Reader validation must reject every contradictory defined/null/count combination and conflict denominator mismatch.  Render unavailable values as `unknown`, never as `0`.

- [ ] **Step 4: Run focused artifact and offline-report tests and verify GREEN**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='BenchmarkArtifactTest.*'`

Expected: all selected artifact tests pass, including rejection of schema 2 and malformed schema 3 summaries.

- [ ] **Step 5: Record completion without a commit**

Do not stage or commit. Mark this task complete in this plan after the focused tests pass.

## Task 4: Benchmark Window, Minimal Availability, and Evidence Refresh

**Files:**
- Modify: `include/cedar/benchmark/workload_driver.h`
- Modify: `src/benchmark/workload_driver.cc`
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

**Interfaces:**
- Consumes `CedarDatabase::transaction_measurements()` and Task 3 artifact fields.
- Produces transaction measurements in `BenchmarkWorkloadResult` and regenerated HTAP/observability evidence.

- [ ] **Step 1: Write failing workload-window tests**

```cpp
TEST(BenchmarkWorkloadTest, MeasurementWindowExcludesWarmupAndCarriesCommitSamples) {
  // Prepare performs warmup writes; RunBenchmarkWorkload must report only its before/after delta.
  ASSERT_TRUE(result.transaction_measurements.available);
  EXPECT_EQ(result.transaction_measurements.started, result.logical_work_units);
}

TEST(BenchmarkWorkloadTest, MinimalInstrumentationMarksTransactionWindowUnavailable) {
  EXPECT_FALSE(result.transaction_measurements.available);
  EXPECT_EQ(result.transaction_measurements.availability_reason,
            "minimal_instrumentation");
}
```

- [ ] **Step 2: Run the focused workload tests and verify RED**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='BenchmarkWorkloadTest.MeasurementWindow*:BenchmarkWorkloadTest.MinimalInstrumentation*'`

Expected: compilation failure because workload results do not expose transaction windows.

- [ ] **Step 3: Implement before/after capture and profile availability**

Capture the coordinator snapshot immediately before `RunBenchmarkWorkloadImpl` and immediately after it returns.  Build a delta even for non-OK terminal workload results; preserve an unavailable diagnostic instead of resetting to zero.  In a minimal build set the explicit `minimal_instrumentation` availability reason.  Copy the result field into the artifact summary at the existing benchmark runner call site.

- [ ] **Step 4: Run focused normal validation and regenerate affected evidence**

Run: `cmake --build build-current -j1 --target test_correctness_kernel && build-current/tests/test_correctness_kernel --gtest_filter='BenchmarkWorkloadTest.*:BenchmarkArtifactTest.*:DurableLogTest.CoordinatorRecords*'`

Then regenerate only the HTAP correctness and observability roots using the existing `cedar_bench_pair`/artifact validation flow, verify each `SHA256SUMS`, strict-read each artifact, and update the completion matrix with the real measurement provenance.  Do not run any ASAN/UBSAN/TSAN matrix.

- [ ] **Step 5: Record completion without a commit**

Do not stage or commit. Mark this task complete in this plan after focused validation and evidence hash checks pass.

## Plan Self-Review

- Spec coverage: Tasks 1-2 cover authoritative boundaries, bounded labels, typed conflict classification, non-interference, and minimal-profile availability. Task 3 covers clean-break schema-3 serialization/reader/report behavior. Task 4 covers measurement-window deltas and evidence regeneration.
- Placeholder scan: no deferred implementation placeholders are permitted; evidence commands use the existing runner because its exact invocation is defined by the active release-closure plan.
- Type consistency: Tasks use `TransactionMeasurementSnapshot`, `TransactionMeasurementWindow`, `TransactionMeasurementEvent`, `BuildTransactionMeasurementWindow`, and `transaction_measurements()` consistently. Task 4 consumes the Task 2 database accessor and Task 3 summary field.
