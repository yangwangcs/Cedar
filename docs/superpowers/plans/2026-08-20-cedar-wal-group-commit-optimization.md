# Cedar WAL Group Commit Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Increase the number of Cedar transactions committed by one durable RocksDB WAL sync while preserving Cedar's single-WAL, WAL-before-MemTable, conflict-validation, recovery, and authoritative-columnar-facts contracts.

**Architecture:** Cedar remains the semantic group leader: it admits concurrent requests, validates non-conflicting footprints, assembles one epoch WriteBatch, and calls the existing exclusive `WriteCedarEpoch` path exactly once. RocksDB retains WAL append, sync, MANIFEST, MemTable, VersionSet, and recovery ownership. The optimization increases real queue occupancy, makes epoch collection pressure-aware rather than sync-latency-reactive, and adds group-fill evidence so throughput is never inferred from `ops/s` alone.

**Tech Stack:** C++20, Cedar Kernel, embedded RocksDB fork under `src/engine/rocksdb`, CMake, GoogleTest/CTest, shell benchmarks, macOS `F_FULLFSYNC` durability.

## Global Constraints

- Preserve one Cedar epoch WriteBatch and one durable WAL synchronization per committed epoch.
- Keep `WriteCedarEpoch` on `sync=true`; production commits must keep WAL enabled and must not use `disableWAL=true`.
- Preserve WAL append -> WAL sync -> required MANIFEST durability -> Cedar durable callback -> MemTable insertion -> publication.
- Keep Cedar's conflict index, N+1 preparation/promotion, terminal outcomes, and recovery semantics unchanged.
- Keep `enable_pipelined_write=false`, `unordered_write=false`, and `two_write_queues=false` in Kernel mode.
- RocksDB `max_write_batch_group_size_bytes` is a generic-path guard only; Kernel epoch size is controlled by Cedar.
- Do not add a second Cedar WAL, bypass RocksDB recovery, or weaken reopen/crash verification.
- Do not use the archived `22,288 ops/s` warm result as a qualification baseline.
- A 30-second run is `warm_not_sustained`; only a 1,800-second Release run with reopen verification is sustained.
- Every result reports committed operations, epochs, epoch transactions, WAL syncs, transactions/sync, group-fill distribution, latency, retained WAL, compaction debt, and errors.

## Current Findings

- `DatabaseOptions` already exposes `group_commit_max_batch_size=128`, `group_commit_max_batch_bytes=2 MiB`, `group_commit_window_us=200`, and bounded queue limits in `include/cedar/database.h`.
- `src/kernel/database.cc` already performs conflict-aware request selection and calls `WriteDecidedGroup` or `CommitGroupWithWalCallback` once per epoch.
- RocksDB already merges a native write group and syncs it once; Cedar's exclusive `WriteCedarEpoch` prevents unrelated RocksDB writers from joining a Cedar epoch.
- Cedar `CommitAsync()` blocks until its request reaches an epoch and the WAL is durable. The benchmark's `pending.reserve(2)` is not actual per-thread in-flight depth; caller concurrency comes from writer threads.
- The sustained script fixes `writer_clients=2`, so it measures low occupancy rather than Kernel saturation.

## File Map

| Area | Files | Responsibility |
| --- | --- | --- |
| Group-fill metrics | `include/cedar/database.h`, `src/kernel/database.cc`, `benchmarks/cedar_kernel_bench.cc` | Record and export per-epoch fill counts. |
| Adaptive collection | `src/kernel/adaptive_epoch_controller.h/.cc`, `src/kernel/database.cc` | Keep deep groups across slow syncs; reduce on queue-age or pressure violations. |
| Benchmark admission | `benchmarks/cedar_kernel_bench_options.*`, `benchmarks/cedar_kernel_bench_workload.cc` | Support 1..128 blocking writer callers and explicit group knobs. |
| RocksDB profile | `src/storage/rocks/rocksdb_config.cc`, `tests/storage/test_rocksdb_profile.cc` | Make compatible write-thread options explicit. |
| Campaigns | `benchmarks/run_cedar_maintenance_campaign.sh`, new `benchmarks/run_cedar_group_commit_matrix.sh` | Separate latency and throughput evidence. |
| Tests | `tests/kernel/*`, `tests/performance/*`, `tests/recovery/*` | Correctness, crash, sanitizer, space, and performance gates. |
| Documentation | `README.md`, `docs/superpowers/evidence/2026-08-20-cedar-wal-group-commit-optimization.md` | Reproducible commands and interpretation rules. |

### Task 1: Establish a Reproducible Group-Fill Baseline

**Files:**
- Create: `benchmarks/run_cedar_group_commit_matrix.sh`
- Test: `tests/performance/test_kernel_benchmark_csv.cmake`

**Interfaces:**
- Consumes current benchmark CSV fields `operations`, `commit_epochs`, `epoch_transactions`, and `wal_sync_count`.
- Produces a matrix CSV with `writer_clients`, `transactions_per_sync`, and qualification status.

- [ ] **Step 1: Write the matrix runner.** Run clients `2 4 8 16 32 64 128`, each for 30 seconds, with absolute database paths and `--verify-reopen false`. Parse the current CSV by header name, not positional fields, and compute `epoch_transactions / wal_sync_count` with a zero-denominator guard.
- [ ] **Step 2: Run the baseline.**

```bash
cmake --build build-main-release --target cedar_kernel_bench -j2
bench="/Users/wangyang/Desktop/Cedar/build-main-release/cedar_kernel_bench"
benchmarks/run_cedar_group_commit_matrix.sh "$bench" "/private/tmp/cedar-group-baseline"
```

Expected: no writer/background/maintenance errors; the 2-client row is a low-fill reference and the 32-client row is comparable with the current approximately 7,705 ops/s result.
- [ ] **Step 3: Extend `test_kernel_benchmark_csv.cmake` to fail when `transactions_per_sync`, `group_fill_p95`, or `qualification` is missing.
- [ ] **Step 4: Commit.**

```bash
git add benchmarks/run_cedar_group_commit_matrix.sh tests/performance/test_kernel_benchmark_csv.cmake
git commit -m "bench: establish Cedar group fill baseline"
```

### Task 2: Add First-Class Epoch Group-Fill Metrics

**Files:**
- Modify: `include/cedar/database.h`
- Modify: `src/kernel/database.cc`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Test: `tests/kernel/test_kernel_commit.cc`
- Test: `tests/performance/test_kernel_bounded_benchmark.cc`

**Interfaces:**
- Add `CommitGroupFillMetrics` with `groups`, `total_transactions`, `max_transactions`, and buckets `<=1, <=2, <=4, <=8, <=16, <=32, <=64, <=128, >128`.
- Add `CommitPipelineMetrics::group_fill`.
- Export `group_fill_p50`, `group_fill_p95`, and `group_fill_max`.
- Define `size_t GroupFillBucket(size_t request_count)` next to the metric type; values above 128 use the final bucket.

- [ ] **Step 1: Write the failing test.** Add `KernelCommitReportsGroupFill`: two independent concurrent commits must report one group, two total transactions, and one `<=2` bucket.
- [ ] **Step 2: Verify RED.**

```bash
cmake --build build-main-debug --target test_kernel_commit -j2
ctest --test-dir build-main-debug -R "KernelCommitReportsGroupFill" --output-on-failure
```

Expected: compilation failure because the metric type is absent.
- [ ] **Step 3: Implement `RecordGroupFill(size_t request_count)`.** Call it exactly once after the selected `requests` vector is finalized and before physical write; rejected requests are not counted.
- [ ] **Step 4: Emit bounded-memory group-fill percentiles in the CSV.**
- [ ] **Step 5: Run focused tests and commit.**

```bash
cmake --build build-main-debug --target test_kernel_commit test_kernel_bounded_benchmark cedar_kernel_bench -j2
ctest --test-dir build-main-debug -R "KernelCommitReportsGroupFill|KernelBoundedBenchmark" --output-on-failure
git add include/cedar/database.h src/kernel/database.cc benchmarks/cedar_kernel_bench.cc tests/kernel/test_kernel_commit.cc tests/performance/test_kernel_bounded_benchmark.cc
git commit -m "perf: expose Cedar epoch group fill"
```

### Task 3: Make High-Concurrency Admission Measurable

**Files:**
- Modify: `benchmarks/cedar_kernel_bench_options.h`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `benchmarks/cedar_kernel_bench_workload.cc`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Test: `tests/performance/test_kernel_bench_options.cc`
- Test: `tests/performance/test_kernel_bounded_benchmark.cc`

**Interfaces:**
- Extend `writer_clients` validation to `[1,128]`.
- Add benchmark-only `--group-max-batch`, `--group-max-bytes`, `--group-window-us`, and `--group-queue-requests`, mapping directly to `DatabaseOptions`.
- Preserve the existing durable-return contract of `Transaction::CommitAsync()`.

- [ ] **Step 1: Add parser tests.** `--writer-clients 128` passes; 129 and zero fail; group values reject zero and overflow.
- [ ] **Step 2: Verify RED.**

```bash
cmake --build build-main-debug --target test_kernel_bench_options -j2
ctest --test-dir build-main-debug -R "KernelBenchmarkOptions" --output-on-failure
```

- [ ] **Step 3: Implement parsing and pass-through in `MakeBenchmarkDatabaseOptions`.**
- [ ] **Step 4: Simplify `RunBoundedWriters`.** Each worker issues one blocking `CommitAsync()` at a time. Remove `pending.reserve(2)` and front-wait code so the benchmark measures actual caller fan-in instead of implying multiple per-thread handles.
- [ ] **Step 5: Run tests and commit.**

```bash
cmake --build build-main-debug --target test_kernel_bench_options test_kernel_bounded_benchmark cedar_kernel_bench -j2
ctest --test-dir build-main-debug -R "KernelBenchmarkOptions|KernelBoundedBenchmark" --output-on-failure
git add benchmarks/cedar_kernel_bench_options.h benchmarks/cedar_kernel_bench_options.cc benchmarks/cedar_kernel_bench_workload.cc benchmarks/cedar_kernel_bench.cc tests/performance/test_kernel_bench_options.cc tests/performance/test_kernel_bounded_benchmark.cc
git commit -m "bench: measure Cedar writer fan-in explicitly"
```

### Task 4: Preserve Deep Groups Across Slow WAL Syncs

**Files:**
- Modify: `src/kernel/adaptive_epoch_controller.h`
- Modify: `src/kernel/adaptive_epoch_controller.cc`
- Test: `tests/kernel/test_adaptive_epoch_controller.cc`

**Interfaces:**
- Extend `AdaptiveEpochController::Options` with `min_transactions_under_load` default 2 and `deep_queue_threshold` default 16.
- Keep `EpochLimits NextLimits(const EpochQueueSnapshot&)` unchanged.
- Define `uint64_t BytesForTarget(uint32_t target) const` as a saturating product of the byte-per-transaction EWMA and target, capped at `max_encoded_bytes`.

- [ ] **Step 1: Add failing controller tests.**

```cpp
TEST(AdaptiveEpochControllerTest, SlowWalWithDeepQueueKeepsAmortizingGroup) {
  AdaptiveEpochController controller({128, 2ULL << 20, 5'000, 200, 16, 16});
  for (int i = 0; i < 4; ++i) {
    controller.Observe({8'000, 1'000, 64, 64 * 1024});
  }
  const auto limits = controller.NextLimits({64, 100, PressureState::kNormal});
  EXPECT_GE(limits.max_transactions, 64U);
  EXPECT_EQ(limits.max_age_us, 0U);
}
```

Also add `ShallowQueueStillHonorsLatencyWindow`: depth 1 returns max transactions 1 and a non-zero collection age.
- [ ] **Step 2: Verify RED.**

```bash
cmake --build build-main-debug --target test_adaptive_epoch_controller -j2
ctest --test-dir build-main-debug -R "SlowWalWithDeepQueue|ShallowQueueStill" --output-on-failure
```

- [ ] **Step 3: Implement the policy.** Apply hard/soft pressure reductions first. In normal pressure, queue depth and oldest age are the latency signals. A high WAL-sync EWMA alone must not halve a deep, young queue; with available work, the correct response is to amortize sync with a larger group. Reduce when queue age exceeds the SLO or pressure is soft/hard.
- [ ] **Step 4: Run controller tests and commit.**

```bash
cmake --build build-main-debug --target test_adaptive_epoch_controller -j2
ctest --test-dir build-main-debug -R "AdaptiveEpochController" --output-on-failure
git add src/kernel/adaptive_epoch_controller.h src/kernel/adaptive_epoch_controller.cc tests/kernel/test_adaptive_epoch_controller.cc
git commit -m "perf: preserve deep Cedar groups across slow syncs"
```

### Task 5: Align Append Collection and N+1 Preflight

**Files:**
- Modify: `src/kernel/database.cc`
- Modify: `src/kernel/database_impl.h` only if a named helper is required.
- Test: `tests/kernel/test_kernel_commit.cc`
- Test: `tests/performance/test_kernel_bounded_benchmark.cc`

**Interfaces:**
- Add internal `EpochLimits LimitsForQueueLocked() const`, used by both append and preflight workers.
- No public commit or recovery API changes.

- [ ] **Step 1: Add a barrier-based test with at least 16 independent queued requests.** Assert the first physical write contains more than one request and the N+1 plan uses the same request prefix.
- [ ] **Step 2: Verify RED.**

```bash
cmake --build build-main-debug --target test_kernel_commit test_kernel_bounded_benchmark -j2
ctest --test-dir build-main-debug -R "DeepQueue|NPlusOne.*Group" --output-on-failure
```

- [ ] **Step 3: Extract one limit calculation.** Snapshot queue depth, oldest age, pressure, runtime target count/bytes, and adaptive limits once; both workers use the same values for the generation.
- [ ] **Step 4: Preserve bounded waiting.** Wake on target count/bytes, collection deadline, pressure transition, cancellation, or shutdown. Never busy-spin or wait for a target exceeding queue admission limits.
- [ ] **Step 5: Run and commit.**

```bash
ctest --test-dir build-main-debug -R "KernelCommit|KernelBoundedBenchmark|NPlusOne|Shutdown" --output-on-failure
git add src/kernel/database.cc src/kernel/database_impl.h tests/kernel/test_kernel_commit.cc tests/performance/test_kernel_bounded_benchmark.cc
git commit -m "perf: align Cedar epoch and N-plus-one collection"
```

### Task 6: Make the Kernel RocksDB Profile Explicit

**Files:**
- Modify: `src/storage/rocks/rocksdb_config.cc`
- Test: `tests/storage/test_rocksdb_profile.cc`
- Modify: `README.md`

**Interfaces:**
- Kernel profile explicitly reports `enable_pipelined_write=false`, `unordered_write=false`, `two_write_queues=false`, `allow_concurrent_memtable_write=true`, `enable_write_thread_adaptive_yield=true`, and WAL sync settings.
- Generic profile remains a differential oracle.

- [ ] **Step 1: Add failing assertions for all five write-thread options and WAL settings.**
- [ ] **Step 2: Verify RED.**

```bash
cmake --build build-main-debug --target test_rocksdb_profile -j2
ctest --test-dir build-main-debug -R "RocksDbProfile" --output-on-failure
```

- [ ] **Step 3: Set explicit values in the production branch.** Set native `max_write_batch_group_size_bytes=2 MiB` only as a generic-path guard. Kernel epoch size remains Cedar-controlled because `WriteCedarEpoch` receives one assembled batch.
- [ ] **Step 4: Document that `sync=false`, `unordered_write`, and `enable_pipelined_write` are not Cedar production settings.
- [ ] **Step 5: Run and commit.**

```bash
cmake --build build-main-debug --target test_rocksdb_profile -j2
ctest --test-dir build-main-debug -R "RocksDbProfile" --output-on-failure
git add src/storage/rocks/rocksdb_config.cc tests/storage/test_rocksdb_profile.cc README.md
git commit -m "perf: make Cedar kernel write profile explicit"
```

### Task 7: Separate Low-Latency and Throughput Campaigns

**Files:**
- Modify: `benchmarks/run_cedar_maintenance_campaign.sh`
- Create or retain: `benchmarks/run_cedar_group_commit_matrix.sh`
- Modify: `README.md`

**Interfaces:**
- Existing maintenance script remains a 1,800-second low-latency/reopen campaign.
- The group matrix runs 2/4/8/16/32/64/128 clients and labels 30-second runs `warm_not_sustained).

- [ ] **Step 1: Rename metadata for the existing 2-client sustained run to `latency_sustained`; do not delete it.**
- [ ] **Step 2: Add a separate 32/64-client Release sustained case with `--verify-reopen true).**
- [ ] **Step 3: Refuse to run when another `cedar_kernel_bench` process is active in the same worktree or the output directory is non-empty.**
- [ ] **Step 4: Document comparison identity: commit, binary, profile, clients, group limits, reopen flag, host, and duration must match.**
- [ ] **Step 5: Verify shell scripts and commit.**

```bash
bash -n benchmarks/run_cedar_maintenance_campaign.sh benchmarks/run_cedar_group_commit_matrix.sh
git add benchmarks/run_cedar_maintenance_campaign.sh benchmarks/run_cedar_group_commit_matrix.sh README.md
git commit -m "bench: split Cedar latency and throughput campaigns"
```

### Task 8: Add Large-Group Recovery and Ordering Coverage

**Files:**
- Modify: `tests/kernel/test_kernel_commit.cc`
- Modify: `tests/recovery/test_crash_matrix.cc`
- Modify: `tests/CMakeLists.txt`
- Modify: `tests/performance/test_kernel_bounded_benchmark.cc`

**Interfaces:**
- No production API changes.
- Tests exercise existing `WriteCedarEpoch` durable callback and reopen behavior with groups of 32, 64, and 128 requests.

- [ ] **Step 1: Add `LargeIndependentGroupUsesOneWalSync).** Submit 32 independent writes concurrently; assert all commit results are committed, WAL sync count is one, group fill is 32, and callback count is one.
- [ ] **Step 2: Add `ConflictingRequestsSplitGroups).** Submit two writes to the same fact plus 30 independent writes; assert the conflicting pair is not grouped while independent requests still group.
- [ ] **Step 3: Add a crash case before MemTable insertion.** Reopen and assert every durable outcome is recovered exactly once and no non-durable request is reported committed.
- [ ] **Step 4: Add shutdown coverage.** Stop while a large group is collecting; assert N+1 `kShutdown) discard accounting, no unresolved request, and successful reopen.
- [ ] **Step 5: Run focused Debug and ASAN gates.**

```bash
cmake --build build-main-debug --target test_kernel_commit test_kernel_bounded_benchmark test_recovery_crash_matrix -j2
ctest --test-dir build-main-debug -R "LargeIndependentGroup|ConflictingRequests|Crash|Shutdown" --output-on-failure
cmake -S . -B build-main-asan -DCMAKE_BUILD_TYPE=Debug -DCEDAR_ENABLE_ASAN=ON -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
cmake --build build-main-asan --target test_kernel_commit test_kernel_bounded_benchmark -j2
ctest --test-dir build-main-asan -R "KernelCommit|KernelBoundedBenchmark" --output-on-failure
```

- [ ] **Step 6: Commit.**

```bash
git add tests/kernel/test_kernel_commit.cc tests/recovery/test_crash_matrix.cc tests/CMakeLists.txt tests/performance/test_kernel_bounded_benchmark.cc
git commit -m "test: qualify large Cedar WAL groups"
```

### Task 9: Verify Space, Maintenance, and Read-Side Effects

**Files:**
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Modify: `benchmarks/run_cedar_group_commit_matrix.sh`
- Test: `tests/performance/test_kernel_benchmark_csv.cmake`

**Interfaces:**
- Results include retained WAL, live/obsolete SST bytes, pending compaction, runtime sampler age, maintenance errors, and group-fill derived values.

- [ ] **Step 1: Emit `transactions_per_sync`, `wal_bytes_per_transaction`, `peak_queue_requests`, `peak_queue_bytes`, and `group_fill_max` with zero-denominator guards.**
- [ ] **Step 2: Reject campaign output with writer, background, maintenance, write-stopped, or unexplained-autonomous errors.**
- [ ] **Step 3: Run write/read controls with the same seed and database size: `mixed-90-write-10-point-read`, `point-read`, and `projected-event-scan`.**
- [ ] **Step 4: Require retained WAL and pending compaction below existing hard limits; record WAL bytes per transaction to detect space regressions.**
- [ ] **Step 5: Commit.**

```bash
git add benchmarks/cedar_kernel_bench.cc benchmarks/run_cedar_group_commit_matrix.sh tests/performance/test_kernel_benchmark_csv.cmake
git commit -m "bench: report group space and maintenance effects"
```

### Task 10: Release Qualification and Evidence

**Files:**
- Modify: `README.md`
- Create: `docs/superpowers/evidence/2026-08-20-cedar-wal-group-commit-optimization.md`

**Interfaces:**
- Evidence includes commit SHA, RocksDB revision, compiler, host, exact commands, raw CSV paths, and pass/fail decisions.

- [ ] **Step 1: Build current Debug and Release from one commit.**

```bash
cmake -S . -B build-main-debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
cmake --build build-main-debug --target cedar_kernel_bench -j2
cmake -S . -B build-main-release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=ON
cmake --build build-main-release --target cedar_kernel_bench -j2
```

- [ ] **Step 2: Run the complete Debug correctness gate.**

```bash
ctest --test-dir build-main-debug --output-on-failure
```

Expected: zero failures before performance claims.
- [ ] **Step 3: Run the 30-second matrix in Debug and Release for clients 2, 4, 8, 16, 32, 64, and 128.**
- [ ] **Step 4: Run both 1,800-second campaigns with `--verify-reopen true`: low-latency at 2 clients and throughput at 32 or 64 clients.**
- [ ] **Step 5: Apply gates.**
  - Matrix: no correctness errors, all group-fill fields present, and 64/128-client cases are not silently capped at 32.
  - Throughput sustained: average group fill at least 32, N+1 promotion at least 95%, no writer/background/maintenance errors, no unexplained autonomous work, retained WAL and pending compaction below hard limits, and reopen verification succeeds.
  - Latency sustained: preserve the 2-client durable/reopen contract and report its lower throughput as intentional.
  - Read/columnar controls: no more than 10% regression under the same seed and database size.
- [ ] **Step 6: Publish the report.** Do not claim an absolute ops/s target; report host-specific throughput, group-fill percentiles, sync latency, WAL bytes per transaction, and the remaining durable-device ceiling.
- [ ] **Step 7: Commit evidence.**

```bash
git add README.md docs/superpowers/evidence/2026-08-20-cedar-wal-group-commit-optimization.md
git commit -m "docs: publish Cedar WAL group commit qualification"
```

## Self-Review Checklist

- RocksDB write-group behavior is reused rather than reimplemented; Cedar's exclusive Kernel epoch remains the semantic boundary.
- NebulaGraph's batch-apply pattern is used only as an upper-layer grouping reference; its `rocksdb_wal_sync=false` default is not copied into Cedar.
- No task enables `sync=false`, disables WAL, changes recovery ownership, or enables an incompatible pipelined/unordered path.
- The plan corrects the benchmark's misleading pending-handle assumption and separates 2-client latency from high-concurrency throughput.
- Group fill, WAL sync count, recovery, maintenance debt, space, and read-side effects all have explicit coverage.
- No queue or memory bound is unbounded and no absolute throughput promise is made.

## Execution Notes

The following implementation shapes are normative for the tasks above.

### Group-fill recording

~~~cpp
void RecordGroupFill(CommitPipelineMetrics* metrics, size_t request_count) {
  ++metrics->group_fill.groups;
  metrics->group_fill.total_transactions += request_count;
  metrics->group_fill.max_transactions = std::max<uint64_t>(
      metrics->group_fill.max_transactions, request_count);
  ++metrics->group_fill.buckets[GroupFillBucket(request_count)];
}
~~~

Call this once after the selected requests vector is finalized and before WriteDecidedGroup or CommitGroupWithWalCallback; rejected requests are not counted.

The matrix runner must resolve fields by header name:

~~~bash
column() {
  awk -F, -v name="$1" -v header="$header" -v row="$row" \
    'BEGIN { split(header,h,","); split(row,v,","); for (i in h) if (h[i] == name) print v[i]; }'
}
ops=$(column operations)
txns=$(column epoch_transactions)
syncs=$(column wal_sync_count)
awk -v txns="$txns" -v syncs="$syncs" \
  'BEGIN { printf "%.3f", syncs ? txns / syncs : 0 }'
~~~

### Bounded writer fan-in

CommitAsync remains a blocking durable-return API. The benchmark worker must issue one request, wait for its terminal result, and start the next request:

~~~cpp
while (Clock::now() < deadline) {
  auto handle = WriteVertexAsync(database, next_id.fetch_add(1));
  if (!handle.ok()) {
    record_failure(handle.status());
    continue;
  }
  auto completed = handle.ValueOrDie().Wait();
  if (!completed.ok() ||
      completed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
    record_failure(completed.ok() ? completed.ValueOrDie().status
                                  : completed.status());
  } else {
    committed.fetch_add(1, std::memory_order_relaxed);
  }
}
~~~

The number of concurrent callers, not a local pending vector, controls Cedar queue occupancy.
`record_failure(Status)` is a local lambda that increments the failure counter and stores the first failure under the existing status mutex.

### Adaptive collection rule

Hard/soft pressure reductions are evaluated first. In normal pressure, queue age is the latency signal and WAL sync duration alone does not halve a deep queue:

~~~cpp
if (snapshot.depth <= 1) {
  return {1, options_.max_encoded_bytes, options_.maximum_collection_age_us};
}
if (snapshot.oldest_age_us > options_.latency_slo_us) {
  return {std::max(1U, options_.max_transactions / 2),
          std::max<uint64_t>(1, options_.max_encoded_bytes / 2), 0};
}
const uint32_t target = static_cast<uint32_t>(std::min<uint64_t>(
    options_.max_transactions,
    std::max<uint64_t>(options_.min_transactions_under_load,
                       snapshot.depth)));
return {target, BytesForTarget(target),
        snapshot.depth >= target ? 0 : options_.maximum_collection_age_us};
~~~

### Shared append/preflight limits

Both workers use one locked snapshot and the same effective limits:

~~~cpp
EpochLimits limits = adaptive_epoch_controller.NextLimits(snapshot);
limits.max_transactions = std::min({
    limits.max_transactions, append_commit_max_batch_size,
    runtime_target_count.load(std::memory_order_acquire)});
limits.max_encoded_bytes = std::min({
    limits.max_encoded_bytes, append_commit_max_batch_bytes,
    runtime_target_bytes.load(std::memory_order_acquire)});
limits.max_age_us = std::min({
    limits.max_age_us, append_commit_window_us,
    runtime_collection_window_us.load(std::memory_order_acquire)});
return limits;
~~~

### Kernel RocksDB profile

Set the profile explicitly; do not enable incompatible write modes:

~~~cpp
result.allow_concurrent_memtable_write = true;
result.enable_write_thread_adaptive_yield = true;
result.enable_pipelined_write = false;
result.unordered_write = false;
result.two_write_queues = false;
result.max_write_batch_group_size_bytes = 2ULL * 1024ULL * 1024ULL;
~~~

The last field is a generic-path guard. Kernel epoch size remains Cedar-controlled because WriteCedarEpoch receives one assembled batch.

### Large-group correctness test

Use a start barrier so independent callers enter the queue together:

~~~cpp
std::barrier start(32);
for (auto& worker : workers) {
  worker = std::jthread([&] {
    start.arrive_and_wait();
    submit_independent_commit();
  });
}
workers.clear();
EXPECT_EQ(metrics.latency.wal_sync.count, 1U);
EXPECT_EQ(metrics.group_fill.total_transactions, 32U);
~~~

### Derived benchmark values

Use committed epoch counts and actual sync counts:

~~~cpp
const double transactions_per_sync =
    wal_sync_count == 0
        ? 0.0
        : static_cast<double>(epoch_transactions) / wal_sync_count;
const double wal_bytes_per_transaction =
    epoch_transactions == 0
        ? 0.0
        : static_cast<double>(epoch_bytes) / epoch_transactions;
~~~
