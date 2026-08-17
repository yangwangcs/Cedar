# Cedar-Owned RocksDB Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved Cedar Lean Profile and Cedar Kernel Mode so Cedar controls runtime policy, WAL-priority scheduling, N+1 preparation, and maintenance while RocksDB retains WAL, recovery, MemTable, table, VersionSet, and MANIFEST mechanics.

**Architecture:** Stage A adds exact stage metrics, disables unneeded periodic work, fixes WAL options, and publishes one Cedar-owned runtime snapshot. Stage B strengthens the existing `third_party/rocksdb/db/cedar_commit.cc` seam into an exclusive kernel write path, replaces the current speculative handoff with a two-slot protocol, and adds Cedar-admitted flush/compaction budgets. The generic path remains available as a differential oracle and rollback profile until every gate passes.

**Tech Stack:** C++20, RocksDB fork in `third_party/rocksdb`, CMake, GoogleTest, existing Cedar Parquet table and pressure-controller modules, ASAN/UBSAN/TSAN.

## Global Constraints

- Preserve one RocksDB WAL record and one durable synchronization per committed Cedar epoch.
- Preserve WAL-before-MemTable ordering and invoke the durable callback exactly once after the WAL and required MANIFEST update are durable and before MemTable insertion.
- Do not replace the RocksDB WAL, WAL recovery, MANIFEST, VersionSet, snapshots, checkpoints, or backups.
- Do not write a second Cedar WAL or call RocksDB with `disableWAL=true` for production commits.
- On macOS, treat `Sync()` and `Fsync()` as `fcntl(F_FULLFSYNC)`; do not claim that `use_fsync`, `manual_wal_flush`, or `wal_bytes_per_sync` removes the full sync.
- Keep `F_BARRIERFSYNC` experimental and never use it in the production profile without a filesystem and power-loss evidence matrix.
- Production must not construct RocksDB statistics or register periodic statistics, info-log, or time-trigger compaction work unless an explicit diagnostic profile is selected.
- Foreground commit and preflight paths read cached Cedar runtime state and do not poll RocksDB properties.
- Kernel mode must reuse or extract RocksDB primitives instead of copying a drifting private snapshot of `WriteImpl`.
- Cedar stops admission at immutable-memory, WAL-retention, L0, stale-snapshot, or background-error hard limits.
- No kernel-mode default rollout occurs until correctness, crash, sanitizer, reopen, and sustained-load gates pass.
- All implementation tasks use focused tests first and leave the generic profile available for differential testing.
- Execution profiles are explicit: `generic` means the existing developer options and generic `WriteCommittedGroup`; `lean` means `StorageProfile::kProductionAppend` with `kernel_mode=false`; `kernel` means `StorageProfile::kProductionAppend` with `kernel_mode=true`. These are runtime execution modes, not database formats.

---

## File Map

| Area | Files | Responsibility |
| --- | --- | --- |
| Profile and options | `src/fact/rocksdb_config.h`, `src/fact/rocksdb_config.cc`, `include/cedar/fact/fact_store.h` | Explicit WAL, thread, statistics, rotation, and maintenance-mode options. |
| Runtime sampling | `include/cedar/fact/fact_store.h`, `src/fact/fact_store.cc`, `src/kernel/database_impl.h`, `src/kernel/database.cc` | Cedar-owned sampler and immutable cached snapshot. |
| N+1 pipeline | `src/kernel/database_impl.h`, `src/kernel/database.cc`, `include/cedar/database.h` | Two immutable epoch slots, promotion/discard reasons, and metrics. |
| Write kernel | `third_party/rocksdb/include/rocksdb/cedar_commit.h`, `third_party/rocksdb/db/cedar_commit.cc`, `third_party/rocksdb/db/db_impl/db_impl.h`, `third_party/rocksdb/db/db_impl/db_impl_write.cc`, `src/fact/fact_store.cc` | Narrow exclusive Cedar write entry point with shared RocksDB correctness primitives. |
| Maintenance kernel | `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`, `third_party/rocksdb/db/cedar_maintenance.cc`, `third_party/rocksdb/db/db_impl/db_impl.h`, `third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc`, `src/fact/fact_store.cc` | Poll debt and execute bounded flush/compaction without autonomous policy. |
| Tests | `tests/test_rocksdb_profile.cc`, `tests/test_kernel_commit.cc`, new focused test files, `tests/recovery/test_crash_matrix.cc`, `tests/CMakeLists.txt` | Profile, N+1, kernel, maintenance, crash, and shutdown evidence. |
| Benchmarks | `benchmarks/cedar_kernel_bench.cc`, `benchmarks/commit_workloads.cc`, new raw sync benchmark | Generic/lean/kernel comparison and stage-level latency evidence. |

## Task 1: Establish Stage Metrics and the Lean Profile

**Files:**
- Modify: `include/cedar/fact/fact_store.h` (`ProductionStorageOptions`, `RocksDbRuntimeMetrics`)
- Modify: `src/fact/rocksdb_config.cc` (`MakeRocksDbOptions`, `ResolveStorageProfile`)
- Modify: `src/fact/fact_store.cc` (`FactStoreImpl`, `WriteDecidedGroupLocked`)
- Modify: `include/cedar/database.h` (`CommitPipelineLatencyMetrics`, `CommitPipelineMetrics`)
- Modify: `src/kernel/database.cc` (epoch timing and metrics publication)
- Test: `tests/test_rocksdb_profile.cc`
- Test: `tests/test_kernel_commit.cc`

**Interfaces:**
- Consumes: existing `FactStoreOptions`, `ProductionStorageOptions`, `WriteDecidedGroupLocked`.
- Produces: `ProductionStorageOptions` fields `kernel_mode`, `diagnostic_periodic_tasks`, and `recycle_log_file_num`, with `manual_wal_flush=false`, `use_fsync=false`, `wal_bytes_per_sync=0`, `recycle_log_file_num=0`, and explicit background-job values; stage histograms for append, sync, callback, MemTable, and publication.

- [ ] **Step 1: Add failing profile assertions.** Add a `ProductionProfileUsesCedarWalDefaults` test that opens a production profile and asserts the resolved options expose `manual_wal_flush == false`, `use_fsync == false`, `wal_bytes_per_sync == 0`, `recycle_log_file_num == 0`, `stats_dump_period_sec == 0`, and `stats_persist_period_sec == 0`. Add a `CommitMetricsExposeWalStages` test that commits one epoch and asserts each new histogram has one sample.
- [ ] **Step 2: Run the focused tests and verify RED.**

```bash
cmake --build build --target test_rocksdb_profile test_kernel_commit -j2
ctest --test-dir build -R 'ProductionProfileUsesCedarWalDefaults|CommitMetricsExposeWalStages' --output-on-failure
```

Expected: compilation or assertion failure because the new fields and profile values do not exist.
- [ ] **Step 3: Implement the minimum profile and timing fields.** Add named options rather than positional booleans, set the values above in the production branch, remove unconditional production `CreateDBStatistics()`, and add counters for `wal_append`, `wal_sync`, `memtable_insert`, `publication`, `wal_rotation`, and `manifest`. Record these around the existing `store.WriteDecidedGroup` call; do not change write ordering. `diagnostic_periodic_tasks=true` is the only mode allowed to construct statistics or register periodic statistics/info-log/compaction work.
- [ ] **Step 4: Run the focused tests GREEN and inspect resolved options.**

```bash
cmake --build build --target test_rocksdb_profile test_kernel_commit -j2
ctest --test-dir build -R 'ProductionProfileUsesCedarWalDefaults|CommitMetricsExposeWalStages' --output-on-failure
```

Expected: all selected tests pass and the test output reports the explicit option values.
- [ ] **Step 5: Commit the independently reviewable profile change.**

```bash
git add include/cedar/fact/fact_store.h src/fact/rocksdb_config.cc src/fact/fact_store.cc include/cedar/database.h src/kernel/database.cc tests/test_rocksdb_profile.cc tests/test_kernel_commit.cc
git commit -m "perf: make Cedar WAL profile explicit"
```

## Task 2: Replace Foreground Property Polling with a Cedar Runtime Sampler

**Files:**
- Modify: `include/cedar/fact/fact_store.h` (`RocksDbRuntimeMetrics`)
- Modify: `src/fact/fact_store.cc` (`SamplePressure`, `SampleRuntimeMetrics`)
- Modify: `src/kernel/database_impl.h` (sampler thread, snapshot atomics, stop state)
- Modify: `src/kernel/database.cc` (`StartAppendCommitPipeline`, shutdown, admission)
- Test: `tests/test_pressure_controller.cc`
- Test: `tests/test_kernel_commit.cc`

**Interfaces:**
- Consumes: `FactStore::SamplePressure()` and `FactStore::SampleRuntimeMetrics()`.
- Produces: `CedarRuntimeSnapshot ReadRuntimeSnapshot() const`, `StartRuntimeSampler()`, and `StopRuntimeSampler()`; foreground code reads only the snapshot.

- [ ] **Step 1: Add a sampler test seam and failing tests.** Add a test-only sample counter callback to `FactStoreOptions`, then write tests that enqueue commits while the sampler runs and assert the callback count remains bounded by the configured 50/10/5 ms cadence rather than one call per epoch. Add a stale-snapshot test that sets the sample timestamp older than 250 ms and expects admission to stop increasing.
- [ ] **Step 2: Run the tests RED.**

```bash
cmake --build build --target test_pressure_controller test_kernel_commit -j2
ctest --test-dir build -R 'RuntimeSampler|StaleRuntimeSnapshot' --output-on-failure
```

Expected: missing sampler symbols or assertion failure from per-epoch polling.
- [ ] **Step 3: Implement one sampler loop.** Add a `CedarRuntimeSnapshot` containing sample time, immutable/active bytes, cache bytes, WAL bytes, L0, pending compaction, background errors, and `PressureState`. The loop samples every 50 ms in normal, 10 ms in soft, and 5 ms in hard/emergency pressure; publish the complete object under a sequence lock or atomic shared pointer.
- [ ] **Step 4: Remove foreground polls.** Delete `store.SamplePressure()` from the 5 ms preflight timeout path and delete `store.SampleRuntimeMetrics()` after every write. Replace both with `ReadRuntimeSnapshot()`, preserving conservative stale-snapshot admission behavior.
- [ ] **Step 5: Run the tests GREEN and run the existing commit suite.**

```bash
cmake --build build --target test_pressure_controller test_kernel_commit -j2
ctest --test-dir build -R 'RuntimeSampler|StaleRuntimeSnapshot|KernelGroupCommitTest' --output-on-failure
```

- [ ] **Step 6: Commit.**

```bash
git add include/cedar/fact/fact_store.h src/fact/fact_store.cc src/kernel/database_impl.h src/kernel/database.cc tests/test_pressure_controller.cc tests/test_kernel_commit.cc
git commit -m "perf: cache Cedar runtime pressure samples"
```

## Task 3: Make N+1 Preparation a Two-Slot Protocol

**Files:**
- Modify: `src/kernel/database_impl.h` (replace `active_predecided_*` and use pending slots)
- Modify: `src/kernel/database.cc` (`StartAppendCommitPipeline`, promotion and discard accounting)
- Modify: `include/cedar/database.h` (`CommitPipelineMetrics`)
- Test: `tests/test_kernel_commit.cc`
- Test: `tests/test_commit_workloads.cc`

**Interfaces:**
- Consumes: `internal::DecidedEpoch`, `PendingVersionOverlay`, `Store::DecideIndependentAppendGroup`.
- Produces: `DecidedEpochSlot`, `SlotState`, and discard-reason counters; eligible N+1 epochs use the exact successor base sequence.

- [ ] **Step 1: Write deterministic RED tests.** Add tests for arrivals during sync, queue arrivals after the frozen prefix, cancellation, predecessor failure, indeterminate failure, and shutdown. The normal-arrival test must assert promotion rather than discard. The tests use a WAL-sync/prewrite barrier and inspect `n_plus_one_eligible`, `n_plus_one_promoted`, and `n_plus_one_discarded_by_reason`.
- [ ] **Step 2: Run the N+1 tests RED.**

```bash
cmake --build build --target test_kernel_commit test_commit_workloads -j2
ctest --test-dir build -R 'NPlusOne|PreflightsNextEpoch|RetriesStalePredecided' --output-on-failure
```

Expected: the arrival-during-sync case records a discard because the current prefix matching logic invalidates the prepared suffix.
- [ ] **Step 3: Implement two immutable slots.** Define `DecidedEpochSlot { generation, base_visible_seq, requests, epoch, state }`. Freeze N+1 from a precise queue prefix; later arrivals remain outside the slot. Promote only when predecessor publish, generation, successor base, request identity, cancellation, shutdown, and recovery checks all pass.
- [ ] **Step 4: Account for every discard reason.** Add enum values `kPredecessorFailure`, `kIndeterminate`, `kCancelled`, `kGenerationMismatch`, `kBaseMismatch`, and `kShutdown`; increment one reason exactly once and never count normal queue arrivals as discards.
- [ ] **Step 5: Run deterministic and stress tests GREEN.**

```bash
cmake --build build --target test_kernel_commit test_commit_workloads -j2
ctest --test-dir build -R 'NPlusOne|PreflightsNextEpoch|RetriesStalePredecided' --output-on-failure
```

Then run the 2,048-worker smoke command from Task 7 and require at least 95% promotion among eligible epochs and less than 1% discard excluding injected faults, cancellation, and shutdown.
- [ ] **Step 6: Commit.**

```bash
git add src/kernel/database_impl.h src/kernel/database.cc include/cedar/database.h tests/test_kernel_commit.cc tests/test_commit_workloads.cc
git commit -m "perf: promote Cedar two-slot commit preparation"
```

## Task 4: Add the Exclusive Cedar Write Kernel Entry Point

**Files:**
- Modify: `third_party/rocksdb/include/rocksdb/cedar_commit.h`
- Modify: `third_party/rocksdb/db/cedar_commit.cc`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `third_party/rocksdb/db/db_impl/db_impl_write.cc`
- Modify: `src/fact/fact_store.cc` (`WriteDecidedGroupLocked`)
- Modify: `include/cedar/fact/fact_store.h` (`StorageProfile`)
- Test: new `tests/test_rocksdb_cedar_kernel.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: one already-decided `WriteBatch`, `WriteOptions{sync=true, disableWAL=false}`, and the existing `WalDurableCallback`.
- Produces: `rocksdb::WriteCedarEpoch(DB*, const CedarEpochOptions&, WriteBatch*, WalDurableCallback, void*)` with the same callback ordering and recovery semantics as the generic path.

- [ ] **Step 1: Add differential RED tests.** Write the same decided batches through generic `WriteCommittedGroup` and kernel `WriteCedarEpoch`; compare durable callback count, visible sequence, reopened facts, WAL replay, duplicate transaction behavior, and injected pre-sync/indeterminate errors.
- [ ] **Step 2: Run the differential tests before kernel implementation.**

```bash
cmake --build build --target test_rocksdb_cedar_kernel -j2
ctest --test-dir build -R 'CedarKernelWrite' --output-on-failure
```

Expected: the kernel symbol is absent or the test cannot select the kernel profile.
- [ ] **Step 3: Extract a shared internal write primitive.** Keep validation, protection bytes, column-family checks, sequence overflow checks, WAL append, `MarkLogsSynced`, `ApplyWALToManifest`, pre-release callback, MemTable insertion, and publication in one internal implementation used by both generic and Cedar entry points. The Cedar entry point bypasses only write-thread grouping, generic leader admission, redundant per-write property sampling, and autonomous maintenance submission.
- [ ] **Step 4: Wire `FactStore::WriteDecidedGroupLocked` to the kernel profile.** Select the kernel entry point only when `StorageProfile::kProductionAppend` and `kernel_mode=true`; retain the current generic call for developer and rollback profiles. On any post-callback failure set `recovery_required` and do not retry in-process.
- [ ] **Step 5: Run differential tests GREEN and verify callback order with sync points.**

```bash
cmake --build build --target test_rocksdb_cedar_kernel test_kernel_commit -j2
ctest --test-dir build -R 'CedarKernelWrite|KernelGroupCommitTest' --output-on-failure
```

- [ ] **Step 6: Commit.**

```bash
git add third_party/rocksdb/include/rocksdb/cedar_commit.h third_party/rocksdb/db/cedar_commit.cc third_party/rocksdb/db/db_impl/db_impl.h third_party/rocksdb/db/db_impl/db_impl_write.cc src/fact/fact_store.cc include/cedar/fact/fact_store.h tests/test_rocksdb_cedar_kernel.cc tests/CMakeLists.txt
git commit -m "feat: add Cedar exclusive RocksDB write kernel"
```

## Task 5: Expose Manual Maintenance State and Bounded Execution

**Files:**
- Create: `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`
- Create: `third_party/rocksdb/db/cedar_maintenance.cc`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc`
- Modify: `third_party/rocksdb/CMakeLists.txt` and `third_party/rocksdb/src.mk`
- Modify: `src/fact/fact_store.cc` and `include/cedar/fact/fact_store.h`
- Create: `tests/test_cedar_maintenance.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: DB and column-family handles plus `MaintenanceBudget { max_input_bytes, max_output_bytes, deadline_us, allow_flush, allow_compaction, yield_for_wal_sync }`.
- Produces: `PollCedarMaintenance(DB*, MaintenanceState*)` and `RunCedarMaintenance(DB*, const MaintenanceBudget&, MaintenanceResult*)`; no method schedules work based on host concurrency or periodic timers.

- [ ] **Step 1: Write RED tests for debt and budget.** Create mutable/immutable facts data, call `PollCedarMaintenance`, and assert immutable bytes, L0 files, pending compaction bytes, retained WAL bytes, error, and shutdown state are coherent. Call `RunCedarMaintenance` with zero budget and assert it performs no work; call it with a bounded flush budget and assert consumed input bytes do not exceed the limit.
- [ ] **Step 2: Run the maintenance tests RED.**

```bash
cmake --build build --target test_cedar_maintenance -j2
ctest --test-dir build -R 'CedarMaintenance' --output-on-failure
```

Expected: missing header and symbols.
- [ ] **Step 3: Implement the poll surface.** Read existing DB mutex-protected state and properties once into `MaintenanceState`; include immutable count/bytes, oldest age, retained WAL bytes, L0, pending compaction bytes, manual conflict, background error, and shutdown. Return a coherent error rather than a partially updated state.
- [ ] **Step 4: Implement bounded manual flush and compaction.** Reuse `FlushMemTable` and `RunManualCompaction` through a Cedar-only wrapper. Check the deadline and `yield_for_wal_sync` before each bounded input/output unit; allow only atomic file install and cleanup beyond the byte budget. Return consumed bytes, elapsed time, remaining debt, yield reason, and error.
- [ ] **Step 5: Run budget, error, and shutdown tests GREEN.**

```bash
cmake --build build --target test_cedar_maintenance -j2
ctest --test-dir build -R 'CedarMaintenance' --output-on-failure
```

- [ ] **Step 6: Commit.**

```bash
git add third_party/rocksdb/include/rocksdb/cedar_maintenance.h third_party/rocksdb/db/cedar_maintenance.cc third_party/rocksdb/db/db_impl/db_impl.h third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc third_party/rocksdb/CMakeLists.txt third_party/rocksdb/src.mk src/fact/fact_store.cc include/cedar/fact/fact_store.h tests/test_cedar_maintenance.cc tests/CMakeLists.txt
git commit -m "feat: expose budgeted Cedar maintenance kernel"
```

## Task 6: Move Maintenance Admission and WAL Priority into Cedar

**Files:**
- Modify: `src/kernel/database_impl.h` (maintenance worker, I/O token, stop state)
- Modify: `src/kernel/database.cc` (sampler wakeups, budgets, shutdown order)
- Modify: `src/fact/fact_store.cc` (kernel-mode open options)
- Modify: `src/fact/rocksdb_config.cc` (explicit thread counts and periodic task suppression)
- Modify: `third_party/rocksdb/db/db_impl/db_impl.cc` and `db_impl_compaction_flush.cc` only at the smallest kernel-mode guard points
- Test: `tests/test_rocksdb_lifecycle.cc`
- Test: new `tests/test_kernel_maintenance_pipeline.cc`

**Interfaces:**
- Consumes: `PollCedarMaintenance`, `RunCedarMaintenance`, `CedarRuntimeSnapshot`, and `MaintenanceBudget`.
- Produces: one Cedar maintenance worker, explicit flush/compaction tokens, sync-critical I/O guard, hysteresis, and deterministic shutdown.

- [ ] **Step 1: Add RED tests for autonomous-work suppression.** Open kernel mode with sync points around `MaybeScheduleFlushOrCompaction`, periodic task registration, `BackgroundCallFlush`, and `RunManualCompaction`; assert no autonomous policy task is registered or submitted after open.
- [ ] **Step 2: Run lifecycle tests RED.**

```bash
cmake --build build --target test_rocksdb_lifecycle -j2
ctest --test-dir build -R 'KernelMode.*Background|KernelMode.*Periodic|KernelMode.*Shutdown' --output-on-failure
```

Expected: current RocksDB scheduler registers or submits autonomous work.
- [ ] **Step 3: Add Cedar maintenance admission.** The sampler wakes the maintenance worker on pressure/debt transitions. The worker grants emergency flush first, normal flush second, emergency L0 compaction third, then normal compaction from remaining byte/deadline budget. Use hysteresis so one sample cannot flip state repeatedly.
- [ ] **Step 4: Add the sync-critical guard.** The commit worker sets an atomic `wal_sync_critical` token before the kernel write and clears it after publication. Maintenance submission pauses while set; executing maintenance checks the token at bounded table-builder/input boundaries and yields without interrupting a record or file install.
- [ ] **Step 5: Implement hard-limit admission and shutdown.** Stop or reject new requests at immutable, WAL, L0, stale-snapshot, or background-error limits. Shutdown order is queue stop, preparation join, active commit resolution, commit join, maintenance join, final snapshot, and RocksDB close.
- [ ] **Step 6: Run tests GREEN.**

```bash
cmake --build build --target test_rocksdb_lifecycle test_kernel_maintenance_pipeline test_kernel_commit -j2
ctest --test-dir build -R 'KernelMode|CedarMaintenance|KernelGroupCommitTest' --output-on-failure
```

- [ ] **Step 7: Commit.**

```bash
git add src/kernel/database_impl.h src/kernel/database.cc src/fact/fact_store.cc src/fact/rocksdb_config.cc third_party/rocksdb/db/db_impl/db_impl.cc third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc tests/test_rocksdb_lifecycle.cc tests/test_kernel_maintenance_pipeline.cc
git commit -m "perf: let Cedar schedule RocksDB maintenance"
```

## Task 7: Add Crash Matrix, Differential Recovery, and Sanitizer Gates

**Files:**
- Modify: `tests/recovery/test_crash_matrix.cc`
- Create: `tests/recovery/test_cedar_runtime_crash_matrix.cc`
- Modify: `tests/CMakeLists.txt`
- Modify: `third_party/rocksdb/db/db_impl/db_impl_write.cc` and `cedar_commit.cc` for named fault points
- Modify: `src/fact/fact_store.cc` for pre-append/post-callback fault injection plumbing

**Interfaces:**
- Consumes: generic and kernel profiles, existing `recovery_required` state, and named sync/fault points.
- Produces: deterministic outcomes for before append, after append/pre-sync, after sync/pre-callback, after callback/pre-MemTable, during MemTable, after MemTable/pre-publication, flush construction, pre-MANIFEST install, and post-install cleanup.

- [ ] **Step 1: Add one RED test per crash boundary.** For each fault point, close or terminate the child process, reopen with the generic profile, and compare committed/published transaction resolution and canonical facts to the expected outcome.
- [ ] **Step 2: Run the crash target RED to identify missing fault points.**

```bash
cmake --build build --target test_cedar_runtime_crash_matrix -j2
ctest --test-dir build -R 'CedarRuntimeCrashMatrix' --output-on-failure
```

- [ ] **Step 3: Add named fault points without changing normal control flow.** Ensure a post-callback failure sets recovery-required and never retries in the same process; ensure pre-durable determinate failures remain retryable only where RocksDB proves no durable record exists.
- [ ] **Step 4: Run crash, reopen, checkpoint, backup, rotation, and recycling tests GREEN.** Enable `recycle_log_file_num=2` only in a separate qualified test profile after all rotation tests pass; keep production value zero otherwise.
- [ ] **Step 5: Run dynamic analysis.**

```bash
cmake --preset asan
cmake --build --preset asan -j2
ctest --preset asan --output-on-failure
cmake --preset ubsan
cmake --build --preset ubsan -j2
ctest --preset ubsan --output-on-failure
cmake --preset tsan
cmake --build --preset tsan -j2
ctest --preset tsan -R 'KernelGroupCommitTest|CedarMaintenance|CedarRuntimeCrashMatrix' --output-on-failure
```

- [ ] **Step 6: Commit.**

```bash
git add tests/recovery/test_cedar_runtime_crash_matrix.cc tests/recovery/test_crash_matrix.cc tests/CMakeLists.txt third_party/rocksdb/db/db_impl/db_impl_write.cc third_party/rocksdb/db/cedar_commit.cc src/fact/fact_store.cc
git commit -m "test: qualify Cedar runtime crash boundaries"
```

## Task 8: Add Raw Sync Benchmark and Profile Comparison Gate

**Files:**
- Create: `benchmarks/cedar_wal_sync_bench.cc`
- Modify: `CMakeLists.txt` (benchmark target)
- Modify: `benchmarks/cedar_kernel_bench.cc` and `benchmarks/commit_workloads.cc`
- Test: `tests/test_commit_workloads.cc`
- Modify: `README.md` with reproducible commands and interpretation rules

**Interfaces:**
- Consumes: generic, lean, and kernel `StorageProfile` values and `CommitPipelineMetrics`.
- Produces: raw append/sync latency, stage timing, N+1 rate, maintenance debt, and reopen-qualified comparison output.

- [ ] **Step 1: Add benchmark option tests.** Require exactly one profile, worker count, group limit, duration/operation limit, and durability mode; reject a barrier-sync experiment under the production profile.
- [ ] **Step 2: Implement the raw sync benchmark.** Write realistic Cedar-sized records to a temporary WAL-like file and measure append, `F_FULLFSYNC`, optional experimental `F_BARRIERFSYNC`, and close. Print filesystem, device, group bytes, operation count, p50/p95/p99/max, and durability profile.
- [ ] **Step 3: Extend end-to-end CSV.** Add WAL append/sync, MemTable, publication, rotation, manifest, N+1 eligible/promoted/discarded-by-reason, maintenance bytes/debt, actual thread counts, WAL parameters, background errors, hard-pressure time, and reopen verification.
- [ ] **Step 4: Run the required campaign.**

```bash
cmake -S . -B build -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
cmake --build build --target cedar_wal_sync_bench cedar_kernel_bench -j2
./build/cedar_wal_sync_bench --group-bytes 1048576 --sync full --operations 10000
./build/cedar_kernel_bench --workload independent_append --async --verify-reopen --profile generic --workers 2048 --group-max 256 --duration 30
./build/cedar_kernel_bench --workload independent_append --async --verify-reopen --profile lean --workers 2048 --group-max 256 --duration 30
./build/cedar_kernel_bench --workload independent_append --async --verify-reopen --profile kernel --workers 2048 --group-max 256 --duration 30
./build/cedar_kernel_bench --workload independent_append --async --verify-reopen --profile kernel --workers 512 --group-max 256 --duration 3600
```

Expected report: comparable runs include reopen verification, bounded memory/WAL/L0/compaction debt, no background errors, at least 95% eligible N+1 promotion, and no unexplained autonomous work. The report must state the remaining durable-sync ceiling rather than claim 30,000 transactions/s without the required sync rate or group occupancy.
- [ ] **Step 5: Commit benchmark and documentation changes.**

```bash
git add benchmarks/cedar_wal_sync_bench.cc CMakeLists.txt benchmarks/cedar_kernel_bench.cc benchmarks/commit_workloads.cc tests/test_commit_workloads.cc README.md
git commit -m "bench: compare Cedar RocksDB runtime profiles"
```

## Task 9: Qualification, Rollout, and Rollback Evidence

**Files:**
- Modify: `src/fact/rocksdb_config.cc` (profile selection only)
- Modify: `src/kernel/database.cc` (runtime evidence)
- Modify: `README.md` and `CONTEXT.md` with final commands and limits
- Test: existing complete Cedar suite

**Interfaces:**
- Consumes: all prior task outputs and generic/lean/kernel profiles.
- Produces: explicit runtime evidence identifying profile, thread counts, WAL options, sampler cadence, maintenance ownership, and rollback state.

- [ ] **Step 1: Run the complete correctness gate.**

```bash
cmake --build build --target cedar_core -j2
ctest --test-dir build --output-on-failure
```

Expected: all existing tests, including the current 323-test correctness gate, pass.
- [ ] **Step 2: Verify profile rollback.** Create data with kernel mode, reopen with generic mode, compare snapshots/scans/transaction resolution, then reopen with lean mode. A recovery-required database must reject profile switching until reopened.
- [ ] **Step 3: Verify no autonomous work.** Collect sync-point and thread evidence after kernel-mode open, during 60-minute load, and during shutdown; fail if a RocksDB periodic, flush, or compaction policy task runs without a Cedar token.
- [ ] **Step 4: Publish the qualification record.** Record hardware, filesystem, durability primitive, exact options, thread counts, test commits, benchmark CSV, sanitizer results, crash matrix, reopen result, N+1 rate, ending debt, and the physical WAL limit.
- [ ] **Step 5: Make kernel mode the default only after all evidence is present.** Keep `generic` as an explicit emergency rollback profile and do not enable experimental barrier sync or WAL recycling by default.

## Plan Self-Review

- **Spec coverage:** WAL parameters and rotation are covered by Tasks 1 and 8; cached sampling by Task 2; N+1 promotion by Task 3; kernel write ownership by Task 4; manual maintenance and WAL priority by Tasks 5–6; hard limits and shutdown by Task 6; crash and sanitizer evidence by Task 7; sustained comparison and honest physical limits by Tasks 8–9.
- **Placeholder scan:** no step relies on an unresolved decision, an unspecified file, or an unbounded "add tests" instruction; every task names files, symbols, commands, and expected outcomes.
- **Type consistency:** `CedarRuntimeSnapshot`, `DecidedEpochSlot`, `MaintenanceBudget`, `MaintenanceState`, and `MaintenanceResult` are introduced before later tasks consume them. Existing `WriteCommittedGroup` remains the generic oracle while `WriteCedarEpoch` is introduced in Task 4.
- **Scope:** the plan deliberately excludes Cedar-owned WAL, extra WAL shards, and format changes. Each task has an independently runnable test target and commit boundary.
