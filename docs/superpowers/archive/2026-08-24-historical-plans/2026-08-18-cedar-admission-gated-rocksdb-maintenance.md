# Cedar Admission-Gated RocksDB Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax and each task ends with an independent test gate.

**Goal:** Replace the facts-only synchronous manual-maintenance path with DB-wide Cedar admission tokens that run RocksDB's native flush and compaction jobs, while preserving one RocksDB WAL/recovery path, authoritative Cedar Parquet facts, high write throughput, bounded read amplification, and bounded space amplification.

**Architecture:** Lean remains the automatic-maintenance control profile. Kernel Mode lets RocksDB discover and queue DB-wide debt but requires a Cedar grant before ordinary background job submission. Cedar runs independent flush and compaction policy lanes; RocksDB retains candidate selection, job execution, VersionSet/MANIFEST installation, recovery, and file reclamation.

**Tech Stack:** C++20, the pinned RocksDB fork in `third_party/rocksdb`, Cedar Parquet table modules, CMake, GoogleTest, RocksDB SyncPoint, ASAN, UBSAN, TSAN, and the existing Cedar benchmark binaries.

## Global Constraints

- Keep one Cedar epoch as one RocksDB WriteBatch, one WAL record, and one configured durable sync.
- Do not add a Cedar WAL, `disableWAL=true`, a second recovery protocol, a facts side manifest, or a Cedar file garbage collector.
- Keep facts authoritative only in RocksDB MANIFEST-installed Cedar Parquet v2 files.
- Keep meta/default as RocksDB BlockBasedTable column families.
- RocksDB owns WAL/recovery, sequence numbers, MemTable mechanics, picker/job mechanics, VersionSet, MANIFEST, checkpoints, backups, and obsolete-file deletion.
- Cedar owns commit admission, N+1, sampling cadence, maintenance priority, grant concurrency, byte/deadline budgets, and shutdown ordering.
- Kernel grants are process-local and never persisted.
- Recovery work must not require a live Cedar controller.
- Kernel must reopen Kernel-written databases without conversion. Lean is a
  fresh-database measurement configuration, not a compatibility path.
- Production WAL defaults remain `manual_wal_flush=false`, `use_fsync=false`, `wal_bytes_per_sync=0`, and `recycle_log_file_num=0`.
- No successful run shorter than 1,800 seconds may be labeled sustained.
- Do not restore a fabricated 30,000 transactions/s pass condition; report the measured durable-sync ceiling and software costs.
- Keep Kernel opt-in until every correctness, crash, sanitizer, read, write, space, reopen, and 30-minute gate passes; do not retain an older Cedar write path as rollback code.

## File Map

### RocksDB fork seam

- `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`: DB-wide snapshot, grant, result, yield, and Cedar fork functions.
- `third_party/rocksdb/include/rocksdb/options.h`: admission-gated Kernel setting.
- `third_party/rocksdb/db/cedar_maintenance.cc`: coherent DB-wide observation, grant installation, completion wait, and result construction.
- `third_party/rocksdb/db/db_impl/db_impl.h`: grant/job state, counters, and helper declarations.
- `third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc`: token consumption at native scheduling, candidate budget checks, and completion signaling.
- `third_party/rocksdb/db/db_impl/db_impl_write.cc`: safe pre-stop WBM/flush coordination.
- `third_party/rocksdb/db/db_impl/db_impl.cc`: periodic/recovery/shutdown behavior and evidence counters.

### Cedar control plane

- `src/fact/fact_store.cc`: DB-wide RocksDB snapshot and complete maintenance result adapter.
- `include/cedar/fact/fact_store.h`: Cedar-facing DB-wide runtime and maintenance structures.
- Create `src/kernel/maintenance_policy.h` and `src/kernel/maintenance_policy.cc`: pure policy.
- Create `src/kernel/maintenance_controller.h` and `src/kernel/maintenance_controller.cc`: independent lanes and feedback.
- `src/kernel/database_impl.h` and `src/kernel/database.cc`: controller lifecycle, cached snapshot, and WAL-critical signal.
- `CMakeLists.txt`: compile new Cedar modules.

### Verification and evidence

- `tests/test_rocksdb_cedar_kernel.cc`: real DB-wide grant and native scheduler tests.
- `tests/test_rocksdb_profile.cc`: Lean/Kernel options.
- Create `tests/test_maintenance_policy.cc` and `tests/test_maintenance_controller.cc`.
- `tests/test_fact_store_commit.cc`, `tests/test_kernel_commit.cc`, `tests/test_rocksdb_lifecycle.cc`, and `tests/recovery/test_crash_matrix.cc`.
- `tests/CMakeLists.txt`: register new tests.
- `benchmarks/cedar_kernel_bench.cc`, `benchmarks/cedar_kernel_bench_options.cc`, `benchmarks/commit_workloads.cc`, and their headers.
- `tests/test_kernel_bench_options.cc`, `tests/test_commit_workloads.cc`, and `README.md`.
- Create `docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md`.

### Superseded material

- `docs/superpowers/archive/2026-08-19-superseded-runtime/`: historical
  2026-08-17 Cedar-owned-runtime design and plan, retained only for audit.
- `docs/superpowers/specs/2026-08-18-cedar-admission-gated-rocksdb-maintenance-design.md`:
  the only active maintenance design after Task 10 cleanup.
- The 2026-08-17 bounded-async documents remain active only for
  `CommitAsync` mailbox admission; their maintenance paragraphs are updated to
  point to this plan.

---

### Task 1: Replace the column-family maintenance interface with a DB-wide contract

**Files:**
- Modify: `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`
- Modify: `third_party/rocksdb/db/cedar_maintenance.cc`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `src/fact/fact_store.cc`
- Modify: `include/cedar/fact/fact_store.h`
- Test: `tests/test_rocksdb_cedar_kernel.cc`
- Test: `tests/test_fact_store_commit.cc`

**Interfaces:**
- Consumes: RocksDB `ColumnFamilySet`, `SuperVersion`, WriteBufferManager, WriteController, WAL retention, and current runtime counters.
- Produces: `CedarMaintenanceSnapshot`, per-CF debt, DB-wide Cedar runtime mapping, and `PollCedarMaintenance(DB*, CedarMaintenanceSnapshot*)`.

- [ ] **Step 1: Write the failing multi-CF snapshot test**

Add `RocksDbCedarKernelTest.DbWideSnapshotIncludesFactsMetaAndDefaultDebt`.
Open all three CFs, write only to `meta`, force immutable debt with
`no_slowdown=true`, and assert the new DB-only poll reports all CFs and nonzero
meta debt:

```cpp
rocksdb::CedarMaintenanceSnapshot snapshot;
ASSERT_TRUE(rocksdb::PollCedarMaintenance(db.get(), &snapshot).ok());
EXPECT_EQ(snapshot.column_families.size(), 3U);
const auto meta = std::find_if(
    snapshot.column_families.begin(), snapshot.column_families.end(),
    [](const rocksdb::CedarColumnFamilyDebt& debt) {
      return debt.role == rocksdb::CedarColumnFamilyRole::kMeta;
    });
ASSERT_NE(meta, snapshot.column_families.end());
EXPECT_GT(meta->active_memtable_bytes + meta->immutable_memtable_bytes, 0U);
EXPECT_GT(snapshot.total_active_memtable_bytes +
              snapshot.total_immutable_memtable_bytes,
          0U);
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_rocksdb_cedar_kernel -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.DbWideSnapshotIncludesFactsMetaAndDefaultDebt'
```

Expected: compile failure because the DB-wide snapshot overload and per-CF debt
type do not exist.

- [ ] **Step 3: Define the DB-wide RocksDB types**

Add these types and remove the CF-handle requirement from the observation
interface:

```cpp
enum class CedarColumnFamilyRole : uint8_t {
  kDefault, kFacts, kMeta, kOther,
};

struct CedarColumnFamilyDebt {
  uint32_t id = 0;
  CedarColumnFamilyRole role = CedarColumnFamilyRole::kOther;
  uint64_t active_memtable_bytes = 0;
  uint64_t immutable_memtable_bytes = 0;
  uint64_t immutable_memtable_count = 0;
  uint64_t oldest_immutable_age_us = 0;
  uint64_t l0_files = 0;
  uint64_t pending_compaction_bytes = 0;
  bool flush_pending = false;
  bool compaction_pending = false;
};

struct CedarMaintenanceSnapshot {
  uint64_t generation = 0;
  uint64_t sampled_at_us = 0;
  uint64_t total_active_memtable_bytes = 0;
  uint64_t total_immutable_memtable_bytes = 0;
  uint64_t total_immutable_memtable_count = 0;
  uint64_t write_buffer_manager_bytes = 0;
  uint64_t write_buffer_manager_limit_bytes = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_pending_compaction_bytes = 0;
  uint64_t running_flushes = 0;
  uint64_t running_compactions = 0;
  uint64_t background_errors = 0;
  bool write_delayed = false;
  bool write_stopped = false;
  bool manual_conflict = false;
  bool recovery_in_progress = false;
  bool shutting_down = false;
  std::vector<CedarColumnFamilyDebt> column_families;
};

Status PollCedarMaintenance(DB* db, CedarMaintenanceSnapshot* snapshot);
```

Delete the old overload requiring one `ColumnFamilyHandle*`; retaining it would
allow a new caller to recreate the facts-only bug.

- [ ] **Step 4: Implement one coherent DBImpl snapshot**

Under `DBImpl::mutex_`, iterate initialized, non-dropped CFs. Read each current
and immutable MemTable, VersionStorageInfo, L0 count, pending bytes, oldest
immutable age, and queue flags. Saturating-add totals. Read WBM usage/limit,
WriteController state, running-job counters, errors, manual conflict, recovery,
shutdown, and RocksDB-maintained WAL bytes under the same coherent lock.

Use this helper for all totals:

```cpp
uint64_t CedarSaturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
             ? std::numeric_limits<uint64_t>::max()
             : left + right;
}
```

Increment `generation` when a Cedar-visible maintenance queue, MemTable set,
Version, write-stop state, recovery state, or shutdown state changes. Generation
is monotonic and process-local; it is not wall time.

- [ ] **Step 5: Map DB-wide state into Cedar without double-counting cache**

Replace the facts-only call in `FactStore::SampleRuntime()` with:

```cpp
rocksdb::CedarMaintenanceSnapshot maintenance;
const rocksdb::Status runtime_status =
    rocksdb::PollCedarMaintenance(store->db.get(), &maintenance);
```

Add per-CF and DB-total fields to `RocksDbRuntimeMetrics`. Set
`columnar_flush_pending_bytes` from facts immutable bytes, not pending
compaction bytes. Set WBM pressure from DB totals. Query the shared block cache
once so three CF rows do not triple-count it. Define the Cedar-facing value
types in `include/cedar/fact/fact_store.h`:

```cpp
enum class RocksDbColumnFamilyRole : uint8_t {
  kDefault, kFacts, kMeta, kOther,
};

struct RocksDbColumnFamilyMetrics {
  uint32_t id = 0;
  RocksDbColumnFamilyRole role = RocksDbColumnFamilyRole::kOther;
  uint64_t active_memtable_bytes = 0;
  uint64_t immutable_memtable_bytes = 0;
  uint64_t immutable_memtable_count = 0;
  uint64_t oldest_immutable_age_us = 0;
  uint64_t l0_files = 0;
  uint64_t pending_compaction_bytes = 0;
  bool flush_pending = false;
  bool compaction_pending = false;
};
```

Add `std::vector<RocksDbColumnFamilyMetrics> column_families` and DB-total
fields to `RocksDbRuntimeMetrics`; map the RocksDB role enum by value in
`FactStore::SampleRuntime()` and never retain a `ColumnFamilyHandle*` in Cedar.

- [ ] **Step 6: Add the Cedar mapping regression**

Add `FactStoreCommitTest.RuntimeMetricsIncludeMetaDebtWithoutChangingFactsDebt`:

```cpp
EXPECT_GT(metrics.total_active_memtable_bytes +
              metrics.total_immutable_memtable_bytes,
          metrics.active_memtable_bytes + metrics.immutable_memtable_bytes);
EXPECT_EQ(sample.columnar_flush_pending_bytes,
          metrics.immutable_memtable_bytes);
```

- [ ] **Step 7: Run GREEN and commit**

```bash
cmake --build build-runtime-control \
  --target test_rocksdb_cedar_kernel test_fact_store_commit -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.DbWideSnapshot*'
./build-runtime-control/tests/test_fact_store_commit \
  --gtest_filter='FactStoreCommitTest.RuntimeMetrics*'
git add third_party/rocksdb/include/rocksdb/cedar_maintenance.h \
  third_party/rocksdb/db/cedar_maintenance.cc \
  third_party/rocksdb/db/db_impl/db_impl.h \
  include/cedar/fact/fact_store.h src/fact/fact_store.cc \
  tests/test_rocksdb_cedar_kernel.cc tests/test_fact_store_commit.cc
git commit -m "feat: expose DB-wide Cedar maintenance debt"
```

Expected: selected tests pass and mutable Version state is read only while
holding DBImpl mutex.

---

### Task 2: Gate native RocksDB flush submission with one Cedar grant

**Files:**
- Modify: `third_party/rocksdb/include/rocksdb/options.h`
- Modify: `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc`
- Modify: `third_party/rocksdb/db/cedar_maintenance.cc`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.cc`
- Test: `tests/test_rocksdb_cedar_kernel.cc`
- Test: `tests/test_rocksdb_profile.cc`

**Interfaces:**
- Consumes: Task 1 DB-wide generation/snapshot and native RocksDB flush queue.
- Produces: admission-gated Kernel option, one-shot flush grant, exactly-one-job consumption, and completed `CedarMaintenanceResult`.

- [ ] **Step 1: Write failing flush tests**

Add `KernelGateQueuesFlushDebtWithoutSubmittingAJob`,
`OneFlushGrantSubmitsExactlyOneNativeBackgroundJob`, and
`DbWideFlushGrantSelectsMetaDebtBeforeUnpressuredFacts`.

```cpp
rocksdb::CedarMaintenanceGrant grant;
grant.snapshot_generation = before.generation;
grant.kind = rocksdb::CedarMaintenanceKind::kFlush;
grant.priority = rocksdb::CedarMaintenancePriority::kEmergency;
grant.max_input_bytes = 64ULL * 1024ULL * 1024ULL;
grant.max_output_bytes = 64ULL * 1024ULL * 1024ULL;
grant.deadline_us = 5'000'000;
rocksdb::CedarMaintenanceResult result;
ASSERT_TRUE(rocksdb::RunCedarMaintenance(db.get(), grant, &result).ok());
EXPECT_GT(result.input_bytes, 0U);
EXPECT_EQ(result.selected_column_family_id, meta_handle->GetID());
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_rocksdb_cedar_kernel -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.*FlushGrant*'
```

Expected: compile failure for grant types or behavioral failure because the
current implementation invokes synchronous `FlushMemTable()`.

- [ ] **Step 3: Rename the setting to describe admission gating**

Replace `cedar_manual_maintenance` with one fork option:

```cpp
// RocksDB discovers debt and maintains native queues, but ordinary jobs require
// a one-shot Cedar grant before background submission.
bool cedar_admission_gated_maintenance = false;
```

Update profile tests so Lean expects false and Kernel expects true. Do not keep
two booleans that can disagree about the same DBImpl.

- [ ] **Step 4: Add one-shot grant state inside DBImpl**

Add mutex-guarded state:

```cpp
struct CedarGrantState {
  uint64_t id = 0;
  CedarMaintenanceGrant grant;
  bool installed = false;
  bool consumed = false;
  bool completed = false;
  Status status;
  CedarMaintenanceResult result;
};

uint64_t next_cedar_grant_id_ = 1;
std::optional<CedarGrantState> cedar_flush_grant_;
InstrumentedCondVar cedar_maintenance_cv_{&mutex_};
```

Add `ConsumeCedarFlushGrant(FlushRequest*, uint64_t*)` and
`CompleteCedarFlushGrant(uint64_t, const Status&, uint64_t, uint64_t,
CedarMaintenanceYield)` with `mutex_` as a documented precondition.

- [ ] **Step 5: Gate job submission, not debt discovery**

Remove the early return from `MaybeScheduleFlushOrCompaction()`. Keep queue
construction, WBM accounting, WriteController updates, safety checks, and
recovery logic. Require a matching installed flush grant immediately before
allocating `FlushThreadArg` and calling `Env::Schedule`. Without a grant, leave
the candidate queued and return. Recovery requests carry an explicit recovery
flag and bypass ordinary grants while incrementing a recovery-exception
counter.

- [ ] **Step 6: Implement grant-install plus native-job wait**

Delete direct calls to `FlushMemTable()` and `AtomicFlushMemTables()` from
`RunCedarMaintenance`. Validate generation, budget, shutdown, recovery, WAL
critical state, and an existing grant under mutex. Install one grant, invoke the
normal scheduler, and wait for completion:

```cpp
while (!grant.completed &&
       !shutting_down_.load(std::memory_order_acquire)) {
  cedar_maintenance_cv_.Wait();
}
```

A started flush finishes its current MemTable/file install and reports any
atomic overrun; a not-started grant can be cancelled at its deadline.

- [ ] **Step 7: Complete from native background cleanup and run GREEN**

Carry actual flush input/output bytes and selected CF through
`BackgroundCallFlush` cleanup. Signal after queue/counter updates and job-context
cleanup, then populate the remaining DB-wide snapshot.

```bash
cmake --build build-runtime-control \
  --target test_rocksdb_cedar_kernel test_rocksdb_profile -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.*FlushGrant*'
./build-runtime-control/tests/test_rocksdb_profile
```

Expected: no native flush before a grant, exactly one after one grant, and
meta debt is selectable.

- [ ] **Step 8: Commit**

```bash
git add third_party/rocksdb/include/rocksdb/options.h \
  third_party/rocksdb/include/rocksdb/cedar_maintenance.h \
  third_party/rocksdb/db/db_impl/db_impl.h \
  third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc \
  third_party/rocksdb/db/cedar_maintenance.cc \
  third_party/rocksdb/db/db_impl/db_impl.cc \
  tests/test_rocksdb_cedar_kernel.cc tests/test_rocksdb_profile.cc
git commit -m "feat: gate native RocksDB flushes with Cedar grants"
```

---
### Task 3: Gate native compaction and enforce aggregate budgets

**Files:**
- Modify: `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc`
- Modify: `third_party/rocksdb/db/cedar_maintenance.cc`
- Test: `tests/test_rocksdb_cedar_kernel.cc`

**Interfaces:**
- Consumes: Task 1 DB-wide snapshot and Task 2 one-shot grant state.
- Produces: exactly-one native compaction per grant, complete-overlap budget rejection, safe deadline/WAL yielding, aggregate output accounting, and completion signaling.

- [ ] **Step 1: Write failing compaction tests**

Add `KernelGateQueuesCompactionDebtWithoutSubmittingAJob`,
`OneCompactionGrantSubmitsExactlyOneNativeBackgroundJob`,
`CompactionGrantRejectsAnIncompleteOverlapSet`,
`CompactionResultReportsAggregateOutputOverrun`,
`WalCriticalGrantYieldsBeforeCompactionSubmission`, and
`ManualConflictYieldsWithoutConsumingDebt`.

Construct L0/L1 overlap fixtures with incompressible values. Assert that a
too-small grant starts zero jobs and yields `kInputBudget`; a fitting grant
starts one native job; and the result reports total output across all output
files.

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_rocksdb_cedar_kernel -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.*CompactionGrant*:RocksDbCedarKernelTest.WalCriticalGrant*'
```

Expected: current `CompactFiles()` cannot satisfy native job counters and
aggregate-budget assertions.

- [ ] **Step 3: Add compaction grant state and attach it to native job arguments**

Add `cedar_compaction_grant_` alongside the flush grant and a `grant_id` field to
the native compaction argument/job context. Permit at most one outstanding
compaction grant. Before submission, let the native picker form the complete
legal compaction and saturating-sum all input files. If input exceeds
`max_input_bytes`, release picker state, leave debt queued, complete with
`kInputBudget`, and start no job.

- [ ] **Step 4: Enforce aggregate output admission without partial VersionEdits**

Before submission, estimate the complete picked job's output with native file
size history and the selected input bytes. If the estimate exceeds
`max_output_bytes`, release the picker state, leave debt queued, and return
`kOutputBudget` with the smallest complete candidate size. Do not start a job
that cannot fit the aggregate admission budget.

Once a native compaction starts, it runs through its complete input overlap and
one atomic VersionEdit/MANIFEST installation. Do not stop after an output file:
RocksDB cannot safely publish a partial overlap set without a separate native
resume protocol. If compression or value distribution makes the finished job
cross the estimate, record `atomic_overrun_bytes` and `kOutputBudget` in the
completion while retaining all installed files and versions. Do not use
`CompactionOptions::output_file_size_limit` as the total budget; it remains a
per-file sizing hint.

- [ ] **Step 5: Integrate WAL-critical and deadline checks at job boundaries**

Before submission, a normal grant yields immediately if `wal_sync_critical` is
set or its deadline has passed. A running native compaction is not interrupted
mid-job; it completes the current atomic VersionEdit and reports the elapsed
time. The controller will not submit another job until WAL critical clears and
the next snapshot is sampled. Emergency flush remains independent and is not
implemented through this compaction token.

- [ ] **Step 6: Return actual native job statistics**

Populate result fields from `CompactionJobStats`, including all input levels,
all output files, elapsed time, selected CF, `remaining_smallest_complete_unit_bytes`,
`atomic_overrun_bytes`, yield reason, and remaining DB-wide debt. Return non-OK
RocksDB status as the function status and retain it in completion counters.

- [ ] **Step 7: Run GREEN and commit**

```bash
cmake --build build-runtime-control --target test_rocksdb_cedar_kernel -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.*CompactionGrant*:RocksDbCedarKernelTest.WalCriticalGrant*:RocksDbCedarKernelTest.ManualConflict*'
git add third_party/rocksdb/include/rocksdb/cedar_maintenance.h \
  third_party/rocksdb/db/db_impl/db_impl.h \
  third_party/rocksdb/db/db_impl/db_impl_compaction_flush.cc \
  third_party/rocksdb/db/cedar_maintenance.cc \
  tests/test_rocksdb_cedar_kernel.cc
git commit -m "feat: gate native RocksDB compactions with Cedar grants"
```

Expected: exactly one native compaction per accepted grant, no unsafe partial
overlap, and aggregate output/yield fields match the fixture.

---

### Task 4: Extract a pure Cedar maintenance policy

**Files:**
- Create: `src/kernel/maintenance_policy.h`
- Create: `src/kernel/maintenance_policy.cc`
- Create: `tests/test_maintenance_policy.cc`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `tests/test_kernel_commit.cc`
- Modify: `tests/CMakeLists.txt`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: DB-wide Cedar snapshot, last flush/compaction completion, WAL-critical state, and fixed production thresholds.
- Produces: pure `SelectCedarMaintenance()` returning independent optional flush and compaction decisions.

- [ ] **Step 1: Write policy RED tests**

Cover meta-only immutable debt, WBM 70% normal flush, WBM 85% emergency flush,
write stop, WAL-critical suppression of normal work, emergency flush priority,
running compaction with required flush, input-budget retry sizing, stale
generation backoff, manual conflict, and no-debt no-op. Build all tests with
three CF rows so no test can silently become facts-only.

```cpp
CedarRuntimeSnapshot SnapshotWithDebt(
    RocksDbColumnFamilyRole role, uint64_t active, uint64_t immutable,
    uint64_t l0_files, uint64_t pending_compaction_bytes);
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_maintenance_policy -j2
./build-runtime-control/tests/test_maintenance_policy
```

Expected: target and interface are missing.

- [ ] **Step 3: Define the small policy interface**

Keep RocksDB grant/result types out of Cedar policy code. Put the following
types in `src/kernel/maintenance_policy.h`; `database_impl.h` includes that
header and no longer defines a second `CedarRuntimeSnapshot` or priority enum.
The adapter in Task 5 is the only place that maps these Cedar types to the
RocksDB types from Task 1 and Tasks 2-3. The header includes `<array>`,
`<cstddef>`, `<cstdint>`, and `<optional>`, plus the Cedar status, pressure,
and FactStore metric headers:

```cpp
enum class CedarMaintenanceKind : uint8_t {
  kFlush = 0,
  kCompaction,
};

enum class CedarMaintenancePriority : uint8_t {
  kNone = 0,
  kNormal,
  kEmergency,
};

enum class CedarMaintenanceYield : uint8_t {
  kNone = 0,
  kNoDebt,
  kStaleGeneration,
  kInputBudget,
  kOutputBudget,
  kDeadline,
  kWalSync,
  kManualConflict,
  kRecovery,
  kShutdown,
  kInvariantViolation,
};

struct CedarRuntimeSnapshot {
  uint64_t generation = 0;
  uint64_t sampled_at_us = 0;
  PressureSample pressure;
  RocksDbRuntimeMetrics rocksdb;
  PressureState pressure_state = PressureState::kNormal;
};

struct CedarMaintenanceCompletion {
  uint64_t grant_id = 0;
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  CedarMaintenanceYield yield = CedarMaintenanceYield::kNone;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t elapsed_us = 0;
  uint64_t remaining_smallest_complete_unit_bytes = 0;
  Status status = Status::OK();
};

struct CedarMaintenanceDecision {
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  CedarMaintenancePriority priority = CedarMaintenancePriority::kNone;
  uint64_t max_input_bytes = 0;
  uint64_t max_output_bytes = 0;
  uint64_t deadline_us = 0;
  bool yield_for_wal_sync = true;
};

struct CedarMaintenancePlan {
  std::optional<CedarMaintenanceDecision> flush;
  std::optional<CedarMaintenanceDecision> compaction;
};

struct CedarMaintenanceHistory {
  std::optional<CedarMaintenanceCompletion> last_flush;
  std::optional<CedarMaintenanceCompletion> last_compaction;
};

inline constexpr size_t kCedarMaintenanceYieldCount = 11;

struct CedarMaintenanceMetrics {
  uint64_t snapshots_published = 0;
  uint64_t flush_grants_requested = 0;
  uint64_t flush_grants_accepted = 0;
  uint64_t compaction_grants_requested = 0;
  uint64_t compaction_grants_accepted = 0;
  uint64_t stale_grants = 0;
  uint64_t completed_grants = 0;
  uint64_t queue_delay_us = 0;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint64_t recovery_exception_jobs = 0;
  uint64_t unexplained_autonomous_jobs = 0;
  uint64_t maintenance_errors = 0;
  uint64_t max_snapshot_age_us = 0;
  std::optional<Status> first_error;
  std::array<uint64_t, kCedarMaintenanceYieldCount> yields{};
};

CedarMaintenancePlan SelectCedarMaintenance(
    const CedarRuntimeSnapshot& snapshot,
    const CedarMaintenanceHistory& history,
    bool wal_sync_critical);
```

- [ ] **Step 4: Implement fixed priority and feedback sizing**

Move policy constants out of `database.cc`. Compute DB-wide WBM occupancy with
overflow-safe integer arithmetic. Use the fixed order: error, emergency flush,
normal flush, emergency L0 compaction, normal compaction. Flush and compaction
may both be planned because they have separate lanes, but normal compaction is
not submitted during WAL critical.

Use completion feedback to cover one complete native unit instead of retrying a
fixed undersized grant:

```cpp
uint64_t NextBudget(uint64_t baseline,
                    const std::optional<CedarMaintenanceCompletion>& prior) {
  if (!prior.has_value() ||
      (prior->yield != CedarMaintenanceYield::kInputBudget &&
       prior->yield != CedarMaintenanceYield::kOutputBudget)) {
    return baseline;
  }
  return std::max(baseline, prior->remaining_smallest_complete_unit_bytes);
}
```

Cap decisions at configured memory/disk safety limits; never construct an unsafe
partial compaction merely to fit a policy cap.

- [ ] **Step 5: Remove the old policy implementation**

Delete `SelectCedarMaintenanceInternal` and facts-only constants from
`database.cc`. Include the new header from `database_impl.h` and migrate all
policy tests to the new target.

- [ ] **Step 6: Run GREEN and commit**

```bash
cmake --build build-runtime-control \
  --target test_maintenance_policy test_kernel_commit -j2
./build-runtime-control/tests/test_maintenance_policy
./build-runtime-control/tests/test_kernel_commit \
  --gtest_filter='CedarMaintenancePolicyTest.*'
git add src/kernel/maintenance_policy.h src/kernel/maintenance_policy.cc \
  src/kernel/database_impl.h src/kernel/database.cc \
  tests/test_maintenance_policy.cc tests/test_kernel_commit.cc \
  tests/CMakeLists.txt CMakeLists.txt
git commit -m "refactor: isolate Cedar maintenance policy"
```

Expected: all policy tests pass and the policy has one test seam.

---

### Task 5: Add independent Cedar flush and compaction lanes

**Files:**
- Create: `src/kernel/maintenance_controller.h`
- Create: `src/kernel/maintenance_controller.cc`
- Create: `tests/test_maintenance_controller.cc`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `src/fact/fact_store.cc`
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `tests/test_kernel_commit.cc`
- Modify: `tests/CMakeLists.txt`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: Task 4 policy plan and Task 2/3 RocksDB DB-wide grant/result seam.
- Produces: one flush lane, one compaction lane, completion history, deterministic shutdown, and observable grant metrics.

- [ ] **Step 1: Write controller RED tests with a fake adapter**

Define a fake adapter and block compaction on a condition variable. Add
`BlockedCompactionDoesNotPreventEmergencyFlush`,
`ControllerNeverHasTwoOutstandingFlushGrants`,
`ControllerNeverHasTwoOutstandingCompactionGrants`,
`WalCriticalStatePreventsNormalSubmission`,
`CompletionFeedsTheNextPolicyDecision`, and
`ShutdownJoinsBothLanesAndPublishesNoLateCallback`.

```cpp
adapter.BlockCompaction();
controller.PublishSnapshot(snapshot_with_compaction);
ASSERT_TRUE(adapter.WaitForCompactionStart());
controller.PublishSnapshot(snapshot_with_emergency_flush);
EXPECT_TRUE(adapter.WaitForFlushCompletion());
EXPECT_FALSE(adapter.CompactionCompleted());
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_maintenance_controller -j2
./build-runtime-control/tests/test_maintenance_controller
```

Expected: target and controller types are missing.

- [ ] **Step 3: Define the controller interface**

```cpp
class MaintenanceAdapter {
 public:
  virtual ~MaintenanceAdapter() = default;
  virtual StatusOr<CedarMaintenanceCompletion> RunFlush(
      const CedarMaintenanceDecision& decision,
      const std::atomic<bool>* wal_sync_critical) = 0;
  virtual StatusOr<CedarMaintenanceCompletion> RunCompaction(
      const CedarMaintenanceDecision& decision,
      const std::atomic<bool>* wal_sync_critical) = 0;
};

class MaintenanceController {
 public:
  explicit MaintenanceController(MaintenanceAdapter* adapter);
  Status Start();
  void PublishSnapshot(CedarRuntimeSnapshot snapshot);
  void SetWalSyncCritical(bool critical);
  void Stop();
  CedarMaintenanceMetrics metrics() const;
};
```

There are two adapters: the real FactStore adapter and the deterministic fake.
RocksDB types stay below this seam.

- [ ] **Step 4: Implement two lane loops**

Each lane waits on policy generation but owns its own thread, outstanding flag,
result, stop signal, and last completion. Flush notification is processed first.
Compaction rechecks WAL critical immediately before calling the adapter. Both
publish completion history under one controller mutex and wake policy evaluation.

Store the first maintenance error and stop increasing foreground admission
through the runtime snapshot; never call `.IgnoreError()` on maintenance status.

- [ ] **Step 5: Replace the old single maintenance worker**

Remove `maintenance_worker`, `maintenance_wakeup_pending`, and the direct
`RunMaintenanceBudget(...).IgnoreError()` call from `database.cc`. Construct the
controller after the initial runtime snapshot and start it before accepting
production writes. On every successful sample call `PublishSnapshot()`.

- [ ] **Step 6: Implement exact shutdown ordering**

After commit-worker join, stop new grants, cancel unstarted grants, join
compaction then flush lanes, and return only after adapter calls complete. Take
the final DB-wide snapshot before sampler/RocksDB shutdown. No completion waiter,
callback, or token may outlive DBImpl.

- [ ] **Step 7: Run GREEN and commit**

```bash
cmake --build build-runtime-control \
  --target test_maintenance_controller test_kernel_commit -j2
./build-runtime-control/tests/test_maintenance_controller
./build-runtime-control/tests/test_kernel_commit \
  --gtest_filter='KernelGroupCommitTest.*Shutdown*:KernelGroupCommitTest.*Maintenance*'
git add src/kernel/maintenance_controller.h \
  src/kernel/maintenance_controller.cc src/kernel/database_impl.h \
  src/kernel/database.cc src/fact/fact_store.cc \
  include/cedar/fact/fact_store.h tests/test_maintenance_controller.cc \
  tests/test_kernel_commit.cc tests/CMakeLists.txt CMakeLists.txt
git commit -m "feat: run Cedar flush and compaction lanes independently"
```

Expected: blocked compaction does not block emergency flush and all lane threads
join without late callbacks.

---

### Task 6: Prevent write stop with DB-wide WBM-aware flush admission

**Files:**
- Modify: `third_party/rocksdb/db/db_impl/db_impl_write.cc`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `third_party/rocksdb/db/cedar_maintenance.cc`
- Modify: `src/kernel/maintenance_policy.cc`
- Test: `tests/test_rocksdb_cedar_kernel.cc`
- Test: `tests/test_maintenance_policy.cc`
- Test: `tests/test_kernel_commit.cc`

**Interfaces:**
- Consumes: DB-wide WBM state and native flush grants.
- Produces: pre-stop flush progress, an explicit active-only invariant result, and a deterministic blocking-writer regression.

- [ ] **Step 1: Add a real blocking-writer RED test**

Do not use only `WriteOptions::no_slowdown`; that avoids the production wait.
Create facts and meta CFs sharing a small WBM, fill meta until a writer enters
the RocksDB stall SyncPoint, then issue an emergency DB-wide flush grant from
another thread. Assert both futures finish within two seconds:

```cpp
EXPECT_EQ(writer_future.wait_for(std::chrono::seconds(2)),
          std::future_status::ready);
EXPECT_EQ(flush_future.wait_for(std::chrono::seconds(2)),
          std::future_status::ready);
EXPECT_TRUE(writer_future.get().ok());
EXPECT_TRUE(flush_future.get().ok());
```

Add `WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation` so an
unexpected state returns instead of waiting in `EnterUnbatched`.

- [ ] **Step 2: Run RED with a bounded test timeout**

```bash
cmake --build build-runtime-control --target test_rocksdb_cedar_kernel -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.BlockedWriterResumesAfterDbWideFlushGrant:RocksDbCedarKernelTest.WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation'
```

Expected: the old path times out or cannot select meta debt. Terminate only the
test binary after the configured timeout; do not leave a hung process running.

- [ ] **Step 3: Keep WBM flush discovery in the writer safe point**

In `PreprocessWrite`, preserve `HandleWriteBufferManagerFlush()` and
`ScheduleFlushes()` so the current RocksDB writer switches the correct MemTable
before `DelayWrite()` or `WriteBufferManagerStallWrites()`. Kernel gating may
prevent job submission, but it may not suppress MemTable switching or debt
queueing. Add a SyncPoint assertion showing an immutable candidate is queued
before the writer sleeps.

- [ ] **Step 4: Remove write-stop recovery through public manual flush**

When a flush grant is installed during write stop, schedule an already-queued
native immutable flush. If the DB-wide snapshot contains no immutable candidate,
complete immediately with `kInvariantViolation`; do not call
`FlushMemTable()`/`EnterUnbatched()`.

For active-only idle flushing while writes are not stopped, use an extracted
RocksDB internal safe-point helper that joins the write queue before any stop
condition. The helper creates a native queued request and then returns; the
background job still requires and consumes the Cedar grant.

- [ ] **Step 5: Add pre-stop policy thresholds**

Implement WBM normal flush at 70% and emergency flush at 85%. Retain per-CF
active 75%, immutable-count, and WAL-retention triggers. An emergency decision
must cover the complete smallest flush unit reported by RocksDB.

- [ ] **Step 6: Run GREEN five times**

Run the regression five times so a timing-only pass is not accepted:

```bash
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.BlockedWriterResumesAfterDbWideFlushGrant:RocksDbCedarKernelTest.WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation'
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.BlockedWriterResumesAfterDbWideFlushGrant:RocksDbCedarKernelTest.WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation'
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.BlockedWriterResumesAfterDbWideFlushGrant:RocksDbCedarKernelTest.WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation'
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.BlockedWriterResumesAfterDbWideFlushGrant:RocksDbCedarKernelTest.WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation'
./build-runtime-control/tests/test_rocksdb_cedar_kernel \
  --gtest_filter='RocksDbCedarKernelTest.BlockedWriterResumesAfterDbWideFlushGrant:RocksDbCedarKernelTest.WriteStoppedWithOnlyActiveMemtablesReturnsInvariantViolation'
./build-runtime-control/tests/test_maintenance_policy \
  --gtest_filter='*WriteStop*:*WriteBufferManager*'
```

Expected: five passes, no timeout, and no synchronous manual flush in a stopped
state.

- [ ] **Step 7: Commit**

```bash
git add third_party/rocksdb/db/db_impl/db_impl_write.cc \
  third_party/rocksdb/db/db_impl/db_impl.h \
  third_party/rocksdb/db/cedar_maintenance.cc \
  src/kernel/maintenance_policy.cc tests/test_rocksdb_cedar_kernel.cc \
  tests/test_maintenance_policy.cc tests/test_kernel_commit.cc
git commit -m "fix: admit DB-wide flushes before RocksDB write stop"
```

---

### Task 7: Complete N+1 epoch metrics and retain the high-write fast path

**Files:**
- Modify: `include/cedar/database.h`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Test: `tests/test_kernel_commit.cc`
- Test: `tests/test_commit_workloads.cc`

**Interfaces:**
- Consumes: existing two-slot protocol and WAL duration timestamps.
- Produces: epoch and transaction counters, discard reasons, hidden CPU time, and an enforceable promotion ratio.

- [ ] **Step 1: Add metric RED tests**

Extend steady append, predecessor failure, indeterminate, cancellation, and
shutdown fixtures to assert both epoch and transaction counters:

```cpp
EXPECT_GE(metrics.n_plus_one_eligible_epochs, 20U);
EXPECT_EQ(metrics.n_plus_one_eligible_epochs,
          metrics.n_plus_one_promoted_epochs);
EXPECT_EQ(metrics.n_plus_one_discarded_epochs, 0U);
EXPECT_GT(metrics.n_plus_one_hidden_cpu_us, 0U);
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_kernel_commit -j2
./build-runtime-control/tests/test_kernel_commit \
  --gtest_filter='KernelGroupCommitTest.*NPlusOne*'
```

Expected: epoch and hidden-CPU fields do not exist.

- [ ] **Step 3: Add explicit epoch and transaction metrics**

Keep existing transaction counters and add:

```cpp
uint64_t n_plus_one_preflight_epochs = 0;
uint64_t n_plus_one_decided_epochs = 0;
uint64_t n_plus_one_eligible_epochs = 0;
uint64_t n_plus_one_promoted_epochs = 0;
uint64_t n_plus_one_discarded_epochs = 0;
uint64_t n_plus_one_hidden_cpu_us = 0;
std::array<uint64_t, kNPlusOneDiscardReasonCount>
    n_plus_one_discarded_epochs_by_reason{};
```

Increment epoch counters once per slot transition, not once per request. Measure
validation plus assembly CPU performed while predecessor WAL sync is active and
record only the overlapping portion.

- [ ] **Step 4: Preserve exact-prefix promotion rules**

Do not relax generation, base sequence, cancellation, predecessor failure,
indeterminate, or shutdown checks to raise the metric. Later arrivals remain
legal and do not invalidate the frozen prefix.

- [ ] **Step 5: Print ratios without a zero-denominator claim**

Add CSV fields for eligible/promoted/discarded epochs and transactions, hidden
CPU, and promotion rate. Print zero rate with status `n_plus_one_not_exercised`
when no epoch was eligible; never report 100% from a zero denominator.

- [ ] **Step 6: Run GREEN and commit**

```bash
cmake --build build-runtime-control \
  --target test_kernel_commit test_commit_workloads cedar_kernel_bench -j2
./build-runtime-control/tests/test_kernel_commit \
  --gtest_filter='KernelGroupCommitTest.*NPlusOne*'
./build-runtime-control/tests/test_commit_workloads \
  --gtest_filter='CommitWorkloadsTest.*NPlusOne*'
git add include/cedar/database.h src/kernel/database_impl.h \
  src/kernel/database.cc benchmarks/cedar_kernel_bench.cc \
  tests/test_kernel_commit.cc tests/test_commit_workloads.cc
git commit -m "feat: report N+1 promotion by epoch and transaction"
```

Expected: failure reasons remain correct and the steady fixture exercises at
least twenty eligible epochs.

---

### Task 8: Add read-amplification and space-amplification guardrails

**Files:**
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `src/fact/fact_store.cc`
- Modify: `benchmarks/commit_workloads.h`
- Modify: `benchmarks/commit_workloads.cc`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Test: `tests/test_columnar_fact_scan.cc`
- Test: `tests/test_commit_workloads.cc`

**Interfaces:**
- Consumes: point Get, MultiGet, projected scan, Parquet page/index metrics, and DB-wide space snapshot.
- Produces: identical-seed point/MultiGet/projected/mixed results and live/WAL/obsolete/temporary space metrics.

- [ ] **Step 1: Add benchmark-result RED tests**

Extend `CommitSample` with read and space results. Add a fixture that writes a
fixed seed, reopens it, performs point hits/misses and a one-column projected
scan, then asserts:

```cpp
EXPECT_GT(sample.point_read_operations, 0U);
EXPECT_GT(sample.projected_scan_rows, 0U);
EXPECT_GT(sample.projected_scan_bytes_read, 0U);
EXPECT_LT(sample.projected_scan_bytes_read,
          sample.canonical_scan_bytes_read);
EXPECT_GT(sample.logical_facts_bytes, 0U);
EXPECT_GT(sample.live_sst_bytes, 0U);
```

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_commit_workloads -j2
./build-runtime-control/tests/test_commit_workloads \
  --gtest_filter='CommitWorkloadsTest.ReadAndSpaceMetrics*'
```

Expected: result fields and mixed workload are missing.

- [ ] **Step 3: Expose read physical metrics**

Collect Cedar Parquet counters for candidate files, Bloom outcomes, row groups
and pages opened, compressed bytes read, decoded bytes, cache hits/misses, and
projected columns. Keep point reads index-directed and add an assertion in
`test_columnar_fact_scan` that a point lookup opens fewer than a full row
group's pages. Do not add foreground RocksDB property calls; read accumulated
module counters after the workload.

- [ ] **Step 4: Expose space classes**

Record logical encoded facts bytes, live SST bytes, retained WAL bytes, obsolete
bytes awaiting deletion, temporary output peak, and compression input/output.
Use RocksDB-maintained metadata/counters; a benchmark-only post-close inventory
may cross-check counters but cannot run on the commit path.

- [ ] **Step 5: Add identical-seed workload modes**

Add explicit workload names:

```text
property-put
point-read
multi-get
projected-event-scan
full-event-scan
mixed-90-write-10-point-read
mixed-append-projected-scan
```

Generate a seed once, close it, clone it with RocksDB checkpoint semantics, and
run Lean and Kernel against equivalent clones. Do not compare different initial
LSM layouts.

- [ ] **Step 6: Keep the initial compression profile unchanged**

Assert facts outer compression is `kNoCompression`, Parquet page compression is
`LZ4_RAW`, WAL compression is off, target file size is 128 MiB, and meta/default
retain qualified BlockBasedTable settings. No Zstd or WAL recycling experiment
is enabled in this task.

- [ ] **Step 7: Run GREEN and commit**

```bash
cmake --build build-runtime-control \
  --target test_columnar_fact_scan test_commit_workloads cedar_kernel_bench -j2
./build-runtime-control/tests/test_columnar_fact_scan
./build-runtime-control/tests/test_commit_workloads \
  --gtest_filter='CommitWorkloadsTest.ReadAndSpaceMetrics*'
git add include/cedar/fact/fact_store.h src/fact/fact_store.cc \
  benchmarks/commit_workloads.h benchmarks/commit_workloads.cc \
  benchmarks/cedar_kernel_bench.cc tests/test_columnar_fact_scan.cc \
  tests/test_commit_workloads.cc
git commit -m "bench: gate Cedar read and space amplification"
```

Expected: projected bytes are lower than canonical bytes, point reads remain
index-directed, and all space classes are reported.

---

### Task 9: Make maintenance evidence complete and remove the false throughput gate

**Files:**
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `src/fact/fact_store.cc`
- Modify: `src/kernel/maintenance_controller.h`
- Modify: `src/kernel/maintenance_controller.cc`
- Modify: `benchmarks/commit_workloads.h`
- Modify: `benchmarks/commit_workloads.cc`
- Modify: `benchmarks/cedar_kernel_bench_options.h`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Modify: `tests/test_kernel_bench_options.cc`
- Modify: `tests/test_commit_workloads.cc`
- Modify: `README.md`
- Create: `docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md`

**Interfaces:**
- Consumes: `CedarMaintenanceMetrics`, controller completion history, DB-wide
  RocksDB counters, N+1 metrics, read metrics, and space metrics from Tasks 1-8.
- Produces: one CSV/evidence schema, explicit qualification statuses, and a
  measured durable-sync ceiling with no hard-coded 30,000 transactions/s pass.

- [ ] **Step 1: Add failing qualification tests**

Add these cases to `test_kernel_bench_options.cc`:

```cpp
TEST(BenchmarkQualificationTest, SuccessfulThirtySecondsIsWarmOnly) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 30;
  CommitSample sample;
  sample.elapsed_seconds = 30;
  sample.reopen_verified = true;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample),
            "warm_not_sustained");
}

TEST(BenchmarkQualificationTest, SustainedRunDoesNotUseThroughputFloor) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 1800;
  options.verify_reopen = true;
  options.execution_profile = BenchmarkExecutionProfile::kKernel;
  CommitSample sample;
  sample.operations = 1;
  sample.elapsed_seconds = 1800;
  sample.reopen_verified = true;
  sample.operations_per_second = 1.0;
  sample.durable_sync_writes = 1;
  sample.durable_sync_ceiling = 1.0;
  sample.pipeline_metrics.submitted = 1;
  sample.pipeline_metrics.durably_accepted = 1;
  sample.pipeline_metrics.published = 1;
  sample.pipeline_metrics.n_plus_one_eligible_epochs = 1;
  sample.pipeline_metrics.n_plus_one_promoted_epochs = 1;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample),
            "sustained_local_gates_passed");
}

TEST(BenchmarkQualificationTest, UnexplainedAutonomousJobFailsClosed) {
  KernelBenchmarkOptions options;
  options.duration_seconds = 1800;
  options.execution_profile = BenchmarkExecutionProfile::kKernel;
  options.verify_reopen = true;
  CommitSample sample;
  sample.elapsed_seconds = 1800;
  sample.reopen_verified = true;
  sample.maintenance.unexplained_autonomous_jobs = 1;
  EXPECT_EQ(BenchmarkQualificationStatus(options, sample),
            "sustained_unexplained_autonomous_maintenance");
}
```

The test fixture must initialize every metric used by the status function; it
must not pass merely because an omitted field defaults to zero.

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control --target test_kernel_bench_options -j2
./build-runtime-control/tests/test_kernel_bench_options \
  --gtest_filter='BenchmarkQualificationTest.*'
```

Expected: the test either cannot compile because the maintenance fields are
missing or fails on the existing `sustained_throughput_below_30000` branch.

- [ ] **Step 3: Define the public FactStore maintenance adapter contract**

Keep `include/cedar/fact/fact_store.h` independent of RocksDB and kernel-policy
headers. Add these value types beside `RocksDbRuntimeMetrics`:

```cpp
enum class FactStoreMaintenanceKind : uint8_t { kFlush, kCompaction };

enum class FactStoreMaintenanceYield : uint8_t {
  kNone = 0,
  kNoDebt,
  kStaleGeneration,
  kInputBudget,
  kOutputBudget,
  kDeadline,
  kWalSync,
  kManualConflict,
  kRecovery,
  kShutdown,
  kInvariantViolation,
};

struct FactStoreMaintenanceRequest {
  FactStoreMaintenanceKind kind = FactStoreMaintenanceKind::kFlush;
  uint64_t snapshot_generation = 0;
  uint64_t max_input_bytes = 0;
  uint64_t max_output_bytes = 0;
  uint64_t deadline_us = 0;
  bool emergency = false;
  bool yield_for_wal_sync = true;
};

struct FactStoreMaintenanceResult {
  uint64_t grant_id = 0;
  FactStoreMaintenanceKind kind = FactStoreMaintenanceKind::kFlush;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t elapsed_us = 0;
  uint64_t remaining_smallest_complete_unit_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint32_t selected_column_family_id = 0;
  FactStoreMaintenanceYield yield = FactStoreMaintenanceYield::kNone;
  Status status = Status::OK();
};
```

Add `StatusOr<FactStoreMaintenanceResult> RunNativeMaintenance(
    const FactStoreMaintenanceRequest&, const std::atomic<bool>*)` to
`FactStore`. The implementation maps the request to the RocksDB
`CedarMaintenanceGrant`, calls `RunCedarMaintenance`, maps every yield and
status back, and never calls the old synchronous `FlushMemTable` path. The
controller's real adapter converts the result into
`CedarMaintenanceCompletion`; fake adapters remain entirely in Cedar tests.

- [ ] **Step 4: Export one complete metric sample**

Add a benchmark-only `CommitMaintenanceSample` value type to
`benchmarks/commit_workloads.h`, store it as `CommitSample::maintenance`, and
copy the internal controller metrics into it at the workload boundary. Also
extend `CommitSample` and the CSV row with the fields below. Values are
monotonic counters or interval values as indicated; do not derive them by
subtracting samples with different seeds:

```cpp
struct CommitMaintenanceSample {
  uint64_t flush_grants_requested = 0;
  uint64_t flush_grants_accepted = 0;
  uint64_t compaction_grants_requested = 0;
  uint64_t compaction_grants_accepted = 0;
  uint64_t stale_grants = 0;
  uint64_t yield_no_debt = 0;
  uint64_t yield_input_budget = 0;
  uint64_t yield_output_budget = 0;
  uint64_t yield_deadline = 0;
  uint64_t yield_wal_sync = 0;
  uint64_t yield_manual_conflict = 0;
  uint64_t yield_recovery = 0;
  uint64_t yield_shutdown = 0;
  uint64_t unexplained_autonomous_jobs = 0;
  uint64_t recovery_exception_jobs = 0;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint64_t max_snapshot_age_us = 0;
  std::optional<std::string> first_error;
  uint64_t pending_compaction_bytes = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t write_stopped = 0;
  uint64_t point_read_operations = 0;
  uint64_t multiget_operations = 0;
  uint64_t projected_scan_rows = 0;
  uint64_t projected_scan_bytes_read = 0;
  uint64_t canonical_scan_bytes_read = 0;
  uint64_t logical_facts_bytes = 0;
  uint64_t live_sst_bytes = 0;
  uint64_t obsolete_bytes = 0;
  uint64_t temp_output_peak_bytes = 0;
};
```

Add `CommitMaintenanceSample maintenance` and
`double durable_sync_ceiling = 0.0` to `CommitSample`. `CommitSample` also
keeps `uint64_t durable_sync_writes` and
`CommitPipelineMetrics pipeline_metrics`; Task 7 adds the epoch fields to the
latter. The controller maps its internal `CedarMaintenanceMetrics` into this
field-for-field benchmark copy in `cedar_kernel_bench.cc`; no RocksDB type is
included in the benchmark header.

```text
maintenance_flush_grants_requested,maintenance_flush_grants_accepted,
maintenance_compaction_grants_requested,maintenance_compaction_grants_accepted,
maintenance_stale_grants,maintenance_yield_no_debt,
maintenance_yield_input_budget,maintenance_yield_output_budget,
maintenance_yield_deadline,maintenance_yield_wal_sync,
maintenance_yield_manual_conflict,maintenance_yield_recovery,
maintenance_yield_shutdown,maintenance_unexplained_autonomous_jobs,
maintenance_recovery_exception_jobs,maintenance_input_bytes,
maintenance_output_bytes,maintenance_atomic_overrun_bytes,
maintenance_max_snapshot_age_us,maintenance_first_error,
n_plus_one_preflight_epochs,n_plus_one_decided_epochs,
n_plus_one_eligible_epochs,n_plus_one_promoted_epochs,
n_plus_one_discarded_epochs,n_plus_one_hidden_cpu_us,
point_read_operations,multiget_operations,projected_scan_rows,
projected_scan_bytes_read,canonical_scan_bytes_read,logical_facts_bytes,
live_sst_bytes,retained_wal_bytes,obsolete_bytes,temp_output_peak_bytes,
compression_input_bytes,compression_output_bytes,durable_sync_ceiling
```

Capture the first non-OK maintenance status verbatim in the row and stop new
grants after it. A zero eligible-epoch denominator prints
`n_plus_one_not_exercised`; it never prints a 100% promotion rate.

- [ ] **Step 5: Define qualification predicates in code**

Replace the current throughput branch in
`BenchmarkQualificationStatus` with this order:

```cpp
if (options.duration_seconds == 0) return "smoke";
if (sample.elapsed_seconds < 30.0) return "diagnostic_duration_below_30s";
if (options.duration_seconds < 1800) return "warm_not_sustained";
if (sample.elapsed_seconds < options.duration_seconds)
  return "sustained_elapsed_incomplete";
if (!options.verify_reopen || !sample.reopen_verified)
  return "sustained_reopen_failed";
if (sample.pipeline_metrics.submitted != sample.operations ||
    sample.pipeline_metrics.durably_accepted != sample.operations ||
    sample.pipeline_metrics.published != sample.operations ||
    sample.pipeline_metrics.aborted != 0 ||
    sample.pipeline_metrics.indeterminate != 0 ||
    sample.pipeline_metrics.rejected != 0)
  return "sustained_visibility_incomplete";
if (sample.pipeline_metrics.pressure_hard_us != 0)
  return "sustained_hard_pressure";
if (sample.pipeline_metrics.rocksdb.background_errors != 0 ||
    sample.maintenance.first_error.has_value())
  return "sustained_background_error";
if (sample.maintenance.write_stopped != 0)
  return "sustained_write_stopped";
if (sample.maintenance.unexplained_autonomous_jobs != 0)
  return "sustained_unexplained_autonomous_maintenance";
if (sample.maintenance.max_snapshot_age_us > 250'000)
  return "sustained_snapshot_stale";
if (sample.pipeline_metrics.n_plus_one_eligible_epochs == 0)
  return "sustained_n_plus_one_not_exercised";
if (sample.pipeline_metrics.n_plus_one_eligible_epochs != 0 &&
    sample.pipeline_metrics.n_plus_one_promoted_epochs * 100 <
        sample.pipeline_metrics.n_plus_one_eligible_epochs * 95)
  return "sustained_n_plus_one_below_95_percent";
const uint64_t non_fault_discards =
    sample.pipeline_metrics.n_plus_one_discarded_epochs_by_reason[
        static_cast<size_t>(NPlusOneDiscardReason::kGenerationMismatch)] +
    sample.pipeline_metrics.n_plus_one_discarded_epochs_by_reason[
        static_cast<size_t>(NPlusOneDiscardReason::kBaseMismatch)];
if (static_cast<long double>(non_fault_discards) /
        static_cast<long double>(
            sample.pipeline_metrics.n_plus_one_eligible_epochs) >= 0.01L)
  return "sustained_n_plus_one_control_discard_at_or_above_1_percent";
if (sample.maintenance.retained_wal_bytes >=
        768ULL * 1024ULL * 1024ULL ||
    sample.maintenance.pending_compaction_bytes >=
        8ULL * 1024ULL * 1024ULL * 1024ULL)
  return "sustained_maintenance_debt_above_soft_bound";
return "sustained_local_gates_passed";
```

The report must separately print `transactions_per_sync`, measured
`durable_sync_ceiling`, WAL append/sync/manifest/publication costs, and the
physical reason for the ceiling. No test or runtime branch may contain
`30000` as a pass threshold.

- [ ] **Step 6: Update documentation and run GREEN**

Document the status meanings, the 1,800-second minimum, the no-shortening rule
for a successful 30-minute run, and the distinction between a warm preflight
and sustained qualification in `README.md`. Then run:

```bash
cmake --build build-runtime-control \
  --target test_kernel_bench_options test_commit_workloads cedar_kernel_bench -j2
./build-runtime-control/tests/test_kernel_bench_options
./build-runtime-control/tests/test_commit_workloads \
  --gtest_filter='CommitWorkloadsTest.*Metrics*:*Qualification*'
rg -n '30000|sustained_throughput_below_30000' \
  benchmarks tests README.md
```

Expected: the tests pass and the final `rg` command returns no production gate
or legacy status string. Record the metric schema and a sample CSV row in
`docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md`.

- [ ] **Step 7: Commit**

```bash
git add include/cedar/fact/fact_store.h src/fact/fact_store.cc \
  src/kernel/maintenance_controller.h src/kernel/maintenance_controller.cc \
  benchmarks/commit_workloads.h benchmarks/commit_workloads.cc \
  benchmarks/cedar_kernel_bench_options.h \
  benchmarks/cedar_kernel_bench_options.cc benchmarks/cedar_kernel_bench.cc \
  tests/test_kernel_bench_options.cc tests/test_commit_workloads.cc README.md \
  docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md
git commit -m "feat: qualify Cedar maintenance with complete evidence"
```

---

### Task 10: Retire superseded runtime designs and legacy benchmark contracts

**Files:**
- Move: `docs/superpowers/archive/2026-08-19-superseded-runtime/2026-08-17-cedar-owned-rocksdb-runtime-design.md`
- Move: `docs/superpowers/archive/2026-08-19-superseded-runtime/2026-08-17-cedar-owned-rocksdb-runtime.md`
- Create: `docs/superpowers/archive/2026-08-19-superseded-runtime/README.md`
- Modify: `docs/superpowers/specs/2026-08-17-cedar-bounded-async-executor-design.md`
- Modify: `docs/superpowers/archive/2026-08-24-historical-plans/2026-08-17-cedar-bounded-async-executor.md`
- Modify: `README.md`
- Modify: `benchmarks/commit_workloads.h`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `tests/test_kernel_bench_options.cc`
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `src/fact/fact_store.cc`
- Modify: `src/fact/rocksdb_config.cc`
- Modify: `src/kernel/database.cc`
- Modify: `third_party/rocksdb/include/rocksdb/cedar_maintenance.h`
- Modify: `third_party/rocksdb/include/rocksdb/options.h`
- Modify: `tests/test_rocksdb_cedar_kernel.cc`
- Modify: `tests/test_rocksdb_lifecycle.cc`

**Interfaces:**
- Consumes: the replacement DB-wide grants, controller lanes, metrics, and
  qualification schema from Tasks 1-9.
- Produces: one active maintenance design, one benchmark schema, no legacy
  synchronous Cedar maintenance API, and archived historical rationale that
  cannot be mistaken for executable guidance.

- [ ] **Step 1: Record the legacy RED inventory**

Run these focused searches and save the exact matches in the archive README:

```bash
rg -n 'cedar_manual_maintenance|RunMaintenanceBudget|CedarMaintenanceBudget|CedarMaintenanceState' \
  include src tests third_party/rocksdb/include/rocksdb \
  third_party/rocksdb/db/cedar_maintenance.cc
rg -n '30000|sustained_throughput_below_30000|n_plus_one_eligible,' \
  benchmarks/cedar_kernel_bench* tests/test_kernel_bench_options.cc README.md
rg -n -i 'manual maintenance worker|bounded manual flush|budgeted manual compaction' \
  docs/superpowers/specs docs/superpowers/plans
```

Expected: every match is attributable to the superseded runtime design, the
old facts-only maintenance implementation, or the old benchmark contract. Do
not include unrelated numeric literals from vendored dependencies or archives.

- [ ] **Step 2: Remove the incorrect design from active documentation**

Preserve history without leaving it in the active `specs/` and `plans/`
directories:

```bash
mkdir -p docs/superpowers/archive/2026-08-19-superseded-runtime
git mv docs/superpowers/archive/2026-08-19-superseded-runtime/2026-08-17-cedar-owned-rocksdb-runtime-design.md \
  docs/superpowers/archive/2026-08-19-superseded-runtime/
git mv docs/superpowers/archive/2026-08-19-superseded-runtime/2026-08-17-cedar-owned-rocksdb-runtime.md \
  docs/superpowers/archive/2026-08-19-superseded-runtime/
```

The archive README must say that synchronous public/manual flush and
compaction, facts-only debt, a single maintenance worker, mid-compaction byte
cancellation, and the 30,000 TPS gate are rejected. It must point to
`docs/superpowers/specs/2026-08-18-cedar-admission-gated-rocksdb-maintenance-design.md`
and this implementation plan as the only active maintenance authority.

- [ ] **Step 3: Delete legacy maintenance APIs and aliases**

After Tasks 1-5 have migrated all callers, delete these symbols rather than
keeping deprecated overloads:

```text
rocksdb::CedarMaintenanceBudget
rocksdb::CedarMaintenanceState
rocksdb::PollCedarMaintenance(DB*, ColumnFamilyHandle*, ...)
rocksdb::RunCedarMaintenance(DB*, ColumnFamilyHandle*, ...)
FactStore::RunMaintenanceBudget(...)
DBOptions::cedar_manual_maintenance
```

`src/fact/rocksdb_config.cc` sets only
`cedar_admission_gated_maintenance`. `database.cc` calls only the controller
adapter. Rewrite old tests to the DB-wide snapshot/grant API. Direct
`CompactFiles` calls may remain only in an explicit Kernel manual-conflict test;
add a test name/comment that states that purpose so it cannot become the Cedar
production path.

- [ ] **Step 4: Replace the benchmark schema without compatibility duplicates**

Make `cedar_maintenance_v2` the first CSV schema field. Remove the old
`sustained_throughput_below_30000` branch and ambiguous
`n_plus_one_eligible`/`n_plus_one_promoted` columns; emit the explicit epoch and
transaction columns from Tasks 7 and 9. Do not emit both old and new names.

Keep `cedar_wal_sync_bench`: it is the independent physical durability-ceiling
measurement and is not a qualification gate. `README.md` must distinguish its
raw device result from end-to-end Cedar throughput.

- [ ] **Step 5: Correct the bounded-async documents in place**

The bounded async executor remains active for mailbox admission, but it no
longer owns a valid maintenance design. Add a banner to its spec and plan:

```text
Maintenance note (2026-08-19): all references to the former manual
maintenance worker are superseded by the admission-gated RocksDB maintenance
design dated 2026-08-18. This document remains authoritative only for bounded
CommitAsync admission and mailbox lifecycle.
```

Replace the sentence that says the manual maintenance worker remains in place
with a reference to `MaintenanceController`; do not rewrite the mailbox,
durable callback, or single-WAL invariants.

- [ ] **Step 6: Run the no-legacy guard and focused tests**

```bash
rg -n 'cedar_manual_maintenance|RunMaintenanceBudget|CedarMaintenanceBudget|CedarMaintenanceState' \
  include src tests third_party/rocksdb/include/rocksdb \
  third_party/rocksdb/db/cedar_maintenance.cc
rg -n '30000|sustained_throughput_below_30000|n_plus_one_eligible,' \
  benchmarks/cedar_kernel_bench* tests/test_kernel_bench_options.cc README.md
rg -n -i 'manual maintenance worker|bounded manual flush|budgeted manual compaction' \
  docs/superpowers/specs docs/superpowers/plans
cmake --build build-runtime-control \
  --target test_rocksdb_cedar_kernel test_kernel_bench_options \
  test_commit_workloads -j2
./build-runtime-control/tests/test_rocksdb_cedar_kernel
./build-runtime-control/tests/test_kernel_bench_options
./build-runtime-control/tests/test_commit_workloads
```

Expected: all three `rg` commands return no active-tree matches and all focused
tests pass. Historical matches are allowed only below
`docs/superpowers/archive/2026-08-19-superseded-runtime/`.

- [ ] **Step 7: Commit**

```bash
git add -f docs/superpowers/archive/2026-08-19-superseded-runtime \
  docs/superpowers/specs/2026-08-17-cedar-bounded-async-executor-design.md \
  docs/superpowers/archive/2026-08-24-historical-plans/2026-08-17-cedar-bounded-async-executor.md
git add README.md benchmarks/commit_workloads.h benchmarks/cedar_kernel_bench.cc \
  benchmarks/cedar_kernel_bench_options.cc tests/test_kernel_bench_options.cc \
  include/cedar/fact/fact_store.h src/fact/fact_store.cc \
  src/fact/rocksdb_config.cc src/kernel/database.cc \
  tests/test_rocksdb_cedar_kernel.cc
git commit -m "refactor: retire legacy Cedar maintenance contracts"
```

---

### Task 11: Verify recovery, checkpoint/backup, and deterministic shutdown

**Files:**
- Modify: `third_party/rocksdb/db/db_impl/db_impl.cc`
- Modify: `third_party/rocksdb/db/db_impl/db_impl.h`
- Modify: `third_party/rocksdb/db/cedar_maintenance.cc`
- Modify: `src/fact/fact_store.cc`
- Modify: `src/kernel/maintenance_controller.cc`
- Modify: `tests/test_rocksdb_cedar_kernel.cc`
- Modify: `tests/test_rocksdb_lifecycle.cc`
- Modify: `tests/test_kernel_lifecycle.cc`
- Modify: `tests/recovery/test_crash_matrix.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: RocksDB recovery/MANIFEST ownership, process-local grants, the
  controller stop ordering, and the FactStore adapter from Task 9.
- Produces: crash-boundary coverage, profile-independent reopen proof, explicit
  recovery exceptions, and no late maintenance callback or use-after-close.

- [ ] **Step 1: Add failing recovery and lifecycle tests**

Add child-process crash cases at these named SyncPoints:

```text
Cedar::BeforeWalAppend
Cedar::AfterWalAppendBeforeWalSync
Cedar::AfterWalSyncBeforePublish
Cedar::BeforeMemTableInsert
Cedar::BeforeFlushVersionEdit
Cedar::AfterFlushVersionEdit
Cedar::BeforeCompactionManifestInstall
Cedar::AfterCompactionManifestInstall
Cedar::BeforeObsoleteFileDeletion
```

Each child writes a fixed seed, exits with `_exit(137)` at the selected point,
and the parent reopens the same directory. Assert that committed transaction
outcomes are either visible or resolvable as indeterminate, no duplicate
transaction outcome is created, visible facts match the last durable sequence,
and `PollCedarMaintenance` reports a fresh process-local generation. Add
`CheckpointAndBackupDoNotCreateOrdinaryKernelJobs` and
`ShutdownJoinsMaintenanceLanesBeforeClose` to the lifecycle tests.

- [ ] **Step 2: Run RED**

```bash
cmake --build build-runtime-control \
  --target test_rocksdb_cedar_kernel test_rocksdb_lifecycle \
  test_kernel_lifecycle test_recovery_crash_matrix -j2
ctest --test-dir build-runtime-control --output-on-failure \
  -R 'Cedar.*(Crash|Lifecycle|Recovery|Checkpoint|Backup)'
```

Expected: the crash matrix lacks the new SyncPoints or reports late callbacks,
ordinary-job counters during checkpoint/backup, or an incorrect reopen result.

- [ ] **Step 3: Add explicit recovery bypass accounting**

In `DBImpl`, add an internal `CedarMaintenanceOrigin` field with
`kOrdinary`, `kRecovery`, and `kBackup` values. Mark open-time and
error-recovery flush requests as `kRecovery`. They bypass ordinary grants only
while `recovery_in_progress` is true, increment `recovery_exception_jobs`, and
never enter the Cedar policy queue. Ordinary flush and compaction requests
continue to return the Cedar yield `kRecovery` while the recovery flag is set.
Clear the flag only after WAL replay, VersionSet recovery, and required MemTable
installation complete.

Expose the exception count through `CedarMaintenanceSnapshot` and preserve it
in the final evidence row. A recovery exception is acceptable only when named;
an unlabelled background job remains a qualification failure.

- [ ] **Step 4: Add crash hooks without changing durable ownership**

Place SyncPoint processing at the existing WAL append/sync, MemTable insert,
VersionEdit/MANIFEST install, and obsolete-file deletion boundaries. Hooks must
be test-only and compile to no-ops without SyncPoint support. They may block or
terminate a test process, but must not add a Cedar log, side manifest, file
deletion queue, or alternate recovery path.

- [ ] **Step 5: Enforce checkpoint/backup and shutdown ordering**

Before `Checkpoint::Create` or `BackupEngine::CreateNewBackup`, snapshot the
ordinary-job counters and assert that any job created during the operation is a
RocksDB-owned recovery/backup operation with an explicit reason. In
`MaintenanceController::Stop`, set the stop flag, cancel unstarted grants,
join compaction first, join flush second, publish final completions, and only
then allow `FactStore::Close` to close DBImpl. Every waiter observes either a
completion or `kShutdown`; no callback captures a destroyed controller.

- [ ] **Step 6: Run GREEN repeatedly and commit**

```bash
for i in 1 2 3; do
  ctest --test-dir build-runtime-control --output-on-failure \
    -R 'Cedar.*(Crash|Lifecycle|Recovery|Checkpoint|Backup)'
done
git add third_party/rocksdb/db/db_impl/db_impl.cc \
  third_party/rocksdb/db/db_impl/db_impl.h \
  third_party/rocksdb/db/cedar_maintenance.cc src/fact/fact_store.cc \
  src/kernel/maintenance_controller.cc tests/test_rocksdb_cedar_kernel.cc \
  tests/test_rocksdb_lifecycle.cc tests/test_kernel_lifecycle.cc \
  tests/recovery/test_crash_matrix.cc tests/CMakeLists.txt
git commit -m "test: verify Cedar recovery and maintenance shutdown"
```

Expected: all three repetitions pass, every crash reopen is deterministic, and
ordinary Kernel maintenance has zero unexplained jobs.

---

### Task 12: Run native, ASAN, UBSAN, and TSAN verification profiles

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `cmake/CedarRocksDB.cmake`
- Modify: `tests/CMakeLists.txt`
- Modify: `README.md`
- Create: `tests/test_kernel_verification.cc`

**Interfaces:**
- Consumes: all production code and tests from Tasks 1-11 plus the existing
  `CEDAR_ENABLE_ASAN`, `CEDAR_ENABLE_UBSAN`, and `CEDAR_ENABLE_TSAN` switches.
- Produces: reproducible profile commands and a sanitizer evidence record with
  no data-race, lifetime, undefined-behavior, or native RocksDB build gap.

- [ ] **Step 1: Add a verification manifest test**

Create `test_kernel_verification.cc` with a table-driven test that asserts the
profile name, Kernel gate state, single-WAL settings, and batching bounds:

```cpp
FactStoreOptions ProductionOptionsForVerification() {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.kernel_mode = true;
  options.production.recycle_log_file_num = 0;
  options.production.max_commit_batch_count = 512;
  options.production.max_commit_batch_bytes = 2ULL * 1024ULL * 1024ULL;
  return options;
}

TEST(KernelVerificationTest, ProductionDefaultsRemainSingleWal) {
  const FactStoreOptions options = ProductionOptionsForVerification();
  EXPECT_EQ(options.production.recycle_log_file_num, 0U);
  EXPECT_TRUE(options.production.kernel_mode);
  EXPECT_EQ(options.production.max_commit_batch_count, 512U);
  EXPECT_EQ(options.production.max_commit_batch_bytes,
            2ULL * 1024ULL * 1024ULL);
}
```

Add test-only helpers in `tests/test_kernel_verification.cc`; do not expose
RocksDB internals through the public Cedar API. Register the target in
`tests/CMakeLists.txt`.

- [ ] **Step 2: Run the native RED/GREEN baseline**

```bash
cmake -S . -B build-kernel-native -DBUILD_TESTS=ON \
  -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build-kernel-native -j2
ctest --test-dir build-kernel-native --output-on-failure
```

Expected: the full native suite passes before sanitizer-specific failures are
interpreted. Record compiler, OS, RocksDB revision, and build directory in the
evidence file.

- [ ] **Step 3: Run ASAN and UBSAN as separate builds**

```bash
cmake -S . -B build-kernel-asan -DBUILD_TESTS=ON \
  -DCEDAR_ENABLE_ASAN=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-kernel-asan -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  ctest --test-dir build-kernel-asan --output-on-failure

cmake -S . -B build-kernel-ubsan -DBUILD_TESTS=ON \
  -DCEDAR_ENABLE_UBSAN=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-kernel-ubsan -j2
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-kernel-ubsan --output-on-failure
```

Expected: zero sanitizer diagnostics in grant state, completion waiters,
controller lanes, N+1, recovery, and shutdown tests. A RocksDB cache built
without the matching sanitizer is a failed profile, not a valid result.

- [ ] **Step 4: Run TSAN with serialized stress tests**

```bash
cmake -S . -B build-kernel-tsan -DBUILD_TESTS=ON \
  -DCEDAR_ENABLE_TSAN=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-kernel-tsan -j2
TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
  ctest --test-dir build-kernel-tsan --output-on-failure -j1
```

Expected: no data race or lock-order report. Run the controller, kernel commit,
crash matrix, and lifecycle tests with `-j1` so a test failure is attributable
to the code under test rather than test-process contention.

- [ ] **Step 5: Verify ownership and autonomous-job evidence**

```bash
rg -n 'FlushMemTable\(|AtomicFlushMemTables\(|CompactFiles\(' \
  src third_party/rocksdb/db | tee /tmp/cedar_maintenance_calls.txt
rg -n 'RunCedarMaintenance\(|cedar_admission_gated_maintenance' \
  src third_party/rocksdb tests
```

Review every match: Cedar may invoke only the grant adapter; RocksDB recovery
and native scheduler paths retain ownership. The Kernel test report must show
zero ordinary autonomous jobs and a named count for recovery exceptions.

- [ ] **Step 6: Document and commit the verification matrix**

Add native/ASAN/UBSAN/TSAN commands, compiler versions, expected exit status,
and the exact test filters to `README.md` and the evidence document. Then run:

```bash
git add CMakeLists.txt cmake/CedarRocksDB.cmake tests/CMakeLists.txt \
  tests/test_kernel_verification.cc README.md \
  docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md
git commit -m "test: verify Cedar kernel across sanitizer profiles"
```

---

### Task 13: Execute bounded campaign, production-device comparison, and rollout gate

**Files:**
- Modify: `benchmarks/cedar_kernel_bench_options.h`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Modify: `benchmarks/commit_workloads.h`
- Modify: `benchmarks/commit_workloads.cc`
- Modify: `tests/test_kernel_bench_options.cc`
- Modify: `README.md`
- Modify: `docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md`
- Create: `benchmarks/run_cedar_maintenance_campaign.sh`

**Interfaces:**
- Consumes: the complete CSV/evidence schema and qualification predicates from
  Task 9, crash/sanitizer gates from Tasks 11-12, and identical-seed workload
  modes from Task 8.
- Produces: reproducible 2,048-operation, 30/60/300-second, and 1,800-second
  campaign results, Lean comparison, production-device evidence, and an
  explicit rollout decision.

- [ ] **Step 1: Add campaign option and validation tests**

Add `CampaignKind { kNone, kSmoke, kWarm, kPreflight, kSustained }`, plus
`campaign`, `seed`, `prepare_seed_database`, `seed_database`, and
`database_path` fields to `KernelBenchmarkOptions`. The benchmark creates the
seed once, closes it, and uses RocksDB Checkpoint to clone it into each
`database_path`; it must reject source and destination paths that resolve to the
same directory. `--campaign` is a reporting/exit-policy label and the duration
remains authoritative. Validate the allowed durations and seed paths:

```cpp
TEST(BenchmarkOptionsTest, CampaignDurationsAreBounded) {
  EXPECT_TRUE(ParseKernelBenchmarkOptions({"--campaign", "smoke",
                                           "--operations", "2048",
                                           "--seed", "20260818"}).ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions({"--campaign", "warm",
                                           "--duration-seconds", "30"}).ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions({"--campaign", "preflight",
                                           "--duration-seconds", "300"}).ok());
  EXPECT_TRUE(ParseKernelBenchmarkOptions({
      "--campaign", "sustained", "--duration-seconds", "1800",
      "--profile", "kernel", "--memory-budget-mib", "1024",
      "--seed-db", "/tmp/cedar-seed", "--database-path",
      "/tmp/cedar-kernel-1800"}).ok());
  EXPECT_FALSE(ParseKernelBenchmarkOptions({
      "--campaign", "sustained", "--duration-seconds", "1799",
      "--profile", "kernel", "--memory-budget-mib", "1024"}).ok());
}

TEST(BenchmarkQualificationTest, FailedPreflightReturnsNonzeroExitCode) {
  KernelBenchmarkOptions options;
  options.campaign = CampaignKind::kPreflight;
  options.duration_seconds = 300;
  CommitSample sample;
  sample.maintenance.unexplained_autonomous_jobs = 1;
  EXPECT_NE(CampaignExitCode(options, sample), 0);
}
```

The option parser must reject `--campaign sustained` below 1,800 seconds and
must not silently turn a shortened run into a sustained result.

- [ ] **Step 2: Run RED**

```bash
cmake --build build-kernel-native --target test_kernel_bench_options -j2
./build-kernel-native/tests/test_kernel_bench_options \
  --gtest_filter='BenchmarkOptionsTest.CampaignDurationsAreBounded'
```

Expected: the campaign option is absent or accepts a falsely shortened
sustained run.

- [ ] **Step 3: Add the reproducible campaign runner**

Create `benchmarks/run_cedar_maintenance_campaign.sh` with strict shell mode
and one output directory argument. The script must use identical seed, WAL
options, memory budget, group shape, workload, and host for Lean and Kernel:

```bash
#!/usr/bin/env bash
set -euo pipefail
output_dir="$1"
mkdir -p "$output_dir"
bench="${CEDAR_BENCH:-./build-kernel-native/cedar_kernel_bench}"
common=(--workload property-put --group-workers 512 --group-max 256
        --group-window-us 200 --verify-reopen --seed 20260818)
production=("${common[@]}" --memory-budget-mib 1024)

"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/lean-30s-db" \
  --profile lean --campaign warm --duration-seconds 30 \
  >"$output_dir/lean-30s.csv"
"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/kernel-30s-db" \
  --profile kernel --campaign warm --duration-seconds 30 \
  >"$output_dir/kernel-30s.csv"
"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/kernel-60s-db" \
  --profile kernel --campaign preflight --duration-seconds 60 \
  >"$output_dir/kernel-60s.csv"
"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/lean-300s-db" \
  --profile lean --campaign preflight --duration-seconds 300 \
  >"$output_dir/lean-300s.csv"
"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/kernel-300s-db" \
  --profile kernel --campaign preflight --duration-seconds 300 \
  >"$output_dir/kernel-300s.csv"
"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/lean-1800s-db" \
  --profile lean --campaign sustained --duration-seconds 1800 \
  >"$output_dir/lean-1800s.csv"
"$bench" "${production[@]}" --seed-db "$output_dir/seed-db" \
  --database-path "$output_dir/kernel-1800s-db" \
  --profile kernel --campaign sustained --duration-seconds 1800 \
  >"$output_dir/kernel-1800s.csv"
```

The 1,800-second command runs only after the 300-second row has no hang,
write-stop, rejection, background error, unbounded debt, or reopen failure.
The runner records command line, seed checkpoint identity, host/device, commit
revision, RocksDB revision, compiler, and exact configuration next to every
CSV. Lean and Kernel use the same explicit 1 GiB budget. There is no Generic
benchmark or compatibility workload.

- [ ] **Step 4: Add campaign stop conditions and evidence validation**

At the end of each interval, validate the CSV row before proceeding:

```text
reopen_verified == true
maintenance_first_error is empty
maintenance_unexplained_autonomous_jobs == 0
maintenance_max_snapshot_age_us <= 250000
retained_wal_bytes < 1073741824
pending_compaction_bytes < 34359738368
write_stopped == 0 during the final interval
```

Implement `CampaignExitCode(const KernelBenchmarkOptions&, const CommitSample&)`
in `cedar_kernel_bench_options.cc`. Smoke/warm/preflight return nonzero for any
listed correctness or maintenance failure while still reporting
`warm_not_sustained`; sustained additionally requires all Task 9 qualification
predicates. `cedar_kernel_bench` returns that code after flushing the CSV row,
so the runner's `set -e` prevents the next duration from starting after a bad
preflight.

For the final ten minutes of the sustained run also require non-monotonic L0,
WAL, obsolete, and temporary-output classes, projected scan bytes below the
canonical scan, and Kernel live SST within 5% of Lean unless the evidence row
itemizes file-boundary effects. A failed run may stop early and is labeled
`failed_<reason>`; a successful run may not stop early.

- [ ] **Step 5: Run campaign and production-device comparison**

```bash
cmake --build build-kernel-native --target cedar_kernel_bench -j2
./benchmarks/run_cedar_maintenance_campaign.sh \
  docs/superpowers/evidence/2026-08-18-campaign-native

# Repeat the same command on the provisioned production WAL device and set
# CEDAR_BENCH to the production build path.
CEDAR_BENCH=./build-kernel-production/cedar_kernel_bench \
  ./benchmarks/run_cedar_maintenance_campaign.sh \
  docs/superpowers/evidence/2026-08-18-campaign-production
```

Do not average Lean and Kernel from different seeds or different initial LSM
layouts. Report median/p95/p99 latency, transactions per sync, durable-sync
ceiling, WAL append/sync/manifest costs, read bytes, live/WAL/obsolete/temp
space classes, maintenance debt, and every qualification status.

- [ ] **Step 6: Apply rollout decision**

Mark Kernel production-ready only if native correctness, crash, reopen,
sanitizer, read, write, space, N+1, and both 1,800-second campaign rows pass.
Otherwise keep Kernel opt-in and use Lean only for fresh-database measurement;
no database rewrite or Cedar-specific replay is permitted. Record the decision and the limiting
measurement in the evidence document, including whether the remaining limit is
durable device sync, WAL serialization, Cedar CPU, RocksDB job CPU, or storage
space.

- [ ] **Step 7: Commit the campaign harness and evidence template**

```bash
chmod +x benchmarks/run_cedar_maintenance_campaign.sh
git add benchmarks/cedar_kernel_bench_options.h \
  benchmarks/cedar_kernel_bench_options.cc benchmarks/cedar_kernel_bench.cc \
  benchmarks/commit_workloads.h benchmarks/commit_workloads.cc \
  tests/test_kernel_bench_options.cc README.md \
  benchmarks/run_cedar_maintenance_campaign.sh \
  docs/superpowers/evidence/2026-08-18-cedar-admission-gated-maintenance.md
git commit -m "perf: run Cedar maintenance qualification campaign"
```

The plan is complete only after the evidence file contains all command lines,
CSV rows, failed-run reasons, and the final rollout decision. A 30-second,
60-second, or 300-second success remains a preflight and cannot be reported as
sustained.

---
