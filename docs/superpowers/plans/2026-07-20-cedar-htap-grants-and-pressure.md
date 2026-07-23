# Cedar HTAP Grants and Pressure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make queued work, query spill, Blob GC, prepared transaction completion, pressure policy, and scheduler telemetry obey one explicit resource-grant contract.

**Architecture:** `WorkExecutionService` acquires and owns the atomic `ResourceLease` for every admitted queued task, so admission, cancellation, execution, and shutdown have one release point. Query spill performs bounded descriptor and temporary-space extensions at the file boundary, while Blob GC declares relocation output before it enters the maintenance queue. The transaction coordinator separates revocable pre-prepare admission from a critical completion grant that survives pressure changes, and the pressure controller exposes typed states, causes, and actions without replacing the existing scheduler.

**Tech Stack:** C++17, Cedar `Status`/`StatusOr`, RAII `ResourceLease`, POSIX spill files, GoogleTest correctness kernel, CMake/CTest.

## Global Constraints

- Preserve the existing `ResourceProfile` field order and all nine independently enforced dimensions.
- Non-critical work must never consume `ResourceGovernor::critical_reserve`; only `commit_critical` requests may use it.
- Acquire a complete task request atomically before enqueue; rejected admission must not create scheduler work or an accepted handle.
- One owner charges each physical resource once. Maintenance callbacks must not reacquire a lease already owned by their queued task.
- Query spill must acquire one descriptor before `mkstemp`, acquire temporary bytes before each write, and release both on close, destruction, cancellation, and error.
- Spill partitions remain lazy; an unopened or empty partition consumes no descriptor or temporary-space grant.
- Blob relocation must reserve bounded output bytes and descriptors before copying; it must not consume the critical disk reserve.
- A transaction must acquire its completion grant before the first durable PREPARE append and hold it through DecisionLog durability, visible-prefix publication, outcome/index updates, and reservation release.
- After any durable decision, Cedar returns committed only after visible publication; otherwise it returns indeterminate/recovery-required, never an ordinary admission or maintenance error.
- Pressure states are exactly `NORMAL`, `SOFT_PRESSURE`, `HARD_PRESSURE`, `WRITE_STALL`, `DISK_EMERGENCY`, `RECOVERY`, and `SHUTDOWN`; transitions preserve hysteresis.
- Prepared completion bypasses new-write stalls. Optional maintenance and new analytical admission yield before commit-critical work.
- Metrics use registered stable names and bounded `WorkClass`/pressure-cause labels; no query text, transaction id, path, or other unbounded identifier becomes a label.
- Use TDD for every production behavior: add a focused test, run it and observe the intended failure, then implement the minimum behavior and rerun.
- Do not commit, reset, clean, or revert the current reconstructed worktree unless the user explicitly requests it.
- Defer the previously registered Sort/HashJoin codec-memory, file-descriptor batching, hot-key fallback, NaN ordering, and late terminal-status constraint debt until all four functionality tasks are complete.

---

## File Structure

- `include/cedar/runtime/work_execution_service.h`: public admitted-task request, handle, task grant ownership, and bounded service telemetry.
- `src/runtime/work_execution_service.cc`: atomic admission, queue ownership, cancellation/stop release, completion, and metrics updates.
- `include/cedar/runtime/maintenance_executor.h`: maintenance task declaration and shared-governor wiring.
- `src/runtime/maintenance_executor.cc`: submit typed requests without callback-time double acquisition.
- `include/cedar/runtime/resource_profile.h`: overflow-safe resource arithmetic and lease extension support used by spill files.
- `include/cedar/tcypher/runtime/query_spill.h`: spill resource owner constructor and per-file descriptor/temp leases.
- `src/tcypher/runtime/query_spill.cc`: reserve-before-open/write and release-on-all-terminal-paths behavior.
- `include/cedar/tcypher/executor.h`: query-local pointer to the database resource governor for physical spill extensions.
- `src/db/cedar_database_v2.cc`: install the governor into admitted query options without precharging spill descriptors twice.
- `include/cedar/tcypher/runtime/query_result.h`, `src/tcypher/runtime/query_result.cc`, `src/tcypher/runtime/query_runtime.cc`, `src/tcypher/executor.cc`: propagate the spill resource owner to every aggregate/distinct/sort/hash-join spill file and partition set.
- `include/cedar/blob/blob_store.h`, `src/blob/blob_store.cc`: estimate live bytes that actually require relocation.
- `src/transaction/transaction_coordinator.cc`: Blob GC task request and prepared completion grant lifecycle.
- `include/cedar/transaction/transaction_coordinator.h`: prepared-boundary fault point and completion grant helper declarations.
- `include/cedar/runtime/pressure_controller.h`: typed pressure states, causes, actions, and hysteresis.
- `include/cedar/observability/metric_registry.h`, `src/db/cedar_database_v2.cc`: bounded scheduler/resource/pressure metric registration and publication.
- `tests/test_correctness_kernel.cc`: all RED/GREEN model, ownership, failure, and integration tests.
- `.superpowers/sdd/progress.md`: durable completion ledger after each clean review.

---

### Task 1: Admitted Work Task Contract

**Files:**
- Modify: `include/cedar/runtime/work_execution_service.h`
- Modify: `src/runtime/work_execution_service.cc`
- Modify: `include/cedar/runtime/maintenance_executor.h`
- Modify: `src/runtime/maintenance_executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `ResourceGovernor::Acquire(const ResourceProfile&, bool)` and move-only `ResourceLease`.
- Produces:

```cpp
struct WorkTaskRequest {
  WorkClass work_class = WorkClass::kAnalyticalQuery;
  ResourceProfile resources;
  bool commit_critical = false;
  uint64_t deadline_sequence = 0;
};

struct WorkExecutionStats {
  std::array<uint64_t, 13> submitted{};
  std::array<uint64_t, 13> admitted{};
  std::array<uint64_t, 13> rejected{};
  std::array<uint64_t, 13> cancelled{};
  std::array<uint64_t, 13> completed{};
};

StatusOr<WorkTaskHandle> WorkExecutionService::Submit(
    WorkTaskRequest request, std::function<Status()> callback);
Status WorkExecutionService::ConfigureResourceGovernor(
    ResourceGovernor* resource_governor);
WorkExecutionStats WorkExecutionService::stats() const;
```

- The existing `Submit(WorkClass, callback, deadline)` remains and delegates to an empty-resource `WorkTaskRequest`.
- `RegisteredTask` owns `std::optional<ResourceLease> grant`; moving it to a worker transfers, rather than copies, ownership.
- `MaintenanceExecutor::Configure` calls `ConfigureResourceGovernor` to install its single `ResourceGovernor` into the execution service. Reconfiguration is accepted only when no task is registered; the synchronous no-service fallback may acquire directly, while the queued path must not.

- [ ] **Step 1: Write failing queued-grant ownership tests**

Add tests that block the only worker with a commit-critical callback, submit a second resource-bearing task, and assert the second task's descriptor/CPU grant is visible before its callback executes. Add cancellation and callback-failure variants that assert every used dimension returns to zero after `Wait()`.

```cpp
WorkTaskRequest request{WorkClass::kStatsMerge,
                        ResourceProfile{4, 3, 1, 2, 1, 5, 6, 7, 8},
                        false, 0};
auto queued = service.Submit(request, [] { return Status::OK(); });
ASSERT_TRUE(queued.ok());
EXPECT_EQ(governor.used().descriptors, 1U);
service.Cancel(queued.ValueOrDie().id());
EXPECT_TRUE(queued.ValueOrDie().Wait().IsQueryCancelled());
EXPECT_EQ(governor.used().descriptors, 0U);
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='WorkExecutionServiceTest.QueuedTaskRetainsAtomicGrantBeforeExecution:WorkExecutionServiceTest.CancellationAndFailureReleaseEveryGrantedDimension:WorkExecutionServiceTest.RejectedAdmissionNeverEntersScheduler'
```

Expected: compile failure because `WorkTaskRequest` and the typed overload do not exist.

- [ ] **Step 3: Implement atomic admission and single lease ownership**

Acquire the request under the service admission lock after verifying `running_ && !stopping_`, before allocating/enqueueing scheduler work. On any later allocation or insertion failure, let the local lease destruct. Move the lease into `RegisteredTask`; erase/destruction releases it on cancel, callback completion, and any accepted-task drain during `Stop()`.

Use an explicit `WorkClassIndex(WorkClass)` mapping shared by stats; never index using enum aliases or casts.

- [ ] **Step 4: Make maintenance enqueue the full request**

Replace callback-time `ResourceGovernor::Acquire` in the queued path with:

```cpp
WorkTaskRequest request{spec.work_class, spec.resources,
                        spec.commit_critical, 0};
auto submitted = execution_service_->Submit(
    request, [this, owned_spec] { return RunIoAndCallback(*owned_spec); });
```

`RunIoAndCallback` consumes I/O tokens immediately before the callback but does not reacquire `spec.resources`. Keep direct synchronous resource admission only when no execution service is configured.

- [ ] **Step 5: Verify critical reserve isolation and stop behavior**

Add tests where an optional request exactly reaches the non-critical cap and is rejected when it crosses into the reserve, while an otherwise identical `commit_critical` request is admitted. Retain the existing contract that `Stop()` drains accepted callbacks, completes all handles, and releases every task grant.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='ResourceGovernorTest.*:WorkSchedulerTest.*:WorkExecutionServiceTest.*:MaintenanceExecutorTest.*:DurableLogTest.FlushWaitsOnlyForItsOwnExecutableTask:DurableLogTest.FlushCancelsRejectedMaintenanceWorkBeforeRetrying'
```

Expected: all selected tests pass and `governor.used()` is zero after every terminal path.

- [ ] **Step 6: Review and record Task 1**

Run `git diff --check`, perform task-scoped spec and code-quality review, fix all Critical/Important findings, re-run the focused filter, then append a Task 1 completion line to `.superpowers/sdd/progress.md`. Do not commit.

---

### Task 2: Spill and Blob GC Descriptor/Temporary-Space Accounting

**Files:**
- Modify: `include/cedar/runtime/resource_profile.h`
- Modify: `include/cedar/tcypher/runtime/query_spill.h`
- Modify: `src/tcypher/runtime/query_spill.cc`
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/db/cedar_database_v2.cc`
- Modify: `include/cedar/tcypher/runtime/query_result.h`
- Modify: `src/tcypher/runtime/query_result.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `src/tcypher/executor.cc`
- Modify: `include/cedar/blob/blob_store.h`
- Modify: `src/blob/blob_store.cc`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: Task 1's queued `WorkTaskRequest` grant ownership.
- Produces:

```cpp
Status ResourceLease::Extend(const ResourceProfile& additional,
                             bool commit_critical = false);

QuerySpillFile(std::string directory,
               std::shared_ptr<QueryCancellation> cancellation = nullptr,
               ResourceGovernor* resources = nullptr);

PartitionedSpillSet(std::string directory, uint32_t partition_count,
                    std::shared_ptr<QueryCancellation> cancellation = nullptr,
                    ResourceGovernor* resources = nullptr);

StatusOr<uint64_t> BlobStore::EstimateRelocationBytes(
    const std::vector<BlobHash>& hashes) const;
```

- `TcypherQueryOptions::spill_resource_governor` is non-owning and populated by `CedarDatabaseV2` after admission; a caller cannot replace the database-owned pointer.
- Each spill file owns one descriptor lease plus an accumulated temporary-byte lease. `bytes_written()` and reserved temporary bytes advance only after reserve succeeds; failed writes keep the conservative reservation until close.
- `EstimateRelocationBytes` sums record bytes only for live hashes currently in sealed segments, with saturating/checked arithmetic and a corruption error for a missing live hash.

- [ ] **Step 1: Write failing spill accounting tests**

Add tests for:

- descriptor is charged before a successful `Open()` and released by `Close()`;
- zero descriptor budget prevents file creation;
- header and record bytes are charged before writing;
- insufficient temporary space rejects append without growing the file;
- cancellation, failed open, explicit close, and destruction release all charges;
- `PartitionedSpillSet` charges only the partitions first appended to.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='DurableLogTest.QuerySpillAccountsDescriptorAndTemporaryBytes:DurableLogTest.QuerySpillReleasesAccountingOnCancellationAndErrors:DurableLogTest.PartitionedSpillChargesOnlyOpenedPartitions'
```

Expected: compile failure because spill constructors do not accept a governor.

- [ ] **Step 2: Implement reserve-before-I/O spill ownership**

In `Open()`, acquire `ResourceProfile{0, 0, 1}` before `create_directories`/`mkstemp`, then reserve the file-header bytes before `WriteAll`. In `AppendRecord()`, compute `8 + payload.size()` with checked arithmetic, extend the temporary lease, and only then allocate/encode/write the record. `Close()` closes/unlinks first and releases both leases even if close or unlink reports an error.

`ResourceLease::Extend` must lock the same governor state, test the current global used profile plus the additional profile atomically, update both global `used` and the lease's `reservation_`, and reject overflow without partial mutation.

- [ ] **Step 3: Propagate the spill governor through every physical operator**

Set `admitted_options.spill_resource_governor = &resource_governor_` in `CedarDatabaseV2`. Remove `kQueryDescriptorReservation` from the base query lease so physical descriptors are not charged twice; retain query hard memory and one CPU slot. Pass the pointer through aggregate, collect, distinct, sort, HashJoin, MultiHashJoin, executor materialization, and partitioned spill constructors.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='*Spill*:*ExternalSort*:PhysicalHashJoinTest.*:PhysicalMultiHashJoinTest.*:DurableLogTest.DatabaseRejectsQueryWhoseDeclaredMemoryGrantExceedsBudget'
```

Expected: all selected spill and query-admission tests pass.

- [ ] **Step 4: Write failing Blob GC grant tests**

Create a sealed segment with one live payload, set a governor whose temporary-space cap is one byte below `EstimateRelocationBytes`, and assert `CollectBlobGarbage()` is rejected before the active segment or index grows. With the exact reserve, assert relocation completes and all task resources return to zero.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='DurableLogTest.BlobGcReservesRelocationOutputBeforeCopy:DurableLogTest.BlobGcReleasesRelocationGrantAfterFailure'
```

Expected: test fails because the current Blob GC request declares zero temporary/output bytes.

- [ ] **Step 5: Declare Blob GC relocation resources before enqueue**

Snapshot `blob_reference_catalog_.LiveHashes()`, call `EstimateRelocationBytes`, and submit `WorkClass::kBlobGc` with descriptor count sufficient for one input segment, active output segment, and index delta, plus `temporary_bytes` and `write_bytes` equal to the estimate. Capture the exact live-hash snapshot in the callback so the estimate and copied set agree; revalidate catalog safety before Manifest retirement.

- [ ] **Step 6: Review and record Task 2**

Run the Task 2 filters and `git diff --check`, perform task-scoped review/re-review, then append a Task 2 completion line to `.superpowers/sdd/progress.md`. Do not commit.

---

### Task 3: Prepared Transaction Completion Grant

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `ResourceGovernor::Acquire`, critical reserve isolation, `IoGovernor` critical tokens, DecisionLog and visible-prefix ordering.
- Produces:

```cpp
enum class CommitFaultPoint : uint8_t {
  kAfterPrepareDurable = 0,
  kAfterDecisionDurable = 1,
};

struct PreparedCompletionGrant {
  ResourceLease resources;
  ResourceProfile profile;
};
```

- The completion profile covers one CPU slot, participant prepare/DecisionLog descriptors, estimated DecisionLog/write bytes, and participant-plus-publication metadata operations. It is acquired with `commit_critical=true` before the first PREPARE append and remains in the `CommitInternal` stack frame until committed, aborted before decision, or indeterminate.
- [ ] **Step 1: Write failing prepared-boundary tests**

Add tests that:

- exhaust the normal pool while leaving the critical reserve and prove commit completion enters the reserved capacity;
- at `kAfterPrepareDurable`, transition pressure to `WRITE_STALL` and consume all remaining non-critical capacity, then return `OK`; the transaction must still durably decide and publish;
- reject before any PREPARE record when the completion grant cannot be acquired;
- inject failure at `kAfterDecisionDurable` and preserve the existing indeterminate/recovery-required contract.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='DurableLogTest.PreparedCommitRetainsCompletionGrantAcrossWriteStall:DurableLogTest.CommitRejectsBeforePrepareWhenCompletionGrantIsUnavailable:DurableLogTest.DurableDecisionPublicationFailureIsIndeterminateUntilRecovery'
```

Expected: compile failure for `kAfterPrepareDurable` or behavioral failure because completion ownership is not exposed as a distinct boundary.

- [ ] **Step 2: Split pre-prepare and completion admission**

Keep pressure and conflict/schema validation revocable. After participants and exact completion requirements are known but before appending any prepare record, acquire the critical completion grant and critical I/O tokens atomically for the remaining durability chain. Move Blob externalization and any ordinary allocation that can abort ahead of this boundary where safe.

Invoke `kAfterPrepareDurable` after the final participant prepare append and before allocating/appending the decision. Ignore cancellation and new pressure admission after this point; only durability/protocol errors may terminate it.

- [ ] **Step 3: Preserve post-decision outcome semantics**

Keep one `require_recovery` path for all failures after `DecisionLog::AppendCommit`. Verify `AddDurableCommit`, `InstallDecision`, and visible-prefix advancement execute while the completion grant is active. Release the grant only after final outcome construction and resource/catalog publication are complete.

- [ ] **Step 4: Verify commit and recovery regression**

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='DurableLogTest.*Commit*:DurableLogTest.*Decision*:DurableLogTest.*Recovery*:VisiblePrefixTest.*:ResourceGovernorTest.*'
```

Expected: all selected tests pass; a durable-decision fault remains indeterminate and a pre-prepare resource failure remains aborted.

- [ ] **Step 5: Review and record Task 3**

Run `git diff --check`, review the precise durability boundary and RAII lifetime, fix and re-review all Critical/Important findings, then append a Task 3 completion line to `.superpowers/sdd/progress.md`. Do not commit.

---

### Task 4: Typed Pressure Actions and Bounded Telemetry

**Files:**
- Modify: `include/cedar/runtime/pressure_controller.h`
- Modify: `include/cedar/runtime/work_execution_service.h`
- Modify: `src/runtime/work_execution_service.cc`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `src/db/cedar_database_v2.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: Task 1 `WorkExecutionStats`, Task 3 completion boundary, existing `MetricRegistry`.
- Produces:

```cpp
enum class PressureState : uint8_t {
  kNormal,
  kSoftPressure,
  kHardPressure,
  kWriteStall,
  kDiskEmergency,
  kRecovery,
  kShutdown,
};

enum class PressureCause : uint8_t {
  kNone,
  kMemtable,
  kWal,
  kCompaction,
  kCache,
  kDisk,
};

struct PressureDecision {
  PressureState state;
  PressureCause cause;
  bool admit_writes;
  bool admit_analytical;
  bool admit_optional_maintenance;
  bool cancel_queued_analytical;
  bool require_flush;
  bool require_urgent_compaction;
  bool require_blob_gc;
};
```

- Cause selection is deterministic: highest normalized signal wins; ties use disk, WAL, MemTable, compaction, cache safety order.
- Disk at the emergency threshold enters `kDiskEmergency`; WAL/MemTable at the write-safety threshold enters `kWriteStall`; other hard signals enter `kHardPressure`; values between low/high enter `kSoftPressure`.
- Falling transitions require the low watermark, preserving current hysteresis. `kRecovery` and `kShutdown` are explicit externally requested modes and do not arise from scalar pressure sampling.

- [ ] **Step 1: Write failing pressure model tests**

Add a table-driven test for every state, deterministic cause tie, action flags, and hysteresis on downward transitions. Update the old warning/critical/emergency expectation to the new typed state names.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='PressureControllerTest.*:DurableLogTest.PressureStallsNewWritesWhenFailedFlushRetainsFrozenMemtable'
```

Expected: compile failures for the new enum names and action fields.

- [ ] **Step 2: Implement typed states, causes, and actions**

Keep `Update(uint64_t)` as a compatibility adapter using a MemTable signal. Add explicit `EnterRecovery()` and `EnterShutdown()` methods. Ensure `TransactionCoordinator::AdmitQuery` rejects only new analytical admission as directed, while point reads remain available unless recovery/shutdown correctness requires rejection. Commit checks write admission only before the completion boundary.

- [ ] **Step 3: Write failing scheduler/resource telemetry tests**

Register and assert these stable metrics with bounded labels:

```text
cedar_scheduler_tasks_submitted_total
cedar_scheduler_tasks_admitted_total
cedar_scheduler_tasks_rejected_total
cedar_scheduler_tasks_cancelled_total
cedar_scheduler_tasks_completed_total
cedar_resource_used_bytes
cedar_resource_used_units
cedar_pressure_transitions_total
cedar_pressure_state
```

Labels are the 13 static work-class names, nine static resource dimension names, seven pressure-state names, and six pressure-cause names only.

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='WorkExecutionServiceTest.ExposesBoundedPerClassTelemetry:PressureControllerTest.RecordsCauseAndTransitionTelemetry:DurableLogTest.DatabaseExportsSchedulerResourceAndPressureMetrics'
```

Expected: tests fail because the metrics are not registered/published.

- [ ] **Step 4: Integrate lightweight telemetry**

Increment task counters at the exact ownership transition: submitted before admission, rejected on failed grant/service state/id allocation, admitted after registration/enqueue, cancelled on accepted cancellation, completed after callback status publication. Snapshot `ResourceGovernor::used()` and the current pressure decision when exporting or refreshing metrics; never add a callback or allocation to a hot record loop.

- [ ] **Step 5: Run the unified functionality regression**

Run:

```bash
cmake --build build-v2 -j2
./build-v2/tests/test_correctness_kernel --gtest_filter='ResourceGovernorTest.*:WorkSchedulerTest.*:WorkExecutionServiceTest.*:MaintenanceExecutorTest.*:PressureControllerTest.*:*QuerySpill*:*PartitionedSpill*:*BlobGc*:*Commit*:DurableLogTest.DatabaseExportsSchedulerResourceAndPressureMetrics'
ctest --test-dir build-v2 --output-on-failure
git diff --check
```

Expected: focused filter and complete CTest pass; whitespace check is clean.

- [ ] **Step 6: Review and record Task 4**

Perform task review/re-review, append Task 4 completion to `.superpowers/sdd/progress.md`, and then perform one whole-branch functionality review across Tasks 1-4. Do not claim the deferred constraint debt is complete.

---

## Post-Functionality Constraint and Regression Gate

Only after Tasks 1-4 are reviewed clean:

1. Address the registered external Sort constraints: bounded generated-run FDs, strict floating-point total order, codec/merge/output memory charging, byte-bounded merge batches, late terminal status, and cancellation/NULL-order coverage.
2. Address shared HashJoin/spill constraints: bounded partition descriptors, codec/decode charging, hot-key replay fallback, and late build/probe terminal status.
3. Run focused constraint tests first, then crash/fault/recovery/oracle tests, ASan/UBSan, complete correctness kernel, CTest, and benchmark structural acceptance as one final matrix.
4. Record exact commands and pass counts in `.superpowers/sdd/progress.md`; report any environment-limited sanitizer or benchmark command explicitly.

## Plan Self-Review

- Spec coverage: Tasks 1-4 cover admitted task grants, cancellation release, query spill and Blob relocation temporary space/descriptors, non-revocable prepared completion, typed pressure actions/hysteresis, and bounded observability. Full device token yielding, shutdown phase decomposition, and performance tuning remain outside this minimum functionality batch and are not claimed.
- Placeholder scan: no TBD/TODO/"similar to" steps remain; every task names interfaces, tests, commands, failure expectations, and release invariants.
- Type consistency: `WorkTaskRequest.resources` is the single queued task profile; `spill_resource_governor` is the single query spill extension owner; `PreparedCompletionGrant` owns one critical `ResourceLease`; `PressureDecision` is the only pressure action envelope consumed by the coordinator.
