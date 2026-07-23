# Cedar Executing Maintenance Cancellation Implementation Plan

**Status:** implemented and verified on 2026-07-23; archived normal/sanitizer
closure is 849/849 and a later fresh normal matrix passes 855/855. Task
checkboxes below preserve the original execution recipe rather than serving as
live progress.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cooperatively cancel already-running optional maintenance during scheme C shutdown without cancelling correctness-critical work or revoking Manifest publication.

**Architecture:** Add a runtime-owned `WorkCancellation` token and a bounded running-task registry to `WorkExecutionService`. `MaintenanceExecutor` passes the token into maintenance callbacks, while compaction, index, statistics, and Blob GC check it only at declared safe boundaries. `CedarDatabase::Close()` requests queued and running optional-maintenance cancellation before waiting for maintenance leases, repeats the request after accepted commit/flush work drains, and checkpoints only after callbacks release all grants and temporary output ownership.

**Tech Stack:** C++17, CMake/CTest, GoogleTest, atomics, shared ownership, Cedar scheduler/resource governor, Manifest/VersionSet durability.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Run every build and test with `-j1`.
- Keep internal database format number `1`.
- Do not restore external V2/Vn names, old runtime paths, fallbacks, or old-layout readers.
- Only `kCompactionNormal`, `kIndexBuild`, `kStatsMerge`, and `kBlobGc` are preemptible.
- Never cancel urgent compaction, flush, checkpoint, recovery, shutdown, commit-critical, foreground write, or point-read work.
- Cancellation returns typed `QueryCancelled`; real durable I/O or indeterminate publication status wins after publication begins.

---

### Task 1: Runtime cancellation token and running-task registry

**Files:**
- Create: `include/cedar/runtime/work_cancellation.h`
- Modify: `include/cedar/runtime/work_execution_service.h`
- Modify: `src/runtime/work_execution_service.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `class WorkCancellation` with `bool Cancel()`, `bool IsCancelled() const`, and `Status Checkpoint(std::string_view owner) const`.
- Produces: `WorkTaskRequest::preemptible`, `WorkTaskRequest::cancellation`, and `size_t WorkExecutionService::CancelPreemptible(WorkClass)`.
- Preserves: `CancelQueued(WorkClass)` for pressure/query cancellation that must not signal already-running callbacks.

- [x] **Step 1: Write RED tests for a running preemptible task**

  Add a one-worker test that blocks inside a `kStatsMerge` callback, calls `CancelPreemptible(kStatsMerge)`, observes the shared token become cancelled, returns `token->Checkpoint("test maintenance")`, and asserts typed `QueryCancelled`, `cancelled == 1`, `completed == 1`, and all resource grants released.

- [x] **Step 2: Write RED tests for classification and idempotence**

  Submit a running urgent compaction with `preemptible=false` and assert class cancellation returns zero and does not signal it. Call `CancelPreemptible` twice for one running normal compaction and assert the cancellation counter increments exactly once.

- [x] **Step 3: Run focused RED tests**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'WorkExecutionServiceTest\.(CancelsRunningPreemptibleTask|RunningCancellationIsIdempotent|DoesNotCancelNonPreemptibleRunningTask)' --output-on-failure`

  Expected: compile/test failure because the token and running cancellation API do not exist.

- [x] **Step 4: Implement `WorkCancellation`**

  Use one `std::atomic<bool>`; `Cancel()` performs `exchange(true, std::memory_order_acq_rel)`, `IsCancelled()` uses acquire load, and `Checkpoint(owner)` returns `Status::QueryCancelled(owner, "optional maintenance cancelled")` only when signalled.

- [x] **Step 5: Register running tasks before releasing the service mutex**

  Extend each registered task with its id, preemptible flag, and shared token. When either `WorkerLoop()` or nested `WaitForTask()` selects a task, move it from `tasks_` into `running_tasks_` before unlocking. On completion erase the running record, decrement `outstanding_tasks_`, increment `completed`, and only then release the final task-owned grant.

- [x] **Step 6: Implement atomic queued-and-running class cancellation**

  Under `mutex_`, remove queued matching tasks exactly as `CancelQueued()` does and signal only running records whose `preemptible` flag is true. Increment `cancelled` only when a queued entry is removed or `WorkCancellation::Cancel()` first returns true. Complete removed queued handles outside the mutex with typed `QueryCancelled`.

- [x] **Step 7: Run focused GREEN tests**

  Run the command from Step 3 and require all selected tests to pass.

### Task 2: Pass task-scoped cancellation through maintenance execution

**Files:**
- Modify: `include/cedar/runtime/maintenance_executor.h`
- Modify: `src/runtime/maintenance_executor.cc`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `std::shared_ptr<WorkCancellation>` from Task 1.
- Produces: `MaintenanceTaskSpec::preemptible` and `std::function<Status(const std::shared_ptr<WorkCancellation>&)> run`.

- [x] **Step 1: Write a RED executor propagation test**

  Block a preemptible maintenance callback, cancel its running class through the service, and assert the callback receives the exact task token and returns `QueryCancelled`. Repeat through one-worker nested progress to prove no deadlock.

- [x] **Step 2: Run the focused RED test**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'MaintenanceExecutorTest\.(PropagatesRunningCancellation|NestedProgressPropagatesCancellation)' --output-on-failure`

- [x] **Step 3: Change the executor contract**

  `SubmitAndRun()` creates one token, sets `WorkTaskRequest.preemptible` and `.cancellation`, and invokes `run(token)`. The direct/no-service path also creates a token so production callbacks never branch on scheduler presence. Check cancellation before I/O admission and immediately before invoking the callback, but do not convert a real I/O error into cancellation.

- [x] **Step 4: Mark only the four optional classes preemptible**

  Set `preemptible=true` for normal compaction, index build, stats merge, and Blob GC call sites. All other `MaintenanceTaskSpec` instances use `false` and ignore the token parameter.

- [x] **Step 5: Run focused GREEN tests**

  Run the command from Step 2 and the existing `MaintenanceExecutorTest` group.

### Task 3: Scheme C shutdown ordering

**Files:**
- Modify: `src/db/cedar_database.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `WorkExecutionService::CancelPreemptible(WorkClass)`.

- [x] **Step 1: Write a RED database shutdown test**

  Start an admitted maintenance operation whose scheduled callback is already running and waiting at a safe boundary. Begin `Close(kCancelQueries)`, assert its token is signalled before `WaitForNoOperations(kMaintenance)` can finish, release the callback, and assert checkpoint executes afterward.

- [x] **Step 2: Write a RED derived-work race test**

  Hold an accepted flush across the first cancellation pass, let it derive optional maintenance, and assert the second pass cancels that work before checkpoint.

- [x] **Step 3: Run focused RED tests**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'DurableLogTest\.CloseCancels(RunningOptionalMaintenanceBeforeWaiting|OptionalMaintenanceDerivedByAcceptedFlush)' --output-on-failure`

- [x] **Step 4: Reorder `Close()`**

  Immediately after `BeginClose()`, call `CancelPreemptible` for all four optional classes before waiting for maintenance leases. After accepted commit and flush work drain, repeat the four calls. Keep a final pass in the `kShutdown` callback, then wait for maintenance callbacks before entering checkpointing.

- [x] **Step 5: Preserve non-preemptible behavior**

  Verify the shutdown task, urgent compaction, flush, and commit-critical work retain non-cancellable tokens and complete before checkpoint/teardown.

- [x] **Step 6: Run focused GREEN tests**

  Run the command from Step 3 plus all existing close/lifecycle tests.

### Task 4: Cancellable normal compaction with publication fence

**Files:**
- Modify: `include/cedar/columnar/sst.h`
- Modify: `src/columnar/sst.cc`
- Modify: `include/cedar/storage/sst_compaction.h`
- Modify: `src/storage/sst_compaction.cc`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: optional `std::shared_ptr<WorkCancellation>` parameters on `MergeSstFilesStreaming()` and `CompactSstPartition()`.

- [x] **Step 1: Write RED tests for temporary-output cleanup and urgent immunity**

  Cancel normal compaction after at least one input/output block boundary and assert `QueryCancelled`, no Manifest generation change, and neither final nor `.tmp` output remains. Run the same hook with urgent compaction and assert it completes.

- [x] **Step 2: Write a RED publication-race test**

  Signal cancellation from a `VersionSet` fault hook after Manifest rename and assert the exact `Indeterminate` status is preserved and the possibly published output is retained.

- [x] **Step 3: Run focused RED tests**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'DurableLogTest\.(RunningNormalCompactionCancellationCleansOutput|UrgentCompactionIgnoresOptionalCancellation|CompactionCancellationAfterManifestRenamePreservesPublication)' --output-on-failure`

- [x] **Step 4: Add streaming safe boundaries**

  Check the token before opening each input, whenever an input cursor loads a new SST block, before every output block append, and before writer finish. The writer destructor already removes unfinished `.tmp` files.

- [x] **Step 5: Fence Manifest publication**

  After the final output is durable, check cancellation once and remove the unmanifested final file when cancelled. Do not check again after `VersionSet::ApplyEdit(edit)` begins; return its exact status and retain output on indeterminate publication.

- [x] **Step 6: Run focused GREEN tests**

  Run the command from Step 3 plus existing compaction tests.

### Task 5: Cancellable index and statistics maintenance

**Files:**
- Modify: `include/cedar/index/index_sidecar.h`
- Modify: `src/index/index_sidecar.cc`
- Modify: `include/cedar/statistics/stats_fragment.h`
- Modify: `src/statistics/stats_fragment.cc`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: optional cancellation parameters for `BuildIndexCandidateSidecar()` and `BuildStatsFragment()`.

- [x] **Step 1: Write RED index tests**

  Cancel between 64-event posting quanta and assert no fragment is attached. Cancel after sidecar write but before `IndexCatalog::AttachFragment` and assert the unmanifested sidecar is removed.

- [x] **Step 2: Write RED stats tests**

  Cancel between 64-event stats quanta and before each `StatsSnapshotStore::Upsert`; assert no partial fragment is published while fragments completed for earlier source identities remain valid.

- [x] **Step 3: Run focused RED tests**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'DurableLogTest\.Running(IndexBuild|StatsMerge)Cancellation' --output-on-failure`

- [x] **Step 4: Implement index safe boundaries**

  Check before every source SST, every 64 source events, after encoding, after sidecar write, and immediately before attach. If cancellation is observed after file publication but before Manifest attach, remove the sidecar and return `QueryCancelled`; once attach begins, return its exact status.

- [x] **Step 5: Implement stats safe boundaries**

  Check before every source SST, every 64 source events, after fragment construction, and immediately before `Upsert`. Never construct or upsert a partial fragment.

- [x] **Step 6: Run focused GREEN tests**

  Run the command from Step 3 plus existing index/statistics tests.

### Task 6: Cancellable Blob GC without unsafe retirement

**Files:**
- Modify: `include/cedar/blob/blob_store.h`
- Modify: `src/blob/blob_store.cc`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `WorkCancellation`.
- Produces: cancellable `BlobStore::RelocateLiveHashes()` at shard/relocation-batch boundaries.

- [x] **Step 1: Write RED cancellation/reopen tests**

  Cancel after relocating one shard or bounded batch and before retirement. Assert `QueryCancelled`, no segment-delete Manifest edit, and reopen can read every live Blob value.

- [x] **Step 2: Write a RED retirement-publication race test**

  Signal cancellation after the Blob retirement Manifest rename and assert the exact success/indeterminate publication result wins; never delete a segment whose publication outcome is uncertain.

- [x] **Step 3: Run focused RED tests**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 -R 'DurableLogTest\.RunningBlobGcCancellation' --output-on-failure`

- [x] **Step 4: Add relocation boundaries**

  Check before reclaim, before each shard, and between bounded relocation batches. Partial relocation is retained because the index may safely point at newly copied records while old sealed segments remain.

- [x] **Step 5: Fence retirement and Manifest publication**

  Check immediately before `RetireUnreferencedSealedSegments()` and again before constructing/applying the segment-delete `VersionEdit`. Do not check after `ApplyEdit()` begins. Only enqueue retired physical segments after successful publication.

- [x] **Step 6: Run focused GREEN tests**

  Run the command from Step 3 plus existing Blob GC/recovery tests.

### Task 7: Full verification and release artifact

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`
- Create: `results/release-closure-20260723-maintenance-cancellation/README.md`
- Create: `results/release-closure-20260723-maintenance-cancellation/manifest.json`
- Create: `results/release-closure-20260723-maintenance-cancellation/SHA256SUMS`

- [x] **Step 1: Run focused scheduling/shutdown/maintenance tests**

  Run all newly added tests and existing `WorkExecutionServiceTest`, `MaintenanceExecutorTest`, close/lifecycle, compaction, index, stats, and Blob GC groups with `-j1`.

- [x] **Step 2: Run full normal matrix**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1 --output-on-failure --output-log results/release-closure-20260723-maintenance-cancellation/normal-ctest.log`

- [x] **Step 3: Rebuild and run ASAN, UBSAN, and TSAN matrices**

  Build each current sanitizer directory with `-j1`, then run each complete CTest matrix with `-j1` and a stable `--output-log` path. Require zero sanitizer diagnostics and zero failed tests.

- [x] **Step 4: Produce the production shutdown/reopen artifact**

  Run a real database workload that starts at least one optional maintenance callback, records the cancellation boundary and checkpoint order, closes, reopens, and verifies all committed values. Record run id, source commit, dirty flag, host, resource profile, binary/log SHA-256, and exact commands.

- [x] **Step 5: Verify repository hygiene**

  Run: `git diff --check`

  Run: `rg -n '[[:blank:]]+$' include src tests docs/superpowers .superpowers results/release-closure-20260723-maintenance-cancellation`

- [x] **Step 6: Update closure tracking**

  Mark the cooperative-cancellation functional gap closed only after the full evidence exists. Keep unrelated workstation/paper/stress and approved-baseline release artifacts open.

## Self-Review

- Every cancellation-safe boundary in the approved design is assigned to Tasks 4-6.
- Queued/running separation, exactly-once counters, nested progress, resource release, and non-preemptible classes are covered by Tasks 1-3.
- Manifest publication is explicitly non-revocable in compaction, index attachment, and Blob retirement paths.
- The plan retains database format `1` and introduces no external versioned names or legacy reader/runtime.
- No implementation step requires staging or committing the dirty worktree.
