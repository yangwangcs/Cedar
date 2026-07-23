# Cedar Maintenance Admission Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make actual flush work and Blob active-segment rotation use the shared typed scheduler/resource/I/O admission path, with deterministic fault and reopen evidence.

**Architecture:** Keep `TransactionCoordinator` synchronous from the public API's perspective. The caller freezes or snapshots the required inputs, submits the complete callback to the existing `WorkExecutionService`, and waits while preserving the existing publication lock order. No new worker, runtime, disk layout, or compatibility path is introduced.

**Tech Stack:** C++17, CMake, GoogleTest, Cedar `WorkExecutionService`, `ResourceGovernor`, `IoGovernor`, `MaintenanceExecutor`, POSIX filesystem.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Use `-j1` for every configure, build, test, and sanitizer command.
- Preserve database internal format number 1 and all current clean-break paths.
- Runtime must not download or install dependencies.
- Existing synchronous public APIs and lock ordering remain stable.
- Cancellation, rejection, and fault paths must preserve explicit typed statuses and release all leases/references.

---

### Task 1: Add red regressions for actual production admission

**Files:**
- Modify: `/Users/wangyang/Desktop/Cedar/tests/test_correctness_kernel.cc`

**Interfaces:**
- Consume existing `TransactionCoordinator`, `WorkExecutionService`, `ResourceGovernor`, `WorkScheduler`, Blob fault injectors, and test fixtures.
- Produce failing tests that distinguish an admitted callback from an empty scheduler token and prove rotation is governed.

- [ ] **Step 1: Add flush callback/admission regression**

Add a test under the durable-log scheduler tests that configures a one-worker
service and a resource governor, writes one property, calls `Flush()`, then
asserts `flush` submitted/admitted/completed counters and zero used resources.
Block the worker with a higher-priority task before calling `Flush()` and assert
the flush work is observable as a queued `kFlush` task before releasing the
blocker. The test must also assert the SST is absent while the flush callback is
blocked, proving the actual I/O is inside the callback rather than after a
no-op token.

- [ ] **Step 2: Add Blob rotation admission regression**

Queue a commit-critical blocker, call `RotateBlobSegments()` on a coordinator
with an active segment, wait until `WorkClass::kBlobGc` is queued, and assert
that no segment size changes while blocked. Release the blocker, wait for
success, and assert all resource dimensions return to zero and the scheduler
records one Blob-GC completion.

- [ ] **Step 3: Add rejection/fault assertions**

Add one test that supplies a governor with insufficient write bytes and asserts
`RotateBlobSegments()` returns `QueryMemoryLimit`/`ResourceExhausted` without
changing the active segment. Add one injected rotation/reconcile failure test
that asserts the status is non-OK, the next reopen remains valid, and the Blob
reference catalog still returns the original live hash.

- [ ] **Step 4: Build and run the red tests**

Run:

```bash
cmake --build /Users/wangyang/Desktop/Cedar/build-current -j1 --target test_correctness_kernel
/Users/wangyang/Desktop/Cedar/build-current/test_correctness_kernel \
  --gtest_filter='*Flush*Admission*:*Blob*Rotation*'
```

Expected: the new flush callback and rotation admission assertions fail against
the current implementation; no existing test may regress.

### Task 2: Route Blob rotation through MaintenanceExecutor

**Files:**
- Modify: `/Users/wangyang/Desktop/Cedar/include/cedar/transaction/transaction_coordinator.h`
- Modify: `/Users/wangyang/Desktop/Cedar/src/transaction/transaction_coordinator.cc`
- Test: `/Users/wangyang/Desktop/Cedar/tests/test_correctness_kernel.cc`

**Interfaces:**
- Consume existing `BlobStore::Estimate...`, `MaintenanceExecutor::SubmitAndRun`, and `WorkClass::kForegroundWrite`.
- Produce a governed `RotateBlobSegments()` implementation with no direct fallback while the coordinator is configured.

- [ ] **Step 1: Add a bounded rotation estimate helper**

Declare a private `EstimateBlobRotationResourcesLocked()` returning
`StatusOr<ResourceProfile>`. Compute descriptor, write-byte, and metadata-op
upper bounds from active segment sizes and the reconciliation manifest estimate;
use checked/saturating arithmetic and reserve at least one descriptor, one CPU
slot, and one metadata operation when work exists.

- [ ] **Step 2: Submit the complete rotation callback**

Change `RotateBlobSegments()` to capture the estimate under
`commit_mutex_`/`flush_mutex_`, then call `maintenance_executor_.SubmitAndRun`
with `WorkClass::kForegroundWrite`, the estimate, an `IoTokenRequest` matching write and
metadata dimensions, and a callback that reacquires the same locks, rechecks
`CheckMutationAllowed`, calls `TrackBlobMutation(blob_store_.RotateActiveSegments())`,
and calls `ReconcileBlobSegments()`.

- [ ] **Step 3: Preserve retry and fault semantics**

Do not publish a new reference source until rotation/reconciliation succeeds.
Ensure all callback exits release the lease through the executor and retain the
existing `recovery_required_` behavior for indeterminate publication results.

- [ ] **Step 4: Run the focused rotation tests**

Run the Task 1 filter and the existing Blob fault/reopen filters with `-j1`.
Expected: all rotation admission, rejection, fault, and reopen tests pass.

### Task 3: Put the complete flush body in the admitted callback

**Files:**
- Modify: `/Users/wangyang/Desktop/Cedar/src/transaction/transaction_coordinator.cc`
- Modify: `/Users/wangyang/Desktop/Cedar/include/cedar/transaction/transaction_coordinator.h` only if a small private helper is needed
- Test: `/Users/wangyang/Desktop/Cedar/tests/test_correctness_kernel.cc`

**Interfaces:**
- Consume existing `FlushEventsToSst`, `WorkTaskRequest`, `ResourceProfile`, and Blob-reference publication helpers.
- Produce a flush task whose callback owns the full SST publication and frozen-shard handoff.

- [x] **Step 1: Extract one-shard flush callback logic**

Extract the current per-shard body into a private helper with parameters
`StorageShard*`, `std::vector<TemporalEvent>`, and the estimated write bytes.
The helper must perform the existing resource-independent work, update file
number/watermark counters, call `CompleteFlushPublished`, and rebuild the Blob
reference source before returning.

- [x] **Step 2: Submit the helper as the actual `kFlush` callback**

For each frozen shard, calculate the same bounded write estimate before submit,
create a `WorkTaskRequest{WorkClass::kFlush, resources, false, 0}`, and submit a
callback capturing the shard and frozen events. Wait for the handle before
moving to the next shard. Remove the empty callback and duplicate caller-side
resource/I/O acquisition. Keep `flush_mutex_` held through the callback wait so
file numbering and Manifest generation remain serialized.

- [x] **Step 3: Handle submit/callback failure without leaks**

On any failed submission or callback, cancel all remaining handles, call the
existing pressure refresh, and ensure every frozen shard is either completed or
returned through its existing abort path. Do not call optional compaction,
index-build, stats-merge, or checkpoint work until all essential flush handles
have completed successfully.

- [x] **Step 4: Run flush and reference regressions**

Run the focused admission tests plus the existing flush, compaction, Blob
reference, and Manifest publication tests with `-j1`. Expected: actual SST
creation is blocked with the worker, and all normal flush/reopen tests remain
green.

### Task 4: Unified sanitizer and fault verification

**Files:**
- Modify: `/Users/wangyang/Desktop/Cedar/docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `/Users/wangyang/Desktop/Cedar/.superpowers/sdd/progress.md`

**Interfaces:**
- Consume the focused tests and existing release artifact conventions.
- Produce exact commands, test counts, and artifact/run evidence; do not mark the overall six-design goal complete yet.

- [x] **Step 1: Run normal focused and full correctness tests**

```bash
cmake --build /Users/wangyang/Desktop/Cedar/build-current -j1 --target test_correctness_kernel
/Users/wangyang/Desktop/Cedar/build-current/test_correctness_kernel
ctest --test-dir /Users/wangyang/Desktop/Cedar/build-current -j1 --output-on-failure
```

- [x] **Step 2: Refresh ASAN, UBSAN, and TSAN focused/full tests**

Use `build-current-asan`, `build-current-ubsan`, and `build-current-tsan`,
always configuring/building/testing with `-j1`; record sanitizer output and
test counts in the progress file.

- [x] **Step 3: Run fault/crash/reopen/oracle scheduler regressions**

Run the Blob rotation, flush publication, Manifest post-rename, Blob INDEX,
sidecar, checkpoint, scheduler cancellation, and independent oracle filters;
record exact command lines and pass counts.

- [x] **Step 4: Update the completion matrix**

Replace the two explicit production-path gap paragraphs with exact code/test
locations and command output. Keep paired baseline/candidate regression and
paper-scale profiles marked evidence-missing until distinct binaries and
artifacts exist.

### Task 5: Repository-wide hygiene gate

**Files:**
- No source changes expected.

- [ ] **Step 1: Run formatting and whitespace checks**

```bash
git diff --check
rg -n '[[:blank:]]+$' /Users/wangyang/Desktop/Cedar/src/transaction/transaction_coordinator.cc /Users/wangyang/Desktop/Cedar/include/cedar/transaction/transaction_coordinator.h /Users/wangyang/Desktop/Cedar/tests/test_correctness_kernel.cc
```

- [ ] **Step 2: Confirm clean-break invariants**

Search only current source/docs for accidental external V2/Vn runtime names or
legacy manifest readers; do not delete unrelated dirty-worktree files.

- [ ] **Step 3: Leave the goal active**

Because paired baseline/candidate, paper-scale, and cross-design evidence are
not covered by this plan, do not call `update_goal complete` after these tasks.
