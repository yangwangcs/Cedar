# Recovery Typed Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the complete `TransactionCoordinator::Open()` recovery and rebuild path as one synchronous `WorkClass::kRecovery` task on the shared `WorkExecutionService`, preserving exact success and failure status.

**Architecture:** `Open()` first ensures that a running execution service exists, then submits one recovery callback with a one-CPU resource request and waits through `WaitForTask`. The existing recovery body moves unchanged into `OpenInternal()`, including nested index/stats rebuild submissions; `WaitForTask` supplies nested progress for a one-worker service. Externally supplied services must be started before `Open()` because there is no direct-execution fallback.

**Tech Stack:** C++17, Cedar `TransactionCoordinator`, `WorkExecutionService`, `WorkScheduler`, `ResourceGovernor`, GoogleTest.

## Global Constraints

- Preserve database format version `1` and all current clean-break layouts.
- Do not restore external V2/Vn names, old runtimes, old layout readers, or direct recovery fallbacks.
- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Build and test with `-j1`.
- Preserve the exact typed status returned by recovery and by scheduler admission.

---

### Task 1: RED recovery execution and accounting contract

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `TransactionCoordinator::Open`, `WorkExecutionService::stats`, `WorkClass::kRecovery` at stats index `1`.
- Produces: a test-only recovery execution hook and regressions proving successful and failed recovery callbacks are submitted, admitted, completed, and executed away from the caller thread.

- [x] Add `SetRecoveryExecutionHookForTesting(std::function<void()>)` with mutex-protected storage matching the existing flush execution-hook pattern.
- [x] Add `DurableLogTest.OpenRunsRecoveryThroughTypedScheduler` using a started one-worker external service; capture the caller and hook thread IDs, require them to differ, and require `submitted[1]`, `admitted[1]`, and `completed[1]` to equal one.
- [x] Add `DurableLogTest.FailedOpenStillCompletesTypedRecoveryTask` using a current `FORMAT` plus unsupported `manifest/SCHEMA`; require `NotSupported` and one submitted/admitted/completed recovery task.
- [x] Run `cmake --build build-current -j1` followed by the two focused tests and confirm RED because `Open()` still performs recovery directly and recovery counters remain zero.

### Task 2: Route the full recovery body through the shared service

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`

**Interfaces:**
- Consumes: `WorkExecutionService::Submit(WorkTaskRequest, callback)` and `WaitForTask`.
- Produces: private `Status OpenInternal()` containing the complete durable recovery/rebuild body.

- [x] Declare private `Status OpenInternal()` and move everything after `EnsureWorkExecutionService()` from `Open()` into it without changing durable ordering.
- [x] Invoke the recovery hook at the beginning of `OpenInternal()` after copying it under its mutex.
- [x] In `Open()`, submit `WorkTaskRequest{WorkClass::kRecovery, ResourceProfile{0, 0, 0, 0, 1}, false, 0}` and call `OpenInternal()` only inside the callback.
- [x] Return submission rejection directly; otherwise return `WaitForTask(handle)`, which is the callback's exact typed status.
- [x] Run the two focused tests and require GREEN.

### Task 3: External-service ordering and nested recovery regressions

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: the new precondition that an externally injected service is running before `Open()`.
- Produces: all existing external-service tests using the valid start-bind-open order and retained one-worker nested index/stats recovery coverage.

- [x] Move `service.Start()` before `coordinator.Open()` in every existing test that injects an external service, without changing unrelated assertions.
- [x] Run all `DurableLogTest` tests involving pressure, flush, blob rotation, reopen queue reconstruction, and work-service binding with `-j1`.
- [x] Run focused `WorkExecutionService` and recovery tests with `-j1` to prove nested one-worker progress and typed failure preservation.

### Task 4: Closure evidence

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: focused and regression test output.
- Produces: accurate closure evidence while leaving shutdown, executing-maintenance cancellation, sanitizer refresh, and release artifacts open.

- [x] Record recovery typed admission as implemented with exact code and tests.
- [x] Run `cmake --build build-current -j1` and `ctest --test-dir build-current -j1 --output-on-failure`.
- [x] Run whitespace checks for tracked and touched untracked source/doc files.
- [x] Do not claim the six-design closure goal complete; advance next to drain-first shutdown/`WorkClass::kShutdown`.

Verification checkpoint (2026-07-23): both recovery counter tests first failed
with submitted/admitted/completed remaining zero. After the complete open body
was moved to `OpenInternal()` and submitted as `kRecovery`, both passed; the
thread hook proves the durable body executes off the caller. All 302
`DurableLogTest` cases and the full normal 814/814 matrix pass with `-j1`.
Tracked and touched-untracked whitespace checks pass. A subsequent local
normal/ASAN/UBSAN/TSAN refresh passes 814/814 in every tree. Shutdown,
executing-task cancellation, and replacement release artifacts remain open.
