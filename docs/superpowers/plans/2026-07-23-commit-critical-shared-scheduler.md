# Commit-Critical Shared Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove production `std::thread` creation from transaction PREPARE and participant installation while preserving bounded cross-shard parallelism, commit completion grants, recovery semantics, and deterministic shutdown draining.

**Architecture:** Each shard PREPARE and participant install is a `WorkClass::kCommitCritical` task submitted to the database-wide `WorkExecutionService`. The transaction's already-acquired `PreparedCompletionGrant` owns the physical resource reservation, so child tasks request zero incremental resources but remain typed, queued, observable, and drainable. A directly constructed `TransactionCoordinator` owns a bounded fallback scheduler/service rather than bypassing scheduling; CedarDatabase uses its shared four-slot service.

**Tech Stack:** C++17, Cedar `WorkScheduler`, `WorkExecutionService`, `ResourceGovernor`, transaction DecisionLog/WAL, GoogleTest.

## Global Constraints

- Preserve database format version `1` and all current clean-break layouts.
- Do not add old runtime/fallback readers or external V2/Vn names.
- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Build and test with `-j1`.
- Prepared transactions must never wait behind optional work or lose their completion grant.

---

### Task 1: RED typed-lane and concurrency contracts

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `TransactionCoordinator::SetWorkExecutionService`, `WorkExecutionService::stats`, `WorkClass::kCommitCritical`.
- Produces: regression proof that one two-shard commit submits two PREPARE and two install tasks through the shared service.

- [x] Extend the cross-shard PREPARE/install tests with a started two-worker `WorkExecutionService` and assert `submitted`, `admitted`, and `completed` for `kCommitCritical` increase by four.
- [x] Record callback thread IDs and assert both phases run off the commit caller while retaining two-shard overlap.
- [x] Run the focused tests and confirm RED because current production code creates direct threads and leaves service counters unchanged.

### Task 2: Bounded coordinator-owned execution service

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`

**Interfaces:**
- Produce: `Status EnsureWorkExecutionService()` and coordinator-owned `WorkScheduler`/`WorkExecutionService` used only when no service was injected before `Open()`.

- [x] Add an explicit coordinator destructor that stops only its owned service.
- [x] Start the owned service before recovery installation, configure it with the existing governor, and use `max(1, min(shard_count, 4))` workers.
- [x] Reject replacing the execution service after open through an explicit status-returning configuration path or keep the existing pre-open setter contract internally enforced.
- [x] Add a direct-coordinator regression proving ordinary commits still work without an injected service.

### Task 3: Submit PREPARE and install work to the critical lane

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`

**Interfaces:**
- Produce: `Status RunCommitCriticalTasks(std::vector<std::function<Status()>> tasks)`.

- [x] Implement batch submission with `WorkTaskRequest{WorkClass::kCommitCritical, {}, true, 0}`.
- [x] Wait with `WorkExecutionService::WaitForTask` so calls originating on a scheduler worker can make nested progress without deadlock.
- [x] If submission fails partway, cancel queued handles, wait for every accepted handle, and return the original submission error.
- [x] Replace both direct thread vectors with task callbacks that write distinct result slots/references.
- [x] Remove the production `<thread>` include and verify no transaction source creates an OS thread.

### Task 4: Shared worker capacity, verification, and closure docs

**Files:**
- Modify: `src/db/cedar_database.cc`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: `DefaultResourceLimits().cpu_slots`.
- Produces: bounded shared service capacity sufficient for cross-shard critical parallelism.

- [x] Construct CedarDatabase's shared service with the configured CPU-slot limit instead of the fixed single worker.
- [x] Run PREPARE/install/fault/reopen/scheduler focused tests.
- [x] Run `cmake --build build-current -j1` and full normal `ctest --test-dir build-current -j1 --output-on-failure`.
- [x] Run tracked and untracked whitespace checks.
- [x] Update the completion matrix accurately: commit PREPARE/install bypass is complete, while point-read/recovery/shutdown typed admission and release artifacts remain open.

Verification checkpoint (2026-07-23): the binding API is status-returning and
pre-open-only, rejects null and replacement after both successful and failed
open, and all callers check the result. The focused scheduler/transaction set
passes 21/21; the complete normal matrix passes 808/808 in 41.99 seconds with
`-j1`; transaction production sources contain no `std::thread`; tracked and
touched-untracked whitespace checks pass.
