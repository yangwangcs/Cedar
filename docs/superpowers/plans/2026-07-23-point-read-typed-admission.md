# Point-Read Typed Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route every public synchronous Cedar point read through the shared `WorkExecutionService` as `WorkClass::kPointRead` with a bounded CPU grant, while preserving exact read, cache, I/O, status, metric, and trace semantics.

**Architecture:** `CedarDatabase::Get()` submits one synchronous point-read callback and waits with `WorkExecutionService::WaitForTask`, which also supports calls made from a scheduler worker. `TransactionCoordinator::GetChecked()` remains the internal unscheduled read primitive so T-Cypher property gathers and transaction internals do not create one scheduler task per property lookup. The service-owned `ResourceLease` holds one CPU slot until the callback finishes; existing `IoGovernor` and cache accounting remain the physical I/O and memory authorities.

**Tech Stack:** C++17, Cedar `WorkExecutionService`, `WorkScheduler`, `ResourceGovernor`, `IoGovernor`, GoogleTest.

## Global Constraints

- Preserve database format version `1` and all current clean-break layouts.
- Do not add old runtime/fallback readers or external V2/Vn names.
- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Build and test with `-j1`.
- Do not schedule internal `TransactionCoordinator::GetChecked()` calls individually.

---

### Task 1: RED public point-read queue contract

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `CedarDatabase::Get`, scheduler metrics exported by `CedarDatabase::metrics()`.
- Produces: regression proof that one successful public read submits, admits, and completes exactly one `point_read` task.

- [x] Add `DurableLogTest.DatabasePointReadUsesTypedScheduler` that opens a database, writes one typed property, records the three `point_read` scheduler counters, calls `Get`, verifies the exact value, and requires each counter to increase by one.
- [x] Run the focused test and confirm RED because the current `CedarDatabase::Get()` calls `GetChecked()` directly and leaves all three counters unchanged.

### Task 2: Synchronous typed submission

**Files:**
- Modify: `src/db/cedar_database.cc`

**Interfaces:**
- Consumes: `WorkTaskRequest`, `WorkClass::kPointRead`, `ResourceProfile`, `WorkExecutionService::WaitForTask`.
- Produces: one public point read admitted with `ResourceProfile{0, 0, 0, 0, 1}` and no direct-execution fallback.

- [x] Initialize the callback result with a deterministic failure sentinel.
- [x] Submit `WorkTaskRequest{WorkClass::kPointRead, ResourceProfile{0, 0, 0, 0, 1}, false, 0}` and execute `coordinator_.GetChecked(key, valid_time, snapshot_seq)` only inside the callback.
- [x] If submission fails, use that typed status as the operation result; otherwise wait through `WaitForTask` and return callback data only after successful completion.
- [x] Preserve the existing latency histogram, success/failure counters, and point-read trace around the complete queue-plus-execution latency.

### Task 3: Nested progress, regression, and closure evidence

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: existing `WorkExecutionService::WaitForTask` nested-worker contract.
- Produces: normal-suite and documentation evidence for public point-read typed admission.

- [x] Add or extend a regression proving a scheduler worker can synchronously invoke a public point read without deadlock.
- [x] Run point-read, cache, selective-SST-I/O, WorkExecution, and scheduler metric focused tests.
- [x] Run `cmake --build build-current -j1` and full normal `ctest --test-dir build-current -j1 --output-on-failure`.
- [x] Run tracked and touched-untracked whitespace checks.
- [x] Update the completion matrix accurately: public point-read typed admission is complete; recovery, drain-first shutdown, executing-maintenance cancellation, sanitizer refresh, and release artifacts remain open.

Verification checkpoint (2026-07-23): the public point-read typed counter test
failed with submitted/admitted/completed all remaining zero, then passed after
the shared-service submission was implemented. A forced three-worker blocker
test proves a fourth scheduler worker can call public `Get()` and make nested
progress. The change exposed and fixed a ResourceGovernor reserve-accounting
bug: critical usage and noncritical pool usage are now tracked separately, so
an occupied critical reserve is not subtracted twice from shared capacity.
The resource-pool unit regression, HTAP balanced workload, focused point-read/
scheduler tests, and full normal matrix pass; full normal is 812/812 in 42.18
seconds with `-j1`.
