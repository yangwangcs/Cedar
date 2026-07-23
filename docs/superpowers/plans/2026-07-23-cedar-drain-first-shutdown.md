# Cedar Drain-First Shutdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved scheme C database shutdown protocol with typed admission rejection, safe lazy-query drain/cancellation, real `WorkClass::kShutdown` execution, durable checkpointing, and idempotent teardown.

**Architecture:** A shared database-lifecycle registry owns phase, operation counters, query cancellation entries, and the final close result. Every public operation acquires a typed lease; returned query streams are wrapped by a lifecycle tracker. `CedarDatabase::Close` quiesces admission, drains pre-existing writes, submits the actual shutdown body to the shared execution service, then stops workers and telemetry outside the worker callback.

**Tech Stack:** C++17, Cedar `Status`, `WorkExecutionService`, `QueryResultStream`, GoogleTest/CTest, CMake, ASAN/UBSAN/TSAN.

## Global Constraints

- Preserve the current dirty worktree; do not reset, clean, stage, commit, or push.
- Run all builds and tests with `-j1`.
- Keep the clean-break API and on-disk format number `1`.
- Do not restore external `V2`/`Vn` names, old runtimes, fallback paths, or old-layout readers.
- `kCancelQueries` is the default and destructor policy; `kDrainQueries` is explicit.
- The actual checkpointing shutdown body runs as `WorkClass::kShutdown`; worker-pool `Stop()` remains outside its callback.

---

### Task 1: Typed shutdown status and lifecycle state

**Files:**
- Create: `include/cedar/db/database_lifecycle.h`
- Create: `src/db/database_lifecycle.cc`
- Modify: `include/cedar/core/status.h`
- Modify: `src/core/status.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `Status::ShutdownInProgress`, `Status::IsShutdownInProgress`, `DatabasePhase`, `DatabaseOperationClass`, `DatabaseLifecycle::TryEnter`, `BeginClose`, `SetPhase`, `FinishClose`, and counter waits.

- [x] **Step 1: Write failing status and lifecycle admission tests**

Add tests that require the exact `ShutdownInProgress` predicate/string, prove
only one caller wins `BeginClose`, and prove admission succeeds in `RUNNING`
but fails after `QUIESCING`.

- [x] **Step 2: Run RED**

Run: `cmake --build build-current -j1 && ctest --test-dir build-current -R 'StatusTest.*Shutdown|DatabaseLifecycleTest' --output-on-failure -j1`

Expected: compile failure because the status and lifecycle interfaces do not exist.

- [x] **Step 3: Implement the minimal status and lifecycle primitives**

Use one mutex/condition-variable state. Return move-only RAII operation leases
that decrement the matching counter. Store the first close result and allow
concurrent close callers to wait for it.

- [x] **Step 4: Run GREEN**

Run the Task 1 command and require all selected tests to pass.

### Task 2: Query registry and outermost tracked stream

**Files:**
- Modify: `include/cedar/db/database_lifecycle.h`
- Modify: `src/db/database_lifecycle.cc`
- Modify: `include/cedar/tcypher/runtime/query_result.h`
- Modify: `src/tcypher/runtime/query_result.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: Task 1 shared lifecycle state.
- Produces: `DatabaseLifecycle::RegisterQuery`, cancel/drain waits, and `LifecycleTrackedResultStream`.

- [x] **Step 1: Write failing query-lifetime tests**

Cover idle cancellation, an executing `Next()` barrier, drain waiting until
terminal/release, and a retained stream returning `QueryCancelled` after its
database has been destroyed.

- [x] **Step 2: Run RED**

Run the named query-lifetime tests and confirm the current retained stream
still executes database-backed work instead of returning cancellation.

- [x] **Step 3: Implement query entries and the tracked stream**

The wrapper enters before delegating `Next()`, leaves afterward, marks terminal
on any non-OK status, and unregisters in its destructor. Cancel-close flips the
entry before waiting for active calls. Drain-close does not cancel.

- [x] **Step 4: Run GREEN**

Require all query-lifetime tests, including existing scheduled/spilling stream
tests with their updated scheme C expectations, to pass.

### Task 3: Public Close API and admission gates

**Files:**
- Modify: `include/cedar/db/cedar_database.h`
- Modify: `src/db/cedar_database.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: Task 1 operation leases and Task 2 query registration.
- Produces: `ClosePolicy`, `CedarDatabase::Close`, and lifecycle-gated public APIs.

- [x] **Step 1: Write failing close API tests**

Test default cancel policy, explicit drain policy, post-quiesce rejection for
writes/reads/queries/maintenance, and idempotent concurrent close status.

- [x] **Step 2: Run RED**

Build and run the named close tests; require failure because `Close` and
`ClosePolicy` are absent.

- [x] **Step 3: Implement public operation leases and query wrapping**

Acquire a write lease for schema/index/write APIs, point-read lease for `Get`,
query lease/entry for `ExecuteTcypher`, and maintenance lease for flush,
compaction, Blob maintenance, and checkpoint. Ensure all early returns release
their lease.

- [x] **Step 4: Implement idempotent `Close` and destructor cancel-close**

Quiesce once, wait pre-existing writes, submit the shutdown callback, stop the
service and telemetry, publish `CLOSED`, and return the stored first result.
The destructor calls `Close(kCancelQueries).IgnoreError()`.

- [x] **Step 5: Run GREEN**

Run all close and retained-stream tests and require zero failures.

- [x] **Step 6: Close retained-session and snapshot API gaps**

Make database-created sessions share lifecycle ownership, drain an already-
entered session commit before checkpoint, and return typed admission failures
from session creation, visible sequence, cache/storage snapshots, and benchmark
storage snapshots. Keep metrics and traces readable after close.

- [x] **Step 7: Enforce one-shot open and worker-close safety**

Reject a second/concurrent `Open()` before recovery is re-entered. Reject
blocking `Close()` from an execution-service worker without changing lifecycle.

### Task 4: Real typed shutdown body and maintenance ordering

**Files:**
- Modify: `include/cedar/runtime/work_execution_service.h`
- Modify: `src/runtime/work_execution_service.cc`
- Modify: `src/db/cedar_database.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `WorkClass::kShutdown`, lifecycle waits, coordinator flush/checkpoint.
- Produces: queued optional-work cancellation needed by close and exact shutdown accounting.

- [x] **Step 1: Write failing scheduler/order tests**

Assert one shutdown submission/admission/completion, worker-thread execution,
commit-before-checkpoint ordering, optional queued-work cancellation, callback
error propagation, and one-worker progress.

- [x] **Step 2: Run RED**

Run the named scheduler/order tests and confirm shutdown counters remain zero.

- [x] **Step 3: Implement the shutdown callback**

Transition through draining commits, query cancel/drain, maintenance drain, and
checkpointing. Cancel only optional query/maintenance classes; never cancel
commit-critical or recovery work. Preserve the first exact failure while still
attempting teardown.

- [x] **Step 4: Run GREEN**

Require the named shutdown/scheduler tests to pass without hangs.

- [x] **Step 5: Preserve essential urgent compaction**

Cancel queued normal compaction/index/statistics/Blob-GC work, but never cancel
queued `kCompactionUrgent`. Prove the distinction with a one-worker deterministic
queue regression.

### Task 5: Recovery, observability, and closure evidence

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: completed scheme C behavior.
- Produces: focused fault/reopen evidence and updated six-design matrix references.

- [x] **Step 1: Add checkpoint-failure/reopen and prepared-transaction tests**

Inject checkpoint failure, assert exact close status, construct a new database,
and verify recovery. Hold a prepared/commit-critical transaction across close
and prove completion is not cancelled or pressure-stalled.

- [x] **Step 2: Run focused regression**

Run: `cmake --build build-current -j1 && ctest --test-dir build-current -R 'Shutdown|Close|ScheduledQueryStream|SpillingQueryStream|Prepared' --output-on-failure -j1`

- [x] **Step 3: Run the full normal matrix**

Run: `ctest --test-dir build-current -j1 --output-on-failure`

- [x] **Step 4: Refresh sanitizer matrices**

Build and run `build-current-asan`, `build-current-ubsan`, and
`build-current-tsan` with `-j1`; accept evidence only when each final CTest
summary reports zero failed tests.

- [x] **Step 5: Update closure documents with exact evidence**

Replace the functional shutdown gap with code/test references and retain all
remaining benchmark/paper artifact gaps. Do not mark the six-design goal
complete unless every matrix gate is independently proven.
