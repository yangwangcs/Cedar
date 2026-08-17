# Cedar-Owned Bounded Async Executor Implementation Plan

> For agentic workers: use executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

Goal: Bound Cedar asynchronous commit submission independently of client thread count while preserving the existing single-WAL durable acceptance contract.

Architecture: Add a Cedar-owned FIFO mailbox with request and byte reservations and one fixed production submission worker. The worker hands tickets to the existing append pipeline; reservations are released only at the WAL-durable callback or a definite pre-durability failure. Remove the sampler watchdog and the asynchronous foreground blocking gate, then verify rejection, durability, shutdown, recovery, sustained load, and sanitizer behavior.

Tech stack: C++20, Cedar Database::Impl, RocksDB Cedar write/runtime seams, CMake, GoogleTest, ASAN, UBSAN, and TSAN.

## Global Constraints

- Preserve exactly one RocksDB WAL record and one durability synchronization per committed Cedar epoch.
- RocksDB owns WAL creation, synchronization, rotation, recovery, MemTable, VersionSet, MANIFEST, checkpoints, and backups.
- CommitAsync returns an accepted handle only after the existing WAL-durable callback.
- Keep kRuntimeSnapshotStaleUs = 250'000; stale snapshots fail closed.
- Do not add foreground GetIntProperty calls or GetSortedWalFiles calls.
- macOS Sync and Fsync remain F_FULLFSYNC; F_BARRIERFSYNC stays out of production.
- Production defaults are one submission worker, 32 mailbox requests, and 4 MiB mailbox bytes.
- Mailbox capacity is never waited for by a caller; a full bound returns ResourceExhausted immediately.
- Every ticket releases its reservation exactly once.
- Generic and lean profiles remain available for differential and rollback testing.

---

### Task 1: Add the Standalone Bounded Mailbox

Files:
- Create src/kernel/async_submission_executor.h
- Create src/kernel/async_submission_executor.cc
- Create tests/test_async_submission_executor.cc
- Modify tests/CMakeLists.txt and CMakeLists.txt

Interfaces:
- AsyncSubmissionExecutor::Options has worker_count, max_requests, and max_bytes.
- AsyncSubmissionExecutor::Ticket has id, estimated_bytes, handoff, fail, and release.
- Start starts exactly worker_count workers.
- TrySubmit(shared_ptr<Ticket>) never waits for capacity.
- Stop(Status) rejects pending tickets, joins workers, and releases reservations once.
- ReservedRequests and ReservedBytes include dequeued tickets waiting for durable acceptance.

- [ ] Step 1: Write failing tests for immediate request-bound rejection, byte-bound rejection, and idempotent Release. Use a handoff barrier so the first ticket remains reserved while the second call is measured.
- [ ] Step 2: Run cmake --build build-runtime-control --target test_async_submission_executor -j2 and ctest --test-dir build-runtime-control -R AsyncSubmissionExecutorTest --output-on-failure. Expected: missing symbols or target.
- [ ] Step 3: Implement one mutex, one condition variable, a FIFO deque, checked request/byte arithmetic, and monotonically increasing ticket IDs. TrySubmit checks stopping, zero bytes, request count, and byte overflow before enqueueing and never blocks.
- [ ] Step 4: Make workers invoke handoff. A non-OK handoff invokes fail and release. Release uses an atomic ticket bit and decrements counters once.
- [ ] Step 5: Make Stop mark stopping, wake workers, fail pending tickets with ShutdownInProgress, release them once, and join every worker.
- [ ] Step 6: Re-run the focused target and commit only the executor files:
    git add src/kernel/async_submission_executor.h src/kernel/async_submission_executor.cc tests/test_async_submission_executor.cc tests/CMakeLists.txt CMakeLists.txt
    git commit -m "feat: add Cedar bounded async mailbox"

### Task 2: Integrate Async Submission With Database::Impl

Files:
- Modify include/cedar/database.h
- Modify src/kernel/database_impl.h
- Modify src/kernel/database.cc
- Modify src/kernel/transaction.cc
- Modify src/kernel/async_commit.h
- Modify tests/test_kernel_commit.cc

Interfaces:
- Add AsyncExecutorOptions async_executor to DatabaseOptions.
- Add AsyncSubmissionExecutor async_executor to Database::Impl.
- Factor the existing append body into SubmitAsyncCommitToAppendPipeline.
- Add mailbox accepted/rejected counters, reserved request/byte gauges, and per-reason rejection counters to CommitPipelineMetrics.

- [ ] Step 1: Add a prewrite-barrier test with max_mailbox_requests = 1. Assert the second caller gets immediate ResourceExhausted, the append enqueue observer fires once, and no rejected handle is returned.
- [ ] Step 2: Add a durable-release test. Hold the WAL callback, assert the reservation remains occupied after handoff, release at the callback, and assert a later caller can then reserve capacity.
- [ ] Step 3: Run the new tests before implementation. Expected: options, metrics, and executor handoff do not compile.
- [ ] Step 4: Validate options at Database::Open. Kernel defaults are one worker, 32 requests, and 4 MiB. Reject zero values, worker counts above two, and mailbox bytes below group_commit_max_batch_bytes with InvalidArgument.
- [ ] Step 5: Move the old batch-size, shutdown, stale-snapshot, pressure, queue-limit, generation, metrics, and notification code into SubmitAsyncCommitToAppendPipeline without changing its ordering.
- [ ] Step 6: Build a ticket around AppendCommitRequest. Its handoff calls the factored method; fail completes CommitHandle::State::result and notifies the caller; release decrements the executor reservation. The caller waits only for wal_durable or a terminal result.
- [ ] Step 7: Extend WalDurabilityContext with ticket references. NotifyWalDurable marks wal_durable, notifies handles, and invokes idempotent release; it performs no RocksDB operation.
- [ ] Step 8: Run ctest --test-dir build-runtime-control -R 'AsyncExecutor|Mailbox|KernelAsyncCommitTest' --output-on-failure. Commit:
    git add include/cedar/database.h src/kernel/database_impl.h src/kernel/database.cc src/kernel/transaction.cc src/kernel/async_commit.h tests/test_kernel_commit.cc
    git commit -m "feat: route async commits through Cedar executor"

### Task 3: Remove Watchdog and Async Foreground Contention

Files:
- Modify src/kernel/database.cc
- Modify src/kernel/database_impl.h
- Modify include/cedar/database.h
- Modify tests/test_kernel_commit.cc

- [ ] Step 1: Add a test observer that counts runtime sampler worker starts and assert exactly one worker.
- [ ] Step 2: Run the observer test against the current code and capture the RED result caused by runtime_watchdog_worker.
- [ ] Step 3: Delete runtime_watchdog_worker start, stop, member, and refresh loop. Keep one sampler loop, the best-effort dedicated-thread QoS hint, and the exact stale bound. Retain runtime_refresh_mutex only if initial/final refresh can overlap.
- [ ] Step 4: Remove ForegroundAdmissionSlot from the asynchronous path. Synchronous commits retain their existing coordinator queue and deadline behavior.
- [ ] Step 5: Replace foreground-admission test assertions with mailbox reservation assertions.
- [ ] Step 6: Run ctest --test-dir build-runtime-control -R 'KernelRuntimeSamplerTest|KernelAdmissionControlTest|KernelAsyncCommitTest|AsyncExecutor' --output-on-failure and commit:
    git add src/kernel/database.cc src/kernel/database_impl.h include/cedar/database.h tests/test_kernel_commit.cc
    git commit -m "perf: remove async foreground and sampler watchdog contention"

### Task 4: Verify WAL Retention and All Terminal Paths

Files:
- Modify third_party/rocksdb/db/cedar_maintenance.cc
- Modify third_party/rocksdb/include/rocksdb/cedar_maintenance.h
- Modify third_party/rocksdb/db/db_impl/db_impl.h
- Modify src/fact/fact_store.cc
- Modify tests/test_fact_store.cc
- Modify tests/test_rocksdb_cedar_kernel.cc
- Modify tests/test_kernel_commit.cc

- [ ] Step 1: Write a batch larger than one WAL block, sample GetCedarRecoveryWalBytes, and assert retained bytes are non-zero and remain coherent until RocksDB purges the WAL. Use wals_total_size_; do not add GetSortedWalFiles.
- [ ] Step 2: Test definite prewrite failure, stale snapshot, hard pressure, close racing with queued tickets, and indeterminate write. Assert the documented status, exactly-once release, and no durable acceptance before the callback.
- [ ] Step 3: Audit that GetCedarRecoveryWalBytes uses wals_total_size_ and that executor callbacks never call RocksDB.
- [ ] Step 4: Run:
    cmake --build build-runtime-control --target test_fact_store test_rocksdb_cedar_kernel test_kernel_commit -j2
    ctest --test-dir build-runtime-control -R 'FactStore|CedarKernelWrite|AsyncExecutor|KernelAsyncCommit' --output-on-failure
- [ ] Step 5: Commit the focused WAL and failure tests.

### Task 5: Add Load Evidence and Operational Documentation

Files:
- Modify benchmarks/commit_workloads.cc
- Modify benchmarks/cedar_kernel_bench.cc
- Modify include/cedar/database.h
- Modify README.md
- Modify docs/superpowers/specs/2026-08-17-cedar-bounded-async-executor-design.md

- [ ] Step 1: Print mailbox bounds, peak reservations, accepted count, explicit overload rejection count/reasons, stale rejections, WAL-sync p99, and reopen verification. Do not silently retry rejected CommitAsync calls.
- [ ] Step 2: Add bounded-client mode capped at mailbox capacity while preserving the 512-client overload mode. Report the two modes separately.
- [ ] Step 3: Run the 60-second 512-client command and a bounded-client command. Record operations, accepted/rejected counts, throughput, WAL-sync p99, max refresh duration, maximum completion gap, peak reservations, errors, and reopen_verified.
- [ ] Step 4: Document immediate overload rejection and the fact that mailbox capacity is volatile, not a durable queue. Preserve one-WAL and stale-snapshot constraints.
- [ ] Step 5: Commit benchmark and documentation changes.

### Task 6: Full Verification and Qualification

Files:
- Modify docs/superpowers/plans/2026-08-17-cedar-bounded-async-executor.md
- Create docs/superpowers/evidence/2026-08-17-cedar-bounded-async-executor.md

- [ ] Step 1: Build and run the focused suite, then ctest --test-dir build-runtime-control --output-on-failure. Record exact counts and pre-existing failures separately.
- [ ] Step 2: Configure ASAN/UBSAN with -fsanitize=address,undefined -fno-omit-frame-pointer. Run mailbox, recovery, and 60-second bounded-client tests; record commands and results.
- [ ] Step 3: Configure TSAN and run mailbox saturation, durable release, close race, and the short 512-client load. Fail on data races or leaked workers.
- [ ] Step 4: Run 300-second 512-client and bounded-client campaigns. Record throughput, accepted/rejected counts, WAL-sync p50/p99/max, refresh duration, completion gap, mailbox occupancy, errors, and reopen verification.
- [ ] Step 5: Write the qualification record with hardware, filesystem/device placement, exact RocksDB WAL options, Cedar bounds, worker counts, sanitizer results, crash/reopen result, and residual risk.
- [ ] Step 6: Mark completed plan checkboxes and commit the evidence.

## Self-Review

- Spec coverage: Tasks 1-3 implement bounded admission and remove known contention experiments; Task 4 covers WAL retention and terminal release; Task 5 covers observable load behavior; Task 6 covers correctness and sanitizer evidence.
- Placeholder scan: no TBD, TODO, or unspecified test command is used.
- Type consistency: ticket fields are id, estimated_bytes, handoff, fail, and release in Tasks 1, 2, and 4; options are submission_workers, max_mailbox_requests, and max_mailbox_bytes throughout.
- Scope: the plan changes async admission and evidence only. It does not introduce a second log, alter RocksDB ownership, or change synchronous commit semantics.

