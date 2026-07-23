# Cedar HTAP Parallel Participant Installation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one durable cross-shard commit install all participant MemTable fragments concurrently while preserving prepare-reference validation, idempotent recovery, and continuous visible-prefix publication.

**Architecture:** `InstallDecision` first reads and validates every referenced PREPARE fragment without publishing any event. It then launches one worker per validated participant shard, collects every installation status, and advances `VisiblePrefix` only after all workers succeed. A test-only participant hook makes actual overlap and failure schedules deterministic.

**Tech Stack:** C++17, `std::thread`, GoogleTest, Cedar `DecisionLog`, `StorageShard`, `VisiblePrefix`.

## Global Constraints

- Preserve the approved `LogicalKey`, immutable `TemporalEvent`, version-chain MemTable, and bitemporal semantics.
- Keep DecisionLog append order and `commit_seq` allocation under `commit_mutex_`.
- Never advance `visible_seq` after a partial or failed participant installation.
- After a durable decision, any participant installation failure returns `Indeterminate` and puts the coordinator into recovery-required state.
- Preserve all existing workspace changes; do not stage, commit, push, reset, clean, or roll back files.

---

### Task 1: Deterministic participant-install overlap test

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `SetParticipantInstallHookForTesting(std::function<void(uint64_t, uint32_t)>)`.
- Consumes: `CommitWithResult`, `ShardDirectory`, and a cross-shard transaction containing one event per shard.

- [x] **Step 1: Write the failing test**

Add `DurableLogTest.OneCrossShardDecisionInstallsParticipantsConcurrently`. The hook increments an arrival counter and makes the first participant wait up to two seconds for the second participant. Submit event input in reverse shard order and require both participants to overlap and the transaction to commit.

- [x] **Step 2: Run the test and observe RED**

Run:

```bash
cmake --build build-v2 -j2 --target test_correctness_kernel
```

Expected: compilation fails because `SetParticipantInstallHookForTesting` does not exist.

- [x] **Step 3: Add the hook without parallelizing installation**

Store the hook under `commit_mutex_`, copy it inside `CommitInternal`, and invoke it immediately before each participant calls `StorageShard::InstallCommitted`.

- [x] **Step 4: Run the deterministic test and observe behavioral RED**

Run:

```bash
build-v2/tests/test_correctness_kernel --gtest_filter=DurableLogTest.OneCrossShardDecisionInstallsParticipantsConcurrently
```

Expected: the first hook times out because `InstallDecision` still processes participants serially.

### Task 2: Two-phase parallel participant installation

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Changes `InstallDecision` to accept the copied participant hook.
- Produces no new production-facing API.

- [x] **Step 1: Validate all PREPARE references before installation**

Build a vector containing `(PrepareReference, PrepareRecord)` after checking shard range, LSN/checksum lookup, transaction identity, and duplicate participant shard identity. Return corruption before launching workers when any reference is invalid.

- [x] **Step 2: Install validated participants concurrently**

Create one status slot and one worker per validated participant. Each worker invokes the hook, installs its shard events when not already covered by `published_commit_watermarks_`, and refreshes MemTable Blob references. Join every created worker. If thread creation or any worker fails, return the error without marking the commit installed.

- [x] **Step 3: Advance the visible prefix only after all participants succeed**

Call:

```cpp
visible_prefix_.MarkInstalled(decision.commit_seq);
```

only after every participant status is OK.

- [x] **Step 4: Run the concurrency and recovery subset**

Run:

```bash
build-v2/tests/test_correctness_kernel --gtest_filter='DurableLogTest.OneCrossShardDecisionInstallsParticipantsConcurrently:DurableLogTest.EarlierInstallFailureWakesAheadOfPrefixCommitAsIndeterminate:DurableLogTest.CoordinatorCommitsAcrossShardsAndRecoversVisibleState:StorageShardTest.InstallsCommittedEventsIdempotently'
```

Expected: all tests pass.

- [x] **Step 5: Verify partial participant failure recovery**

Add `SetParticipantInstallFaultInjectorForTesting(std::function<Status(uint64_t, uint32_t)>)` and `DurableLogTest.PartialParticipantInstallFailureReplaysEveryShardOnReopen`. Inject an error for one participant before its MemTable install while allowing the other worker to finish. Require `Indeterminate`, `recovery_required`, and `visible_seq == 0`; after reopen require both shard values and transaction outcome sequence 1 to be present.

### Task 3: Regression and sanitizer evidence

**Files:**
- Modify only if verification exposes a defect.

- [x] **Step 1: Run ordinary full regression**

```bash
cmake --build build-v2 -j2 --target test_correctness_kernel
build-v2/tests/test_correctness_kernel
```

- [x] **Step 2: Run sanitizer regressions**

```bash
cmake --build build-asan -j2 --target test_correctness_kernel
build-asan/tests/test_correctness_kernel
cmake --build build-ubsan -j2 --target test_correctness_kernel
build-ubsan/tests/test_correctness_kernel
cmake --build build-tsan -j2 --target test_correctness_kernel
build-tsan/tests/test_correctness_kernel
```

- [x] **Step 3: Check patch integrity**

```bash
git diff --check
```

Expected: ordinary, ASAN, UBSAN, and TSAN suites all pass; patch integrity reports no whitespace errors.
