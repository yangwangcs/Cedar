# Cedar Atomic Commit Stage B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add CAC FIFO commit generations/group fsync, temporal OCC and strict reservations without transaction-wide `commit_mutex_`, parallel idempotent installation, and continuous `VisiblePrefix` waiting.

**Architecture:** Validated transactions atomically acquire canonical shard reservations, then enqueue immutable requests into one bounded FIFO `CommitQueue`. A single queue writer assigns sequence/HLC values and performs one atomic-log sync per generation; confirmed durable records are published to immutable outcome/timeline snapshots, installed in parallel, and acknowledged as committed only after their own sequence enters `VisiblePrefix`.

**Tech Stack:** C++17, CMake, GoogleTest, `std::mutex`, `std::condition_variable`, immutable `std::shared_ptr<const Snapshot>`, Cedar `WorkExecutionService`.

## Global Constraints

- This plan consumes Phase A’s `AtomicCommitRecord` codec and single-request `AtomicCommitLog` append/recovery implementation.
- Durable truth and public result are distinct: confirmed durable + install/prefix failure returns `kIndeterminate`, but `Resolve(txn_id)` reports the durable commit after reopen.
- `kCommitted` is returned only after the transaction’s own `commit_seq` is visible.
- Snapshot mode validates only overlapping temporal write intervals; it does not validate ordinary reads.
- Strict reads must originate from Cedar capture APIs and validate observed event plus predecessor/successor fences, including empty reads.
- Reservation order is exactly `shard_id, logical_key, valid_from, reservation_kind`.
- Atomic records use ascending `shard_id`, then `logical_key, valid_from`.
- No caller may retain references to mutable timeline or outcome containers.
- Do not delete the legacy PREPARE/Decision implementation in Stage B; clean-break deletion belongs to the final CAC switch task.

---

## File Structure

- Create `include/cedar/transaction/commit_queue.h`
- Create `src/transaction/commit_queue.cc`
- Create `include/cedar/transaction/transaction_validator.h`
- Create `src/transaction/transaction_validator.cc`
- Create `include/cedar/transaction/commit_installer.h`
- Create `src/transaction/commit_installer.cc`
- Create `tests/test_commit_queue.cc`
- Create `tests/test_transaction_validator.cc`
- Create `tests/test_commit_installer.cc`
- Modify `include/cedar/transaction/atomic_commit_log.h`
- Modify `src/transaction/atomic_commit_log.cc`
- Modify `include/cedar/transaction/commit_timeline.h`
- Modify `src/transaction/commit_timeline.cc`
- Modify `include/cedar/transaction/visible_prefix.h`
- Modify `src/transaction/visible_prefix.cc`
- Modify `include/cedar/storage/storage_shard.h`
- Modify `src/storage/storage_shard.cc`
- Modify `include/cedar/transaction/transaction_coordinator.h`
- Modify `src/transaction/transaction_coordinator.cc`
- Modify `CMakeLists.txt`
- Modify `tests/CMakeLists.txt`

---

### Task 1: Publish Thread-Safe Durable Outcomes and Commit Timeline

**Files:**

- Modify: `include/cedar/transaction/commit_timeline.h`
- Modify: `src/transaction/commit_timeline.cc`
- Modify: `include/cedar/transaction/atomic_commit_log.h`
- Modify: `src/transaction/atomic_commit_log.cc`
- Test: `tests/test_commit_queue.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

- Produces:

```cpp
struct CommitTimelineSnapshot {
  std::vector<CommitTimelineEntry> entries;
};

class CommitTimeline {
 public:
  std::shared_ptr<const CommitTimelineSnapshot> Snapshot() const;
  Status PublishDurableGeneration(
      const std::vector<CommitTimelineEntry>& entries);
  Status AllocateAfterSnapshot(
      const CommitTimelineSnapshot& base, uint64_t wall_clock_us,
      SystemHlc* result) const;
  StatusOr<uint64_t> ResolveAsOf(
      const std::shared_ptr<const CommitTimelineSnapshot>& snapshot,
      uint64_t timestamp_us, uint64_t visible_seq_ceiling) const;
};

enum class AtomicAppendCertainty : uint8_t {
  kDefinitelyAbsent,
  kIndeterminate,
  kDefinitelyDurable,
};

struct AtomicCommitAppendResult {
  Status status;
  AtomicAppendCertainty certainty;
  bool requires_reopen = false;
  bool sync_attempted = false;
  uint64_t sync_latency_ns = 0;
  std::vector<std::shared_ptr<const AtomicCommitRecord>> durable_records;
};
```

- [ ] **Step 1: Write failing concurrent snapshot test**

```cpp
TEST(CommitTimelineTest, ReadersPinImmutableSnapshotsDuringPublication) {
  CommitTimeline timeline(path);
  ASSERT_TRUE(timeline.Open().ok());
  auto before = timeline.Snapshot();

  ASSERT_TRUE(timeline.PublishDurableGeneration({
      CommitTimelineEntry{1, SystemHlc{100, 0}},
  }).ok());

  EXPECT_TRUE(before->entries.empty());
  auto after = timeline.Snapshot();
  ASSERT_EQ(after->entries.size(), 1U);
  EXPECT_EQ(after->entries[0].commit_seq, 1U);
}
```

Add a second test with one publishing thread and four readers repeatedly calling `Snapshot()` and `ResolveAsOf()`.

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
cmake -S . -B build -DBUILD_TESTS=ON
cmake --build build --target test_commit_queue -j
build/tests/test_commit_queue \
  --gtest_filter='CommitTimelineTest.*'
```

Expected: compilation failure because immutable snapshot APIs do not exist.

- [ ] **Step 3: Implement immutable publication**

Store:

```cpp
mutable std::mutex mutex_;
std::shared_ptr<const CommitTimelineSnapshot> snapshot_;
```

Build a complete new snapshot under `mutex_`, validate contiguous sequence and strictly increasing HLC, then publish with `std::atomic_store(&snapshot_, next)`.

Remove or privatize APIs returning `const std::vector<CommitTimelineEntry>&`.

- [ ] **Step 4: Add append-certainty tests**

Test these exact mappings:

- failure before writing any frame → `kDefinitelyAbsent`;
- partial/full write, sync failure, or injected post-sync ambiguity → `kIndeterminate`;
- completed sync and required new-segment directory sync → `kDefinitelyDurable`.

- [ ] **Step 5: Run focused tests**

Expected: all `CommitTimelineTest.*` and `AtomicCommitLogCertaintyTest.*` pass under repeated execution.

- [ ] **Step 6: Commit**

```bash
git add include/cedar/transaction/commit_timeline.h \
  src/transaction/commit_timeline.cc \
  include/cedar/transaction/atomic_commit_log.h \
  src/transaction/atomic_commit_log.cc \
  tests/test_commit_queue.cc tests/CMakeLists.txt
git commit -m "feat: publish immutable CAC durable state"
```

---

### Task 2: Add FIFO Commit Queue and Bounded Generations

**Files:**

- Create: `include/cedar/transaction/commit_queue.h`
- Create: `src/transaction/commit_queue.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_commit_queue.cc`

**Interfaces:**

```cpp
struct CommitQueueLimits {
  size_t maximum_records = 64;
  uint64_t maximum_bytes = 4U << 20;
  size_t maximum_queued_records = 4096;
  uint64_t maximum_queued_bytes = 64U << 20;
};

struct CommitRequestResult {
  CommitOutcome public_outcome = CommitOutcome::kAborted;
  AtomicAppendCertainty durable_truth =
      AtomicAppendCertainty::kDefinitelyAbsent;
  uint64_t txn_id = 0;
  uint64_t commit_seq = 0;
  Status status = Status::OK();
};

class CommitQueue {
 public:
  StatusOr<std::shared_ptr<CommitRequest>> Enqueue(
      std::shared_ptr<const AtomicCommitIntent> intent);
  CommitRequestResult Wait(const std::shared_ptr<CommitRequest>& request);
  Status StopAdmission();
  Status Drain();
};
```

- [ ] **Step 1: Write failing FIFO/group tests**

Add tests:

```cpp
TEST(CommitQueueTest, OneLeaderFsyncsReadyFollowersAsOneGeneration);
TEST(CommitQueueTest, AllocatesSequenceAndHlcInFifoOrder);
TEST(CommitQueueTest, ClosesGenerationAtRecordLimit);
TEST(CommitQueueTest, ClosesGenerationAtByteLimit);
TEST(CommitQueueTest, LateArrivalJoinsNextGeneration);
TEST(CommitQueueTest, RejectsAdmissionWhenQueueBoundsAreExceeded);
```

Use a fake atomic-log sink recording generation membership, write order, and sync count.

- [ ] **Step 2: Verify tests fail**

Run:

```bash
cmake --build build --target test_commit_queue -j
build/tests/test_commit_queue \
  --gtest_filter='CommitQueueTest.*'
```

Expected: compilation failure because `CommitQueue` is absent.

- [ ] **Step 3: Implement request state**

Use a per-request mutex/CV and explicit states:

```cpp
enum class CommitRequestPhase : uint8_t {
  kQueued,
  kWriting,
  kDurable,
  kIndeterminate,
  kDefinitelyAbsent,
};
```

Only the queue leader may assign tentative sequence/HLC values. Publish them to requests and timeline only after `kDefinitelyDurable`.

- [ ] **Step 4: Implement bounded generation closure**

Leader behavior:

1. take the FIFO head;
2. merge already queued followers until record/byte limit;
3. if no follower is ready, call one injectable scheduler-yield hook;
4. close membership;
5. append the immutable generation once;
6. publish one terminal durable truth to every member.

- [ ] **Step 5: Test ambiguity fan-out**

Add:

```cpp
TEST(CommitQueueTest, AmbiguousGenerationStopsLaterSequences);
```

Verify all possibly covered members become `kIndeterminate`, future enqueue fails with `RecoveryRequired`, and no later sequence is published.

- [ ] **Step 6: Run tests**

Expected: all queue tests pass; fake sink reports one sync for grouped requests.

- [ ] **Step 7: Commit**

```bash
git add include/cedar/transaction/commit_queue.h \
  src/transaction/commit_queue.cc \
  tests/test_commit_queue.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add bounded CAC group commit queue"
```

---

### Task 3: Extract Canonical Temporal OCC and Reservation Acquisition

**Files:**

- Create: `include/cedar/transaction/transaction_validator.h`
- Create: `src/transaction/transaction_validator.cc`
- Modify: `include/cedar/storage/storage_shard.h`
- Modify: `src/storage/storage_shard.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_transaction_validator.cc`

**Interfaces:**

```cpp
enum class ReservationKind : uint8_t {
  kRead = 0,
  kWrite = 1,
};

struct ReservationIntent {
  uint32_t shard_id;
  LogicalKey logical_key;
  uint64_t valid_from;
  uint64_t valid_to;
  ReservationKind kind;
};

enum class ReservationPhase : uint8_t {
  kValidating,
  kQueued,
  kDurablePendingInstall,
  kReleased,
};

class ReservationHandle {
 public:
  Status MarkQueued();
  Status MarkDurablePendingInstall();
  Status ReleaseDefinitelyAbsent();
  Status ReleaseInstalled();
};

class TransactionValidator {
 public:
  StatusOr<ReservationHandle> ValidateAndReserve(
      uint64_t txn_id,
      uint64_t snapshot_seq,
      TransactionMode mode,
      const std::vector<PendingEvent>& canonical_events,
      const std::vector<StrictReadIdentity>& strict_reads);
};
```

- [ ] **Step 1: Write failing snapshot interval matrix**

Cover:

- same key overlapping interval → conflict;
- touching half-open boundary → allowed;
- same key disjoint interval → allowed concurrently;
- different key and different shard → allowed concurrently;
- ordinary snapshot read changes → ignored.

- [ ] **Step 2: Write canonical-order test**

Pass events and reads in reverse order and assert hook output equals:

```text
(shard 0, key A, time 10, read)
(shard 0, key A, time 20, write)
(shard 1, key B, time 5, write)
```

- [ ] **Step 3: Verify failure**

Run:

```bash
cmake --build build --target test_transaction_validator -j
build/tests/test_transaction_validator \
  --gtest_filter='TransactionValidatorSnapshotTest.*:TransactionValidatorOrderTest.*'
```

Expected: missing validator/canonical reservation APIs.

- [ ] **Step 4: Implement canonicalization**

Sort once with:

```cpp
return std::tie(left.shard_id, left.logical_key, left.valid_from, left.kind) <
       std::tie(right.shard_id, right.logical_key, right.valid_from, right.kind);
```

Reject duplicate event identity before acquiring any shard lock. Acquire shard validation locks in ascending shard order. Validate all shards before inserting any reservation.

- [ ] **Step 5: Implement phase-safe release**

`ReleaseDefinitelyAbsent()` must reject `kDurablePendingInstall`. `ReleaseInstalled()` is the only normal release from durable state.

- [ ] **Step 6: Run focused tests repeatedly**

```bash
for i in {1..100}; do
  build/tests/test_transaction_validator \
    --gtest_filter='TransactionValidatorSnapshotTest.*:TransactionValidatorOrderTest.*' \
    || exit 1
done
```

Expected: 100 clean passes and no deadlock.

- [ ] **Step 7: Commit**

```bash
git add include/cedar/transaction/transaction_validator.h \
  src/transaction/transaction_validator.cc \
  include/cedar/storage/storage_shard.h \
  src/storage/storage_shard.cc \
  tests/test_transaction_validator.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add canonical temporal OCC reservations"
```

---

### Task 4: Make Strict Read Identity Opaque and Fence-Complete

**Files:**

- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `include/cedar/transaction/transaction_validator.h`
- Modify: `src/transaction/transaction_validator.cc`
- Test: `tests/test_transaction_validator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**

```cpp
class StrictReadIdentity {
 public:
  const LogicalKey& logical_key() const;
  uint64_t valid_time() const;

 private:
  friend class TransactionCoordinator;
  StrictReadIdentity(
      LogicalKey key, uint64_t valid_time, uint64_t snapshot_seq,
      std::optional<TemporalEvent> observed_event,
      std::optional<uint64_t> predecessor_fence,
      std::optional<uint64_t> successor_fence,
      uint64_t capture_nonce);
};
```

Remove the public identity-free `StrictReadPoint(LogicalKey, uint64_t)` constructor.

- [ ] **Step 1: Write failing fence tests**

Add:

```cpp
TEST_F(DurableLogTest, StrictEmptyReadRejectsNewSuccessorFence);
TEST_F(DurableLogTest, StrictReadRejectsChangedPredecessorFenceWithSamePointValue);
TEST_F(DurableLogTest, StrictReadRejectsChangedSuccessorFenceWithSameObservedEvent);
```

Each test captures at snapshot `S`, commits a boundary change after `S`, then attempts an unrelated strict write using the captured identity.

- [ ] **Step 2: Add compile-time construction test**

```cpp
static_assert(!std::is_constructible_v<
    TransactionCoordinator::StrictReadIdentity,
    LogicalKey, uint64_t>);
```

- [ ] **Step 3: Verify current failure**

Expected: successor/predecessor fence tests incorrectly commit, or opaque type does not compile.

- [ ] **Step 4: Validate complete current identity**

At validation time rebuild the full current point:

```text
observed_event
predecessor_fence
successor_fence
```

Compare all three to the captured snapshot identity. Empty reads with all optional fields empty remain valid captured identities because capture validity is represented by the private token, not by optional-field presence.

- [ ] **Step 5: Run tests**

```bash
build/tests/test_transaction_validator \
  --gtest_filter='TransactionValidatorStrictTest.*'
build/tests/test_correctness_kernel \
  --gtest_filter='DurableLogTest.Strict*'
```

Expected: all strict identity, fence, cycle, and reservation tests pass.

- [ ] **Step 6: Commit**

```bash
git add include/cedar/transaction/transaction_coordinator.h \
  src/transaction/transaction_coordinator.cc \
  include/cedar/transaction/transaction_validator.h \
  src/transaction/transaction_validator.cc \
  tests/test_transaction_validator.cc tests/test_correctness_kernel.cc
git commit -m "fix: validate complete strict temporal identity"
```

---

### Task 5: Add Parallel Idempotent Commit Installer

**Files:**

- Create: `include/cedar/transaction/commit_installer.h`
- Create: `src/transaction/commit_installer.cc`
- Modify: `include/cedar/storage/storage_shard.h`
- Modify: `src/storage/storage_shard.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_commit_installer.cc`

**Interfaces:**

```cpp
struct InstallResult {
  uint64_t txn_id;
  uint64_t commit_seq;
  Status status;
};

class CommitInstaller {
 public:
  Status Schedule(
      std::shared_ptr<const AtomicCommitRecord> record,
      ReservationHandle reservation);
  Status WaitUntilInstalled(uint64_t txn_id);
  Status Drain();
};
```

- [ ] **Step 1: Write failing parallel-install tests**

Add:

```cpp
TEST(CommitInstallerTest, DifferentShardBatchesInstallConcurrently);
TEST(CommitInstallerTest, MarksTransactionInstalledAfterEveryParticipant);
TEST(CommitInstallerTest, ParticipantFailureCannotExposePartialTransaction);
```

Use per-shard hooks and a two-worker `WorkExecutionService`.

- [ ] **Step 2: Write idempotency tests**

Cover:

- exact replay inserts one event;
- retry after one participant completed finishes remaining participants;
- same `(txn_id, event ordinal)` with different commit sequence or content returns corruption;
- Blob reference and index-delta side effects occur once.

- [ ] **Step 3: Verify failure**

Run:

```bash
cmake --build build --target test_commit_installer -j
build/tests/test_commit_installer
```

Expected: installer type or completion tracking absent.

- [ ] **Step 4: Implement explicit completion state**

Maintain:

```cpp
struct TransactionInstallState {
  size_t remaining_participants;
  bool failed;
  Status first_failure;
};
```

Schedule shard batches independently. Call the transaction-complete callback exactly once when `remaining_participants` reaches zero and no participant failed.

- [ ] **Step 5: Preserve canonical event ordinals**

Require each shard batch to arrive in canonical `logical_key, valid_from` order. Use canonical event ordinal for the idempotency slot, never caller input order.

- [ ] **Step 6: Run tests**

Expected: participant overlap observed, partial failure remains invisible, and exact replay is side-effect free.

- [ ] **Step 7: Commit**

```bash
git add include/cedar/transaction/commit_installer.h \
  src/transaction/commit_installer.cc \
  include/cedar/storage/storage_shard.h \
  src/storage/storage_shard.cc \
  tests/test_commit_installer.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add parallel idempotent CAC installation"
```

---

### Task 6: Move Waiting into VisiblePrefix and Wait for Own Sequence

**Files:**

- Modify: `include/cedar/transaction/visible_prefix.h`
- Modify: `src/transaction/visible_prefix.cc`
- Test: `tests/test_commit_installer.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**

```cpp
class VisiblePrefix {
 public:
  void RestorePersistedPrefix(uint64_t commit_seq);
  void MarkInstalled(uint64_t commit_seq);
  Status WaitUntilVisible(
      uint64_t commit_seq,
      const std::function<bool()>& stop_waiting);
  uint64_t visible_seq() const;

 private:
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  uint64_t visible_seq_ = 0;
  std::set<uint64_t> installed_ahead_;
};
```

- [ ] **Step 1: Write failing waiter tests**

Add:

```cpp
TEST(VisiblePrefixTest, OutOfOrderCompletionAdvancesOnlyContinuousPrefix);
TEST(VisiblePrefixTest, EachWaiterWaitsForItsOwnSequence);
TEST(VisiblePrefixTest, InstallingTwoDoesNotWakeOneAsVisible);
TEST(VisiblePrefixTest, StopPredicateTerminatesBlockedWaiter);
```

- [ ] **Step 2: Verify failure**

Expected: `WaitUntilVisible` absent and coordinator still owns a separate installation CV.

- [ ] **Step 3: Implement integrated notification**

Under one mutex:

1. insert completed sequence;
2. advance only while `visible_seq + 1` is present;
3. notify waiters only if `visible_seq` advanced.

Use predicate:

```cpp
visible_seq_ >= commit_seq || stop_waiting()
```

- [ ] **Step 4: Remove coordinator’s `installation_wait_mutex_` and `installation_cv_`**

Replace its wait loop with `visible_prefix_.WaitUntilVisible(assigned_seq, ...)`.

- [ ] **Step 5: Run tests**

```bash
build/tests/test_commit_installer \
  --gtest_filter='VisiblePrefixTest.*:CommitInstallerTest.*'
build/tests/test_correctness_kernel \
  --gtest_filter='VisiblePrefixTest.*:DurableLogTest.LaterDecisionCanInstallAheadButWaitsForTheVisiblePrefix'
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add include/cedar/transaction/visible_prefix.h \
  src/transaction/visible_prefix.cc \
  include/cedar/transaction/transaction_coordinator.h \
  src/transaction/transaction_coordinator.cc \
  tests/test_commit_installer.cc tests/test_correctness_kernel.cc
git commit -m "feat: wait on continuous CAC visible prefix"
```

---

### Task 7: Connect Coordinator and Remove Transaction-Body `commit_mutex_`

**Files:**

- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**

Coordinator pipeline:

```text
Validate/schema/blob/admission
  -> TransactionValidator::ValidateAndReserve
  -> CommitQueue::Enqueue
  -> wait durable truth
  -> ReservationHandle::MarkDurablePendingInstall
  -> CommitInstaller::Schedule
  -> VisiblePrefix::WaitUntilVisible
  -> public CommitResult
```

- [ ] **Step 1: Add disjoint transaction overlap test**

Block the first transaction after reservation installation and verify a second transaction on another shard reaches queue admission without waiting.

- [ ] **Step 2: Add durable-truth/public-result fault matrix**

Required expectations:

| Failure boundary | Durable truth | Public result |
|---|---:|---:|
| definite pre-write failure | absent | aborted |
| partial/full write ambiguity | unknown | indeterminate |
| sync failure | unknown | indeterminate |
| confirmed sync, successful install/prefix | durable | committed |
| confirmed sync, participant install failure | durable | indeterminate |
| confirmed sync, predecessor prefix failure | durable | indeterminate |

After reopen, every confirmed durable transaction must resolve committed and every definitely absent transaction must resolve absent.

- [ ] **Step 3: Verify tests fail against old pipeline**

Expected: tests expose old PREPARE/Decision sequencing, mutable timeline access, or incorrect reservation release.

- [ ] **Step 4: Replace `CommitInternal` body**

Do not retain `commit_mutex_` across admission, validation, Blob work, queue wait, install, or visibility wait.

Keep separate short locks for:

```cpp
lifecycle_mutex_
checkpoint_mutex_
hook_mutex_
blob_mutation_mutex_
```

Queue ordering remains internal to `CommitQueue`.

- [ ] **Step 5: Remove shared-container inference**

Delete uses equivalent to:

```cpp
decision_log_.commits().back()
commit_timeline_.entries()
```

The queue result must carry the exact immutable durable record belonging to the request.

- [ ] **Step 6: Protect hooks and lifecycle state**

Copy testing hooks under `hook_mutex_` and invoke them after unlocking it. Make `opened_` lifecycle-protected or atomic. Do not invoke user hooks while holding queue, shard, timeline, or lifecycle locks.

- [ ] **Step 7: Run focused coordinator tests**

```bash
build/tests/test_correctness_kernel --gtest_filter='\
DurableLogTest.IndependentShardValidationsRunConcurrently:\
DurableLogTest.PreparedStrictReadReservationRejectsSnapshotWriter:\
DurableLogTest.CoordinatorWriteReservationsRespectHalfOpenIntervalMatrix:\
DurableLogTest.RandomStrictDependencyCyclesNeverCommitEveryParticipant:\
DurableLogTest.LaterDecisionCanInstallAheadButWaitsForTheVisiblePrefix:*CAC*'
```

Expected: all pass with no timeout.

- [ ] **Step 8: Commit**

```bash
git add include/cedar/transaction/transaction_coordinator.h \
  src/transaction/transaction_coordinator.cc \
  tests/test_correctness_kernel.cc
git commit -m "feat: route coordinator through CAC queue and installer"
```

---

### Task 8: Stage B Concurrency, Sanitizer, and Regression Gate

**Files:**

- Modify: `tests/test_commit_queue.cc`
- Modify: `tests/test_transaction_validator.cc`
- Modify: `tests/test_commit_installer.cc`
- Modify: `tests/test_correctness_kernel.cc`

- [ ] **Step 1: Add deterministic stress test**

Run at least 128 concurrent transactions covering:

- disjoint shards;
- same key disjoint intervals;
- same key overlapping intervals;
- strict read/write cycles;
- reversed input ordering;
- out-of-order install completion.

Assert contiguous durable sequences, strictly increasing HLC, continuous visible prefix, and no duplicate transaction outcome.

- [ ] **Step 2: Run focused Stage B tests**

```bash
cmake --build build -j
ctest --test-dir build --output-on-failure \
  -R 'CommitQueueTest|CommitTimelineTest|TransactionValidator|CommitInstaller|VisiblePrefix'
```

Expected: 100% pass.

- [ ] **Step 3: Run complete Debug suite**

```bash
ctest --test-dir build --output-on-failure
```

Expected: 100% pass.

- [ ] **Step 4: Run TSAN**

```bash
cmake -S . -B build-tsan \
  -DBUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCEDAR_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure \
  -R 'CommitQueue|TransactionValidator|CommitInstaller|VisiblePrefix'
```

Expected: tests pass with no ThreadSanitizer report for timeline publication, queue leadership, hooks, reservations, installation, or prefix waiting.

- [ ] **Step 5: Run ASAN and UBSAN**

Repeat with `CEDAR_ENABLE_ASAN=ON` and `CEDAR_ENABLE_UBSAN=ON`.

Expected: zero sanitizer findings.

- [ ] **Step 6: Verify the global-lock objective**

Run:

```bash
rg -n 'commit_mutex_' \
  include/cedar/transaction/transaction_coordinator.h \
  src/transaction/transaction_coordinator.cc
```

Expected: no transaction-body use. Any remaining compatibility lock must be confined to legacy code scheduled for final CAC deletion and documented at its declaration.

- [ ] **Step 7: Commit**

```bash
git add tests/test_commit_queue.cc \
  tests/test_transaction_validator.cc \
  tests/test_commit_installer.cc \
  tests/test_correctness_kernel.cc
git commit -m "test: gate CAC Stage B concurrency semantics"
```

---

## Stage B Completion Criteria

- Different-shard transactions validate, queue, sync-share, and install concurrently.
- A single group sync covers multiple FIFO transactions without publishing tentative sequence/HLC state.
- Snapshot transactions conflict only on overlapping temporal write intervals.
- Strict transactions validate event identity and both temporal fences, including empty reads.
- Reservation acquisition is all-or-nothing and canonical.
- Confirmed durable transactions are never changed to aborted.
- Confirmed durable install/prefix failures return public `indeterminate`, remain resolvably committed, and retain protection until recovery/install.
- `kCommitted` is returned only after the caller’s own sequence enters `VisiblePrefix`.
- Timeline and outcome readers retain immutable snapshots, not mutable vector references.
- Parallel installation is exact-replay idempotent and partial installation never becomes visible.
- Focused tests, full CTest, TSAN, ASAN, and UBSAN pass.
