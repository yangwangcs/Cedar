# Cedar Atomic Commit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Cedar's PREPARE/Decision transaction protocol with clean-break Cedar Atomic Commit and prove at least 100,000 synchronous durable transactions per second in three independent Release runs.

**Architecture:** Delivery is split into three implementation plans with strict dependencies. Stage A adds the new record, segmented log, and FORMAT without touching the production commit path. Stage B adds the group-commit queue, temporal validator, thread-safe durable publication, parallel installer, and coordinator integration. Stage C/D migrates recovery/checkpoint/Blob/metrics, removes the legacy protocol, and closes crash, sanitizer, and Release performance gates.

**Tech Stack:** C++17, CMake/CTest, GoogleTest, POSIX durable file APIs, CRC32C, BLAKE3, ASAN, UBSAN, TSAN, Cedar benchmark artifacts.

## Global Constraints

- `AtomicCommitRecord` is the only durable transaction fact in the final implementation.
- The database format is incompatible with the old PREPARE/Decision layout; old databases are rejected read-only.
- No dual-write, fallback recovery, asynchronous acknowledgement, or relaxed durability is permitted.
- Snapshot mode retains snapshot isolation and validates only overlapping temporal writes.
- Strict mode validates captured event identity plus predecessor/successor fences and uses read reservations.
- Confirmed durable records are never aborted; failures before visible acknowledgement may return `indeterminate` and must resolve committed after reopen.
- `committed` is returned only after the caller's own sequence enters the continuous visible prefix.
- Checkpoint target is the minimum of captured continuous visibility and continuous SST coverage.
- Blob payload and Manifest liveness are durable before any commit record references a BlobRef.
- Acceptance requires complete CTest, ASAN, UBSAN, TSAN, deterministic crash campaign, legacy source inventory, and three independent Release runs at or above 100,000 durable tx/s.

---

### Task 1: Execute Stage A — Record, Log, and FORMAT Foundation

**Plan:** `docs/superpowers/archive/2026-08-24-historical-plans/2026-07-27-cedar-cac-stage-a.md`

**Produces:**

```cpp
AtomicCommitRecord
EncodeAtomicCommitRecord(...)
DecodeAtomicCommitRecord(...)
AtomicCommitLog::Open(...)
AtomicCommitLog::Append(...)
DatabaseFormat version 2
```

- [ ] Execute every Stage A task in order using RED → GREEN → commit.
- [ ] Verify `test_atomic_commit_log`, FORMAT tests, release source contract, and full Debug CTest.
- [ ] Verify the production coordinator still uses the legacy path at this boundary, so the new primitive is independently reviewable.
- [ ] Record the Stage A completion commit in this file.

Expected: a tested, standalone segmented AtomicCommitLog exists, while production commit behavior remains unchanged.

---

### Task 2: Execute Stage B — Queue, OCC, Durable Publication, and Installation

**Plan:** `docs/superpowers/archive/2026-08-24-historical-plans/2026-07-27-cedar-cac-stage-b.md`

**Consumes:** Stage A record and log interfaces.

**Produces:**

```cpp
CommitQueue
TransactionValidator
ReservationHandle
CommitInstaller
VisiblePrefix::WaitUntilVisible(...)
CommitTimeline::Snapshot()
```

- [ ] Execute every Stage B task in dependency order.
- [ ] Parallelize only independent focused-test/component tasks; serialize changes that touch `transaction_coordinator.cc` or shared interface definitions.
- [ ] Verify durable truth/public outcome fault matrix and exact transaction ownership in queue results.
- [ ] Verify snapshot overlap rules, strict temporal phantoms, canonical lock order, parallel install, and continuous visibility.
- [ ] Run Stage B focused tests, full Debug CTest, TSAN, ASAN, and UBSAN.
- [ ] Record the Stage B completion commit in this file.

Expected: the coordinator uses CAC queue/validation/install components without holding a transaction-wide commit mutex, while the legacy durable files remain available only until recovery/checkpoint migration is complete.

---

### Task 3: Execute Stage C/D — Recovery, Checkpoint, Removal, and Performance Closure

**Plan:** `docs/superpowers/archive/2026-08-24-historical-plans/2026-07-27-cedar-cac-stage-cd.md`

**Consumes:** Stage A/B AtomicCommitLog, queue, validator, installer, timeline, and visible-prefix interfaces.

- [ ] Migrate open/recovery to Manifest outcome prefix plus retained commit segments.
- [ ] Implement continuous visible/SST checkpoint and whole-segment reclamation.
- [ ] Complete Blob durability and GC fault boundaries.
- [ ] Replace PREPARE/Decision metrics, artifacts, reports, and fault campaign terminology.
- [ ] Delete `decision_log.h/.cc`, legacy symbols, paths, hooks, estimates, and recovery joins.
- [ ] Add and run the independent CAC Release gate.
- [ ] Complete Debug/Release CTest, crash campaign, ASAN, UBSAN, TSAN, and source inventory.
- [ ] Tune bounded generation sizes, queue limits, writer buffering, and installation parallelism until each of three clean Release runs reaches at least 100,000 durable tx/s with reopen verification.
- [ ] Record the final acceptance commit and evidence directories in this file.

Expected: Cedar has one clean-break CAC path, no legacy implementation, and complete correctness/performance evidence.

---

## Parallel Execution Rules

The shared repository must not receive overlapping edits from concurrent workers.

- Stage A may parallelize record-codec tests and FORMAT read-only rejection investigation, but the worker editing `atomic_commit_record.h` owns its interface until review.
- Stage B may parallelize queue, validator, and installer implementation in isolated worktrees after Stage A interfaces are fixed. Coordinator integration begins only after those commits are reviewed and merged.
- Stage C/D may parallelize Blob fault coverage, observability/artifact migration, and benchmark-gate construction after recovery/checkpoint interfaces are fixed. Legacy deletion waits until every production consumer has migrated.
- Each implementation task gets a fresh worker plus specification-compliance and code-quality review before integration.
- Every merged task reruns its focused tests; every stage ends with full regression and required sanitizer gates.

## Final Acceptance

- [ ] `FORMAT` rejects the old protocol without mutating old files.
- [ ] No production source contains PREPARE/Decision implementation symbols or files.
- [ ] Every acknowledged commit survives close/reopen and resolves to the same sequence.
- [ ] Atomic-log sequence and HLC are contiguous/strictly monotonic across checkpoint suffixes.
- [ ] Torn final frames truncate safely; complete corruption rejects open.
- [ ] Multi-shard transactions add no additional commit-log fsync.
- [ ] Physical syncs per committed transaction are at most 0.25; high-concurrency measurements report whether the preferred value below 0.1 is reached.
- [ ] Three clean Release runs independently report at least 100,000 committed durable transactions per second.
- [ ] Full CTest, crash campaign, ASAN, UBSAN, TSAN, and HTAP progress gates pass.
