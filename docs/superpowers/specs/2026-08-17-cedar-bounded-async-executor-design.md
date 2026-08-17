# Cedar-Owned Bounded Async Executor Design

**Date:** 2026-08-17  
**Status:** Draft for user review  
**Scope:** Cedar Kernel Mode asynchronous commit submission

## 1. Decision

Cedar will own the admission and scheduling boundary for `CommitAsync()`.
Callers will submit requests to a bounded Cedar mailbox. A fixed, small set of
Cedar submission workers will move accepted requests into the existing Cedar
append pipeline. The mailbox reservation remains held until the request either
receives the existing RocksDB WAL-durable callback or reaches a definite
pre-durability failure. A caller never waits for mailbox capacity: it either
owns a bounded slot or receives `ResourceExhausted` immediately.

The existing append writer remains the only Cedar thread that enters the
RocksDB durable write seam. The executor changes who may submit work and how
overload is handled; it does not change the one-WAL-record, one-durable-sync
commit protocol.

## 2. Why This Is Needed

The current foreground admission counter limits work after an arbitrary number
of client threads have entered `SubmitAsyncCommit()`. Under a 512-thread load,
those callers still compete with the sampler, preflight worker, maintenance
worker, and durable writer. QoS hints and a second sampler watchdog are
best-effort scheduler interventions and cannot provide a bound on runnable
client threads. The watchdog also adds concurrent RocksDB runtime sampling.

The executor makes the bound structural. At most the configured mailbox
capacity can be waiting for Cedar acceptance, independent of the number of
client threads. Excess callers fail at the Cedar-owned boundary before they
enter the append queue or wait on a durable callback.

## 3. Invariants

The following existing contracts remain unchanged:

1. Every committed Cedar epoch has exactly one RocksDB WAL record and one
   durability synchronization.
2. RocksDB owns WAL creation, append, synchronization, rotation, recovery,
   MemTable insertion, VersionSet, MANIFEST, checkpoints, and backups.
3. `CommitAsync()` returns an accepted `CommitHandle` only after its epoch's
   existing WAL-durable callback has run. The callback still runs after WAL
   synchronization and before MemTable insertion.
4. No request is acknowledged as accepted after a definite pre-durability
   failure. An indeterminate write remains recovery-required and is not retried
   in-process.
5. The Cedar visible prefix advances only after the existing publication path
   completes. A mailbox slot is not a visibility or durability record.
6. `kRuntimeSnapshotStaleUs` remains exactly `250'000` microseconds. Stale
   runtime state fails closed for admission; the executor does not weaken this
   rule.
7. Production does not use foreground `GetIntProperty()` calls or
   `GetSortedWalFiles()`. Runtime pressure continues to use the Cedar runtime
   snapshot and RocksDB's internal WAL byte counter seam.
8. macOS `Sync()` and `Fsync()` continue to use `F_FULLFSYNC`; WAL option
   selection does not claim to remove that cost.

## 4. Architecture

### 4.1 Public boundary

`Transaction::CommitAsync()` keeps its current signature and durable-return
semantics. After building the existing `StoreCommitBatch` and handle state, it
calls a Cedar executor operation equivalent to:

```cpp
Status SubmitAsyncCommit(StoreCommitBatch batch,
                         std::shared_ptr<CommitHandle::State> handle);
```

The operation performs a non-blocking mailbox reservation. The reservation is
weighted by the encoded batch estimate and counts both requests and bytes. If
the database is closing, the runtime snapshot is stale, hard pressure denies
admission, or either bound would be exceeded, it returns the corresponding
non-OK status immediately. `Transaction::CommitAsync()` still consumes the
transaction through its existing `Finish()` and `EndCommit()` path before
returning that status.

For an accepted request, `SubmitAsyncCommit()` enqueues an executor ticket and
waits only on that ticket's existing WAL-durable/terminal state. It does not
wait for a free mailbox slot, and it does not expose a handle before durable
acceptance.

### 4.2 Cedar executor

The executor is a focused module, separate from RocksDB and from transaction
state. It owns:

- a mutex-protected FIFO mailbox of `AsyncSubmission` tickets;
- request and byte reservations;
- one stop state and a condition variable;
- a fixed production worker count of one submission worker;
- test-only counters and observers for accepted, rejected, started, durable,
  failed, and released tickets.

The worker takes a ticket, performs Cedar-side request preparation and runtime
admission, and hands the request to the existing append queue. It never calls
RocksDB directly. It may continue taking later tickets while an earlier ticket
waits in the append pipeline, but each ticket retains its mailbox reservation
until durable acceptance or a definite failure. This prevents worker progress
from turning an unbounded number of client waits into an unbounded number of
accepted requests.

The production worker count is deliberately fixed and small. It is not derived
from `hardware_concurrency()` and is not expanded in response to queue depth.
Developer and diagnostic profiles may select a larger bounded value for
differential testing, but validation rejects zero and any value above two.

### 4.3 Existing append pipeline

The existing Cedar preflight worker, single durable append writer, N+1 slot
protocol, runtime sampler, and manual maintenance worker remain in place. The
executor replaces only the direct external-to-append asynchronous admission
path. The asynchronous foreground admission semaphore and its blocking wait
are removed; synchronous commits retain their existing coordinator queue and
deadline behavior and are not changed by this design.

The sampler watchdog experiment is removed. There is one runtime sampler
worker. On macOS, Cedar may retain a best-effort QoS class on dedicated Cedar
workers, but QoS is never used as a correctness or freshness guarantee.

## 5. Configuration and Limits

The options are explicit and named rather than inferred from host thread
count:

```cpp
struct AsyncExecutorOptions {
  uint32_t submission_workers = 1;
  uint32_t max_mailbox_requests = 32;
  uint64_t max_mailbox_bytes = 4ULL * 1024ULL * 1024ULL;
};
```

The production Kernel profile uses the values above. The mailbox byte limit is
checked with overflow-safe subtraction and must be at least the configured
maximum single-batch size; otherwise database open fails with
`InvalidArgument`. A request larger than either the single-batch limit or the
mailbox byte limit is rejected without entering the mailbox. The existing
append queue limits remain an independent second bound.

The executor exposes accepted/rejected counts and current reserved
request/byte gauges through `CommitPipelineMetrics`. Rejections distinguish
`mailbox_requests_full`, `mailbox_bytes_full`, `runtime_snapshot_stale`,
`runtime_pressure`, `shutdown`, and `batch_too_large`. These counters are
diagnostic only and do not alter admission decisions.

## 6. Request and Error Flow

1. The caller builds a `StoreCommitBatch` and a `CommitHandle::State`.
2. Cedar validates the batch estimate and atomically reserves mailbox request
   and byte capacity. Failure releases no other request and returns an
   immediate status.
3. The executor worker dequeues the ticket and marks it started. The ticket
   remains reserved while the worker hands it to the append queue.
4. The append pipeline performs its cached-snapshot admission, preflight,
   validation, sequencing, and one-WAL write.
5. On the existing durable callback, the handle is marked `wal_durable` and
   the executor releases exactly one reservation. `CommitAsync()` returns an
   accepted handle. Publication continues as today and later completes
   `CommitHandle::Wait()`.
6. On definite pre-durability failure, the worker stores the terminal status,
   releases the reservation exactly once, and wakes the caller. No accepted
   handle is returned.
7. On an indeterminate or post-callback error, Cedar marks recovery-required,
   releases the reservation after the callback state is recorded, and returns
   the existing indeterminate/recovery-required result. The request is not
   silently retried.

Every terminal transition is idempotent. A ticket has one release bit so a
duplicate callback, shutdown path, or error path cannot underflow the bounded
capacity.

## 7. Shutdown and Recovery

`Database::Close()` first stops new mailbox reservations, then drains accepted
tickets and the existing append pipeline in their current order. It wakes
workers, joins the executor before closing RocksDB, and releases any ticket
that failed before durability with `ShutdownInProgress`. A ticket whose WAL
callback already fired remains an accepted request and follows the existing
publication/handle completion path.

If a process crashes, the executor mailbox is not a persistence mechanism.
Only the existing RocksDB WAL and Cedar metadata are consulted on reopen. The
existing recovery path replays the durable epoch and resolves accepted handles
through `ResolveTransaction()`; no executor ticket is reconstructed from
memory.

## 8. WAL and RocksDB Ownership

The executor never creates a WAL, calls `WriteOptions{disableWAL=true}`, calls
`Sync()` itself, rotates logs, or polls generic RocksDB properties. It passes a
decided batch to the existing Cedar RocksDB write seam. RocksDB remains the
sole owner of WAL file lifecycle, sync and recovery ordering, and MemTable and
MANIFEST publication.

Runtime pressure sampling continues through one
`GetCedarRuntimeMetrics()` snapshot plus the direct RocksDB WAL-retention
counter. The implementation must retain a regression test that writes WAL
bytes and observes a non-zero retained-WAL metric before admission decisions
use that metric.

## 9. Testing and Acceptance

Tests are test-first and cover the executor independently before integration:

- a full request-bound mailbox rejects the next async call immediately without
  entering the append queue;
- a full byte-bound mailbox rejects a large request while a smaller request can
  still be admitted when capacity permits;
- accepted tickets release exactly once at the WAL callback;
- callback ordering, one WAL record, one sync, MemTable visibility, and
  `CommitHandle::Wait()` are unchanged;
- runtime stale, hard-pressure, shutdown, preflight failure, and indeterminate
  write paths release capacity and return the correct status;
- many client threads cannot raise Cedar executor active submissions above the
  configured mailbox bound;
- reopen/recovery resolves all durable accepted requests and never reconstructs
  volatile tickets;
- ThreadSanitizer exercises close racing with rejected and accepted callers;
- ASAN/UBSAN cover ticket ownership, queue shutdown, and duplicate release.

The sustained-load gate runs the existing 512-client benchmark with the
production mailbox defaults, verifies that rejection counts are explicit rather
than hidden, and records throughput, WAL-sync p99, stale-snapshot failures,
mailbox occupancy, and reopen verification. A passing run must show no stale
snapshot admission caused by unbounded client contention; it need not claim
zero overload rejections because bounded admission is the intended contract.

## 10. Rollout and Rollback

The executor is enabled only for the explicit `kernel` profile. `generic` and
`lean` retain their existing storage ownership and can serve as differential
or rollback profiles. The feature is not considered production-ready until the
focused tests, full CTest suite, crash/reopen matrix, ASAN/UBSAN, TSAN, and
the sustained-load gate pass on the target host.

No change in this design permits relaxing the stale-snapshot bound, adding a
second WAL, enabling `F_BARRIERFSYNC` in production, or delegating admission
policy back to RocksDB.
