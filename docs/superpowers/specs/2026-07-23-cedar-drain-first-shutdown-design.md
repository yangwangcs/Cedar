# Cedar Drain-First Shutdown Design

**Status:** implemented; local normal/sanitizer verification complete, release
artifact pending (scheme C)

**Date:** 2026-07-23

## 1. Scope

This design closes the database-level shutdown gap in the authoritative HTAP
resource-scheduling design. It adds one public close protocol to the current
clean-break API. It does not restore an old runtime, old layout reader, or any
external `V2`/`Vn` name. The on-disk format number remains `1`.

The protocol must protect lazy T-Cypher result streams, in-flight commits,
point reads, maintenance, checkpoint durability, telemetry, and the shared
execution service. A submitted shutdown task is real work: the protocol body,
not an empty marker, runs as `WorkClass::kShutdown`.

## 2. Public contract

```cpp
enum class ClosePolicy : uint8_t {
  kCancelQueries = 0,
  kDrainQueries = 1,
};

Status CedarDatabase::Close(
    ClosePolicy policy = ClosePolicy::kCancelQueries);
```

`kCancelQueries` is the default and destructor policy. It rejects new public
work, cooperatively cancels every accepted query, waits for `Next()` calls that
were already executing to leave the database-backed runtime, then checkpoints
and tears down services. It does not wait for a client to destroy an idle
result stream.

`kDrainQueries` rejects new public work and waits until every accepted result
stream has either reached a terminal status or been released. It then drains
maintenance, checkpoints, and tears down services. This explicit policy may
wait for the client; the destructor never selects it.

`Close()` is thread-safe and idempotent. Concurrent callers observe the same
final status. `Open()` is a one-shot operation on a database object: opening a
closed or closing object returns `ShutdownInProgress`, while a second or
concurrent attempt on the same running object returns deterministic
`InvalidArgument` without running recovery twice. Reopen uses a new
`CedarDatabase` instance and format `1` recovery. Blocking `Close()` invoked
from an execution-service worker returns `InvalidArgument` before changing the
lifecycle, because that worker cannot safely wait for and join its own service.

## 3. Lifecycle state and admission

The database owns a shared `DatabaseLifecycle` state with these phases:

```text
RUNNING
  -> QUIESCING
  -> DRAINING_COMMITS
  -> DRAINING_MAINTENANCE
  -> CHECKPOINTING
  -> CLOSED
```

All public work enters through a lifecycle lease before dereferencing database
subsystems. Leases are classified as commit/write, point read, query, or
maintenance. `visible_seq`, cache/storage snapshots, benchmark storage
snapshots, and session creation return `StatusOr` so they can reject admission
with the same typed status. Once quiescing starts, admission returns the typed
status:

```text
ShutdownInProgress: database lifecycle: database is closing
```

The close initiator atomically wins the `RUNNING -> QUIESCING` transition.
Other close callers wait for `CLOSED` and receive the stored close result.
Register/schema/index mutation APIs are treated as writes. Metrics and trace
exports remain readable after close from their retained snapshots, but do not
start runtime work.

Database-created `TcypherSession` objects retain the shared lifecycle identity
alongside the coordinator pointer. `Begin`, `Stage`, `RecordRead`, and `Commit`
enter before dereferencing the coordinator; an already-entered session commit
owns a commit lease and therefore drains before checkpoint. A retained session
after database destruction fails with `ShutdownInProgress` instead of touching
destroyed coordinator state.

## 4. Lazy-query registry and safe cancellation

Every successful `ExecuteTcypher` receives an internal cancellation token even
when the caller supplied none. If the caller supplied a token, cancellation is
the logical OR of caller cancellation and shutdown cancellation.

The returned outermost stream is a `LifecycleTrackedResultStream`. Its shared
registry entry records:

- accepted and terminal/released state;
- the shutdown cancellation token;
- the count of `Next()` calls currently inside the wrapped runtime.

`Next()` enters the registry before touching the wrapped stream and leaves it
after the call. During cancel-close, registry cancellation prevents all future
entries and causes them to return `QueryCancelled`; close waits only for
already-entered calls to leave. The wrapper and cancellation state outlive the
database, so a client-held idle stream remains safe after database destruction.
The wrapped stream retains its existing immutable snapshots, resource
extension, and execution-service ownership, but cannot re-enter code paths that
use database-owned coordinator or I/O pointers after cancellation.

During drain-close, future `Next()` calls remain permitted for already accepted
streams. The registry reaches drained state when each stream is terminal or
released. A terminal `NotFound` counts as normal completion; any other terminal
status also releases the drain obligation.

## 5. Shutdown execution and ordering

The close initiator first closes admission and lets public calls that already
own write/commit, point-read, query-admission, or maintenance leases reach their
scheduler-visible completion. Cancel-close then cancels registered queries,
waits for active `Next()` calls to leave, and invokes their bounded cleanup
callbacks so idle client-held streams release query/spill grants without being
destroyed by the client. Drain-close waits for accepted streams to become
terminal or released and then performs the same safe cleanup. This pre-submit
barrier avoids a single-worker inversion where a higher-priority shutdown
worker waits for lower-priority work that only that worker can execute. It then
submits one synchronous task:

```cpp
WorkTaskRequest{
  WorkClass::kShutdown,
  ResourceProfile{0, 0, 0, 0, 1},
  true,
  0,
}
```

The callback performs the actual database protocol:

1. publish `DRAINING_COMMITS` and verify accepted writes/commits are drained;
2. verify the selected query cancel/drain barrier has completed;
3. publish `DRAINING_MAINTENANCE`, cancel queued optional analytical and
   maintenance work, preserve queued `kCompactionUrgent` as essential pressure
   relief, and wait for already-entered maintenance calls to leave their safe
   boundary;
4. publish `CHECKPOINTING`, flush frozen/active state and durably checkpoint
   WAL safe positions, transaction outcomes, CommitTimeline, Manifest, index
   and statistics metadata using the existing checkpoint order;
5. publish the callback result.

After the shutdown task completes, the close caller stops the execution service
and telemetry, then publishes `CLOSED`. Stopping a worker pool is deliberately
outside the worker callback because a worker cannot join itself. It is teardown,
not a substitute for the typed shutdown task.

If submission itself fails after quiescing, close still performs a bounded
cancel-close teardown and returns the exact typed failure. It never reopens
admission or reports success.

## 6. Error and durability semantics

`ShutdownInProgress` is a distinct `Status` code with constructor, predicate,
and stable string form. It is used only for lifecycle admission failures;
accepted queries cancelled by close return `QueryCancelled`.

The first checkpoint or teardown error becomes the stored close result. Later
cleanup is still attempted. A failed close remains `CLOSED`; reopening requires
a new object and normal recovery. A close that begins after a prepared
transaction must not convert the transaction into maintenance backoff or
cancel its commit-critical completion grant.

## 7. Observability

The existing scheduler counters must show one submitted/admitted/completed
`shutdown` task for a successful close. A callback failure still increments
completed and preserves its typed status. Lifecycle exports bounded gauges for
the current phase and active commits, queries, query calls, and maintenance.
Cancellation increments the existing query/scheduler cancellation metrics at
the point where cancellation occurs; no unbounded query identifier becomes a
metric label.

## 8. Verification

Deterministic tests must prove:

- new public work returns `ShutdownInProgress` after quiescing;
- close runs the protocol callback on a scheduler worker and accounts one
  `WorkClass::kShutdown` task;
- cancel-close cancels idle and executing streams without use-after-free;
- drain-close waits for terminal/released streams and does not cancel them;
- a stream retained beyond database destruction returns `QueryCancelled`;
- concurrent/idempotent close callers receive the same result;
- worker-originated blocking close is rejected without changing lifecycle;
- one database object performs at most one recovery attempt, including under
  concurrent `Open()` calls;
- accepted commits drain before checkpoint and prepared transactions complete;
- accepted session commits drain before checkpoint;
- queued optional work is cancelled while urgent compaction and commit-critical
  work complete;
- public session/snapshot/stat APIs reject after quiescing with the typed
  lifecycle status;
- checkpoint failure is returned and a new object recovers consistently;
- one-worker nested progress cannot deadlock;
- normal, ASAN, UBSAN, and TSAN matrices pass with `-j1`.

Release closure additionally requires fault/reopen artifacts and the completion
matrix to point to the exact tests and archived run identifiers. This feature
does not by itself complete the six-design release/paper goal.
