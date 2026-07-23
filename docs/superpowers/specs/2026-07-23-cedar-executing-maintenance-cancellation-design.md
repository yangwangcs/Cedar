# Cedar Executing Maintenance Cancellation Design

**Status:** implemented; archived normal/sanitizer closure is 849/849 and the
current post-closure normal matrix is 855/855

**Date:** 2026-07-23

## 1. Scope

This design closes the remaining functional cancellation gap in the
authoritative HTAP resource-scheduling design. Database shutdown already
cancels queued optional maintenance and safely drains callbacks that have begun.
The missing behavior is cooperative cancellation of already-executing optional
maintenance so shutdown does not have to wait for an arbitrarily long normal
compaction, index build, statistics merge, or Blob GC operation.

The change preserves the clean-break API, database format `1`, Manifest
authority, snapshot pins, prepared-transaction completion grants, and the
scheme C drain-first shutdown protocol. It adds no legacy runtime, background
pool, hard thread interruption, or old-layout compatibility.

## 2. Cancellation classification

Only these classes are shutdown-preemptible:

- `WorkClass::kCompactionNormal`;
- `WorkClass::kIndexBuild`;
- `WorkClass::kStatsMerge`;
- `WorkClass::kBlobGc`.

These classes are never cancelled by this mechanism:

- `kCommitCritical`, `kForegroundWrite`, and `kPointRead`;
- `kFlush` and `kCompactionUrgent`;
- `kRecovery` and `kShutdown`;
- accepted interactive or analytical queries, which retain their existing
  query-registry cancellation protocol.

Cancellation returns typed `QueryCancelled`. It is not reported as successful
maintenance, pressure backoff, corruption, or `ShutdownInProgress`.

## 3. Task-scoped cancellation state

The runtime owns a small `WorkCancellation` object containing one atomic flag.
A preemptible `WorkTaskRequest` carries a shared instance. The same instance is
visible to:

1. `WorkExecutionService`, which records it while the task is queued or running;
2. `MaintenanceExecutor`, which passes it into the maintenance callback;
3. the production algorithm, which checks it only at declared safe boundaries.

`WorkExecutionService` continues to remove a task callback from the queued map
when a worker selects it, but retains a bounded running-task record containing
task id, work class, preemptible bit, and cancellation state until completion.
A class cancellation operation performs both actions under the service mutex:

- queued matches are removed exactly as today and completed immediately with
  `QueryCancelled`;
- running preemptible matches have their atomic state set and remain owned by
  their worker until the callback returns.

The cancellation counter increments once per task when its queued entry is
removed or its running token first transitions to cancelled. A cancelled
running callback also increments the normal completed counter when it exits.
Resource and I/O grants remain owned until callback completion.

## 4. Shutdown ordering

After winning `RUNNING -> QUIESCING`, `CedarDatabase::Close()` requests
preemptible maintenance cancellation before waiting for maintenance lifecycle
leases. It repeats the request after accepted commit/flush work drains because
an already-entered flush may have derived optional post-flush work. Close then
waits for all accepted maintenance leases to leave their safe boundary.

The typed `kShutdown` callback retains a final queued/running cancellation pass
before checkpoint as a bounded race safeguard. It never signals urgent
compaction or correctness-critical work. Checkpoint starts only after every
cancelled maintenance callback has returned and released its grants, pins, and
temporary output ownership.

## 5. Algorithm safe boundaries

### 5.1 Normal compaction

Cancellation is checked:

- before selecting each partition closure;
- while streaming input at SST block/output-block boundaries;
- after the output file is finished but before the Manifest edit begins.

Cancellation before publication closes writers and removes the unmanifested
output. Once `VersionSet::ApplyEdit` begins, publication is non-revocable: its
exact success, failure, or indeterminate status wins over a later cancellation.
Already-published output is never deleted because cancellation raced with the
Manifest rename.

Urgent compaction uses the same algorithm with a non-cancellable token and is
therefore unaffected.

### 5.2 Index build

Cancellation is checked before each source SST, between bounded posting-build
quanta, after sidecar encoding, and before its Manifest publication. A completed
but unpublished sidecar is removed on cancellation. Published fragments remain
valid advisory state; a later cancellation stops subsequent source files.

### 5.3 Statistics merge

Cancellation is checked before each source SST, between bounded event/statistic
quanta, and before each `StatsSnapshotStore::Upsert`. Already-upserted fragments
remain valid because statistics are source-identified and rebuildable. No
partial fragment is published.

### 5.4 Blob GC

Cancellation is checked before reclaim, between shards and relocation batches,
before segment retirement, and before the Manifest edit. Partial relocation is
safe: the Blob index may point to newly copied records while old sealed segments
remain present and referenced. On cancellation no segment is retired and no
Manifest deletion is published. Temporary buffers and grants are released.

Once Blob retirement Manifest publication begins, its exact status wins over a
later cancellation, matching compaction publication semantics.

## 6. Error and recovery semantics

Cancellation never sets `recovery_required` by itself. Real I/O, checksum,
Manifest-indeterminate, or Blob-index-indeterminate errors take precedence once
the corresponding durable mutation has begun. Reopen reconstructs optional
queues from Manifest/SST/Blob state; scheduler cancellation state is not a
durability log.

If shutdown cancellation occurs before any mutation, the task returns
`QueryCancelled` with no output. If it occurs after safe partial advisory work,
reopen or the next maintenance pass deterministically completes or rebuilds the
remaining work.

## 7. Observability

Existing bounded per-class scheduler counters remain authoritative:

- queued and running cancellations increment `cancelled` once;
- every started callback increments `completed` once;
- a submission blocked by the persistent class drain gate increments
  `rejected`, not `cancelled`, because it never entered scheduler admission;
- the rejected caller still receives typed `QueryCancelled`, and a supplied
  task token is signalled so caller-owned work cannot continue independently.

No task id, path, hash, index id, or schema epoch becomes a metric label. The
database shutdown result remains the checkpoint/teardown result; cancellation
of optional maintenance is an expected drain action, not a close failure.

## 8. Verification

Deterministic RED/GREEN tests must prove:

- a running normal compaction observes shutdown cancellation, removes its
  unpublished output, releases grants, and lets close reach checkpoint;
- running index and statistics work stop at source/quantum boundaries without
  publishing partial fragments;
- running Blob GC stops before retirement publication and reopen retains every
  live Blob value;
- `kCompactionUrgent`, flush, checkpoint, recovery, shutdown, and
  commit-critical callbacks ignore optional-maintenance cancellation;
- queued and running cancellation counters increment exactly once and started
  callbacks still complete exactly once;
- cancellation racing with Manifest publication preserves the exact
  success/indeterminate status and never deletes possibly published output;
- one-worker nested progress cannot deadlock;
- normal, ASAN, UBSAN, and TSAN full matrices pass with `-j1`.

Release closure additionally requires an archived production shutdown artifact
that exercises at least one running optional maintenance cancellation and
binds its logs, binary hash, environment, resource profile, and reopen
verification to a run id.

The accepted artifact is
`results/release-closure-20260723-maintenance-cancellation/`. Deterministic
checkpoint observers prove cancellation at the normal-compaction output-block,
index/statistics 64-event, Blob-GC 64-hash, and one-worker nested-progress
boundaries without timing sleeps. A separate real-database workload writes 65
Blob-backed values, cancels a running Blob GC at its second relocation batch
during `Close()`, checkpoints, reopens, and verifies all values. The focused
selection passes 48/48; normal, ASAN, UBSAN, and TSAN each pass 849/849 with
`-j1` and no sanitizer or race diagnostic. Compaction, index attachment, and
Blob retirement tests also signal cancellation from the post-rename fault
boundary and prove that the exact `Indeterminate` result and possibly published
artifact win over the later cancellation.

After subsequent unrelated closure work expanded the correctness kernel, a
fresh `cmake --build build-current -j1` and full normal `ctest --test-dir
build-current -j1 --output-on-failure` run passed 855/855 on 2026-07-23. This
does not rewrite the immutable 849-test release artifact or imply refreshed
855-test sanitizer evidence.
