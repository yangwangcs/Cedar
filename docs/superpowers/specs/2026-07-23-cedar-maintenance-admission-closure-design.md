# Cedar Maintenance Admission Closure Design

Date: 2026-07-23

## Goal

Close the two production-path admission gaps found by the six-design release
audit without changing the clean-break layout or database format 1:

1. the scheduler record for a flush must cover the callback that performs the
   actual SST publication and reference handoff;
2. Blob active-segment rotation must use the same typed admission, I/O budget,
   cancellation, shutdown, and metrics path as other maintenance work.

This is a synchronous ownership design: callers retain the publication locks
and frozen/immutable inputs while a single shared worker executes the admitted
callback. It does not introduce a new background thread or an alternate
runtime.

## Flush execution model

`TransactionCoordinator::Flush()` freezes each shard before submission. For
each frozen shard it computes the existing bounded write estimate, submits a
`WorkTaskRequest` with `WorkClass::kFlush`, the complete resource profile, and
the actual flush callback, then waits for that handle. The callback performs
`FlushEventsToSst`, Manifest publication, watermark advancement,
`CompleteFlushPublished`, and Blob-reference catalog handoff. The caller keeps
`flush_mutex_` held for the same critical section as today, so file-number and
VersionSet updates remain serialized. A failed submission or callback cancels
remaining queued flushes and returns the error; the frozen shard is restored or
released using the existing failure path. The resource lease is owned by the
execution service until callback completion.

The post-flush compaction/index/statistics/checkpoint decisions remain outside
the per-shard callback and use their existing `MaintenanceExecutor` paths.

## Blob rotation execution model

`RotateBlobSegments()` captures a bounded estimate while holding the existing
publication locks, then submits a `kForegroundWrite` task with a write,
descriptor, metadata, and I/O budget derived from the active Blob segments.
The callback executes `RotateActiveSegments()` and `ReconcileBlobSegments()`
under the same publication locks. If admission is rejected, no rotation is
performed. If the callback fails or the process shuts down while queued, the
lease and task completion are released and the next explicit rotation can
retry. The operation remains synchronous to its caller, preserving the
existing API and reference-safety semantics.

## Metrics and failure semantics

Both paths must increment the existing bounded scheduler counters for their
work class and expose resource/I/O usage through the existing metrics export.
Foreground classification lets an explicit rotation invalidate an already
queued optional Blob-GC estimate; GC must revalidate before copying.
Cancellation returns `QueryCancelled`; governor rejection returns its typed
resource status; publication uncertainty continues to set `recovery_required`
through the existing coordinator paths. No legacy scheduler, V2/Vn name, or
fallback direct execution path is added.

## Verification

Add regressions for:

- actual flush callback runs as `kFlush` and is covered by admission metrics;
- flush resource rejection occurs before SST publication and releases frozen
  references;
- Blob rotation is queued behind a blocker, reports `kBlobGc` admission and
  releases all resource dimensions;
- Blob rotation governor rejection and injected rotation/reconcile failure do
  not mutate the active segment or lose Blob references;
- shutdown drains accepted tasks and all leases are zero afterward;
- flush/rotation reopen and Blob-reference catalog consistency remain valid.

All builds and tests use `-j1`; this design does not require network access or
host package installation.
