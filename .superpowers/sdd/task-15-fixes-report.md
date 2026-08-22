# Task 15 Fixes Report

## Implemented

- `QueryExecutionState` now tracks active `Next` operations and provides a
  close barrier. Database shutdown requests cancellation, waits for all query
  operations, then invokes the one-shot close callback that releases the
  Cedar query Snapshot, batches, output lease, and scratch before RocksDB is
  closed.
- `Next` calls are serialized per cursor while `Cancel` remains non-blocking.
  `terminal_info()` reads only the synchronized shared execution state. The
  terminal error proxy and operation guard transition every failure to
  `kFailed`/`kCancelled`; clean EOS remains `kCleanEnd` with `complete=true`.
- Interactive and analytical cursors are both registered with the database
  query registry. Close/destructor/database shutdown unregister through an
  idempotent close callback, eliminating stale Snapshot pins.
- Projection Open removes unpublished `.tmp` files before loading CURRENT and
  validates an optional authoritative visible/oldest-readable watermark.
  A damaged derived segment is recorded as an unavailable region and causes
  canonical fallback without disabling unrelated regions or quarantining
  authoritative facts.
- `test_query_crash_matrix` now supports the five requested phase arguments,
  absolute database path, and ready-fd protocol. The parent forks a child,
  waits for readiness, sends SIGKILL, reopens, and verifies temporary cleanup
  for `segment_sync`, `manifest_sync`, `current_replace`, `delta_enqueue`,
  and `scratch_write`.

## Verification

```
cmake --build build/query-debug -j2 --target \
  test_query_lifecycle test_query_crash_matrix test_vacuum \
  test_kernel_lifecycle test_query_canonical test_projection_store
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryLifecycle|QueryCrashMatrix|Vacuum|KernelLifecycle'
```

Result: 23/23 focused lifecycle/recovery/kernel/vacuum tests passed. The
projection and canonical targets build successfully. One pre-existing
`QueryCanonicalTest.PinsSnapshotUntilEndOfStreamOrExplicitClose` assertion
still expects the old behavior where `Database::Close` returns
`SnapshotPinned`; Task 15 intentionally changes this contract to cancel and
join active queries before closing the store, so that test must be updated by
the owning task.

## Residual risks

- The projection repair queue/quarantine worker is not yet wired to a Cedar
  maintenance queue; damaged derived regions are isolated and canonical
  fallback is safe, but rebuild scheduling remains follow-up work.
- Database Open now exposes `query_open_stage_observer_for_testing` and uses a
  deferred projection catalog: authoritative recovery/watermarks are finished,
  only the projection base metadata is read, QueryDelta is repaired through
  the visible watermark, and only then is the full derived generation loaded
  and enabled. Future/stale bases are rejected by the authoritative watermark
  checks.
