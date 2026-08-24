# Task 15 Report: Query Lifecycle, Projection Retirement, and Recovery

## Result

`DONE_WITH_CONCERNS`

The query lifecycle state machine, analytical-query shutdown registration,
Vacuum snapshot pinning, projection generation retirement integration, and
deterministic derived-file open checks are implemented. The complete
subprocess SIGKILL matrix for all five requested query crash phases remains an
open follow-up because the current public Database test hooks do not expose
deterministic delta/scratch phase injection.

## Implemented

- Added `QueryCursorState` and `QueryTerminalInfo` with idempotent clean EOS,
  cancellation, failure, and terminal status reporting.
- Added shared `QueryExecutionState` cancellation state. `Cancel` is atomic and
  does not wait on a read callback; destructor and explicit close request
  cancellation and release cursor-owned scratch/snapshot resources.
- Registered analytical query states with `Database::Impl`. `Database::Close`
  closes admission, requests query cancellation, acknowledges query task
  shutdown, drains accepted commits, stops query delta/projection components,
  joins maintenance, cleans scratch, then closes the authoritative store.
- Added query snapshot sequence accounting to the same lifecycle registry.
  `Database::Vacuum` returns `SnapshotPinned` rather than cancelling an older
  analytical query and calls projection `RetireBefore` after a successful
  canonical vacuum.
- Corrupt/IO-failing derived projection reads disable the current generation
  and force subsequent readers to use canonical fallback. Existing pinned
  generation leases remain valid until released.
- Added lifecycle, Vacuum pin, derived-current corruption, temporary-file
  recovery, deadline, resource-budget, and shutdown-order tests.

## Verification

Commands:

```text
cmake --build build/query-debug -j2 --target \
  test_query_lifecycle test_query_crash_matrix test_vacuum test_kernel_lifecycle
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryLifecycle|QueryCrashMatrix|Vacuum|KernelLifecycle'
```

Result: `22/22` tests passed.

Additional regression command:

```text
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryCanonical|ProjectionStore'
```

Result: `30/30` tests passed.

## Remaining risks

- The requested `segment_sync`, `manifest_sync`, `current_replace`,
  `delta_enqueue`, and `scratch_write` subprocess kill/reopen matrix is not
  complete. Projection segment/manifest/current fault hooks already exist and
  are covered by projection tests; delta and scratch need DatabaseOptions
  phase injectors plus a ready-fd child harness.
- Query cancellation cleanup currently resets cursor-owned snapshot and scratch
  from the shutdown callback. It is intentionally non-blocking, but a future
  ThreadSanitizer pass should harden concurrent `Next`/shutdown access with a
  small cursor lifecycle mutex.
- Projection corruption handling disables the whole current derived
  generation rather than recording a per-region unavailable bitmap and
  enqueuing a Cedar P4 rebuild. Canonical facts are never quarantined.
- The 200-seed exhaustive oracle from Tasks 13/14 remains outstanding.
