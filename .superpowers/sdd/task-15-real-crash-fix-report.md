# Task 15 Real Crash Fix Report

## Changes

- Added `DatabaseOptions::query_crash_fault_injector_for_testing`, propagated
  through Database recovery, projection publication, QueryDelta enqueue, and
  analytical QueryScratch writes.
- Added deterministic phase hooks at `segment_sync`, `manifest_sync`,
  `current_replace`, `delta_enqueue`, and `scratch_write`.
- QueryScratch now publishes through a `.tmp` file and Cedar Open removes stale
  active scratch instances after an interrupted process.
- QueryProjectionStore removes temporary publication files during deferred
  open. A pinned generation with a checksum/header/page hole is immediately
  retired from `current_`, queues rebuild, and is quarantined after the final
  pin drains.
- Failed QueryDelta repair no longer advances the derived base to the
  authoritative visible watermark; canonical fallback remains available until
  exact repair succeeds.
- Crash matrix child now opens a real Database, commits canonical data, and
  invokes the actual projection, delta, or scratch publication path. The
  parent SIGKILLs after the phase hook and verifies reopen/query output and
  temporary-file cleanup without test-side deletion.

## Verification

```text
cmake --build build/query-debug -j2 --target \
  test_query_lifecycle test_query_crash_matrix test_vacuum \
  test_kernel_lifecycle test_query_canonical test_projection_store

ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryLifecycle|QueryCrashMatrix|Vacuum|KernelLifecycle|QueryCanonical|ProjectionStore'
```

Result: **55/55 tests passed**, including all five real crash phases and the
pinned-corruption quarantine regression.
