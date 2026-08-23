# Task 15 Gate Fixes

## Scope

This patch closes the Task 15 final-review lifecycle and recovery findings.

## Changes

- Query admission is now atomic with `Database::Close`: `RegisterQueryState`
  takes the lifecycle mutex, rejects closing/closed databases with
  `ShutdownInProgress`, and returns a `Status`. `QueryRuntime::Execute`
  closes a state when admission fails, releasing its snapshot, scratch, output
  leases, and callbacks before returning.
- Cursor close now releases the pinned projection generation along with the
  snapshot and scratch. Returned batches retain their independent backing
  storage.
- Projection reads never return a partial chain set. A missing or undecodable
  segment marks an exact coverage hole and returns `NotFound`, so runtime uses
  canonical Cedar facts for the complete slice. The generation is retired,
  a rebuild request is queued (observable through
  `pending_rebuild_requests()`), and damaged files are quarantined once leases
  drain.
- The crash matrix now uses real `Database::Open`, canonical transaction
  commit, SIGKILL, observer-order assertions, database reopen, and canonical
  query verification for all five phases. Unpublished phase artifacts are
  ignored by authoritative recovery and cleaned before the final filesystem
  assertion.
- Added a projection corruption regression covering fallback, rebuild queueing,
  and post-lease cleanup.

## Verification

```text
cmake --build build/query-debug -j2 \
  --target test_query_lifecycle test_projection_store \
  test_query_canonical test_query_relational test_query_crash_matrix

ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryLifecycle|ProjectionStore|QueryCanonical|QueryCrashMatrix'
```

Result: all selected tests passed (36/36 in the focused query/canonical/
projection run; 3/3 crash-matrix tests).
