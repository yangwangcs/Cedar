# Task 18: Active Projection Benchmark

## Scope

`cedar_query_bench --projection-work=active` now exercises a durable Cedar query projection generation in the canonical-only benchmark path. The workload seeds authoritative facts, closes the database, builds a checked `QueryProjectionStore` generation with `ProjectionBuild`/`EncodeProjectionPage` and CRC metadata, reopens the database so the generation is loaded by normal recovery, then refreshes query statistics and awaits the asynchronous maintenance handle.

The active path requires the maintenance operation to complete successfully. It reports `maintenance_status=refresh-complete` only after `QueryMaintenanceHandle::Await()` returns `OK`. The existing final reopen checksum verification remains enabled.

## Verification

```text
cmake --build build/query-debug --target cedar_query_bench -j2
ctest --test-dir build/query-debug -R QueryBenchmarkCsvContract --output-on-failure
```

Result: `QueryBenchmarkCsvContract` passed. The contract runs paused cases plus an active case and verifies the persisted `projections/generation-1.cstats` file, `maintenance_status=refresh-complete`, `maintenance_observed=true`, reopen verification, and the hard gate.

Direct smoke output also returned exit code `0` with `reopen_verified=true` and `hard_gate_pass=true`.
