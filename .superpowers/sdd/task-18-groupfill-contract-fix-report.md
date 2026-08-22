# Task 18 Group-Fill and Artifact Contract Fix

Status: implemented

Changes:

- Captured `CommitPipelineMetrics` immediately before and after the timed
  writer window. `group_fill_p50` is now computed from a non-negative delta of
  group and bucket counters, so seed/projection setup groups are excluded.
- Kept query physical-byte accounting honest: the runtime still reports
  `query_physical_bytes=0`, `query_bytes_complete=false`, and zero query MiB/s
  when no authoritative decoder counter is available.
- Added CSV quoting (including doubled quotes, commas, and embedded newlines)
  and JSON escaping for every string field. JSON now also emits the counted
  `query_operations` denominator alongside `query_qps` and `rows_per_second`.
- Expanded the CMake CSV contract from three operations to the complete
  fifteen-operation matrix. It checks required accounting columns, non-zero
  rows, `terminal_status=OK`, `reopen_verified=true`, and
  `hard_gate_pass=true`. It also checks a path containing a comma and quote in
  both CSV and JSON artifacts.
- Corrected the synthetic temporal-aggregate workload input to use valid,
  same-group intervals so the operation exercises the runtime and returns
  rows instead of failing during the contract run.

Verification:

```text
cmake --build build/query-debug -j2 --target cedar_query_bench test_query_bench_options
ctest --test-dir build/query-debug --output-on-failure -R 'QueryBench|KernelBench|QueryBenchmarkCsvContract'
100% tests passed, 16/16
```

The contract ran all fifteen query operations, the active projection case, and
the delimiter-containing artifact case. No physical read capability is
claimed by this task.
