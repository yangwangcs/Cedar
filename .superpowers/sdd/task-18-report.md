# Task 18 Report

Status: implemented as a bounded Kernel-only calibration harness.

Commit: `c32096d perf: add Kernel bitemporal query campaigns`

Implemented:

- Typed query operation/projection parser with documented degree, selectivity, readers, cache, hops, result limit, profiling, seed, duration, facts-per-transaction, writer, projection-work, and reopen controls.
- Deterministic Kernel transaction workload with reopen verification.
- Stable CSV header/row and JSON run output, including throughput and gate placeholder fields.
- Campaign shell runner with command manifest and raw CSV/JSON artifacts.
- CMake targets, parser tests, and CSV smoke contract.

Verification:

```text
cmake --build build/query-debug -j2 --target test_query_bench_options cedar_query_bench
build/query-debug/tests/test_query_bench_options
3 tests passed

cmake -DCEDAR_BENCHMARK=$PWD/build/query-debug/cedar_query_bench \
  -P tests/performance/test_query_benchmark_csv.cmake
exit 0
```

Concerns:

- This is a bounded calibration harness, not a completed 30-minute campaign implementation. It currently exercises a canonical state-at smoke workload; projection construction, all query operation execution, physical inspection/space amplification accounting, percentile sample aggregation, and full hard-gate classifier remain follow-up work.
- Existing protected Kernel benchmark edits and `.superpowers/sdd/task-12-report.md` were preserved unchanged.
