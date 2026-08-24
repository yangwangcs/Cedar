# Task 18 P1 Fix Report

Implemented the P1 benchmark safety fixes while preserving the dirty Task12
report.

## Changes

- The campaign runner now supports the ordered Task18 phase names, five-repeat
  write phases, per-case CSV/JSON artifacts, a command manifest, and CSV/JSONL
  summary files. Any non-zero benchmark exit or failed hard-gate column makes
  the runner exit non-zero.
- Typed operations backed by the current Snapshot API are explicitly marked
  supported. Graph, property, aggregate, and journey operations now return an
  explicit `NotSupported` result instead of silently executing a state scan.
- Warm runs perform an unmeasured conditioning pass; readers execute in
  concurrent threads; seed and a deterministic dataset checksum are emitted.
- WAL-sync and end-to-end p99 now come from Cedar's commit pipeline metrics.
  The result records operation support, cache conditioning, and metric
  completeness, and an incomplete result cannot pass the hard gate.

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_bench_options cedar_query_bench
ctest --test-dir build/query-debug --output-on-failure -R 'QueryBench|KernelBench'
  -> 16/16 passed
benchmarks/run_cedar_query_campaign.sh --phase release-calibration --duration-seconds 1
  -> exit 0; per-case CSV/JSON and summary emitted; hard_gate_pass=true
```
