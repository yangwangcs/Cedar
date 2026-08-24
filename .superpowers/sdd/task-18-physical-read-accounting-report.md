# Task 18 Physical Read Accounting

## Result

DONE

The Cedar storage adapter now reports foreground canonical physical read bytes
using the embedded RocksDB `PerfContext` counters. The implementation keeps the
public Cedar metrics engine-neutral and does not use query output/logical bytes
as a physical-read substitute.

## Implementation

- Added `canonical_read_physical_bytes` to `RocksDbRuntimeMetrics` and the public
  Cedar `RuntimeMetrics` mapping.
- Added a scoped PerfContext delta tracker for `get_read_bytes`,
  `multiget_read_bytes`, and `iter_read_bytes`. The tracker commits its delta on
  normal and error returns.
- Instrumented canonical `Read`, `Scan`, `ScanFamily`, `ReadExactFact`,
  `ReadExactFacts`, and temporal-neighborhood reads.
- Changed the query benchmark timed window to sample Cedar `RuntimeMetrics`
  before and after readers finish. `query_physical_bytes` is the monotonic delta
  of canonical physical reads plus projected scan physical bytes.
- A successful sample with a zero delta is reported as `query_bytes_complete=true`
  because a cache-resident query can legitimately perform zero physical reads.
- Extended the CSV contract to require the MiB/s column, non-negative physical
  bytes, and CSV/JSON agreement for physical bytes and completeness.

## Verification

```text
cmake --build build/query-debug -j2 --target cedar_query_bench test_fact_store test_kernel_snapshot
  passed

ctest --test-dir build/query-debug --output-on-failure \
  -R 'FactStoreTest.CountsCanonicalPhysicalReadBytes|KernelSnapshotTest.MultiExistsPreservesRequestOrder'
  2/2 passed

ctest --test-dir build/query-debug --output-on-failure -R QueryBenchmarkCsvContract
  1/1 passed (15-operation matrix plus CSV/JSON checks)
```

An isolated Debug benchmark run reported a non-zero physical delta:
`query_physical_bytes=7,851,726`, `query_bytes_complete=true`, and
`query_mib_per_second=7.47089` for the one-reader `state-at` workload.

## Scope

No legacy/Lean benchmark was restored. Existing user dirty reports were left
untouched; this report is specific to the physical-read accounting subtask.
