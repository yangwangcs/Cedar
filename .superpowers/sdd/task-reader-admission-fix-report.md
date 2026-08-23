# Reader Query Admission Fix Report

Status: `DONE_WITH_CONCERNS`

## Scope

The current P1 is the real benchmark workload
`facts_per_txn=16`, `readers=32` being rejected by Cedar query-memory
admission. The shared worktree already contains the focused benchmark change:

- benchmark query pool: `128 MiB -> 256 MiB`;
- minimum interactive per-reader budget: `4 MiB -> 8 MiB`;
- database production memory budget remains `1 GiB`;
- block cache remains `256 MiB`, projection cache `32 MiB`, and query delta
  `32 MiB`.

The change is benchmark-local and does not alter Cedar public database defaults.
No projection fixture was changed by this report.

## Regression evidence

Prebuilt focused regression command:

```bash
build/query-debug/tests/test_query_bench_workload \
  --gtest_filter=QueryBenchWorkload.ReaderMatrixHasEnoughQueryAdmissionCapacity
```

Repeated result: **5/5 runs passed**, exit code `0`. The test covers both
`readers=8` and `readers=32` with `facts_per_txn=16`, and asserts
`hard_gate_pass`, `terminal_status=OK`, and `reopen_verified`.

Direct real-workload command:

```bash
build/query-debug/cedar_query_bench \
  --operation=state-at --degree=1 --selectivity-percent=1 \
  --readers=32 --cache-state=cold --duration-seconds=1 \
  --facts-per-txn=16 --reopen-verify=true
```

Repeated result: **3/3 runs passed**, exit code `0`; each CSV row reported
`terminal_status="OK"`, `reopen_verified=true`, `hard_gate_pass=true`.

## Source rebuild status

Attempted:

```bash
cmake --build build/query-debug -j2 --target test_query_bench_workload
```

This currently fails before linking because a pre-existing, uncommitted parent
change expanded `BuildBenchmarkProjection` from three to five arguments while
one call site still uses the old signature:

```text
cedar_query_bench_workload.cc:978:24: error:
no matching function for call to 'BuildBenchmarkProjection'
candidate requires 5 arguments, call provides 3
```

That projection fixture change belongs to the parent task. It was not modified
or reverted here. The focused evidence above uses the already-built binary, so
it validates the staged admission behavior but does not replace a clean source
rebuild. After the parent resolves that unrelated call-site mismatch, this
focused test must be rebuilt and rerun.

## Resource-accounting review

The 256 MiB pool remains below the 1 GiB production memory ceiling after the
configured 256 MiB block cache, 32 MiB projection cache, 32 MiB delta cache,
and Cedar fact-store allocations. However, the benchmark query-pool cap itself
is doubled from 128 MiB to 256 MiB. Therefore this is not literally an
unchanged query-pool global limit; it is an increased benchmark-local Cedar
allocation within the unchanged 1 GiB process/storage profile.

The admission failure is therefore resolved for the real workload, but the
resource contract should be reviewed before calling the P1 fully closed. A
future implementation could preserve the 128 MiB query-pool cap by reducing
per-query materialization or adding explicit dynamic borrowing against the
existing aggregate pool, rather than raising the benchmark pool.

## Concerns / blockers

- Focused source rebuild is blocked by the parent projection fixture signature
  mismatch, not by query admission code.
- The current fix passes repeated real-workload and reopen checks, but doubles
  the benchmark query pool. It stays inside the 1 GiB production budget; the
  “no global resource upper-limit change” requirement should be explicitly
  accepted or replaced with a dynamic-borrowing/streaming fix.
- No projection fixture, threshold, or public Cedar default was changed here.
