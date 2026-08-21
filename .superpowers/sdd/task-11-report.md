# Task 11 Report

Status: implemented with one documented follow-up limitation and committed.

Commit: `2d8c005 feat: bound Cedar query resources and spill`

Implemented:

- Query resource pool admission with overflow-safe memory and independent budget dimensions (scratch, read/prefetch bytes, decoded/output rows and bytes, interval fragments, graph labels, visited vertices, CPU), including exact dimension names in `ResourceExhausted` statuses. Prepared queries now admit against the Database-owned pool, with pool-wide outstanding memory/worker accounting released by reservation destruction.
- Existing relational reservations now synchronize lease accounting and support exact-once release helpers.
- Query runtime options with the required defaults are part of `DatabaseOptions`; production Open validates query allocations against the configured production memory budget.
- Retained output batch backpressure is enforced by reference-counted `QueryBatch` leases; interactive and analytical batch starts are capped at 256 and 4096 rows respectively.
- Cursor cancellation is lock-free/idempotent and deadline checks return `DeadlineExceeded`.
- Lazy `<db>/query/scratch/<instance>/<query>` scratch creation, immutable `CDRSCR1\0` blocks, query-id/length framing, CRC32C verification, quota/free-space checks, path/symlink escape rejection, and cleanup.
- CMake and focused resource tests are wired into the build.

Verification:

```text
cmake --build build/query-debug -j2 --target test_query_resources test_query_relational test_kernel_lifecycle
100% built successfully

ctest --test-dir build/query-debug --output-on-failure -R 'QueryResource|QueryRelational|KernelLifecycle'
100% tests passed, 0 tests failed out of 53

build/query-debug/tests/test_query_resources
7 tests passed
```

Concerns:

- Existing physical query operators still materialize their source streams synchronously; the pool and leases bound allocations and output retention, but do not introduce a new executor scheduler.
- Analytical hash/sort joins write serialized spill payloads through `QueryScratch`, close and verify-read the run, then use bounded fallback execution; interactive joins keep `NeedsSpill` as `ResourceExhausted`. `CleanupOldInstances` runs after database store open while the instance lock is held.
- Remaining limitation: the current relational seam verifies serialized runs but does not yet reconstruct both full `BatchStream` inputs from partition files before fallback. A complete partitioned external hash/sort executor should follow as a separate change to avoid altering Task10 operator contracts.

## Final review fixes (2026-08-22)

- Added RED coverage for malformed spill frames, then made `DeserializeRows`
  preflight every frame and cell count before vector capacity changes. A
  complete decoded partition now holds a reservation lease through its use;
  serialized payload bytes cover string/binary scalar allocations.
- Replaced stack-address scratch IDs with a process-wide monotonic query ID,
  and resolved `kAuto` from `PhysicalPlan::lane` before admission, worker
  reservation, and scratch creation.
- `Database::Open` now checks both production and kernel-test resolved
  profiles, including facts/meta write buffers, block cache, query memory,
  projection cache, and query delta. Developer profile behavior is unchanged.
- `QueryCursor::Close` and normal terminal completion synchronously clean the
  exact scratch directory; cancellation/deadline and repeated terminal reads
  also invoke cleanup. Scratch free-space reserve is shared through an atomic
  process reservation and released by `Cleanup`.

Verification performed:

```text
cmake --build build/query-debug -j2 --target test_query_resources test_query_relational test_query_canonical
build/query-debug/tests/test_query_resources
  13 tests passed
build/query-debug/tests/test_query_relational
  43 tests passed
build/query-debug/tests/test_query_canonical --gtest_filter=QueryCanonicalTest.StreamsStateAtFromTheConsumedSnapshot:QueryCanonicalTest.ConcurrentExecutionsDoNotShareCursorState
  2 tests passed
```

Remaining concerns: several existing kernel-test fixtures still use the old
256 MiB query/projection/delta defaults despite the 1 MiB kernel-test profile;
those fixtures need explicit smaller allocations before the full Debug
profile suite can pass. Non-cancellation terminal error branches still defer
their first cleanup call until the next terminal `Next()` or cursor
destruction.

## Repair re-review closure (2026-08-22)

Implemented the requested re-review repairs:

- `DeserializeRows` now preflights and charges the total decoded cell count in
  addition to row objects and serialized payload bytes. The lease remains owned
  by `DecodedRows` for the complete decoded-row lifetime. Added
  `SpilledRowsReserveEveryDecodedCellCapacity` RED/GREEN coverage.
- Query cursor terminal errors now use an exit guard that synchronously cleans
  analytical scratch on the first non-cancel/deadline error return.
- `QueryScratch::WriteRun` rolls back both scratch bytes and a newly acquired
  process-wide free-space admission on every post-admission failure. `Cleanup`
  releases admissions even when no query directory exists. Added
  `FailedDirectoryCreationReleasesFreeSpaceAdmission` RED/GREEN coverage.
- Production 1 GiB lifecycle/commit/recovery/benchmark fixtures now explicitly
  use 32 MiB each for query memory, projection cache, and query delta. The two
  kernel-test benchmark fixtures use 128 KiB each. WBM, block cache, and all
  query/projection/delta accounting constraints remain enforced. The benchmark
  workload defaults were updated so `KernelBenchmarkCsvContract` opens too.

Verification:

```text
cmake --build build/query-debug --clean-first -j1 --target \
  test_query_resources test_query_relational test_query_canonical \
  test_kernel_lifecycle test_kernel_commit test_kernel_bounded_benchmark \
  test_recovery_crash_matrix cedar_kernel_bench
  all targets built successfully

ctest --test-dir build/query-debug --output-on-failure -j1 -R \
  'QueryResource|QueryRelational|QueryCanonical|KernelLifecycle|KernelCommit|KernelBoundedBenchmark|RecoveryCrashMatrix|KernelBenchmarkCsvContract'
  106 passed, 0 failed
```

## Final re-review P1 lease closure (2026-08-22)

- External hash and sort-merge spill joins now transfer every decoded partition
  lease into the temporary `BatchStream` consumed by the in-memory operator.
  The lease therefore covers partition sorting, hash/index construction, and
  output production, then releases with the temporary stream; decoded leases
  are not copied into final output streams, so accounting is not double-charged.
- Added `ExternalSpillJoinRetainsDecodedReservationDuringUse`, which uses
  deterministic reservation boundaries to reject both external joins if a
  decoded lease would be released before the next partition's output/index
  reservations. Both failure paths return `NeedsSpill` and release all bytes.

Verification:

```text
cmake --build build/query-debug --target clean
cmake --build build/query-debug -j1 --target \
  test_query_relational test_query_resources test_query_canonical
  all targets built successfully

ctest --test-dir build/query-debug --output-on-failure -j1 -R \
  'QueryRelational|QueryResource|QueryCanonical'
  100% tests passed, 0 tests failed out of 77
```
