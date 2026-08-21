# Task 11 Review Fix Report

## Closed

- External hash and sort-merge spill no longer build one full serialized
  payload per partition. Rows are serialized and written one at a time under
  a query memory guard and `QueryScratch` scratch reservation.
- Spill reads are bounded to one row run at a time. Decoded partition memory,
  temporary read payloads, scratch bytes, and output leases are accounted by
  the query reservation. Hash output probes left rows in source order and
  preserves inner, semi, and anti join semantics.
- `Database::Impl` now wires `wal_sync_critical` into query resource-pool
  options. Scratch reads aggregate `read_bytes` through the query reservation.

## Remaining review items

The following Important findings are not fully closed in this change:

- `QueryResourcePool` admission still aggregates memory and workers only;
  scratch/read/prefetch/decoded/output/interval/graph/visited/cpu pool-wide
  admission counters and a usable `reserved_interactive_workers` partition
  remain to be implemented.
- Scratch I/O rate limits are still configured but are not enforced through a
  time-window token bucket for `QueryScratch` writes/reads.
- Canonical property materialization and column builders estimate string and
  binary output, but the property binder can allocate payloads before a
  reservation is taken; a reservation-aware binder/column append path is
  still needed for strict allocation-before-reservation ordering.
- Materialization and spill partition/run boundaries do not yet perform the
  full cancellation/deadline checks requested by the review.
- Production `Open` storage-profile resolution and kernel-mode query-budget
  safety were outside the touched spill path and remain for a follow-up.

## Verification

```text
cmake --build build/query-debug -j2 --target test_query_resources test_query_relational test_kernel_lifecycle
ctest --test-dir build/query-debug --output-on-failure -R 'QueryResource|QueryRelational|KernelLifecycle'
54/54 tests passed
```
