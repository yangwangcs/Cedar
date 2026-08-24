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
- Pool admission now atomically aggregates all `ResourceDimension` limits and
  releases every dimension through reservation RAII. Analytical admissions
  honor `reserved_interactive_workers` while interactive/auto admissions retain
  the full worker pool.
- `QueryScratch` now enforces one-second read and write byte windows when rate
  limits are configured; runtime analytical scratch inherits the resource-pool
  read/scratch rates.
- Production `Database::Open` now resolves the effective storage profile (WBM,
  block cache, and production budget) before checking Cedar query/cache/delta
  allocations. The check applies in kernel mode as well; the kernel lifecycle
  fixture uses explicit query/cache sizes that fit its 1 GiB profile.
- Canonical property materialization now reserves actual string/binary payload
  bytes after binding, and canonical column construction reserves each payload
  before copying it into output vectors.
- `QueryScratch` accepts an abort callback and checks it at every read/write
  run boundary. Query cursors wire cancellation and deadline checks into this
  callback, covering external spill boundaries.

## Accounting caveat

The canonical binder now charges the actual string/binary payload as soon as
the bound rows are returned, and `BuildColumns` reserves payload bytes before
copying. The underlying history reader still constructs its `Value` before the
runtime reservation hook, and vector allocator capacity overhead is not
charged byte-for-byte; strict allocation-before-reservation for that reader
requires a lower-level reservation-aware history API.

## Remaining review items

The following Important findings are not fully closed in this change:


## Verification

```text
cmake --build build/query-debug -j2 --target test_query_resources test_query_relational test_kernel_lifecycle
ctest --test-dir build/query-debug --output-on-failure -R 'QueryResource|QueryRelational|KernelLifecycle'
54/54 tests passed
```
