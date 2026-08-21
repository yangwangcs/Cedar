# Task 11 Final Fix Report

Baseline: `a6f45df`

Closed the final Task 11 review findings:

- `QueryBudget::cpu_us == 0` remains the default unlimited CPU budget. The
  canonical runtime only reserves CPU microseconds when a nonzero cap is set.
- Pre-materialization memory is owned exclusively by its
  `QueryReservationLease`; the runtime resets the lease after materialization
  and no longer manually releases the same bytes.
- `QueryScratch` admission now returns a shared RAII `IoPermit` token. The
  token remains alive through the complete physical `WriteRun`/`ReadRun`, so
  WAL-critical gating and aggregate I/O accounting cover actual spill I/O.
- Read-byte reservations are released when rate checks or checksum/trailing
  frame validation fails. A regression test asserts failed reads leave no
  residual read reservation.
- The database resource pool's prefetch capacity is sized for all admitted
  workers (`max_prefetch_bytes * query_workers`) with overflow saturation,
  while each query retains its own per-query reservation.

Verification was delegated to the parent agent because the shared build tree
was concurrently rebuilding and produced a transient malformed archive during
the handoff. The parent will rerun the requested serial build and ctest command
after this commit.
