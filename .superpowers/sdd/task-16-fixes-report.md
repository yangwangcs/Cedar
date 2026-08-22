# Task 16 Review Fixes

## Scope

Addressed the independent review findings for statistics identity, refresh
maintenance, runtime observability, Cedar file inspection, and crash-safe
statistics publication.

## Changes

- Added an authoritative `FactStore::SchemaFingerprint()` derived from the
  latest registered property definitions. Refresh records it and planner
  loading requires the current fingerprint, database identity, generation and
  base sequence. Empty or stale statistics remain unknown/conservative.
- `RefreshQueryStatistics()` now snapshots the current manifest and schema
  under the database mutex, then submits bounded Cedar-owned work to the
  existing executor. The move-only handle awaits completion and cancellation is
  observed at the task boundary without holding the database mutex during I/O.
- Connected query execution states to Cedar `QueryMetrics`; terminal states and
  captured output batches update bounded enum counters. Profile batch actuals
  include row, batch and decoded-byte totals when `capture_profile` is enabled;
  the disabled path does not perform profile accounting or timing.
- Cedar inspection decodes `.cmanifest` and projection-page headers for
  `.cstate`, `.csegment`, `.cadj`, `.cprop`, and `.cscratch`, exposing
  generation, base sequence, canonical coverage and checksum validity. RocksDB
  live `.sst` metadata remains a separate classification path.
- `.cstats` publication now writes, flushes and fsyncs a temporary file,
  renames it, and fsyncs the containing directory. A missing publication is
  therefore safely treated as unavailable by planner binding.
- Statistics decode validates the HLL precision/register cardinality.

## Verification

```text
cmake --build build/query-debug -j2                         PASS
ctest --test-dir build/query-debug --output-on-failure \
  -R 'QueryObservability|CedarFiles|QueryPlanner|QueryTypes|CoexistingPath' PASS
32/32 focused tests passed.
```

The pre-existing user-owned `.superpowers/sdd/task-12-report.md` modification
was preserved.
