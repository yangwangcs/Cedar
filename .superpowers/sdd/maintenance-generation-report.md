# Maintenance Generation Freshness Fix

## Scope

The fix is limited to Cedar maintenance-generation identity and one focused
RocksDB kernel regression test. It preserves Cedar-only maintenance admission:
the test observes exactly one Cedar-approved native background flush. It does
not alter WAL, recovery, MemTable, VersionSet, MANIFEST, or durability paths.

## Root Cause and Fix

`CedarMaintenanceSignature()` previously incorporated scheduling telemetry:
active MemTable bytes, WBM occupancy, retained WAL bytes, running jobs,
delay/write-stop state, and immutable age. At high write rates those values
changed between `PollCedarMaintenance()` and `RunCedarMaintenance()`, causing
the strict generation check to reject an otherwise valid grant as stale.

The generation now changes only when selectable per-column-family debt changes
(immutable bytes/count, L0 files, pending compaction bytes) or when a safety
boundary changes (background errors, manual conflict, recovery, shutdown).
The engine-side lifecycle and scheduling checks remain in force when the grant
is consumed.

## TDD Evidence

### RED

Before changing `src/engine/rocksdb/db/cedar_maintenance.cc`, the focused test
was added and built with:

```sh
cmake --build build-main-debug --target test_rocksdb_cedar_kernel -j4
./build-main-debug/tests/test_rocksdb_cedar_kernel \
  --gtest_filter=CedarKernelMaintenanceTest.FlushGrantSurvivesActiveMemtableTelemetryChange
```

It failed as required with:

```text
Operation failed. Try again.: Cedar maintenance snapshot is stale
```

The fixture was then tightened to retain one immutable facts MemTable while
leaving a partially filled active MemTable, so the final regression performs a
small active-only write without creating a second immutable MemTable. The test
asserts that immutable/L0/compaction debt is unchanged, the generation is
unchanged, and the original flush grant succeeds.

### GREEN

After narrowing the signature and rebuilding the content-addressed embedded
RocksDB Debug static library, the same focused command passed:

```text
[  PASSED  ] 1 test.
```

Full Debug kernel verification:

```sh
./build-main-debug/tests/test_rocksdb_cedar_kernel
git diff --check
```

Result:

```text
[==========] 14 tests from 2 test suites ran. (1004 ms total)
[  PASSED  ] 14 tests.
```

`git diff --check` produced no output.

## Remaining Qualification

This focused change has Debug regression evidence only. The parent sustained
Release campaign remains the required end-to-end qualification for high-rate
maintenance admission, crash recovery, and performance.
