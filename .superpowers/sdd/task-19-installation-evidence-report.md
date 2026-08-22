# Task 19 Installation and Evidence Report

Status: **DONE for the executed installation/focused Debug scope; sustained Release scope remains blocked**

## Implemented

- Replaced the trivial install consumer with a public-header-only typed query
  consumer. It writes a vertex and two temporal property versions, prepares
  `StateAt`, executes against a consumed Snapshot, verifies the typed result,
  closes/reopens, and verifies the result again.
- Added package/header contract scans for implementation names, private
  includes, engine handles, and absolute source/build paths.
- Made the installed implementation archive Cedar-owned (`libcedar_engine.a`)
  while exporting only `Cedar::cedar`.
- Documented the installed query API in `README.md`.
- Removed the obsolete direct-engine `cedar_kernel_columnar_bench` and its
  smoke contract. No synthetic replacement was introduced.
- Added auditable acceptance evidence at
  `docs/superpowers/evidence/2026-08-21-cedar-bitemporal-query-acceptance.md`.

## Verification

- `cmake --build build/query-debug -j2 --target cedar_core`: passed.
- `ctest --test-dir build/query-debug --output-on-failure -R
  'PublicHeaderContract|InstallConsumer|EmbeddedEngineContract'`: 3/3 passed
  (latest run 38.74 seconds).
- Focused query/projection/temporal/public suite: 128/128 passed (latest run
  73.90 seconds).
- `git diff --check`: passed.
- The user-owned dirty SDD reports were not staged or modified by this task.

## Concerns and bounded claims

- Full clean Debug, fresh ASAN/UBSAN/TSAN, Release turning-point matrices,
  cold/warm read matrices, space audit, and the 1,800-second mixed campaign
  were not run after these edits. Their status is calibration-only/blocked.
- No Release throughput or 30-minute sustained capability is claimed here.
- The Task 18 physical-read accounting review is approved at the pre-commit
  source snapshot; this task does not add a fabricated physical-byte metric.

The implementation commit is the commit containing this report and the files
listed above; its exact hash is recorded by `git log` at handoff.
