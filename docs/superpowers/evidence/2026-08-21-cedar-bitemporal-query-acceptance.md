# Cedar Bitemporal Query Acceptance Evidence

Date: 2026-08-22 (Asia/Shanghai)

This document records only commands actually run in this worktree. It is an
installation, Debug/sanitizer, and campaign-contract record. It does not make
a sustained-capability claim because the complete Release matrices and the
1,800-second mixed campaign did not pass all gates.

## Build identity

- Worktree: `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query`
- Branch: `codex/cedar-bitemporal-query-execution`
- Source snapshot: `c5ee3f1` (`codex/cedar-bitemporal-query-execution`)
- Host: Darwin arm64, Apple clang 21.0.0, CMake 4.2.1
- Build: `build/query-debug`, C++20, Debug
- Install prefix: `build-install-consumer-prefix`

## Public installation proof

The following command was run after rebuilding `cedar_core`:

```text
cmake --build build/query-debug -j2 --target cedar_core
```

It completed with exit code 0. The installed consumer test was then run by
CTest. It configures a clean producer, installs Cedar, compiles a separate
consumer using only `Cedar::cedar` and installed Cedar headers, and executes
the consumer:

```text
ctest --test-dir build/query-debug --output-on-failure \
  -R 'PublicHeaderContract|InstallConsumer|EmbeddedEngineContract'
```

Result: **3/3 passed**, including `CedarInstallConsumer` in 38.60 seconds.
The consumer registers an `int64` vertex property, writes values at valid
times 10 and 20, prepares a typed `StateAt` query at time 15, consumes a
Snapshot, checks the typed vertex and value result, closes, reopens, and checks
the same result again.

The package contained exactly the Cedar public target `Cedar::cedar`. The
implementation archive is installed as the Cedar-owned internal artifact
`lib/cedar/internal/libcedar_engine.a`; no engine headers or implementation
targets are installed. The package/header scans reject implementation names,
private includes, and absolute source/build paths.

Recorded artifact SHA-256 values from this run:

```text
cb23563828f5f804bb217dbd23d42db0925ecbc67ac2ac1146922e3438f81ac7  build-install-consumer-prefix/lib/libcedar_core.a
66157cd4fee973028c48866d1df98c476831ab018c7f92fb83f713a239c44db6  build-install-consumer-prefix/lib/cmake/Cedar/CedarConfig.cmake
bdab1103e39965cb2072c45d4d5ccf933404d7c4d780199d6d311bad66f7670c  build-install-consumer-prefix/lib/cmake/Cedar/CedarTargets.cmake
```

## Focused Debug query/projection gate

```text
ctest --test-dir build/query-debug --output-on-failure -R \
  'QueryCanonical|QueryLifecycle|QueryDelta|ProjectionStore|ProjectionFormat|TemporalModel|TemporalExpand|TemporalJourney|CoexistingPath|PublicHeaderContract|InstallConsumer|EmbeddedEngineContract'
```

Result: **128/128 passed**, total time 74.71 seconds. This includes canonical
typed StateAt/property reads, query lifecycle and cancellation, projection
publication/reopen/pinning, delta repair, temporal expansion, paths, journeys,
and the public installation checks.

## Fresh Debug and sanitizer gates

These results were run after the Task 19 installation and campaign changes:

```text
ctest --test-dir build/query-debug --output-on-failure
684/684 passed, exit 0, 210.30 seconds

ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 ctest --test-dir build/query-asan \
  --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
254/254 passed, exit 0

UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --test-dir build/query-ubsan \
  --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
255/255 passed, exit 0

TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 ctest --test-dir build/query-tsan \
  --output-on-failure -R 'QueryLifecycle|QueryDelta|ProjectionStore|QueryCrashMatrix|Vacuum'
51/51 passed, exit 0, 44.60 seconds
```

macOS does not support the requested ASAN leak detector in this environment;
the ASAN run therefore used `detect_leaks=0`. No leak capability is claimed.

The campaign option contract was then run against the Release benchmark after
the runner fixes:

```text
ctest --test-dir build/query-debug --output-on-failure \
  -R QueryCampaignOptionsContract
1/1 passed, exit 0, 160.02 seconds
```

This contract covers strict matrix parsing, property-filter result status,
turning-point artifact use, missing-input failures, and active-projection
baseline failures.

## Release artifacts and limits

Release calibration output is at
`build/query-release/evidence/release-calibration/` and recorded one successful
calibration row: `17,660 facts/s`, hard gate true, 5,000 microsecond end-to-end
p99. This is calibration-only and is not a Cedar capability claim.

The attempted short write sweep is at
`build/query-release/evidence/write-idle-short/`. It was intentionally stopped
after 53 data rows while covering the 1/4/8/16/32/64/128/256/512/1024/2048
facts-per-transaction values and writers 1/8; it is incomplete and cannot
identify a production turning point. No cold/warm read matrix, active
projection matrix, 1,800-second mixed campaign, reopen verification, or space
audit passed in this evidence point.

The campaign runner now audits supplied `run.csv` artifacts for
`reopen_verified`, terminal status, hard-gate status, byte accounting, scratch,
space amplification, and statistics bounds. It writes `audit-summary.csv/json`
and fails closed on malformed or mismatching rows. This is an audit of the
benchmark's recorded close/reopen and inspection evidence; it does not itself
reopen the database or recompute a checksum. A true database-level reopen and
checksum implementation remains required before Step 7 can be called complete.

The current runner summary schema reports facts/s and end-to-end p99 for these
campaign cases. It does not yet enforce the complete WAL-sync p99 and
idle-overhead gates required by the full Release acceptance plan; those remain
open implementation and evidence work.

## Gates not completed in this evidence point

The complete Release write/read/space and 1,800-second mixed gates remain
**blocked/incomplete**. Consequently this document does not claim Release
throughput, physical-read capability, turning-point capability, or sustained
Cedar capability.

## Obsolete benchmark cleanup

The former `cedar_kernel_columnar_bench` and its smoke test were removed in
this task. It opened private engine handles directly and therefore could not
serve as evidence for Cedar's public query capability. The supported benchmark
surface is now the Cedar-owned kernel/query benchmark and campaign tooling;
the removed executable is not replaced by a synthetic Cedar metric.

## Source audit at handoff

`git diff --check` passed before the focused test run. User-owned dirty SDD
reports were preserved; generated install/build outputs remain under ignored
test paths and are not part of the acceptance commit.
