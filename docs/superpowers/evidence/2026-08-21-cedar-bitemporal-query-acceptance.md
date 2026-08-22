# Cedar Bitemporal Query Acceptance Evidence

Date: 2026-08-22 (Asia/Shanghai)

This document records only commands actually run in this worktree. It is an
installation and focused Debug gate record, not a sustained-capability claim.

## Build identity

- Worktree: `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query`
- Branch: `codex/cedar-bitemporal-query-execution`
- Source snapshot before the acceptance commit: `b4f04d5403ec62bbd05bf74328f0cbbd75f8fcba`
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

## Gates not run in this evidence point

- The full clean Debug suite was not rerun after the installation-only edits;
  the focused 128-test set above is the executed Debug evidence.
- ASAN, UBSAN, and TSAN commands were not rerun after these source changes.
  Existing sanitizer directories are therefore stale and are **blocked**, not
  evidence for this commit.
- No `build/query-release` was available in this worktree. The write
  turning-point matrix, cold/warm read matrices, reopen/space audit, and the
  1,800-second mixed campaign were not run. Their status is
  **calibration-only/blocked**.

Consequently this document does not claim Release throughput, physical-read
byte capability, or a 30-minute sustained Cedar capability. Task 18's physical
read accounting review is approved at the source snapshot, but the Release
throughput and sustained capability claims still require fresh Release and
sanitizer builds plus all preceding gates.

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
