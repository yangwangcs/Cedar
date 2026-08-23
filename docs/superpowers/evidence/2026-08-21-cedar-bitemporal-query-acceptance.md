# Cedar Bitemporal Query Acceptance Evidence

Date: 2026-08-23 (Asia/Shanghai)

This document records evidence actually produced in this worktree. The current
sustained mixed run and database reopen checks pass. The strict space audit is
blocked by a statistics-to-derived-bytes gate on the `canonical-only` dataset;
no threshold or implementation was weakened.

## Build identity

- Worktree: `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query`
- Branch: `codex/cedar-bitemporal-query-execution`
- Source snapshot: `e6be96f6d680dbaa79d1bb1e28f8b7623c09f7f9`
- Host: Darwin arm64, Apple clang 21.0.0, CMake 4.2.1
- Release benchmark: `build/query-release/cedar_query_bench`
- Public defaults were not changed. Campaign admission was explicitly
  `commit_deadline_us=5000000`, `group_queue_requests=2048`,
  `group_queue_bytes=33554432`.

## Debug, install, and sanitizer gates

Recorded results:

```text
ctest --test-dir build/query-debug --output-on-failure
684/684 passed, exit 0

ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 ctest --test-dir build/query-asan --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
254/254 passed, exit 0

UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --test-dir build/query-ubsan --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
255/255 passed, exit 0

TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 ctest --test-dir build/query-tsan --output-on-failure -R 'QueryLifecycle|QueryDelta|ProjectionStore|QueryCrashMatrix|Vacuum'
51/51 passed, exit 0

ctest --test-dir build/query-debug --output-on-failure -R 'PublicHeaderContract|InstallConsumer|EmbeddedEngineContract'
3/3 passed, exit 0
```

The macOS host cannot provide LeakSanitizer, so ASAN used `detect_leaks=0` and
is not a leak-capability claim.

## Sustained mixed release evidence

Exact command:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase mixed-30-minute \
  --duration-seconds 1800 --writers 32 --readers 32 \
  --facts-per-txn auto-turning-point \
  --input build/query-release/evidence/calibration-final \
  --output build/query-release/evidence/mixed-sustained-final2
```

`build/query-release/evidence/mixed-sustained-final2/` contains ten operations;
all exited 0 with `terminal_status=OK` and `hard_gate_pass=true`. The sum of
timed operation elapsed values is `2163.412 s` (at least the required 1800 s).

| operation | facts/s |
| --- | ---: |
| state-at | 11325.6 |
| events | 13174.5 |
| expand-out | 13980.4 |
| temporal-aggregate | 13323 |
| interval-join | 10029.3 |
| k-hop | 12785.7 |
| coexisting-shortest-path | 14253 |
| earliest-arrival | 12272.4 |
| latest-departure | 17497.6 |
| fastest-duration | 14262.8 |

This is a capability result for these exact workload parameters, not a claim
that every graph shape or projection state has the same rate.

## Curated reopen verification

Historical artifacts were not deleted or overwritten. A curated input was made
by copying only the ten successful databases and `run.csv` files from
`mixed-sustained-final2`:

`build/query-release/evidence/curated-successful/`

Exact command:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase reopen-verification \
  --input build/query-release/evidence/curated-successful \
  --output build/query-release/evidence/reopen-curated
```

Result: exit `0`; `run_files=10`, `rows=10`, `failed_rows=0` in
`build/query-release/evidence/reopen-curated/audit-summary.json`. Every row in
`audit-summary.csv` has `terminal_status=OK`, `hard_gate_pass=true`, and
`reopen_verified=true`. Each database was actually reopened and its stored
facts/checksum matched; scratch bytes were zero in every row.

## Strict space audit

Exact command:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase space-audit \
  --input build/query-release/evidence/curated-successful \
  --output build/query-release/evidence/space-curated
```

Result: exit `1`; `run_files=10`, `rows=10`, `failed_rows=10` in
`build/query-release/evidence/space-curated/audit-summary.json`. All ten rows
reopened successfully, have zero scratch bytes, and satisfy the derived
projection `<=1.5x` authoritative-byte bound. They fail only the unchanged
statistics gate: `statistics_bytes` is about 16.8 KiB while `derived_bytes` is
about 17.5 KiB, roughly 96% of the denominator rather than at most 2%.

This is a real evidence/design mismatch, not a runner or reopen failure. The
source artifacts use `canonical-only`, where there is no material adjacency or
property projection; a percentage bound whose denominator is only that tiny
derived metadata block is not representative. The gate remains strict. A
projected-data campaign, or an explicitly reviewed statistics accounting model,
is required before space acceptance can be called complete.

## Historical failed probes

The original `build/query-release/evidence/` tree remains intact. Its global
audit had 930 input run files and 17 failed rows, primarily old append-queue
timeout probes, incomplete rows, and stale reopen failures. These artifacts are
retained for auditability and deliberately excluded from the curated current
capability input; they are not silently relabeled as passing.

## Status

Current status is **DONE_WITH_CONCERNS**: Debug/install/sanitizer gates,
sustained mixed execution, and curated database reopen verification pass.
Strict space acceptance is blocked by the canonical-only statistics ratio and
requires a projected-data rerun or approved accounting design. No source code
or public default changed in this evidence task.
