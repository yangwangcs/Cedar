# Cedar Bitemporal Query Acceptance Evidence

Date: 2026-08-24 (Asia/Shanghai)

This document records evidence actually produced in this worktree. The
historical sustained mixed run and database reopen checks pass. The current
bounded Release calibration and cold/warm query matrices also pass, including
real persisted base, short-delta, long-delta, and partial-coverage fixtures.
The full 30-minute mixed run and five-repeat write matrices remain open gates.

## Build identity

- Worktree: `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-bitemporal-query`
- Branch: `codex/cedar-bitemporal-query-execution`
- Source snapshot: `f6900c2` (`perf: bound query calibration admission`)
- Evidence commits: `02bbbe0`, `605da2b`, `ae093ec`, and `f6900c2`; these add
  persisted projection fixtures, real retract/assert QueryDelta tails, corrected
  temporal coverage, and Cedar-owned reader admission capacity. Historical
  mixed/reopen artifacts remain named explicitly where they were produced from
  earlier snapshots.
- Host: Darwin arm64, Apple clang 21.0.0, CMake 4.2.1
- Release benchmark: `build/query-release/cedar_query_bench`
- Public defaults were not changed. Campaign admission was explicitly
  `commit_deadline_us=5000000`, `group_queue_requests=2048`,
  `group_queue_bytes=33554432`.
- Dataset: historical canonical-only mixed workload, 32 writers/readers, 1,024
  facts/transaction, seed `1` in every mixed row (seed range `1..1`). The
  first Release calibration remains calibration-only; its turning-point
  artifact is `build/query-release/evidence/calibration-final/turning-point.json`
  (1,024 facts/transaction; calibration peak reported as 89,890.5 facts/s).
- Acceptance thresholds: sustained elapsed >=1,800 s; derived projection
  <=1.0x target and <=1.5x hard bound of authoritative live bytes; statistics
  <=2% of projection bytes; scratch bytes zero after close/reopen.
- Current bounded Release evidence: calibration facts/txn `1,16,64,256` (four
  rows, one reader), and cold/warm read matrices with all 15 operations and all
  five projection states at degree `1`, selectivity `1`, one reader (75 rows per
  phase). All rows exited `0`, passed the hard gate, and verified reopen.
- The required five-repeat write-idle/write-active matrices, 1/8/32-reader
  read matrices at all degrees/selectivities, and fresh 30-minute mixed run are
  still unrun after `f6900c2`; they are not passing evidence.

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

The strict space audit passes separately below, but this mixed run remains raw
workload observation/partial evidence only. The ten rows' `hard_gate_pass=true`
values are case-level runner results, not the complete Release acceptance gate:
the idle/active write gates, cold/warm read matrices, and a fresh Release
rerun were not completed. The mixed rows also report WAL-sync p99 `10,000 us`
and end-to-end p99 range `6,975,545..12,707,197 us`. This is not complete Cedar
capability evidence and must not be used to claim full acceptance or general
Cedar performance capability.

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

Result: exit `0`; `run_files=10`, `rows=10`, `failed_rows=0` in
`/tmp/cedar-space-audit-current/audit-summary.json` (copied to
`build/query-release/evidence/space-curated-current/`). All ten rows reopened
successfully, have zero scratch bytes, and satisfy the derived projection
`<=1.5x` authoritative-byte bound. For canonical-only rows, statistics are
audited against authoritative bytes; all ten ratios are below 0.001%.

The benchmark now creates truthful persisted projection fixtures. `base` covers
the seeded state chains; `short-delta` and `long-delta` publish the same base
followed by real retract/assert commit tails; `partial-coverage` is intentionally
classified by the planner as canonical fallback. Each fixture is closed,
reopened, queried, and space-inspected. The bounded cold/warm matrix contains
no failed rows.

The CSV campaign records `state-at`, history/events/changes, property-filter,
expansion, k-hop, and coexisting-path through the public Query/PreparedQuery/
QueryCursor surface. Temporal aggregate, interval join, and the three journey
objectives are explicitly labeled `internal-operator` because the benchmark
fixture cannot yet produce a truthful public journey result; public journey
contracts remain covered by the query unit suite.

Key artifact SHA-256 values:

```text
mixed-sustained-final2/summary.csv  fd9b6c44d9468f1adc2490f7267ef0bc6d003f87ce64016801d18c476ec9527f
reopen-curated/audit-summary.json   e5c24ae27d711739a3be55db1ed79b8e4bf44408de5a10c7199c6cda36a6aa08
reopen-curated/audit-summary.csv    fe37e27e8b04484d76f9b39aa034de567dbdac83e413e1f62a3a6bcaec843464
space-curated-current/audit-summary.json  04d76b969ad283c444779a8b16e02522b8410c70bcc4e78fbe04c4523877675a
space-curated-current/audit-summary.csv   be8ff046f99b4899d78acbe28f2aef5237df1f1ad1c41f61321369e289107387
```

## Historical failed probes

The original `build/query-release/evidence/` tree remains intact. Its global
audit had 930 input run files and 17 failed rows, primarily old append-queue
timeout probes, incomplete rows, and stale reopen failures. These artifacts are
retained for auditability and deliberately excluded from the curated current
capability input; they are not silently relabeled as passing.

## Status

Current status is **IN_PROGRESS**: Debug/install/sanitizer gates, historical
sustained mixed execution, bounded Release calibration, bounded projected read
matrices, and reopen verification pass. Full Release turning-point/write
overhead evidence and the fresh 30-minute mixed campaign remain before the
goal can be marked complete.

## Worktree status captured during acceptance

The required `git status --short --branch` capture showed the three preserved
pre-existing dirty reports:

```text
## codex/cedar-bitemporal-query-execution
 M .superpowers/sdd/task-12-report.md
 M .superpowers/sdd/task-18-groupfill-contract-fix-report.md
 M .superpowers/sdd/task-18-report.md
```
