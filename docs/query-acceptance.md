# Cedar Bitemporal Query Acceptance Evidence

Date: 2026-08-24

This document summarizes the correctness, recovery, sanitizer, projection, and
long-running evidence for the Cedar bitemporal query system on `main`. It is a
public evidence summary, not a hardware-independent performance claim.

## Scope

The acceptance surface covers:

- system-time snapshot isolation and valid-time state reconstruction;
- point, history, event, and change queries;
- typed property filtering and columnar batches;
- temporal expansion and k-hop traversal;
- coexisting shortest path, earliest arrival, latest departure, and fastest
  duration;
- interval joins and temporal aggregates;
- projection base, short-delta, long-delta, and partial-coverage fallback;
- WAL/reopen recovery and public install contracts; and
- space accounting and query scratch cleanup.

Projection files are derived. When a projection is missing, stale, corrupt, or
only partially covered, the query runtime falls back to authoritative
CedarParquet facts and does not return silently stale results.

## Acceptance Summary

The following gates passed in the recorded Release/Debug evidence:

| Gate | Result | Interpretation |
| --- | --- | --- |
| Debug CTest | `711/711` passed | Full debug suite for the recorded build |
| ASAN query/projection/temporal/vacuum suite | `254/254` passed | Leak detection disabled on macOS; no claim of LeakSanitizer coverage |
| UBSAN query/projection/temporal/vacuum suite | `255/255` passed | No UBSAN failure in the focused suite |
| TSAN lifecycle/delta/projection/crash/vacuum suite | `51/51` passed | No reported data race in the focused suite |
| QueryDifferential + QueryDelta under TSAN | `30/30` passed | Projection and canonical-result agreement |
| QueryDifferential + QueryDelta under UBSAN | `30/30` passed | Same agreement under UBSAN |
| Public header/install/embedded-engine contracts | `3/3` passed | Consumer-facing boundary remains intact |
| Reopen verification | `10/10` passed | Persisted databases reopened and facts/checksums matched |
| Space audit | `10/10` passed | Zero scratch bytes after reopen; projection bound satisfied |

On the recorded development host, the long-running mixed campaign exceeded
1,800 seconds. It used 32 writers, 32 readers, active maintenance, and
1,024 facts per transaction. Every operation exited successfully, reopened
successfully, and passed its case-level hard gate.

This long-running campaign is stability and recovery evidence. Its throughput
must not be treated as a universal Cedar SLA; the measured values are reported
in the [development-host performance report](query-performance.md).

## Reproduction Commands

Build directories are examples and may be changed by the caller:

```bash
cmake -S . -B build/query-debug -DBUILD_TESTS=ON
cmake --build build/query-debug -j2
ctest --test-dir build/query-debug --output-on-failure
```

Focused sanitizer gates:

```bash
ctest --test-dir build/query-tsan \
  -R 'QueryDifferential|QueryDelta' --output-on-failure

ctest --test-dir build/query-ubsan \
  -R 'QueryDifferential|QueryDelta' --output-on-failure
```

The public query campaign runner supports calibration, read matrices, mixed
workloads, reopen verification, and space audits:

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release \
  --phase release-calibration \
  --duration-seconds 2 \
  --facts-per-txn 1,4,8,16,32,64,128,256,512,1024,2048 \
  --output build/query-release/evidence/calibration
```

A sustained qualification requires an actual elapsed duration of at least
1,800 seconds. Shorter runs are calibration or warm-up measurements and must
be labeled accordingly.

## Projection and Recovery Evidence

The persisted fixture set includes four projection states:

| Fixture | Purpose | Required behavior |
| --- | --- | --- |
| `base` | Seeded projection generation | Read from the projection |
| `short-delta` | Small real retract/assert tail | Merge the delta with the base |
| `long-delta` | Larger authoritative tail | Preserve exact Snapshot semantics |
| `partial-coverage` | Deliberately incomplete coverage | Fall back to canonical facts |

Each fixture was closed, reopened, queried, and space-inspected. Differential
checks compared projected results with the authoritative evaluator. The
reopen path reconstructs query state from the database and does not depend on
a live query worker or an external side manifest.

The storage invariants remain unchanged:

- one Cedar-controlled WAL;
- RocksDB-derived ownership of WAL, recovery, MemTables, VersionSet, and
  MANIFEST; and
- CedarParquet as the authoritative logical columnar facts.

## Known Limits and Interpretation Rules

- The macOS host used for this evidence cannot provide LeakSanitizer, so ASAN
  runs used `detect_leaks=0`.
- Two resource-heavy ASAN workload cases were terminated by the host and are
  not counted as ASAN runtime passes.
- Internal relational operator measurements are not end-to-end public query
  API throughput.
- A case-level campaign hard gate is not the same as complete product
  acceptance; correctness, recovery, sanitizer, space, and duration gates must
  be considered together.
- The performance report contains development-host numbers for comparison. It
  does not define a portable SLA or a result for every workload shape.

## Related Documents

- [Development-host performance results](query-performance.md)
- [Cedar documentation index](README.md)
