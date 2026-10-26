# Cedar Query Performance: Development-Host Results

Date: 2026-08-25

This report records measurements from the Cedar development host. The numbers
are reproducible workload observations, not hardware-independent claims or a
portable service-level objective.

## Test Configuration

- Release build of the Cedar bitemporal query system;
- one Cedar-controlled WAL;
- authoritative CedarParquet facts;
- Cedar temporal projections and QueryDelta overlays;
- WAL admission of 5,000,000 microseconds, 2,048 queue requests, and
  33,554,432 queued bytes; and
- macOS arm64 development host with Apple Clang.

The write calibration used one writer and one reader, paused projection work,
and a two-second timed window for each transaction size. Focused write cases
used five repeats. The sustained mixed case used 32 writers, 32 readers,
active maintenance, and 1,024 facts per transaction.

## Write Performance

`facts/s` is the primary metric because one transaction can contain many
facts. `txn/s` is derived as `facts/s / facts_per_txn`.

| Facts per transaction | Facts/s | Approx. transactions/s |
|---:|---:|---:|
| 1 | 366 | 366 |
| 4 | 1,380 | 345 |
| 8 | 2,580 | 323 |
| 16 | 4,555 | 285 |
| 32 | 8,355 | 261 |
| 64 | 15,208 | 238 |
| 128 | 25,022 | 195 |
| 256 | 41,404 | 162 |
| 512 | 73,495 | 144 |
| 1024 | **111,396** | **109** |
| 2048 | 108,704 | 53 |

The local turning point is approximately 1,024 facts per transaction. Through
that point, a durable WAL sync is amortized over more facts. Beyond it,
transaction assembly and processing cost begin to offset the batching gain.

### Focused write cases

| Workload | Average facts/s | Average end-to-end p99 |
| --- | ---: | ---: |
| Idle, 16 facts, 1 writer | 5,018 | 5 ms |
| Idle, 16 facts, 8 writers | 19,704 | 13 ms |
| Idle, 64 facts, 1 writer | 15,761 | 7 ms |
| Idle, 64 facts, 8 writers | 49,999 | 25 ms |
| Active projection, 16 facts, 1 writer | 4,773 | 5 ms |
| Active projection, 16 facts, 8 writers | 18,829 | 16 ms |
| Active projection, 64 facts, 1 writer | 14,856 | 6 ms |
| Active projection, 64 facts, 8 writers | 65,301 | 19 ms |

WAL-sync p99 was normally 5 ms in the focused Release cases. The active
8-writer/64-fact result is treated as run-to-run variance until a larger paired
sample is collected; it is not a universal maintenance-overhead claim.

## Read Performance

The cold and warm matrices cover 15 query operators, five projection states,
reader counts of 1 and 8, multiple degrees and selectivities, reopen
verification, and space accounting. The values below are matrix averages and
are useful for relative comparison rather than a fixed-query SLA.

| Operator | Cold qps | Warm qps | Cold p99 | Warm p99 |
| --- | ---: | ---: | ---: | ---: |
| `state-at` | 36.6 | 37.4 | 229 ms | 232 ms |
| `history` | 81.7 | 85.7 | 126 ms | 116 ms |
| `events` | 77.3 | 82.0 | 126 ms | 117 ms |
| `property-filter` | 53.6 | 57.2 | 123 ms | 98 ms |
| `expand-out` | 195.6 | 201.5 | 47 ms | 43 ms |
| `expand-in` | 134.3 | 136.7 | 92 ms | 89 ms |
| `expand-both` | 217.5 | 223.3 | 39 ms | 36 ms |
| `k-hop` | 60.7 | 65.6 | 127 ms | 101 ms |
| `coexisting-shortest-path` | 206.6 | 216.8 | 45 ms | 40 ms |
| `earliest-arrival` | 92.7 | 96.7 | 76 ms | 66 ms |
| `latest-departure` | 86.9 | 91.8 | 94 ms | 78 ms |
| `fastest-duration` | 22.1 | 23.7 | 261 ms | 227 ms |

`interval-join` and `temporal-aggregate` were also measured as internal
relational operators at approximately 477k and 457k queries/s respectively.
Those values are not end-to-end public query API throughput and are therefore
reported separately.

## Sustained Mixed Workload

The sustained campaign exceeded 1,800 seconds. Every case exited successfully,
reopened successfully, and passed its case-level hard gate.

| Operation | Facts/s |
| --- | ---: |
| `state-at` | 11,326 |
| `events` | 13,175 |
| `expand-out` | 13,980 |
| `temporal-aggregate` | 13,323 |
| `interval-join` | 10,029 |
| `k-hop` | 12,786 |
| `coexisting-shortest-path` | 14,253 |
| `earliest-arrival` | 12,272 |
| `latest-departure` | 17,498 |
| `fastest-duration` | 14,263 |

This campaign validates long-running stability, WAL/reopen behavior,
maintenance, and space accounting. Its latency is not directly comparable to
the cold/warm read matrix because writers and readers contend simultaneously.

## Temporal Range and Transaction Read Profile

The temporal-range profile is a separate, short fixture-level measurement of
the newly completed T-Cypher read paths. It uses 16 vertices, 16 typed edges,
two configured property lanes, three commit sequences, one session, a one
second per-case window, and a 64 MiB query budget. It reports the effective
source, output rows, physical/decoded bytes, pages read/skipped, p50/p95/p99,
RSS, and errors; the acceptance gate requires zero errors and bounded memory.

### Debug profile

| Case | Ops/s | p50/p95/p99 (us) |
| --- | ---: | ---: |
| point state | 1,589 | 562/1,010/1,693 |
| system-time AS OF | 1,613 | 562/886/1,637 |
| system-time BETWEEN | 1,714 | 526/941/1,565 |
| two-segment path | 209 | 4,460/6,473/8,301 |
| transaction read-your-writes | 2,557 | 352/563/1,038 |

### Release profile

| Case | Ops/s | Rows/s | p50/p95/p99 (us) | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| point state | 16,771 | 268,336 | 53/116/168 | 18.9 MiB |
| system-time AS OF | 17,050 | 272,800 | 53/91/160 | 19.2 MiB |
| system-time BETWEEN | 18,711 | 299,376 | 48/97/144 | 19.3 MiB |
| two-segment path | 1,916 | 15,328 | 487/713/1,137 | 19.4 MiB |
| transaction read-your-writes | 26,297 | 447,049 | 34/49/111 | 19.9 MiB |

These measurements are fixture-level comparisons, not universal capacity
claims. The profile executable and gate are
`build-tcypher[-release]/cedar_temporal_range_profile` and
`benchmarks/check_cedar_temporal_range_profile.sh`.

## Correctness and Resource Gates

- Debug CTest: `812/812` passed.
- QueryDifferential + QueryDelta under TSAN: `30/30` passed.
- QueryDifferential + QueryDelta under UBSAN: `30/30` passed.
- `base`, `short-delta`, `long-delta`, and `partial-coverage` fixtures passed
  typed canonical differential checks and reopen verification.
- Space audits passed with zero scratch bytes after reopen.

See the [acceptance evidence](query-acceptance.md)
for the complete gate interpretation and known limits.

## Reproduction

```bash
ctest --test-dir build/query-debug -j1 --output-on-failure
ctest --test-dir build/query-tsan \
  -j1 -R 'QueryDifferential|QueryDelta' --output-on-failure
ctest --test-dir build/query-ubsan \
  -j1 -R 'QueryDifferential|QueryDelta' --output-on-failure

benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release \
  --phase release-calibration \
  --duration-seconds 2 \
  --facts-per-txn 1,4,8,16,32,64,128,256,512,1024,2048 \
  --output build/query-release/evidence/calibration
```

Compare future results only when the Cedar revision, compiler, host,
filesystem, workload, transaction size, duration, writer count, group-commit
limits, and reopen setting are documented together.

## Column Page Cache Metrics

`GetCommitPipelineMetrics()` separates these layers:

| Fields | Meaning |
| --- | --- |
| `column_page_cache_hits`, `column_page_cache_misses`, `column_page_cache_decodes` | Physical immutable column-page lookups and successful decodes |
| `column_page_resident_charge_bytes` | Payload capacity charged to the shared strict cache |
| `column_page_pinned_charge_bytes` | Resident charge currently pinned; a subset of resident bytes |
| `column_page_uncached_live_bytes` | Live transient column pages, charged to the read working pool |
| `storage_file_read_calls`, `storage_file_read_bytes` | Data-page requests issued to the filesystem, including checksum and canonical-key validation pages |
| `storage_read_working_bytes`, `storage_read_working_peak_bytes`, `storage_read_working_rejections` | Current, peak and rejected DB read reservations |
| `parquet_reader_metadata_bytes`, `parquet_reader_metadata_peak_bytes` | Current and peak reader-owned footer, indexes, Bloom storage and table-property capacity estimates |
| `projection_page_cache_hits`, `projection_page_cache_misses`, `projection_page_cache_resident_bytes` | Independent derived-page cache lookups and resident usage |

Filesystem request bytes are not device I/O. Buffered requests can be satisfied
by the Linux page cache. Opening readers also reads metadata, which is outside
the data-page request counters. Recovery validation can warm both layers before
a workload starts; use counter deltas after recovery for workload comparisons.

The legacy `canonical_page_decodes` counter counts canonical path cold loads.
It is not the total of all column decodes. Legacy hits, misses, decoded bytes,
resident, pin and insertion failures now count physical column pages: a
canonical sort/value pair can produce two rejected insertions. Do not add the
legacy gauges to the corresponding column gauges, or add pins to residency.
The DB read pool and a query's logical pin allowance also describe different
constraints on memory; a query pin does not allocate a second payload.

`projection_cache_bytes=0` disables derived-page residency. Point-entity query
reads admit validated pages; range reads look up existing pages without
admission. Cache entries contain unfiltered immutable pages. Their keys include
database/segment identity, generation, base sequence, schema, page and payload
checksum. Resident entries do not keep retired generation files alive; active
read handles retain generation and query leases until consumption completes.

The persisted-dataset runner and matrix are documented in
`columnar-cache-and-deployment.md`. Their Debug smoke verifies correctness and
counter collection; it does not establish a cold-read or production latency
improvement. Preserve the raw CSV and per-run sidecars when comparing builds.

`CanonicalFactReader::ReadExact` now accepts an optional
`std::shared_ptr<FactReadMemory>` request argument. Calls retain the default,
but custom reader subclasses must update their override signature. Ordinary
state/event/exact requests and columnar reads propagate the query resource to
storage page reservations. Reader-open metadata remains reader-owned rather
than part of the data-page request accounting.

Exact reads return a movable `FactEventBatch`; use `.events` for rows and retain
the batch while using its buffer. Clearing `.events` retains its capacity and
reservation until the batch is destroyed or replaced. Point `FactEvent` results
carry their result reservation as well. Budgeted exact reads inspect value sizes
at the pinned snapshot before `MultiGet` copies encoded strings; this adds a
sizing pass and should be included in read-cost comparisons. Ordinary scan
decoding, event buffers, state history and correction workspace reserve query
memory before owning allocations. Runtime reservation owners are not persisted.
The exact-read array charge includes RocksDB's temporary pinned-value slots,
KeyContext and sorted-pointer heap arrays, plus per-batch LookupKey and
GetContext heap workspaces. Geometric array growth includes overlapping old
and new buffers. These charges apply before the corresponding read allocations;
insufficient allowance returns ResourceExhausted and rolls back reservations.
Transaction-overlay merged rows, sorting workspace and callback copies are
query-budgeted. Columnar overlay output consumes event batches directly. Event
limits apply after merging; state limits apply after correction-history reduction.
Other engine-internal allocations, including iterator and file-selection
structures, still require an allocation audit; these counters do not establish
a hard bound on all process memory.

`parquet_reader_metadata_bytes` reports the estimated dynamic storage owned by
currently open Parquet readers. `parquet_reader_metadata_peak_bytes` retains
the maximum combined reader charge for that database resource lifetime,
including after readers close. It excludes decoded pages charged to the shared
cache and is an estimate of owned capacity, not allocator RSS or a reservation.
# Column-page decode timing

The dedicated columnar cache benchmark reports `decode_us` as the measured
interval's primitive page decode time, accumulated in nanoseconds and converted
to microseconds once for CSV output. The timer starts after file I/O and ends
when primitive decompression/decoding returns. It includes decoder budget
callbacks and failed decode attempts; it excludes typed-column conversion,
canonical validation, cache lookup, and file reads. It is elapsed time summed
across workers, so it is neither CPU time nor end-to-end latency.

Internal runtime counters additionally separate all 25 physical columns:
lookup hits/misses, admissions, admission rejections, completed typed-page
decodes and retained allocation charges, primitive decode attempts and time.
Temporary internal-key validation contributes primitive attempts/time but no
cache lookup or retained typed-page charge. Admission rejection means admission
was requested but disabled, oversized, or rejected by the cache; lookup-only
bypasses are not counted as rejected admissions. A cache-hit query pin denial
returns a resource error before recording a successful hit.
