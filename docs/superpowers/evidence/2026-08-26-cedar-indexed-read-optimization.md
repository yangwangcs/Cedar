# Cedar Indexed Read Optimization Evidence

Date: 2026-08-26

## Verification

- Build configuration: `build-indexed-debug` with
  `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1`, `CMAKE_BUILD_PARALLEL_LEVEL=1`.
- Focused build/test commands use `-j1`.
- The indexed baseline fixture creates 10,000 committed vertices and verifies
  full scan, `VertexPoint`, and unordered `LIMIT 10` result equivalence and
  profile counters.
- Complete CTest was rerun after configuring `BUILD_BENCHMARKS=ON`; benchmark
  contract tests require the benchmark executables to exist. The latest
  sequential run passed `836/836` tests in 588.54 seconds.

## Implemented Read-Side Contracts

- `VertexPoint` lowers to an exact canonical `ReadStateAt` point read. Its
  expected lookup cost is `O(1)` in the canonical key path.
- State-row `LIMIT K` uses a bounded state stream. It retains one identity
  chain and stops after `K` emitted rows, giving `O(K + scanned prefix)` work
  and bounded output memory instead of materializing all chains.
- Property postings are sorted by typed in-memory order. CPI1 now carries a
  fixed-size page directory with value/entity/valid-time bounds, a page bloom
  mask, and a page payload CRC. After a pinned segment is decoded, seek uses
  page-level binary search followed by the matching posting run, giving
  `O(log P + k)` in-memory work for `P` pages and `k` matching postings. The
  current reader still decodes a bounded segment before that seek; physical
  page IO is therefore not counted as a production claim.
- CPI manifest version 3 records `CoverageRegion.built_through`. The planner,
  generation pin, and reader reject a property index whose watermark is below
  the requested snapshot, so stale coverage is visible canonical fallback.
- Property-index segment names include generation IDs. Rebuilding after a
  delete publishes a new CPI generation without colliding with retired files;
  the runtime differential test verifies the deleted posting is absent.
- Source-bound adjacency uses the existing immutable adjacency index and
  records an explicit seek in the query profile; missing generation coverage
  keeps the existing bounded canonical fallback.
- T-Cypher predicates are parsed and bound once, then lowered to Cedar's
  `BindVertexProperty` and `Where` operators. No second executor or write path
  is introduced.

## Scope and Caveats

`RefreshQueryIndexes` builds CPI1 property-index segments through the existing
projection generation publication path. Runtime validates each candidate
against canonical state and falls back explicitly on stale or incomplete
coverage. Property indexes are page-described and CRC-checked on open. The
active adjacency capability remains Cedar's immutable source-bound in-memory
index; it is generation/watermark checked and merges bounded QueryDelta records.
Persisting adjacency postings as independent segment files remains outside this
read-side optimization because the current projection store has no canonical
edge-identity page schema to publish without introducing a second format.

## Release Characterization

Command (all builds use `-j1`):

```bash
benchmarks/run_cedar_indexed_read_matrix.sh \
  --build-dir build-indexed-release \
  --entities 1,10,100,1000 \
  --seconds 1 \
  --output /tmp/cedar-indexed-read-release.csv
```

Raw run from this session (`/tmp/cedar-indexed-read-release-final.csv`,
SHA-256 `df042f5b5ededfc56c442e237cae91b40415f4c1231c399cd0646e829ed21d9f`):

| entities | point p50/p95/p99 | graph p50/p95/p99 | peak RSS |
| ---: | ---: | ---: | ---: |
| 1 | 28/36/60 us | 38/47/73 us | 17.5 MiB |
| 10 | 130/155/228 us | 559/654/799 us | 17.7 MiB |
| 100 | 1,580/1,781/2,213 us | 62,561/64,726/64,726 us | 19.1 MiB |
| 1,000 | 17,545/17,545/17,545 us | 6,487,175/6,487,175/6,487,175 us | 22.8 MiB |

The benchmark repeats each fixture for one second and reports aggregate
output rows. Its canonical fixture does not expose physical-byte counters, so
these numbers are latency/RSS characterization only, not production
throughput or an indexed-vs-scan speedup claim.

The latest Debug indexed-read matrix was also run with `-j1` for entities
`1,10,100` and is available at `/tmp/cedar-indexed-debug-final.csv`. Its
point-state p50/p95/p99 were `220/259/315 us`, `1307/1425/1485 us`, and
`14373/14616/14668 us`, respectively; peak RSS was `27.5`, `27.9`, and
`29.2 MiB`. This remains fixture characterization only: the CSV's canonical
physical-byte fields are zero for this benchmark binary.

WAL append, commit ordering, conflict validation, recovery, MemTable
insertion, VersionSet, and MANIFEST write paths remain unchanged.
