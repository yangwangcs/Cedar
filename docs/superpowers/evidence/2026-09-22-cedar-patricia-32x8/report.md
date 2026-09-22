# Cedar Patricia 32-by-8 Release Decision

## Decision

Reject and revert the 32-by-8 candidate. All 240 timed samples were valid,
reported zero errors, and matched the expected hash within each seed/workload,
but seed 20260920 missed the mandatory eight-writer gate: candidate/SkipList
was 0.9233, above the 0.85 maximum. Per-seed gates are binding; the two passing
seeds cannot average away this failure. Full-chain validation was not started.

## Build And Source Controls

- Candidate commit: `48bba504495e2d6ed35492088ec84fec26ca75fc`
- Nibble control commit: `77593a87a64f03d7716318e3c9ce7bb672a78327`
- Four-by-64 Byte control commit: `76adf448257aaaa4eacac8ba5451ba38a51223f3`
- All three benchmark source SHA-256 values:
  `4c4ea921641adcd529a995cfb1fa0e3e9bf398e77b4134a6c65023dc6594fa53`
- All three source worktrees were clean.
- Candidate compiler: `/usr/bin/c++`; `Release`; `-O3 -DNDEBUG`;
  `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1`.
- Candidate build:
  `/Volumes/E/CedarBuild/patricia-retention-32x8-o3-20260922`.

## Correctness And Diagnostics

The 1024-key one-, four-, and eight-writer `phase=all` smokes all reported
1024 operations, zero errors, and hash `16340194407465152223`. The untimed
131072-key eight-writer diagnostic reported zero errors, hash
`454339457082336225`, index insertion 16.007 ms, and Arena 19,027,944 bytes.

The deterministic direct-index hook fixture allocated one 280-byte branch and
72 bytes of exact-size child blocks, copied one existing child pointer, and
observed two successful root CAS operations plus two successful segment CAS
operations, with no forced failures. These counters are diagnostic only and
were disabled in every timed process.

## Release Medians

Times below are MemTable elapsed milliseconds; Arena values are bytes.

| Seed | Writers | Candidate ms | Control | Control ms | Ratio | Candidate Arena | Nibble Arena ratio | Gate |
|---|---:|---:|---|---:|---:|---:|---:|---|
| 20260920 | 1 | 26.688 | Byte | 29.520 | 0.9041 | 17,678,624 | 1.0800 | pass |
| 20260920 | 4 | 9.782 | SkipList | 17.242 | 0.5673 | 18,990,160 | 1.0891 | pass |
| 20260920 | 8 | 15.236 | SkipList | 16.502 | 0.9233 | 19,023,696 | 1.0895 | **fail** |
| 20260921 | 1 | 26.796 | Byte | 29.449 | 0.9099 | 17,681,664 | 1.0801 | pass |
| 20260921 | 4 | 10.121 | SkipList | 19.479 | 0.5196 | 19,008,696 | 1.0900 | pass |
| 20260921 | 8 | 14.175 | SkipList | 18.443 | 0.7686 | 19,024,840 | 1.0904 | pass |
| 20260922 | 1 | 26.689 | Byte | 29.571 | 0.9025 | 17,681,336 | 1.0801 | pass |
| 20260922 | 4 | 10.102 | SkipList | 17.674 | 0.5716 | 18,998,368 | 1.0883 | pass |
| 20260922 | 8 | 14.612 | SkipList | 17.194 | 0.8498 | 19,020,752 | 1.0897 | pass |

Arena passed the `<=1.25x Nibble` gate at every writer count and seed. The
historical single-writer Vector gate failed on every seed and is reported only
as historical context, as required.

## Artifacts

- `matrix-raw/matrix.csv`: 240 samples plus header.
- `matrix-raw/*.csv`: one raw file per independent process.
- `summary.json`: machine-evaluated per-seed ratios and gates.

The candidate is rejected solely because a mandatory performance gate failed;
the Debug correctness suite was 44/44 and both model negative controls detected
their injected publication defects before Release measurement.
