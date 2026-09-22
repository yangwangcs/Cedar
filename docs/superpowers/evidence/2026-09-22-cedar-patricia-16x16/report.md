# Sixteen-By-Sixteen Byte Patricia Release Comparison

Date: 2026-09-22

## Decision

The 16-by-16 candidate is rejected. The summarizer exited `11`. It passes
the one-writer Byte gate and the four-writer SkipList gate for every seed,
but fails both the eight-writer SkipList gate and the Arena gate for every
seed. The candidate therefore must be reverted and the goal returns to design
review; these results do not authorize another representation.

## Source And Build Identity

| Item | Candidate |
| --- | --- |
| Source revision | `36c3254d82fda288e841805d709aa7f2c0cd0d70` |
| Revision subject | `perf: publish sixteen-segment byte patricia` |
| Benchmark SHA-256 | `4c4ea921641adcd529a995cfb1fa0e3e9bf398e77b4134a6c65023dc6594fa53` |
| Build directory | `/Volumes/E/CedarBuild/patricia-retention-16x16-o3-20260922` |

The candidate, Nibble, and four-by-64 Byte benchmark sources have identical
SHA-256 values. The candidate outer cache records `CMAKE_BUILD_TYPE=Release`,
`CMAKE_CXX_FLAGS_RELEASE=-O3 -DNDEBUG`, and
`CMAKE_CXX_COMPILER=/usr/bin/c++`, matching the controls. The shared
project-controlled RocksDB cache is `RelWithDebInfo` with `-O2 -g -DNDEBUG`,
as it was for the controls.

## Matrix Validation

The timed matrix uses 131072 random keys, seeds `20260920`, `20260921`, and
`20260922`, five independent processes per seed/configuration, and no
`--stats`. `matrix-raw/matrix.csv` has 241 lines: one 29-field header and 240
29-field samples across 48 five-sample groups. Every row records 131072
operations and zero errors. The runner verified one result hash within every
seed/phase/writer group before accepting the rows.

The runner writes `<output>/matrix-raw`; an empty temporary output root was
therefore used and its generated directory was installed here. This avoids
the extra nesting implied by passing the planned child directory directly.

## Per-Seed Medians

Times are `phase=memtable` medians in ns. Arena values are median bytes.

| Seed | Candidate 1w | Byte 1w | Ratio | Candidate 4w | SkipList 4w | Ratio | Candidate 8w | SkipList 8w | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20260920 | 29096791 | 31077542 | 0.936 | 11755208 | 18048167 | 0.651 | 16524000 | 18744083 | 0.882 |
| 20260921 | 29165833 | 30511667 | 0.956 | 11828125 | 17567083 | 0.673 | 16900417 | 17391625 | 0.972 |
| 20260922 | 30380250 | 31792792 | 0.956 | 12270416 | 19243083 | 0.638 | 18589334 | 18754833 | 0.991 |

The gates are `<= 1.05` for one writer, `<= 0.80` for four writers, and
`<= 0.85` for eight writers. One writer and four writers pass on every seed.
Eight writers fail on every seed with exact ratios `0.881558`, `0.971756`,
and `0.991176`. The historical candidate-versus-Vector `<= 1.10x` criterion
also fails on every seed and is not claimed as met.

| Seed | Candidate Arena 1w/4w/8w | Nibble Arena 1w/4w/8w | Candidate/Nibble 1w/4w/8w |
| --- | --- | --- | --- |
| 20260920 | 21934192 / 23502256 / 23533880 | 16369832 / 17442080 / 17455752 | 1.340 / 1.347 / 1.348 |
| 20260921 | 21937552 / 23503080 / 23529088 | 16370216 / 17452896 / 17457800 | 1.340 / 1.347 / 1.348 |
| 20260922 | 21933664 / 23495528 / 23537928 | 16370344 / 17451616 / 17457480 | 1.340 / 1.346 / 1.348 |

All nine Arena ratios exceed the `<= 1.25x` gate. The candidate reduces
Arena use substantially from four-by-64 Byte's roughly 50.1-51.4 MB, but its
roughly 21.9-23.5 MB remains too large relative to Nibble's 16.4-17.5 MB.

## Untimed Correctness And Diagnostics

The candidate's 1024-key `phase=all` smoke runs at 1, 4, and 8 writers each
emitted 28 fields, 1024 operations, zero errors, and the same result hash
`16340194407465152223`. The 131072-key eight-writer diagnostic emitted 28
fields, zero errors, and result hash `454339457082336225`, matching both
existing controls.

| Variant | Arena bytes | Peak RSS bytes | Branch loads | Segment snapshot CAS | Boundary candidates |
| --- | ---: | ---: | ---: | ---: | ---: |
| Candidate 16-by-16 | 23490384 | 30654464 | 392363 | 131145 | 784366 |
| Nibble control | 17457120 | 24707072 | 637896 | 131087 | 1275684 |
| Four-by-64 control | 51254968 | 58621952 | 392850 | 131302 | 784366 |

An immutable candidate block contains at most 16 child pointers, while a
four-by-64 control block contains at most 64. These are representation-level
copy upper bounds. The CAS counters above count publication attempts; they do
not measure successful copied-child counts and are not used as such.

## Selected Path

Summarizer command:

```bash
python3 benchmarks/summarize_cedar_patricia_retention.py \
  --matrix docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16/matrix-raw/matrix.csv \
  --candidate candidate \
  --output docs/superpowers/evidence/2026-09-22-cedar-patricia-16x16/summary.json
```

Exit code: `11`.

The 16-by-16 candidate is not retained. Its implementation and associated
16-by-16 contracts are reverted with new commits after preserving this
evidence.
