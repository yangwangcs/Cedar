# Nibble vs Four-by-64 Byte Patricia Release Comparison

Date: 2026-09-22

## Decision

The Nibble candidate does not meet the retention gates. The summarizer exited
`11`: all three seeds exceed the one-writer `1.05x` Byte gate, and seed
`20260922` exceeds the eight-writer `0.85x` SkipList gate. Per the approved
design, this is a hard failure and does not authorize the 16-by-16 candidate.
The existing four-by-64 Byte representation remains unchanged; no
representation code was changed by this task.

## Source and Build Identity

| Item | Nibble | Four-by-64 Byte |
| --- | --- | --- |
| Source revision | `77593a87a64f03d7716318e3c9ce7bb672a78327` | `76adf448257aaaa4eacac8ba5451ba38a51223f3` |
| Revision subject | `test: preserve radix standalone entry pointer contract` | `perf: traverse segmented byte patricia blocks` |
| Benchmark SHA-256 | `4c4ea921641adcd529a995cfb1fa0e3e9bf398e77b4134a6c65023dc6594fa53` | `4c4ea921641adcd529a995cfb1fa0e3e9bf398e77b4134a6c65023dc6594fa53` |
| Build directory | `/Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922` | `/Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922` |

Both outer `CMakeCache.txt` files record `CMAKE_BUILD_TYPE=Release`,
`CMAKE_CXX_FLAGS_RELEASE=-O3 -DNDEBUG`, and
`CMAKE_CXX_COMPILER=/usr/bin/c++`. The shared project-controlled RocksDB
cache was built as `RelWithDebInfo` with `-O2 -g -DNDEBUG`; this applies to
both sides and is not presented as an all-O3 dependency build.

Before building, both required source diffs were empty: the benchmark source
is identical between Nibble and Byte, and `77593a8` differs from `0cd5d86`
only outside the Nibble production index and benchmark files.

## Matrix Validation

The timed matrix uses 131072 random keys, seeds `20260920`, `20260921`, and
`20260922`, five independent processes per seed/configuration, and no
`--stats`. `matrix-raw/matrix.csv` has 181 lines: one 29-field header and 180
29-field samples across 36 five-sample groups. Every timed row records 131072
operations and zero errors; no timed CSV contains `radix_stats`. Same-group
result hashes were checked by the runner before rows were accepted.

The runner contract writes `<output>/matrix-raw`; because the Task 2 prose
named the child directory as its output, the first completed collection was
placed one level too deep and was discarded to Trash. The final collection was
run with an empty temporary output root, then its generated `matrix-raw`
directory was installed here. This preserves the runner's validated interface
and the planned final evidence layout.

## Per-Seed Medians

Times are `phase=memtable` medians in ns. Arena and RSS are median bytes.

| Seed | Nibble 1w | Byte 1w | Nibble/Byte | Nibble 4w | SkipList 4w | Nibble/SkipList | Nibble 8w | SkipList 8w | Nibble/SkipList |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20260920 | 37696792 | 31346834 | 1.203 | 12974958 | 21494875 | 0.604 | 15747000 | 18534458 | 0.850 |
| 20260921 | 38189666 | 31468875 | 1.214 | 13845917 | 17651333 | 0.784 | 15821167 | 18947125 | 0.835 |
| 20260922 | 38052167 | 31890000 | 1.193 | 12819250 | 18335459 | 0.699 | 15981084 | 17777958 | 0.899 |

The gates are `<= 1.05` for one writer, `<= 0.80` for four writers, and
`<= 0.85` for eight writers. Four writers pass on all seeds; eight writers
passes on the first two seeds but fails on `20260922`. All one-writer gates
fail. The historical Nibble-versus-Vector `<= 1.10x` criterion also fails on
all three seeds and is not claimed as met.

| Seed | Nibble Arena 1w/4w/8w | Byte Arena 1w/4w/8w | Nibble RSS 1w/4w/8w |
| --- | --- | --- | --- |
| 20260920 | 16369832 / 17437296 / 17453528 | 50061536 / 51242760 / 51358536 | 23166976 / 24690688 / 24756224 |
| 20260921 | 16370216 / 17451728 / 17455592 | 50072616 / 51244304 / 51355488 | 23166976 / 24707072 / 24805376 |
| 20260922 | 16370344 / 17441352 / 17456720 | 50089032 / 51253360 / 51357672 | 23150592 / 24690688 / 24805376 |

Nibble's Arena ratio to itself is 1.0 at every seed/writer, so it satisfies
the `<= 1.25x` Arena gate. Its measured Arena remains about 16.4-17.5 MB,
versus Byte's about 50.0-51.4 MB.

## Untimed Diagnostics

Both 131072-key, eight-writer, `memtable --stats` runs emitted a 28-field CSV,
zero errors, and the same result hash `454339457082336225`.

| Variant | Arena bytes | Peak RSS bytes | Branch loads | Segment snapshot CAS | Boundary candidates |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nibble | 17457120 | 24707072 | 637896 | 131087 | 1275684 |
| Byte | 51254968 | 58621952 | 392850 | 131302 | 784366 |

The diagnostics establish that Byte's much larger Arena use is present in the
same workload, but they are not timing samples and do not establish a causal
explanation for the seed-specific eight-writer retention failure.

## Selected Path

Summarizer command:

```bash
python3 benchmarks/summarize_cedar_patricia_retention.py \
  --matrix docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab/matrix-raw/matrix.csv \
  --candidate nibble \
  --output docs/superpowers/evidence/2026-09-22-cedar-patricia-retention-ab/summary.json
```

Exit code: `11`.

No Nibble retention and no 16-by-16 implementation follow from this run. The
approved selection rule requires investigation/reporting at this point rather
than treating an incomplete gate result as a permission to change the
representation.
