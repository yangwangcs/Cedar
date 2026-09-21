# Segmented Nibble Patricia Evidence

## Source And Configuration

This evidence measures commit `6bfffab` plus its retained segmented immutable
Patricia representation. Every branch has four independently CAS-published
four-nibble `ChildBlock` slots. The benchmark uses the existing calibrated
`RelWithDebInfo` configuration (`-O2 -DNDEBUG`) at
`/Volumes/E/CedarBuild/pure-radix-cas-cursor-release-exact-20260921`.

The index remains a pure path-compressed Patricia Radix with fixed normalized
40-byte keys, immutable arena-owned payloads, and release-CAS publication. No
fallback index, lock, staging buffer, worker, WAL, recovery, or durable-format
change was introduced.

## Correctness And Safety

- Debug `PartitionedVersionRadix`: 38/38 passed.
- ASan `PartitionedVersionRadix`: 38/38 passed with `halt_on_error=1`.
- TSan `PartitionedVersionRadix`: 38/38 passed with `halt_on_error=1`.
- Sequential and interleaving Python models passed with negative controls.
- New contracts cover local packed rank, every segment-boundary Seek and
  SeekForPrev, direct/wrapper publication across distinct segments, and stale
  same-segment retry.
- `--stats` keeps the standard 28-column stdout CSV and emits
  `branch_loads,segment_snapshot_cas,boundary_candidates` only on stderr.

## Release Samples

Raw files are under `matrix-raw/`: five independent processes per row, 131072
random keys, seed 20260921, expected operations, zero errors, and one matching
hash (`454339457082336225`). `matrix.csv` records elapsed and arena medians.

| Gate | Requirement | Result |
| --- | --- | --- |
| B0 index, 1 writer | Diagnostic context | Radix 25.267 ms / Vector 1.220 ms = 20.705x |
| B0 memtable, 1 writer | Radix <= 1.10 * Vector | Fail: 38.469 ms / 13.171 ms = 2.921x |
| B1 4 writers | Radix <= 1.05 * SkipList | Pass: 16.172 ms / 19.564 ms = 0.827x |
| B1 8 writers | Radix <= 1.05 * SkipList | Pass: 16.026 ms / 17.617 ms = 0.910x |
| Arena 1 writer | <= retained binary Radix 19.211 MB | Pass: 16.370 MB = 0.852x |
| Arena 4 writers | <= retained binary Radix 19.211 MB | Pass: 17.429 MB = 0.907x |
| Arena 8 writers | <= retained binary Radix 19.211 MB | Pass: 17.451 MB = 0.908x |

For B0 context, the previously retained pure-Radix candidate was 42.577 ms
versus 13.525 ms Vector (3.148x). The segmented representation is a 9.65%
improvement in the current calibrated Radix memtable median and repairs the
four/eight-writer Arena regression, but it does not meet the required B0 gate.

## Decision

`PURE-RADIX-CONTINUE`: retain the segmented block representation because it
preserves safety, retains B1, fixes the Arena budget, and improves B0. The
objective remains active because 2.921x is above the required 1.10x B0 gate.
The next experiment must target remaining per-insert Patricia work without
introducing a staging vector, fallback index, lock, or worker.
