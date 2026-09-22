# Path Frame Initialization Evidence

## Change

`InsertWithBorrowedKey` previously value-initialized all 80 stack
`TraversalFrame` slots before every insertion. Descent writes each frame before
incrementing `depth`, and every consumer reads only `[0, depth)`. The retained
change leaves the unused tail uninitialized; it does not change the frame
layout, branch/leaf publication, CAS orders, allocation, or reader behavior.

## Validation

- Debug `PartitionedVersionRadix`: 38/38 passed.
- ASan `PartitionedVersionRadix`: 38/38 passed with `halt_on_error=1`.
- TSan `PartitionedVersionRadix`: 38/38 passed with
  `halt_on_error=1:report_bugs=1` and no race report.
- Sequential and interleaving publication models passed with their negative
  controls.
- Lifecycle, WAL, crash recovery, and format recovery: 21/21 passed.
- The Radix `--stats` contract retained 28 stdout columns, 131072 operations,
  zero errors, the expected hash, and the stderr-only diagnostic record.

## Release Samples

The calibrated `RelWithDebInfo` build (`-O2 -DNDEBUG`) ran five independent,
alternating-order processes for each row: 131072 random keys, seed `20260921`.
All 40 raw rows are in `matrix-raw/`; every one has 28 columns, the expected
operation count, zero errors, and hash `454339457082336225`.

| Gate | Requirement | Result |
| --- | --- | --- |
| B0 index, 1 writer | Context | Radix 21.416 ms / Vector 1.218 ms = 17.580x |
| B0 memtable, 1 writer | Radix <= 1.10 * Vector | Fail: 34.976 ms / 12.997 ms = 2.691x |
| B1 4 writers | Radix <= 1.05 * SkipList | Pass: 11.819 ms / 17.208 ms = 0.687x |
| B1 8 writers | Radix <= 1.05 * SkipList | Pass: 14.781 ms / 17.995 ms = 0.821x |
| Arena 1 writer | No regression | Pass: 16.370 MB, unchanged |

Against the retained segmented-block baseline, the Radix medians improve from
25.267 ms to 21.416 ms in `index` (-15.2%) and from 38.469 ms to 34.976 ms in
`memtable` (-9.1%). The B0 Vector gate still fails, so this result retains the
optimization but does not complete the goal.

## Decision

`PURE-RADIX-CONTINUE`: retain unused frame-tail initialization removal because
the single-writer reduction is repeatable, concurrent performance remains
within the B1 gate, arena use is unchanged, and all safety checks pass. The
next experiment must target remaining structural Patricia insertion work while
preserving immutable nodes and CAS-only publication.
