# Adaptive Nibble Patricia Evidence

## Scope

This evidence measures the Cedar facts MemTable after the sparse 16-way,
path-compressed Patricia representation (`ff327de`, read-path coverage
`fdece4e`). The release benchmark support was present as working-tree changes
when these samples were collected. This is a MemTable measurement, not Cedar
transaction throughput.

The implementation remains a pure immutable Patricia Radix: fixed normalized
40-byte keys, sparse immutable `ChildTable` snapshots, CAS-only publication,
and no reclamation before owning MemTable destruction. Source audit found no
lock, SkipList/Vector/ART fallback, staging buffer, or background writer in
`cedar_pure_radix_index.{h,cc}`.

## Verification

- `git diff --check` passed before evidence generation.
- Sequential and child-table interleaving models passed with their negative
  controls.
- Debug `PartitionedVersionRadix`: 35/35 passed after the representation,
  publication, and sparse read-path changes.
- ASan `PartitionedVersionRadix`: 34/34 passed with `halt_on_error=1`.
- TSan `PartitionedVersionRadix`: 34/34 passed with `halt_on_error=1` and no
  race report.
- Direct lifecycle/WAL/recovery tests passed: lifecycle 11/11, crash matrix
  6/6, format recovery 4/4. Direct execution is used because Homebrew CMake
  GoogleTest discovery is incompatible with the installed GoogleTest 1.17
  JSON protocol.
- The benchmark CSV contract passed: standard output remains 28 fields and
  `--stats` emits `branch_loads`, `table_snapshot_cas`, and
  `boundary_candidates` on stderr only.

## Raw Matrix

`matrix-raw/matrix.csv` has 1,215 `phase=all` rows: all Vector, SkipList,
and Radix combinations across three seeds, 1024/16384/131072 entries,
ascending/random/versions workloads, 1/4/8 writers, and five independent
processes per cell. All rows have 29 fields including implementation, the
expected operation count, zero errors, and one matching result hash per
workload/seed/size/writer group.

`matrix-raw/index-*.csv` and `matrix-raw/memtable-*.csv` contain the 20
phase-split B0 samples. `matrix-raw/4-*.csv` and `matrix-raw/8-*.csv` contain
the 20 direct B1 samples. `matrix.csv` records their medians.

## Gates

| Gate | Requirement | Result |
| --- | --- | --- |
| B0 index, 1 writer | Diagnostic context | Radix 35.557 ms / Vector 1.360 ms = 26.145x |
| B0 memtable, 1 writer | Radix <= 1.10 * Vector | **Fail**: 53.359 ms / 14.308 ms = 3.729x |
| B1, 4 writers | Radix <= 1.05 * SkipList | **Time pass**: 23.933 ms / 28.017 ms = 0.854x |
| B1, 8 writers | Radix <= 1.05 * SkipList | **Time pass**: 24.930 ms / 26.477 ms = 0.942x |
| Arena, 1 writer | <= 1.25 * retained binary Radix | Pass: 23.530 MB / 19.211 MB = 1.225x |
| Arena, 4 writers | <= 1.25 * retained binary Radix | **Fail**: 25.110 MB / 19.211 MB = 1.307x |
| Arena, 8 writers | <= 1.25 * retained binary Radix | **Fail**: 25.154 MB / 19.211 MB = 1.309x |
| Correctness | Expected operations, zero errors, equal hashes | Pass |

The retained binary baseline arena values are from
`docs/superpowers/evidence/2026-09-20-cedar-pure-radix-matrix-final/` on the
same 131072-random workload. The full matrix's `phase=all` rows are not used
for B0/B1, because they include scan and frozen-chain work.

## Decision

`PURE-RADIX-CONTINUE`: the sparse nibble representation preserves correctness
and the contended time gate, but it does not satisfy B0 and exceeds the
contended arena budget. The next approved experiment must reduce immutable
snapshot-table allocation/copy cost or change the Patricia representation
while retaining CAS-only publication, fixed 40-byte keys, and WAL/recovery
semantics. It must begin with concurrent visibility and fresh B0/B1 tests.

Do not add a write staging vector, SkipList/ART fallback, lock, or hidden
writer to make this comparison pass. The goal remains active until B0 passes
or the user explicitly approves `ACCEPTANCE-REBASELINE`.
