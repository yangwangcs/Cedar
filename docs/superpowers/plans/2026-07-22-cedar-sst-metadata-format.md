# Cedar SST Metadata Format Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the current clean-break SST format self-identifying and self-describing, with complete format algorithm IDs, repeated file identity, bounded file statistics, and Manifest ownership validation.

**Architecture:** SST header/footer move together to one new numeric format version. A fixed header records partition identity plus sort/hash/encoding/compression/checksum registry IDs and a 256-bit BLAKE3 file identity. A checksummed variable file-statistics region records full key/time/commit ranges, row/value-class counts, and typed min/max metadata; the footer repeats identity/version and locates every metadata region. The fixed-size BlockIndex persists row counts and GranuleBlock identities, while each page directory persists the BLAKE3 hash of every encoded page. `SstMetadata` and `SstFileMeta` carry the same ownership fields through flush, compaction, Manifest persistence, reopen, and identity-scoped cache keys.

**Tech Stack:** C++17, portable little-endian encoding, BLAKE3-256, CRC32C, CMake, GoogleTest.

## Global Constraints

- Database format number remains 1; SST/subformat numeric versions and magic are format-validation fields.
- No decoder or migration shim for the prior SST/header/footer layout.
- Unknown required features and unknown algorithm IDs fail deterministically before data-page reads.
- SST open reads only header/footer and bounded metadata regions, never all data pages.
- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.

---

### Task 1: Public metadata contract

**Files:**
- Modify: `include/cedar/columnar/sst.h`
- Modify: `include/cedar/columnar/granule_block.h`
- Modify: `tests/test_correctness_kernel.cc`

- [x] Define stable numeric sort/hash/encoding/compression/checksum registry IDs.
- [x] Define `SstFileIdentity`, `SstFileStatistics`, and complete `SstMetadata` fields.
- [x] Add RED tests for descriptor IDs, nonzero repeated identity, row/value counts, complete logical-key/time/commit ranges, and typed statistics.

### Task 2: Header, statistics region, and footer migration

**Files:**
- Modify: `src/columnar/sst.cc`
- Modify: `tests/test_correctness_kernel.cc`

- [x] Encode the expanded portable header with all algorithm IDs and identity.
- [x] Encode a bounded, checksummed file Bloom/statistics region.
- [x] Encode the expanded footer with every metadata offset/length, row/block counts, identity/version, and checksums.
- [x] Update in-memory, file, cursor, ordinal, selective, cache, and streaming readers.
- [x] Update golden bytes and add corruption tests for every new ID, identity mismatch, region checksum, range, and count.
- [x] Reject wrapped footer arithmetic and over-limit metadata before allocation or I/O; derive the exact BlockIndex length from `block_count`.
- [x] Persist block/page content commitments so metadata-only open verifies the identity root and full/selective reads verify the bytes they consume.

### Task 3: Manifest ownership lifecycle

**Files:**
- Modify: `include/cedar/storage/version_set.h`
- Modify: `src/storage/version_set.cc`
- Modify: `src/storage/sst_flush.cc`
- Modify: `src/storage/sst_compaction.cc`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `tests/test_correctness_kernel.cc`

- [x] Persist SST identity, descriptor IDs, row count, key/time/commit ranges, and statistics checksum in `SstFileMeta`.
- [x] Populate exact metadata during flush and streaming compaction publication.
- [x] Validate Manifest-owned metadata against header/footer/statistics on reopen without reading data pages.
- [x] Reject mismatches and partial publication deterministically.

### Task 4: Verification and evidence

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-remaining-closure-goal.md`
- Modify: `.superpowers/sdd/progress.md`

- [x] Run normal full `-j1`.
- [x] Run format/metadata/reopen focused tests under ASAN, UBSAN, and TSAN.
- [x] Record exact evidence while keeping the six-design goal active for remaining Blob/schema/scheduler/query/benchmark work.

## Verification evidence

- Normal correctness kernel: 748/748.
- Focused SST/Manifest/page-format ownership gate: ASAN 74/74, UBSAN 74/74,
  TSAN 74/74.
- Current SST subformat: neutral internal magic `CSST/CSFT/CBIX`, numeric
  version 10, 80-byte header, bounded checksummed metadata regions, fixed-size
  hash-bearing BlockIndex entries, and a 136-byte footer. Granule/page-directory
  identity is `GBK5/5` and `PDR3`. The database format number remains 1.
- Metadata, page, and whole-block cache keys include the persisted file
  identity, so same-path/same-size replacement cannot reuse stale cache data.
- Manifest layout advanced cleanly to current magic `MSC1`, which owns the
  canonical schema catalog; no old Manifest
  magic recognizer or decoder remains.
