# Cedar Columnar v2 Release Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the Columnar v2 and Blob v2 release-gate gaps without changing the existing on-disk format or weakening allocation/error contracts.

**Architecture:** Keep `page_format.cc` as the single portable page codec and add a checked encode boundary that returns `StatusOr<std::string>`. Existing callers migrate to the checked API; tests use the existing GoogleTest binary with deterministic `std::mt19937_64` seeds. Blob concurrency and randomized bitemporal equivalence remain separate tasks so each can be verified independently.

**Tech Stack:** C++17, GoogleTest, POSIX file APIs, existing LZ4 and CRC32C implementations, no new dependencies.

## Global Constraints

- Preserve `kPageFormatVersion`, header layout, encoding IDs, compression IDs, and existing valid bytes.
- Reject allocation beyond `kHardMaxPageBytes` before codec/compressor allocation.
- Preserve explicit corruption, unsupported-format, schema-mismatch, and resource-limit statuses.
- Every randomized failure prints its fixed seed and case index.
- Run the focused test first, then the complete normal and sanitizer matrices after the phase.

---

### Task 1: Checked page encoding boundary

**Files:**
- Modify: `include/cedar/columnar/page_format.h`
- Modify: `src/columnar/page_format.cc`
- Modify: `src/columnar/granule_block.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Add `StatusOr<std::string> EncodePageChecked(PageHeader header, const std::string& payload)`.
- Keep `EncodePage` only as an internal compatibility wrapper until all production/test call sites migrate; it must not be used by production code after this task.

- [ ] Write `PageFormatTest.RejectsOversizedPageBeforeCodecAllocation` with a payload of `kHardMaxPageBytes + 1`; assert `IsQueryMemoryLimit()` or the page resource-limit status selected by the existing status taxonomy.
- [ ] Run the focused test and confirm it fails because the current `EncodePage` returns bytes instead of an error.
- [ ] Implement preflight checks for raw payload size, fixed-width codec input shape, encoded-size bound, compressor input `INT_MAX` bound, and final header-plus-payload bound; migrate GranuleBlock production encoding.
- [ ] Run the focused test and the existing page/granule tests; confirm all pass.

### Task 2: Golden and deterministic codec matrix

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Exercise `EncodePageChecked`, `DecodePage`, `EncodePageDirectory`, and `DecodePageDirectory` without adding a second codec implementation.

- [ ] Add fixed golden-byte assertions for one header, one directory, and one page per valid encoding/compression combination.
- [ ] Add deterministic `std::mt19937_64` cases for Bool, Int32, Int64, Float32, Float64, Timestamp64, String, and Binary values; verify round-trip bytes and type metadata.
- [ ] Add explicit unsupported-shape cases for Delta payloads that are not complete 64-bit lanes and RLE malformed runs.
- [ ] Add mutations for truncation, one-bit payload changes, invalid offsets/lengths, unknown required flags, unknown compression, bad CRC, and decoded-size mismatch; assert typed status classes.
- [ ] Run the matrix before implementation changes to establish RED for every newly asserted contract, then run it green after implementation.

### Task 3: Blob concurrent deduplication and pinned relocation

**Files:**
- Inspect/modify: `include/cedar/blob/blob_store.h`, `src/blob/blob_store.cc`
- Inspect/modify: `include/cedar/blob/blob_gc.h`, `src/blob/blob_gc.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Preserve `BlobStore::Put`, `Get`, `Rotate`, `CollectBlobGarbage`, and `BlobReferenceCatalog` public contracts.

- [ ] Add a barrier-controlled concurrent equal-content write test and assert one authoritative hash mapping and readable content after reopen.
- [ ] Add a long-lived snapshot pin while GC relocates live hashes and a concurrent writer rotates segments; assert old and new references remain readable until pin release.
- [ ] Add cancellation/failure tests around relocation before and after mapping CAS; assert grants, descriptors, temporary bytes, and catalog pins return to baseline.
- [ ] Implement only the synchronization or lifetime fixes exposed by RED tests, preserving manifest-owned retirement.

### Task 4: Randomized multi-SST/schema bitemporal equivalence

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Inspect/modify: `src/storage/storage_shard.cc`, `src/columnar/sst_v2.cc`, `src/columnar/temporal_read_merger.cc`

**Interfaces:**
- Use the existing independent scalar oracle under `tests/model/` and existing `CedarDatabaseV2` durable APIs.

- [ ] Generate fixed-seed out-of-order PUT/DELETE/resurrection histories across multiple schema epochs and multiple flushes.
- [ ] Compare point, range, change, edge visibility, provenance, and path results before flush, after each flush/compaction, and after reopen.
- [ ] Assert complete source participation beyond ten SSTs and preserve all tombstones and historical versions.
- [ ] Fix any production mismatch found by the randomized RED corpus, then run focused oracle tests under all sanitizer builds.

### Task 5: Columnar/Blob/compaction resource and performance metrics

**Files:**
- Inspect/modify: `include/cedar/observability/explain_analyze_profile.h`, `src/observability/explain_analyze_profile.cc`
- Inspect/modify: `include/cedar/columnar/sst_v2.h`, `src/columnar/sst_v2.cc`
- Inspect/modify: `include/cedar/blob/blob_store.h`, `src/blob/blob_store.cc`
- Inspect/modify: `include/cedar/benchmark/artifact_writer.h`, `src/benchmark/artifact_writer.cc`, `src/benchmark/workload_driver.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Extend existing typed runtime/profile counters; do not add parallel metric registries.

- [ ] Add counters for page bytes decoded/skipped, Blob payload bytes read/written/deduplicated, compaction Blob payload reads, GC live/rewritten bytes, and page/Blob lookup latency samples.
- [ ] Attribute each counter to the existing operator/profile/artifact schema and preserve unavailable-versus-zero semantics.
- [ ] Add artifact assertions that every new numerator/denominator is present for a verified workload and undefined only when no physical sample exists.
- [ ] Run the Columnar/Blob focused suite, normal CTest, ASAN, UBSAN, and TSAN; update the progress ledger with exact counts.

## Verification Commands

```bash
cmake -S . -B build-v2
cmake --build build-v2 -j4 --target test_correctness_kernel
build-v2/tests/test_correctness_kernel --gtest_filter='PageFormatTest.*:GranuleBlockTest.*:SstV2Test.*:DurableLogTest.Blob*'
ctest --test-dir build-v2 --output-on-failure -j4
```

## Plan Self-Review

- No task changes the page format version or silently substitutes a fallback.
- Every production behavior task has a preceding RED test and a focused GREEN command.
- Randomized tests use only deterministic standard-library facilities and report seeds.
- Resource metrics are wired through existing profile/artifact ownership rather than a duplicate path.
