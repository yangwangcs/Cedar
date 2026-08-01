# Cedar RocksDB Stage D Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the RocksDB kernel the only production Cedar path, delete obsolete canonical storage/query coupling, and close correctness, recovery, sanitizer, and performance gates.

**Architecture:** First split build targets and focused tests while both implementations still compile. Then switch installed/public targets to the new kernel and delete the complete legacy closure in dependency order. Finish with source inventory and whole-system verification proving no hidden durable path remains.

**Tech Stack:** C++20, CMake/CTest, RocksDB v11.1.2, ASAN, UBSAN, TSAN.

## Global Constraints

- Follow the master plan's Global Constraints.
- Do not preserve source compatibility shims for old `CedarDatabase::Put/Delete` or T-Cypher entry points.
- Do not leave dead legacy source in the production repository after final inventory.

---

### Task D1: Split Kernel, Projection, Query, and Tool Build Targets

**Files:**
- Replace source aggregation in: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Create: `cmake/CedarTargets.cmake`

- [ ] Write a build-contract test that inspects target link interfaces and installed headers.
- [ ] Create `cedar_core` from core/types/fact/kernel only, `cedar_projection` from derived artifacts, and temporary optional legacy targets for deletion work.
- [ ] Prove `cedar_core` does not link query, optimizer, benchmark, observability, custom storage, or legacy transaction objects.
- [ ] Commit as `build: isolate Cedar embedded kernel`.

### Task D2: Replace the Monolithic Correctness Test

**Files:**
- Delete after extraction: `tests/test_correctness_kernel.cc`
- Create focused tests under: `tests/core`, `tests/fact`, `tests/kernel`, `tests/projection`, `tests/recovery`
- Modify: `tests/CMakeLists.txt`

- [ ] Inventory every legacy test and map relevant clean-break behavior to a focused target or explicit rejection.
- [ ] Move shared bitemporal oracle/helpers into `tests/model` without production friend access.
- [ ] Run focused tests individually and through CTest.
- [ ] Delete the monolith only after the mapping has no uncovered new-kernel requirement.
- [ ] Commit as `test: split Cedar kernel verification`.

### Task D3: Publish New FORMAT and Read-Only Legacy Rejection

**Files:**
- Create: `include/cedar/format.h`
- Create: `src/kernel/format.cc`
- Delete after replacement: `include/cedar/transaction/database_format.h`, `src/transaction/database_format.cc`
- Create: `tests/recovery/test_format.cc`

- [ ] Write RED tests for new database identity/options, matching reopen, corrupted metadata, future version, and old Cedar directory rejection without any file mutation.
- [ ] Make RocksDB `meta/format/current` authoritative; an outer small marker may identify the engine before Open but contains no WAL/Manifest paths.
- [ ] Commit as `feat: publish RocksDB Cedar format`.

### Task D4: Delete Legacy Canonical Storage Closure

**Files:**
- Delete: `include/cedar/transaction/decision_log.h`, `src/transaction/decision_log.cc`
- Delete: `include/cedar/transaction/transaction_coordinator.h`, `src/transaction/transaction_coordinator.cc`
- Delete legacy storage: `include/cedar/storage/*`, `src/storage/*`, canonical `include/cedar/columnar/sst.h`, `src/columnar/sst.cc`, old granule canonical glue
- Delete: `include/cedar/blob/*`, `src/blob/*`
- Delete old database facade: `include/cedar/db/cedar_database.h`, `src/db/cedar_database.cc`
- Modify remaining includes/build/tests.

- [ ] Before deletion, run all new kernel/recovery/projection tests GREEN.
- [ ] Delete the dependency closure rather than leaving aliases or unused files.
- [ ] Run `rg` inventory for `PrepareRecord|PrepareReference|DecisionLog|ShardPrepareLog|TemporalMemTable|VersionSet|CedarDatabase` and require zero production matches.
- [ ] Commit as `refactor: remove legacy Cedar storage engine`.

### Task D5: Remove Query and Research Runtime from the Kernel Repository Path

**Files:**
- Delete or move outside installed targets: `include/cedar/tcypher`, `src/tcypher`, `include/cedar/optimizer`, `src/optimizer`, `include/cedar/statistics`, `src/statistics`, old `include/cedar/runtime`, `src/runtime`, `include/cedar/cache`, `src/cache`, production observability and benchmark sources.
- Modify: `README.md`, `CMakeLists.txt`, install/export files.

- [ ] Preserve only projection-neutral public Snapshot/Scan interfaces.
- [ ] Delete circular dependencies and source that cannot compile against the new seam.
- [ ] Keep benchmark executables only if they consume the public kernel interface; otherwise replace them with focused kernel benchmarks.
- [ ] Commit as `refactor: reduce Cedar to embedded kernel`.

### Task D6: Recovery, Sanitizer, Source, and Performance Closure

**Files:**
- Create: `tests/recovery/test_crash_matrix.cc`
- Create: `tests/recovery/test_projection_rebuild.cc`
- Create: `cmake/VerifyRocksKernelSourceContract.cmake`
- Create: `benchmarks/cedar_kernel_bench.cc`
- Modify: `README.md`, `third_party/CODECS.md`

- [ ] Run Debug and Release CTest from clean build directories.
- [ ] Run ASAN, UBSAN, and TSAN complete focused suites.
- [ ] Run deterministic process-crash/reopen matrix for commit, metadata lease, Vacuum, and projection publication.
- [ ] Run source contract proving standard RocksDB is the sole canonical engine and no forbidden legacy symbol/path exists.
- [ ] Run durable transaction and temporal point-read benchmarks; compare to the pre-migration baseline without weakening `sync=true`.
- [ ] Delete Visual Companion/session artifacts from local state only if requested; they are already gitignored.
- [ ] Commit final evidence/documentation as `test: close RocksDB kernel migration`.

## Completion Audit

For every item in the approved design's Verification section, record the exact test/source/command evidence. The migration is not complete merely because new tests pass: the build graph and source inventory must prove the old canonical engine and public entry points are gone.
