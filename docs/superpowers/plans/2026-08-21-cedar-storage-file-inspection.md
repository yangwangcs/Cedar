# Cedar Storage File Inspection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide `cedar files` and a public read-only API that explain every live SST without changing RocksDB file ownership.

**Architecture:** A short-lived read-only engine operation opens Cedar's three required column families with their existing table factories, converts `LiveFileMetaData` to Cedar values, then closes. The CLI only formats these values.

**Tech Stack:** C++20, embedded RocksDB `OpenForReadOnly`, GoogleTest, CMake/CTest.

## Global Constraints

- Keep `.sst`, one WAL, and RocksDB ownership of recovery, MemTables, VersionSet, MANIFEST, and file lifecycle.
- Inspection must not create or mutate a database, start Cedar threads, schedule maintenance, or expose RocksDB public types.
- `facts` maps to `authoritative-facts/CedarParquet`; `meta` to `transaction-metadata/BlockBased`; `default` to `engine-internal/BlockBased`.
- Derive files exclusively from `GetLiveFilesMetaData`, ordered by CF, level, and relative filename.

---

### Task 1: Public inspection model

**Files:**
- Create: `include/cedar/storage_files.h`
- Modify: `include/cedar/database.h`
- Test: `tests/public/test_kernel_interface.cc`

**Produces:** `StorageFileRole`, `StorageTableFormat`, `StorageFileInfo`, `StorageFileInspectionOptions`, and `StatusOr<std::vector<StorageFileInfo>> InspectStorageFiles(StorageFileInspectionOptions)`.

- [ ] Write a failing public-contract test that includes `cedar/storage_files.h`, requires `InspectStorageFiles(options)` to have the exact return type, and confirms no RocksDB include is needed.
- [ ] Run `cmake --build build-debug --target test_kernel_interface && ctest --test-dir build-debug -R '^KernelInterfaceTest\\.' --output-on-failure`; expect compile failure because the header and function are missing.
- [ ] Create the public header with `<cstdint>`, `<string>`, `<vector>`, `cedar/core/status.h`, and `cedar/storage_options.h`; place no engine type in it. Include the header from `cedar/database.h`.
- [ ] Re-run the focused contract test; expect PASS.
- [ ] Commit with message `feat: define Cedar storage file inspection API`.

### Task 2: Read-only live-file translation

**Files:**
- Create: `src/storage/rocks/storage_file_inspection.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/storage/test_rocksdb_lifecycle.cc`

**Consumes:** Task 1 public types and existing `ResolveStorageProfile`, `MakeRocksDbOptions`, and `MakeRocksDbColumnFamilyDescriptors`.

- [ ] Write a failing integration test that creates and flushes a temporary Cedar database, closes it, snapshots directory filenames and sizes, calls `InspectStorageFiles({.path = path_})`, asserts a `facts` item has role `kAuthoritativeFacts` and format `kCedarParquet`, asserts deterministic order, then asserts the directory snapshot is unchanged.
- [ ] Run `cmake --build build-debug --target test_rocksdb_lifecycle && ctest --test-dir build-debug -R 'Inspection' --output-on-failure`; expect link failure because the API has no definition.
- [ ] Implement a local source-only adapter: reject empty paths; require `CURRENT`; list and validate exactly `default`, `facts`, `meta`; create Cedar options/descriptors; set `create_if_missing=false`; call `DB::OpenForReadOnly(DBOptions(options), path, descriptors, &handles, &db, false)`; call `GetLiveFilesMetaData`; map the three known CF names; lowercase-hex encode binary key bounds; sort with `std::tie(cf, level, filename)`; destroy all CF handles before DB destruction on every post-open exit.
- [ ] Add the source to `cedar_core`, re-run the focused test, then run `ctest --test-dir build-debug -R '^(RocksDbLifecycleTest|CrashMatrixTest)\\.' --output-on-failure`; expect PASS.
- [ ] Commit with message `feat: inspect live Cedar storage files read-only`.

### Task 3: `cedar files` frontend

**Files:**
- Create: `tools/cedar_main.cc`
- Create: `tests/tools/test_cedar_files.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Consumes:** `InspectStorageFiles` from Task 2.

- [ ] Write failing end-to-end tests against a temporary flushed database. `cedar files --path DB_PATH` must contain `FILE`, `facts`, `authoritative-facts`, and `CedarParquet`; `--json` must produce one object with a `files` array and all public fields; both must leave the directory unchanged; missing `--path` must fail.
- [ ] Run `cmake --build build-debug --target test_cedar_files && ctest --test-dir build-debug -R '^CedarFilesTest\\.' --output-on-failure`; expect a missing-target or missing-binary failure.
- [ ] Add executable `cedar` linked only to `cedar_core`. It accepts exactly `cedar files --path DB_PATH [--json]`; invalid arguments return 2, inspection failures return 1. Render text columns `FILE CF ROLE FORMAT LEVEL SIZE SEQ`; emit compact valid JSON using local JSON escaping and no trailing comma. Do not scan the directory or call `Database::Open`.
- [ ] Register the CLI test target and pass `$<TARGET_FILE:cedar>` to it. Re-run the focused CLI suite; expect PASS.
- [ ] Run `cmake --build build-debug -j4 && ctest --test-dir build-debug --output-on-failure`; then build ASAN and run `KernelInterfaceTest`, `RocksDbLifecycleTest`, and `CedarFilesTest`; then build Release and run `CedarFilesTest`. All must pass.
- [ ] Commit with message `feat: add cedar files inspection command`.

## Plan Self-Review

- Every specification requirement maps to Tasks 1-3, including the true read-only opening path and unchanged-directory checks.
- Each production task is preceded by a focused failing test and followed by focused and adjacent verification.
- The public signatures named in Tasks 2-3 are defined exactly in Task 1.
