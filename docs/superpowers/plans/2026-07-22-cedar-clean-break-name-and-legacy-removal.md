# Cedar Clean-Break Naming and Legacy Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every legacy runtime and external `V2/Vn` name so Cedar has one canonical API, source tree, durable layout, and physical execution path.

**Architecture:** Establish the rejection contract before changing current paths. Then rename the metadata/value layer, storage layer, database API, and consumers in dependency order. Remove compatibility-only scheduler and T-Cypher paths only after production-path tests prove no accepted query depends on them.

**Tech Stack:** C++17, CMake, GoogleTest/CTest, Cedar binary formats, ASAN, UBSAN, TSAN, `rg`, `git diff --check`.

## Global Constraints

- The approved design is `docs/superpowers/specs/2026-07-22-cedar-clean-break-name-and-legacy-removal-design.md`.
- The current internal database format is `1` with version-neutral `CDFM` magic.
- Current durable paths are `manifest/MANIFEST`, `.sst`, and `.idx`.
- Old paths and formats are rejected without mutation; no migration, aliases, forwarding headers, or compatibility switches are permitted.
- Internal magic values, numeric format versions, feature bits, schema epochs, and benchmark protocol versions remain available for validation.
- Preserve LogicalKey, TemporalEvent, bitemporal visibility, durability, physical-query behavior, resource governance, and benchmark semantics.
- Production behavior changes use TDD with an observed RED before GREEN.
- Build and test with `-j1`.
- Preserve existing worktree changes. Do not reset, clean, roll back, stage, commit, or push.

---

### Task 1: Freeze the clean-break rejection contract

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `include/cedar/transaction/database_format.h`
- Modify: `src/transaction/database_format.cc`

**Interfaces:**
- Consumes: existing `ReadDatabaseFormat`, `CreateOrValidateDatabaseFormat`, and `TransactionCoordinator::Open`.
- Produces: strict tests for old `FMT2`, format number `2`, `MANIFEST-V2`, `.sst2`, and `.idx1` layouts with directory non-mutation evidence.

- [x] **Step 1: Add a deterministic directory snapshot helper**

Add this test-only helper near the existing fixture helpers:

```cpp
std::vector<std::string> SnapshotRelativeTree(const std::string& root) {
  std::vector<std::string> entries;
  if (!std::filesystem::exists(root)) return entries;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
    const auto relative = std::filesystem::relative(entry.path(), root).generic_string();
    entries.push_back((entry.is_directory() ? "d:" : "f:") + relative);
  }
  std::sort(entries.begin(), entries.end());
  return entries;
}
```

- [x] **Step 2: Add failing old-layout tests**

Add tests with these exact contracts:

```cpp
TEST_F(DurableLogTest, DatabaseRejectsOldFormatMagicWithoutMutatingDirectory);
TEST_F(DurableLogTest, DatabaseRejectsOldManifestLocationWithoutMigration);
TEST_F(DurableLogTest, DatabaseRejectsOldSstAndSidecarPathsWithoutCleanup);
```

Each test records `SnapshotRelativeTree(path_)` immediately before `Open()`, expects `IsNotSupportedError()`, and compares the complete tree afterward.

- [x] **Step 3: Run the RED tests**

Run:

```bash
cmake --build build-v2 -j1 --target test_correctness_kernel
./build-v2/tests/test_correctness_kernel --gtest_filter='DurableLogTest.DatabaseRejectsOld*'
```

Expected: at least the old magic/path cases fail because the current runtime still accepts or classifies the old layout incorrectly.

- [x] **Step 4: Establish canonical FORMAT identity**

Change the public type and factory in the later rename task, but establish these exact values now:

```cpp
constexpr uint32_t kCedarDatabaseFormatVersion = 1;
constexpr uint32_t kFormatMagic = 0x4d464443U;  // CDFM
constexpr uint32_t kOldFormatMagic = 0x32544d46U;  // FMT2
```

Decode `kOldFormatMagic` as `NotSupported`. Unknown magic remains `Corruption`. Validate old Manifest locations as `NotSupported` before any directory creation or recovery cleanup.

- [x] **Step 5: Run focused GREEN tests**

Run the three new tests plus:

```text
DurableLogTest.CoordinatorCreatesAndValidatesPersistentDatabaseFormat
DurableLogTest.CoordinatorRejectsUnknownPersistentFormatVersion
DurableLogTest.CoordinatorRejectsNonemptyLegacyDirectoryWithoutFormat
DurableLogTest.VersionSetRejectsLegacyManifestWithoutMigration
```

Expected: all pass and directory snapshots are identical before and after rejection.

### Task 2: Centralize canonical durable layout names

**Files:**
- Create: `include/cedar/storage/storage_layout.h`
- Modify: `src/transaction/database_format.cc`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `src/storage/version_set_v2.cc`
- Modify: `src/storage/sst_flush_v2.cc`
- Modify: `src/storage/sst_compaction_v2.cc`
- Modify: `src/index/index_catalog.cc`

**Interfaces:**
- Produces: one source of truth for current and recognizable-old durable paths.

- [x] **Step 1: Add compile-time layout constants**

Create:

```cpp
namespace cedar::storage_layout {
inline constexpr char kManifestRelativePath[] = "manifest/MANIFEST";
inline constexpr char kOldManifestRelativePath[] = "manifest/MANIFEST-V2";
inline constexpr char kSstExtension[] = ".sst";
inline constexpr char kOldSstExtension[] = ".sst2";
inline constexpr char kIndexExtension[] = ".idx";
inline constexpr char kOldIndexExtension[] = ".idx1";
inline constexpr char kTemporarySuffix[] = ".tmp";
}
```

- [x] **Step 2: Add RED assertions for canonical paths**

Update creation, flush, compaction, index publication, orphan cleanup, and reopen tests to expect `MANIFEST`, `.sst`, and `.idx`. Run the affected tests and observe failures on old paths.

- [x] **Step 3: Replace production path construction**

Use the constants in database format creation, coordinator construction, SST flush/compaction, VersionSet path validation, index fragment publication, temp cleanup, and orphan cleanup. Do not search old extensions as candidates for cleanup.

- [x] **Step 4: Verify current and old path behavior**

Run all tests matching:

```bash
./build-v2/tests/test_correctness_kernel --gtest_filter='*Manifest*:*Sst*:*Index*Publication*:*Orphan*:*Reopen*'
```

Expected: current paths pass; old paths return `NotSupported` without mutation.

### Task 3: Rename core metadata and Blob reference types

**Files:**
- Modify: `include/cedar/blob/blob_store.h`
- Modify: `src/blob/blob_store.cc`
- Modify: `include/cedar/storage/temporal_event.h`
- Modify: `src/storage/temporal_event.cc`
- Modify: `include/cedar/transaction/decision_log.h`
- Modify: `include/cedar/transaction/database_format.h`
- Modify: `src/transaction/database_format.cc`
- Modify: all `include/cedar`, `src`, `tests`, and `benchmarks` consumers returned by the exact searches below.

**Interfaces:**
- Produces: `BlobRef`, `DatabaseFormat`, and `MakeDatabaseFormat` with no compatibility aliases.

- [x] **Step 1: Record the exact consumer set**

Run:

```bash
rg -l '\bBlobRefV2\b|\bDatabaseFormatV2\b|\bMakeDatabaseFormatV2\b' include src tests benchmarks
```

Save the sorted result in the progress log before editing.

- [x] **Step 2: Perform the mechanical symbol rewrite**

Apply these exact replacements across the recorded files:

```text
BlobRefV2 -> BlobRef
DatabaseFormatV2 -> DatabaseFormat
MakeDatabaseFormatV2 -> MakeDatabaseFormat
```

Do not introduce aliases for the old symbols.

- [x] **Step 3: Add compile-time absence checks**

Run:

```bash
rg -n '\bBlobRefV2\b|\bDatabaseFormatV2\b|\bMakeDatabaseFormatV2\b' include src tests benchmarks
```

Expected: no output.

- [x] **Step 4: Build and run Blob/FORMAT/transaction tests**

Run `cmake --build build-v2 -j1 --target test_correctness_kernel`, then focused Blob, FORMAT, DecisionLog, TemporalEvent, and transaction tests. Expected: all pass with unchanged encoded-value behavior apart from the new FORMAT identity.

### Task 4: Rename VersionSet files, types, and APIs

**Files:**
- Move: `include/cedar/storage/version_set_v2.h` -> `include/cedar/storage/version_set.h`
- Move: `src/storage/version_set_v2.cc` -> `src/storage/version_set.cc`
- Modify: every consumer found by `rg -l 'version_set_v2|VersionSetV2|VersionSnapshotV2|VersionEditV2|SstFileMetaV2|BlobSegmentMetaV2|BlobSegmentKeyV2|IndexFragmentV2|IndexFragmentKeyV2|DurableCheckpointV2' include src tests benchmarks CMakeLists.txt`

**Interfaces:**
- Produces: canonical `VersionSet`, `VersionSnapshot`, `VersionEdit`, `SstFileMeta`, `BlobSegmentMeta`, `BlobSegmentKey`, `IndexFragment`, `IndexFragmentKey`, and `DurableCheckpoint`.

- [x] **Step 1: Add a compile RED for canonical includes**

Change the correctness-kernel include to `cedar/storage/version_set.h` and
change `DurableLogTest.VersionSetPublishesAtomicFileSnapshots` to instantiate
`VersionSet`. Build and observe missing-header/type failures.

- [x] **Step 2: Move files and rewrite the exact mapping**

Apply the mappings listed in the approved design to the full consumer set. Update include guards to `CEDAR_STORAGE_VERSION_SET_H_`, includes to `cedar/storage/version_set.h`, and CMake to `src/storage/version_set.cc`.

- [x] **Step 3: Remove the old files rather than forwarding them**

Verify:

```bash
test ! -e include/cedar/storage/version_set_v2.h
test ! -e src/storage/version_set_v2.cc
rg -n 'version_set_v2|\bVersion(Set|Snapshot|Edit)V2\b|\bSstFileMetaV2\b|\bBlobSegment(Meta|Key)V2\b|\bIndexFragment(Key)?V2\b|\bDurableCheckpointV2\b' include src tests benchmarks CMakeLists.txt
```

Expected: both `test` commands succeed and `rg` prints nothing.

- [x] **Step 4: Run VersionSet publication and recovery tests**

Run all VersionSet, Manifest, compaction publication, Blob retirement, checkpoint, stale-generation CAS, and reopen tests. Expected: all pass.

### Task 5: Rename SST, flush, and compaction files and APIs

**Files:**
- Move: `include/cedar/columnar/sst_v2.h` -> `include/cedar/columnar/sst.h`
- Move: `src/columnar/sst_v2.cc` -> `src/columnar/sst.cc`
- Move: `include/cedar/storage/sst_flush_v2.h` -> `include/cedar/storage/sst_flush.h`
- Move: `src/storage/sst_flush_v2.cc` -> `src/storage/sst_flush.cc`
- Move: `include/cedar/storage/sst_compaction_v2.h` -> `include/cedar/storage/sst_compaction.h`
- Move: `src/storage/sst_compaction_v2.cc` -> `src/storage/sst_compaction.cc`
- Modify: all consumers in `include`, `src`, `tests`, `benchmarks`, and `CMakeLists.txt`.

**Interfaces:**
- Produces: `Sst*`, `WriteSstFile`, `ReadSstFile`, `OpenSstEventCursor`, `VisitSstEvents`, `FlushEventsToSst`, `FlushShardToSst`, and `CompactSstPartition`.

- [x] **Step 1: Convert the SST golden-header test to canonical names and observe RED**

Rename `SstV2Test.GoldenBytesAreStableForFileBlockAndFooterHeaders` to
`SstTest.GoldenBytesAreStableForFileBlockAndFooterHeaders`, include
`cedar/columnar/sst.h`, and call `BuildSst`. Build must fail before production
files move.

- [x] **Step 2: Move files and apply the complete symbol mapping**

Remove `V2` from all `SstV2*`, `*SstV2*`, `FlushResultV2`, and `CompactionResultV2` production identifiers. Rename include guards and CMake sources. Preserve encoded SST header version `8` and feature bits behind unversioned constant names.

- [x] **Step 3: Normalize magic constants without changing bytes**

Use names such as:

```cpp
constexpr uint32_t kSstHeaderMagic = 0x38565353U;
constexpr uint16_t kSstEncodingVersion = 8;
```

Test names say `CurrentFileHeader` rather than `V8FileHeader`.

- [x] **Step 4: Verify no external SST version suffix remains**

Run:

```bash
rg -n 'sst_v2|SstV2|SST v2|\.sst2|FlushResultV2|CompactionResultV2' include src tests benchmarks CMakeLists.txt README.md
```

Expected: only explicit old-format rejection constants/tests may remain; each is added to the retained-hit inventory.

- [x] **Step 5: Run SST/page/flush/compaction/fault tests**

Expected: current golden bytes remain stable, current filenames use `.sst`, publication faults retain correct temp ownership, and old `.sst2` layouts are rejected without cleanup.

### Task 6: Rename the public database API and build target inputs

**Files:**
- Move: `include/cedar/db/cedar_database_v2.h` -> `include/cedar/db/cedar_database.h`
- Move: `src/db/cedar_database_v2.cc` -> `src/db/cedar_database.cc`
- Modify: `CMakeLists.txt`
- Modify: `README.md`
- Modify: `benchmarks/cedar_bench.cc`
- Modify: all database consumers in tests and production.

**Interfaces:**
- Produces: the sole public `cedar::CedarDatabase` API.

- [x] **Step 1: Add a public-header compile RED**

Change one test and the benchmark binary to include `cedar/db/cedar_database.h` and instantiate `CedarDatabase`. Build must fail before the move.

- [x] **Step 2: Move and rename the implementation**

Rename the class, constructor, destructor, member definitions, include guard, files, includes, and all call sites. Rename `CEDAR_V2_SOURCES` to `CEDAR_SOURCES`.

- [x] **Step 3: Remove the old API surface**

Run:

```bash
test ! -e include/cedar/db/cedar_database_v2.h
test ! -e src/db/cedar_database_v2.cc
rg -n 'CedarDatabaseV2|cedar_database_v2|CEDAR_V2_SOURCES' include src tests benchmarks CMakeLists.txt README.md
```

Expected: no output and no forwarding header.

- [x] **Step 4: Build the library, correctness kernel, and benchmark binaries**

Run all targets with `-j1`. Execute a Cedar-TG and LDBC smoke run and verify benchmark manifests report `database_format_version: 1`.

### Task 7: Normalize index encoding names and sidecar paths

**Files:**
- Modify: `include/cedar/index/index_definition.h`
- Modify: `src/index/index_sidecar.cc`
- Modify: `include/cedar/index/index_sidecar.h`
- Modify: `src/index/index_catalog.cc`
- Modify: index tests and all encoding consumers.

**Interfaces:**
- Produces: canonical raw/dictionary/bitmap/sorted-delta encoding constants and `.idx` artifacts.

- [x] **Step 1: Add RED tests for `.idx` publication and `.idx1` rejection**

Update active index publication tests to expect `.idx`. Add a directory non-mutation test for an old `.idx1` artifact.

- [x] **Step 2: Rename encoding constants**

Use exactly:

```cpp
kIndexCanonicalEncodingRaw
kIndexCanonicalEncodingDictionary
kIndexCanonicalEncodingBitmap
kIndexCanonicalEncodingSortedDelta
```

Keep numeric IDs `1`, `2`, `3`, and `4` unchanged.

- [x] **Step 3: Name the internal sidecar magic**

Replace raw `"CSI2"` literals with one unversioned constant such as `kIndexSidecarMagic`; preserve the four encoded bytes. No CSI1 reader is introduced.

- [x] **Step 4: Run the four-encoding corruption, repair, concurrent-query, publication, and reopen matrix**

Expected: all encodings behave identically at the logical level; current files use `.idx`; old `.idx1` is rejected.

### Task 8: Delete the scheduler compatibility queue

**Files:**
- Modify: `include/cedar/runtime/work_scheduler.h`
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Removes: `CompatibilityWorkId`, `ScheduledCompatibilityWork`, compatibility queue state, and compatibility methods.

- [x] **Step 1: Convert scheduler tests to executable IDs**

Rewrite priority/cancellation tests to allocate `ExecutableTaskId` values and use `EnqueueExecutable`, `CancelExecutable`, and `NextExecutableWork`.

- [x] **Step 2: Run tests to establish GREEN on the production API**

The converted tests must pass before compatibility declarations are removed.

- [x] **Step 3: Delete compatibility declarations and state**

Delete the types, public methods, queue member, and comments. Do not replace them with aliases.

- [x] **Step 4: Verify absence**

```bash
rg -n 'CompatibilityWorkId|ScheduledCompatibilityWork|Compatibility|queued_compatibility' include src tests benchmarks
```

Expected: no output.

### Task 9: Delete the materializing T-Cypher runtime

**Files:**
- Modify: `src/tcypher/executor.cc`
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `src/tcypher/physical_plan.cc`, `src/tcypher/runtime/query_runtime.cc`,
  or typed operator files identified by a failing support-matrix test.

**Interfaces:**
- Removes: `ExecuteMultiRootMatchAsOf`, `ExecuteMultiFixedMatchAsOf`, `CrossJoinRootResultStream`, `RootMatchBinding`, and `legacy_multi_root_materialized_rows`.

- [x] **Step 1: Add a support-matrix RED gate**

For every currently accepted multi-root and multi-relationship query, assert:

```cpp
EXPECT_GT(stats->physical_multi_join_builds + stats->physical_cross_join_builds, 0U);
EXPECT_GT(stats->pipeline_builds, 0U);
```

Do not use the legacy counter as proof. For deliberately unsupported shapes, assert binder/planner `NotSupported` before execution.

- [x] **Step 2: Run the matrix and identify every old-runtime caller**

Run all multi-root, cross-join, relationship multi-join, range/change, EXPLAIN, spill, cancellation, and fixed-seed oracle tests. Any accepted query that reaches the old functions is a valid RED.

- [x] **Step 3: Close each physical coverage gap**

Extend typed physical planning/runtime only for the accepted shapes exposed by Step 2. Preserve bounded memory, spill, cancellation, complete relationship identity, and snapshot pinning.

- [x] **Step 4: Remove the old dispatch branch and implementation**

Delete the materializing structs, streams, functions, dispatch calls, and stats field. There is no replacement fallback branch.

- [x] **Step 5: Verify absence and rerun the support matrix**

```bash
rg -n 'ExecuteMultiRootMatchAsOf|ExecuteMultiFixedMatchAsOf|CrossJoinRootResultStream|RootMatchBinding|legacy_multi_root_materialized_rows' include src tests benchmarks
```

Expected: no output; all accepted shapes use physical runtime.

### Task 10: Rename the current design document and update documentation

**Files:**
- Move: historical `docs/superpowers/specs/2026-07-17-cedar-columnar-v2-design.md` -> `docs/superpowers/specs/2026-07-17-cedar-columnar-design.md`
- Modify: the six current design documents
- Modify: `README.md`
- Modify: current non-historical plans that link the renamed specification.

**Interfaces:**
- Produces: canonical documentation with historical terms explicitly marked.

- [x] **Step 1: Move the current Columnar design document**

Update direct links and references. Do not leave a forwarding copy under the old filename.

- [x] **Step 2: Rewrite current terminology**

Use `CedarDatabase`, `VersionSet`, `SST`, `Manifest`, `.sst`, and `.idx`. Retain old names only in sections explicitly titled as old-format rejection or historical evidence.

- [x] **Step 3: Verify documentation links and residuals**

Run `rg` for the old filename and canonical symbol mappings. Every remaining hit must be historical and recorded in the final inventory.

### Task 11: Full residual audit and evidence package

**Files:**
- Create: `docs/superpowers/plans/2026-07-22-cedar-clean-break-evidence.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Produces: authoritative rename, deletion, retained-field, rejection, and verification evidence.

- [x] **Step 1: Scan production filenames**

```bash
rg --files include src tests benchmarks | rg '(^|/)[^/]*(v[0-9]+|V[0-9]+)[^/]*$'
```

Expected: no Cedar version-suffixed current file. Each remaining standard
external name is listed explicitly in the evidence document with its origin.

- [x] **Step 2: Scan symbols and paths**

```bash
rg -n '\b[A-Za-z_][A-Za-z0-9_]*(V[0-9]+|v[0-9]+)[A-Za-z0-9_]*\b|MANIFEST-V[0-9]+|\.sst[0-9]+|\.idx[0-9]+' include src tests benchmarks CMakeLists.txt README.md
rg -n -i 'legacy|compatibility|migration|fallback|old format|old path' include src tests benchmarks CMakeLists.txt README.md
```

Classify every retained hit as internal format validation, standard algorithm identity, correctness fallback, or explicit rejection test. Unclassified hits block completion.

- [x] **Step 3: Record exact evidence tables**

The evidence document contains:

```text
old symbol/path | canonical replacement | files changed
deleted symbol/file | reason | production reachability proof
retained version/magic field | persisted purpose | rejection behavior
old input case | expected status | directory unchanged evidence
```

### Task 12: Unified release verification

**Files:**
- Modify only files needed to fix failures exposed by the verification matrix.

**Interfaces:**
- Produces: completion evidence for the full clean-break goal.

- [x] **Step 1: Configure fresh canonical build trees**

Use new non-versioned local build names, for example `build-current`, `build-current-asan`, `build-current-ubsan`, and `build-current-tsan`. Do not delete existing build directories.

- [x] **Step 2: Build and run normal full tests**

```bash
cmake --build build-current -j1
ctest --test-dir build-current -j1 --output-on-failure
```

- [x] **Step 3: Build and run ASAN, UBSAN, and TSAN full tests**

Build and execute each tree independently with `-j1`. No sanitizer report is permitted.

- [x] **Step 4: Run focused fault/crash/oracle/benchmark matrices**

Include Manifest/SST/index publication faults, Blob GC, crash/reopen, orphan cleanup, old-format rejection/no-mutation, fixed-seed temporal/query oracle, benchmark artifact generation, and offline regeneration.

- [x] **Step 5: Run hygiene gates**

```bash
git diff --check
rg -n '[ \t]+$' CMakeLists.txt README.md include src tests benchmarks docs/superpowers/specs docs/superpowers/plans .superpowers/sdd/progress.md
```

Expected: no output.

- [x] **Step 6: Perform requirement-by-requirement completion audit**

Compare the final tree and evidence document against every objective and completion criterion in the active goal. Keep the goal active if any requirement lacks direct evidence.
