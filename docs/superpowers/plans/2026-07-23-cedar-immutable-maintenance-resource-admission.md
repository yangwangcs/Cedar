# Cedar Immutable Maintenance Resource Admission Implementation Plan

> **Execution status:** Tasks 1-5 are complete in the current dirty worktree. All 20 implementation and verification steps are checked; remaining work is limited to later sanitizer and release/paper evidence campaigns.

**Goal:** Make Index Build and Stats Merge admit, account, yield, cancel, and publish one immutable SST at a time without allocating or mutating outside their complete resource grants.

**Architecture:** The columnar, index, statistics, and Manifest owners each expose checked preflight estimates. `TransactionCoordinator` creates immutable per-SST work items and combines those estimates into one `MaintenanceTaskSpec`; `MaintenanceExecutor` remains the sole grant/IO admission owner. Stats checkpoint publication uses copy-project, fsync, rename, parent-directory fsync, then in-memory swap.

**Tech Stack:** C++17, Cedar `ResourceGovernor`, `IoGovernor`, `MaintenanceExecutor`, VersionSet/Manifest, GoogleTest/CTest.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Run every build and test with `-j1`.
- Keep internal database format number `1`.
- Do not add an old Manifest reader, migration, legacy runtime, fallback, or external V2/Vn name.
- A task must acquire its complete grant before source SST I/O, temporary-file creation, fragment detachment, checkpoint mutation, or Manifest publication.
- Optional pressure refusal returns typed `MaintenanceBackoff`; concrete governor rejection retains `ResourceExhausted` or `QueryMemoryLimit`.
- A successful logical publication retains existing generation-CAS, cancellation-boundary, indeterminate, and reopen-required semantics.

---

### Task 1: Typed maintenance backoff and Blob-aware statistics canonicalization

**Files:**
- Modify: `include/cedar/core/status.h`
- Modify: `src/core/status.cc`
- Modify: `include/cedar/statistics/stats_fragment.h`
- Modify: `src/statistics/stats_fragment.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `Status::MaintenanceBackoff()` and `Status::IsMaintenanceBackoff()` with a new terminal status code appended after existing codes.
- Produces: `BuildStatsFragment()` canonicalization of Blob-backed PUTs through `EncodeIndexBlobHash(const BlobRef&)`.

- [x] **Step 1: Write failing tests**

  Add `StatusTest.MaintenanceBackoffIsTypedAndStable`, asserting the factory,
  predicate, and `ToString()` type. Add
  `StatsFragmentTest.BlobReferencesUseDistinctHashesWithoutPayloadReads`,
  constructing two different `TemporalEvent::PutBlob` values and one repeated
  hash, then asserting `put_count` and `distinct_value_count` are `3` and `2`.

- [x] **Step 2: Run RED**

  Run:

  ```text
  cmake --build build-current -j1 && ctest --test-dir build-current -j1
    -R 'StatusTest.MaintenanceBackoffIsTypedAndStable|StatsFragmentTest.BlobReferencesUseDistinctHashesWithoutPayloadReads'
    --output-on-failure
  ```

  Expected: compilation fails because the status factory/test and Blob hash
  canonicalization are absent.

- [x] **Step 3: Implement the minimal behavior**

  Append `kMaintenanceBackoff` after `kShutdownInProgress` without renumbering
  existing codes. Format its string as `MaintenanceBackoff: ...`. In
  `BuildStatsFragment`, select `EncodeIndexBlobHash(*event.blob_ref())` when
  `event.is_blob_reference()` is true; otherwise retain inline canonicalization.

- [x] **Step 4: Run GREEN**

  Re-run the exact RED command and require both tests to pass. Run
  `git diff --check`.

---

### Task 2: Bounded sidecar reads and component-owned index estimates

**Files:**
- Modify: `include/cedar/index/index_sidecar.h`
- Modify: `src/index/index_sidecar.cc`
- Modify: `include/cedar/columnar/sst.h`
- Modify: `src/columnar/sst.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `StatusOr<uint64_t> EstimateIndexSidecarEncodedBytes(const IndexDefinition&, const SstFileStatistics&, uint64_t source_file_bytes, const ColumnSchema&)`.
- Produces: `StatusOr<ResourceProfile> EstimateSstDecodeResources(const SstFileStatistics&, uint64_t source_file_bytes, const ColumnSchema&)` without making the columnar owner depend on `SstFileMeta`.
- Produces: `ReadVerifiedIndexSidecarFile(path, definition, source_sst_id, checksum, max_bytes)`; it fstats before allocating and returns `Corruption` when the file exceeds `max_bytes`.

- [x] **Step 1: Write failing tests**

  Add `IndexSidecarTest.RejectsOversizedVerifiedReadBeforeAllocation`, writing
  a file whose size is larger than a one-byte bound and asserting corruption.
  Add `IndexSidecarTest.EstimateIncludesPostingEncodingBound`, using a bitmap
  definition and source statistics with nonzero rows/puts to assert a nonzero
  encoded bound. Add `SstTest.DecodeResourceEstimateIncludesReadAndPeakMemory`
  asserting file read bytes, memory bytes, and one CPU slot are nonzero.

- [x] **Step 2: Run RED**

  Run:

  ```text
  cmake --build build-current -j1 && ctest --test-dir build-current -j1
    -R 'IndexSidecarTest.(RejectsOversizedVerifiedReadBeforeAllocation|EstimateIncludesPostingEncodingBound)|SstTest.DecodeResourceEstimateIncludesReadAndPeakMemory'
    --output-on-failure
  ```

  Expected: compilation fails because the bounded reader and estimate helpers
  do not exist.

- [x] **Step 3: Implement checked estimates and bounded read**

  Derive the maximum canonical inline bytes from the registered schema's
  `blob_threshold`; use fixed-width bounds for numeric values and the fixed
  BlobRef hash bound for Blob rows. Include row-count capacity, one
  `kMaxRowsPerGranuleBlock` decode transient, temporary bytes, descriptors,
  source sequential-read bytes, and one CPU slot. For sidecars include the
  worst-case posting vector, encoding intermediates, encoded bytes, and the
  existing `kMaximumIndexSidecarBytes` ceiling. `ReadAll` must fstat and reject
  an over-bound file before constructing its body string.

- [x] **Step 4: Run GREEN**

  Re-run the RED command. Also run the existing sidecar group:

  ```text
  ctest --test-dir build-current -j1 -R 'DurableLogTest.(ActivatingIndexBuildsManifestOwnedSidecarsForExistingSsts|IndexRepairRetainsPossiblyPublishedSidecarAfterManifestRename)' --output-on-failure
  ```

---

### Task 3: Projected Manifest and Stats checkpoint estimates

**Files:**
- Modify: `include/cedar/storage/version_set.h`
- Modify: `src/storage/version_set.cc`
- Modify: `include/cedar/statistics/stats_snapshot.h`
- Modify: `src/statistics/stats_snapshot.cc`
- Modify: `include/cedar/statistics/stats_fragment.h`
- Modify: `src/statistics/stats_fragment.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `StatusOr<uint64_t> VersionSet::EstimateManifestEditRewriteBytes(const VersionEdit&)`, encoding the projected edit with the current generation and returning checked framed bytes.
- Produces: `StatusOr<ResourceProfile> StatsSnapshotStore::EstimateUpsertResources(const StatsFragment&, uint64_t expected_generation) const`.
- Produces: `Status StatsSnapshotStore::UpsertExpected(const StatsFragment&, uint64_t expected_generation)` with copy-project/persist/fsync/rename/parent-directory-fsync/swap ordering.

- [x] **Step 1: Write failing tests**

  Add `DurableLogTest.ManifestEditEstimateIncludesIndexFragmentRecord`,
  comparing a projected fragment edit against the no-edit estimate. Add
  `DurableLogTest.ExpectedStatsUpsertRejectsStaleGenerationWithoutMutation` and
  `DurableLogTest.FailedStatsCheckpointPublicationKeepsPublishedGeneration`.
  Introduce a checkpoint publication fault injector on `StatsSnapshotStore` and
  use it to prove stale/failing updates do not change the in-memory generation
  or the old checkpoint bytes.

- [x] **Step 2: Run RED**

  Run:

  ```text
  cmake --build build-current -j1 && ctest --test-dir build-current -j1
    -R 'DurableLogTest.(ManifestEditEstimateIncludesIndexFragmentRecord|ExpectedStatsUpsertRejectsStaleGenerationWithoutMutation|FailedStatsCheckpointPublicationKeepsPublishedGeneration)'
    --output-on-failure
  ```

  Expected: compilation fails because projected-edit and expected-generation
  APIs are absent.

- [x] **Step 3: Implement projected publication**

  Clone the current snapshot/map under the owner mutex, apply the proposed
  edit, encode the complete framed bytes, and expose its exact size. For stats,
  write the temporary checkpoint, fsync its file, rename it, open/fsync/close
  the parent directory, then swap the projected map and increment generation.
  If any pre-rename operation fails, leave disk and memory unchanged. A failure
  injected after rename is indeterminate for this advisory checkpoint: return
  the I/O status and leave the in-memory snapshot unchanged without setting the
  database reopen-required gate. Delete the temporary file on pre-rename error.

- [x] **Step 4: Run GREEN**

  Re-run the RED command and existing statistics persistence tests:

  ```text
  ctest --test-dir build-current -j1 -R 'DurableLogTest.(StatsSnapshotCheckpointRestartsAndFallsBackConservativelyOnCorruption|QueryContextPinsCoherentVersionCatalogAndStatisticsIdentity)' --output-on-failure
  ```

---

### Task 4: Per-SST maintenance admission and pressure yielding

**Files:**
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `include/cedar/runtime/maintenance_executor.h`
- Modify: `src/runtime/maintenance_executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: one immutable index/statistics work item per source SST, each submitted with a complete `ResourceProfile` and matching `IoTokenRequest`.
- Produces: optional-pressure refusal as `Status::MaintenanceBackoff()`.
- Consumes: Task 2 decode/sidecar estimates and Task 3 Manifest/Stats estimates.

- [x] **Step 1: Write failing production-path tests**

  Add `DurableLogTest.IndexBuildResourceRejectionPrecedesAnyMutation`,
  `DurableLogTest.StatsMergeResourceRejectionPrecedesAnyMutation`,
  `DurableLogTest.IndexAndStatsMaintenanceReleaseEveryDimension`, and
  `DurableLogTest.PerSstMaintenanceReleasesBeforeNextSource`. Use a bound
  governor with zero memory or write capacity, a source SST with one property
  row, and assertions that no `indexes/` directory, stats checkpoint change,
  fragment detach, or Manifest generation change occurs. Use the existing
  cancellation observer to inspect governor usage during the callback.

- [x] **Step 2: Run RED**

  Run:

  ```text
  cmake --build build-current -j1 && ctest --test-dir build-current -j1
    -R 'DurableLogTest.(IndexBuildResourceRejectionPrecedesAnyMutation|StatsMergeResourceRejectionPrecedesAnyMutation|IndexAndStatsMaintenanceReleaseEveryDimension|PerSstMaintenanceReleasesBeforeNextSource)'
    --output-on-failure
  ```

  Historical RED expectation: the pre-change whole-batch implementation either
  admitted with zero memory/write dimensions or left the callback untestable at
  the per-SST boundary. The implemented path now rejects before source I/O or
  mutation and grants every applicable resource dimension per immutable SST.

- [x] **Step 3: Implement per-SST scheduling**

  Capture source metadata, matching definition/fragment, Manifest/statistics
  generation, and deterministic path into each work item. Preflight all
  component estimates, combine dimensions without overflow, and submit one
  `MaintenanceTaskSpec` with `preemptible=true`. The callback revalidates
  identities before `ReadSstFile`, uses bounded sidecar reads, performs Blob
  hash statistics canonicalization, and publishes through the expected
  generation APIs. Between tasks call `RefreshPressure()`; return
  `MaintenanceBackoff` when policy declines optional work. Keep urgent,
  flush, commit-critical, recovery, and shutdown classes unchanged.

- [x] **Step 4: Run GREEN**

  Re-run the RED command. Then run the maintenance admission/cancellation
  selection:

  ```text
  ctest --test-dir build-current -j1 -R 'MaintenanceExecutorTest|DurableLogTest.(RunningNormalCompactionCancellationAtOutputBoundaryCleansOutput|RunningBlobGcCancellationStopsAtThe64HashBatch|RepairsCorruptIndexSidecarWhileQueriesRemainCorrect|ActivatingIndexBuildsManifestOwnedSidecarsForExistingSsts)' --output-on-failure
  ```

---

### Task 5: Fault, regression, and closure evidence

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`
- Modify: `docs/superpowers/specs/2026-07-23-cedar-immutable-maintenance-resource-admission-design.md`

**Interfaces:**
- Consumes: all preceding component estimates and per-SST scheduling behavior.
- Produces: exact focused test names/counts and an honest Temporal Index/CBO resource-accounting status; it does not claim sanitizer or release closure.

- [x] **Step 1: Add fault and concurrency regressions**

  Add tests for cancellation before sidecar publication, sidecar post-rename
  indeterminate status, stale index work item conflict, stale stats generation,
  reopen cleanup, pressure backoff between two SSTs, and concurrent query
  fallback while explicit repair runs. Assert exact typed statuses and zero
  unauthorized resource usage.

- [x] **Step 2: Run focused closure selection**

  Run all new tests plus existing `DropIndex`, index repair, stats checkpoint,
  Manifest CAS, cancellation, and resource tests with `-j1`. Record the exact
  count and command in the plan and progress ledger.

- [x] **Step 3: Run the fresh normal matrix**

  Run:

  ```text
  cmake --build build-current -j1 && ctest --test-dir build-current -j1 --output-on-failure
  ```

  Require zero failures before updating evidence. Do not refresh sanitizer
  matrices until the next feature batch, consistent with the current closure
  priority.

- [x] **Step 4: Update evidence without overstating closure**

  Mark Temporal Index/CBO resource accounting as functionally complete only if
  every new test passes and all production requests have nonzero dimensions
  where operations occur. Automatic health-event repair scheduling and
  concurrent runtime-feedback generation isolation are now implemented and
  covered by focused regressions. Keep sanitizer refresh, production-scale
  fairness, and release/paper artifacts explicitly open.

## Self-Review Record

- The plan covers every section of the approved design: typed pressure status,
  component estimates, bounded sidecar reads, projected Manifest/stats
  publication, per-SST yielding, failure semantics, Blob hash statistics, and
  focused evidence.
- No old reader, migration, fallback, external V2/Vn name, or format change is
  introduced.
- All interface names used by later tasks are defined in the producing task.
- No task contains a placeholder or asks for an unspecified edge-case test.

## Execution Record

- Tasks 1-5 implemented in the existing dirty worktree without staging or
  committing.
- Focused closure selection: 18/18 passed with `-j1`.
- Additional production regressions cover successful stats grant release,
  between-SST pressure backoff, and recovery queue reconstruction.
- Fresh normal matrix after the final accounting correction: 873/873 passed
  with `-j1`.
- `git diff --check` passed. Sanitizer and release/paper evidence remain open.
