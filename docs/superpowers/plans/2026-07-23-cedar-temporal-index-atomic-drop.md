# Cedar Temporal Index Atomic Drop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:test-driven-development and execute each task in RED/GREEN order.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make index drop one Manifest generation that removes the definition
and every fragment, preserves old snapshot sidecars until release, gates all
mutations after indeterminate publication, and never reuses a durable index
identity.

**Architecture:** `IndexCatalog::DropIndex()` builds one generation-CAS
`VersionEdit` containing the definition deletion and all matching fragment
deletions. `VersionSnapshot::next_index_id` is a Manifest-persisted high-water
mark used by registration, so a replacement index cannot collide with files
owned by a pinned pre-drop snapshot. `TransactionCoordinator` retains the old
snapshot for physical reclamation after successful publication and enters
recovery-required state after an indeterminate drop.

**Tech Stack:** C++17, Cedar VersionSet/Manifest, IndexCatalog,
TransactionCoordinator, GoogleTest/CTest.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Run every build and test with `-j1`.
- Keep internal database format number `1`.
- Do not add an old Manifest reader, migration, legacy runtime, fallback, or
  external V2/Vn name.
- A successful logical drop is exactly one Manifest generation.
- An indeterminate Manifest publication requires reopen before every mutation.

---

### Task 1: Atomic catalog edit

**Files:**
- Modify: `include/cedar/index/index_catalog.h`
- Modify: `src/index/index_catalog.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `Status IndexCatalog::DropIndex(uint64_t index_id)` with direct
  definition/fragment removal and `expected_generation` CAS.
- Removes: `RetireIndex()` and `PurgeRetiredIndex()` transitional APIs.

- [x] **Step 1: Write the failing catalog test**

  Change `IndexCatalogValidatesSchemaAndPublishesLifecycleEdits` to assert that
  one `DropIndex()` increments generation by exactly one and leaves neither the
  definition nor any fragment in the resulting snapshot.

- [x] **Step 2: Run RED**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1
  -R 'DurableLogTest.IndexCatalogValidatesSchemaAndPublishesLifecycleEdits'
  --output-on-failure`

  Expected: failure because the current API persists `DROPPING` and requires
  two additional Manifest edits.

- [x] **Step 3: Implement the single edit**

  Capture one snapshot, validate the definition exists, set
  `edit.expected_generation = snapshot->generation`, append `index_id` to
  `index_deletes`, append every matching `(index_id, source_sst_id)` to
  `index_fragment_deletes`, and call `ApplyEdit()` once.

- [x] **Step 4: Run GREEN**

  Run the Step 2 command and require one passing test.

### Task 2: Durable non-reused index identities

**Files:**
- Modify: `include/cedar/storage/version_set.h`
- Modify: `src/storage/version_set.cc`
- Modify: `src/index/index_catalog.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produces: `VersionSnapshot::next_index_id`, encoded as a required format-1
  Manifest field.
- `RegisterIndex()` consumes the pinned high-water value and rejects exhaustion.

- [x] **Step 1: Write failing persistence and replacement tests**

  Add a VersionSet round-trip test proving `next_index_id` survives reopen.
  Extend the coordinator drop test to register the same column again while the
  old snapshot is pinned and assert the replacement ID is greater, its sidecar
  path differs, and the old sidecar still exists.

- [x] **Step 2: Run RED**

  Run the two named tests and confirm ID reuse or missing Manifest metadata.

- [x] **Step 3: Encode and validate the high-water mark**

  Encode `next_index_id` immediately after Manifest generation; decode requires
  a nonzero value. On every index add, advance it to `max(current, index_id+1)`
  and reject `UINT64_MAX`. Drop never decrements it.

- [x] **Step 4: Allocate from the high-water mark**

  `IndexCatalog::RegisterIndex()` uses `snapshot->next_index_id`, sets
  `expected_generation`, and publishes the add. A concurrent catalog edit
  returns typed `Conflict`; callers may retry at their existing ownership
  boundary.

- [x] **Step 5: Run GREEN**

  Run the Task 2 tests and the Manifest/index catalog group.

### Task 3: Coordinator fault and reclamation semantics

**Files:**
- Modify: `src/transaction/transaction_coordinator.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: atomic `IndexCatalog::DropIndex()`.
- Preserves: `RetiredSstSet` old-snapshot pin and startup orphan cleanup.

- [x] **Step 1: Write the failing atomic-generation test**

  Extend `DropIndexRetiresSidecarsAfterPinnedVersionSnapshotsRelease` to assert
  one generation increment, direct removal, replacement non-collision, and old
  sidecar reclamation only after the pinned snapshot releases.

- [x] **Step 2: Write the failing indeterminate/reopen test**

  Inject `VersionSetFaultPoint::kAfterManifestRename`, assert drop returns
  `Indeterminate`, `recovery_required()` becomes true, and a subsequent schema,
  index, commit, ID allocation, flush, compaction, Blob, and checkpoint mutation
  returns `RecoveryRequired`. Reopen and assert definition/fragments are absent,
  the orphan sidecar is removed, and the same column can register under a new
  non-reused ID.

- [x] **Step 3: Run RED**

  Run: `cmake --build build-current -j1 && ctest --test-dir build-current -j1
  -R 'DurableLogTest.(DropIndexRetiresSidecarsAfterPinnedVersionSnapshotsRelease|IndexDropIndeterminateGatesMutationsAndReopenCleansSidecar)'
  --output-on-failure`

  Expected: generation delta is three and indeterminate drop does not set the
  coordinator recovery gate.

- [x] **Step 4: Implement coordinator ownership**

  Call `catalog.DropIndex()` exactly once. An indeterminate Manifest edit sets
  `VersionSet::requires_reopen()`; `TransactionCoordinator::recovery_required()`
  incorporates that state and therefore gates all subsequent mutations without
  a duplicate coordinator flag. Only after success enqueue the captured sidecar
  paths with the captured snapshot and call `ReclaimRetiredSsts()`.

- [x] **Step 5: Run GREEN**

  Run the Step 3 command and require both tests to pass.

### Task 4: Regression and closure documentation

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

- [x] **Step 1: Run focused lifecycle/fault tests**

  Run all `DropIndex`, `IndexCatalog`, `IndexRegistrationIndeterminate`,
  Manifest CAS, orphan cleanup, repair, and candidate-completeness tests with
  `-j1`.

- [x] **Step 2: Run the complete normal matrix**

  Run: `ctest --test-dir build-current -j1 --output-on-failure`

- [x] **Step 3: Update evidence without overstating closure**

  Record exact test names and counts. Keep resource accounting, automatic
  corrupt-fragment repair scheduling, concurrent feedback snapshot visibility,
  sanitizer refresh, and release/paper workloads open until independently
  proven.

## Verification Record

- The focused Manifest/catalog/drop/repair/orphan-cleanup selection passed
  34/34 with `-j1`.
- The complete fresh normal matrix passed 856/856 with `-j1`.
- The atomic-drop behavior is functionally complete. Temporal-index/CBO remains
  `PARTIAL` at release/paper level because automatic corrupt-fragment repair
  scheduling, index-build resource accounting, concurrent runtime-feedback
  visibility, current sanitizer matrices, and lifecycle/performance artifacts
  remain open.
