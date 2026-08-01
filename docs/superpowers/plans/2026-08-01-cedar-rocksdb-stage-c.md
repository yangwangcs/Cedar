# Cedar RocksDB Stage C Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit resumable history Vacuum and a rebuildable Cedar columnar analytical projection with canonical fallback.

**Architecture:** Vacuum durably advances the oldest-readable watermark before bounded cleanup and retains one baseline per `(fact, valid_from)`. The projection consumes durable sequence records into checksummed columnar segments whose manifest proves coverage; no projection artifact is required for Open, commit, recovery, or correctness.

**Tech Stack:** C++20, RocksDB iterators/WriteBatch, Cedar page codecs, CRC32C/BLAKE3, GoogleTest.

## Global Constraints

- Follow the master plan's Global Constraints.
- Vacuum is explicit and monotonic; it never cancels a Snapshot.
- Projection corruption never marks canonical RocksDB recovery-required.

---

### Task C1: Implement Active Snapshot Registry and Vacuum Preparation

**Files:**
- Create: `src/kernel/snapshot_registry.h`
- Create: `src/kernel/snapshot_registry.cc`
- Modify: `src/kernel/snapshot.cc`
- Modify: `src/kernel/database.cc`
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `src/fact/fact_store.cc`
- Create: `tests/test_vacuum.cc`

- [ ] Write RED tests for `SnapshotExpired`, `SnapshotPinned`, monotonic boundaries, invalid future boundary, and move/destruction registration.
- [ ] Implement durable `{target, phase=prepared}` plus oldest watermark in one synchronous WriteBatch before cleanup.
- [ ] Run GREEN and commit as `feat: prepare resumable temporal vacuum`.

### Task C2: Implement Baseline-Preserving Vacuum Cleanup

**Files:**
- Create: `src/fact/vacuum.cc`
- Create: `src/fact/vacuum.h`
- Modify: `src/fact/fact_store.cc`
- Extend: `tests/test_vacuum.cc`

- [ ] Write RED histories containing multiple valid times and multiple corrections on both sides of the boundary.
- [ ] Implement bounded prefix iteration retaining greatest `commit_seq <= B` per `(fact, valid_from)` plus all newer versions.
- [ ] Store resumable cursor progress in `vacuum/state`; issue physical RocksDB Delete only in Vacuum batches.
- [ ] Inject crash/reopen before boundary write, after boundary write, during cleanup, and before completion; verify monotonic resume.
- [ ] Commit as `feat: vacuum obsolete system-time versions`.

### Task C3: Define Projection Manifest and Columnar Segment Codec

**Files:**
- Create: `include/cedar/projection/projection.h`
- Create: `src/projection/projection_manifest.h`
- Create: `src/projection/projection_manifest.cc`
- Create: `src/projection/columnar_segment.h`
- Create: `src/projection/columnar_segment.cc`
- Create: `tests/test_projection_codec.cc`

- [ ] Write RED golden/corruption tests for source sequence range, property/family, identity range, checksums, row groups, system columns, typed values, and tombstones.
- [ ] Reuse focused Cedar page codecs but remove dependencies on query memory, cancellation, IO governor, and canonical VersionSet.
- [ ] Commit as `feat: define rebuildable columnar segments`.

### Task C4: Build Projection from Durable Sequence Records

**Files:**
- Create: `src/projection/projection_builder.h`
- Create: `src/projection/projection_builder.cc`
- Modify: `src/kernel/database.cc`
- Create: `tests/test_projection_builder.cc`

- [ ] Write RED tests for ordered incremental coverage, restart, duplicate build idempotence, sparse property changes, and fallen-behind Vacuum rebuild.
- [ ] Consume `sequence/<commit_seq>` in order and fetch exact canonical events at a pinned Snapshot.
- [ ] Publish segment then projection manifest atomically using temp-file, file fsync, rename, and directory fsync.
- [ ] Commit as `feat: build columnar projections from FactStore`.

### Task C5: Add Projection-Aware Scan with Canonical Fallback

**Files:**
- Create: `src/projection/projection_reader.h`
- Create: `src/projection/projection_reader.cc`
- Modify: `src/kernel/snapshot.cc`
- Create: `tests/test_projection_fallback.cc`

- [ ] Write one oracle scan corpus and run it with no projection, full coverage, partial coverage, stale coverage, deleted directory, checksum corruption, and canonical suffix.
- [ ] Select projection data only when manifest coverage proves the requested family/property/identity/snapshot range; otherwise use `FactStore::Scan` for the uncovered region.
- [ ] Verify deleting the entire projection directory does not change any public result.
- [ ] Commit as `feat: scan through rebuildable projections`.

### Task C6: Add Adjacency Projection Contract

**Files:**
- Create: `include/cedar/projection/adjacency.h`
- Create: `src/projection/adjacency_projection.cc`
- Create: `tests/test_adjacency_projection.cc`

- [ ] Write RED tests deriving outbound/inbound adjacency from one canonical EdgeIdentity/EdgeState stream and intersecting endpoint state at read time.
- [ ] Implement EdgeOut/EdgeIn only as projection rows with canonical fallback.
- [ ] Verify projection rebuild after vertex retract/reassert and Vacuum.
- [ ] Commit as `feat: derive adjacency from canonical edges`.
