# Cedar Bundled Codecs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship fixed LZ4 and Zstd sources with Cedar, build offline by default, and verify compiled codec capabilities before database recovery touches persistent state.

**Architecture:** Cedar owns static `cedar_lz4` and `cedar_zstd` targets built from pinned source snapshots under `third_party/`. `page_format` exposes immutable build metadata and a bounded round-trip self-test; `CedarDatabase::Open()` runs that self-test before starting telemetry, workers, or opening the coordinator. An explicit Zstd-disabled developer build retains deterministic `NotSupported`, but the release/default build contains both codecs and never invokes a package manager or network at runtime.

**Tech Stack:** C++17, CMake, LZ4 1.10.0, Zstd 1.5.7, GoogleTest.

## Global Constraints

- Keep the database format number at 1 and preserve existing numeric codec IDs.
- Do not restore external V2/Vn names, legacy runtime, manifest, or disk-layout compatibility.
- Runtime startup must not download, install, or mutate host packages.
- Production/default builds must not depend on host LZ4 or Zstd packages.
- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.

---

### Task 1: Codec capability and self-test contract

**Files:**
- Modify: `include/cedar/columnar/page_format.h`
- Modify: `src/columnar/page_format.cc`
- Modify: `tests/test_correctness_kernel.cc`

- [x] Add a failing test requiring fixed codec names/versions and successful bounded round trips for every compiled codec.
- [x] Run the focused test and confirm it fails because the capability API does not exist.
- [x] Implement `GetPageCodecCapabilities()` and `VerifyPageCodecCapabilities()`.
- [x] Run the focused test and existing LZ4/Zstd tests.

### Task 2: Pinned offline static build

**Files:**
- Create: `third_party/lz4/`
- Create: `third_party/zstd/`
- Modify: `CMakeLists.txt`

- [x] Vendor exact upstream source snapshots, licenses, version/hash metadata.
- [x] Replace default host discovery with static bundled targets.
- [x] Configure and build without host codec discovery and confirm no dynamic codec dependency.
- [x] Preserve `CEDAR_ENABLE_ZSTD=OFF` as an explicit negative-capability build.

### Task 3: Database startup gate

**Files:**
- Modify: `src/db/cedar_database.cc`
- Modify: `tests/test_correctness_kernel.cc`

- [x] Add a failing metric-contract test proving database open records the startup self-test.
- [x] Call the verifier first in `CedarDatabase::Open()` before telemetry, workers, and coordinator recovery.
- [x] Export bounded success/failure counters and verify successful open/recovery paths in the full normal matrix.

### Task 4: Release evidence

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-remaining-closure-goal.md`
- Modify: `.superpowers/sdd/progress.md`

- [x] Record versions, archive hashes, licenses, build mode, commands, and test evidence.
- [x] Run normal and Zstd-disabled focused tests, then sanitizer focused tests with `-j1`.
- [x] Keep the six-design goal active because other functional and artifact gaps remain.
