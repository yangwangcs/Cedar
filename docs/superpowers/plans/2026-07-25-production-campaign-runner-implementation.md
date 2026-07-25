# Cedar Production Campaign Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a clean candidate and approved baseline executable on a qualified workstation/stress host through the complete production campaign contract without silent profile shrink or partial-result promotion.

**Architecture:** Add a machine-readable build-provenance probe to `cedar_bench` and require `cedar_bench_pair --production-release` to validate both binary snapshots before creating its result root. Add a focused production-campaign planner/CLI that enumerates the frozen workload/cache and fault matrices, executes existing benchmark CLIs, records atomic per-command completion state, resumes only verified completed commands, and emits a final campaign index only after every child artifact passes strict reading and report regeneration.

**Tech Stack:** C++17, POSIX process APIs, CMake, GoogleTest, Cedar benchmark artifact readers.

## Global Constraints

- Preserve the dirty worktree; do not reset, clean, stage, commit, or push.
- Use `-j1` for every build and CTest command.
- Only workstation and stress are production profiles; never shrink their dataset, workers, RAM, CPU, or storage floors.
- Require an independently approved, distinct baseline SHA-256 and clean full source commits before result-root creation.
- Keep paper and external LDBC outside this runner.
- Reuse `cedar_bench_pair --production-release` and `cedar_bench --fault`; do not duplicate benchmark workload logic.

---

### Task 1: Preflight binary build provenance

**Files:**
- Modify: `include/cedar/benchmark/profile.h`
- Modify: `src/benchmark/profile.cc`
- Modify: `benchmarks/cedar_bench.cc`
- Modify: `benchmarks/cedar_bench_pair.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produce `BenchmarkBinaryProvenance` and `ValidateProductionBenchmarkBinaryProvenance(...)`.
- Produce `cedar_bench --build-provenance`, a single strict line containing source commit, dirty flag, instrumentation profile, and database format.
- Require both binary snapshots to report clean provenance, Tier 0/1 instrumentation, format 1, and the approved baseline commit before `cedar_bench_pair` creates its root.

- [x] Write a focused failing validation test for dirty, malformed, wrong-profile, wrong-format, and baseline-commit mismatch cases.
- [x] Run the focused test and observe the expected compile failure.
- [x] Implement the struct and pure validation helper.
- [x] Add the read-only CLI probe and strict parent-side parsing from the already-open executable snapshots.
- [x] Verify focused tests and production-pair fail-close behavior with `-j1`.

### Task 2: Freeze the production command matrix

**Files:**
- Create: `include/cedar/benchmark/production_campaign.h`
- Create: `src/benchmark/production_campaign.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produce `BuildProductionCampaignPlan(...)` returning stable IDs and argv vectors.
- Enumerate 13 public workloads across all 5 cache modes as paired production commands and all 10 typed fault/reopen scenarios as candidate commands.
- Reject CI/paper, fewer than five pairs, empty/non-distinct binaries, and result roots that alias binary paths.

- [x] Write a failing test asserting exact count, stable unique IDs, exact profile/seed/pair count, and complete workload/cache/fault coverage.
- [x] Run the focused test and observe the expected compile failure.
- [x] Implement the minimal immutable planner.
- [x] Run the focused planner tests with `-j1`.

### Task 3: Execute and resume fail-closed

**Files:**
- Create: `benchmarks/cedar_production_campaign.cc`
- Modify: `include/cedar/benchmark/production_campaign.h`
- Modify: `src/benchmark/production_campaign.cc`
- Modify: `CMakeLists.txt`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produce `cedar_production_campaign <baseline> <candidate> <workstation|stress> <seed> <results-root> [pair-count] [order-seed]`.
- Preflight approval, host, binary hashes/provenance, output-root freshness, and durable filesystem before creating the campaign root.
- Write one atomic state record per command; on resume, skip only a command whose child roots strict-read successfully and whose reports regenerate successfully.

- [x] Write failing state-transition and resume-validation tests using synthetic command results.
- [x] Run focused tests and observe the expected failures.
- [x] Implement bounded child execution, signal/exit propagation, atomic state publication, and strict resume validation.
- [x] Add the CLI and build target.
- [x] Verify help/argument errors, dirty-build rejection, and undersized-host rejection create no campaign root.

### Task 4: Seal and verify the campaign archive

**Files:**
- Modify: `include/cedar/benchmark/production_campaign.h`
- Modify: `src/benchmark/production_campaign.cc`
- Modify: `benchmarks/cedar_production_campaign.cc`
- Modify: `docs/superpowers/plans/2026-07-23-cedar-external-release-evidence-contract.md`
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Emit a campaign index binding command IDs, argv, binary SHA-256 values, approval identity, host profile, child artifact paths, statuses, and hashes.
- Exit zero only when every paired gate passes, every fault artifact strict-reads, every report regenerates, and the campaign ledger verifies.

- [x] Write a failing finalization test that rejects missing, failed, duplicated, or unverified command records.
- [x] Implement strict finalization and byte ledger generation.
- [x] Run focused tests, then the existing benchmark/artifact aggregate with `-j1`.
- [x] Run `git diff --check` and document the current external blockers without claiming production release.

### Task 5: Independent-review integrity hardening

**Files:**
- Modify: `include/cedar/benchmark/artifact_reader.h`
- Modify: `src/benchmark/artifact_reader.cc`
- Modify: `src/benchmark/production_campaign.cc`
- Modify: `benchmarks/cedar_production_campaign.cc`
- Test: `tests/test_correctness_kernel.cc`

**Review findings and closure:**
- [x] Reconstruct baseline/candidate samples from strict-read child artifacts and rerun `ComparePairedBenchmarkRuns` during resume; do not trust an archived PASS gate.
- [x] On Linux execute each child from the already verified snapshot descriptor with `fexecve`; verify all snapshots before and after every command.
- [x] Add `VerifySha256LedgerDirectory`, require an exact expected path set, reject tampering and symlink entries, and safely read back the campaign ledger after publication.
- [x] Bind full command argv, approval identity, source/binary identities, complete host provenance, and paired/fault child paths and hashes in the campaign index.
- [x] Bind campaign seed and order seed in schema-2 paired ledgers, validate the actual alternating arm order, and reject relocated artifacts with mismatched generator seeds.
- [x] Revalidate all 75 command outputs at finalization, use a schema-2 JSON index with argv arrays and explicit child hashes, prune database directories, and require two complete ledger snapshots to match path-for-path and hash-for-hash around the second semantic pass.
- [x] Build `cedar_production_campaign`, `cedar_bench_pair`, and `test_correctness_kernel` with `-j1`; pass the 9/9 campaign selection, the latest normal/ASAN/UBSAN/TSAN 928/928 matrices, and `git diff --check`.
- [x] Re-run workstation, stress, paired-release, and campaign dirty-build probes; all exit 2 before creating their result roots.

The runner and negative gates are implemented and locally verified. The active
release goal remains open because the working tree is dirty, this host is below
the declared workstation/stress floors, no approved distinct baseline exists,
and no positive 75-command campaign has run on a qualified host.
