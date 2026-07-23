# Cedar Six-Design Batched Final Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to execute this plan in the current dirty worktree. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the three remaining functional slices, generate the six release/paper evidence families in batches, and run the full normal/ASAN/UBSAN/TSAN matrix only once at final closure.

**Architecture:** Functional work is completed before release campaigns so later evidence exercises one stable implementation. Each functional slice uses RED/GREEN focused normal tests only; sanitizer and full-matrix execution are deferred to the single final verification phase. Release artifacts bind format-1 data, current clean-break names, commands, hashes, environment, resource profiles, and statistical policy.

**Tech Stack:** C++17, CMake/CTest, GoogleTest, Cedar WorkExecutionService/MaintenanceExecutor, VersionSet/IndexCatalog, RuntimeFeedbackStore, benchmark artifact and paired-comparison tools.

## Global Constraints

- Preserve the existing dirty worktree; do not reset, clean, stage, commit, or push.
- Use `-j1` for every build and test command.
- Keep the internal database format number at `1`.
- Do not restore legacy readers, migration, old runtimes, old layouts, or external V2/Vn names.
- During Tasks 1-3, run only the named focused normal tests and `git diff --check`; do not run a full normal or sanitizer matrix.
- Do not regenerate an evidence family until all three functional slices are complete.
- Run full normal, ASAN, UBSAN, and TSAN matrices exactly once, in Task 10.
- Do not mark the goal complete until every required artifact is valid or has an explicitly approved tested exclusion.

---

### Task 1: Automatic index health-event repair scheduling

**Files:**
- Modify: `include/cedar/index/index_catalog.h`
- Modify: `src/index/index_catalog.cc`
- Modify: `include/cedar/transaction/transaction_coordinator.h`
- Modify: `src/transaction/transaction_coordinator.cc`
- Modify: `src/tcypher/executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produce a bounded, deduplicated health event keyed by index ID, source SST identity, fragment identity, catalog generation, and failure class.
- Queue repair through existing typed optional maintenance admission without mutating query-pinned snapshots.
- Preserve base-scan fallback and hard propagation of source-SST corruption.

- [x] Add a failing regression proving corrupt sidecar fallback records one health event and schedules one repair without an explicit `RepairIndexes()` call.
- [x] Add failing regressions for duplicate observations, stale generation, close cancellation, repair failure, and successful reopen-visible repair.
- [x] Implement bounded health-event recording, deduplication, generation validation, and typed repair submission.
- [x] Run only the new health/repair tests plus existing index repair/drop/fallback focused tests with `-j1`; require GREEN and run `git diff --check`.

### Task 2: Concurrent runtime-feedback generation isolation

**Files:**
- Modify: `include/cedar/optimizer/runtime_feedback.h`
- Modify: `src/optimizer/runtime_feedback.cc`
- Modify: `src/tcypher/executor.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Retain the approved two-observation, 64-epoch decay, 256-epoch expiry, and independent LRU policy.
- Bind observations and application to the query-pinned plan shape, schema epochs, Manifest/catalog generation, statistics snapshot ID, and selectivity bucket.

- [x] Add a failing barrier-controlled regression in which an old query completes after index coverage/drop and statistics generations advance.
- [x] Assert that the old observation updates only its old key and cannot affect the new-generation plan choice.
- [x] Tighten observation publication only where the regression exposes a generation-identity gap; do not introduce a global feedback flush.
- [x] Run only runtime-feedback, pinned-context, index-generation, and statistics-generation focused tests with `-j1`; require GREEN and run `git diff --check`.

### Task 3: Minimal-instrumentation comparison and overhead gate

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `include/cedar/observability/metric_registry.h`
- Modify: `include/cedar/observability/telemetry_aggregator.h`
- Modify: `src/observability/telemetry_aggregator.cc`
- Modify: `include/cedar/benchmark/run_manifest.h`
- Modify: `src/benchmark/run_manifest.cc`
- Modify: `include/cedar/benchmark/regression_compare.h`
- Modify: `src/benchmark/regression_compare.cc`
- Modify: `include/cedar/benchmark/regression_gate.h`
- Modify: `src/benchmark/regression_gate.cc`
- Modify: `benchmarks/cedar_bench.cc`
- Modify: `benchmarks/cedar_bench_pair.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Produce a test-only minimal-instrumentation build identity while retaining mandatory Tier 0 correctness events.
- Compare compatible Tier 0/1 and minimal artifacts with at least five valid paired repetitions.
- Fail when median throughput overhead exceeds 2% or p99 latency overhead exceeds 5%.

- [x] Add failing artifact-schema and comparison-policy tests for build identity, compatibility, minimum pair count, throughput overhead, and p99 overhead.
- [x] Add the test-only build mode and serialize its exact instrumentation profile into every artifact.
- [x] Implement the paired overhead gate and `cedar_bench_pair --instrumentation-overhead` without weakening the existing production regression gate.
- [x] Run only observability schema, benchmark reader, paired comparison, and overhead-gate focused tests with `-j1`; require GREEN and run `git diff --check`.

### Task 4: Columnar release evidence campaign

- [ ] Generate format-1 artifacts proving zero whole-SST production reads, zero Blob payload reads during reference-copy compaction, bounded cache/compaction memory, corruption boundaries, concurrency/relocation behavior, codec capabilities, and clean-break rejection.
- [ ] Validate every artifact and bind commands, binary/source hashes, environment, resource profile, and workload identity.

### Task 5: HTAP correctness evidence campaign

- [ ] Archive serializability/model/concurrency/oracle evidence across commit, visible prefix, flush, compaction, crash, and reopen.
- [ ] Generate conflict-abort, fsync-latency, visible-prefix-stall, and write-amplification measurements using the current benchmark protocol.

### Task 6: HTAP resource-scheduling evidence campaign

- [ ] Generate production-scale fairness, deadline, contention, shutdown, and HTAP stress artifacts.
- [ ] Prove per-class grants, queues, rejection, cancellation, resource release, and restart reconstruction with nonzero production dimensions.

### Task 7: T-Cypher support-matrix evidence campaign

- [ ] Archive supported point/range/change/Expand/join/spill, concurrent snapshot, cancellation, resource-limit, reopen, and EXPLAIN ANALYZE artifacts.
- [ ] Retain deterministic tested exclusions for mixed fixed/variable `CHANGES` and scalar property access on a variable relationship path.

### Task 8: Temporal Index/CBO evidence campaign

- [ ] Archive base/index/hybrid/intersection/graph-order execution, randomized candidate completeness, Blob-hash boundaries, automatic repair/drop/reopen, runtime-feedback generation isolation, and per-SST resource accounting.
- [ ] Require physical counters to prove the intended path and nonzero resource dimensions actually executed.

### Task 9: Observability/benchmark release and paper campaign

- [ ] Run the instrumentation-overhead gate and approved baseline/candidate comparison with at least five compatible pairs.
- [ ] Generate workstation, paper, stress, external LDBC, PR/nightly/release, offline-regeneration, and claim-to-artifact outputs.
- [ ] Stop and record an external dependency if no approved clean-break production baseline or external LDBC input is available; do not substitute the legacy runtime or smoke binary.

### Task 10: One final unified verification and closure update

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

- [x] Freeze functional and artifact changes before starting the final matrix at `66bf270efd6150fa80e713f4d4fd2d3ea1e75407`.
- [x] Run normal, ASAN, UBSAN, and TSAN full CTest matrices once each with `-j1` and stable output logs; all four pass 886/886.
- [x] Run the final fault/crash/reopen/oracle, scheduler/HTAP, and benchmark reproducibility aggregate gates; they pass 64/64, 80/80 and 28/28, with 11/11 offline strict-reader regenerations.
- [x] Bind logs and binaries to `results/release-closure-20260723-final-matrix`, validate hashes and provenance, and run `git diff --check`.
- [ ] Update every completion-matrix row to code/test/artifact evidence or an approved tested exclusion; mark the active goal complete only when no required row remains open.

## Verification Policy

| Phase | Allowed verification |
|---|---|
| Tasks 1-3 | Focused normal RED/GREEN tests only; no full normal/sanitizer matrix |
| Tasks 4-9 | Artifact-specific commands and validators only; no full sanitizer matrix |
| Task 10 | One full normal matrix, one full ASAN matrix, one full UBSAN matrix, one full TSAN matrix, then final aggregate gates |

## Current Baseline

- Frozen final matrices: normal, ASAN, UBSAN and TSAN each pass 886/886 with `-j1` and execute once each.
- Immutable maintenance resource admission: functionally complete; its 20 implementation steps are checked.
- Functional slices 1-3 are complete: automatic index health repair, concurrent feedback generation isolation, and minimal-instrumentation overhead gating. The overhead runner records each arm profile and emits a dedicated gate; its five-pair tiny smoke was `NOISY`, so it is not release evidence.
- Remaining release/paper scope: six evidence campaigns plus the final unified verification.

## Evidence Batch Status

Frozen evidence roots are generated after the tracked source and documents are
frozen, then hash-validated:

- `results/release-closure-20260723-columnar-final`: 16/16 focused physical-path and codec tests.
- `results/release-closure-20260723-htap-correctness-final`: 16/16 focused model, serializability, visible-prefix, fault and oracle tests plus a schema-3 HTAP benchmark artifact.
- `results/release-closure-20260723-htap-resource-final`: 35/35 database and scheduler/executor admission tests.
- `results/release-closure-20260723-tcypher-final`: 27/27 supported-shape and approved-exclusion tests.
- `results/release-closure-20260723-temporal-index-cbo-final`: 26/26 access-path, repair, feedback and resource tests.
- `results/release-closure-20260723-observability-final`: ten schema-3 strict-reader-validated paired runs.

Every CTest root declares database format 1, current clean-break naming,
source/dirty identity, exact command, binary/log hashes and honest eligibility;
`cedar_evidence_verify` validates its manifest and `SHA256SUMS` binds its
files. Benchmark roots are read by the schema-3 offline strict reader. They
remain `release_gate_eligible: false`: the instrumentation smoke is `NOISY`;
production conflict-abort, fairness/deadline stress, per-class queue/grant
telemetry, an approved clean-break production baseline, external LDBC input
and a separately provisioned paper/stress host remain open. Task 10's local
verification is complete; these release/paper dependencies keep the overall
goal active until they are produced or explicitly approved as tested
exclusions.

The exact external inputs, host profiles, prohibited substitutions, campaigns
and acceptance rules are frozen in
`docs/superpowers/plans/2026-07-23-cedar-external-release-evidence-contract.md`.
