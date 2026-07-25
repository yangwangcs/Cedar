# Cedar Six-Design Batched Final Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to execute this plan in the current dirty worktree. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the remaining functional slices, generate the six production-release evidence families in batches, and run the full normal/ASAN/UBSAN/TSAN matrix at final local closure. Paper evidence and external LDBC are outside the active goal.

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

### Task 9: Observability/benchmark production-release campaign

- [ ] Run the instrumentation-overhead gate and approved baseline/candidate comparison with at least five compatible pairs.
- [ ] Generate workstation, stress, PR/nightly/release, offline-regeneration, and claim-to-artifact outputs.
- [ ] Stop and record an external dependency if no approved clean-break production baseline or adequately provisioned production host is available; do not substitute the legacy runtime or smoke binary.

### Task 10: One final unified verification and closure update

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

- [x] Historical checkpoint: freeze functional and artifact changes before starting the earlier final matrix at `66bf270efd6150fa80e713f4d4fd2d3ea1e75407`.
- [x] Historical checkpoints: normal, ASAN, UBSAN, and TSAN full matrices each passed 886/886, later 904/904, 914/914 and archived r10 916/916 with `-j1`; the latest dirty working tree passes 928/928 in all four modes.
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

- Latest dirty-working-tree matrices: normal, ASAN, UBSAN and TSAN each pass 928/928 with `-j1`; they are local verification only. The latest self-contained root remains `results/release-closure-20260725-final-matrix-r10` at 916/916 plus provenance 1/1.
- Immutable maintenance resource admission: functionally complete; its 20 implementation steps are checked.
- Functional slices 1-3 are complete: automatic index health repair, concurrent feedback generation isolation, and minimal-instrumentation overhead gating. The overhead runner records each arm profile and emits a dedicated gate; its five-pair tiny smoke was `NOISY`, so it is not release evidence.
- The real CI `maintenance-cycle`, `scheduler-saturation`, `index-path-matrix` and typed spill-fault campaigns are complete and strict-reader verified; six corrected current-binary focused roots pass the directory verifier.
- Remaining production-release scope: production-scale executions on an adequately provisioned host plus an approved distinct production baseline and paired comparison.
- The executable production preflight now rejects named-profile worker shrink,
  incomplete CPU/RAM/filesystem provenance, dirty or malformed source identity,
  absent/mismatched baseline approval and undersized hosts before data generation.
  Its negative campaign is release-ineligible by design; the external host and
  approved-baseline requirements remain open.
- A fail-closed `cedar_production_campaign` runner now materializes the frozen
  65 paired workload/cache configurations plus ten fault/reopen configurations,
  archives and executes immutable baseline/candidate/pair-runner bytes, strictly
  rebuilds paired regression decisions from child evidence on resume, binds
  argv/approval/host/child hashes in its index, and safely verifies an exact-set
  scoped SHA-256 ledger after writing it. Finalization uses two semantic passes
  and requires the second complete ledger snapshot to match the first
  path-for-path and hash-for-hash; database trees are pruned. Linux children
  execute from verified snapshot descriptors. This closes the production orchestration gap only; no
  qualified-host positive campaign has run, the current candidate build is
  dirty, and no approved baseline exists.

## Evidence Batch Status

Frozen evidence roots were generated after the tracked source and documents
were frozen. The historical roots below retain their format-1 manifests and
focused logs, but are not self-contained byte-verifiable release evidence:
their manifests name a test binary outside the evidence root and their
`SHA256SUMS` ledgers do not archive that binary. They therefore fail the
current directory-level `cedar_evidence_verify` integrity contract and remain
archive-incomplete.

- `results/release-closure-20260723-columnar-final`: 16/16 focused physical-path and codec tests; binary archive missing.
- `results/release-closure-20260723-htap-resource-final`: 35/35 database and scheduler/executor admission tests; binary archive missing.
- `results/release-closure-20260723-tcypher-final`: 27/27 supported-shape and approved-exclusion tests; binary archive missing.
- `results/release-closure-20260723-temporal-index-cbo-final`: 26/26 access-path, repair, feedback and resource tests; binary archive missing.
- `results/release-closure-20260724-htap-correctness-final`: self-contained current-source binary plus 14/14 focused serializability, recovery and persistence tests; production stress remains missing.
- `results/release-closure-20260724-observability-final`: self-contained current-source binary plus 27/27 metrics, telemetry, artifact and gate tests; production-scale paired evidence remains missing.
- `results/release-closure-20260724-final-matrix`: self-contained current-source binary plus normal, ASAN, UBSAN and TSAN 887/887 logs.

Current replacements are self-contained and byte-verifiable:

- `results/release-closure-20260725-columnar-functional-r10`: 74/74.
- `results/release-closure-20260725-htap-functional-r10`: 36/36.
- `results/release-closure-20260725-resource-functional-r10`: 57/57.
- `results/release-closure-20260725-tcypher-functional-r10`: 416/416, including one Blob-bearing corpus across active/frozen handoff/live SST/verified explicit compaction/reopen/nonzero Blob relocation/reopen-after-relocation.
- `results/release-closure-20260725-temporal-index-cbo-functional-r10`: 57/57, including verified compact/repair/drop/reopen/ID non-reuse.
- `results/release-closure-20260725-observability-functional-r10`: 58/58, including executable snapshot restoration detection, close-on-exec descriptor enforcement and namespaced-cgroup fail-close regressions.
- `results/release-closure-20260725-source-contract-r1`: 2/2 deterministic static-contract tests run from an archived 194-file input snapshot; exact durable-mutation, Manifest publication, all-direct-delete and retained-Stats inventories plus the reviewed persistent-delete subset; zero direct diagnostics, duplicate latency/cache metrics and unbound performance claims; nine negative fixtures preserve or restore accepted bytes, and a dedicated startup test recovers a stranded accepted-output backup.
- `results/release-closure-20260724-final-matrix-r3`: superseded historical normal, ASAN, UBSAN and TSAN 904/904.
- `results/release-closure-20260725-final-matrix-r10`: superseding normal, ASAN, UBSAN and TSAN 916/916 after the executable-binding, lifecycle-compaction, close-on-exec and cgroup-provenance fixes; independent provenance remains 1/1.

`cedar_evidence_verify` now verifies every ledger byte plus the manifest's
binary/log bindings, rather than trusting declared audit booleans. None of the
four historical CTest roots currently meets that stronger contract. The
current `20260725` final-matrix-r10, all six `-functional-r10` roots and the
source-contract root pass the same verifier. Benchmark roots remain read by the
schema-3 offline strict reader.
Production-scale conflict/stall/multi-shard, persistence-boundary, scheduler,
snapshot/disk-pressure, CBO/resource-distribution campaigns, an approved
clean-break production baseline, and adequately provisioned workstation/stress
hosts remain open. The instrumentation smoke remains
`release_gate_eligible: false` because it is `NOISY`; the overall goal stays
active until the functional production gaps are produced or explicitly approved
as tested exclusions.

The exact external inputs, host profiles, prohibited substitutions, campaigns
and acceptance rules are frozen in
`docs/superpowers/plans/2026-07-23-cedar-external-release-evidence-contract.md`.
Paper evidence and external LDBC remain explicitly out of scope.
