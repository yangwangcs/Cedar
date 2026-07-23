# Cedar Benchmark Artifact Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with verification checkpoints.

**Goal:** Make the existing `cedar_bench` protocol emit a complete, reproducible measurement artifact with latency distributions and explicit optional telemetry/profile outputs.

**Architecture:** Keep benchmark drivers on the public `CedarDatabaseV2` API. Extend the in-memory artifact summary with canonical JSON payloads for histograms, traces, and explain profiles; the artifact writer persists those payloads atomically and records presence in `summary.json`. The benchmark entry point derives open-loop timing and latency distribution data from measured samples without adding storage-specific dependencies.

**Tech Stack:** C++17, existing Cedar `Status`, `Histogram`, benchmark artifact writer, GoogleTest.

## Global Constraints

- Correctness and reopen verification must pass before a report emits headline latency.
- Benchmark artifacts must retain provenance and explicit absence of optional telemetry.
- No benchmark code may bypass WAL, Manifest, snapshots, or public database APIs.
- Existing dirty worktree changes must be preserved.

### Task 1: Artifact payloads and protocol tests

**Files:**
- Modify: `include/cedar/benchmark/artifact_writer.h`
- Modify: `src/benchmark/artifact_writer.cc`
- Modify: `tests/test_correctness_kernel.cc`

- [ ] Write tests asserting histogram/trace/explain payload paths are emitted, summary JSON records payload presence, and incomplete verification suppresses headline latency.
- [ ] Run the focused tests and observe the new assertions fail.
- [ ] Add payload fields and artifact paths, serialize them, and write them atomically when present.
- [ ] Re-run focused tests and then the existing benchmark/observability tests.

### Task 2: Measurement distribution in `cedar_bench`

**Files:**
- Modify: `benchmarks/cedar_bench.cc`
- Modify: `include/cedar/benchmark/artifact_writer.h`
- Modify: `src/benchmark/artifact_writer.cc`
- Modify: `src/benchmark/report_builder.cc`

- [ ] Add measured count, throughput, queue/service latency fields and a bounded histogram JSON payload derived from samples.
- [ ] Populate these fields only after successful measurement and retain failed samples for diagnosis.
- [ ] Include the distribution summary in `summary.json` and `report.md`.
- [ ] Run `cedar_bench` on a small deterministic Cedar-TG dataset and inspect all artifact files.

### Task 3: Regression verification

**Files:**
- No production changes unless verification exposes a defect.

- [ ] Run the focused benchmark tests, `test_correctness_kernel`, CTest, and `git diff --check`.
- [ ] Record remaining observability/benchmark specification gaps in `.superpowers/sdd/progress.md` without claiming full specification completion.

### Task 4: Offline report regeneration

**Files:**
- Create: `include/cedar/benchmark/artifact_reader.h`
- Create: `src/benchmark/artifact_reader.cc`
- Create: `benchmarks/cedar_bench_report.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/test_correctness_kernel.cc`

- [x] Write a round-trip test that archives manifest, summary, and verification JSON, removes `report.md`, and requests report regeneration without opening a database.
- [x] Run the focused test and observe the missing reader API failure.
- [x] Add a strict schema-v2 artifact reader that validates field types, manifest `run_id`, and protocol completeness before atomically replacing `report.md`.
- [x] Add `cedar_bench_report <run-directory>` as a database-independent regeneration command.
- [x] Add corruption and unsupported-schema tests and run the focused benchmark artifact suite.
- [x] Run the command against a real `cedar_bench` archive and verify the regenerated report is byte-identical.
