# Cedar Kernel-Only Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Lean benchmark execution so all benchmark databases use Cedar Kernel mode.

**Architecture:** `KernelBenchmarkOptions` no longer models an execution profile. The option parser rejects the removed CLI flag, `MakeBenchmarkDatabaseOptions` fixes Kernel mode to true, and the campaign runner only invokes Kernel cases. CSV output removes the obsolete profile column.

**Tech Stack:** C++23, CMake, GoogleTest, Bash.

## Global Constraints

- Do not retain any Lean compatibility option or execution path.
- Preserve Cedar single RocksDB WAL and RocksDB WAL/recovery/MemTable/VersionSet/MANIFEST ownership.
- Preserve authoritative columnar facts and Cedar public semantics.

---

### Task 1: Remove the benchmark profile interface

**Files:**
- Modify: `benchmarks/cedar_kernel_bench_options.h`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `benchmarks/cedar_kernel_bench_workload.cc`
- Test: `tests/performance/test_kernel_bench_options.cc`
- Test: `tests/performance/test_kernel_bounded_benchmark.cc`

- [x] Remove `BenchmarkExecutionProfile`, `execution_profile`, and `BenchmarkExecutionProfileName`.
- [x] Remove `--profile` parsing; unknown options continue to return `InvalidArgument`.
- [x] Set `database_options.production.kernel_mode = true` unconditionally.
- [x] Replace Lean-specific tests with Kernel-only option rejection and invariant tests.
- [x] Run `cmake --build build-debug-perf --target test_kernel_bench_options test_kernel_bounded_benchmark -j4` and both binaries.

### Task 2: Make evidence production Kernel-only

**Files:**
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Modify: `benchmarks/run_cedar_maintenance_campaign.sh`
- Test: `tests/performance/test_kernel_benchmark_csv.cmake`

- [x] Remove the CSV profile field and update its contract from 63 to 62 columns.
- [x] Remove Lean baseline exception handling and all Lean campaign invocations.
- [x] Keep Kernel warm, 60/300-second preflight, and 1800-second sustained campaigns.
- [x] Run `bash -n benchmarks/run_cedar_maintenance_campaign.sh` and the CSV contract test.

### Task 3: Verify and commit

**Files:**
- Modify: `docs/superpowers/specs/2026-08-20-cedar-kernel-only-benchmark-design.md`
- Modify: `docs/superpowers/plans/2026-08-20-cedar-kernel-only-benchmark.md`

- [x] Run `git diff --check`.
- [x] Run `ctest --test-dir build-debug-perf --output-on-failure -j4`.
- [ ] Commit the Kernel-only benchmark removal with its tests and documentation.
