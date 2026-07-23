# Process-level Paired Benchmark Closure

Status: Implemented in the current dirty worktree; release evidence still
requires a distinct approved baseline binary.

## Contract

`cedar_bench_pair` launches two explicit Cedar benchmark binaries in alternating
baseline/candidate order. Each arm receives the same seed, data dimensions,
workload, durability mode, cache preparation, and worker profile. The tool
requires at least five pairs, rejects identical binary hashes, and never treats
a child failure or invalid artifact as a successful sample.

Each child writes into an isolated pair directory. The driver validates
`manifest.json`, `summary.json`, and `verification.json` through
`ReadBenchmarkArtifact`, checks current database format 1 and run-id
provenance, checks binary-hash provenance, and derives samples only from a
complete load/result/reopen protocol. Existing bootstrap confidence intervals
and regression policy are reused through `WriteBenchmarkRegressionGate`.

The output directory contains:

- `regression-gate.json`: PASS/FAIL/NOISY/INCOMPATIBLE/INVALID gate with policy,
  paired statistics, and confidence intervals;
- `paired-runs.json`: pair/arm/status/binary-hash/run-id/artifact mapping;
- one durable artifact directory per pair and arm.

The tool returns zero only for a passing release gate. Any child failure,
missing artifact, provenance mismatch, incompatible baseline key, or fewer than
five valid pairs remains a non-zero, machine-readable failure.

## Verification

The artifact reader regression
`BenchmarkArtifactTest.ReadsVerifiedArtifactForProcessPairedBenchmark` covers
offline validation. A smoke run with distinct current and alternate binaries
completed five pairs and produced a PASS gate with ten verified artifacts;
using a sanitizer binary with a different workload/resource key correctly
produced INCOMPATIBLE rather than a false comparison.
