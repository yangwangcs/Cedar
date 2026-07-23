# Cedar External Release Evidence Contract

Date: 2026-07-23

Purpose: define the inputs and execution environment required to move the six-design closure from locally verified functional evidence to an honest release/paper claim. This contract does not permit the removed runtime, an ablation binary, or a smoke run to stand in for production evidence.

## Frozen local checkpoint

- Source under test: `66bf270efd6150fa80e713f4d4fd2d3ea1e75407`.
- Internal database format: `1`.
- Final local verification: normal, ASAN, UBSAN and TSAN each 886/886 with `-j1`; fault/oracle 64/64; scheduler/HTAP 80/80; benchmark aggregate 28/28; offline strict reader 11/11.
- Evidence root: `results/release-closure-20260723-final-matrix`.
- Claim ledger: `results/release-closure-20260723-final-matrix/CLAIM-TO-ARTIFACT.md`.

## Required external inputs

### Approved production baseline

Supply an independently preserved clean-break production binary and its source commit. It must:

- open only database format 1;
- use current non-versioned public naming;
- have a distinct SHA-256 from the candidate;
- implement the same workload, durability, cache and resource contracts;
- be explicitly approved as the comparison baseline.

The removed legacy runtime, `origin/main` legacy layout, `cedar_bench_alt`, and sanitizer binaries are prohibited substitutes.

### External LDBC-derived input

Supply the source dataset or an accessible immutable location together with:

- exact LDBC/SNB release identity and source hash;
- license identifier/text and redistribution constraints;
- deterministic transformation policy for valid time, commit time and corrections;
- input and transformed-output SHA-256 values;
- expected vertex, edge and event counts.

The result must be described as LDBC-derived temporal input, not as an official LDBC temporal benchmark.

### Execution hosts

Archive one host/environment manifest for each claimed profile. The current declared sizes are:

| Profile | Vertices | Edges | Property events per vertex | Workers |
|---|---:|---:|---:|---:|
| workstation | 1,000,000 | 4,000,000 | 4 | 8 |
| paper | 10,000,000 | 40,000,000 | 8 | 16 |
| stress | 25,000,000 | 100,000,000 | 16 | 32 |

The host must have enough RAM and durable storage to complete the declared profile without shrinking it. Record CPU, logical cores, RAM limit, kernel, filesystem/device, compiler/flags, thermal/power policy and free space before and after the campaign.

## Required campaigns

1. Run at least five alternating baseline/candidate pairs per claimed configuration through `cedar_bench_pair`. Preserve every child artifact, `paired-runs.json`, `regression-gate.json` and binary hash.
2. Run cold-process/database, cold-database/warm-process, warm-metadata, warm-working-set and steady-state-maintenance cache modes where applicable.
3. Run durable ingestion, bitemporal reads, analytical scans, graph paths, index equality, maintenance, HTAP-balanced and recovery workloads.
4. Run the production scheduler campaign with nonzero per-class grant, queue, reject, cancel, release and restart reconstruction counters; report fairness, deadline lateness and commit latency under analytical saturation.
5. Run the complete persistence-boundary fault/reopen campaign and compare every durable outcome with the independent oracle.
6. Run Columnar peak-memory/selective-I/O/reference-copy measurements, T-Cypher concurrent snapshot and disk-full/spill-corruption scenarios, and Temporal Index/CBO nonzero base/index/hybrid/intersection/graph-order candidate/resource distributions.
7. Strict-read every artifact, regenerate every report offline, verify all SHA-256 ledgers, and preserve raw distributions, plots, bootstrap confidence intervals, failure counts and limitations.

## Acceptance

The overall goal may be marked complete only when:

- every campaign artifact is format 1/schema 3 where applicable;
- all provenance and strict-reader checks pass;
- no run silently shrinks its declared profile;
- every performance claim points to an approved paired gate with at least five valid pairs and acceptable variability;
- all remaining completion-matrix rows are `PROVEN` or an explicitly user-approved tested exclusion;
- the claim-to-artifact ledger is promoted from local audit to release/paper ledger.

If any required input is unavailable, record `EXTERNAL-BLOCKED`; do not replace it with zero-valued telemetry, old formats, legacy binaries or CI smoke evidence.
