# Cedar External Release Evidence Contract

Date: 2026-07-23

Purpose: define the inputs and execution environment required to move the six-design closure from locally verified functional evidence to an honest production-release claim. Paper evidence and external LDBC are explicitly outside the active goal. This contract does not permit the removed runtime, an ablation binary, or a smoke run to stand in for production evidence.

## Frozen local checkpoint

- Source base commit: `8c3369bf7bb6da30c08e95f7109eea7b2937080c`, with the archived manifests explicitly recording the current dirty worktree.
- Internal database format: `1`.
- Latest dirty-working-tree verification: normal, ASAN, UBSAN and TSAN each 928/928 with `-j1`, with no sanitizer or race diagnostic. This is local verification, not release evidence.
- Latest self-contained full-matrix evidence root: `results/release-closure-20260725-final-matrix-r10`, which retains the preceding 916/916 plus independent provenance 1/1 checkpoint.
- Current focused roots: `results/release-closure-20260725-{columnar,htap,resource,tcypher,temporal-index-cbo,observability}-functional-r10`; all six archive their current binary, record exact executable test filters, and pass both the directory-level SHA-256 verifier and `shasum -a 256 -c SHA256SUMS`.
- Real CI campaign roots: `results/goal-current-maintenance/009740a8088c05930253999cbe05eb52f3ffc37b0d59a40af4b438d2f279f386`, `results/goal-current-scheduler-saturation/6ffee3ad18e4836d88c17b48b5ef045eab81c517ece2ae459e265da95f5e8218`, `results/goal-current-index-path-matrix/4fbeaaa84d1eea590c3a01fa58bf231f682b4372b2bc0e6727278393f3d742ae`, and the two typed spill-fault roots recorded in the completion matrix.
- Production preflight is now executable and fail-closed: named workstation/stress
  profiles cannot be shrunk through worker overrides; CPU, memory, free durable
  storage and filesystem/device provenance must be complete; every production
  artifact must carry a clean full source commit; and paired release mode requires
  an externally approved distinct baseline SHA-256. Linux resource provenance
  additionally fails closed outside the initial cgroup namespace, and paired
  binaries are hashed and executed from the same private snapshots. The self-contained negative
  campaign root is `results/release-closure-20260725-production-preflight-r2`.

## Required external inputs

### Approved production baseline

Supply an independently preserved clean-break production binary and its source commit. It must:

- open only database format 1;
- use current non-versioned public naming;
- have a distinct SHA-256 from the candidate;
- implement the same workload, durability, cache and resource contracts;
- be explicitly approved as the comparison baseline.

The removed legacy runtime, `origin/main` legacy layout, `cedar_bench_alt`, and sanitizer binaries are prohibited substitutes.

### Explicitly out of scope

- External LDBC-derived input and all LDBC licensing/transformation evidence.
- Paper-profile execution, paper claims, paper plots and publication-specific ledgers.

These exclusions do not remove any functional or production-release workload, durability, fault, observability or paired-baseline requirement.

### Execution hosts

Archive one host/environment manifest for each claimed profile. The current declared sizes are:

| Profile | Vertices | Edges | Property events per vertex | Workers |
|---|---:|---:|---:|---:|
| workstation | 1,000,000 | 4,000,000 | 4 | 8 |
| stress | 25,000,000 | 100,000,000 | 16 | 32 |

The host must have enough RAM and durable storage to complete the declared profile without shrinking it. Record CPU, logical cores, RAM limit, kernel, filesystem/device, compiler/flags, thermal/power policy and free space before and after the campaign.

### Current local blocker snapshot

The 2026-07-25 development host reports 16 GiB RAM, 8 logical CPUs and about
17.5 GiB free workspace storage. No archived manifest has
`approved_production_baseline: true`; the only matching paired-smoke manifest
explicitly records `false`. This host and smoke baseline therefore cannot be
used to claim the declared stress profile or approved paired production gate.
Record this state as `EXTERNAL-BLOCKED`; do not silently shrink the profile.

The production CLI now enforces that snapshot before dataset generation or
result-root creation: workstation is rejected for insufficient RAM, stress is
rejected for insufficient CPU, missing approval exits with an argument error,
and an otherwise valid local approval still fails the host floor. These
negative results prove the gate fails closed; they are not positive production
campaign evidence and do not authorize release.

`cedar_production_campaign` is the production-host entry point. It freezes a
75-command plan: all 13 public workload families across all five cache modes
through five-or-more alternating approved-baseline/candidate pairs, followed by
all ten typed fault/reopen scenarios. Before creating the campaign root it
requires baseline approval, distinct immutable binary snapshots, clean full
source commits, format 1, Tier 0/1 instrumentation, and the exact workstation or
stress host floor. It archives the baseline, candidate, and pair runner, runs
the archived bytes, and refuses to resume a command unless the gate, paired
ledger, binary identity, profile/workload/cache identity, production metrics,
child artifact strict reader, and offline report regeneration all pass again.
For paired commands, resume reconstructs both sample sets from the child
artifacts and reruns the default regression comparison instead of trusting the
archived PASS document. On Linux each child is executed from the already
verified executable-snapshot descriptor, closing the path-replacement window.
The schema-2 JSON campaign index binds unambiguous argv arrays, approval identity, source/binary
identities, complete host provenance, child evidence paths and child evidence
hashes. Finalization revalidates all 75 commands, seals a complete ledger
snapshot, revalidates all commands again, and requires the second complete
path/SHA set to equal the first before the final safe readback. The final
`SHA256SUMS` covers executables, configuration, state, paired gates, and
artifact protocol/report files and must contain exactly the expected path set.
Database directories are pruned from archive traversal and remain
explicitly outside that top-level ledger and are validated by each workload's
reopen and result oracle.

Example on a qualified host, after setting the five
`CEDAR_APPROVED_*` baseline variables:

```sh
build-release/cedar_production_campaign \
  /approved/cedar_bench /candidate/cedar_bench workstation 20260725 \
  /durable-results/cedar-workstation-20260725 5 1
```

Re-running the identical command resumes only strictly revalidated completed
commands. A mismatched configuration or incomplete existing command root is
rejected without deleting evidence.

## Required campaigns

1. Run at least five alternating baseline/candidate pairs per claimed configuration through `cedar_bench_pair`. Preserve every child artifact, `paired-runs.json`, `regression-gate.json` and binary hash.
2. Run cold-process/database, cold-database/warm-process, warm-metadata, warm-working-set and steady-state-maintenance cache modes where applicable.
3. Run durable ingestion, bitemporal reads, analytical scans, graph paths, index equality, index-path-matrix, maintenance-cycle, scheduler-saturation, HTAP-balanced and recovery workloads.
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
- the claim-to-artifact ledger is promoted from local audit to production-release ledger.

If any required input is unavailable, record `EXTERNAL-BLOCKED`; do not replace it with zero-valued telemetry, old formats, legacy binaries or CI smoke evidence.
