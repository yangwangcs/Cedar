# Cedar Observability, Verification, and Reproducible Benchmark Design

Date: 2026-07-17

Status: Approved authoritative design; functional implementation substantially complete; release/paper closure remains incomplete and is tracked in `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

Depends on:

- `2026-07-17-cedar-htap-design.md`
- `2026-07-17-cedar-columnar-design.md`
- `2026-07-17-cedar-tcypher-vectorized-execution-design.md`
- `2026-07-17-cedar-temporal-index-cbo-design.md`
- `2026-07-17-cedar-htap-resource-scheduling-design.md`

## 1. Purpose

This document defines Cedar's sixth design stage: a unified observability model and an independent, reproducible benchmark and regression harness for the single-node temporal graph HTAP engine.

The preceding designs make architectural claims about:

- durable serializable commits;
- stable bitemporal snapshots;
- page-selective columnar reads;
- delayed Blob materialization;
- vectorized graph execution;
- candidate indexes and cost-based planning;
- fair HTAP resource scheduling.

Those claims are not established by one QPS number. Cedar needs a measurement system that can answer which pages, bytes, tasks, snapshots, and maintenance actions produced a result, and a benchmark harness that can reproduce the answer on a known data and hardware profile.

This stage adds:

- always-on low-overhead metrics with bounded cardinality;
- sampled query and scheduler traces;
- structured `EXPLAIN ANALYZE` profiles;
- correctness and fault telemetry that never becomes a durability dependency;
- a deterministic dataset and workload generator;
- warm, cold, durability, concurrency, maintenance, and recovery benchmark modes;
- paired ablation and regression methodology;
- machine-readable artifacts and reviewable reports;
- release gates that reject performance claims without correctness and provenance evidence.

## 2. Relationship to Earlier Designs

### 2.1 Authoritative Runtime Contracts

The correctness kernel, columnar, T-Cypher V1, temporal index/CBO, and HTAP scheduler define the events and operations being measured. This document does not change their semantics.

In particular:

- `commit_seq`, HLC, and `visible_seq` remain the transaction truth;
- `QuerySnapshot` and VersionSet pinning remain the query truth;
- Page, Block, SST, Blob, index sidecar, and scheduler metrics describe work but cannot make a corrupt or uncommitted result valid;
- benchmark instrumentation may observe a task or delay it slightly, but it cannot reorder durability dependencies;
- a benchmark run using unsafe durability is never a headline comparison for durable Cedar.

### 2.2 Observability Refinements

1. Metrics use stable names and bounded label sets. Entity IDs, property values, query literals, and arbitrary logical keys are never labels.
2. Thread-local counters and histograms are merged at batch, task, or query boundaries. Per-row global locks are forbidden.
3. Detailed tracing is sampled or explicitly enabled for a run. Dropped spans and metric export failures are counted.
4. Benchmark artifacts include enough environment and source information to reproduce a result, not only a summary table.
5. Correctness gates run before throughput gates. A faster run with a missing event, unstable snapshot, or durability violation is invalid.

## 3. Current Measurement Problems

The current repository has:

1. three narrow unit-test files and no end-to-end temporal, SST, Blob, crash, or concurrency harness;
2. independent `GetStats()` structs with incompatible names and scopes;
3. no metric registry, stable metric schema, histogram standard, trace context, or export contract;
4. `EXPLAIN` for the old row executor but no `EXPLAIN ANALYZE` with page, Blob, vector, or scheduler counters;
5. no deterministic dataset generator or workload manifest;
6. no distinction between cold cache, warm cache, durability mode, or background maintenance state;
7. no fixed seed, environment manifest, binary hash, or data checksum attached to performance results;
8. no open-loop latency methodology, making coordinated omission likely in write/query tests;
9. no paired ablation protocol for page pruning, indexes, vectorization, cache admission, or scheduling;
10. README performance numbers without a verifiable artifact and configuration;
11. no explicit rule for confidence intervals, variance, outliers, or regression thresholds;
12. no artifact retention or result-schema versioning.

## 4. Goals and Non-Goals

### 4.1 Goals

1. Make every correctness and performance claim traceable to structured evidence.
2. Measure durable writes, visible-prefix lag, historical reads, graph paths, Blob behavior, indexes, and maintenance separately.
3. Attribute latency to queueing, CPU, page I/O, Blob I/O, spill, compaction, WAL fsync, or scheduler pressure.
4. Provide a deterministic small model suite suitable for CI and a scalable suite suitable for papers and releases.
5. Compare base scans, indexes, hybrid plans, cache states, and scheduler profiles without changing query semantics.
6. Detect correctness regressions before allowing performance aggregation.
7. Keep instrumentation overhead bounded and measurable.
8. Produce reports that can be reviewed without access to the live database.

### 4.2 Non-Goals

This stage does not add:

- a distributed telemetry service;
- an external monitoring dependency required for database correctness;
- user-facing query profiling that exposes property values or graph identities by default;
- a benchmark claim against another database without a semantic and durability parity adapter;
- automatic tuning that silently changes correctness or isolation settings;
- a paper result based only on synthetic data or only on one cache state;
- retention of unbounded raw traces or per-entity metrics.

## 5. Observability Architecture

### 5.1 Telemetry Tiers

Cedar has four telemetry tiers:

```text
Tier 0: correctness events and bounded counters, always on
Tier 1: histograms and gauges, always on with thread-local aggregation
Tier 2: sampled query/task spans, configurable sampling rate
Tier 3: deterministic diagnostic trace and per-operator profile, benchmark/admin mode
```

Tier 0 includes commit failures, corruption, snapshot errors, write stalls, pressure transitions, and dropped telemetry. It cannot be disabled.

Tier 1 is the default operational view. Tier 2 is sampled by query class and tail latency. Tier 3 is enabled only for a bounded query, benchmark run, or explicit diagnostic window.

### 5.2 Telemetry Pipeline

The pipeline is:

```text
thread-local events/counters
  -> bounded per-component buffers
  -> asynchronous TelemetryAggregator
  -> snapshot registry and optional exporters
```

Telemetry buffers are memory-accounted under the resource governor. When full, low-priority spans are dropped and a drop counter is incremented. Correctness events use a reserved small ring and are never silently dropped; if that ring cannot accept a fatal event, the process records a synchronous diagnostic before returning the error.

Exporters are optional:

- a local JSON snapshot for diagnostics;
- a Prometheus-compatible metrics view;
- OpenTelemetry-compatible spans when configured;
- benchmark artifact writers.

No exporter is on the WAL or DecisionLog durability path.

### 5.3 IDs and Clock Domains

Every request receives:

```text
database_instance_id
query_id or transaction_id
execution_id
parent_task_id
```

Durations use a monotonic steady clock. User-visible temporal values use HLC or valid-time timestamps. Wall-clock timestamps appear only in artifact metadata and are not used to order runtime spans.

### 5.4 Privacy and Cardinality

Metric labels are fixed enums or bounded buckets:

```text
work_class, entity_type, column_type, operator_kind,
error_kind, pressure_state, io_class, cache_kind,
storage_device_class, schema_epoch_bucket
```

Query text, literals, logical keys, Blob hashes, and entity IDs are excluded from labels. Diagnostic traces may include normalized query shape and hashed identifiers only when explicitly requested. Raw values are never exported by default.

## 6. Metric Model

### 6.1 Metric Types

The registry supports:

```text
Counter     monotonically increasing event count
Gauge       current value or sampled state
Histogram   mergeable latency/size distribution
Event       bounded structured point-in-time record
Span        sampled begin/end interval with parent context
```

Metric names are versioned. A rename or unit change creates a new metric version rather than silently changing historical meaning.

### 6.2 Common Fields

Every exported snapshot contains:

```text
metric_schema_version
database_format_version
language_version
process_start_id
resource_profile_id
catalog_generation
version_set_id
stats_snapshot_id
```

Metrics that do not apply to a generation use an explicit `unknown` value rather than an omitted or misleading label.

### 6.3 Histograms

Latency and size histograms use a mergeable high-dynamic-range representation with nanosecond or byte units declared in the schema. The implementation uses a vetted HDR-style histogram library or an equivalent bounded logarithmic representation; Cedar does not invent an untested percentile algorithm.

Every latency report includes at least:

```text
count, min, p50, p95, p99, p99.9, max, sum
```

Histogram overflow, underflow, and dropped samples have separate counters.

## 7. Metric Families

### 7.1 Durability and Transactions

```text
cedar_txn_started_total
cedar_txn_committed_total
cedar_txn_aborted_total
cedar_txn_conflict_total
cedar_txn_commit_latency_ns
cedar_txn_prepare_latency_ns
cedar_txn_decision_latency_ns
cedar_txn_visible_prefix_lag_seq
cedar_wal_append_bytes_total
cedar_wal_fsync_latency_ns
cedar_decisionlog_fsync_latency_ns
cedar_prepared_txn_age_ns
cedar_commit_completion_stall_total
```

The metric labels distinguish snapshot versus strict mode and work class, not transaction IDs.

### 7.2 Query and T-Cypher

```text
cedar_query_started_total
cedar_query_completed_total
cedar_query_failed_total
cedar_query_cancelled_total
cedar_query_queue_delay_ns
cedar_query_latency_ns
cedar_query_snapshot_age_ns
cedar_query_result_rows_total
cedar_query_result_intervals_total
cedar_query_spill_bytes_total
cedar_query_memory_peak_bytes
cedar_query_plan_cache_hit_total
cedar_query_admission_reject_total
```

Additional bounded dimensions include query class, temporal mode, plan shape hash bucket, and terminal error class.

### 7.3 Operator and Vector Runtime

```text
cedar_operator_input_rows_total
cedar_operator_output_rows_total
cedar_operator_input_intervals_total
cedar_operator_output_intervals_total
cedar_operator_batches_total
cedar_operator_cpu_ns
cedar_operator_blocked_ns
cedar_operator_memory_peak_bytes
cedar_operator_selection_rows_total
cedar_path_frontier_rows
cedar_expand_candidates_total
cedar_expand_emitted_total
cedar_temporal_intersection_empty_total
```

The `EXPLAIN ANALYZE` profile associates these counters with a query-local operator ID. Global metric labels remain bounded.

### 7.4 Storage, Page, and Blob

```text
cedar_sst_files_considered_total
cedar_sst_files_pruned_total
cedar_blocks_considered_total
cedar_blocks_pruned_total
cedar_pages_read_total
cedar_pages_decoded_total
cedar_page_read_bytes_total
cedar_page_decode_bytes_total
cedar_page_corruption_total
cedar_property_gather_requests_total
cedar_blob_refs_seen_total
cedar_blob_payload_reads_total
cedar_blob_payload_bytes_total
cedar_blob_hash_lookup_total
cedar_blob_cache_hit_total
```

These metrics make the columnar claims testable: a filtered projection should reduce projected page and Blob payload reads, not only elapsed time.

### 7.5 Index and Optimizer

```text
cedar_index_probe_total
cedar_index_candidate_rows_total
cedar_index_validated_rows_total
cedar_index_false_candidate_rows_total
cedar_index_fallback_scan_total
cedar_index_coverage_ratio
cedar_index_sidecar_build_bytes_total
cedar_index_sidecar_repair_total
cedar_optimizer_plan_time_ns
cedar_optimizer_memo_groups
cedar_optimizer_alternatives
cedar_optimizer_budget_exhausted_total
cedar_optimizer_estimate_error_ratio
cedar_optimizer_runtime_switch_total
```

The final result always comes from the base temporal validator; an index metric cannot be used as proof of correctness.

### 7.6 Scheduler, Cache, and Maintenance

The resource and maintenance metrics from the fifth design are part of the unified schema:

```text
cedar_scheduler_queue_delay_ns
cedar_scheduler_service_ns
cedar_scheduler_admission_reject_total
cedar_scheduler_pressure_transition_total
cedar_scheduler_write_stall_ns
cedar_memory_pool_bytes
cedar_memory_reservation_fail_total
cedar_io_tokens_consumed_total
cedar_cache_hit_total
cedar_cache_bypass_total
cedar_cache_eviction_total
cedar_cache_pinned_bytes
cedar_flush_bytes_total
cedar_compaction_input_bytes_total
cedar_compaction_output_bytes_total
cedar_compaction_write_amplification
cedar_blob_gc_relocated_bytes_total
```

## 8. Query Profiles and Tracing

### 8.1 EXPLAIN ANALYZE Profile

Every completed diagnostic query can produce a profile containing:

```text
query_id
normalized_query_hash
temporal_scopes
snapshot_seq and version_set_id
catalog/statistics generations
chosen plan and rejected alternatives
operator tree and pipeline graph
per-operator rows/intervals/batches
page/index/Blob counters
memory/spill/scheduler counters
terminal status
```

The profile is self-contained and does not include raw property values by default.

### 8.2 Sampled Spans

Spans cover:

- query admission and planning;
- SST/Block/page reads;
- index probes and base validation;
- vector pipeline execution;
- WAL/DecisionLog fsync;
- flush/compaction/index/Blob GC phases.

Tail queries, errors, pressure transitions, and write stalls receive a higher sampling probability. Sampling decisions are recorded in the artifact so a missing span is not mistaken for zero work.

### 8.3 Trace Backpressure

Tracing is asynchronous and bounded. On pressure:

1. ordinary analytical spans are sampled down;
2. verbose per-page spans are dropped;
3. error, pressure, commit-failure, and corruption events remain;
4. the drop count and current sampling rate are exported.

Tracing cannot block a WAL append or hold a PageCache pin while waiting for an exporter.

## 9. Benchmark Harness

### 9.1 Separate Binary and Protocol

Benchmarks run through a separate `cedar_bench` binary and a versioned runner protocol. The runner starts a fresh Cedar process or opens a fresh database directory, applies a complete configuration manifest, executes phases, verifies results, and writes artifacts.

The harness does not call internal test-only methods that bypass WAL, Manifest, Blob durability, or QuerySnapshot. A benchmark may select a plan or codec for an ablation only through a documented benchmark profile; it cannot silently change production semantics.

### 9.2 Run Manifest

Every run writes a machine-readable manifest before measurement:

```text
run_id
source_commit
source_dirty_state
binary_hash
compiler_and_flags
os_kernel
cpu_model_and_count
memory_limit
storage_device_and_filesystem
resource_profile_id
database_format_version
language_version
schema_hash
dataset_id_and_hash
generator_seed
workload_id_and_hash
durability_mode
cache_mode
thread_and_worker_limits
measurement_protocol_version
```

The `run_id` is derived from the manifest plus a unique execution nonce. The manifest is copied into every summary and report.

### 9.3 Phases

Each benchmark has explicit phases:

```text
environment_check
database_create_or_open
dataset_load
load_verification
cache_prepare
warmup
measurement
drain_and_maintenance
result_verification
reopen_verification
artifact_finalize
```

Warmup samples are not included in measurement. A run that fails load, verification, durability, or reopen is invalid even if its measurement throughput is high.

### 9.4 Open-Loop and Closed-Loop Modes

Latency-sensitive writes and point queries use an open-loop arrival schedule so queueing delay and coordinated omission are visible. The harness records requested arrival time, admission time, start time, completion time, and terminal status.

Throughput saturation and analytical scans use closed-loop workers with explicit concurrency. The report names the mode; results from the two modes are not combined.

### 9.5 Cache Modes

Every applicable workload runs in explicitly named modes:

```text
cold_process_and_database
cold_database_warm_process
warm_metadata_only
warm_full_working_set
steady_state_with_background_maintenance
```

The harness never claims a cold-cache result without recording how the cache was prepared. Platform-specific cache dropping is optional; a fresh database/device directory is the portable baseline.

### 9.6 Durability Modes

The primary Cedar result uses:

```text
durable = WAL/DecisionLog/Manifest fsync enabled
```

Unsafe or relaxed modes may be reported as engineering ablations, but they must be labeled `NON_DURABLE` and cannot be compared with durable results or used in README/paper claims about the engine's correctness or write latency.

## 10. Dataset Suite

### 10.1 Cedar-TG Synthetic Dataset

The canonical generator creates vertices, edges, typed properties, valid-time events, transaction commits, and Blob payloads from a fixed seed. Parameters include:

```text
vertex_count
edge_count
label/type cardinality
degree distribution: uniform | Zipf | power-law
supernode fraction and maximum degree
property type and null/absence rates
property cardinality and temporal churn
out-of-order event fraction
PUT/DELETE/restore mix
commit burstiness and shard skew
Blob size distribution and duplicate ratio
valid-time span and event density
```

The generator emits a canonical event file, schema manifest, expected counts, and a hash. It can create CI, workstation, and paper-scale profiles without changing event semantics.

### 10.2 LDBC-Derived Temporal Adapter

An adapter may derive valid-time and commit-time events from LDBC SNB graph/update data. The adapter records the source version, transformation policy, license metadata, and generated hash. The derived temporal semantics are not claimed to be an official LDBC temporal benchmark.

Results are reported separately from Cedar-TG synthetic results because the distributions and transformation rules differ.

### 10.3 Scale Profiles

The standard profiles are:

```text
CI:          small graph, short histories, deterministic fast checks
WORKSTATION: million-scale entities/events, realistic Blob and skew mix
PAPER:       largest reproducible local profile supported by the device
STRESS:      targeted extreme versions, supernodes, skew, or long snapshots
```

The profile records actual generated counts. A failed allocation or truncated dataset invalidates the run rather than silently shrinking it.

## 11. Workload Families

### 11.1 Durable Ingestion

Measure:

- single-key autocommit;
- multi-property same-shard transactions;
- multi-shard transactions and DecisionLog;
- concurrent writers with group commit;
- out-of-order valid-time writes;
- PUT/DELETE/restore histories;
- medium and large Blob payloads;
- conflict-heavy strict transactions.

Report throughput, open-loop latency, commit queue delay, fsync latency, conflict rate, visible-prefix lag, WAL/SST/Blob bytes, and write amplification.

### 11.2 Bitemporal Point Reads

Measure `AS OF valid_time` under current and historical `SYSTEM_TIME AS OF`, equal-valid-time corrections, deleted/restored entities, property absence, and Blob references. Run MemTable-only, mixed MemTable/SST, many-SST, and reopened states.

### 11.3 Analytical Columnar Reads

Measure:

- narrow projection versus complete entity projection;
- selective typed predicates;
- low and high cardinality predicates;
- `BETWEEN` state intervals;
- `CHANGES FOR VALID_TIME`;
- `SYSTEM_TIME` audit changes;
- grouped temporal aggregates;
- BlobRef predicates with and without payload projection.

Report rows, intervals, Blocks/pages considered and read, decoded bytes, Blob reads, spill, and result verification checksum.

### 11.4 Graph Traversal

Measure:

- one-hop outgoing and incoming Expand;
- bound-endpoint `ExpandInto`;
- two- to four-hop fixed paths;
- bounded variable-length `TRAIL` paths;
- continuous interval path queries;
- normal vertices and supernodes;
- property-index-first versus adjacency-first plans.

Report frontier sizes, candidates/emitted edges, interval rejection, visited-edge state, memory, spill, and tail latency.

### 11.5 Index and Optimizer

For each indexed property, vary selectivity and temporal churn:

- equality, `IN`, ordered range, and binary prefix;
- index-only covered fragments;
- partial coverage and repair;
- many overlapping SST fragments;
- index intersection versus most-selective-index plans;
- base scan versus index plan crossover;
- parameter variants and runtime reoptimization.

Every indexed result is compared with a full base scan on the same snapshot.

### 11.6 Maintenance and HTAP Mix

Run writes, point reads, analytical queries, flush, compaction, sidecar build, statistics merge, Blob GC, and long snapshots concurrently. Vary workload profiles:

```text
balanced
ingestion_priority
analytics_priority
maintenance_debt
snapshot_pressure
disk_pressure
```

Report per-class queue delay, service share, write stalls, pressure transitions, cache behavior, and result correctness.

### 11.7 Recovery and Faults

Inject crashes at prepare, DecisionLog, SST fsync/rename, Manifest, sidecar, Blob index, GC CAS, and shutdown phases. Measure reopen time, replay bytes, visible-prefix recovery, orphan cleanup, and result equivalence.

## 12. Correctness Oracles

### 12.1 Independent Small-Scale Oracle

The CI and stress generator includes a test-only scalar bitemporal reference model. It is independent of production storage, planner, and index code. It computes:

- valid-time successor intervals;
- system-time visibility by commit order;
- property absence and deletes;
- endpoint-intersection edge visibility;
- path interval propagation;
- aggregate results.

Production results are compared by canonical typed serialization and sorted logical identity where query order is unspecified.

### 12.2 Large-Scale Checksums

Large benchmarks use deterministic checksums over:

```text
logical identity
valid interval
selected value hash
commit provenance
operation
```

Checksums are computed at fixed sampling points and at final verification. A sample mismatch invalidates the run and includes a reproducible seed/range for diagnosis.

### 12.3 Cross-State Equivalence

For the same database and snapshot, compare results before and after:

- flush;
- overlapping compaction;
- sidecar build or drop;
- Blob relocation;
- checkpoint and reopen;
- scheduler profile changes;
- cache transitions.

The state transition may change performance metrics, never result checksums.

## 13. Metrics and Derived Ratios

### 13.1 Latency and Throughput

Every measured operation reports count, throughput, p50, p95, p99, p99.9, max, and error/cancel rate. Latency includes queue delay and is also decomposed into queue, CPU, blocked I/O, and client-visible completion portions.

Throughput is defined in logical committed events, durable transactions, returned rows, returned intervals, or traversed edges as appropriate. Physical bytes/sec is reported separately and never substituted for logical throughput.

### 13.2 Amplification

```text
write_amplification = physical durable bytes / logical committed bytes
read_amplification  = physical bytes read / logical result bytes
space_amplification = live physical bytes / logical live bytes
```

Components are reported separately:

```text
WAL, DecisionLog, SST flush, compaction, index sidecar, Blob, Manifest
```

### 13.3 Temporal and HTAP Ratios

```text
visible_prefix_lag = committed_seq - visible_seq
index_survival_ratio = validated_rows / candidate_rows
interval_survival_ratio = output_intervals / candidate_intervals
blob_materialization_ratio = Blob payload reads / BlobRefs seen
cache_admission_ratio = admitted pages / requested pages
maintenance_share = maintenance service time / total service time
```

Ratios include denominators and are undefined when no samples exist; zero is not used to hide missing work.

## 14. Artifact Format and Reports

### 14.1 Run Directory

Each run produces:

```text
results/<run_id>/
  manifest.json
  summary.json
  histograms/
  metrics.jsonl.zst
  traces.jsonl.zst
  explain/
  verification.json
  environment.txt
  report.md
```

Raw traces and metrics are optional by run policy but their absence is recorded. `summary.json` references schema versions and all source hashes.

### 14.2 Report Requirements

Every report includes:

- exact workload and dataset parameters;
- durability and cache mode;
- resource profile and background maintenance state;
- repetitions, warmup policy, and arrival model;
- latency/throughput distributions and confidence intervals;
- correctness and reopen status;
- amplification and resource breakdown;
- known exclusions, failed samples, and artifact paths.

A report cannot display a headline result whose verification status is not `PASS`.

## 15. Statistical and Regression Methodology

### 15.1 Repetition and Pairing

Performance comparisons use alternating paired runs on the same hardware and dataset image:

```text
A1, B1, B2, A2, A3, B3 ...
```

The ordering is seeded and recorded to reduce drift bias. CI smoke benchmarks may use fewer repetitions, but release and paper profiles require at least five valid measured repetitions per configuration.

### 15.2 Confidence

Reports use the median across repetitions and a bootstrap 95% confidence interval for relative differences. Raw run distributions remain available. A coefficient of variation or environmental drift above the profile threshold marks the comparison `NOISY` and prevents an automatic performance claim.

### 15.3 Regression Gates

Correctness, corruption, and durability regressions have zero tolerance.

The default performance gates are:

- throughput regression greater than 5% with the paired 95% interval excluding zero;
- p99 latency regression greater than 10% with the paired 95% interval excluding zero;
- memory, disk, or amplification increase greater than 10% when the corresponding metric is a declared objective;
- instrumentation overhead greater than 2% median throughput or 5% p99 latency in Tier 1 mode.

A gate may be overridden only by a checked-in benchmark policy change that states the reason and updates the expected baseline artifact. A single noisy sample cannot update the baseline.

### 15.4 Baseline Ownership

Baselines are keyed by:

```text
benchmark_protocol_version
hardware_profile
dataset_hash
workload_hash
durability_mode
resource_profile
format_version
```

Changing a key creates a new baseline series. It does not overwrite or splice an incompatible history.

## 16. Ablations and External Comparisons

### 16.1 Cedar Ablations

Supported ablations include:

- base scan versus healthy index versus partial hybrid index;
- predicate-first versus complete projection;
- Blob reference-only versus payload projection;
- cold versus warm caches;
- compression/encoding choices allowed by one format version;
- single-worker versus bounded multi-worker execution;
- balanced versus ingestion/analytics resource profiles;
- maintenance idle versus flush/compaction/index/GC pressure.

Ablation controls live in the benchmark harness or explicit diagnostic plan policy. They are not old production fallback engines or dual disk formats.

### 16.2 External Systems

An external comparison is valid only when the adapter documents:

- data and query semantic mapping;
- valid-time and system-time behavior;
- durability and fsync policy;
- transaction isolation;
- graph path semantics;
- Blob/large-value representation;
- cache preparation and resource limits;
- unsupported operations and deviations.

If parity cannot be established, the result is labeled a workload comparison rather than an engine superiority claim.

## 17. Performance Claim Policy

Every number published in README, documentation, a paper, or a demo must include or link:

```text
artifact run_id
source commit
dataset/workload hash
hardware profile
durability mode
cache mode
repetition and confidence method
verification status
```

Existing untraceable values, including the current README write-latency table, must be removed, clearly marked historical/unverified, or regenerated under this protocol before the new engine claims them.

Performance summaries distinguish:

- end-to-end durable latency;
- engine service time without queueing;
- unsafe/non-durable engineering ablations;
- cold and warm reads;
- foreground-only and HTAP mixed load.

No chart may mix these categories in one unlabeled series.

## 18. Failure and Telemetry Semantics

Telemetry failure cannot change database correctness. Export failure increments a counter and may disable that exporter. A benchmark artifact write failure invalidates the benchmark run but does not corrupt the database.

Structured statuses include:

```text
TelemetryDropped
TelemetryExporterFailed
BenchmarkEnvironmentInvalid
BenchmarkVerificationFailed
BenchmarkNoisy
BenchmarkArtifactFailed
BenchmarkProtocolMismatch
```

Fatal storage or transaction errors retain their original status and appear in telemetry; they are never replaced by an observability error.

## 19. Module Boundaries

The new ownership layout is:

```text
observability/
  metric_schema
  metric_registry
  thread_local_metrics
  histogram
  event_ring
  trace_context
  trace_sampler
  telemetry_aggregator
  explain_analyze_profile
  exporters/

benchmarks/
  runner
  run_manifest
  environment_probe
  dataset_generator
  ldbc_adapter
  workload_driver
  arrival_scheduler
  correctness_oracle
  artifact_writer
  report_builder
  regression_compare

tests/model/
  bitemporal_oracle
  scheduler_model
  crash_oracle
```

Dependency rules:

- runtime components emit typed telemetry through bounded APIs and do not depend on exporters;
- exporters consume snapshots and spans without calling storage internals;
- benchmark drivers use public Cedar APIs and production durability paths;
- correctness oracles are test-only and never link into production query execution;
- artifact and report code cannot mutate a database under measurement;
- metric schemas have stable versions and bounded labels;
- telemetry memory and worker use is governed by the fifth-stage ResourceGovernor.

## 20. Complete Legacy Metrics and Claim Removal

The clean-break implementation must remove or replace:

- component-specific `Stats` structs that duplicate unified metric names or use incompatible units;
- ad hoc average latency fields without distribution/count metadata;
- old `Explain()` output that cannot represent vector pipelines or runtime profiles;
- cache hit-rate counters not associated with the unified CacheManager;
- console-only compaction and configuration reports;
- unbounded trace flags and direct `fprintf` diagnostics in production paths;
- README/paper/demo performance numbers without a valid artifact reference;
- benchmark helpers that bypass WAL, fsync, Manifest, snapshots, or base temporal validation.

Component-local structs may remain as typed internal snapshots only when generated from the unified registry and documented as views, not independent counters.

## 21. Verification Strategy

### 21.1 Metric Schema Tests

- every metric has a stable name, unit, type, description, and bounded label schema;
- counters never decrease within one process start;
- histogram merge produces the same distribution independent of merge order within declared precision;
- unknown labels and unit mismatches fail registration;
- high-cardinality values are rejected or normalized;
- exporter failure does not block runtime tasks.

### 21.2 Instrumentation Overhead

Run Tier 0/1 enabled versus a test-only minimal instrumentation build under point, scan, path, write, and HTAP workloads. The default gate is at most 2% median throughput and 5% p99 latency overhead. Detailed Tier 3 overhead is reported but is not held to the always-on gate.

### 21.3 Benchmark Reproducibility

- rerunning one manifest and seed produces the same event file and expected checksum;
- manifest hashes detect binary, configuration, dataset, or workload changes;
- warmup samples never appear in measured histograms;
- open-loop schedules record intended and actual arrival time;
- interrupted runs are marked incomplete and cannot update baselines;
- artifacts validate against a versioned JSON schema;
- reports can be regenerated from artifacts without rerunning Cedar.

### 21.4 Correctness and Fault Gates

- CI oracle results match production before and after flush, compaction, sidecar changes, Blob relocation, and reopen;
- crash matrices cover every durability boundary defined in earlier specifications;
- HTAP mixes preserve snapshots and visible-prefix semantics;
- sanitizer, thread-sanitizer, and deterministic scheduler runs are separate required gates;
- any mismatch stops performance aggregation.

### 21.5 Regression Harness

- paired-run ordering and bootstrap calculations are deterministic from the comparison seed;
- synthetic regressions trigger the 5% throughput and 10% p99 gates;
- noisy environments become `NOISY`, not pass or fail by chance;
- incompatible baseline keys are rejected;
- baseline updates require a verified artifact and explicit review.

## 22. Release and Paper Gates

### 22.1 Pull Request Gate

- unit, format, model, and deterministic scheduler tests;
- small crash/reopen subset;
- CI Cedar-TG correctness workloads;
- short instrumentation-overhead smoke test;
- no benchmark regression claim from one sample.

### 22.2 Nightly Gate

- full randomized temporal model suite;
- sanitizer and thread-sanitizer variants;
- medium crash matrix;
- workstation-scale ingestion, query, index, graph, and HTAP workloads;
- paired baseline regression comparison;
- artifact schema and report regeneration.

### 22.3 Release Gate

- complete crash and recovery matrix;
- all six design completion definitions audited;
- cold/warm, durable, and HTAP mixed profiles;
- at least five valid repetitions per claimed configuration;
- verified dataset and result checksums;
- no unexplained write stalls, corruption, or telemetry drops in headline runs;
- release artifacts archived and linked from claims.

### 22.4 Paper/Demo Gate

- Cedar-TG plus at least one externally derived graph workload;
- semantic and durability notes for every baseline;
- raw artifacts, manifests, plotting data, and scripts retained;
- confidence intervals and failure counts shown;
- all limitations and known non-goals stated alongside results.

## 23. Implementation Dependency Order

The future implementation plan must follow this order:

1. Define metric schema, bounded labels, thread-local counters, and histogram format.
2. Implement TelemetryAggregator, snapshots, event ring, trace context, and sampling.
3. Instrument transactions, WAL/DecisionLog, storage pages, Blob, vector operators, indexes, and scheduler.
4. Implement new `EXPLAIN ANALYZE` profile and local metric/trace exporters.
5. Implement deterministic Cedar-TG generator, manifests, and correctness oracle integration.
6. Implement benchmark phase runner, open/closed-loop drivers, cache/durability modes, and artifacts.
7. Add workload families for ingestion, bitemporal reads, graph paths, indexes, maintenance, HTAP, and recovery.
8. Implement paired regression statistics, baseline keys, reports, and release gates.
9. Add LDBC-derived adapter and external-comparison parity documentation.
10. Remove legacy stats, trace flags, unsupported performance claims, and bypass benchmarks.
11. Run schema, overhead, reproducibility, correctness, crash, regression, and report-generation acceptance.

This is architectural sequencing, not a code implementation plan. Implementation begins only after explicit authorization and uses test-first reviewable tasks.

## 24. Completion Definition

The observability and benchmark stage is complete only when:

1. all production subsystems emit through one versioned, bounded-cardinality metric and trace model;
2. always-on telemetry overhead passes the declared gate;
3. `EXPLAIN ANALYZE` attributes rows, intervals, pages, Blob reads, index candidates, memory, spill, and scheduler time;
4. benchmark manifests fully describe source, binary, environment, data, workload, durability, cache, and resource profile;
5. Cedar-TG generation and small-scale correctness oracles are deterministic;
6. ingestion, bitemporal, analytical, graph, index, maintenance, HTAP, and recovery workloads all produce verified artifacts;
7. open-loop latency tests avoid coordinated omission and report queue delay;
8. amplification and temporal survival ratios use explicit numerators and denominators;
9. paired regression gates and baseline keys reject noisy or incompatible comparisons;
10. crash, reopen, sanitizer, scheduler, and result-equivalence gates run before performance claims;
11. every published number has a verified artifact ID and provenance;
12. untraceable old performance claims and duplicate Stats/trace paths are removed;
13. reports can be regenerated from archived artifacts without rerunning the engine;
14. the completion definitions and invariants of all preceding Cedar designs remain intact.
