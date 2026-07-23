# Cedar HTAP Transaction Measurement Design

**Status:** approved approach A; implementation contract pending document review

## 1. Purpose

The HTAP and observability designs require release evidence for transaction
conflicts, durable decision latency, and visibility stalls.  The current
benchmark artifacts can report storage deltas, but transaction measurements
are not produced at their authoritative boundaries.  Campaign sidecars or
wall-clock inference cannot prove these properties.

This design adds first-class structured transaction measurements.  A
measurement originates in the component that owns the relevant boundary:

- `TransactionCoordinator` owns transaction result, prepare-phase, decision
  phase, and visible-prefix-wait measurements.
- `DecisionLog` owns the precise duration of the durable record fsync.
- `CedarDatabase` maps bounded measurement events to the existing
  `MetricRegistry` and exposes cumulative snapshots to benchmark callers.
- `RunBenchmarkWorkload` takes before/after snapshots and places the delta in
  the structured artifact summary.

The database format stays `1`.  This is an in-memory observability and
artifact-schema change only; it has no legacy reader, migration, external
`V2`/`Vn` name, or layout-compatibility path.

## 2. Alternatives Considered

### A. Boundary-owned measurements with benchmark window deltas (chosen)

The coordinator and DecisionLog measure their own phases.  The benchmark
driver records a before/after snapshot for its measurement window.  This
preserves a single authoritative source, works for every public transaction
entry point, and makes missing samples explicit.

### B. Measure only around benchmark operations

This could produce campaign numbers quickly, but would include queueing and
application work, omit non-benchmark transactions, and could not isolate the
DecisionLog fsync interval.  It is rejected.

### C. Add `MetricRegistry` directly to storage and DecisionLog ownership

This couples durability code to the database telemetry lifetime and makes
standalone coordinator tests depend on registry setup.  It is rejected.

## 3. Measurement Contract

All durations use `steady_clock` and are stored in nanoseconds.  Counters and
histograms saturate at `UINT64_MAX`.  Measurement failure, allocation failure,
or metric-registry rejection is ignored and must never alter a transaction
result, durability decision, recovery requirement, or visible-prefix state.

Full transaction measurement is enabled only in the `tier0-tier1`
instrumentation profile.  The `tier0-minimal` profile retains the existing
minimal error signal but does not take per-transaction timing samples, acquire
the measurement mutex, or synthesize zero-valued transaction data.  Its
benchmark window is explicitly unavailable with
`availability_reason: "minimal_instrumentation"`.  Release/paper transaction
claims therefore use the `tier0-tier1` provenance profile.

### 3.1 Transaction outcome and labels

Each call to `CommitInternal` records exactly one terminal outcome after it
has entered the coordinator, including validation, admission, prepare,
decision, installation, and visibility failures.  It records:

- `started`: one for every entered call;
- `committed`: one only for `CommitOutcome::kCommitted`;
- `aborted`: one only for `CommitOutcome::kAborted`;
- `indeterminate`: one only for `CommitOutcome::kIndeterminate`.

The mode label is exactly `snapshot` when `strict_reads` is empty and `strict`
otherwise.  The abort-reason label is a closed status-code vocabulary:
`invalid_argument`, `schema_mismatch`, `serialization_conflict`,
`write_stalled`, `resource_exhausted`, `recovery_required`, `io_error`,
`corruption`, `not_supported`, and `other`.  No status message, transaction
ID, key, shard, schema, or query text may become a label.

`conflict` increments only when a terminal aborted result has the typed
`SerializationConflict` code.  It is not inferred from error text and does
not include admission refusal, schema rejection, or indeterminate outcomes.

### 3.2 Phase duration inclusion

`commit_latency_ns` covers the entire entered `CommitInternal` call through
its terminal result.  It is recorded for all terminal outcomes and labelled
by `mode` and `outcome` (`committed`, `aborted`, or `indeterminate`).

`prepare_latency_ns` starts immediately before the prepare work is submitted
to the critical executor and ends after all prepare tasks have completed or a
submission/execution failure makes the phase terminal.  It is wall-clock
phase duration, not the sum of per-shard durations, and is recorded whenever
that phase is entered.  Its labels are `mode` and `outcome` (`succeeded` or
`failed`).

`decision_latency_ns` starts immediately before `AppendCommitWithResult` and
ends immediately after it returns.  It includes decision-record encoding,
append, and fsync.  It is recorded whenever that call is made, using the same
bounded `mode` and `outcome` labels.

`decisionlog_fsync_latency_ns` is the duration of the actual fsync in the
successful or failed DecisionLog append.  `DecisionAppendResult` carries this
duration and an `fsync_attempted` bit.  Early argument/reopen rejection has
`fsync_attempted=false` and contributes no sample.  A failed fsync after the
attempt contributes a failed sample; this makes fault-campaign behavior
observable without claiming durability.

### 3.3 Visible-prefix stall

After a durable decision is installed, the coordinator records a
visible-prefix stall only if the wait predicate is entered.  The duration
starts immediately before waiting on `installation_cv_` and ends after the
predicate becomes true.  A zero-duration successful predicate is still a
sample, because it proves the transaction crossed the boundary without a
stall; `stall_count` increments only when duration is nonzero.

The sample contains the assigned commit sequence and the visible sequence at
wait completion.  `visible_prefix_lag_seq` is a gauge for the latest safe
`max(assigned_commit_seq - visible_seq, 0)` sample.  Its benchmark summary
also provides the maximum observed lag in the measurement window.  A recovery
termination records the duration as failed but does not publish a successful
lag value.

## 4. Interfaces and Ownership

`transaction_measurements.h` defines a small coordinator-owned data model:

- `TransactionMeasurementEvent`: one bounded event emitted at a terminal or
  phase boundary, with mode, bounded outcome/reason, and optional duration or
  sequence values;
- `TransactionMeasurementSnapshot`: monotonic counters plus mergeable
  histogram snapshots for each required distribution;
- `TransactionMeasurementWindow`: subtraction/validation of two snapshots.

`TransactionCoordinator` stores the canonical cumulative snapshot behind its
own measurement mutex.  It accepts an optional event sink set by
`CedarDatabase` at construction/open time.  The sink is observational only;
the coordinator updates its own snapshot first and calls the sink afterward.
Direct coordinator users therefore retain complete measurements without a
database telemetry dependency.

`CedarDatabase` registers and updates these stable families:

```text
cedar_txn_started_total
cedar_txn_committed_total
cedar_txn_aborted_total
cedar_txn_indeterminate_total
cedar_txn_conflict_total
cedar_txn_commit_latency_ns
cedar_txn_prepare_latency_ns
cedar_txn_decision_latency_ns
cedar_decisionlog_fsync_latency_ns
cedar_txn_visible_prefix_stall_ns
cedar_txn_visible_prefix_lag_seq
cedar_commit_completion_stall_total
```

The existing ambiguous `cedar_txn_failed_total` publisher is removed for
transaction paths.  It cannot coexist as an alternative semantic source.

## 5. Benchmark Artifact Contract

Artifact schema advances from `2` to `3`; the strict reader accepts only
schema `3`.  Existing release evidence is intentionally not a compatibility
input under the clean-break policy and must be regenerated.

`BenchmarkArtifactSummary` and `BenchmarkWorkloadResult` gain
`transaction_measurements`, a structured window result.  Each distribution
uses this exact representation:

```json
{
  "defined": true,
  "sample_count": 12,
  "min_ns": 100,
  "p50_ns": 200,
  "p95_ns": 800,
  "p99_ns": 900,
  "p999_ns": 900,
  "max_ns": 1000,
  "sum_ns": 3200
}
```

When `sample_count` is zero, `defined` is `false` and every distribution value
is JSON `null`; zero is never used to mean unavailable.  Counter fields are
always present because a zero counter delta is meaningful.  The window also
contains `conflict_abort_rate`, represented as `{ "defined", "numerator",
"denominator", "value" }`; it is defined only when aborted count is nonzero.
`visible_prefix_stall` contains successful and failed sample distributions,
nonzero-stall count, and `max_lag_seq` with an explicit `defined` flag.

The strict writer and reader must reject contradictory forms, including a
defined distribution with zero samples, an undefined distribution with a
numeric percentile, conflict-rate denominator different from aborted count,
or an undefined lag carrying a numeric value.  Report rendering shows
`unknown` for undefined values.

`RunBenchmarkWorkload` takes the coordinator snapshot after cache/storage
snapshots and again after `RunBenchmarkWorkloadImpl` returns, even when the
workload terminal status is non-OK.  Delta validity requires monotonic
counters and identical histogram bounds.  A reset or incompatible snapshot
makes the transaction window undefined with a diagnostic; it is not silently
treated as zero.  The summary writer carries that diagnostic in a structured
`availability_reason` field.

## 6. Tests and Evidence

Focused normal tests must cover:

1. snapshot success records one start/commit, all successful phase samples,
   and a defined benchmark window;
2. strict serializable conflict increments only conflict and the typed abort
   bucket, without a committed sample;
3. validation/admission failure records an abort but no prepare or decision
   sample;
4. DecisionLog fsync fault records a failed attempted fsync sample and retains
   the existing recovery semantics;
5. delayed installation produces a nonzero visible-prefix stall and sequence
   lag; a resolved immediate wait produces a zero-duration sample;
6. minimal instrumentation leaves the transaction window explicitly
   unavailable and does not change commit outcomes;
7. workload before/after deltas exclude warmup and reject a reset;
8. schema-3 writer/reader round-trip, undefined encodings, and malformed
   combinations are all strictly checked.

After focused tests pass, regenerate the HTAP correctness and observability
release-evidence roots.  Their manifests remain database format `1` and must
include summary, metrics, histograms, environment, provenance, verification,
and `SHA256SUMS`.  The final normal/ASAN/UBSAN/TSAN matrix remains deferred
until all remaining code and evidence changes are frozen.

## 7. Non-Goals

This work does not add a Prometheus server, unbounded per-transaction tracing,
cross-process metric aggregation, a new on-disk telemetry format, automatic
installation of external dependencies, or a compatibility reader for artifact
schema `2`.
