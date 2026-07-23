# Cedar T-Cypher Disconnected Multi-Root Cross Join Design

**Date:** 2026-07-22

**Status:** Approved design, awaiting implementation

## Objective

Eliminate the legacy full-materialization fallback for disconnected multi-root
T-Cypher queries. A multi-root physical plan must be able to combine connected
components with hash joins and disconnected components with an explicit,
bounded, spillable, cancellable cross join.

The implementation must not represent a Cartesian product as a hash join with
empty keys, and it must not reuse the handwritten `CrossJoinRootResultStream`
fallback in `executor.cc`.

## Scope

The design covers point-in-time multi-root MATCH inputs already supported by
`PlanPhysicalMultiHashJoin`, including:

- a connected component plus one or more disconnected roots;
- multiple disconnected connected-components;
- a query with no cross-root equality predicates;
- node and fixed/multi-hop relationship child inputs already supported by the
  physical root runtime;
- final projection, aggregate, distinct, sort, skip, and limit handling through
  the existing physical result pipeline.

Range/change support is not added to multi-root planning by this change. Such
queries remain outside the existing point-scope multi-root candidate and are
tracked by the broader T-Cypher release goal.

## Physical Plan

Add `PhysicalOperatorKind::kCrossJoin` and allow every
`PhysicalMultiHashJoinStep` to describe either:

- `kHashJoin`, with non-empty, type-compatible accumulated/input key columns;
  or
- `kCrossJoin`, with both key-column vectors empty.

The existing multi-root plan remains the composition boundary. It may contain a
mixture of hash-join and cross-join steps. This avoids a second top-level plan
and preserves the existing child-plan, pipeline, result-sink, EXPLAIN, and plan
identity integration.

Although the compatibility type remains named `PhysicalMultiHashJoinPlan`, its
steps carry the authoritative operator kind. No runtime may infer cross-join
semantics merely from an empty hash key.

## Candidate Validation

`CanPlanPhysicalMultiHashJoin` accepts a multi-root statement when every root
input, projection, local predicate, temporal scope, and relationship expansion
is independently supported. Connectivity is no longer a candidate condition.

For two relationship-free roots:

- a connected equality continues to use the existing two-root physical hash
  join candidate;
- a disconnected query uses the multi-root plan because there is no hash-join
  candidate capable of representing it.

Literal predicates remain assigned to exactly one child input and execute below
both hash joins and cross joins.

## Join-Order Planning

The planner builds the same root-input ownership map and logical equality graph
as today. Dynamic programming is used for up to six roots; the deterministic
greedy fallback is used above six roots.

For each candidate attachment:

- if one or more equality edges connect the candidate to the accumulated set,
  the step is a hash join;
- otherwise the step is a cross join that introduces a new disconnected
  component.

Candidate comparison is deterministic and ordered by:

1. saturated cumulative cardinality cost;
2. number of cross-join steps introduced so far;
3. binding-id join order.

The accumulated row estimate uses saturated multiplication for both step kinds.
Confidence is the minimum input confidence, and the conservative flag is the
logical OR of both inputs. The planner never fabricates equality selectivity;
the later CBO stage may replace the current conservative product estimate with
measured selectivity.

## Runtime

`OpenPhysicalMultiHashJoinRuntime` dispatches each step by its explicit operator
kind:

- `kHashJoin` opens the existing `AdaptivePhysicalHashJoinResultStream`;
- `kCrossJoin` opens a new `AdaptivePhysicalCrossJoinResultStream` backed by a
  `PhysicalCrossJoinResultStream`.

The adaptive wrapper samples bounded prefixes using the same two-batch policy as
the hash join. It chooses the smaller sampled/planned side as the replay side and
replays both prefixes through `PrefixReplayResultStream`.

The cross-join stream drains only the replay side before producing output. Rows
are retained in memory while the query memory account remains below its soft
limit. When the next row cannot be retained below the soft limit, all retained
rows and subsequent replay-side rows are written to a one-partition
`PartitionedSpillSet`, after which the in-memory reservations are released.

For each row from the streaming side, the operator emits that row combined with
every replay-side row. Output is produced in bounded `batch_capacity` batches.
When the replay side spilled, its partition is rewound for each streaming row;
only the current spill batch and output batch are resident. The operator never
stores the Cartesian output and never materializes both complete inputs.

An empty input terminates the result without opening unnecessary downstream
work. Output mappings continue to use `PhysicalHashJoinPlan::Output`, so scalar,
list, relationship, provenance, and nullable result cells preserve their current
representation.

## Memory, Spill, and Resource Ownership

- Every retained replay row is charged to `QueryMemoryAccount` including cell
  payloads and row/container overhead.
- Crossing the soft limit starts spill; failure to encode or write spill is a
  terminal query error.
- A spill batch that cannot be decoded within the hard limit returns
  `QueryMemoryLimit` rather than allocating outside the account.
- `PartitionedSpillSet` owns file descriptors, temporary bytes, governor
  extensions, and cleanup through RAII.
- All retained-row reservations are released when spilling begins and on every
  terminal, cancellation, and destruction path.
- Output rows are built directly into the result batch and remain covered by the
  existing result-batch lease semantics.

## Cancellation and Errors

Cancellation is checked while:

- sampling both inputs;
- draining the replay side;
- spilling retained and incoming replay rows;
- obtaining each streaming batch and row;
- rewinding and reading spill batches;
- generating Cartesian output rows.

The first non-OK child terminal status is preserved. Corrupt spill rows, invalid
output mappings, non-nullable nulls, and shape mismatches become terminal
statuses and do not return partial success after the error is observed.

## Validation and EXPLAIN

`ValidatePhysicalMultiHashJoinPlan` requires:

- hash-join steps to have equal, non-empty, valid key vectors;
- cross-join steps to have both key vectors empty;
- only `kHashJoin` and `kCrossJoin` step kinds;
- unchanged ownership, output-layout, estimate propagation, operator-id, and
  pipeline dependency invariants.

`PhysicalOperatorKindName(kCrossJoin)` returns `CrossJoin`. Both EXPLAIN and
EXPLAIN ANALYZE therefore expose each disconnected attachment explicitly instead
of reporting a hash join or legacy executor path.

## Observability

Add production counters for:

- physical cross-join builds;
- replay/build input rows;
- streaming/probe input rows;
- emitted cross-product rows;
- spill starts and spill bytes;
- adaptive side switches;
- cancellation and memory-limit terminal outcomes through existing query status
  reporting.

`legacy_multi_root_materialized_rows` must remain zero for every query accepted
by the physical multi-root candidate. `pipeline_builds` increments once per
physical hash-join or cross-join step.

## Test Strategy

All production work follows RED-GREEN TDD.

1. Convert the existing disconnected three-root fallback test to require one
   physical multi-root plan, non-zero pipelines, and zero legacy materialization.
2. Add a planner test that requires one `kHashJoin` step and one `kCrossJoin`
   step for a partially disconnected graph.
3. Add a fully disconnected query test whose steps are all `kCrossJoin`.
4. Add validator tests rejecting empty hash keys, keyed cross joins, and unknown
   step kinds.
5. Force a small soft limit and verify replay-side spill preserves the exact row
   multiset and releases memory and temporary resources.
6. Verify a hard memory limit returns `QueryMemoryLimit` without unaccounted
   allocation.
7. Cancel during replay drain and spill replay; verify terminal cancellation and
   cleanup.
8. Verify EXPLAIN and EXPLAIN ANALYZE contain `CrossJoin`, execute the same plan
   identity, and never increment legacy materialization.
9. Add a fixed `std::mt19937_64` oracle test for small connected/disconnected
   graphs. On failure it reports seed and case number.
10. Re-run multi-root relationship, spill, temporal override, aggregate, sort,
    and sanitizer regressions.

## Compatibility

No CedarKey, TemporalEvent, version-chain MemTable, SST v2, schema, or on-disk
format changes are introduced. Existing connected physical multi-root plans keep
their current hash-join behavior and plan validation. The legacy multi-root
executor remains temporarily present only for query shapes not yet admitted by
the physical candidate; it is not reachable for the disconnected point-query
shapes covered by this design.

## Completion Criteria

This feature is complete only when disconnected point multi-root queries execute
through explicit physical cross-join steps with bounded memory, spill,
cancellation, EXPLAIN ANALYZE, deterministic planning, production statistics,
fixed-seed oracle coverage, and zero legacy full materialization.
