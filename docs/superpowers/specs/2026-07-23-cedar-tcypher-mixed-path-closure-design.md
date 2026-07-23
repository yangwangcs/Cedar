# Cedar T-Cypher Mixed-Path Closure Design

Date: 2026-07-23

Status: Approved implementation addendum to the authoritative T-Cypher
vectorized-execution design. This addendum does not change the language's
clean-break or single-entry-point contracts.

## 1. Goal

Close the remaining production support gap for a single `MATCH` pattern that
contains a finite mixture of fixed and bounded variable-length relationship
segments, while preserving typed physical planning, vectorized execution,
spill, cancellation, temporal interval semantics, `TRAIL`, and
`EXPLAIN ANALYZE` attribution.

The existing root point/range/change, one-segment variable path, all-fixed
multi-hop path, property projection, and relationship projection paths remain
authoritative. No scalar or legacy executor fallback may be introduced.

## 2. Current Evidence Boundary

The current runtime already passes focused coverage for:

- root valid-time range and valid/system-time change physical execution;
- `EXPLAIN ANALYZE` for root, range, change, fixed Expand, multi-hop fixed
  Expand, variable Expand, joins, aggregate, distinct, sort, and spill;
- all-fixed multi-hop node, relationship, provenance, and property projection;
- one bounded variable relationship with point/range interval intersection,
  `TRAIL`, frontier partitioning, spill, cancellation, and path projection.

The remaining functional rejection is a multi-segment pattern where at least
one segment is variable length and at least one other segment is fixed length.
The old generic `EXPLAIN ANALYZE for range/change physical runtime is not
available yet` branch is wider than the actual unsupported set and must not be
used as a support boundary.

## 3. Language and Result Semantics

Example supported shapes:

```cypher
MATCH (a)-[p:KNOWS*1..3]->(b)-[r:WORKS_WITH]->(c)
RETURN a, p, b, r, c;
```

```cypher
FOR VALID_TIME BETWEEN $from AND $to
MATCH (a)-[r:KNOWS]->(b)-[p:WORKS_WITH*2..4]->(c)
RETURN a.name, valid_from(p), valid_to(p), r, c.name;
```

Every relationship segment has a finite `[min_hops, max_hops]`. A fixed
segment is represented as `[1, 1]`. A variable relationship binding denotes
the ordered list of relationships traversed by that segment; a fixed
relationship binding remains a relationship struct.

Direct property access on a variable relationship binding, such as `p.weight`,
is not well-typed because `p` is a relationship list. It remains a deterministic
bind-time `NotSupported` result. Endpoint-node properties and fixed-segment
relationship properties are supported. `valid_from(p)` and `valid_to(p)` refer
to the demand-aligned interval of the complete matched result, consistent with
existing fixed multi-hop range projection semantics.

Mixed variable paths are supported for valid-time point and valid-time range
state queries. Mixed-path `CHANGES` remains explicitly unsupported because a
single immutable change event does not define which segment owns a multi-event
path change. The rejection must be specific, deterministic, documented in the
support matrix, and covered by a negative test.

## 4. Physical Plan

`PhysicalExpandSpec` remains the per-segment contract and carries:

- source, relationship, and target bindings and slots;
- direction and optional relationship type;
- `min_hops`, `max_hops`, and an optional path slot.

The multi-segment planner emits one ordered `PhysicalExpandSpec` per syntax
segment. It accepts any mixture of fixed and bounded variable segments. Each
variable segment receives a path slot; each fixed segment continues to expose
the edge identity/provenance slots used for relationship structs and property
gather.

The physical-plan fingerprint and validator include every segment's bounds,
path slot, bindings, direction, and edge type. A plan is invalid if bounds are
zero, reversed, unbounded, or if a variable segment lacks a path slot.

## 5. Runtime Data Flow

The runtime uses a segmented frontier rather than treating the entire pattern
as globally fixed or globally variable.

For each input root row:

1. Start one chain state containing the root node, its temporal domain, and an
   empty global visited-edge set.
2. For a fixed segment, run one vectorized expansion and advance directly to
   the next segment.
3. For a variable segment, iterate its frontier from hop 1 through
   `max_hops`; emit completed subpaths beginning at `min_hops`, and feed each
   completed terminal state into the next segment.
4. Carry the visited-edge set across segment boundaries. Therefore `TRAIL`
   applies to the complete mixed pattern, not independently inside each
   segment.
5. Intersect the input domain with every traversed edge and endpoint existence
   interval. Empty intersections are removed before property gather.
6. After the final segment, gather demanded endpoint and fixed-relationship
   properties, align range boundaries, apply predicates, project typed results,
   and stream bounded result batches.

Frontier states use the existing query-private checksummed spill mechanism.
Spill records add segment index, hop count within the segment, per-segment edge
lists, the current result interval, and the global visited-edge set. Spill is
never durable database state.

## 6. Cancellation, Memory, and Metrics

Cancellation is checked before each source morsel, segment transition,
frontier partition, spill append/read, property gather, and output batch.

All frontier rows, visited-edge sets, path references, spill buffers, and
result buffers remain charged to the query memory account. A failed reserve
either starts bounded spill where supported or returns `QueryMemoryLimit`.

`EXPLAIN ANALYZE` reports one Expand operator occurrence per syntax segment.
Variable segments additionally report frontier hops, input/output states,
completed subpaths, partitions, maximum partition size, spill starts/bytes,
and cancellation terminal status. Fixed segments retain their existing
input/output row counters.

The executor removes the generic range/change `NotSupported` dispatch. If the
planner produces a physical plan that has no physical runtime dispatcher, the
result is an internal `Corruption` invariant failure, not a user-facing support
decision.

## 7. Verification

TDD coverage must include:

- variable-then-fixed and fixed-then-variable point queries;
- range queries with exact path interval intersection;
- global `TRAIL` across a variable/fixed segment boundary;
- variable path binding plus fixed relationship struct projection;
- endpoint and fixed relationship property projection and predicate gather;
- morsel-boundary determinism and cancellation;
- forced frontier spill with bounded descriptors and cleanup;
- `EXPLAIN`/`EXPLAIN ANALYZE` operator occurrence and frontier counters;
- physical-plan fingerprint/validation changes for bounds and path slots;
- a deterministic negative test for mixed-path `CHANGES`;
- an independent scalar oracle comparison for point and range results.

The focused normal and ASAN/UBSAN/TSAN matrices must pass before this slice is
recorded as implemented. These focused matrices do not replace the final
cross-design sanitizer and release gates.

## 8. Non-Goals

- unbounded variable-length paths;
- pairwise, sequential, or waiting-time path semantics;
- direct property dereference on a variable relationship list;
- mixed-path `CHANGES` semantics;
- a scalar, materializing, or legacy production fallback;
- changes to database format number 1 or any on-disk format.
