# Cedar T-Cypher and Vectorized Temporal Graph Execution Design

Date: 2026-07-17

Status: Approved authoritative design; functional implementation substantially complete; release/paper closure remains incomplete and is tracked in `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

Depends on:

- `2026-07-17-cedar-htap-design.md`
- `2026-07-17-cedar-columnar-design.md`

## 1. Purpose

This document defines Cedar's third design stage: a clean-break T-Cypher language and a morsel-driven, vectorized temporal graph execution engine. It connects the correctness kernel and Zone-Columnar SST to one end-to-end query and mutation path.

The design replaces the current row-at-a-time Cypher implementation. It does not add a vectorized adapter under the old parser or preserve the old execution API. The new path begins with a new T-Cypher tokenizer and ends at the typed, snapshot-aware storage and transaction contracts defined by the first two designs.

The stage adds:

- a Cedar-owned bitemporal T-Cypher dialect;
- temporal reads, temporal mutations, and explicit transaction statements;
- a typed AST, binder, logical planner, and bounded cost optimizer;
- vectorized graph and temporal operators using `ColumnBatch`;
- morsel-driven parallel pipelines with bounded memory and spill;
- batch storage scans, property gathers, and adjacency expansion over SST;
- stable `SYSTEM_TIME` queries backed by a durable HLC commit timeline;
- end-to-end correctness, recovery, cancellation, and observability tests.

## 2. Relationship to the Earlier Designs

The correctness-kernel design remains authoritative for:

- `valid_time`, `commit_seq`, `snapshot_seq`, and `visible_seq` visibility;
- snapshot isolation and the exact-key strict-serializable mode;
- sharded OCC, shard WALs, DecisionLog, and global visible-prefix publication;
- Manifest, VersionSet, recovery, tombstones, flush, and compaction correctness;
- edge visibility as the intersection of the edge and both endpoint existence facts.

The columnar design remains authoritative for:

- SST partitioning, Block and Page format, schema epochs, and typed values;
- Entity, Target, ValidFrom, CommitSeq, Operation, and value pages;
- page pruning, projection reads, PageCache ownership, and BlobRef behavior;
- entity-major ordering, one physical projection, and size-tiered compaction;
- content-addressed Blob identity, lazy Blob reads, relocation, and GC.

This design makes two normative refinements:

1. A committed DecisionLog record also persists a monotonic `system_time_hlc`. This does not replace `commit_seq`; it supplies the user-facing `SYSTEM_TIME` index.
2. The scalar storage methods sketched in columnar are refined into batch query contracts. Scalar convenience methods may exist inside tests, but no production query path may use them to recreate row-at-a-time execution.

If a term in this document appears to grant visibility beyond the correctness kernel, the correctness kernel wins. In particular, a system-time lookup is always capped by the query's captured `visible_seq`.

## 3. Current Query-Layer Problems Being Replaced

The repository's current query code is a prototype rather than a usable temporal execution layer:

1. Operators exchange one heap-allocated `Record` through `Next()` calls.
2. `ExecutionContext` stores mutable string-keyed variables rather than typed vector bindings.
3. Node scans can enumerate a hard-coded entity range instead of scanning storage metadata.
4. Expand loops over one source row and one adjacency list at a time.
5. Several temporal scan operators initialize successfully and then return no rows.
6. The parser, AST, and temporal dialect duplicate or disagree on temporal clause representation.
7. FIRST, PREV, and NEXT syntax maps to unfinished placeholders rather than defined semantics.
8. Query entry points are duplicated across graph facades, and some return "not implemented".
9. There is no stable query snapshot spanning all operators.
10. Projection and predicate requirements do not drive page reads or Blob materialization.
11. Blocking operators buffer unbounded vectors of row objects.
12. There is no query memory account, spill contract, cooperative cancellation, or backpressure.
13. Planning is structural and cannot use Block statistics or graph degree estimates.
14. Tests do not establish bitemporal, path-interval, storage-reopen, or concurrent-snapshot behavior.

The new design removes these paths. It does not attempt to repair their public contracts.

## 4. Goals and Non-Goals

### 4.1 Goals

1. Make Cedar T-Cypher the only graph query and temporal mutation language path.
2. Preserve identical query results across MemTable, flush, compaction, restart, and Blob relocation.
3. Treat valid-time domains as first-class values throughout binding, planning, and execution.
4. Pin one stable storage view while allowing multiple valid-time and system-time contexts in a query.
5. Decode only the pages and Blob values demanded by surviving rows.
6. Execute scans, filters, projections, joins, aggregates, and graph expansion in typed batches.
7. Parallelize independent SST Blocks, shards, and path frontiers without losing snapshot stability.
8. Bound memory for every query and spill supported blocking state.
9. Make temporal DML atomic through the correctness-kernel transaction protocol.
10. Provide deterministic, explainable error and cancellation behavior.
11. Expose enough execution metrics to verify the columnar and HTAP design claims.

### 4.2 Non-Goals

The first T-Cypher version does not include:

- compatibility with the old C++ Cypher API or old execution plans;
- backward-compatible disk reads or a migration utility;
- `MERGE`, `OPTIONAL MATCH`, `UNION`, or `UNWIND`;
- schema DDL, stored procedures, or user-defined functions;
- unbounded paths, shortest paths, all-shortest paths, or temporal journeys;
- pairwise-overlap or time-increasing path semantics;
- JIT compilation or generated machine code;
- a secondary-index catalog or a general Cascades optimizer;
- distributed planning or execution;
- history retention, valid-time GC, or transaction-time GC;
- a duplicated time-major SST projection;
- replacing size-tiered compaction with leveled compaction.

Unsupported language features fail with `UnsupportedFeature`. They never invoke an old engine.

## 5. Cedar T-Cypher V1

### 5.1 Language Identity

The language version is `CEDAR_TCypher_V1`. Cedar owns its grammar and semantics. It is informed by Cypher, T-Cypher research, T-GQL, and SQL:2011 temporal syntax, but it does not claim strict compatibility with any one external dialect.

A non-temporal statement such as:

```cypher
MATCH (n:Person)
WHERE n.age >= 18
RETURN n.name
ORDER BY n.name;
```

is valid because ordinary graph matching is a subset of Cedar T-Cypher, not because a legacy compatibility parser remains present.

The first version supports the core clauses needed by the approved vertical slice:

- `MATCH` with fixed-length and bounded variable-length patterns;
- `WHERE`;
- `RETURN` and `DISTINCT`;
- grouping and `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `COLLECT`;
- `ORDER BY`, `SKIP`, and `LIMIT`;
- `CREATE`, `SET`, and `DELETE` with `VALID FROM`;
- `BEGIN SNAPSHOT`, `BEGIN STRICT`, `COMMIT`, and `ROLLBACK`;
- `EXPLAIN` and `EXPLAIN ANALYZE`.

### 5.2 Query-Level Temporal Scope

A statement may define a valid-time state scope and a system-time snapshot scope:

```cypher
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-07-17T10:00:00Z'
FOR VALID_TIME AS OF TIMESTAMP '2025-01-01T00:00:00Z'
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a, r, b;
```

The system-time clause is optional. Without it, the statement uses the `visible_seq` captured at query start.

The valid-time clause is optional. Without it, the statement evaluates valid time at the statement-start physical timestamp. `now()` and other stable temporal functions are evaluated once per statement, not once per row or batch.

All intervals are half-open `[start, end)`. Empty and reversed intervals are binding errors.

### 5.3 MATCH-Level Temporal Scope

The query-level scope is a default. Each `MATCH` clause may override valid time, system time, or both:

```cypher
FOR VALID_TIME AS OF $t1
MATCH (a:Person)
MATCH (b:Person) FOR VALID_TIME AS OF $t2
WHERE a.id = b.id
RETURN a, b;
```

All graph elements in one pattern share that pattern's temporal scope. Individual nodes and edges cannot declare unrelated times inside the same pattern. This keeps edge and path visibility coherent.

Different `MATCH` clauses may use different system-time cutoffs. The query still pins one current VersionSet and one current SchemaRegistry containing the retained historical versions. Each scan receives its own `snapshot_seq` cutoff, and no cutoff may exceed the statement's captured `visible_seq`.

### 5.4 State Queries

`FOR VALID_TIME AS OF t` returns graph state visible at time `t` and the selected system snapshot.

`FOR VALID_TIME BETWEEN t1 AND t2` returns every maximal state interval that intersects `[t1, t2)`. Returned `valid_from` and `valid_to` are the true derived event boundaries, not clipped query boundaries. The storage scan therefore reads the visible predecessor at the left boundary and the first required successor beyond the right boundary.

If no visible successor exists, `valid_to` is the typed positive-infinity sentinel used by the storage contract. It is not replaced by the query end time.

State intervals are demand-driven. They are split by:

- vertex and edge existence facts used by the match;
- every property referenced by a predicate, projection, grouping, ordering, or aggregate;
- every property of an entity returned as a complete value by `RETURN n` or `RETURN r`;
- temporal provenance fields explicitly projected by the query.

An unreferenced property change does not split a result interval. Adjacent result intervals are coalesced only when they are contiguous and all demanded values and provenance fields are equal.

Property absence is represented as a runtime null in expressions. Entity or edge absence removes the binding from `MATCH`.

### 5.5 Change Queries

Change queries expose immutable events instead of reconstructed state:

```cypher
CHANGES FOR VALID_TIME BETWEEN $from AND $to
MATCH (n:Person)
RETURN n, operation(n), commit_seq(n);
```

`CHANGES FOR VALID_TIME BETWEEN t1 AND t2` selects events whose `valid_from` lies in `[t1, t2)` at the chosen system snapshot.

`CHANGES FOR SYSTEM_TIME BETWEEN t1 AND t2` selects commits whose persisted HLC lies in `[t1, t2)`. A valid-time state or range predicate may additionally restrict those events. Only one axis is the change axis in one statement.

`SYSTEM_TIME BETWEEN` is not a state query in V1. System time supports state `AS OF` and audit `CHANGES BETWEEN`, while valid time remains Cedar's primary business-time dimension.

### 5.6 Temporal Metadata Functions

The binder recognizes typed temporal fact expressions:

```text
valid_from(x)
valid_to(x)
system_time(x)
commit_seq(x)
operation(x)
```

For a vertex or edge binding, provenance refers to its existence fact. For a property expression such as `n.name`, it refers to the selected property fact. For a path, valid bounds refer to the intersection domain of all path elements. Projecting provenance makes that provenance part of demand-driven interval splitting and coalescing.

`operation(x)` is most useful in `CHANGES` mode. In a state query it reports the selected visible event operation and is necessarily `PUT` for an existing matched entity.

### 5.7 Temporal Paths

At `AS OF t`, every vertex and edge in a returned path must be visible at `t`.

For a valid-time range, the path domain is:

```text
query candidate domain
INTERSECT every vertex existence interval
INTERSECT every edge effective interval
INTERSECT every demanded property interval
```

An edge's effective interval already includes its own existence and both endpoint existence intervals. The extra intersections make multi-hop and property-dependent path semantics explicit.

Variable-length paths use `*min..max`, require a finite maximum, and use `TRAIL` semantics: an edge identity cannot repeat in one path. Repeating a vertex is permitted unless a later language version adds an explicit simple-path modifier.

V1 does not infer pairwise, sequential, or waiting semantics. Those require distinct syntax and a separate design.

## 6. Temporal Mutations and Transactions

### 6.1 Valid-Time Mutations

Every temporal mutation supplies only an event start:

```cypher
CREATE (n:Person {name: 'Li'}) VALID FROM $t;

MATCH (n:Person {id: $id})
SET n.name = 'Wang' VALID FROM $t;

MATCH (n:Person {id: $id})
DELETE n VALID FROM $t;
```

`VALID TO` is illegal. The binder reports `UnsupportedTemporalIntervalWrite` because `valid_to` is derived from the next visible event for the same logical key.

Writes may use arbitrary historical `valid_from` values. An out-of-order write creates a new event and logically splits an existing interval. It does not rewrite an older event in place.

`SET` creates property `PUT` events. Removing a property creates a property `DELETE` event. Deleting a vertex or edge creates an existence `DELETE` event. Vertex deletion does not cascade to edge records; the approved intersection visibility rule controls edge visibility.

Creating an edge requires both endpoints to be visible at the write's valid time in the transaction snapshot plus the transaction's own pending-event overlay. This permits one transaction to create endpoints and then an edge between them. Creating an already visible exact identity or updating an entity that is not visible at that valid time returns a constraint error unless the statement's defined operation explicitly permits it.

### 6.2 Transaction Boundaries

Statements are autocommit by default. Sessions may use:

```cypher
BEGIN SNAPSHOT;
BEGIN STRICT;
COMMIT;
ROLLBACK;
```

`SNAPSHOT` uses the correctness-kernel durable snapshot-isolation mode. `STRICT` uses its exact-key strict-serializable mode.

A strict transaction that attempts a predicate scan, range scan, adjacency expansion, or graph pattern match fails with `UnsupportedSerializablePredicate`. The engine never silently downgrades isolation. Exact-key plans remain eligible for strict execution.

Historical system-time clauses are read-only. A mutation always commits at the current sequencer-assigned system time, although its `valid_from` may be in the past or future.

### 6.3 TransactionSink

`TransactionSink` converts typed mutation batches into `PendingEvent` values. It cannot write a MemTable, SST, Blob file, or WAL directly.

The sink's contract is:

1. evaluate the complete statement against its stable transaction snapshot;
2. validate schema and graph constraints;
3. resolve every large value to a durable BlobRef;
4. submit all pending events to `TransactionManager`;
5. wait for the global visible commit prefix;
6. expose mutation results only after successful commit.

Any pipeline, I/O, constraint, cancellation, or conflict failure aborts the transaction. A mutation with `RETURN` buffers or spills its result until commit; no uncommitted result row reaches the client.

## 7. End-to-End Architecture

The only production query path is:

```text
T-Cypher text
  -> Tokenizer and Parser
  -> Temporal AST
  -> Binder and Type Checker
  -> Logical Plan
  -> Rule and Bounded Cost Optimization
  -> Physical Plan
  -> Pipeline Graph
  -> Morsel Scheduler
  -> Vectorized Storage Query API
  -> Zone-Columnar SST / MemTables / Blob
```

### 7.1 Parser

The parser owns syntax only. It creates immutable source-located AST nodes and does not consult storage, resolve schema, evaluate `now()`, or assign transaction semantics.

Temporal clauses are represented once in the new AST. There is no parallel `TemporalModifier` and `TemporalClause` hierarchy.

### 7.2 Binder

The binder receives a SchemaRegistry view, parameters, session settings, and statement-start temporal values. It resolves:

- labels, relationship types, property columns, and schema epochs;
- variable and `MATCH`-scope ownership;
- query/runtime types and nullability;
- valid-time and system-time scopes;
- temporal metadata provenance;
- legal transaction mode and statement kind.

The output is a typed bound statement. No physical Page or file identity appears in it.

### 7.3 QuerySnapshot

Each statement creates one immutable query envelope:

```text
QuerySnapshot {
  visible_seq_ceiling,
  pinned_version_set,
  pinned_schema_registry,
  blob_reader_epoch,
  statement_start_hlc,
  resolved_temporal_contexts[]
}
```

Every resolved system-time cutoff satisfies:

```text
resolved_snapshot_seq <= visible_seq_ceiling
```

The pinned current VersionSet contains retained historical events. A historical query selects by `commit_seq`; it does not reopen an obsolete file set. Manifest and snapshot lifetime rules still prevent physical deletion of files referenced by an in-flight reader.

### 7.4 Planner and Runtime Boundary

The planner produces immutable physical operator specifications. The runtime creates per-query shared state and per-worker local state from those specifications. Operators never store mutable execution state in cached plans.

## 8. Logical Planning

The core logical nodes are:

```text
LogicalTemporalScan
LogicalChangeScan
LogicalPropertyGather
LogicalExpand
LogicalVariableExpand
LogicalIntervalAlign
LogicalIntervalIntersect
LogicalTemporalCoalesce
LogicalFilter
LogicalProject
LogicalJoin
LogicalAggregate
LogicalDistinct
LogicalSort
LogicalLimit
LogicalMutation
LogicalProduceResult
```

### 8.1 Temporal Lowering

`AS OF` lowers to candidate version access plus `AsOfResolve`. The resolve may be fully pushed into storage when all required facts belong to one storage query contract.

`BETWEEN` lowers to event access, predecessor/successor fence reads, interval derivation, demand-driven interval alignment, expression evaluation, and final temporal coalescing.

`CHANGES` lowers to event access without state coalescing. It retains operation and commit provenance as ordinary typed columns.

An edge pattern automatically introduces endpoint existence dependencies. A range path introduces interval-intersection nodes at every expansion step.

`LogicalMutation` produces pending immutable events and terminates in `TransactionSink`.

### 8.2 Demand Analysis

Before physical planning, the planner computes a `FactDemandSet` containing:

- existence facts needed by graph bindings;
- property facts needed by predicates;
- property facts needed by projections and complete-entity results;
- provenance fields needed by temporal functions;
- keys and values needed by grouping, ordering, and joins.

The demand set drives storage projection, interval splitting, and Blob admission. It is a correctness structure, not only an optimization hint.

## 9. Optimization

### 9.1 Deterministic Rules

The first optimization layer always applies correctness-preserving rules:

- push valid-time and system-time cutoffs into scans;
- push file, Block, dictionary, and zone-map predicates;
- read predicate facts before projection-only facts;
- delay complete entity materialization;
- delay Blob resolution until a surviving value is consumed;
- push filters, projections, and safe limits;
- simplify constants and common expressions;
- select outgoing or incoming adjacency orientation from bound endpoints;
- combine compatible interval alignment and intersection operations;
- insert temporal coalescing only after all demanded facts are known.

### 9.2 Bounded Cost Optimization

The second layer uses a `StatisticsProvider` backed by VersionSet and SST metadata:

- entity and edge counts;
- per-label and per-type counts when available;
- value min/max, null/absence, and dictionary cardinality estimates;
- Block selectivity estimates;
- adjacency degree histograms or conservative fallback estimates;
- historical event density and interval-fragmentation estimates.

For a small connected pattern, a bounded dynamic program selects scan start, join order, and Expand order. The implementation plan will define the exact enumeration threshold. Larger patterns use a deterministic greedy algorithm so planning time remains bounded.

The cost layer chooses among ordinary Expand, `ExpandInto` for two already-bound endpoints, hash join, and interval join. Missing statistics use conservative defaults and never affect correctness.

V1 deliberately does not introduce a generic Cascades framework or secondary-index selection. Those belong to the later index-catalog and cost-planner design.

### 9.3 Plan Cache

Cached plans are keyed by:

```text
normalized query
language version
parameter type signature
schema epoch set
relevant session semantics
```

Time parameter values and captured snapshots are runtime data and are never embedded in a reusable plan. Statistics-version changes may trigger reoptimization but do not invalidate correctness. Cached plans contain no worker, buffer, cursor, or Snapshot state.

## 10. Vector Runtime

### 10.1 ColumnBatch

The internal exchange unit is:

```text
ColumnBatch {
  row_count,
  capacity,
  vectors[],
  validity_masks[],
  selection_vector,
  retained_buffers[]
}
```

The standard capacity is 2,048 logical rows. An 8,192-row SST Block is normally exposed as several batches. Operators may return a smaller batch but cannot grow an output allocation without charging the query memory account.

`selection_vector` contains 32-bit row indices into the current vectors. A filter updates selection state and does not immediately compact or copy every vector.

`retained_buffers` pins referenced PageCache, ValueArena, dictionary, or Blob buffers until the consuming operator releases the batch.

### 10.2 Vector Representations

Runtime vectors support:

- `FlatVector` for materialized typed values;
- `ConstantVector` for one repeated value;
- `DictionaryVector` for indirection through a selection or parent vector;
- `StructVector` for nodes, relationships, events, and intervals;
- `ListVector` for lists and result paths;
- `BlobRefVector` for unresolved content hashes and location hints;
- `PathStateVector` for query-private path arena references.

Graph expansion uses dictionary references for repeated input columns instead of copying the full parent row for every edge. This is a limited, explicit form of factorization, not a separate persisted representation.

Nullability is a runtime concern even though SST property absence is stored as temporal `DELETE`. An absent selected property produces a cleared validity bit. Missing vertex or edge existence removes the graph binding.

### 10.3 Operator State

Each physical operator defines:

- immutable plan state;
- per-query shared state;
- per-worker local state;
- memory and spill hooks;
- cooperative cancellation points;
- metric counters.

No operator uses a process-global mutable singleton.

## 11. Morsel-Driven Pipelines

### 11.1 Pipeline Graph

A physical plan is split into Source, Transform, and Sink pipelines:

```text
SST/MemTable Source
  -> Temporal Resolve
  -> Property Gather
  -> Filter
  -> Expand
  -> Project
  -> Sink
```

Hash build, Aggregate, Distinct, Sort, materialized path frontier, and TransactionSink are pipeline breakers. When a breaker finishes, its finalized state becomes a Source for dependent pipelines.

### 11.2 Morsels

A morsel is a bounded unit of independent work:

- one or more row ranges from an SST Block;
- a sorted MemTable logical-key range;
- a batch of property gather keys;
- a batch of source vertices for adjacency expansion;
- a partition of a path frontier;
- a spill partition for rebuild or merge.

Morsels carry logical ranges and query-owned cursor state, not durable file ownership. The QuerySnapshot and storage cursor own file lifetimes.

### 11.3 Scheduler

The database owns a fixed execution worker pool. A query receives an admission grant and creates runnable pipeline tasks. Workers use work stealing across eligible morsels while respecting query concurrency and memory limits.

Bounded queues connect asynchronously produced batches. When a consumer is slow, backpressure stops Sources from issuing more page reads or path work.

Without `ORDER BY`, parallel result order is unspecified. With `ORDER BY`, a global Sort Sink establishes the requested order before result publication.

## 12. Physical Operators

### 12.1 Sources

- `TemporalScanSource`: state candidates for existence or property partitions.
- `ChangeScanSource`: immutable valid-time or system-time events.
- `ParameterSource`: one batch of bound scalar or table parameters.
- `PathFrontierSource`: one hop's active path states.
- `SpillPartitionSource`: verified temporary partitions.

### 12.2 Transforms

- `AsOfResolve`: chooses the visible event at one valid time and system cutoff.
- `IntervalDerive`: converts visible event streams into implicit intervals.
- `IntervalAlign`: sweep-merges temporal facts by boundary without a Cartesian product.
- `IntervalIntersect`: intersects row, edge, endpoint, property, and path domains.
- `TemporalCoalesce`: merges contiguous identical demanded states.
- `VectorFilter` and `VectorProject`: evaluate typed vector expressions.
- `PropertyGather`: batch-loads demanded property event streams by logical identity.
- `VectorExpand`: expands a batch of sources through EdgeOut or EdgeIn.
- `ExpandInto`: verifies edges between already-bound endpoints.
- `HashProbe` and `IntervalJoinProbe`: join ordinary and temporal bindings.

### 12.3 Sinks

- `HashBuildSink`;
- `AggregateSink`;
- `DistinctSink`;
- `SortSink`;
- `PathFrontierSink`;
- `ResultSink`;
- `TransactionSink`.

Blocking sinks must either spill or fail before exceeding their hard memory grant. Unbounded in-memory accumulation is not a valid implementation.

## 13. Vectorized Storage Query Contract

The execution engine uses these conceptual interfaces:

```text
OpenTemporalScan(QuerySnapshot, ScanSpec) -> ScanCursor
ScanCursor.NextMorsel()                   -> ColumnBatch
BatchGather(ColumnBatch, FactDemandSet)  -> ColumnBatch
ExpandBatch(ColumnBatch, ExpandSpec)     -> ColumnBatch
```

`ScanSpec` contains:

- storage shard and entity-kind constraints;
- label, relationship type, or column partitions;
- state or change mode;
- valid-time point or range;
- system `snapshot_seq` cutoff or HLC change range;
- demanded system and property columns;
- pushdown predicates;
- output ordering requirements.

The contract returns typed logical facts. It does not expose `Descriptor`, old `CedarKey` reconstruction behavior, or an SST-specific row object.

### 13.1 Read Sequence

The required read sequence is:

1. The pinned VersionSet selects every relevant MemTable, FrozenMemTable, and SST.
2. File ranges, Bloom filters, BlockIndexes, zone maps, and dictionaries prune Blocks.
3. The reader fetches Entity, Target, ValidFrom, CommitSeq, and Operation pages first.
4. `TemporalReadMerger` merges candidates across all sources.
5. Temporal resolution and predicates produce a SelectionVector.
6. The reader fetches only selected typed property pages.
7. Blob payloads resolve only for surviving values actually consumed by an expression or result.

No fixed cap may truncate the set of SSTs considered. A single-file reader returns candidates; only the merger produces final cross-file temporal facts.

### 13.2 MemTable and SST Equivalence

MemTables expose sorted event cursors implementing the same logical batch contract as SST readers. Query operators must not branch into unrelated MemTable and disk semantics.

The selected result for one logical key is always derived by:

```text
commit_seq <= selected_snapshot_seq
valid_from satisfies the state or change request
maximum valid_from for a point state
maximum commit_seq for equal valid_from
apply PUT or DELETE
```

Flush, compaction, restart, and file relocation cannot change this result.

### 13.3 Range State Reads

For a valid-time range, storage supplies:

- the selected visible predecessor at or before the left bound;
- every selected event whose valid time can change state inside the range;
- the first selected successor needed to close the final true interval;
- equal-valid-time transaction versions required by the system cutoff.

`IntervalDerive` produces true half-open implicit intervals. `IntervalAlign` then aligns only demanded facts. It uses a boundary sweep per logical binding and does not form a property-event Cartesian product.

### 13.4 PropertyGather

The entity-major, per-property SST layout does not provide wide row alignment. `PropertyGather` therefore groups surviving requests by:

```text
(storage_shard, entity_type, column_id, schema_epoch)
```

Each request carries logical identity, temporal domain, and system cutoff. Physical row locations are only query-local hints and cannot become durable identities because compaction may move data.

Predicate properties are gathered before projection-only properties. Returning a complete node or relationship expands the demand set to every visible registered property and is intentionally more expensive than a narrow projection.

## 14. Vectorized Graph Expansion

`VectorExpand` receives a batch containing source entity identities and input temporal domains. It groups sources by shard and adjacency partition, reads EdgeOut or EdgeIn system pages, and emits complete relationship identity:

```text
edge_id
source_id
target_id
edge_type
direction
valid interval
commit provenance
```

For each candidate, it batch-gathers source and target existence facts and calculates:

```text
input path interval
INTERSECT edge existence interval
INTERSECT source existence interval
INTERSECT target existence interval
```

Candidates with an empty intersection are removed before relationship properties or Blob values are read.

### 14.1 Variable-Length Paths

A variable-length plan iterates a frontier pipeline up to the bound maximum hop count. A path state contains:

- current endpoint;
- start endpoint;
- hop count;
- current valid interval;
- query-arena reference to predecessor path state;
- persistent or copy-on-write visited-edge identity set for `TRAIL` checking.

Each hop expands a batch, intersects intervals, rejects repeated edges, and partitions surviving states into output paths and the next frontier. Empty intervals, cancellation, maximum hops, and hard resource limits terminate work deterministically.

Path state and visited sets are spillable. The spill representation is query-private and checksummed; it is not an SST, WAL, or recoverable database file.

## 15. System Time and CommitTimeline

### 15.1 HLC Assignment

The CommitSequencer assigns a single-node hybrid logical clock value with every committed `commit_seq`:

```text
SystemHlc {
  physical_us,
  logical_counter
}
```

Allocation and DecisionLog append order satisfy:

```text
commit_seq(a) < commit_seq(b)
IMPLIES system_time_hlc(a) < system_time_hlc(b)
```

The physical component follows the wall clock when it advances. Equal or regressing clocks advance the logical component while preserving strict monotonicity. Recovery restores the last durable HLC before assigning another commit.

Logical-counter overflow is a checked condition. The sequencer advances `physical_us` by one and resets the logical counter; integer overflow of the physical component is a fatal database-limit error rather than wraparound.

The DecisionLog `COMMIT` record is refined to:

```text
COMMIT {
  txn_id,
  commit_seq,
  system_time_hlc,
  participant_prepare_refs[],
  record_checksum
}
```

`commit_seq` remains the MVCC and visible-prefix authority.

### 15.2 Timestamp Resolution

`FOR SYSTEM_TIME AS OF timestamp` resolves the input as the HLC upper key `(timestamp_us, MAX_LOGICAL)` and selects the greatest commit at or below it. This includes every logical counter assigned at that physical value. The result is then capped by the statement's `visible_seq_ceiling`.

For `CHANGES FOR SYSTEM_TIME BETWEEN start AND end`, the bounds map to `(start_us, 0)` inclusive and `(end_us, 0)` exclusive. Thus every commit sharing the start physical timestamp is included and every commit sharing the end physical timestamp is excluded.

Exact audit output exposes both `system_time` and `commit_seq`, so commits sharing one physical timestamp remain distinguishable.

### 15.3 Durable Timeline

`CommitTimeline` maintains ordered HLC-to-commit mappings through:

- DecisionLog COMMIT records after the latest checkpoint;
- immutable checksummed timeline checkpoints;
- Manifest metadata identifying the live checkpoint and covered DecisionLog position.

A DecisionLog prefix cannot be removed until a Manifest-live timeline checkpoint covers every mapping in that prefix. Timeline checkpoints follow the same temporary-write, fsync, rename, directory-fsync, and Manifest-publication protocol as other durable metadata.

Recovery verifies ordering, uniqueness, checkpoint coverage, and consistency with DecisionLog decisions. Missing or contradictory committed mappings are corruption. A request older than retained timeline and event history returns `SnapshotNotAvailable`; it never substitutes the oldest available snapshot.

## 16. Memory, Spill, and Admission

### 16.1 Hierarchical Accounting

Memory is charged before allocation:

```text
EngineMemory
  -> QueryMemory
      -> OperatorLocal
      -> OperatorShared
      -> PinnedPage
      -> PathArena
      -> ResultBuffer
```

Each query has a soft target and a hard limit. PageCache owns a global capacity, but pages pinned by one query also consume that query's pinned-page allowance.

### 16.2 Spill

At the soft limit, supported operators revoke memory by spilling:

- hash build partitions;
- aggregate partitions;
- distinct partitions;
- sorted runs;
- path frontiers and visited-edge state;
- staged pending-mutation batches;
- uncommitted mutation result batches.

Spill files use a versioned, query-private typed batch format with length bounds and checksums. They are not added to Manifest and are never treated as recoverable database state. Query cleanup and startup orphan cleanup may delete them.

If an operator cannot release enough memory before the hard limit, the query fails with `QueryMemoryLimit`. It cannot silently allocate outside accounting.

### 16.3 Admission and Backpressure

An admission controller limits concurrent query memory grants and worker concurrency. Bounded batch queues propagate backpressure to page reads and path expansion. This stage does not define a full HTAP workload-class scheduler; maintenance and workload prioritization remain a later design.

## 17. Cancellation and Error Semantics

Cancellation can originate from:

- client cancellation;
- query deadline;
- administrator termination;
- the first operator, storage, or transaction error.

Operators check a shared cancellation state at least at batch boundaries, page I/O boundaries, spill operations, and every path-frontier iteration. After cancellation, the scheduler stops creating new morsels, in-flight tasks exit cooperatively, and the query releases snapshots, buffers, and temporary files.

The public error model includes:

```text
ParseError
BindError
UnsupportedFeature
UnsupportedTemporalIntervalWrite
SchemaMismatch
SnapshotNotAvailable
UnsupportedSerializablePredicate
SerializationConflict
ConstraintViolation
QueryMemoryLimit
QueryCancelled
DeadlineExceeded
IOError
Corruption
BlobCorruption
```

Page corruption, unresolved Blob hashes, checksum failures, and missing committed files are hard errors. They cannot become nulls, absent properties, empty results, tombstones, or warnings.

A query error does not terminate the database process. Recovery that cannot establish a trusted Manifest, visible prefix, CommitTimeline, or required committed file prevents database open.

Read-only result streaming may deliver batches before a later error, but the stream always ends with an explicit terminal status. Mutation results are not streamed before commit.

## 18. Result and Protocol Boundary

The engine returns:

```text
QueryResultStream<ResultBatch>
```

`ResultBatch` contains typed vectors, null masks, column names, and optional temporal metadata. It is a new public result contract and is not the old `Record` or `ResultSet` in batch form.

JSON, gRPC, row iteration, or Arrow-compatible adapters live at protocol boundaries. They consume `ResultBatch`; physical operators never construct protocol objects. Arrow compatibility is an exchange option, not the persisted SST format.

## 19. Explain and Runtime Observability

`EXPLAIN` reports:

- bound temporal scopes;
- system-time cutoffs after resolution when execution context is available;
- logical rewrites;
- selected scan, gather, join, and Expand strategies;
- demanded properties and Blob materialization points;
- pipeline boundaries and expected parallelism;
- estimated rows, intervals, and cost.

`EXPLAIN ANALYZE` additionally reports per operator and pipeline:

```text
input and output rows
input and output intervals
event candidates and versions rejected
files, Blocks, and Pages considered/read/pruned
compressed and decoded bytes
property gather requests and batches
Blob references seen and payloads resolved
Expand sources, edges considered, and edges emitted
path frontier sizes by hop
CPU and blocked I/O time
memory peak and pinned-page peak
spill bytes and partitions
morsel count, worker time, and skew
```

Metrics are query-owned and merged at pipeline completion. They do not require locks on every row.

## 20. Module Boundaries

The new ownership layout is:

```text
tcypher/
  syntax/
    tokenizer
    parser
    ast
  binder/
    name_resolver
    type_checker
    temporal_scope
  logical/
    logical_plan
    temporal_rewriter
    demand_analysis
  optimizer/
    rules
    statistics_provider
    bounded_cost_model
  physical/
    physical_plan
    pipeline_builder
  runtime/
    column_batch
    vectors
    scheduler
    memory
    spill
    cancellation
  operators/
    scan
    gather
    expand
    interval
    join
    aggregate
    path
    mutation
  result/
    result_batch
    query_result_stream

storage/query/
  temporal_scan_cursor
  property_gather
  vector_expand
  commit_timeline
```

Dependency rules are mandatory:

- storage query modules do not include AST, binder, logical, or physical plan headers;
- parser and AST do not include storage headers;
- cached plans do not own runtime or snapshot state;
- query operators cannot append WAL, publish Manifest edits, or write SST/Blob files;
- storage cursors return typed logical facts, not query-language values;
- protocol adapters depend on result contracts, not physical operators.

## 21. Complete Legacy Removal

This is a clean implementation and API break. Completion requires removal of:

- the old `cypher/` Parser, AST, temporal-dialect, planner, and execution-plan implementation;
- `PhysicalOperator::Next()` and every row-at-a-time production operator;
- old `ExecutionContext`, `Record`, and `ResultSet` contracts;
- unfinished temporal operator stubs;
- duplicate graph-facade Cypher entry points;
- the old plan cache and any old-plan conversion logic;
- `ExecuteCypher()` compatibility entry points;
- Descriptor-returning query paths already rejected by columnar;
- dual-engine build flags, runtime format switches, and fallback behavior.

The new entry point is `ExecuteTcypher()` and the new result is `QueryResultStream<ResultBatch>`. No compatibility adapter or migration utility is provided.

Static repository checks must prove that removed public types and entry points have no callable references. Test-only scalar oracle types use distinct names and cannot be linked into the production engine.

## 22. Relationship to Mainstream Designs

### 22.1 Adopted Ideas

| Design family | Cedar adopts |
|---|---|
| DuckDB-style vector execution | 2,048-row vectors, selection vectors, typed batches, pipeline execution, and late materialization |
| ClickHouse-style analytical reads | Block execution, data skipping, predicate-first decoding, and batch codecs |
| Columnar graph engines such as Kuzu | graph-specific Expand, batch adjacency access, and dictionary-factorized parent bindings |
| Neo4j pipelined execution | morsel-oriented graph pipelines and bounded runtime state |
| HyPer/Umbra execution research | explicit pipeline breakers, morsel parallelism, and compact worker-local state |
| SQL:2011 temporal systems | clear valid-time and system-time distinction and reproducible historical snapshots |

### 22.2 Deliberate Differences

Cedar does not persist generic wide relational row groups. It preserves entity-major, per-property temporal partitions and joins demanded properties by logical identity.

Cedar does not materialize one current graph snapshot as the only CSR truth. Immutable valid-time events and `commit_seq` versions remain queryable through the same storage path.

Cedar does not make explicit `valid_to` a write primitive. State intervals are derived from successor events at the selected system snapshot.

Cedar does not duplicate a time-major projection, replace size-tiered compaction, persist Arrow buffers, or introduce code generation in V1.

### 22.3 Known Trade-Offs

1. `RETURN n` over wide entities requires gathers from many property partitions and is weaker than a row-aligned wide table.
2. Size-tiered compaction can require merging more SSTs than leveled designs.
3. A global pure time-range scan is not favored by the entity-major single projection.
4. Multi-hop expansion combined with property history can fragment intervals and enlarge intermediate state.

The mitigations are batch gather, Block pruning, parallel all-SST merge, demand-driven interval alignment, temporal coalescing, dictionary-factorized Expand, path budgets, and spill. Cedar does not hide these costs by abandoning its entity/adjacency-first and low-write-amplification purpose.

## 23. Verification Strategy

### 23.1 Syntax and Binding

- Golden tests cover every V1 clause, valid/system scope combination, and temporal DML form.
- Source positions and diagnostics are deterministic.
- Invalid intervals, `VALID TO`, unsupported syntax, schema mismatches, and illegal strict scans fail during binding when possible.
- Time expressions and `now()` are stable within one statement.

### 23.2 Vector Runtime

- Every operator is tested with Flat, Constant, Dictionary, Struct, List, and BlobRef inputs where applicable.
- Empty, full, sparse-selection, null, minimum-capacity, and maximum-capacity batches are covered.
- Batch boundaries do not change results.
- Buffer pinning tests detect use-after-release and uncharged retention.
- Parallel and single-worker executions produce equivalent unordered multisets or identical ordered results.

### 23.3 Independent Temporal Oracle

Tests include a simple scalar bitemporal reference model that is not compiled into the production engine. Property-based tests generate:

- out-of-order valid-time events;
- equal-valid-time events at different commits;
- PUT/DELETE/restore histories;
- multiple property timelines;
- vertex deletion and restoration around persistent edge events;
- HLC ties and clock regressions.

State, change, interval, provenance, and path results from vector execution must equal the oracle.

### 23.4 Storage Equivalence

The same query corpus runs against:

- active MemTables only;
- active and FrozenMemTables;
- one SST;
- many overlapping size-tiered SSTs;
- post-flush state;
- post-compaction state;
- reopened database state;
- Blob data before and after relocation.

Results and terminal status must be identical. No test may cap the number of SSTs participating in a read.

### 23.5 CommitTimeline and Recovery

- Multiple commits in one physical microsecond remain strictly ordered.
- Wall-clock regression does not regress HLC.
- Group commit preserves DecisionLog, HLC, and `commit_seq` order.
- Recovery restores the HLC high-water mark.
- Timeline checkpoints block premature DecisionLog truncation.
- Missing, reordered, or contradictory timeline mappings are detected as corruption.
- Requests before retained history return `SnapshotNotAvailable`.

### 23.6 Graph and Path Correctness

- Edge visibility always intersects edge and endpoint existence.
- Restoring a vertex can restore visibility of an unchanged edge event.
- Every range hop propagates and intersects its temporal domain.
- `TRAIL` never repeats an edge identity.
- Maximum hop count, empty intervals, cancellation, and resource limits terminate exploration.
- Incoming and outgoing expansion preserve complete edge identity.

### 23.7 Resource and Failure Tests

- Sort, Aggregate, Hash Join, Distinct, and path frontiers complete correctly through spill.
- Disk-full and spill-corruption failures clean up and return errors.
- Query hard memory limits are never exceeded except for a documented fixed allocator granule.
- Cancellation during page reads, Blob reads, expansion, spill, and commit does not leak snapshots or files.
- Page, index, and Blob corruption remain hard errors.
- Mutation failures never expose partial writes or pre-commit result rows.

### 23.8 Concurrent Snapshot Tests

Long queries run while other threads commit, flush, compact, checkpoint CommitTimeline, and relocate Blob records. The query's selected system cutoffs and results cannot drift. Retired files remain available until all relevant readers release them.

### 23.9 End-to-End Corpus

Acceptance includes executable cases for:

```text
VALID_TIME AS OF
VALID_TIME BETWEEN
CHANGES FOR VALID_TIME
SYSTEM_TIME AS OF
CHANGES FOR SYSTEM_TIME
combined valid-time and system-time scopes
cross-time MATCH clauses
historical CREATE, SET, and DELETE
temporal grouping and aggregation
fixed-length paths
bounded variable-length paths
explicit SNAPSHOT and STRICT transactions
EXPLAIN and EXPLAIN ANALYZE
```

## 24. Structural Performance Acceptance

The design does not claim an arbitrary QPS target before a reproducible benchmark exists. Completion instead requires structural evidence:

1. SST open reads metadata rather than the whole file.
2. Scan metrics prove that pruned Blocks and unprojected Pages are not read.
3. Filtered-out BlobRefs do not cause Blob payload reads.
4. Narrow projections do not gather unrelated property partitions.
5. Operators process batches rather than calling a scalar storage API per row.
6. Independent morsels can execute on multiple workers.
7. Bounded queues stop source overproduction.
8. Query peak memory remains within its configured grant and spill completes supported workloads.
9. Size-tiered reads consider every relevant SST while reporting their read-amplification cost.
10. `EXPLAIN ANALYZE` exposes enough counters to reproduce each claim.

Benchmark design, fixed datasets, workload mixes, and regression thresholds are a later dedicated stage. The metrics required to build that benchmark are part of this stage.

## 25. Implementation Dependency Order

The future implementation plan must respect this dependency order:

1. Extend DecisionLog and recovery with HLC, CommitTimeline, and checkpoints.
2. Introduce new query/runtime types, `ColumnBatch`, vectors, `ResultBatch`, and status contracts.
3. Build vectorized storage scan, temporal merge, gather, and Expand contracts over columnar.
4. Build the new tokenizer, AST, binder, and temporal scope resolver.
5. Implement logical plans, demand analysis, and deterministic rewrites.
6. Implement Scan, Resolve, Filter, Project, and Result pipelines.
7. Implement BETWEEN, CHANGES, interval alignment, and coalescing.
8. Implement fixed-length Expand and endpoint visibility.
9. Implement joins, aggregates, distinct, sort, memory accounting, and spill.
10. Implement bounded variable-length paths and spillable frontiers.
11. Implement temporal DML and TransactionSink.
12. Add protocol adapters, EXPLAIN, metrics, and end-to-end tests.
13. Remove every old Cypher and row-at-a-time production path.
14. Run static clean-break checks, crash tests, and structural performance acceptance.

This order is architectural guidance, not the implementation plan requested by the brainstorming workflow. The implementation plan will split each item into test-first, reviewable tasks only after explicit user approval.

## 26. Completion Definition

The T-Cypher vectorized execution stage is complete only when:

1. `ExecuteTcypher()` is the only production graph query entry point;
2. parser, binder, planner, runtime, and result contracts are entirely new and typed;
3. no production operator exchanges old `Record` values or calls `Next()` per row;
4. valid-time state, range, and change semantics match the independent oracle;
5. system-time state and audit queries use the durable HLC CommitTimeline;
6. every query uses a stable `QuerySnapshot` capped by the visible commit prefix;
7. property demand controls interval splitting, Page reads, and Blob resolution;
8. vectorized Expand enforces edge and endpoint interval intersection;
9. bounded variable-length paths enforce `TRAIL` and resource limits;
10. temporal DML commits atomically through the correctness kernel;
11. memory, spill, backpressure, cancellation, and error contracts pass fault tests;
12. results remain stable across flush, compaction, restart, and Blob relocation;
13. `EXPLAIN ANALYZE` verifies page, Blob, vector, morsel, and spill behavior;
14. all old Cypher APIs, implementations, adapters, and fallback switches are removed;
15. the approved correctness-kernel and columnar invariants remain intact.
