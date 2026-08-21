# Cedar Bitemporal Graph Query Design

**Date:** 2026-08-21
**Status:** Approved
**Scope:** Cedar Kernel query model, projections, planner, runtime, temporal graph
algorithms, resource ownership, recovery, and acceptance

## 1. Decision

Cedar will provide one strongly typed C++ query interface backed by a shared
logical operator model and two physical execution lanes:

- an interactive lane for point, adjacency, and bounded path work;
- a vectorized analytical lane for range, history, join, aggregation, and broad
  graph work.

A future T-Cypher frontend must compile to this same logical plan. It must not
create a second execution engine or a second set of temporal semantics.

Cedar will build rebuildable adjacency, property, state, and statistics
projections outside RocksDB. A projection manifest proves coverage at one base
commit sequence. A bounded, reconstructible `QueryDelta` merges all newer
authoritative facts through the query Snapshot. Missing, lagging, corrupt, or
partially covered projections fall back to authoritative facts and never return
silently stale results.

This design preserves the existing storage authority model:

1. RocksDB owns the only WAL, WAL recovery, MemTables, VersionSet, MANIFEST,
   checkpoints, native flush mechanics, native compaction mechanics, and
   obsolete authoritative-file deletion.
2. CedarParquet facts managed by that RocksDB instance remain the only
   authoritative graph facts.
3. Cedar owns query semantics, projection files, QueryDelta, query admission,
   worker counts, memory, prefetch, scratch, projection building, statistics,
   and I/O scheduling policy.
4. The production product is Cedar Kernel only. There is no Lean query path,
   legacy query compatibility layer, second WAL, second recovery path, or
   dual-write index.

## 2. Goals

The query system must provide:

- exact system-time Snapshot semantics and valid-time temporal semantics;
- point state, history, events, changes, overlap, and throughout queries;
- incoming, outgoing, and bidirectional temporal adjacency;
- typed property predicates with explicit missing-value behavior;
- relational operations, row aggregation, and temporal aggregation;
- bounded k-hop, coexisting shortest paths, and time-ordered journeys;
- low first-result latency for interactive work;
- vectorized throughput, pruning, and bounded spill for analytical work;
- high write throughput by keeping projection maintenance off the durable
  commit path;
- space-efficient compressed projections with observable amplification;
- deterministic correctness across projection, delta, fallback, restart, and
  physical execution choices;
- Debug-first verification followed by Release-only performance claims.

## 3. Non-goals

The first release does not provide:

- a T-Cypher parser or language-specific runtime;
- PageRank, connected components, triangle count, centrality, community
  detection, or all-path enumeration;
- distributed query exchange between database processes;
- a caller-visible stale-read mode;
- arbitrary non-FIFO journey duration expressions;
- SQL NULL or three-valued boolean logic;
- outer joins, JIT compilation, or persistent query cursors;
- an alternate storage profile, old database-format reader, or legacy query
  benchmark.

The deferred graph algorithms may later use the same logical plan, projection,
runtime, and resource modules.

## 4. Existing Foundation and Missing Capability

The current Kernel already provides:

- `Snapshot(as_of CommitSeq)` for system-time pinning;
- `Exists` and `Get` at one valid time;
- event and corrected-state scans;
- columnar scans with projected CedarParquet columns;
- event valid-time and commit-sequence bounds;
- edge visibility intersected with source and target vertex visibility;
- projectable edge source, target, and type columns;
- durable `sequence/<CommitSeq>` records containing the exact fact keys written
  by each transaction.

It does not yet provide adjacency indexes, expand operators, interval-state
interfaces, a query planner/runtime, graph search, query scratch, or projection
coverage/delta merging.

The durable sequence records are sufficient to reconstruct QueryDelta without
adding any WAL record or commit-index format. The implementation will add a
batched internal exact-fact read so reconstruction does not become an N+1 read
path.

## 5. Alternatives

### 5.1 Selected: typed plan plus Cedar derived projections

The selected design puts a deep typed query interface over one logical plan,
uses Cedar-owned derived files for read acceleration, and preserves canonical
fallback. It provides high read leverage without adding synchronous index
writes or moving recovery authority.

### 5.2 Rejected: direct Snapshot methods only

Adding one public method for every scan, expand, join, and path variant would
produce a shallow interface, duplicate optimization logic across callers, and
leave a future query language with incompatible semantics.

### 5.3 Rejected: indexes in new authoritative RocksDB column families

Synchronous adjacency and secondary-property writes would increase WAL bytes,
MemTable pressure, flush/compaction work, write amplification, and recovery
surface. Treating them as authoritative would also violate the single
authoritative columnar-facts model.

### 5.4 Rejected: T-Cypher-first execution engine

A language-specific runtime would make C++ and T-Cypher behavior diverge and
would force storage, temporal, and resource decisions into the parser layer.

## 6. Public Interface and Internal Modules

The supported public objects are:

- `Query`: immutable, strongly typed query construction;
- `PreparedQuery`: bound schema, parameter types, normalized logical plan, and
  reusable physical-plan templates;
- `QueryCursor`: streamed pull execution and cancellation;
- `QueryBatch`: owned columnar output;
- `QueryOptions`: execution mode and hard resource budgets;
- `Bindings`: exact typed parameter values;
- `QueryPlanDescription`: structured Explain output;
- `QueryProfile`: optional per-execution measurements.

The normal flow is:

```cpp
Query query = /* immutable typed construction */;
auto prepared = database->PrepareQuery(query);
auto snapshot = database->BeginSnapshot({.as_of = requested_seq});
auto cursor = prepared->Execute(std::move(snapshot), bindings, options);
while (auto batch = cursor->Next().ValueOrDie()) {
  Consume(*batch);
}
```

`Query` construction uses typed variables, typed parameters, and immutable
logical transformations. It does not accept arbitrary C++ callbacks inside an
expression, because such callbacks could not be represented by the shared
logical plan or a future T-Cypher frontend. Property expressions bind an
expected entity kind and physical type. User-facing aliases are display names;
logical identity remains the stable `SlotId` assigned during construction.

`Query` and `PreparedQuery` are immutable and safe for concurrent execution.
Each Execute consumes one movable Snapshot and creates an independent Cursor.
`QueryCursor::Next` and `Close` have one consumer and are not concurrently
callable; `Cancel` is thread-safe and idempotent. `QueryBatch` is movable and
read-only after publication.

`PrepareQuery` resolves only referenced property definitions and records their
IDs, schema epochs, and physical types. An unrelated schema registration does
not invalidate a prepared query. Execution against an incompatible Snapshot
returns `SchemaMismatch`; Cedar does not silently change result types.

The internal deep modules are:

```text
LogicalPlan           semantic operator tree and typed expressions
QueryPlanner          normalization, rewrites, costing, and late binding
QueryRuntime          pull pipelines, vector kernels, joins, and graph search
QueryProjectionStore  manifests, segments, coverage, cache, and generations
QueryDelta            contiguous authoritative changes after a projection base
QueryScratch          one-query bounded spill and cleanup
QueryResourcePool     admission, workers, memory, and I/O permits
```

Public Cedar headers expose no RocksDB type. `src/storage/rocks` remains the
only Cedar adapter that names embedded-engine interfaces.

## 7. Bitemporal Model

### 7.1 Two independent time axes

Every query owns one `Snapshot`, whose Cedar commit sequence fixes system time.
Valid time remains a separate business-effective axis.

```cpp
struct ValidTimeInterval {
  ValidTime from;
  std::optional<ValidTime> to;  // nullopt means positive infinity
};
```

All valid-time intervals are half-open `[from,to)`. A bounded interval requires
`from < to`. Empty or reversed intervals are invalid. Open-ended history is
legal only under an analytical budget.

For one fact chain and one `valid_from`, the event with greatest
`commit_seq <= Snapshot.commit_seq()` is the Snapshot-visible corrected event.
Later corrections do not exist in that Snapshot.

A PUT starts an effective value or existence state. A DELETE starts absence.
Corrected events at adjacent valid-time boundaries are interpreted in boundary
order. Redundant corrected events remain events but are not effective changes.

### 7.2 Explicit temporal scopes

The logical model uses an explicit variant rather than an ambiguous `Between`:

```cpp
using TemporalScope = std::variant<
    At,
    Events,
    Changes,
    Overlaps,
    Throughout,
    History>;
```

- `StateAt(t)` returns the state effective at `t`, carrying the maximal state
  interval that contains `t`.
- `EventsBetween([a,b))` returns corrected PUT/DELETE boundaries in the range.
- `ChangesBetween([a,b))` returns only boundaries that change effective state,
  including before/after presence or value.
- `StateOverlaps([a,b))` returns maximal effective intervals intersecting the
  request, clipped to the request.
- `StateThroughout([a,b))` returns state continuously effective for the whole
  request.
- `History(range)` returns corrected maximal state intervals over a bounded or
  explicitly budgeted unbounded range.

`EventsBetween` uses authoritative CedarParquet facts in the first release.
State/history projections accelerate state-oriented operators without copying
every system-time version.

### 7.3 Edge visibility

An edge traversal is visible only over the intersection of:

```text
edge state
source vertex state
target vertex state
requested temporal scope
```

An edge-property predicate adds the property's effective interval to that
intersection. Disjoint repeated periods are returned as separate maximal
intervals.

`ExpandOut`, `ExpandIn`, and `ExpandBoth` return source, EdgeRef, target, edge
type, and maximal common effective interval. Bidirectional expansion is the
union of incoming and outgoing traversals; a self-loop appears once.

## 8. Property and Expression Semantics

A property is either an exact schema-bound value or `Missing`. Missing is not a
value, is not SQL NULL, and is distinct from a DELETE/Unset history event.

- every ordinary comparison against Missing evaluates to false;
- `IsMissing` and `IsPresent` test presence explicitly;
- boolean evaluation is two-valued;
- because comparisons against Missing are false, the optimizer must not rewrite
  `x != y` as `NOT(x == y)` across possible Missing values;
- implicit type conversions are prohibited;
- explicit conversions are checked and fail on overflow;
- a duration expression has the distinct non-negative `ValidDuration` type.

`BindProperty` acts as a temporal lateral binding. At a point it returns the
typed value or Missing. Over an interval it splits input rows at property
boundaries and clips each output to a maximal interval with a stable value or
stable missing state. The planner derives whether missing intervals are needed;
callers do not manage a physical presence mode.

## 9. Logical Operators and Result Schema

The first-release logical operators are:

```text
VertexScan, EdgeScan
StateAt, EventsBetween, ChangesBetween, History
StateOverlaps, StateThroughout
ExpandOut, ExpandIn, ExpandBoth
BindProperty, Filter, Project
InnerJoin, SemiJoin, AntiJoin
UnionAll, Distinct, Sort, Limit
AggregateRows, TemporalAggregate
KHopExpand, CoexistingShortestPath
EarliestArrival, LatestDeparture, FastestDuration
```

Outer joins and all-path enumeration are not included.

Each operator emits a `RowSchema` containing stable `SlotId`, display name,
exact `QueryType`, and presence capability. Expressions refer to slots, not
strings. The type set includes Cedar scalar physical types plus `VertexRef`,
`EdgeRef`, `ValidTime`, `ValidDuration`, `CommitSeq`, `ValidTimeInterval`,
`Path`, and `Journey`.

`QueryBatch` uses typed vectors and presence bitmaps. Path and journey columns
use offsets plus nested vertex, edge, and time columns. Ordinary result rows
have no implied order; only `Sort` establishes one.

`AggregateRows` aggregates emitted rows. For interval input it therefore counts
intervals, not an implicit population over time. `TemporalAggregate` performs a
valid-time boundary sweep and emits maximal intervals during which the
aggregate value is unchanged. At one boundary, interval exits are applied
before interval entries to preserve half-open semantics.

## 10. Projection Files

### 10.1 Projection families

| Family | Sort order and content | Use |
| --- | --- | --- |
| `VertexState` | `(PartID, VertexID)` plus existence history | point/range state and endpoint visibility |
| `EdgeOut` | `(source, edge_type, target, EdgeRef)` plus edge state | outgoing expand and k-hop |
| `EdgeIn` | `(target, edge_type, source, EdgeRef)` plus edge state | incoming expand and reverse search |
| `VertexProperty` | one PropertyId, `(PartID, VertexID, valid_from)` | property predicates/history |
| `EdgeProperty` | one PropertyId, `(home PartID, EdgeID, valid_from)` | edge property predicates/history |
| `Statistics` | bounded segment/region estimates | physical planning only |

Outgoing and incoming adjacency duplicate compressed edge-existence intervals.
This bounded duplication removes random edge-state lookups from the common
expand path. Properties and endpoint vertex state are not copied into adjacency
records. Embedding endpoint state would make one vertex correction invalidate
all incident adjacency records.

Property projections are property-specific so scans read only requested values.
Missing intervals are not stored; they are derived by subtracting property
presence from entity existence.

### 10.2 Corrected boundaries

A projection cannot retain only coalesced maximal state intervals. Consider a
base generation containing `PUT A` at time 0 and a redundant `PUT A` at time 10.
The visible interval coalesces to `[0,+infinity)`. A later correction inserting
DELETE at time 5 must reveal the latent time-10 PUT and restore A from time 10.

Every projected fact chain therefore stores both:

- coalesced maximal state intervals for fast common reads;
- a compact corrected-boundary stream containing `valid_from`, operation, and
  a shared value reference for every winning boundary at the base Snapshot.

Boundary streams use valid-time delta coding, operation bit packing, shared
value dictionaries, and RLE where applicable.

### 10.3 Page layout and compression

Projection pages carry row counts, compressed sizes, checksums, entity and
valid-time min/max, and edge-type min/max where relevant. Implementations may
use delta coding, RLE, dictionaries, LZ4, and bounded Bloom filters. The format
must remain self-describing and versioned.

The files live outside RocksDB:

```text
<db>/query/
  PROJECTION-CURRENT
  manifests/000042.cmanifest
  vertex-state/*.cstate
  edge-out/*.cadj
  edge-in/*.cadj
  vertex-property/<property-id>/*.cprop
  edge-property/<property-id>/*.cprop
  statistics/*.cstats
  scratch/
```

### 10.4 Projection manifest and coverage

A projection manifest is not the RocksDB MANIFEST and has no recovery
authority. It records:

```text
projection format and database identity
generation ID and base CommitSeq
referenced-property schema fingerprints
PartID, entity-key, and valid-time coverage regions
explicit complete regions and holes
segment IDs, roles, row counts, sizes, and checksums
```

Coverage is never inferred from file presence. A segment is usable only through
a pinned manifest generation that proves the requested region complete.

Publication is:

1. write and validate temporary segments;
2. sync and atomically rename each segment;
3. write and sync an immutable `.cmanifest`;
4. atomically replace `PROJECTION-CURRENT` and sync the directory;
5. expose the new generation to readers.

`PROJECTION-CURRENT` contains a version, generation ID, and checksum. If its
atomic replacement or validation is uncertain after a crash, Open disables the
projection rather than selecting a manifest by filename or modification time.

Readers pin one generation. Retired generations remain until their final reader
exits. Temporary and unreferenced files are safe to remove after database Open
has acquired the exclusive database lock.

## 11. QueryDelta

### 11.1 Source and watermarks

`QueryDelta` is an in-memory, rebuildable, commit-ordered representation of all
fact changes after projection `base_seq = B`.

It tracks:

```text
base_seq
indexed_through
visible_seq
contiguous coverage proof for (base_seq, indexed_through]
```

After visible publication, the commit pipeline moves or shares one immutable
commit descriptor into a bounded Cedar queue. It performs no projection I/O
and no statistics maintenance. A Cedar worker indexes the descriptor by fact
chain and adjacency key.

Queue saturation cannot delay or fail an otherwise committed write. If a
descriptor cannot be enqueued, QueryDelta atomically records the first missing
CommitSeq, stops advancing contiguous coverage at its predecessor, and repairs
from durable sequence records. It never skips the descriptor while advancing
`indexed_through`.

If the worker lags or the process restarts, existing durable
`sequence/<CommitSeq>` records supply exact fact keys. A batched private
FactStore operation reads their authoritative fact values. This reuses the one
RocksDB WAL and does not create a persistent derived delta log.

### 11.2 Query selection

For a query Snapshot sequence `S`:

| Condition | Execution |
| --- | --- |
| `S < B` | use a covering older generation or authoritative facts |
| `S == B` | use the base projection directly |
| `B < S <= indexed_through` | merge projection with QueryDelta `(B,S]` |
| `S > indexed_through` | repair a bounded tail or use authoritative fallback |

A gap invalidates projection-plus-delta use for the affected range. Cedar may
repair an interactive tail only within its explicit commit and byte budget.

### 11.3 Chain merge

For every delta-touched fact chain, execution:

1. reads the projected base boundary stream;
2. merges delta events through `S`;
3. picks the greatest visible commit at each `valid_from`;
4. reconstructs and coalesces maximal state intervals;
5. unions new edge identities into both direction candidates;
6. intersects edge state with source and target state;
7. applies property interval clipping.

If one chain cannot prove completeness, only that chain is replaced by an
authoritative read. The executor cannot mix an incomplete chain into results.

### 11.4 Bounds

Initial production bounds are:

- 256 MiB soft memory limit per database;
- 512 MiB hard memory limit;
- 262,144 commits maximum lag;
- 30-second target projection lag;
- per-interactive-query synchronous repair of at most 4,096 commits or 32 MiB.

Soft pressure raises Cedar projection-maintenance priority. At a hard limit the
generation stops claiming mergeability, queries use authoritative facts, and
Cedar schedules a new generation. Active readers pin the generation and delta
view they already acquired.

## 12. Planning

### 12.1 Preparation and late binding

Preparation performs type checking, referenced-schema binding, temporal
validation, logical normalization, and output-schema construction. It does not
bind file addresses or a projection generation.

Execution late-binds one physical plan using the supplied Snapshot, bindings,
projection generation, QueryDelta watermarks, current statistics, and resource
budget.

### 12.2 Rewrite rules

The planner:

- normalizes every interval to half-open form;
- preserves distinct At, Events, Changes, Overlaps, Throughout, and History
  nodes;
- rewrites Both as incoming/outgoing union with self-loop deduplication;
- pushes PartID, entity, edge-type, valid-time, and property constraints;
- clips temporal ranges as early as semantics permit;
- binds expensive properties after reducing candidate entities where possible;
- removes existence checks already guaranteed by Expand;
- prunes unused columns;
- rewrites `IsMissing` to a temporal anti-join between entity and property
  presence;
- pushes Limit only when doing so preserves explicit ordering semantics.

### 12.3 Coverage splitting

A logical source may become disjoint physical regions:

```text
complete region -> ProjectionScan + DeltaMerge
coverage hole   -> CanonicalCedarParquetScan
```

The planner must prove those regions have neither gaps nor overlap. Every branch
uses the same Snapshot and emits the same schema.

### 12.4 Access paths and lanes

Access paths include point fact lookup, projected interval seek, adjacency page
seek, property projection scan, authoritative columnar scan, and
projection-delta merge.

The interactive lane uses small point/MultiGet batches, adjacency seeks,
batched endpoint/property gather, index nested loops, small hashes, bounded BFS,
and bounded priority queues. It does not spill by default.

The analytical lane uses vectorized CedarParquet/projection scans, selection
vectors, hash and sort-merge joins, interval sweeps, partitioned aggregates,
batched graph frontiers, and budgeted spill.

Auto mode initially favors the interactive lane for no more than 4,096
estimated candidates, no more than four hops, no global sort or temporal
aggregate, and no more than 8 MiB estimated memory. These are planner
heuristics, not resource limits. A plan may contain one explicit `LaneExchange`
from a selective interactive prefix to a broad analytical suffix.

### 12.5 Cost model

The cost model includes estimated rows, pages, physical and decoded bytes,
random reads, delta dirty chains, interval fragments, fanout, join/sort memory,
spill I/O, and first-result latency. Missing or stale statistics produce a
conservative plan.

The runtime may adapt only at declared lane exchanges. It does not silently
replace arbitrary operators mid-query.

## 13. Vectorized Runtime and Cursor

`QueryCursor` is pull-driven:

```cpp
StatusOr<std::optional<QueryBatch>> Next();
void Cancel();
Status Close();
```

Calling `Next` drives work and therefore supplies consumer backpressure.
Returned batches own a resource lease. Holding batches counts against the query
budget; a later `Next` waits only until its deadline or returns
`ResourceExhausted`.

Interactive batches initially contain up to 256 rows and analytical batches up
to 4,096 rows, with bounded adaptation. Empty batches are not exposed.

Execution uses typed vectors, presence bitmaps, selection vectors, late
materialization, and type-specialized compiled kernels. The first release does
not include a JIT. Parallel stages communicate only through bounded queues and
reserve memory before allocating.

### 13.1 Joins and interval processing

| Situation | Physical operation |
| --- | --- |
| small entity binding | batched index nested loop |
| medium unordered equality | in-memory hash join |
| sorted key/time input | sort-merge join |
| broad analytical equality | spillable partitioned hash join |
| temporal interval input | endpoint sweep or interval merge |

Temporal joins emit only non-empty intersections. Adjacent output intervals may
coalesce only when every non-time field is identical. Interval-fragment counts
are hard-budgeted.

For temporal aggregation, exits at a boundary are applied before entries, then
the new aggregate state is emitted through the next boundary.

## 14. Graph Algorithms

### 14.1 Point k-hop

Point-in-time BFS stores `(VertexRef, depth, predecessor)`. Reachability keeps
the minimum depth per vertex. K-hop returns endpoints and depth and may include
one witness; it does not enumerate every path.

### 14.2 Coexisting shortest path

A coexisting-path label is:

```text
(VertexRef, common_effective_interval, depth, predecessor)
```

Expansion intersects the current interval with the traversal interval. For one
vertex, a same-or-shallower label whose interval contains another label
dominates it. Layered BFS minimizes hop count. Results are segmented into
maximal intervals for which the selected witness remains valid.

### 14.3 Temporal journeys

A journey label is:

```text
(VertexRef, departure_time, arrival_time, predecessor)
```

Traversal times are nondecreasing. Waiting is permitted only while the vertex
remains effective over `[arrival,next_departure)`. A zero-duration traversal
still requires source, edge, and target visibility at its traversal time.

An explicit nonzero duration must be a non-negative `ValidDuration`, must not
overflow, and requires the source, edge, and target to remain effective for the
whole traversal. Arrival cannot equal an exclusive interval upper bound. A
missing duration makes that traversal unavailable.

- `EarliestArrival` uses FIFO time-dependent Dijkstra.
- `LatestDeparture` uses reverse search over incoming adjacency.
- `FastestDuration` maintains a bounded Pareto frontier of
  `(departure,arrival)` labels.
- non-FIFO expressions are rejected in the first release.

Frontiers store predecessor label IDs rather than copying complete paths. A
path is materialized only for output. Tied witnesses are chosen by objective,
then hop count, then lexicographic EdgeRef sequence.

Every graph query has explicit hop, visited-vertex, label, interval-fragment,
result-row, and deadline limits.

## 15. Query Resources and Scheduling

### 15.1 Hierarchical budgets

Resources are admitted through:

```text
database QueryResourcePool
  -> interactive / analytical / projection class
    -> one QueryOptions hard budget
```

Production options explicitly configure worker counts, reserved interactive
workers, query memory, projection cache, QueryDelta, scratch disk, free-space
reserve, read and scratch rates, and maximum prefetch.

Database Open validates WBM, RocksDB block cache, query runtime, projection
cache, and QueryDelta against the configured total memory budget. No module may
independently assume it owns total memory.

A query budget covers memory, scratch, read/decode/output bytes, CPU, wall
deadline, parallelism, prefetch, interval fragments, graph labels, and retained
output batches.

Interactive execution normally runs inline in `Next` to avoid a context switch.
Analytical work uses a fixed Cedar worker pool; Cedar does not create a thread
per query or infer an unbounded count from host CPUs.

### 15.2 Work classes

| Priority | Work |
| --- | --- |
| P0 | WAL durability, recovery, emergency write-stop flush |
| P1 | interactive reads and bounded interactive delta repair |
| P2 | normal flush, necessary compaction, contiguous delta progress |
| P3 | analytical reads and scratch I/O |
| P4 | projection rebuild, statistics refresh, orphan cleanup |

The scheduler reserves a minimum P2 share while storage debt exists, so reads
cannot starve compaction. It also reserves a bounded P1 share. Bulk work uses
surplus capacity.

When `wal_sync_critical` or emergency maintenance is active, Cedar stops issuing
new analytical prefetch, scratch, and projection I/O. Already issued OS I/O is
not falsely reported as cancelled. Operators yield at page, batch, run,
partition, or frontier boundaries.

Authoritative pages continue through the configured RocksDB cache, with Cedar
controlling request concurrency and read-ahead through the private adapter.
Projection pages use a Cedar cache. Scratch bypasses long-lived caches.

### 15.3 QueryScratch

Scratch is created lazily only for spill-enabled analytical queries:

```text
<db>/query/scratch/<process-instance-id>/<query-id>/*.cscratch
```

Creation requires memory, disk, and free-space reservations. Scratch contains
immutable hash partitions or sort runs with block checksums and optional LZ4.
It is not synced, logged, or listed in a projection manifest. Corruption fails
only the query.

Normal completion, cancellation, and error remove the exact query directory.
After acquiring the database lock, Open removes verified prior-process scratch
directories. It never recursively deletes an unverified path.

## 16. Errors and Lifecycle

### 16.1 Status model

| Failure | Status |
| --- | --- |
| construction/binding | `BindError` or `InvalidArgument` |
| incompatible referenced schema | `SchemaMismatch` |
| unreadable old Snapshot | `SnapshotExpired` |
| explicit cancellation | `QueryCancelled` |
| deadline | new `DeadlineExceeded` |
| any resource dimension | `ResourceExhausted` |
| numeric/valid-time overflow | new `NumericOverflow` |
| scratch corruption | query-scoped `Corruption` |
| authoritative corruption | `Corruption` and `RecoveryRequired` state |
| database closing | `ShutdownInProgress` |

The new query path deletes the obsolete `QueryMemoryLimit` use and reports all
bounded resource exhaustion consistently with a diagnostic reason.

### 16.2 Stream completeness and cancellation

Every returned batch is a Snapshot-correct prefix. The result becomes complete
only at clean end-of-stream. Error or cancellation makes the stream incomplete,
even when earlier batches were consumed.

Cursor states are Running, CleanEnd, Cancelled, or Failed. Terminal calls are
idempotent. `Cancel` sets a token without waiting for OS I/O. `Close` stops new
work, waits for bounded submitted tasks, releases the Snapshot, and cleans
scratch. The destructor performs cancellation and best-effort cleanup.

Returned batches may outlive the Cursor but own only their buffers/page leases,
not a RocksDB Snapshot.

An embedded-engine cache handle never crosses the public QueryBatch interface.
Authoritative values are decoded or copied into query-owned buffers. A
zero-copy projection buffer is permitted only through a reference-counted
Cedar projection-cache lease whose owner can safely outlive Database Close.

### 16.3 Projection corruption

A bad projection checksum removes that region from the current physical plan,
uses authoritative fallback within the query budget, quarantines the derived
file after pinned readers leave, and schedules rebuild. It never enters
database recovery-required state. Canonical corruption does.

### 16.4 Close

Database Close:

1. rejects new transaction, Snapshot, prepare, and execute admission;
2. cancels queued/running queries and stops new projection/statistics work;
3. waits for query tasks to release engine Snapshots;
4. drains accepted commits;
5. stops QueryDelta, query workers, and projection builders;
6. stops Cedar maintenance lanes;
7. attempts current-instance scratch cleanup;
8. closes RocksDB last.

Prepared queries and cursors may outlive the Database object but cannot execute
or read database state after Close. Already owned batch data remains readable.

### 16.5 Vacuum

Queries pin their normal RocksDB/Cedar Snapshot. `Vacuum(B)` returns
`SnapshotPinned` if any active query has `snapshot_seq < B`; it does not cancel
the query and never trims valid time.

After Vacuum publishes oldest-readable `B`, a projection with base below `B`
is retired for new readers. Existing pinned readers finish. Cedar builds a new
generation from a Snapshot at or above `B`; new queries use authoritative facts
until it is ready. This prevents restart from requiring exact old versions that
Vacuum may have deleted.

### 16.6 Open and crash recovery

Open performs RocksDB WAL recovery and Cedar watermark validation first. After
the exclusive lock is held, it cleans old scratch and unpublished temporary
segments, validates `PROJECTION-CURRENT`, validates database/schema/base
identity, and reconstructs QueryDelta through visible sequence.

If CURRENT is corrupt, Cedar disables projections rather than guessing a
manifest. Until Delta coverage is contiguous, queries use authoritative facts.
Cursor, PreparedQuery, and query results are never recovered.

## 17. Statistics, Explain, and Observability

Statistics are derived and generation-bound. Exact metadata includes rows,
pages, bytes, entity/valid-time range, interval count, and edge count. Bounded
approximate data includes HLL distinct counts, up to 128 equi-depth histogram
buckets, up to 64 top values, and fanout/interval-length quantiles.

QueryDelta reports mutation, dirty-chain, new-edge, boundary, and lag counts.
It does not pretend to update corrected-state cardinalities exactly. High dirty
ratios widen cost uncertainty and eventually disable selectivity assumptions.

Statistics are built with projection generations and refreshed through Cedar
P4 work or explicit `RefreshQueryStatistics`; the write path does not maintain
them.

PreparedQuery supports structured logical and Snapshot-bound physical Explain.
Physical Explain reports operators, lanes, estimates, selected projection and
base, delta range, fallback holes, pushed predicates/columns/time, join/path
algorithm, spill permission, and estimate confidence.

Optional `capture_profile` collects per-operator actual rows, batches, CPU/wall
and queue time, first result, bytes/pages/cache, delta repair, fragments, spill,
frontier/labels, and terminal completeness. Default counters are batch-level;
there is no per-row timer.

Global bounded-label metrics cover query admission, terminal states, latency,
projection hit/fallback, bytes, memory/scratch, worker/I/O waits, Delta lag,
projection health, adjacency pruning, and graph frontier dominance. Query text,
parameters, QueryId, and PropertyId are not global metric labels. Values are not
logged unless an explicitly redacted trace is enabled.

Storage-file inspection recognizes `.cmanifest`, `.cstate`, `.cadj`, `.cprop`,
`.cstats`, and `.cscratch` and labels each authoritative, derived, or temporary.
It reports generation, base sequence, coverage, size, and checksum without
confusing these files with RocksDB `.sst` files.

No query hot path polls generic RocksDB properties or enables RocksDB periodic
statistics threads.

## 18. Correctness and Debug Acceptance

### 18.1 Reference differential testing

A deliberately simple test-only bitemporal evaluator will enumerate facts on
small graphs. Generated histories include same-boundary corrections, deletes,
disjoint intervals, Missing, schema epochs, self-loops, parallel edges, cycles,
hubs, cross-partition identities, duration, and overflow.

Every applicable query is compared across:

```text
reference evaluator
canonical-only
projection at base
projection plus delta
partial coverage fallback
restart-rebuilt delta
interactive lane
analytical lane
hybrid lane
```

Small path/journey graphs use exhaustive enumeration to verify optimum and
witness tie-breaking.

### 18.2 Small-threshold bug amplification

The Debug profile uses the production code path with reduced capacities:

```text
MemTable                 64 KiB
projection segment       64 KiB
projection page           4 KiB
QueryDelta soft/hard     64/128 KiB
delta lag                 8/32 commits
query memory             32 KiB
scratch run              16 KiB
manifest generation      every 16 commits
```

This repeatedly exercises flush, compaction, delta rollover, projection
publication, spill, coverage switch, Vacuum, and orphan cleanup without a
test-only algorithm.

### 18.3 Crash and corruption

Fault points cover segment write/sync/rename, manifest publication, CURRENT
replacement, delta enqueue and repair, scratch, cancellation, Close, and
Vacuum. Real subprocess kill/reopen tests run concurrent commits, queries, and
projection builds; generation switching; Close races; Vacuum pins; projection
bit flips/deletion/truncation; authoritative corruption; delta gaps; and bases
retired by Vacuum.

### 18.4 Sanitizers

- ASAN and UBSAN run the complete focused query/projection/scratch suite.
- TSAN stresses commit/delta publication, generation switching, cancellation,
  Close, and Vacuum.
- LeakSanitizer covers cursors, batch leases, scratch, and retired generations.
- Debug assertions prove coverage continuity, sequence continuity, budget
  non-underflow, and pin lifetime.

## 19. Release Performance and Space Acceptance

Release performance runs only after Debug, differential, crash, and sanitizer
gates pass.

### 19.1 Write sweep

```text
facts per transaction: 1,4,8,16,32,64,128,256,512,1024,2048
writers:               1,2,8,32,64
```

Reports include transactions/s, facts/s, MiB/s, group fill, WAL-sync and
end-to-end p50/p95/p99, write/space amplification, and projection lag. The
benchmark must locate the facts-per-transaction throughput turning point rather
than assuming 64 is optimal.

### 19.2 Read sweep

The matrix covers StateAt, history, events, changes, all adjacency directions
at degrees 1/10/100/1K/10K, property selectivities 0.1/1/10/100 percent,
temporal aggregate, interval join, k-hop, coexisting shortest path, all journey
objectives, base/short-delta/long-delta/partial coverage, cold/warm cache, and
1/8/32 readers.

Reports include QPS, facts/edges per second, MiB/s, first-result and
p50/p95/p99, CPU, physical pages, and bytes. Cold, warm, short peak, and
sustained results are separate.

### 19.3 Hard gates

- an idle query subsystem reduces Kernel write facts/s by no more than 3% and
  increases WAL-sync p99 by no more than 5%; five repeated runs establish the
  comparison;
- active projection building reduces sustained write throughput by no more than
  10% and increases p99 by no more than 15%, with no uncontrolled write stop;
- StateAt never performs a full scan;
- adjacency physical work is `O(log N + candidate/returned degree)`;
- property scans read only required property/pages;
- default profiling overhead is no more than 2%;
- a 30-minute mixed sustained run has bounded projection lag, no stale result,
  no unexplained fallback, and no resource leak;
- every performance database is closed, reopened, and result-verified.

Derived projection bytes target at most 1.0 times authoritative live bytes on
the standard dataset and hard-fail above 1.5 times. Statistics hard-fail above
2% of total projection size. Reports separate authoritative, adjacency,
property, statistics, scratch peak, obsolete, and total bytes.

Every artifact records commit, compiler, host, data set, options, cache state,
projection/base/delta state, and CSV/JSON raw results. The first Release run is
a calibration result, not a capability claim. Only sustained results that pass
the complete gates may be reported as Cedar capability.

The implementation deletes Lean and obsolete query benchmarks. The only
control is the same Kernel implementation with projection work paused; it is
not an alternate code path.

## 20. Implementation Boundary

The implementation will replace, not layer over, any obsolete query code. It
will preserve current benchmark work unrelated to the query design and will
not change the authoritative storage format unless a separately approved
design proves that necessary.

Implementation must proceed from a separate detailed plan, test first, with
review checkpoints after public types, temporal reference semantics,
projection/delta correctness, planner/runtime, graph algorithms, recovery, and
Release acceptance.
