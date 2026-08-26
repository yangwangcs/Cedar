# Cedar User Guide

This guide describes the public Cedar database model, the embedded C++ API,
the T-Cypher query language, and the bundled Bolt server. It is written for
the current clean-break database format on `main`.

## 1. What Cedar Stores

Cedar is a bitemporal graph database. A vertex or edge is identified by a
stable reference; state and properties are independent facts. Every fact has:

```text
(part_id, fact family, object id, property id, valid_from, operation, commit_seq)
```

`valid_from` is business time: when the fact becomes effective in the modeled
world. `commit_seq` is system time: when Cedar durably learned the fact. A
snapshot fixes a system-time cutoff, then reconstructs valid-time intervals
from all visible corrections.

The durable authority is one CedarParquet columnar fact store managed through
the Cedar storage boundary. Temporal and adjacency structures are derived
projections. If a projection is absent, stale, corrupt, or incomplete, Cedar
falls back to canonical facts rather than returning silently stale data.

`PartID` is part of every public vertex and edge reference. `PartID{0}` is a
real partition in the embedded API. A query can use all partitions or an exact
partition; it must never rely on zero meaning wildcard.

## 2. Build Cedar

The repository includes the pinned LZ4 and Zstandard sources used by the
columnar backend. A C++20 compiler and CMake are required.

```bash
cmake -S . -B build -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
cmake --build build -j1
ctest --test-dir build -j1 --output-on-failure
```

Use `-j1` on memory-constrained hosts. The public library target is
`Cedar::cedar`; RocksDB and engine-specific headers are not part of the
installed consumer surface.

## 3. Embedded C++ API

### 3.1 Open a database

```cpp
#include "cedar/database.h"

cedar::DatabaseOptions options;
options.path = "/var/lib/cedar/social";
auto opened = cedar::Database::Open(options);
if (!opened.ok()) {
  std::cerr << opened.status().ToString() << "\n";
  return 1;
}
std::unique_ptr<cedar::Database> database =
    std::move(opened).ConsumeValueOrDie();
```

The directory must use the current Cedar format. This is a clean-break format:
legacy directories are rejected without mutation and there is no in-place
migration command. Export old data with a binary that understands its old
format, then write it into a new Cedar directory through transactions.

### 3.2 Register typed properties

Property names used by T-Cypher are resolved through a schema catalog. Embedded
applications register the definitions before preparing queries.

```cpp
auto status = database->RegisterProperty(cedar::PropertyDefinition{
    cedar::PropertyId{7},             // stable property id
    0,                                // schema epoch
    "score",                         // T-Cypher name
    cedar::PropertyEntityKind::kVertex,
    cedar::PhysicalType::kInt64,
    4096});                           // blob threshold
if (!status.ok()) return status;
```

The property kind and physical type are part of query binding. A vertex
property and an edge property with the same name are distinct schema entries.
Unknown names and type mismatches fail during preparation, before a query
reads data.

### 3.3 Write a transaction

The low-level transaction API is useful when the application already has
typed references. IDs are allocated by the database, not by the caller.

```cpp
auto tx_result = database->BeginTransaction();
if (!tx_result.ok()) return tx_result.status();
auto tx = std::move(tx_result).ConsumeValueOrDie();

auto vertex_id = database->AllocateVertexId();
if (!vertex_id.ok()) return vertex_id.status();
cedar::VertexRef user{{0}, vertex_id.ValueOrDie()};

if (const auto s = tx->Assert(
        cedar::EntityFact::Vertex(user), cedar::ValidTime{20240101});
    !s.ok()) return s;
if (const auto s = tx->Set(
        cedar::PropertyFact::Vertex(user, cedar::PropertyId{7}),
        cedar::ValidTime{20240101}, cedar::Value::Int64(42));
    !s.ok()) return s;

auto committed = tx->Commit();
if (!committed.ok()) return committed.status();
if (committed.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted) {
  return committed.ValueOrDie().status;
}
```

`Commit()` returns only after durable acceptance. `CommitAsync()` provides a
`CommitHandle`; call `Wait()` before treating the transaction as durable. A
transaction can also `Retract` an entity, `Unset` a property, `Scan` facts, or
`BeginReadSnapshot(true)` to read its staged mutations.

### 3.4 Read a snapshot directly

```cpp
auto snapshot_result = database->BeginSnapshot();
if (!snapshot_result.ok()) return snapshot_result.status();
auto snapshot = std::move(snapshot_result).ConsumeValueOrDie();

auto present = snapshot.Exists(
    cedar::EntityFact::Vertex(user), cedar::ValidTime{20240101});
if (!present.ok()) return present.status();
if (present.ValueOrDie()) {
  std::cout << "visible\n";
}
```

Snapshots are pinned until destroyed. They are database-owned and cannot be
used with a different database. `Snapshot::commit_seq()` reports the system
time cutoff used for reconstruction.

## 4. T-Cypher Language

T-Cypher is Cedar's typed, bounded temporal graph language. Keywords are
case-insensitive; identifiers are returned as written. The supported shape is:

```text
[USE graph]
[FOR VALID_TIME AS OF n | FOR VALID_TIME BETWEEN from AND to]
[FOR SYSTEM_TIME AS OF n | FOR SYSTEM_TIME BETWEEN from AND to]
[CHANGES]
(MATCH graph pattern RETURN projection)
or
(CREATE graph pattern [SET ...] [DELETE ...])
```

The two temporal scopes are independent:

- `VALID_TIME` chooses the business-time state or interval.
- `SYSTEM_TIME` chooses the durable commit view.

`VALID_TIME BETWEEN from AND to` is a bounded half-open interval `[from, to)`.
`SYSTEM_TIME BETWEEN from AND to` is an inclusive commit range. The system-time
upper bound is capped to the caller's current snapshot with one snapshot; it
does not create one snapshot per commit.

### 4.1 Match a vertex

```cypher
FOR VALID_TIME AS OF 20240101
MATCH (v)
RETURN v
```

The variable `v` is a `VertexRef` column. Without a valid-time prefix, Cedar
uses valid time `0`, which is normally useful only for facts explicitly
asserted at that time. Prefer an explicit valid-time scope in application
queries.

### 4.2 Select a valid-time history

```cypher
FOR VALID_TIME BETWEEN 20200101 AND 20250101
MATCH (v)
RETURN v, valid_from(v)
```

The result contains the state boundaries intersecting the requested interval.
`valid_from(v)` is typed `ValidTime`; it is not a string conversion.

### 4.3 Inspect system-time history

```cypher
FOR VALID_TIME AS OF 20240101
FOR SYSTEM_TIME AS OF 120
MATCH (v)
RETURN v, commit_seq(v)
```

`commit_seq(v)` exposes the winning durable commit for the returned fact. A
range is useful for auditing corrections:

```cypher
FOR VALID_TIME AS OF 20240101
FOR SYSTEM_TIME BETWEEN 100 AND 120
MATCH (v)
RETURN v, valid_from(v), commit_seq(v)
```

Range state reduction retains predecessor events below the lower system-time
bound before selecting winners. This is required for correct temporal
reconstruction. Range reads use canonical fallback when a derived projection
cannot prove complete coverage.

### 4.4 Expand a typed relationship

```cypher
FOR VALID_TIME AS OF 20240101
MATCH (a)-[e:KNOWS]->(b)
RETURN a, e, b
```

Relationship types are stable hashed identifiers. The same type name must be
used when creating and reading an edge. Direction is currently outgoing in
T-Cypher path syntax. The embedded Query API additionally exposes incoming and
both-direction expansion for applications that build logical queries directly.

### 4.5 Use bounded variable-length paths

```cypher
FOR VALID_TIME AS OF 20240101
MATCH (a)-[e:KNOWS*1..3]->(b)
TRAIL
RETURN a, e, b
```

The maximum hop count is bounded by the binder (64 by default and normally
lowered by `QueryOptions::budget.max_hops`). `TRAIL` requests trail semantics
for the path expansion. Unbounded forms such as `[*]` are rejected instead of
turning into an unbounded scan.

### 4.6 Chain connected path segments

```cypher
FOR VALID_TIME AS OF 20240101
MATCH (a)-[e:KNOWS]->(b)-[f:LIKES*1..2]->(c)
RETURN a, e, b, f, c, valid_from(f)
```

The destination of one segment must be the source of the next segment. Cedar
lowers this to a left-deep sequence of existing expansion operators and keeps
all vertex and edge slots available for later property and metadata binding.
Disconnected products are deliberately rejected:

```cypher
MATCH (a), (b) RETURN a, b   // rejected: no product operator in the public algebra
```

### 4.7 Properties and metadata

The current T-Cypher surface projects variables and the metadata functions
`valid_from(variable)` and `commit_seq(variable)`. It also accepts typed scalar
`WHERE` predicates for registered properties (`=`, `<`, `<=`, `>`, `>=`) and
combines them with `AND`. These expressions lower to Cedar's existing property
binding and filter operators; T-Cypher does not maintain a second executor.
Bind a property in C++ when you need a typed predicate or property output
outside the supported T-Cypher grammar:

```cpp
auto vertex_slot = cedar::Slot<cedar::VertexRef>::Named("v");
auto query = cedar::Query::Vertices(
    vertex_slot, cedar::At{cedar::ValidTime{20240101}});
auto score = cedar::Slot<int64_t>::Optional("score");
auto with_score = query.ValueOrDie().BindVertexProperty(
    vertex_slot, cedar::PropertyId{7}, score);
```

The public Query API supports typed `Where`, `Limit`, `Expand`, k-hop and
analytical operators. See the API headers for the exact expression builders.

### 4.8 Read changes

`CHANGES` requires a bounded valid-time interval:

```cypher
CHANGES
FOR VALID_TIME BETWEEN 20200101 AND 20250101
MATCH (v)
RETURN v, valid_from(v), commit_seq(v)
```

It is an analytical history operation. `CHANGES MATCH (v) RETURN v` is rejected
because an unbounded change scan has no resource boundary. `CHANGES` does not
silently reinterpret system time as valid time.

### 4.9 Create graph facts

`CREATE` allocates new IDs and asserts the vertices and edges in one write
transaction. A write uses one valid-time point, supplied by `FOR VALID_TIME AS
OF n` or `FOR VALID_TIME BETWEEN n AND n` (the latter is still rejected as a
range write; use `AS OF` for writes).

```cypher
FOR VALID_TIME AS OF 20240101
CREATE (a)-[e:KNOWS]->(b)
```

The edge identity records source, target, home partition, and relationship
type. `CREATE` paths must be one hop; variable-length `CREATE` is rejected.

### 4.10 Set and delete in a create statement

Writes require named parameters. The targets must be entities created by the
same statement:

```cypher
FOR VALID_TIME AS OF 20240101
CREATE (a)-[e:KNOWS]->(b)
SET a.score = $score
SET e.weight = $weight
DELETE b
```

Historical system-time writes and valid-time range writes are rejected. `SET`
and `DELETE` are staged in the same transaction as the created graph.

## 5. Prepare, Bind, and Execute T-Cypher in C++

`CypherSession` owns a bounded prepared-query cache and a schema catalog. A
prepared object intentionally does not retain the original query text.

```cpp
#include "cedar/cypher/session.h"

cedar::cypher::SchemaCatalog catalog;
catalog.Add({cedar::PropertyId{7}, 0, "score",
             cedar::PropertyEntityKind::kVertex,
             cedar::PhysicalType::kInt64, 4096});

cedar::cypher::BinderOptions binder_options;
binder_options.graph = "social";
binder_options.part_id = cedar::PartId{0};
cedar::cypher::CypherSession session(*database, std::move(catalog),
                                     binder_options);

auto prepared = session.Prepare(
    "FOR VALID_TIME AS OF 20240101 "
    "MATCH (v) RETURN v, valid_from(v)");
if (!prepared.ok()) return prepared.status();

cedar::cypher::CypherRequest request;
auto cursor = session.Execute(prepared.ValueOrDie(), request);
if (!cursor.ok()) return cursor.status();

while (true) {
  auto next = cursor.ValueOrDie().Next();
  if (!next.ok()) return next.status();
  if (!next.ValueOrDie().has_value()) break;
  const cedar::QueryBatch& batch = *next.ValueOrDie();
  for (size_t row = 0; row < batch.row_count(); ++row) {
    // Access typed columns with the corresponding slots in a logical Query.
  }
}
```

For a statement with parameters, `prepared.ValueOrDie().bound_statement()
.parameters` gives the stable `ParameterId` assigned by the binder. Bind the
exact `QueryType`:

```cpp
const auto& parameters = prepared.ValueOrDie().bound_statement().parameters;
for (const auto& parameter : parameters) {
  if (parameter.name == "score") {
    request.bindings.Bind(parameter.id, cedar::QueryType::kInt64,
                          cedar::Value::Int64(42));
  }
}
```

Missing, duplicate, or wrongly typed bindings fail before execution. The
request may also supply `valid_time`, `part_id`, and `graph`, but those values
must agree with the prepared statement; they cannot silently change its
fingerprint. Prepared system-time range endpoints must match exactly.

### 5.1 Execute inside a transaction

The transaction overload exposes read-your-writes for staged vertices, edge
identities, `SET`, and `UNSET` property changes. Staged rows are ephemeral and
never enter WAL, projections, recovery, or MemTables until commit.

```cpp
auto tx = database->BeginTransaction().ConsumeValueOrDie();
auto write = session.Prepare(
    "FOR VALID_TIME AS OF 20240101 CREATE (v) SET v.score = $score");
// Bind $score and stage/commit the write through the session or low-level API.

auto read = session.Prepare("FOR VALID_TIME AS OF 20240101 MATCH (v) RETURN v");
auto cursor = session.Execute(read.ValueOrDie(), *tx, {});
// The cursor sees the transaction's staged graph.
```

Historical system-time queries deliberately exclude staged rows because the
rows do not have durable commit sequences yet.

### 5.2 Explain a prepared query

```cpp
auto explanation = session.Explain(prepared.ValueOrDie());
if (!explanation.ok()) return explanation.status();
std::cout << explanation.ValueOrDie().logical << "\n"
          << explanation.ValueOrDie().physical << "\n"
          << "source=" << explanation.ValueOrDie().source << "\n";
```

Explain reports the selected access profile, execution mode, projection or
canonical source, and whether a system-time range was clamped to the caller's
snapshot. A range query reports canonical fallback unless complete derived
coverage is proven.

## 6. Query Results and Budgets

Queries return a streaming `QueryCursor`. `Next()` returns an optional
`QueryBatch`; an empty optional is clean end-of-stream. Every batch owns its
decoded column buffers. A cursor must be closed or consumed before its snapshot
can be released.

`QueryOptions` provides explicit resource controls:

```cpp
cedar::QueryOptions options;
options.mode = cedar::QueryExecutionMode::kAnalytical;
options.budget.max_hops = 3;
options.budget.output_rows = 100000;
options.budget.read_bytes = 64ULL << 20;
options.budget.memory_bytes = 128ULL << 20;
options.capture_profile = true;
```

Use `cursor.profile()` for source, rows, decoded/physical bytes, page pruning,
wait time, and fixed-cardinality complexity counters. A query that exceeds a
budget returns a resource error; it is not silently broadened or partially
returned as a successful result. Call `Cancel()` from another thread for
cooperative cancellation.

## 7. Run the Cedar Server

`cedar-server` exposes a small Bolt-compatible endpoint on port 7687 by
default. It accepts `HELLO`, `RUN`, `PULL`, `BEGIN`, `COMMIT`, `ROLLBACK`,
`RESET`, and `GOODBYE`. The server loads a schema manifest so query clients do
not need to compile a C++ catalog.

```bash
./build/cedar-server \
  --db /var/lib/cedar/social \
  --bind 127.0.0.1 \
  --port 7687 \
  --schema /etc/cedar/social.schema \
  --auth-token "$CEDAR_AUTH_TOKEN"
```

Supported options are `--db`, `--bind`, `--port`, `--lock`, `--pid`,
`--schema`, `--auth-token`, `--graph`, and `--part-id`.

The embedded listener is not a TLS terminator. For production, bind it to a
private interface and put it behind an mTLS/TLS proxy or service mesh. The
auth token protects application admission in the Bolt `HELLO` message but does
not encrypt transport. With no token, local development is anonymous.

### 7.1 Schema manifest

The manifest is line-oriented and rejects unknown keys, duplicate fields, and
invalid physical types:

```text
graph=social
part_id=0
property=7,vertex,int64,0,score
property=9,edge,float64,0,weight
```

Property fields are `property_id,entity_kind,physical_type,schema_epoch,name`.
Supported physical types are `bool`, `int32`, `int64`, `float32`, `float64`,
`timestamp64`, `string`, and `binary`. The `graph` line is required. A
configured `--graph` or `--part-id` overrides the manifest value; otherwise
the manifest supplies the binder defaults.

### 7.2 Transaction flow over Bolt

The server maps one Bolt connection to one session. A typical sequence is:

```text
HELLO {"principal":"application","credentials":"..."}
RUN  "FOR VALID_TIME AS OF 20240101 MATCH (v) RETURN v"
PULL {"n": 128}
BEGIN
RUN  "FOR VALID_TIME AS OF 20240101 CREATE (a)-[e:KNOWS]->(b)"
COMMIT
RESET
GOODBYE
```

`RUN` with a write stages it in the open transaction when `BEGIN` was sent;
without an open transaction it executes and commits the write immediately.
`PULL` controls result batches and is bounded by the server's configured frame
and record limits. The transaction overlay gives the same connection
read-your-writes behavior as the embedded session API.

## 8. Partition and Graph Routing

For a single-part deployment, configure `part_id=0`. For a distributed
deployment, assign an explicit `PartID` to each server or embedded session and
include it in the canonical vertex/edge references. A query prepared with an
exact partition cannot be executed with a different request partition. A
session configured without an explicit partition can read all partitions where
the query plan supports it.

`USE social` selects a graph name. The graph is a logical namespace carried in
the prepared fingerprint; a request that names a different graph is rejected.

## 9. Supported Boundaries and Common Errors

The following are intentional errors rather than silent fallbacks:

- reversed or unbounded temporal ranges;
- `CHANGES` without a bounded valid-time interval;
- system-time writes and valid-time range writes;
- disconnected comma-separated graph products;
- unbounded paths or variable-length `CREATE`;
- unknown property names or property kind/type mismatches;
- historical reads that attempt to expose uncommitted transaction rows;
- request graph, partition, parameter, or temporal scope mismatches; and
- query, output, read, CPU, or scratch budget exhaustion.

Legacy `AT` and `DIFF` temporal syntax is rejected. T-Cypher is not a general
openCypher implementation: arbitrary `WHERE`, `WITH`, aggregation syntax,
subqueries, optional matches, and disconnected products are outside this
public language surface. Use the typed embedded Query API for capabilities that
are not represented by the compact parser.

## 10. Recovery and Operations

Only one Cedar process may own a database directory at a time. The server
creates a lock file and optional PID file. Stop it with `SIGTERM` or `SIGINT`
and wait for `Stop()`/process exit before opening the directory elsewhere.

Back up the database as a consistent directory snapshot only when no writer is
active, or use the operational backup procedure of the surrounding deployment.
Do not edit CedarParquet, projection, WAL, MANIFEST, or CURRENT files by hand.
Projection files are rebuildable, but authoritative fact corruption requires
recovery rather than a projection refresh.

For correctness and performance interpretation, consult:

- [query acceptance evidence](query-acceptance.md);
- [development-host performance results](query-performance.md); and
- [the public documentation index](README.md).
