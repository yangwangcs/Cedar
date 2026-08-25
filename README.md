# Cedar

Cedar is a bitemporal graph database kernel built on an LSM-backed columnar
storage architecture.

It combines a single embedded LSM engine for durable writes, WAL recovery,
MemTables, VersionSet, and MANIFEST management with Cedar's authoritative
columnar bitemporal facts. Cedar is designed for applications that need graph
relationships, temporal history, transactional durability, and analytical
scans without running a separate database service.

## Core Model

Cedar is not a traditional row-only LSM database.

- The embedded LSM engine provides ordering, durability, recovery, and file
  lifecycle management.
- CedarParquet stores the authoritative bitemporal facts in columnar form.
- Temporal and adjacency indexes are rebuildable derived projections.
- The public API exposes Cedar abstractions and does not expose engine types.

```text
Cedar public API
      |
      v
Bitemporal transaction and snapshot kernel
      |
      v
Cedar storage and query runtime
      |
      +--> authoritative CedarParquet facts
      +--> temporal projections
      +--> adjacency projections
      |
      v
Embedded Cedar-owned LSM engine
      |
      +--> one WAL
      +--> recovery
      +--> MemTables
      +--> VersionSet and MANIFEST
      +--> native flush and compaction
```

## Features

- Bitemporal graph facts with valid time and system time
- Explicit transactions with synchronous durable commit
- One embedded WAL and one recovery path
- Snapshot-consistent temporal reads
- Point-in-time, history, event, and change queries
- Typed property binding and typed result batches
- Temporal expansion and k-hop traversal
- Temporal shortest path and arrival-time queries
- Temporal interval joins and aggregates
- Columnar analytical scans
- Rebuildable temporal and adjacency projections
- Canonical fallback when a projection is stale or incomplete
- Durable asynchronous commit with bounded group commit
- Clean-break, versioned database format

## Project Status and Paper

### 中文

Cedar 当前主分支是在 Cedar 0.1 原型系统基础上持续演进形成的较为完善的
时态属性图数据库系统。当前版本保留了原型系统关于追加式时态事件、LSM
存储和列式布局的核心设计，并进一步完善了双时态查询、事务、快照、
T-Cypher、列式事实存储、投影回退、嵌入式 API 和 Bolt 服务能力。Cedar
仍在持续开发和优化中。

我们的论文：

> Yang Wang, Xuelian Lin, Jinghe Song, Jianyong Zhu, Yu Zhao, and Shuai Ma.
> **Cedar: A Columnar LSM-Engine for Temporal Property Graphs.**

该论文已被 VLDB 2026 Demo Track 录用。

### English

The current Cedar `main` branch is a substantially more complete temporal
property graph database system developed from the Cedar 0.1 prototype. It
preserves the prototype's core ideas of append-oriented temporal events,
LSM-based storage, and columnar organization, while extending the system with
bitemporal queries, transactions, snapshots, T-Cypher, authoritative columnar
facts, projection fallback, embedded APIs, and a Bolt server. Cedar is under
active development and continuous optimization.

Our paper is:

> Yang Wang, Xuelian Lin, Jinghe Song, Jianyong Zhu, Yu Zhao, and Shuai Ma.
> **Cedar: A Columnar LSM-Engine for Temporal Property Graphs.**

The paper has been accepted to the VLDB 2026 Demo Track.

## Bitemporal Fact Model

Cedar stores graph data as immutable, versioned facts. A fact is an
independently versioned state assertion, not a mutable row or an entire
vertex/edge object.

```text
(fact reference, valid_from, operation, value, commit_seq)
```

### Fact reference

A fact reference identifies the logical object being changed:

- vertex state;
- vertex property;
- edge identity;
- edge state; or
- edge property.

Examples:

```text
VertexState(PartID=0, VertexID=42)
VertexProperty(PartID=0, VertexID=42, PropertyID=7)
EdgeState(PartID=0, EdgeID=100)
EdgeProperty(PartID=0, EdgeID=100, PropertyID=9)
```

An edge identity also records its source vertex, target vertex, and edge type.

### Valid time

`valid_from` is the business-effective time at which a fact starts to apply.
Cedar stores event boundaries rather than repeatedly rewriting complete
intervals:

```text
PUT    Vertex(42) at valid time 2020
PUT    Vertex(42) at valid time 2022
DELETE Vertex(42) at valid time 2024
```

These events describe the effective state:

```text
[2020, 2022)  present
[2022, 2024)  present
[2024, +inf)  absent
```

The storage layer may coalesce adjacent equivalent states, but the corrected
boundary stream is retained so later historical corrections can be resolved
correctly.

### System time

`commit_seq` is the system-time version assigned by the durable transaction
commit. A query snapshot fixes a system-time cutoff. For each fact boundary,
Cedar uses the greatest `commit_seq` visible to that snapshot and reconstructs
the valid-time state from those visible events.

```text
valid time:   when the fact was effective in the modeled world
system time:  when Cedar knew and durably committed that fact
```

For example, if Cedar commits:

| Commit | Fact | Valid time | Operation |
| --- | --- | ---: | --- |
| 101 | `Vertex(42)` | 2020 | `PUT` |
| 120 | `Vertex(42)` | 2022 | `PUT` |
| 130 | `Vertex(42)` | 2024 | `DELETE` |

a snapshot at `commit_seq = 130` sees `Vertex(42)` as present during
`[2020, 2024)` and absent during `[2024, +inf)`. A snapshot at
`commit_seq = 120` does not see the later deletion and sees the vertex as
present during `[2020, +inf)`.

## Architecture and Ownership

Cedar preserves these ownership boundaries:

- Cedar uses one embedded WAL.
- The embedded engine owns WAL framing and recovery.
- The engine owns sequence allocation and MemTable lifecycle.
- The engine owns VersionSet, MANIFEST, flush, compaction, and obsolete-file
  reclamation.
- CedarParquet facts are the authoritative logical columnar representation.
- Temporal and adjacency indexes are rebuildable projections.
- Cedar does not create a second WAL or a second recovery protocol.
- Cedar owns bitemporal graph semantics, query planning, and public API
  behavior.

The LSM engine provides the durable storage lifecycle. Cedar provides the
bitemporal graph model and query semantics.

## Quick Start

### Requirements

- C++20 compiler
- CMake
- Ninja or Make

Pinned LZ4 and Zstandard sources are included under `third_party/` and are
built statically by default. Host codec packages are not required.

### Build and test

```bash
cmake -S . -B build -DBUILD_TESTS=ON
cmake --build build -j1
ctest --test-dir build -j1 --output-on-failure
```

The installed consumer target is `Cedar::cedar`. Engine headers and
engine-specific consumer targets are not installed.

### Cedar server security boundary

`cedar-server --auth-token TOKEN --db PATH` enables constant-time application
authentication in the Bolt `HELLO` message. Cedar's embedded listener is not a
TLS terminator: production deployments must put it behind an mTLS/TLS proxy or
service mesh. The token protects application admission but does not encrypt
the transport. With no token, the local-development Bolt mode remains
anonymous.

## Basic Usage

```cpp
#include "cedar/database.h"

auto opened = cedar::Database::Open({.path = "/data/cedar"});
if (!opened.ok()) return opened.status();

auto database = std::move(opened).ConsumeValueOrDie();
const cedar::VertexRef vertex{{0}, {1}};

auto begun = database->BeginTransaction();
if (!begun.ok()) return begun.status();

auto transaction = std::move(begun).ConsumeValueOrDie();
transaction->Assert(
    cedar::EntityFact::Vertex(vertex), cedar::ValidTime{1000});

auto committed = transaction->Commit();
if (!committed.ok()) return committed.status();
if (committed.ValueOrDie().outcome != cedar::CommitOutcome::kCommitted) {
  return committed.ValueOrDie().status;
}
```

A transaction becomes visible only after Cedar's durable commit boundary has
completed.

For the complete embedded API, T-Cypher grammar and examples, parameter
binding, Bolt server, schema manifests, transaction flow, and operational
limits, see the [Cedar User Guide](docs/user-guide.md).

## Snapshot Reads

A snapshot binds a logical commit sequence and an engine snapshot. Temporal
resolution therefore considers both the requested valid time and the
snapshot's system time.

```cpp
auto begun_snapshot = database->BeginSnapshot();
if (!begun_snapshot.ok()) return begun_snapshot.status();

auto snapshot = std::move(begun_snapshot).ConsumeValueOrDie();
auto exists = snapshot.Exists(
    cedar::EntityFact::Vertex(vertex), cedar::ValidTime{1000});
if (!exists.ok()) return exists.status();
```

Snapshots remain valid until released or destroyed. Cedar refuses database
close while caller-owned snapshots are still pinned.

## Temporal Queries

Queries are prepared once and executed against a Cedar snapshot.

```cpp
#include "cedar/query.h"

auto vertex_slot =
    cedar::Slot<cedar::VertexRef>::Named("vertex");
auto query = cedar::Query::Vertices(
    vertex_slot, cedar::At{cedar::ValidTime{1000}});

auto prepared = database->PrepareQuery(query.ValueOrDie());
if (!prepared.ok()) return prepared.status();

auto query_snapshot = database->BeginSnapshot();
if (!query_snapshot.ok()) return query_snapshot.status();

auto cursor = prepared.ValueOrDie().Execute(
    std::move(query_snapshot).ConsumeValueOrDie(),
    cedar::Bindings{}, cedar::QueryOptions{});
if (!cursor.ok()) return cursor.status();

auto batch = cursor.ValueOrDie().Next();
```

The query layer supports:

- `At` point-in-time reads;
- `History`, `Events`, and `Changes`;
- typed property filtering and binding;
- temporal expansion;
- k-hop traversal;
- coexisting shortest paths;
- earliest-arrival and latest-departure queries;
- fastest-duration queries;
- interval joins; and
- temporal aggregates.

Typed slots preserve declared physical types through planning and result
decoding.

## Durable Asynchronous Commit

`Transaction::Commit()` is the synchronous API. `CommitAsync()` returns only
after the complete transaction reaches Cedar's durable acceptance boundary.

```cpp
auto accepted = transaction->CommitAsync();
if (!accepted.ok()) return accepted.status();

auto result = accepted.ValueOrDie().Wait().ConsumeValueOrDie();
if (result.outcome != cedar::CommitOutcome::kCommitted) {
  return result.status;
}
```

Asynchronous commit uses bounded group commit. It reduces caller wait time by
sharing durable WAL operations, but it does not remove synchronous persistence.

## Database Format

Cedar uses a versioned clean-break database format. On open, Cedar validates
the format record, expected column families, visible watermarks, and sequence
metadata.

Legacy Cedar directories are rejected without modification. There is no online
migration path. Existing data must be exported by a binary that understands
its old format and imported into a fresh Cedar directory through the public
transaction API.

## Performance and Benchmarks

The supported benchmark is `cedar_kernel_bench`.

```bash
cmake -S . -B build-bench \
  -DBUILD_BENCHMARKS=ON \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-bench --target cedar_kernel_bench -j1

./build-bench/cedar_kernel_bench \
  --path /tmp/cedar-kernel-bench \
  --workload mixed-90-write-10-point-read \
  --operations 10000 \
  --read-operations 10000 \
  --campaign warm \
  --duration-seconds 30 \
  --writer-clients 32 \
  --verify-reopen false
```

Benchmark results are workload- and host-specific. Compare results only when
the Cedar commit, binary, host, filesystem, workload, transaction size,
duration, writer count, group-commit limits, and reopen setting match.

The benchmark reports committed operations, WAL sync count, transactions per
sync, encoded bytes per transaction, group-fill latency, retained WAL,
compaction debt, and write, background, and maintenance errors.

Measured results:

- [Bitemporal query acceptance evidence](docs/query-acceptance.md)
- [Development-host performance results](docs/query-performance.md)
- [Cedar User Guide](docs/user-guide.md)
- [Cedar documentation index](docs/README.md)

## Verification

Cedar includes:

- Debug and Release CTest suites;
- AddressSanitizer, UBSAN, and ThreadSanitizer profiles;
- WAL and reopen recovery tests;
- crash-recovery tests;
- temporal query correctness tests;
- projection fallback tests;
- columnar storage and space audits; and
- write, read, mixed, and analytical benchmarks.

## Project Status

The current `main` branch contains the Cedar Kernel implementation and the
bitemporal graph query system. Active design, implementation, and evidence
documents are indexed in [`docs/README.md`](docs/README.md). Superseded
Detailed implementation records are maintained outside the public product
documentation and are not required to build or use Cedar.

## License

Cedar is distributed under the Apache License 2.0.

See [LICENSE](LICENSE).
