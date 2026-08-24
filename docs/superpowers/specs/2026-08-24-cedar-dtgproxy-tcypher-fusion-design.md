# Cedar-native DTGProxy T-Cypher Fusion Design

Date: 2026-08-24

Status: approved direction; implementation starts from `main` on
`codex/cedar-tcypher-fusion`.

## Decision

Cedar owns the only production T-Cypher parser, binder, compiler, executor, transaction
entry point, and process boundary. DTGProxy is the language/semantic source and the future
distributed control plane, not a second Cedar runtime. The implementation is a clean break
from the discarded `codex/rocksdb-kernel-stage-a` branch.

The central invariant is:

```text
DTGProxy event == Cedar FactEvent == one canonical fact/event row
```

The words event and fact name two views of the same immutable row. Cedar never introduces a
second event type, event CF, event payload log, or event-specific commit publisher.

## Ownership boundaries

| Concern | Owner |
| --- | --- |
| T-Cypher syntax, temporal scope, binding, fingerprint | Cedar `src/cypher` |
| Logical/physical operators and vector execution | Cedar `src/query` |
| Commit, WAL, sequence, memtable, recovery | Cedar kernel and RocksDB |
| Canonical fact/event payload | `FactEvent` and `facts` CF |
| Authoritative columnar persistence | Cedar Parquet v2 `.sst` |
| Future routing, topology, distributed 2PC | DTGProxy control plane |
| External process protocol and lifecycle | Cedar `cedar-server` |

DTGProxy must not parse and compile a second production plan for the same Cedar request. A
future distributed adapter receives a versioned Cedar plan or bounded typed primitive request,
never a new query language implementation.

## Canonical fact/event model

`FactEvent` is created once after `commit_seq` assignment. Its identity is the database graph
namespace plus:

```text
(PartID, FactFamily, Property/LabelId, EntityId, valid_from, commit_seq)
```

`PartID` is present in the canonical key and in `VertexRef`/`EdgeRef`. Single-node databases use
zero. `EdgeIdentity` carries home, source, and target parts and remains the provenance payload
of the edge identity fact. Labels, when enabled, are facts in a Cedar fact family rather than a
reserved second log.

The commit publisher writes the encoded canonical row to the RocksDB facts CF in the same
atomic batch as sequence metadata. WAL recovery, active memtables, immutable memtables, flush,
columnar `.sst` rewrite, and compaction preserve the row byte semantics. Sequence metadata may
index canonical fact keys for system-time change scans, but it never stores a second event value.

`CHANGES` scans canonical `FactEvent` rows and returns PUT/DELETE provenance directly. State
queries derive visible winners and half-open intervals from the same rows. `QueryDelta`, if
retained, is a bounded recent-write cache only; it is never authoritative and is never used as
the source of `CHANGES`.

## Language and lowering

The Cedar pipeline is:

```text
UTF-8 -> Lexer -> AST -> Binder -> Cedar logical Query -> PreparedQuery -> QueryRuntime
```

The accepted language includes `USE`, `FOR VALID_TIME`, `FOR SYSTEM_TIME AS OF`, `CHANGES`,
`MATCH`, fixed and bounded relationships, mixed bounded paths with `TRAIL`, metadata functions,
`CREATE`, `SET`, `DELETE`, and `VALID FROM`. Removed `AT ...` and `DIFF GRAPH` forms are rejected
without rewriting.

The compiler lowers to existing Cedar operators (`Vertices`, `Edges`, `Expand`, `KHopExpand`,
property binding, `Where`, `Select`) and adds only missing logical nodes such as `ChangeScan` and
`MetadataProject` to the same planner/runtime. It does not add a second column batch ABI or a
second temporal executor. Named parameters are converted to typed Cedar `Bindings` once per
request.

Temporal semantics are representation-independent: system cutoff and winner selection precede
interval derivation; predecessor/successor rows close range results; edge intervals intersect
endpoint intervals; demand-driven boundaries limit fragmentation; coalesce equality includes
requested provenance. `CHANGES` skips state folding.

## Process boundary

The first production process is a single `cedar-server` owning one `Database`. It exposes a
bounded Bolt v5.4 subset (`HELLO`, `RUN`, `PULL`, `BEGIN`, `COMMIT`, `ROLLBACK`, `RESET`,
`GOODBYE`) and loopback health endpoints (`/live`, `/ready`, `/metrics`). Bolt handles framing,
session state, typed values, backpressure, and status mapping; all semantics stay in Cedar's
embedded API.

Startup acquires an owner-only lock, opens/recover WAL, publishes a visible snapshot, then marks
ready. Shutdown closes admission, drains accepted writes, cancels queued work, drains results,
closes Cedar/RocksDB in kernel order, and removes only process-owned artifacts. A second signal
forces a non-zero exit without deleting data. The server never performs legacy migration or
maintains a DTGProxy Gateway/Data/Meta process graph in single-node mode.

## Complexity and performance hypotheses

```text
parse:          O(input_bytes)
bind/compile:   O(ast_nodes + demanded_facts)
point scan:     O(log row_groups + matching_pages)
changes scan:   O(log index + returned_events)
state scan:     O(returned_events + demanded_boundaries)
bounded path:   O(sum frontier per hop), with hard hop/frontier limits
```

The T-Cypher write path adds no storage write relative to a direct Cedar transaction. The
single-process Bolt path adds framing and one value conversion at the protocol boundary, not an
inter-process query hop. Throughput and p50/p95/p99 claims require a reproducible benchmark; no
fixed TPS target is claimed before continuous measurement.

## Verification gates

Tests must prove canonical event identity across WAL, reopen, memtable, columnar flush, `CHANGES`,
and state scans; parser clean-break behavior; differential temporal semantics; edge/path writes;
read-your-writes; Bolt streaming/backpressure; lock/readiness/signal lifecycle; bounded resource
failure; and installation from a clean database directory. Every build uses `-j1`.
