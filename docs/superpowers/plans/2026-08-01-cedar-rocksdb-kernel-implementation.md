# Cedar RocksDB Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Cedar's custom canonical storage path with a standard RocksDB-backed embedded bitemporal graph kernel, explicit transactions, and rebuildable columnar projections.

**Architecture:** Delivery is split into four dependent stage plans. Stage A adds the pinned RocksDB dependency and a fully tested `FactStore` without changing the current public path. Stage B builds the clean-break public domain and transaction interface on `FactStore`; Stage C adds Vacuum and derived projections; Stage D removes the legacy canonical store and query coupling and closes all verification gates.

**Tech Stack:** C++20, CMake, RocksDB v11.1.2, GoogleTest, CRC32C, ASAN, UBSAN, TSAN.

## Global Constraints

- Standard unmodified RocksDB v11.1.2 is the only canonical WAL, MemTable, SST, Manifest, compaction, blob-file, and recovery implementation.
- RocksDB is pinned as repository-owned source and linked statically; release builds have no system RocksDB fallback.
- Cedar `commit_seq` is encoded in the Cedar fact key and is never replaced by RocksDB internal sequence or user-defined timestamps.
- The only Cedar-owned Column Families are `facts` and `meta`; user properties never create Column Families.
- Ordinary transactions append Cedar PUT/DELETE events and never physically delete fact keys.
- Every acknowledged transaction is one synchronous RocksDB WriteBatch containing facts, outcome, sequence record, and visible watermark.
- All write entry points use explicit `Transaction`; old `Database::Put/Delete` do not survive the clean break.
- Full bitemporal history is retained until explicit Vacuum.
- Columnar, adjacency, index, and statistics data are rebuildable projections and never participate in commit truth or recovery-required state.
- The old database format is rejected without mutation; there is no dual write or online migration path.
- Production source must not contain PREPARE/Decision/CAC log paths after Stage D.

## Stage Plans

### Stage A — RocksDB FactStore Foundation

Plan: `docs/superpowers/plans/2026-08-01-cedar-rocksdb-stage-a.md`

Produces:

```cpp
FactKey
FactEvent
EncodeFactKey / DecodeFactKey
EncodeFactValue / DecodeFactValue
FactStore::Open / BeginSnapshot / Read / Scan / Commit
FactStore::LeaseIds / ResolveTransaction
```

Acceptance:

- [ ] RocksDB v11.1.2 builds statically in Cedar's CMake graph.
- [ ] Golden codecs preserve bytewise fact/version ordering and reject malformed bytes.
- [ ] One synchronous WriteBatch survives close/reopen atomically.
- [ ] The durable visible watermark and sequence/outcome records reopen exactly.
- [ ] Vertex and edge ID leases never reuse IDs after reopen.
- [ ] The existing production path and tests still build at the stage boundary.

### Stage B — Explicit Bitemporal Kernel Interface

Plan: `docs/superpowers/plans/2026-08-01-cedar-rocksdb-stage-b.md`

Consumes Stage A `FactStore` and produces:

```cpp
Database::Open
Database::BeginTransaction
Database::BeginSnapshot
Transaction::Assert / Retract / Set / Unset / Commit / Rollback
Snapshot::Exists / Get / Scan
EdgeIdentity
snapshot and strict validation
```

Acceptance:

- [ ] No new public header includes RocksDB.
- [ ] Snapshot isolation and strict exact-read validation match the bitemporal oracle.
- [ ] Edge identity is immutable and edge visibility intersects both endpoints.
- [ ] Explicit historical snapshots remain stable across concurrent commits and reopen.
- [ ] The new `Database` path uses only `FactStore` for canonical data.

### Stage C — Vacuum and Columnar Projection

Plan: `docs/superpowers/plans/2026-08-01-cedar-rocksdb-stage-c.md`

Consumes Stage B and produces:

```cpp
Database::Vacuum
resumable vacuum state
ColumnarProjection
ProjectionManifest
projection coverage and canonical fallback
```

Acceptance:

- [ ] Vacuum keeps one baseline per `(fact, valid_from)` and every later correction.
- [ ] Snapshots below the durable boundary return `SnapshotExpired`.
- [ ] Active old snapshots reject Vacuum without cancellation.
- [ ] Crash at every Vacuum phase resumes monotonically.
- [ ] Missing, stale, or corrupt projections fall back to canonical facts.
- [ ] Projection rebuild preserves scan results after reopen and Vacuum.

### Stage D — Clean-Break Removal and Closure

Plan: `docs/superpowers/plans/2026-08-01-cedar-rocksdb-stage-d.md`

Acceptance:

- [ ] `cedar_core` contains only the embedded kernel and derived projection interface.
- [ ] T-Cypher, optimizer, benchmark, and production telemetry are outside `cedar_core` or removed.
- [ ] DecisionLog, shard PREPARE WALs, TemporalMemTable, VersionSet, canonical Cedar SST, BlobStore, cache/governor ownership, and old `CedarDatabase` are absent from the production path.
- [ ] Tests are split into focused targets; the monolithic correctness kernel is removed.
- [ ] Old FORMAT is rejected read-only and the new format is documented.
- [ ] Debug/Release CTest, ASAN, UBSAN, TSAN, crash/reopen tests, source inventory, and performance reports pass.

## Execution Order

Execute Stage A through D in order. Within each stage, do not begin a task until its consumed interfaces are green and committed. Do not delete a legacy implementation until the new production path has focused equivalence, reopen, and fault tests. Run `git diff --check` before every commit and preserve unrelated user changes if they appear.

## Final Completion Audit

- [ ] Map every requirement in `docs/superpowers/specs/2026-08-01-cedar-rocksdb-kernel-design.md` to a passing test or source inventory result.
- [ ] Verify RocksDB is the sole canonical persistent engine by inspecting build targets and production includes.
- [ ] Verify the public interface exposes only explicit transactions and RAII snapshots.
- [ ] Verify every bitemporal oracle case before/after reopen, engine compaction, Vacuum, and projection rebuild.
- [ ] Verify a projection directory can be deleted with no loss of database correctness.
- [ ] Record exact commands and outputs for all acceptance gates.
