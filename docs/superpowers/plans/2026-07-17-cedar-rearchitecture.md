# Cedar Six-Stage Rearchitecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` (recommended) or `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Cedar's legacy Descriptor/Frond/row-at-a-time engine with the six approved, durable, typed HTAP subsystems in dependency order.

**Architecture:** The work is a clean API and format break. Each phase supplies an independently testable foundation for later phases, and its public invariants are kept by all following phases. Durable data publication stays Manifest-owned; no compatibility switch or parallel legacy runtime remains active.

**Tech Stack:** C++17, CMake, GoogleTest, POSIX file APIs, LZ4 (existing dependency); add only narrowly scoped dependencies needed by an approved format (for example, BLAKE3).

## Global Constraints

- Implement phases only in the order below: correctness kernel, columnar v2, vector T-Cypher, temporal index/CBO, resource scheduling, observability/benchmarks.
- Use test-first development: run each new test while it fails before adding its production implementation.
- Preserve complete edge identity, bitemporal visibility, a continuous `visible_seq` prefix, and Manifest-owned file lifetime.
- Old `Descriptor`, Frond v1, legacy Cypher, index, cache, thread-pool, metrics, and runtime-switch paths are removed at the owning phase; no shim is retained.
- A successful commit is acknowledged only after its durable decision is installed in the visible prefix.
- Each phase ends with focused tests, full `ctest --output-on-failure`, and a clean-break repository scan.

## Existing Code Baseline

| Current area | Legacy implementation | Replacement owner |
| --- | --- | --- |
| `src/storage/lsm_engine.cc` | Monolithic MemTable, WAL, SST, cache, and compaction control plane | `transaction/`, `storage/`, `manifest/`, `runtime/` modules |
| `src/types/descriptor.cc`, `src/frond/` | Descriptor values, whole-file zone SST, duplicate Blob code | typed value/schema, `columnar/`, `blob/` |
| `src/cypher/` | old parser and row-at-a-time execution | `tcypher/` parser through vector runtime |
| legacy storage index/pool/cache files | independent background services and mutable index assumptions | `index/`, `statistics/`, `runtime/`, `cache/` |
| current tests | 45 legacy smoke/unit tests | phase-specific format, model, crash, scheduling, and benchmark suites |

## Phase 1: HTAP Correctness Kernel

**Files to create:** `include/cedar/transaction/{logical_key,transaction_manager,decision_log,shard_directory,snapshot_registry,recovery_manager}.h`, matching `src/` files; `include/cedar/storage/{storage_shard,temporal_event,version_set}.h`, matching sources; kernel tests under `tests/transaction/` and `tests/storage/`.

- [ ] Add failing unit tests for stable logical-key hashing and persisted shard configuration; implement `LogicalKey` and `ShardDirectory` with full vertex/edge identity.
- [ ] Add failing temporal-model tests for `(valid_time, snapshot_seq)` visibility, equal-valid-time `commit_seq` tie breaks, tombstones, and resurrection; implement immutable `TemporalEvent` and the shared resolver.
- [ ] Add failing DecisionLog tests for durable prepare/commit/abort records and a missing/invalid referenced prepare; implement checksummed shard WAL records, global DecisionLog, and durable outcome lookup.
- [ ] Add failing multi-shard tests that prove no prepare is visible and no acknowledged commit is partial; implement transaction validation, per-shard reservations, commit sequencing, installation, and `visible_seq` advancement.
- [ ] Add failing strict-transaction tests for exact-key read/write conflicts and real-time order; reject unsupported strong range operations explicitly.
- [ ] Add failing Manifest/VersionSet tests for atomic add/delete edits, complete transitive compaction overlap closure, snapshot pinning, and retained tombstones; move live-file ownership to the new VersionSet.
- [ ] Add crash/reopen tests at prepare, decision, install, flush, Manifest, and torn-tail boundaries; implement recovery that reconstructs the committed prefix and cleans only unreferenced output.
- [ ] Migrate public autocommit operations onto the transaction manager, remove legacy independent publication paths, and run kernel/model/crash tests plus the full test suite.

## Phase 2: Columnar SST v2 and Blob v2

**Files to create:** `include/cedar/{types/value.h,schema/schema_registry.h,columnar/{page_format,page_codec,granule_builder,sst_builder_v2,sst_reader_v2,temporal_read_merger,page_cache}.h,blob/{blob_store,blob_segment,blob_hash_index,blob_reference_catalog,blob_gc}.h}` with matching sources and tests under `tests/{types,schema,columnar,blob}/`.

- [ ] Add failing typed-value and schema-epoch tests; implement physical values, registered schemas, durable schema Manifest edits, and explicit mismatch errors.
- [ ] Add failing Blob record/index round-trip and corruption tests; implement content-addressed BLAKE3 Blob references, sharded append-only segments, index publication, and deduplication.
- [ ] Add failing page-header/directory golden-byte, codec round-trip, checksum, offset, and decompression-bound tests; implement portable v2 page format and codec registry.
- [ ] Add failing granule tests for logical-key-aligned boundaries, oversized continuations, complete edge identity, and required temporal system columns; implement block construction and indexes.
- [ ] Add failing SST open/read tests proving metadata-only open, selected-page decoding, bounded reads, and exact temporal results; implement v2 builder, reader, cache, and temporal merger.
- [ ] Add failing flush/reopen/compaction tests; wire v2 SST publication through Phase 1 Manifest and verify compaction copies Blob references without Blob payload reads.
- [ ] Add crash, Blob relocation, and orphan-GC tests; implement reference catalog and snapshot-safe Blob GC.
- [ ] Migrate typed public storage APIs, remove Descriptor/Frond v1 and duplicate Blob paths, then run format/model/crash/full tests and a repository reference scan.

## Phase 3: T-Cypher Vectorized Temporal Execution

**Files to create:** the `include/cedar/tcypher/` and `src/tcypher/` trees specified by the approved T-Cypher design, plus `storage/query/` cursor contracts and `tests/tcypher/` oracle suites.

- [ ] Add failing HLC/CommitTimeline/checkpoint recovery tests; extend Phase 1 decision/recovery contracts with durable system-time mapping.
- [ ] Add failing `ColumnBatch`, null bitmap, flat/constant/dictionary vector, and `ResultBatch` tests; implement bounded vector/runtime contracts.
- [ ] Add failing MemTable/SST batch equivalence and vector scan/gather/expand tests; implement typed storage query cursors over Phase 2.
- [ ] Add parser/binder golden tests for state, range, change, system-time scopes, diagnostics, and temporal DML; implement tokenizer, AST, resolver, and type checking.
- [ ] Add logical rewrite and plan tests; implement temporal lowering, demand analysis, deterministic optimization, and bounded cost planning.
- [ ] Add pipeline tests for scan, resolve, filter, project, intervals, fixed Expand, joins, aggregates, spill, and cancellation; implement bounded morsel execution.
- [ ] Add temporal path and mutation tests; implement bounded `TRAIL` paths, `TransactionSink`, `EXPLAIN`, and `EXPLAIN ANALYZE` contracts.
- [ ] Compare every query class against a separate scalar bitemporal oracle across flush, compaction, restart, and Blob relocation; remove legacy Cypher production paths and run clean-break checks.

## Phase 4: Temporal Index and CBO

**Files to create:** `include/cedar/{index,statistics,optimizer}/` and matching sources; index physical operators under `tcypher/physical/`; tests under `tests/{index,statistics,optimizer}/`.

- [ ] Add failing canonical-value and sidecar format tests; implement typed comparison, format IDs, checksums, immutable directories, and posting codecs.
- [ ] Add failing catalog-generation and pinned snapshot tests; add Manifest-owned index definition/fragment/coverage edits.
- [ ] Add failing candidate-completeness tests over temporal histories; implement sidecar readers/builders and mandatory base validation.
- [ ] Add failing partial-coverage and MemTable delta tests; implement hybrid scans that cannot create false negatives or duplicate logical events.
- [ ] Add failing statistics merge/restart tests; implement immutable statistics fragments and VersionSet snapshots with conservative corruption fallback.
- [ ] Add failing build/repair/drop concurrency tests; implement catalog administration and resource-governed maintenance hooks.
- [ ] Add physical index/hybrid/intersection plans and CBO tests for legal deterministic alternatives, budget exhaustion, parameter variants, and runtime feedback.
- [ ] Add Explain/Analyze metric tests, then remove legacy index headers, settings, builders, and switches; run oracle/crash/full suites.

## Phase 5: HTAP Resource and Maintenance Scheduling

**Files to create:** `include/cedar/{runtime,maintenance,cache}/` and `src/` equivalents listed in the resource-scheduling design; tests under `tests/runtime/` and `tests/maintenance/`.

- [ ] Add failing `ResourceProfile`, request, reservation, and structured-error tests; implement typed resource contracts.
- [ ] Add failing memory, I/O token, descriptor, and temporary-space accounting tests; implement governors with emergency reserves.
- [ ] Add failing scheduler priority, fairness, aging, cancellation, and critical-lane reservation tests; implement `WorkScheduler` and task context.
- [ ] Add failing pressure state/hysteresis/write-admission tests; implement pressure controller using MemTable, WAL, compaction, cache, and disk signals.
- [ ] Move page, metadata, Blob, and query cache allocations under `CacheManager`; test scan-pollution resistance and snapshot pin safety.
- [ ] Route commit-critical WAL/DecisionLog/Manifest work through reserved resources; test completion after prepare under pressure.
- [ ] Route flush, compaction, index, statistics, Blob GC, and T-Cypher morsels through typed maintenance/foreground queues; test cancellation and restart reconstruction.
- [ ] Add fault/shutdown tests and scheduling telemetry; remove legacy component-owned pools, caches, and bypass switches; run deterministic and stress suites.

## Phase 6: Observability and Reproducible Benchmarks

**Files to create:** `include/cedar/observability/`, `src/observability/`, `benchmarks/`, `tests/model/`, and test fixtures/artifact schemas.

- [ ] Add failing metric schema, bounded-label, counter, histogram merge, and exporter-isolation tests; implement registry and thread-local metrics.
- [ ] Add failing aggregation, event-ring, trace-context/sampling, and backpressure tests; implement bounded telemetry pipeline governed by Phase 5.
- [ ] Instrument all durable/runtime/index/scheduler contracts with stable metric families and test metric units/cardinality.
- [ ] Add failing `EXPLAIN ANALYZE` profile tests; implement profiles plus local metric and trace exporters.
- [ ] Add deterministic Cedar-TG generator, run-manifest hash, environment probe, and small-oracle tests.
- [ ] Add benchmark phase, arrival-mode, durability/cache mode, artifact, and report-regeneration tests; benchmark only public durable APIs.
- [ ] Add workload, paired-regression, baseline-key, and noisy-environment tests; implement reports and release gates.
- [ ] Remove untraceable legacy metrics/trace flags/benchmark helpers and unsupported README claims; run schema, overhead, reproducibility, correctness, crash, and report tests.

## Verification Commands

```bash
cmake -S . -B build -DBUILD_TESTS=ON -DBUILD_EXAMPLES=OFF
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

Each phase additionally runs only its newly added test binaries first, followed by the complete command above. Before claiming a phase complete, scan for its removed legacy public identifiers with `rg` and verify no production references remain.

## Plan Self-Review

- All six approved designs are represented in dependency order.
- Each phase defines module boundaries, test-first work items, legacy removals, and final verification.
- The plan intentionally contains no migration or compatibility path, matching the approved clean-break requirement.
