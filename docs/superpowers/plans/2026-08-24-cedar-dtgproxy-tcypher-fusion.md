# Cedar-native DTGProxy T-Cypher Fusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task-by-task. Steps use
> checkbox syntax for tracking.

**Goal:** Fuse DTGProxy T-Cypher semantics into Cedar's existing query/kernel stack while
keeping `FactEvent` as the only canonical event/fact row and package Cedar as a single production
server.

**Architecture:** A Cedar C++ lexer/parser/binder/compiler lowers directly to the existing Cedar
logical operators, snapshot scans, query runtime, and transaction publisher. `cedar-server` is a
thin Bolt/lifecycle wrapper around one `Database`; no DTGProxy executor, Adapter runtime, or
second event log is added.

**Tech Stack:** C++20, CMake, GoogleTest, Cedar kernel, RocksDB fork, Cedar Parquet v2, POSIX
sockets/signals/filesystem. No Arrow, Thrift, Boost, Rust runtime, or external server framework.

## Global constraints

- `FactEvent` is the sole canonical event and fact row.
- `CHANGES` reads canonical fact rows, never `QueryDelta`, projection state, or a history chain.
- Existing Cedar `Query`, `PreparedQuery`, `Snapshot`, `Transaction`, and `QueryRuntime` remain the only execution path.
- Public identities carry `PartID`; single-node execution uses `PartID{0}`.
- Old `AT ...` and `DIFF GRAPH` syntax is rejected with no compatibility rewrite.
- No automatic legacy data migration; the new database directory is created by deployment.
- Every build and test command sets `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1` and uses `-j1`.

## Execution status (2026-08-24)

- Tasks 0-3, 5-6: implemented and covered by focused tests.
- Task 4: bounded paths, CHANGES, system-time `AS OF` ceilings, and typed metadata projections
  are implemented; mixed patterns remain explicitly rejected until a product/sequence node
  exists in the Cedar algebra.
- Task 7: lifecycle, health, lock/pid, text RUN, Bolt handshake, stateful HELLO/RUN/PULL,
  RECORD frames, and transaction lifecycle messages are implemented. Explicit Bolt writes
  stage in one Cedar transaction and publish only on COMMIT; pending-write reads are rejected
  until a transaction-local QueryRuntime view is added.
- Task 8: baseline, evidence, and fixed-duration smoke benchmark are implemented; the
  benchmark is not a production capacity claim.

### Task 0: Baseline and clean branch

**Files:** `docs/audit/2026-08-24-cedar-tcypher-fusion-baseline.md`

- [ ] Configure a Debug test build from this clean branch:

```bash
cd /Users/wangyang/Desktop/Cedar/.worktrees/rocksdb-kernel-stage-a
export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake -S . -B build-tcypher -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=OFF
cmake --build build-tcypher -j1
```

- [ ] Run the existing query, kernel, storage, recovery, and RocksDB profile tests and record
  exact pass counts, host memory, and build command in the audit file.
- [ ] Commit `docs: record clean Cedar T-Cypher fusion baseline`.

### Task 1: Canonical FactEvent contract

**Files:** Modify `include/cedar/fact/fact.h`, `include/cedar/fact/fact_codec.h`,
`src/storage/facts/fact.cc`, `src/storage/facts/fact_codec.cc`, `src/storage/rocks/commit_publisher.cc`,
`include/cedar/snapshot.h`, `src/kernel/snapshot.cc`; test `tests/storage/test_fact_event_canonical.cc`.

- [ ] Add tests proving one committed mutation yields the same decoded `FactEvent` through
  WAL/reopen, `EventScan`, `StateScan`, and columnar scan, including DELETE and edge identity.
- [ ] Add the label fact family using the existing canonical key shape only if the schema tests
  require labels; use a family discriminator, never a reserved parallel log.
- [ ] Make event scan ordering and system-time sequence-index lookup deterministic by `(commit_seq,
  canonical fact key)`; reject duplicate mutations before the write batch is assembled.
- [ ] Document and enforce that sequence metadata contains keys only, not duplicate event payloads.
- [ ] Keep `QueryDelta` explicitly non-authoritative and ensure `CHANGES` cannot select it.
- [ ] Run focused fact/codec/snapshot tests and commit `feat: make FactEvent the canonical event row`.

### Task 2: Cedar-native T-Cypher lexer, AST, and clean break

**Files:** Create `include/cedar/cypher/{ast,lexer,parser}.h`, `src/cypher/{ast,lexer,parser}.cc`,
`tests/cypher/test_cypher_parser.cc`; modify `CMakeLists.txt` and `tests/CMakeLists.txt`.

- [ ] Add red/green tests for `USE`, valid/system scopes, `MATCH`, fixed/bounded/mixed paths,
  `TRAIL`, metadata functions, `CHANGES`, writes, parameters, and source spans.
- [ ] Implement bounded deterministic lexing and recursive descent with source/token/nesting caps.
- [ ] Reject `AT VALID_TIME`, `AT TRANSACTION_TIME`, `DIFF GRAPH`, and unknown trailing input;
  preserve stable error categories without returning rewritten queries.
- [ ] Keep AST storage-independent; use Cedar types only in binding.
- [ ] Build focused parser tests with `cmake --build build-tcypher -j1` and commit
  `feat: add Cedar-native T-Cypher grammar`.

### Task 3: Binder, schema catalog, PartID, and fingerprint

**Files:** Create `include/cedar/cypher/{binder,fingerprint}.h`, `src/cypher/{binder,fingerprint}.cc`,
`tests/cypher/test_cypher_binder.cc`; modify `include/cedar/identity.h`, `include/cedar/schema.h`.

- [ ] Bind named parameters once to typed `ParameterId` entries and reject missing/type-invalid values.
- [ ] Resolve the configured graph namespace, labels, relationship types, properties, and
  `PartID`; reject bare distributed identities and unbounded hops.
- [ ] Normalize inherited/overridden temporal scopes and enforce the statement snapshot ceiling.
- [ ] Collect `FactDemandSet` and include scope, schema, PartID, topology, and security inputs in
  the fingerprint without logging query text or values.
- [ ] Differential-test scope, predecessor/successor demand, metadata provenance, and negative
  syntax cases; commit `feat: bind T-Cypher against Cedar schema`.

### Task 4: Lower to existing Cedar Query and temporal runtime

**Files:** Modify `include/cedar/query/query.h`, `src/query/logical/*`, `src/query/planner/*`,
`src/query/runtime/{temporal_source,query_runtime,graph_frontier}.cc`; create
`tests/cypher/test_cypher_compile_execute.cc`.

- [ ] Compile vertex/edge scans, fixed expand, bounded expand, property predicates, projections,
  metadata projection, and `COUNT(*)` into the existing logical plan and `QueryCursor`.
- [ ] Add only missing same-runtime nodes (`ChangeScan`, `MetadataProject`, segmented frontier);
  do not introduce a T-Cypher executor or a second column batch ABI.
- [ ] Implement `CHANGES` directly over `Snapshot::EventScan`; implement state folding over the
  same canonical rows with half-open intervals, winner selection, predecessor/successor, and
  demand-driven coalescing.
- [ ] Add mixed path and global `TRAIL` tests with explicit hop/frontier budgets and cancellation.
- [ ] Run focused compiler/runtime tests and commit `feat: lower T-Cypher to Cedar operators`.

### Task 5: Transactional writes and recovery

**Files:** Modify `include/cedar/transaction.h`, `src/kernel/transaction.cc`, `src/kernel/database.cc`,
`src/storage/rocks/commit_publisher.cc`, `src/query/query_api.cc`; test
`tests/cypher/test_cypher_writes.cc`, `tests/recovery/test_cypher_recovery.cc`.

- [ ] Lower CREATE/SET/DELETE and edge identity mutations to one Cedar `Transaction`.
- [ ] Add read-your-writes through a shared snapshot/transaction read-view adapter, without a
  second query runtime.
- [ ] Verify one commit produces one canonical fact/event sequence, durable acknowledgement,
  reopen visibility, and immutable PUT/DELETE `CHANGES` rows.
- [ ] Reject historical system-time writes and unresolved MATCH writes with stable statuses.
- [ ] Run recovery and transaction tests; commit `feat: execute T-Cypher writes through Cedar transactions`.

### Task 6: Embedded API closure

**Files:** Modify `include/cedar/cypher.h`, `include/cedar/database.h`, `src/query/query_api.cc`;
test `tests/cypher/test_cypher_embedded_api.cc`.

- [ ] Expose `PrepareCypher`, typed `CypherRequest`, bounded `CypherResult`, cancellation, and
  explicit transaction context while delegating all execution to `PreparedQuery`/`QueryRuntime`.
- [ ] Make database identity stable per configured database path and validate PartID/graph scope.
- [ ] Test point/range/changes/path/read-write and error mapping through the embedded API.
- [ ] Commit `feat: close Cedar embedded T-Cypher API`.

### Task 7: Production server and process wrapper

**Files:** Create `include/cedar/server.h`, `src/server/{server,bolt_codec,bolt_session,server_config,
process_lock,server_health}.{h,cc}`, `tools/cedar_server_main.cc`, `tools/cedar_admin_main.cc`,
`tests/server/{test_server_config,test_server_lifecycle,test_bolt_protocol}.cc`; modify `CMakeLists.txt`,
`tests/CMakeLists.txt`, install rules, and `README.md`.

- [ ] Implement length-checked Bolt v5.4 handshake, HELLO, RUN/PULL, BEGIN/COMMIT/ROLLBACK,
  RESET, GOODBYE, bounded pipeline, typed values, and stable Cedar error codes.
- [ ] Implement config precedence, owner-only lock/pid files, `/live`, `/ready`, `/metrics`,
  ordered signal shutdown, second-signal force exit, and idempotent Stop.
- [ ] Ensure the process owns one `Database` and never starts Gateway/Data/Meta child processes.
- [ ] Add a real restart test: Bolt CREATE, readiness, MATCH/CHANGES, stop, reopen, and WAL
  visibility; commit `feat: package Cedar as cedar-server`.

### Task 8: Clean-break, evidence, and performance

**Files:** Create `tests/cypher/test_tcypher_clean_break.cmake`,
`benchmarks/cedar_tcypher_fusion_bench.cc`, `benchmarks/run_cedar_tcypher_fusion.sh`,
`docs/audit/2026-08-24-cedar-tcypher-fusion-evidence.md`.

- [ ] Audit production code/docs for a second parser, event log, Diff path, or direct storage
  benchmark bypass; retain old syntax only in explicit rejection fixtures.
- [ ] Run full tests and install-consumer checks using `cmake --build build-tcypher -j1`.
- [ ] Measure embedded and Bolt point/range/changes/one-hop/mixed-path/write workloads with
  p50/p95/p99, WAL latency, columnar bytes, interval fragments, peak memory, and errors.
- [ ] Report theory versus measurement; do not claim a TPS target without a continuous run.
- [ ] Commit `test: close Cedar T-Cypher fusion gates`.
