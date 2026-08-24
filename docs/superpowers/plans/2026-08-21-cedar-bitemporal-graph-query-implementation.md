# Cedar Bitemporal Graph Query Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the approved Cedar Kernel bitemporal graph query system with typed C++ construction, Snapshot-correct projections and QueryDelta, interactive and analytical execution, temporal graph algorithms, Cedar-owned resources, and evidence-backed read/write/space acceptance.

**Architecture:** One immutable typed logical plan feeds a late-binding planner. Physical execution chooses canonical CedarParquet reads, Cedar-owned projection segments plus a contiguous QueryDelta, or disjoint mixtures of both; a pull cursor runs interactive operators inline and analytical pipelines through bounded Cedar workers. RocksDB retains the only WAL/recovery/MemTable/VersionSet/MANIFEST authority while Cedar owns query files, scheduling, scratch, statistics, and correctness fallback.

**Tech Stack:** C++20, CMake, GoogleTest, embedded RocksDB 11.1.2-derived engine, CedarParquet facts, pinned LZ4, CRC32C, ASAN, UBSAN, TSAN, CSV/JSON benchmark artifacts.

## Global Constraints

- Work only in `/Users/wangyang/Desktop/Cedar/.worktrees/cedar-wal-group-commit` on branch `codex/cedar-wal-group-commit` unless execution first creates a successor `codex/` worktree with `using-git-worktrees`.
- The authoritative design is `docs/superpowers/specs/2026-08-21-cedar-bitemporal-graph-query-design.md` at commit `286b525`.
- Preserve one RocksDB WAL and RocksDB ownership of recovery, MemTables, VersionSet, MANIFEST, native flush/compaction mechanics, checkpoints, and authoritative obsolete-file deletion.
- CedarParquet facts in the `facts` column family remain the only authoritative graph facts.
- Query projections, statistics, QueryDelta, and scratch are derived or temporary and never enter the commit WriteBatch.
- Production is Cedar Kernel only. Add no Lean, generic, legacy, migration, dual-write, or alternate query path.
- All valid-time intervals are half-open `[from,to)`; bounded intervals require `from < to`.
- Every query is exact for its consumed Snapshot. Projection uncertainty means authoritative fallback or an explicit error, never stale or partial success.
- No SQL NULL, implicit type conversion, all-path enumeration, query JIT, arbitrary non-FIFO journey, or RocksDB periodic query/statistics thread.
- Add no T-Cypher parser/runtime, outer join, persistent Cursor, PageRank,
  connected-components, triangle-count, centrality, community-detection, or
  distributed exchange surface in this implementation.
- `QueryMemoryLimit` is removed; all bounded resource exhaustion uses `ResourceExhausted` with a diagnostic dimension.
- Debug, differential, crash, and sanitizer gates precede every Release performance claim.
- Cold, warm, peak, and 30-minute sustained benchmark results remain separately labeled.
- Existing uncommitted multi-fact benchmark edits in `benchmarks/cedar_kernel_bench*` and `tests/performance/*` belong to the user. Do not overwrite, discard, or stage them in Tasks 1-17; inspect and integrate them explicitly in Task 18.
- Every commit stages exact task paths. Never use `git add .` in this worktree.

---

## File Structure

### Public query interface

```text
include/cedar/query.h                    umbrella include only
include/cedar/query/types.h              temporal scopes, slots, types, budgets, bindings
include/cedar/query/expression.h         typed immutable expression construction
include/cedar/query/query.h              immutable logical query and PreparedQuery
include/cedar/query/result.h             schema, columns, batches, cursor, Explain/profile
```

`include/cedar/database.h` gains `PrepareQuery`, query runtime options, query metrics, and explicit statistics refresh. No public header names RocksDB.

### Internal query modules

```text
src/query/temporal/interval.{h,cc}       half-open interval algebra
src/query/temporal/corrected_chain.{h,cc} corrected boundaries and state materialization
src/query/logical/expression.{h,cc}      erased typed-expression nodes and validation
src/query/logical/logical_plan.{h,cc}    immutable logical nodes and row schemas
src/query/query_api.cc                   public pimpl objects and Database entry points
src/query/planner/query_planner.{h,cc}   rewrites, costing, coverage split, lane binding
src/query/runtime/query_runtime.{h,cc}   cursor state, pull pipelines, cancellation
src/query/runtime/vector_kernels.{h,cc}  filter/project/comparison/presence kernels
src/query/runtime/relational.{h,cc}      joins, distinct, sort, row/temporal aggregate
src/query/runtime/graph_frontier.{h,cc}  expand, k-hop, labels, predecessor arena
src/query/runtime/journey.{h,cc}         FIFO journey objectives and Pareto dominance
src/query/projection/projection_format.{h,cc} versioned pages, compression, checksums
src/query/projection/projection_manifest.{h,cc} coverage and atomic CURRENT publication
src/query/projection/projection_store.{h,cc} generations, page cache, quarantine, builders
src/query/projection/query_delta.{h,cc}   contiguous post-base changes and repair
src/query/resource/query_resource_pool.{h,cc} admission, memory, workers, I/O permits
src/query/resource/query_scratch.{h,cc}  verified per-query spill and orphan cleanup
src/query/observability/query_metrics.{h,cc} bounded global metrics and QueryProfile
```

Keep implementation files focused. If an implementation needs a private helper used by one `.cc`, keep it in that translation unit instead of creating a shallow interface.

### Tests and benchmarks

```text
tests/query/test_query_types.cc
tests/query/test_temporal_model.cc
tests/query/test_logical_plan.cc
tests/query/test_query_canonical.cc
tests/query/test_query_relational.cc
tests/query/test_projection_format.cc
tests/query/test_projection_store.cc
tests/query/test_query_delta.cc
tests/query/test_query_planner.cc
tests/query/test_query_resources.cc
tests/query/test_temporal_expand.cc
tests/query/test_coexisting_path.cc
tests/query/test_temporal_journey.cc
tests/query/test_query_lifecycle.cc
tests/query/test_query_observability.cc
tests/query/test_query_differential.cc
tests/recovery/test_query_crash_matrix.cc
benchmarks/cedar_query_bench_options.{h,cc}
benchmarks/cedar_query_bench_workload.{h,cc}
benchmarks/cedar_query_bench.cc
benchmarks/run_cedar_query_campaign.sh
tests/performance/test_query_bench_options.cc
tests/performance/test_query_benchmark_csv.cmake
docs/superpowers/evidence/2026-08-21-cedar-bitemporal-query-acceptance.md
```

## Execution Preflight

- [ ] Confirm the branch, dirty paths, and design commit before Task 1.

Run:

```bash
git status --short --branch
git show --stat --oneline 286b525
git diff -- benchmarks/ tests/performance/
```

Expected: branch `codex/cedar-wal-group-commit`; design commit present; only the known multi-fact benchmark work is dirty.

- [ ] Configure a reusable Debug build and establish the pre-query baseline.

Run:

```bash
cmake -S . -B build/query-debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
cmake --build build/query-debug -j2
ctest --test-dir build/query-debug --output-on-failure
```

Expected: configure and build succeed; all pre-existing tests pass. Record any pre-existing failure before editing and do not attribute it to query work.

---

### Task 1: Public Temporal Types and Query Statuses

**Files:**
- Create: `include/cedar/query/types.h`
- Create: `src/query/types.cc`
- Create: `tests/query/test_query_types.cc`
- Modify: `include/cedar/core/status.h`
- Modify: `src/core/status.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: existing `ValidTime`, `CommitSeq`, `VertexRef`, `EdgeRef`, `Value`, and `Status`.
- Produces: `ValidTimeInterval`, `ValidDuration`, six exact temporal scope alternatives, `TemporalScope`, `QueryExecutionMode`, `QueryBudget`, `QueryOptions`, `QueryType`, `SlotId`, `ParameterId`, `Bindings`, `Status::DeadlineExceeded`, and `Status::NumericOverflow`.

- [ ] **Step 1: Write the failing public-type and status tests.**

```cpp
#include <gtest/gtest.h>
#include <type_traits>
#include "cedar/query/types.h"

namespace cedar {
TEST(QueryTypesTest, ValidatesHalfOpenIntervals) {
  EXPECT_TRUE(ValidTimeInterval{ValidTime{2}, ValidTime{5}}.Validate().ok());
  EXPECT_TRUE(ValidTimeInterval{ValidTime{2}, std::nullopt}.Validate().ok());
  EXPECT_TRUE(ValidTimeInterval{ValidTime{2}, ValidTime{2}}
                  .Validate().IsInvalidArgument());
  EXPECT_TRUE(ValidTimeInterval{ValidTime{5}, ValidTime{2}}
                  .Validate().IsInvalidArgument());
}

TEST(QueryTypesTest, ExposesDistinctTerminalStatuses) {
  EXPECT_TRUE(Status::DeadlineExceeded("query").IsDeadlineExceeded());
  EXPECT_TRUE(Status::NumericOverflow("valid time").IsNumericOverflow());
}

static_assert(std::is_same_v<decltype(QueryOptions{}.mode), QueryExecutionMode>);
template <typename T>
concept HasObsoleteQueryMemoryLimit = requires {
  T::QueryMemoryLimit("obsolete");
};
static_assert(!HasObsoleteQueryMemoryLimit<Status>);
}  // namespace cedar
```

- [ ] **Step 2: Register and run the test to prove the interface is absent.**

Add to `tests/CMakeLists.txt`:

```cmake
add_executable(test_query_types query/test_query_types.cc)
target_link_libraries(test_query_types PRIVATE cedar_core ${GTEST_MAIN_TARGET})
gtest_discover_tests(test_query_types)
```

Run:

```bash
cmake --build build/query-debug -j2 --target test_query_types
```

Expected: compilation fails because `cedar/query/types.h`, `DeadlineExceeded`, and `NumericOverflow` do not exist.

- [ ] **Step 3: Add the exact temporal and resource value types.**

The public definitions must include these fields and defaults:

```cpp
struct ValidTimeInterval {
  ValidTime from;
  std::optional<ValidTime> to;
  Status Validate() const;
  bool operator==(const ValidTimeInterval&) const = default;
};

struct ValidDuration {
  uint64_t value = 0;
  constexpr bool operator==(const ValidDuration&) const = default;
};

struct At { ValidTime time; };
struct Events { ValidTimeInterval interval; };
struct Changes { ValidTimeInterval interval; };
struct Overlaps { ValidTimeInterval interval; };
struct Throughout { ValidTimeInterval interval; };
struct History { std::optional<ValidTimeInterval> interval; };
using TemporalScope = std::variant<At, Events, Changes, Overlaps, Throughout, History>;

enum class QueryType : uint8_t {
  kBool,
  kInt32,
  kInt64,
  kFloat32,
  kFloat64,
  kTimestamp64,
  kString,
  kBinary,
  kVertexRef,
  kEdgeRef,
  kValidTime,
  kValidDuration,
  kCommitSeq,
  kValidTimeInterval,
  kPath,
  kJourney,
};

enum class QueryExecutionMode : uint8_t { kAuto, kInteractive, kAnalytical };
struct QueryBudget {
  uint64_t memory_bytes = 8ULL * 1024ULL * 1024ULL;
  uint64_t scratch_bytes = 0;
  uint64_t read_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t prefetch_bytes = 8ULL * 1024ULL * 1024ULL;
  uint64_t decoded_rows = 1'000'000;
  uint64_t output_rows = 1'000'000;
  uint64_t output_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t interval_fragments = 1'000'000;
  uint64_t graph_labels = 1'000'000;
  uint64_t visited_vertices = 1'000'000;
  uint64_t cpu_us = 0;
  uint64_t deadline_us = 0;
  uint32_t max_parallelism = 1;
  uint32_t max_hops = 4;
  uint32_t retained_output_batches = 2;
};
struct QueryOptions {
  QueryExecutionMode mode = QueryExecutionMode::kAuto;
  QueryBudget budget;
  bool capture_profile = false;
};
```

`ValidTimeInterval::Validate` performs overflow-free comparison and accepts `to == nullopt`. `Bindings` rejects a duplicate parameter and a value whose `QueryType` differs from the parameter's exact type.

- [ ] **Step 4: Replace the obsolete query-memory status.**

Delete `QueryMemoryLimit`, `IsQueryMemoryLimit`, `kQueryMemoryLimit`, and its string mapping. Add:

```cpp
static Status DeadlineExceeded(const Slice& msg, const Slice& msg2 = Slice());
static Status NumericOverflow(const Slice& msg, const Slice& msg2 = Slice());
bool IsDeadlineExceeded() const;
bool IsNumericOverflow() const;
```

Do not renumber existing explicit codes: leave value 12 unused after deleting
`kQueryMemoryLimit`, assign `kDeadlineExceeded = 23` and
`kNumericOverflow = 24`, and update `ToString`. No on-disk format stores Status
codes, but preserving current numeric values keeps diagnostics stable.

- [ ] **Step 5: Build and run focused and public-header tests.**

Run:

```bash
cmake --build build/query-debug -j2 --target test_query_types
ctest --test-dir build/query-debug --output-on-failure -R 'QueryTypes|PublicHeaderContract'
```

Expected: all selected tests pass and public headers contain no RocksDB symbol.

- [ ] **Step 6: Commit the public foundation.**

```bash
git add include/cedar/query/types.h include/cedar/core/status.h \
  src/query/types.cc src/core/status.cc tests/query/test_query_types.cc \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add bitemporal query value types"
```

---

### Task 2: Corrected Temporal Chain Model and Independent Oracle

**Files:**
- Create: `src/query/temporal/interval.h`
- Create: `src/query/temporal/interval.cc`
- Create: `src/query/temporal/corrected_chain.h`
- Create: `src/query/temporal/corrected_chain.cc`
- Create: `tests/query/test_temporal_model.cc`
- Modify: `tests/model/bitemporal_fact_oracle.h`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 1 `ValidTimeInterval`; existing `FactEvent` and `Value`.
- Produces: `internal::Intersect`, `Clip`, `Coalesce`, `ResolveCorrectedBoundaries`, `MaterializePresentState`, `MaterializeMissingState`, and a test oracle that never calls production temporal helpers.

- [ ] **Step 1: Add failing correction, half-open, and latent-boundary tests.**

```cpp
TEST(TemporalModelTest, PreservesAFormerlyRedundantBoundary) {
  const FactRef ref = PropertyFact::Vertex({PartId{0}, VertexId{1}},
                                            PropertyId{7}).ref();
  std::vector<FactEvent> events = {
      {ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1, Value::Int64(7)},
      {ref, ValidTime{10}, CommitSeq{2}, FactOperation::kPut, 1, Value::Int64(7)},
      {ref, ValidTime{5}, CommitSeq{3}, FactOperation::kDelete, 1, std::nullopt},
  };
  auto boundaries = internal::ResolveCorrectedBoundaries(events, CommitSeq{3});
  ASSERT_TRUE(boundaries.ok());
  auto state = internal::MaterializePresentState(boundaries.ValueOrDie());
  ASSERT_EQ(state.size(), 2U);
  EXPECT_EQ(state[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(state[1].interval,
            (ValidTimeInterval{ValidTime{10}, std::nullopt}));
}

TEST(TemporalModelTest, TouchingIntervalsDoNotOverlap) {
  EXPECT_FALSE(internal::Intersect(
      {ValidTime{1}, ValidTime{2}}, {ValidTime{2}, ValidTime{3}}).has_value());
}
```

- [ ] **Step 2: Run the new test and observe missing temporal helpers.**

Run:

```bash
cmake --build build/query-debug -j2 --target test_temporal_model
```

Expected: compilation fails on the undefined helper types/functions.

- [ ] **Step 3: Implement interval algebra without sentinel infinity arithmetic.**

Use explicit optional upper bounds:

```cpp
struct CorrectedBoundary {
  ValidTime valid_from;
  CommitSeq commit_seq;
  FactOperation operation;
  uint32_t schema_epoch;
  std::optional<Value> value;
  std::optional<EdgeIdentity> edge_identity;
};
struct StateInterval {
  ValidTimeInterval interval;
  std::optional<Value> value;
  bool operator==(const StateInterval&) const = default;
};
```

`ResolveCorrectedBoundaries` discards every event whose `commit_seq` is greater
than the supplied Snapshot sequence, selects the greatest remaining commit at
each `valid_from`, and sorts ascending by valid time. `MaterializePresentState`
retains every corrected boundary while calculating intervals, then coalesces
only final adjacent equal PUT states. DELETE state is emitted only by
`MaterializeMissingState` when a caller supplies the enclosing entity interval.

- [ ] **Step 4: Extend the test oracle independently.**

Add oracle methods with straightforward enumeration:

```cpp
std::vector<FactEvent> CorrectedEvents(const FactRef&, CommitSeq) const;
std::vector<OracleStateInterval> History(const FactRef&, CommitSeq) const;
std::vector<OracleChange> Changes(const FactRef&, CommitSeq) const;
```

The oracle must select and sort directly from `events_`; importing
`src/query/temporal/*` into the oracle is forbidden.

- [ ] **Step 5: Run focused temporal and existing Snapshot tests.**

```bash
cmake --build build/query-debug -j2 --target test_temporal_model test_kernel_snapshot
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalModel|KernelSnapshot'
```

Expected: all selected tests pass, including same-boundary correction and endpoint visibility tests.

- [ ] **Step 6: Commit the temporal model.**

```bash
git add src/query/temporal tests/query/test_temporal_model.cc \
  tests/model/bitemporal_fact_oracle.h CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: define corrected temporal chain semantics"
```

---

### Task 3: Typed Expressions and Immutable Logical Plan

**Files:**
- Create: `include/cedar/query/expression.h`
- Create: `include/cedar/query/query.h`
- Create: `include/cedar/query.h`
- Create: `src/query/logical/expression.h`
- Create: `src/query/logical/expression.cc`
- Create: `src/query/logical/logical_plan.h`
- Create: `src/query/logical/logical_plan.cc`
- Create: `tests/query/test_logical_plan.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 1 scopes/types/bindings and Task 2 interval contracts.
- Produces: `Slot<T>`, `Parameter<T>`, `Expr<T>`, `OptionalExpr<T>`, `Query`, immutable `internal::LogicalPlanNode`, exact `RowSchema`, and all first-release `LogicalOpKind` values.

- [ ] **Step 1: Write compile-time and runtime plan tests.**

```cpp
TEST(LogicalPlanTest, BuildsTypedVertexPropertyPredicate) {
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  OptionalSlot<int64_t> age = OptionalSlot<int64_t>::Named("age");
  auto source = Query::Vertices(vertex, At{ValidTime{10}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto bound = source.ValueOrDie().BindVertexProperty(
      vertex, PropertyId{7}, age);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  auto filtered = bound.ValueOrDie().Where(
      IsPresent(age) && GreaterThan(ValueOf(age), Literal<int64_t>(18)));
  ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();
  auto selected = filtered.ValueOrDie().Select(
      {Project(vertex), Project(age)});
  ASSERT_TRUE(selected.ok()) << selected.status().ToString();
  EXPECT_EQ(selected.ValueOrDie().schema().columns().size(), 2U);
}

template <typename Left, typename Right>
concept CanCompareEqual = requires(Left left, Right right) {
  Equal(ValueOf(left), Literal<Right>(right));
};
static_assert(!CanCompareEqual<OptionalSlot<int64_t>, std::string>);
```

Also test that `Not(Equal(...))` is not rewritten to `NotEqual` for an optional operand and that duplicate SlotIds are rejected.

- [ ] **Step 2: Run the test and verify the typed plan is absent.**

```bash
cmake --build build/query-debug -j2 --target test_logical_plan
```

Expected: compilation fails because the expression and query headers are absent.

- [ ] **Step 3: Implement typed public handles over erased immutable nodes.**

Use this ownership shape:

```cpp
template <typename T>
class Expr {
 public:
  QueryType type() const { return QueryTypeOf<T>(); }
 private:
  std::shared_ptr<const internal::ExpressionNode> node_;
};

class Query {
 public:
  static StatusOr<Query> Vertices(Slot<VertexRef>, TemporalScope);
  StatusOr<Query> Expand(const ExpandSpec&) const;
  template <typename T>
  StatusOr<Query> BindVertexProperty(Slot<VertexRef>, PropertyId,
                                     OptionalSlot<T>) const;
  StatusOr<Query> Where(Expr<bool>) const;
  StatusOr<Query> Select(std::vector<Projection>) const;
  const RowSchema& schema() const;
 private:
  std::shared_ptr<const internal::LogicalPlanNode> root_;
};
```

Each transformation allocates one new immutable node and shares its input. User aliases never determine logical identity. Arbitrary callback expressions are not accepted.

- [ ] **Step 4: Define every logical kind now, without fake execution.**

`LogicalOpKind` must enumerate the complete approved surface:

```cpp
enum class LogicalOpKind : uint8_t {
  kVertexScan, kEdgeScan,
  kStateAt, kEventsBetween, kChangesBetween, kHistory,
  kStateOverlaps, kStateThroughout,
  kExpandOut, kExpandIn, kExpandBoth,
  kBindProperty, kFilter, kProject,
  kInnerJoin, kSemiJoin, kAntiJoin, kUnionAll, kDistinct, kSort, kLimit,
  kAggregateRows, kTemporalAggregate, kKHopExpand, kCoexistingShortestPath,
  kEarliestArrival, kLatestDeparture, kFastestDuration,
};
```

Factories whose execution is delivered in later tasks still construct and validate a real plan; they do not return `NotSupported` from construction.

- [ ] **Step 5: Verify type rules, immutability, and public header isolation.**

```bash
cmake --build build/query-debug -j2 --target test_logical_plan
ctest --test-dir build/query-debug --output-on-failure -R 'LogicalPlan|PublicHeaderContract'
```

Expected: all selected tests pass; `Query` and expressions are copyable immutable handles and expose no internal engine type.

- [ ] **Step 6: Commit the logical model.**

```bash
git add include/cedar/query.h include/cedar/query/expression.h \
  include/cedar/query/query.h src/query/logical tests/query/test_logical_plan.cc \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add typed immutable query plans"
```

---

### Task 4: Canonical StateAt Query End to End

**Files:**
- Create: `include/cedar/query/result.h`
- Create: `src/query/query_api.cc`
- Create: `src/query/runtime/query_runtime.h`
- Create: `src/query/runtime/query_runtime.cc`
- Create: `src/query/runtime/canonical_source.h`
- Create: `src/query/runtime/canonical_source.cc`
- Create: `tests/query/test_query_canonical.cc`
- Modify: `include/cedar/database.h`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Modify: `tests/public/test_kernel_interface.cc`

**Interfaces:**
- Consumes: Task 3 logical plans, Task 2 corrected-chain model, existing move-only `Snapshot` scans.
- Produces: `PreparedQuery`, `QueryCursor`, `QueryBatch`, `Database::PrepareQuery`, and a complete canonical `VertexScan + StateAt + Project` execution path.

- [ ] **Step 1: Write a failing public end-to-end query test.**

```cpp
TEST_F(QueryCanonicalTest, StreamsStateAtFromTheConsumedSnapshot) {
  SeedVertexHistory();
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("v");
  auto source = Query::Vertices(vertex, At{ValidTime{15}});
  ASSERT_TRUE(source.ok()) << source.status().ToString();
  auto query = source.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto prepared = database_->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database_->BeginSnapshot({.as_of = CommitSeq{1}});
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto first = cursor.ValueOrDie().Next();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(first.ValueOrDie().has_value());
  EXPECT_EQ(first.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(first.ValueOrDie()->Get<VertexRef>(vertex, 0),
            (VertexRef{PartId{0}, VertexId{1}}));
  EXPECT_FALSE(cursor.ValueOrDie().Next().ValueOrDie().has_value());
}
```

Add static assertions that PreparedQuery is immutable/copyable and QueryCursor is move-only.

- [ ] **Step 2: Run and observe missing Database/query result interfaces.**

```bash
cmake --build build/query-debug -j2 --target test_query_canonical
```

Expected: compilation fails on `PrepareQuery`, `PreparedQuery::Execute`, and `QueryCursor`.

- [ ] **Step 3: Implement owned columnar results and terminal cursor behavior.**

Use:

```cpp
using QueryColumnVector = std::variant<
    std::vector<uint8_t>, std::vector<int32_t>, std::vector<int64_t>,
    std::vector<float>, std::vector<double>, std::vector<uint64_t>,
    std::vector<std::string>, std::vector<VertexRef>, std::vector<EdgeRef>,
    std::vector<ValidTime>, std::vector<ValidDuration>,
    std::vector<CommitSeq>, std::vector<ValidTimeInterval>>;

struct QueryColumn {
  SlotId slot;
  QueryType type;
  QueryColumnVector values;
  std::vector<uint8_t> present;
};
```

`QueryCursor::Next` returns owned, query-allocated buffers, never a RocksDB cache handle. After clean EOS it repeatedly returns `nullopt`; after an error it repeats the same terminal Status.

- [ ] **Step 4: Bind preparation and execution to Database lifecycle.**

Add:

```cpp
StatusOr<PreparedQuery> Database::PrepareQuery(const Query& query) const;
StatusOr<QueryCursor> PreparedQuery::Execute(
    Snapshot snapshot, const Bindings& bindings,
    const QueryOptions& options) const;
```

Preparation captures a weak database implementation reference plus referenced-schema fingerprint. Execute consumes the Snapshot. The canonical source scans visible vertex-state events, groups by FactRef, invokes `ResolveCorrectedBoundaries`, selects the interval containing `At.time`, and emits batches. It performs no projection lookup.

- [ ] **Step 5: Test Snapshot ownership and Close behavior.**

Add tests proving that a Cursor pins the Snapshot until terminal/Close,
moved-from cursors reject calls, a returned QueryBatch survives Cursor
destruction, and a PreparedQuery Execute after Database Close returns
`ShutdownInProgress`. Run one `PreparedQuery` concurrently against two
Snapshots/binding sets and prove the Cursors do not share state. Register an
unrelated property and prove execution remains valid; change a referenced
property epoch/type and require `SchemaMismatch`. Executing a Snapshot below
the durable oldest-readable boundary returns `SnapshotExpired`.

- [ ] **Step 6: Run focused, lifecycle, and public tests.**

```bash
cmake --build build/query-debug -j2 --target test_query_canonical test_kernel_interface
ctest --test-dir build/query-debug --output-on-failure -R 'QueryCanonical|KernelInterface|PublicHeaderContract'
```

Expected: selected tests pass with canonical-only StateAt results.

- [ ] **Step 7: Commit the first working query slice.**

```bash
git add include/cedar/query/result.h include/cedar/database.h \
  src/query/query_api.cc src/query/runtime src/kernel/database_impl.h \
  src/kernel/database.cc tests/query/test_query_canonical.cc \
  tests/public/test_kernel_interface.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: execute canonical state-at queries"
```

---

### Task 5: Canonical Temporal Sources and Property Semantics

**Files:**
- Create: `src/query/runtime/temporal_source.h`
- Create: `src/query/runtime/temporal_source.cc`
- Create: `src/query/runtime/property_binding.h`
- Create: `src/query/runtime/property_binding.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `src/query/logical/logical_plan.cc`
- Modify: `tests/query/test_query_canonical.cc`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: canonical Cursor from Task 4 and corrected-chain operations from Task 2.
- Produces: canonical Events, Changes, History, Overlaps, Throughout, vertex/edge property binding, explicit Missing intervals, and temporal predicate clipping.

- [ ] **Step 1: Add failing bitemporal source tests.**

Construct one property chain with a redundant boundary and a later correction, then assert:

```cpp
EXPECT_EQ(RunEvents(snapshot3, {10, 40}),
          (std::vector<ExpectedEvent>{{10, kPut, 1}, {20, kDelete, 3},
                                      {30, kPut, 2}}));
EXPECT_EQ(RunChanges(snapshot3, {10, 40}),
          (std::vector<ExpectedChange>{{10, Missing(), I64(7)},
                                       {20, I64(7), Missing()},
                                       {30, Missing(), I64(7)}}));
EXPECT_EQ(RunHistory(snapshot3, {10, 40}),
          (std::vector<ExpectedState>{{{10, 20}, I64(7)},
                                      {{30, 40}, I64(7)}}));
```

Add exact boundary tests: Overlaps `[20,30)` returns no row, Throughout `[10,20)` succeeds, and Throughout `[10,21)` fails.

- [ ] **Step 2: Add failing Missing predicate tests.**

```cpp
TEST_F(QueryCanonicalTest, ClipsPropertyPredicatesAndKeepsTwoValuedMissing) {
  EXPECT_EQ(RunPredicate(IsMissing(score), {0, 30}),
            (Intervals{{0, 10}, {20, 30}}));
  EXPECT_TRUE(RunAt(Not(Equal(ValueOf(score), Literal<int64_t>(7))), 5));
  EXPECT_FALSE(RunAt(NotEqual(ValueOf(score), Literal<int64_t>(7)), 5));
}
```

This guards the approved rule that all ordinary comparisons against Missing are false while boolean NOT remains two-valued.

- [ ] **Step 3: Run tests and verify unsupported temporal nodes fail.**

```bash
cmake --build build/query-debug -j2 --target test_query_canonical
ctest --test-dir build/query-debug --output-on-failure -R QueryCanonical
```

Expected: new tests fail because Task 4 executes only StateAt vertex scans.

- [ ] **Step 4: Implement canonical event/state sources.**

`TemporalSource` must request only the relevant FactFamily/PropertyId from `Snapshot::EventColumnarScan`, group rows by FactRef, and use Task 2 semantics. It emits:

```cpp
struct EventRow { FactRef ref; ValidTime valid_from; CommitSeq commit_seq;
                  FactOperation operation; std::optional<Value> value; };
struct ChangeRow { FactRef ref; ValidTime valid_from;
                   std::optional<Value> before; std::optional<Value> after; };
struct StateRow { FactRef ref; ValidTimeInterval effective;
                  std::optional<Value> value; };
```

Events select the winning correction at each boundary; Changes remove corrected boundaries whose before/after state is equal. State scopes clip only after full corrected materialization.

`EventsBetween` always reads authoritative CedarParquet facts in this release;
the planner must not substitute a state/property projection boundary stream for
the event source because that stream is base-generation state, not the full
system-time event interface.

- [ ] **Step 5: Implement property binding and interval clipping.**

At a point, emit a typed value or `present=0`. Over an interval, split at every property boundary. `IsMissing` derives entity-existence minus property-presence; no Missing fact is written. Enforce exact PropertyDefinition entity kind, schema epoch, and physical type during Prepare.

- [ ] **Step 6: Test all scopes against the independent oracle.**

```bash
cmake --build build/query-debug -j2 --target test_query_canonical test_temporal_model
ctest --test-dir build/query-debug --output-on-failure -R 'QueryCanonical|TemporalModel'
```

Expected: all scopes, corrections, clipping, Missing, and invalid/unbounded-budget cases pass.

- [ ] **Step 7: Commit canonical temporal execution.**

```bash
git add src/query/runtime/temporal_source.* src/query/runtime/property_binding.* \
  src/query/runtime/query_runtime.cc src/query/logical/logical_plan.cc \
  tests/query/test_query_canonical.cc CMakeLists.txt
git commit -m "feat: execute canonical temporal queries"
```

---

### Task 6: Vector Expressions, Relational Operators, and Aggregates

**Files:**
- Create: `src/query/runtime/vector_kernels.h`
- Create: `src/query/runtime/vector_kernels.cc`
- Create: `src/query/runtime/relational.h`
- Create: `src/query/runtime/relational.cc`
- Create: `tests/query/test_query_relational.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: typed expressions and QueryBatch columns from Tasks 3-5.
- Produces: vector Filter/Project, UnionAll, Distinct, Sort, Limit, inner/semi/anti joins, row aggregates, temporal aggregate, fragment accounting, and a bounded spill seam used by Task 11.

- [ ] **Step 1: Add failing vector presence and strict-type tests.**

```cpp
TEST(VectorKernelsTest, ComparisonAgainstMissingIsFalse) {
  QueryColumn left = Int64Column({7, 0, 9}, {1, 0, 1});
  QueryColumn right = Int64Column({7, 7, 7}, {1, 1, 1});
  EXPECT_EQ(EvaluateEqual(left, right), (BoolVector{true, false, false}));
  EXPECT_EQ(EvaluateNotEqual(left, right), (BoolVector{false, false, true}));
}
```

Add an explicit cast-overflow case that returns `NumericOverflow`.

- [ ] **Step 2: Add failing temporal relational tests.**

```cpp
TEST(RelationalTest, IntervalJoinEmitsOnlyClippedIntersections) {
  auto result = TemporalInnerJoin(
      Rows({Row{1, {0, 10}}, Row{1, {15, 30}}}),
      Rows({Row{1, {5, 20}}}));
  EXPECT_EQ(result, Rows({Row{1, {5, 10}}, Row{1, {15, 20}}}));
}

TEST(RelationalTest, TemporalAggregateProcessesExitBeforeEntry) {
  auto result = TemporalCount({{0, 5}, {5, 10}});
  EXPECT_EQ(result, (TemporalCounts{{{0, 10}, 1}}));
}
```

Also test anti-join Missing derivation, stable explicit Sort, unspecified unsorted order, and fragment-budget failure.

- [ ] **Step 3: Register and run the failing target.**

```bash
cmake --build build/query-debug -j2 --target test_query_relational
```

Expected: compilation fails because vector and relational modules do not exist.

- [ ] **Step 4: Implement selection-vector kernels.**

Each kernel accepts typed column spans plus an optional input selection and returns a selection or output column. It must check presence before reading a value lane. Strings compare dictionary IDs when both inputs share a dictionary; otherwise compare bytes. Do not materialize rejected rows.

- [ ] **Step 5: Implement exact relational algorithms.**

Provide these physical operations behind one private interface:

```cpp
StatusOr<BatchStream> IndexNestedLoopJoin(JoinInput);
StatusOr<BatchStream> HashJoin(JoinInput, QueryReservation*);
StatusOr<BatchStream> SortMergeJoin(JoinInput, QueryReservation*);
StatusOr<BatchStream> IntervalMergeJoin(TemporalJoinInput, FragmentBudget*);
StatusOr<BatchStream> AggregateRows(AggregateInput);
StatusOr<BatchStream> TemporalAggregate(TemporalAggregateInput,
                                        FragmentBudget*);
```

Hash and sort operators return `NeedsSpill` through the private seam when their reservation cannot grow; Task 11 supplies scratch. They never allocate past the reservation.

- [ ] **Step 6: Wire logical nodes into the pull runtime.**

Use index nested loop below 4,096 estimated rows, in-memory hash for unordered equality, merge for sorted keys/intervals, and the private spill seam for analytical plans. Coalesce adjacent intervals only when all non-time columns are equal.

- [ ] **Step 7: Run focused and canonical regression tests.**

```bash
cmake --build build/query-debug -j2 --target test_query_relational test_query_canonical
ctest --test-dir build/query-debug --output-on-failure -R 'QueryRelational|QueryCanonical'
```

Expected: relational and canonical suites pass with no implicit conversion or interval-boundary regression.

- [ ] **Step 8: Commit vector relational execution.**

```bash
git add src/query/runtime/vector_kernels.* src/query/runtime/relational.* \
  src/query/runtime/query_runtime.cc tests/query/test_query_relational.cc \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add vectorized temporal relational operators"
```

---

### Task 7: Projection Segment Format and Corrected-Boundary Pages

**Files:**
- Create: `src/query/projection/projection_format.h`
- Create: `src/query/projection/projection_format.cc`
- Create: `src/query/projection/projection_compression.h`
- Create: `src/query/projection/projection_compression.cc`
- Create: `tests/query/test_projection_format.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 2 boundaries/state intervals, Cedar CRC32C, pinned LZ4 paths from `cmake/CedarRocksDB.cmake`.
- Produces: version-1 `.cstate`, `.cadj`, `.cprop`, and `.cstats` page encoding with checksums, min/max metadata, corrected-boundary streams, and typed page readers.

- [ ] **Step 1: Write failing golden round-trip and corruption tests.**

```cpp
TEST(ProjectionFormatTest, RoundTripsIntervalsAndLatentBoundaries) {
  ProjectionChain chain;
  chain.intervals = {{{0, 30}, Value::Int64(7)}};
  chain.boundaries = {{0, kPut, Value::Int64(7)},
                      {10, kPut, Value::Int64(7)}};
  auto encoded = EncodeProjectionPage(chain, CompressionCodec::kLz4);
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(DecodeProjectionPage(encoded.ValueOrDie()).ValueOrDie(), chain);
}

TEST(ProjectionFormatTest, RejectsBitFlippedPayload) {
  std::string bytes = EncodeFixture();
  bytes[bytes.size() / 2] ^= 0x40;
  EXPECT_TRUE(DecodeProjectionPage(bytes).status().IsCorruption());
}
```

Add golden-byte tests for every integer width and an unknown-version rejection.

- [ ] **Step 2: Run the target and observe the missing codec.**

```bash
cmake --build build/query-debug -j2 --target test_projection_format
```

Expected: compilation fails because projection format types are absent.

- [ ] **Step 3: Implement a structured version-1 file header.**

Encode fixed-width integers little-endian and length-prefix every variable field:

```text
magic[8] = "CDRPRJ1\0"
format_version:u32 = 1
projection_kind:u8
compression:u8
generation_id:u64
base_seq:u64
part_id:u32
property_id:u16
schema_epoch:u32
entity_min:u64 entity_max_exclusive:u64
valid_from_min:u64 valid_to_kind:u8 valid_to:u64
page_count:u32
header_crc32c:u32
page_directory[]
page_payloads[]
file_crc32c:u32
```

Each page directory entry contains offset, compressed/uncompressed bytes, row count, entity/valid-time min/max, optional edge-type min/max, and payload CRC32C. Decoders validate lengths before arithmetic and cap allocations by caller budget.

- [ ] **Step 4: Implement compact boundary/state columns.**

Use delta-coded entity IDs and valid times, bit-packed operations/presence,
shared typed dictionaries, RLE for repeated values, and a bounded Bloom filter
only for point/adjacency pages where tests prove it reduces reads. Preserve
every corrected boundary even when state intervals coalesce. Use pinned
`lz4.h` through a Cedar projection compression adapter; no query file includes
a RocksDB header.

- [ ] **Step 5: Run format, sanitizer-friendly malformed-input, and diff checks.**

```bash
cmake --build build/query-debug -j2 --target test_projection_format
ctest --test-dir build/query-debug --output-on-failure -R ProjectionFormat
git diff --check
```

Expected: round trips and corruption cases pass without unchecked allocation.

- [ ] **Step 6: Commit the projection codec.**

```bash
git add src/query/projection/projection_format.* \
  src/query/projection/projection_compression.* \
  tests/query/test_projection_format.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: encode Cedar query projection segments"
```

---

### Task 8: Projection Manifest, Atomic Generations, and Builders

**Files:**
- Create: `src/query/projection/projection_manifest.h`
- Create: `src/query/projection/projection_manifest.cc`
- Create: `src/query/projection/projection_store.h`
- Create: `src/query/projection/projection_store.cc`
- Create: `tests/query/test_projection_store.cc`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 7 segment codec, Snapshot columnar scans, Task 2 corrected semantics.
- Produces: `ProjectionManifest`, `CoverageRegion`, `ProjectionGeneration`, `QueryProjectionStore::Build`, `Acquire`, `RetireBefore`, `Quarantine`, and atomic `PROJECTION-CURRENT` publication.

- [ ] **Step 1: Add failing atomic-publication and coverage tests.**

```cpp
TEST_F(ProjectionStoreTest, NeverInfersCoverageFromAnOrphanSegment) {
  WriteValidUnreferencedSegment();
  auto opened = QueryProjectionStore::Open(options_);
  ASSERT_TRUE(opened.ok());
  EXPECT_FALSE(opened.ValueOrDie()->Acquire(request_).has_value());
}

TEST_F(ProjectionStoreTest, OldReaderPinsRetiredGeneration) {
  auto first = PublishGeneration(10);
  auto pin = store_->Acquire(CoverageRequest{.snapshot_seq = CommitSeq{10}});
  PublishGeneration(20);
  EXPECT_TRUE(first.exists());
  pin.reset();
  store_->CollectRetired();
  EXPECT_FALSE(first.exists());
}
```

Add injected failures after segment sync, manifest sync, CURRENT temporary write, CURRENT rename, and directory sync.

- [ ] **Step 2: Run and prove manifest/store behavior is absent.**

```bash
cmake --build build/query-debug -j2 --target test_projection_store
```

Expected: compilation fails on `QueryProjectionStore` and manifest types.

- [ ] **Step 3: Implement coverage as explicit disjoint regions.**

```cpp
struct CoverageRegion {
  ProjectionKind kind;
  PartId part_id;
  std::optional<PropertyId> property_id;
  uint32_t schema_epoch;
  uint64_t entity_min;
  uint64_t entity_max_exclusive;
  ValidTimeInterval valid_time;
  std::vector<SegmentDescriptor> segments;
};
```

Manifest decode rejects overlap, reversed ranges, duplicate segment IDs, checksum mismatch, wrong database identity, and incompatible referenced schema fingerprints.

- [ ] **Step 4: Implement durable derived publication.**

Write segments under verified `.tmp`, fsync, rename, write immutable `manifests/<generation>.cmanifest`, fsync, then atomically replace a checksummed `PROJECTION-CURRENT`. Open accepts only the referenced manifest; a bad CURRENT disables all projections. File deletion occurs only after generation pins release.

- [ ] **Step 5: Build all projection families from one pinned Snapshot.**

The builder emits:

```text
VertexState: corrected vertex-state intervals and boundaries
EdgeOut: source/type/target/EdgeRef plus edge-state intervals and boundaries
EdgeIn: target/type/source/EdgeRef plus edge-state intervals and boundaries
VertexProperty/EdgeProperty: one file family per PropertyId
Statistics: exact page/segment metadata; sketches arrive in Task 16
```

Do not embed endpoint vertex state or properties in adjacency. A build is invisible until its complete manifest publishes.

- [ ] **Step 6: Integrate Open/Close ownership without using the commit path.**

`Database::Impl` owns `QueryProjectionStore` beside, not inside, `FactStore`. Open initializes it only after RocksDB recovery; Close stops new builds and releases it before RocksDB closes. Add test hooks as function callbacks in query options rather than RocksDB sync points.

- [ ] **Step 7: Run projection, canonical, and lifecycle tests.**

```bash
cmake --build build/query-debug -j2 --target test_projection_store test_query_canonical test_kernel_lifecycle
ctest --test-dir build/query-debug --output-on-failure -R 'ProjectionStore|QueryCanonical|KernelLifecycle'
```

Expected: projection publication/reopen/pinning passes and canonical behavior remains unchanged.

- [ ] **Step 8: Commit projection generations.**

```bash
git add src/query/projection/projection_manifest.* \
  src/query/projection/projection_store.* tests/query/test_projection_store.cc \
  src/kernel/database_impl.h src/kernel/database.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: publish Cedar query projection generations"
```

---

### Task 9: Batched Exact-Fact Reads and Contiguous QueryDelta

**Files:**
- Create: `src/query/projection/query_delta.h`
- Create: `src/query/projection/query_delta.cc`
- Create: `tests/query/test_query_delta.cc`
- Modify: `src/storage/facts/fact_store.h`
- Modify: `src/storage/rocks/rocks_adapter.cc`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: durable `SequenceRecord.fact_keys`, Task 8 projection base generations, Task 2 chain merge.
- Produces: `FactStore::ReadSequenceRange`, `FactStore::ReadExactFacts`, `QueryDelta::ObservePublished`, `RepairThrough`, `AcquireThrough`, and projection-plus-delta state reconstruction.

- [ ] **Step 1: Add failing storage batch-read tests.**

```cpp
TEST_F(FactStoreTest, ReadsAContiguousSequenceRangeAndExactFactsInOrder) {
  CommitThreeBatches();
  auto snapshot = store_.BeginSnapshot();
  auto sequences = store_.ReadSequenceRange(snapshot.ValueOrDie(),
                                             CommitSeq{1}, CommitSeq{3});
  ASSERT_TRUE(sequences.ok());
  ASSERT_EQ(sequences.ValueOrDie().size(), 3U);
  auto facts = store_.ReadExactFacts(snapshot.ValueOrDie(),
                                    sequences.ValueOrDie()[1].fact_keys);
  ASSERT_TRUE(facts.ok());
  EXPECT_EQ(facts.ValueOrDie().size(),
            sequences.ValueOrDie()[1].fact_keys.size());
}
```

The test must include an EdgeIdentity fact and preserve requested key order.

- [ ] **Step 2: Add failing QueryDelta continuity and queue-overflow tests.**

```cpp
TEST(QueryDeltaTest, DoesNotAdvanceAcrossAMissingCommit) {
  QueryDelta delta({.base_seq = CommitSeq{10}, .queue_capacity = 1});
  EXPECT_TRUE(delta.ObservePublished(Commit(11)).ok());
  EXPECT_TRUE(delta.ObservePublished(Commit(12)).IsResourceExhausted());
  EXPECT_EQ(delta.indexed_through(), CommitSeq{11});
  EXPECT_EQ(delta.first_missing(), CommitSeq{12});
}
```

Also test correction at an old valid time, a new edge identity in both directions, hard-memory retirement, and snapshot cut `S < visible_seq`.

- [ ] **Step 3: Implement batched private storage access.**

`ReadSequenceRange` performs one ordered meta iterator scan and verifies every sequence is present. `ReadExactFacts` uses RocksDB MultiGet against the existing Snapshot, decodes each exact key/value, rejects missing facts as canonical corruption, and returns caller order. No public header exposes the native batch API.

- [ ] **Step 4: Implement bounded contiguous delta indexing.**

Use immutable commit chunks keyed by CommitSeq and sharded maps keyed by FactRef/source/target. An enqueue overflow records `first_missing`, leaves `indexed_through` unchanged past the gap, and wakes the repair worker. `RepairThrough` reads sequence ranges and exact facts within explicit commit/byte limits.

Configure the production contract explicitly: 256 MiB soft memory, 512 MiB
hard memory, 262,144 commits maximum lag, 30-second target projection lag, and
per-interactive-query synchronous repair capped at 4,096 commits and 32 MiB.
At the hard memory/lag bound, stop claiming mergeability, retain existing
reader pins, fall back canonically, and schedule a new generation.

- [ ] **Step 5: Publish commit descriptors without delaying writes.**

Before moving `StoreCommitBatch` into the append epoch, create a shared immutable descriptor containing mutations and edge identities. After each successful visible publication, enqueue the descriptor. If enqueue rejects, record the missing sequence and let commit completion proceed unchanged. Never perform exact-fact reads on the publisher thread.

- [ ] **Step 6: Merge base boundaries and delta by Snapshot cut.**

For each dirty chain, merge base corrected boundaries with delta events whose commit is `<= S`, select the greatest commit at each valid time, and rematerialize maximal state. If continuity or a chain read fails, mark only that coverage region for canonical replacement.

- [ ] **Step 7: Run QueryDelta, commit, recovery, and canonical regressions.**

```bash
cmake --build build/query-debug -j2 --target test_query_delta test_fact_store_commit test_kernel_commit test_query_canonical
ctest --test-dir build/query-debug --output-on-failure -R 'QueryDelta|FactStoreCommit|KernelCommit|QueryCanonical'
```

Expected: all tests pass; existing WAL count/sync/publication tests remain unchanged.

- [ ] **Step 8: Commit QueryDelta.**

```bash
git add src/query/projection/query_delta.* tests/query/test_query_delta.cc \
  src/storage/facts/fact_store.h src/storage/rocks/rocks_adapter.cc \
  src/kernel/database_impl.h src/kernel/database.cc \
  src/query/runtime/query_runtime.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: merge contiguous query deltas"
```

---

### Task 10: Late-Binding Planner, Coverage Split, and Explain

**Files:**
- Create: `src/query/planner/query_planner.h`
- Create: `src/query/planner/query_planner.cc`
- Create: `tests/query/test_query_planner.cc`
- Modify: `include/cedar/query/query.h`
- Modify: `include/cedar/query/result.h`
- Modify: `src/query/query_api.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 3 logical plans, Task 8 coverage manifests, Task 9 Delta watermarks, Task 6 physical relational operations.
- Produces: `PhysicalPlan`, `PhysicalOpKind`, `CoverageSlice`, `QueryPlanner::Bind`, `PreparedQuery::ExplainLogical`, and `ExplainPhysical`.

- [ ] **Step 1: Add failing coverage partition tests using a fake catalog view.**

```cpp
TEST(QueryPlannerTest, SplitsCoverageWithoutOverlapOrGap) {
  FakeProjectionCatalog catalog({Covered(0, 100), Covered(200, 300)});
  auto plan = BindScan(catalog, Requested(0, 300), SnapshotSeq(25));
  ASSERT_TRUE(plan.ok());
  EXPECT_EQ(plan.ValueOrDie().slices(),
            (std::vector<CoverageSlice>{
                Projection(0, 100), Canonical(100, 200), Projection(200, 300)}));
}
```

Add rejection tests for overlapping manifest regions and for a projection with `base_seq > snapshot_seq`.

- [ ] **Step 2: Add failing lane and pushdown tests.**

Assert that 100 candidate point rows choose interactive, 100,000 rows choose analytical, global Sort chooses analytical, an interactive prefix plus broad aggregate creates one LaneExchange, edge type reaches adjacency seek, and unused property columns are absent from the scan projection.

- [ ] **Step 3: Run the planner target and observe missing physical types.**

```bash
cmake --build build/query-debug -j2 --target test_query_planner
```

Expected: compilation fails because planner/physical-plan interfaces are absent.

- [ ] **Step 4: Implement normalization and semantic rewrites.**

The rewrite pipeline order is fixed:

```text
validate bindings/schema/scope
normalize temporal nodes and Both
infer property presence requirements
push temporal/entity/type/property predicates
prune columns
remove redundant existence checks
derive required ordering
```

Rewrite Both into incoming/outgoing union plus self-loop deduplication. Push
PartID/entity/type/time/property predicates, bind expensive properties only
after cheaper candidate reduction, rewrite `IsMissing` to temporal anti-join,
and push Limit only when explicit ordering semantics remain unchanged. Never
rewrite `Not(Equal(optional,...))` to NotEqual. Reject unbounded History,
global Sort, or broad path work without an analytical budget.

- [ ] **Step 5: Implement coverage-aware access binding.**

`QueryPlanner::Bind` takes immutable views:

```cpp
struct PlanningContext {
  CommitSeq snapshot_seq;
  const ProjectionCatalogView& projections;
  const QueryDeltaView& delta;
  const QueryStatisticsView& statistics;
  QueryOptions options;
};
StatusOr<PhysicalPlan> QueryPlanner::Bind(
    const LogicalPlanNode&, const PlanningContext&) const;
```

Projection slices require matching database identity, referenced schema, complete key/time coverage, `base_seq <= S`, and contiguous Delta `(base,S]`. Otherwise emit a disjoint canonical slice or a bounded tail-repair operation.

- [ ] **Step 6: Implement cost and lane selection.**

Cost fields are rows, pages, physical/decoded bytes, random reads, dirty chains, interval fragments, fanout, memory, spill bytes, and first-result latency. Auto uses the approved initial `4,096 rows / 4 hops / 8 MiB` interactive heuristic. Missing statistics increase uncertainty and select a conservative plan.

The runtime may rebind/adapt only at an explicit `LaneExchange`; add a planner
test that a misestimated operator without an exchange retains its chosen
physical algorithm or fails its hard budget instead of silently switching.

- [ ] **Step 7: Add structured Explain.**

```cpp
struct QueryPlanNodeDescription {
  LogicalOpKind logical;
  PhysicalOpKind physical;
  QueryExecutionMode lane;
  QueryCostEstimate estimate;
  std::optional<uint64_t> projection_generation;
  std::optional<CommitSeq> projection_base;
  std::vector<CoverageSliceDescription> coverage;
  std::vector<QueryPlanNodeDescription> children;
};
```

Logical Explain requires no Snapshot. Physical Explain borrows a Snapshot and reports projection/delta/fallback, pushdowns, algorithm, spill permission, and confidence without executing.

- [ ] **Step 8: Execute the physical scan choices and test equality.**

Wire ProjectionScan + DeltaMerge and CanonicalScan slices into QueryRuntime, concatenate disjoint regions, and compare normalized output with canonical-only execution.

Run:

```bash
cmake --build build/query-debug -j2 --target test_query_planner test_query_canonical
ctest --test-dir build/query-debug --output-on-failure -R 'QueryPlanner|QueryCanonical'
```

Expected: all tests pass and Explain exposes every fallback range.

- [ ] **Step 9: Commit the planner.**

```bash
git add src/query/planner include/cedar/query/query.h \
  include/cedar/query/result.h src/query/query_api.cc \
  src/query/runtime/query_runtime.cc tests/query/test_query_planner.cc \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: bind coverage-aware query plans"
```

---

### Task 11: Query Resource Pool, Pull Backpressure, and Scratch Spill

**Files:**
- Create: `src/query/resource/query_resource_pool.h`
- Create: `src/query/resource/query_resource_pool.cc`
- Create: `src/query/resource/query_scratch.h`
- Create: `src/query/resource/query_scratch.cc`
- Create: `tests/query/test_query_resources.cc`
- Modify: `include/cedar/database.h`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `src/query/runtime/query_runtime.h`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `src/query/runtime/relational.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 6 `NeedsSpill`, Task 10 lanes/costs, current `wal_sync_critical` and maintenance controller state.
- Produces: `QueryRuntimeOptions`, `QueryResourcePool`, `QueryReservation`, `IoPermit`, bounded analytical workers, QueryBatch leases, and `QueryScratch` runs/partitions.

- [ ] **Step 1: Add failing budget and backpressure tests.**

```cpp
TEST(QueryResourcePoolTest, NeverAllocatesBeyondReservation) {
  QueryResourcePool pool(OptionsWithMemory(1024));
  auto query = pool.Admit(InteractiveBudget(768));
  ASSERT_TRUE(query.ok());
  EXPECT_TRUE(query.ValueOrDie().ReserveMemory(512).ok());
  EXPECT_TRUE(query.ValueOrDie().ReserveMemory(513).IsResourceExhausted());
}

TEST_F(QueryResourceTest, HeldBatchAppliesCursorBackpressure) {
  auto cursor = ExecuteTwoBatchesWithOneBatchBudget();
  auto held = cursor.Next().ConsumeValueOrDie().value();
  EXPECT_TRUE(cursor.Next().status().IsResourceExhausted());
  held = QueryBatch{};
  EXPECT_TRUE(cursor.Next().ok());
}
```

Add overflow-safe accounting, cancellation while queued, deadline, and exact once-release tests.

Exercise every Task 1 dimension independently: memory, scratch, read bytes,
prefetch bytes, decoded rows, output rows/bytes, interval fragments, graph
labels, visited vertices, CPU microseconds, wall deadline, parallelism, hops,
and retained output batches. Each exhaustion returns `ResourceExhausted` with
the dimension name except wall deadline, which returns `DeadlineExceeded`.

- [ ] **Step 2: Add failing scratch safety and cleanup tests.**

Test lazy creation under `<db>/query/scratch/<instance>/<query>`, hard disk/free-space rejection, block checksum failure scoped to one query, normal/cancel/error cleanup, old-instance cleanup after lock, and refusal to delete an invalid child name or symlink escape.

- [ ] **Step 3: Run the target and observe missing resource modules.**

```bash
cmake --build build/query-debug -j2 --target test_query_resources
```

Expected: compilation fails because QueryResourcePool and QueryScratch do not exist.

- [ ] **Step 4: Add exact database-level query options and Open validation.**

```cpp
struct QueryRuntimeOptions {
  uint32_t query_workers = 4;
  uint32_t reserved_interactive_workers = 1;
  uint64_t query_memory_bytes = 256ULL << 20;
  uint64_t projection_cache_bytes = 256ULL << 20;
  uint64_t query_delta_bytes = 256ULL << 20;
  uint64_t scratch_disk_bytes = 4ULL << 30;
  uint64_t scratch_free_space_reserve_bytes = 2ULL << 30;
  uint64_t read_bytes_per_second = 0;
  uint64_t scratch_bytes_per_second = 0;
  uint64_t max_prefetch_bytes = 8ULL << 20;
};
```

Production Open checks query allocations plus WBM/cache against `production.memory_budget_bytes`. Zero rate means no configured rate cap, not unlimited memory or concurrency.

- [ ] **Step 5: Implement hierarchical reservations and work classes.**

P0 durability/recovery/emergency flush preempts new bulk I/O; P1 interactive and P2 necessary maintenance each retain bounded minimum shares; P3 analytical and P4 projection consume surplus. When `wal_sync_critical` is true, new analytical prefetch/spill/projection permits wait or yield. Already issued OS I/O is accounted but not falsely cancelled.

- [ ] **Step 6: Implement verified scratch files and connect spill.**

Scratch writes immutable blocks:

```text
magic "CDRSCR1\0" | query_id | block_length | payload | crc32c
```

No fsync and no manifest. Hash join partitions and sort runs use `QueryScratch::WriteRun`, close the file before reading, and reserve physical bytes before each write. Interactive plans reject `NeedsSpill` with `ResourceExhausted`.

- [ ] **Step 7: Make Cursor cancellation and Close idempotent.**

`Cancel` is lock-free/idempotent; `Next` and `Close` remain single-consumer. Check tokens at page, batch, partition, run, and frontier boundaries. QueryBatch public buffers never hold engine cache handles; zero-copy projection batches retain only reference-counted Cedar cache leases.

Interactive output starts at at most 256 rows per non-empty batch; analytical
output starts at at most 4,096 rows. Adapt only within the retained-batch,
memory, decoded/output-byte, prefetch, CPU, deadline, visited-vertex, label, and
fragment reservations. Parallel stages use bounded queues and reserve bytes
before allocating or issuing I/O; empty batches are consumed internally.

- [ ] **Step 8: Run resource, relational, and lifecycle tests.**

```bash
cmake --build build/query-debug -j2 --target test_query_resources test_query_relational test_kernel_lifecycle
ctest --test-dir build/query-debug --output-on-failure -R 'QueryResource|QueryRelational|KernelLifecycle'
```

Expected: tests pass with bounded allocations, spill, cancellation, and cleanup.

- [ ] **Step 9: Commit Cedar-owned query resources.**

```bash
git add src/query/resource tests/query/test_query_resources.cc \
  include/cedar/database.h src/kernel/database_impl.h src/kernel/database.cc \
  src/query/runtime/query_runtime.* src/query/runtime/relational.cc \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: bound Cedar query resources and spill"
```

---

### Task 12: Temporal Adjacency Expansion and K-Hop

**Files:**
- Create: `src/query/runtime/graph_frontier.h`
- Create: `src/query/runtime/graph_frontier.cc`
- Create: `tests/query/test_temporal_expand.cc`
- Modify: `include/cedar/query/query.h`
- Modify: `include/cedar/query/result.h`
- Modify: `src/query/planner/query_planner.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: EdgeOut/EdgeIn projections, QueryDelta, canonical fallback, Task 2 interval intersection, Task 11 graph budgets.
- Produces: `ExpandOut`, `ExpandIn`, `ExpandBoth`, `KHopExpand`, `TemporalTraversal`, point BFS, and batched endpoint/property gather.

- [ ] **Step 1: Add failing edge-visibility and direction tests.**

Seed a cross-partition edge with independent source, edge, and target gaps. Assert output is exactly:

```cpp
TemporalTraversal{
  .source = VertexRef{PartId{1}, VertexId{10}},
  .edge = EdgeRef{PartId{1}, EdgeId{90}},
  .target = VertexRef{PartId{2}, VertexId{20}},
  .edge_type = 7,
  .effective = ValidTimeInterval{ValidTime{30}, ValidTime{40}},
};
```

Test incoming/outgoing orientation, edge-type pushdown, repeated disjoint periods, both-direction self-loop once, multi-edge preservation, and edge-property clipping.

- [ ] **Step 2: Add failing projection/delta/fallback equality tests.**

Run the same Expand query with a base generation, a new edge in Delta, a target-state correction in Delta, and an adjacency coverage hole. Normalize by `(source, edge, target, interval)` and compare each path with canonical-only output.

- [ ] **Step 3: Add failing point and interval k-hop tests.**

Assert point BFS returns each vertex at minimum depth, interval k-hop carries maximal common intervals, max_hops is enforced, and no all-path duplicates appear.

- [ ] **Step 4: Run the target and observe missing graph operators.**

```bash
cmake --build build/query-debug -j2 --target test_temporal_expand
```

Expected: new tests fail because Expand and KHop have no physical execution.

- [ ] **Step 5: Implement batched temporal expansion.**

Seek adjacency by source/target and optional edge type, overlay Delta candidates, batch-read endpoint state, then intersect edge/source/target/request intervals. Projection absence in an analytical plan scans authoritative EdgeIdentity; an interactive plan that cannot prove its canonical scan fits budget returns `ResourceExhausted` instead of hiding a full scan.

- [ ] **Step 6: Implement bounded k-hop frontier execution.**

Point labels are `(VertexRef, depth, predecessor)`. Interval labels add `effective`. Deduplicate point reachability at minimum depth; for interval reachability coalesce equal endpoint/depth intervals after each layer. Store predecessor IDs in an arena and materialize an optional witness only at output.

- [ ] **Step 7: Verify physical-work bounds with counters.**

On a fixture with one selected degree-10 vertex inside a 100,000-edge graph, assert adjacency pages/candidates scale with the selected posting and do not equal total edges. StateAt must not issue a full graph scan.

- [ ] **Step 8: Run focused, planner, and Snapshot regressions.**

```bash
cmake --build build/query-debug -j2 --target test_temporal_expand test_query_planner test_kernel_snapshot
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalExpand|QueryPlanner|KernelSnapshot'
```

Expected: traversal semantics and physical bounds pass.

- [ ] **Step 9: Commit adjacency and k-hop.**

```bash
git add src/query/runtime/graph_frontier.* tests/query/test_temporal_expand.cc \
  include/cedar/query/query.h include/cedar/query/result.h \
  src/query/planner/query_planner.cc src/query/runtime/query_runtime.cc \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add temporal adjacency and k-hop queries"
```

---

### Task 13: Coexisting Shortest Paths

**Files:**
- Create: `tests/query/test_coexisting_path.cc`
- Modify: `src/query/runtime/graph_frontier.h`
- Modify: `src/query/runtime/graph_frontier.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `include/cedar/query/result.h`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 12 temporal traversal stream and Task 11 budgets.
- Produces: coexisting BFS labels, interval dominance, deterministic `PathValue`, and `CoexistingShortestPath` execution.

- [ ] **Step 1: Add failing coexistence and dominance tests.**

```cpp
TEST_F(CoexistingPathTest, RejectsAPathWithoutACommonInstant) {
  // a->b exists [0,10), b->c exists [10,20): touching is not coexistence.
  EXPECT_TRUE(RunPath(a, c).empty());
}

TEST_F(CoexistingPathTest, KeepsDisjointMaximalWitnessIntervals) {
  EXPECT_EQ(RunPath(a, c),
            (ExpectedPaths{{{0, 5}, {edge1, edge2}},
                            {{20, 30}, {edge3, edge4}}}));
}
```

Add a case where a shallower superset interval dominates a deeper/subset label and a case where disjoint labels must both survive.

- [ ] **Step 2: Add failing deterministic witness and budget tests.**

Two equal-hop paths must choose objective, hop count, then lexicographic EdgeRef sequence. Exceeding graph-label or interval-fragment budget returns `ResourceExhausted`, not a partial CleanEnd.

- [ ] **Step 3: Run and prove the path executor is absent.**

```bash
cmake --build build/query-debug -j2 --target test_coexisting_path
```

Expected: tests fail because CoexistingShortestPath is not dispatched.

- [ ] **Step 4: Implement interval labels and dominance.**

```cpp
struct CoexistingLabel {
  VertexRef vertex;
  ValidTimeInterval common;
  uint32_t depth;
  uint64_t predecessor_label;
  EdgeRef incoming_edge;
};
```

For one vertex, a label dominates another only when its depth is no greater and its interval contains the other. Expand layer by layer, intersect before enqueue, coalesce compatible interval results, and charge every surviving label/fragment.

- [ ] **Step 5: Materialize nested PathValue only at targets.**

`PathValue` contains ordered vertices, ordered edges, and its common effective
interval. Public `PathColumn` stores row offsets plus flat nested vertex, edge,
and interval vectors; no row owns a vector-of-vectors. Reconstruct through
predecessor IDs only for selected targets. Do not copy vectors into frontier
labels and do not expose an all-path mode.

- [ ] **Step 6: Compare with exhaustive small-graph enumeration.**

Use the independent oracle to enumerate simple paths up to the same hop cap for graphs of at most eight vertices, then compare shortest hop, maximal intervals, and witness tie-breaking for at least 200 deterministic seeds.

- [ ] **Step 7: Run path and expansion suites.**

```bash
cmake --build build/query-debug -j2 --target test_coexisting_path test_temporal_expand
ctest --test-dir build/query-debug --output-on-failure -R 'CoexistingPath|TemporalExpand'
```

Expected: all tests pass with bounded labels and no touching-interval false positive.

- [ ] **Step 8: Commit coexisting paths.**

```bash
git add tests/query/test_coexisting_path.cc src/query/runtime/graph_frontier.* \
  src/query/runtime/query_runtime.cc include/cedar/query/result.h \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add coexisting shortest paths"
```

---

### Task 14: Earliest Arrival, Latest Departure, and Fastest Journey

**Files:**
- Create: `src/query/runtime/journey.h`
- Create: `src/query/runtime/journey.cc`
- Create: `tests/query/test_temporal_journey.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `include/cedar/query/query.h`
- Modify: `include/cedar/query/result.h`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 12 traversal access, `ValidDuration`, Task 11 label/deadline budgets.
- Produces: FIFO validation, earliest-arrival Dijkstra, reverse latest-departure search, fastest-duration Pareto labels, and nested `JourneyValue`.

- [ ] **Step 1: Add failing wait and duration validity tests.**

Test that waiting succeeds only while the vertex remains effective on `[arrival,next_departure)`, zero duration still requires instantaneous visibility, positive duration must fit strictly before the exclusive upper bound, missing duration disables an edge, and addition overflow returns `NumericOverflow`.

- [ ] **Step 2: Add failing objective fixtures.**

```cpp
TEST_F(TemporalJourneyTest, DistinguishesAllThreeObjectives) {
  EXPECT_EQ(EarliestArrival(a, d, 0).arrival, ValidTime{12});
  EXPECT_EQ(LatestDeparture(a, d, 30).departure, ValidTime{18});
  EXPECT_EQ(FastestDuration(a, d, {0, 30}).duration.value, 5U);
}
```

Build the fixture so each objective chooses a different witness. Add deterministic EdgeRef tie-breaking.

- [ ] **Step 3: Add failing non-FIFO and label-bound tests.**

A duration expression whose later departure can arrive earlier fails Prepare with `NotSupported`. A fastest-duration search exceeding its Pareto-label budget returns `ResourceExhausted` and an incomplete stream.

- [ ] **Step 4: Run and observe missing journey execution.**

```bash
cmake --build build/query-debug -j2 --target test_temporal_journey
```

Expected: tests fail because journey physical operators are absent.

- [ ] **Step 5: Implement traversal feasibility and FIFO checks.**

```cpp
struct JourneyTraversal {
  TemporalTraversal traversal;
  ValidTime departure;
  ValidTime arrival;
};
```

Validate duration type and non-negativity during Prepare. Segment a time-varying property into constant-duration intervals and prove FIFO per segment. Require source/edge/target validity through traversal and target visibility at arrival.

- [ ] **Step 6: Implement the three searches.**

EarliestArrival uses a min-arrival heap and one best FIFO arrival per vertex. LatestDeparture traverses EdgeIn in reverse from the arrival deadline. FastestDuration retains non-dominated `(departure,arrival)` labels; label A dominates B when A departs no earlier and arrives no later, with one strict inequality.

- [ ] **Step 7: Materialize and verify JourneyValue.**

`JourneyValue` contains ordered vertices, edges, departure times, arrival
times, initial departure, final arrival, and objective value. Public
`JourneyColumn` uses row offsets and flat nested vertex, edge, departure, and
arrival vectors. Reconstruct from predecessor IDs only at output. Compare all
three algorithms with exhaustive bounded journey enumeration for deterministic
graphs/time ranges.

- [ ] **Step 8: Run journey, path, and overflow regressions.**

```bash
cmake --build build/query-debug -j2 --target test_temporal_journey test_coexisting_path test_query_types
ctest --test-dir build/query-debug --output-on-failure -R 'TemporalJourney|CoexistingPath|QueryTypes'
```

Expected: all objective, wait, FIFO, Missing, overflow, and budget tests pass.

- [ ] **Step 9: Commit temporal journeys.**

```bash
git add src/query/runtime/journey.* tests/query/test_temporal_journey.cc \
  src/query/runtime/query_runtime.cc include/cedar/query/query.h \
  include/cedar/query/result.h CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add temporal journey queries"
```

---

### Task 15: Query Lifecycle, Projection Retirement, and Recovery

**Files:**
- Create: `tests/query/test_query_lifecycle.cc`
- Create: `tests/recovery/test_query_crash_matrix.cc`
- Modify: `include/cedar/query/result.h`
- Modify: `include/cedar/database.h`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `src/query/query_api.cc`
- Modify: `src/query/runtime/query_runtime.h`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `src/query/projection/projection_manifest.cc`
- Modify: `src/query/projection/projection_store.h`
- Modify: `src/query/projection/projection_store.cc`
- Modify: `src/query/projection/query_delta.cc`
- Modify: `src/query/resource/query_scratch.cc`
- Modify: `src/storage/facts/fact_store.h`
- Modify: `src/storage/facts/vacuum.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Tasks 4, 7-11 Cursor, projection generation, QueryDelta, resource, scratch, and existing `Database::Vacuum` contracts.
- Produces: `QueryCursorState`, terminal completeness, ordered query shutdown, query Snapshot pins, `RetireBefore`, derived-corruption fallback/quarantine, projection-disabled Open, and deterministic query crash fault points.

- [ ] **Step 1: Add failing Cursor terminal-state tests.**

Expose only state needed by callers:

```cpp
enum class QueryCursorState : uint8_t {
  kRunning,
  kCleanEnd,
  kCancelled,
  kFailed,
};

struct QueryTerminalInfo {
  QueryCursorState state = QueryCursorState::kRunning;
  bool complete = false;
  Status status;
};
```

Test clean EOS idempotence, explicit cancellation, deadline, resource failure,
`Close` after each terminal state, destructor cancellation, and that consumed
prefix batches remain readable but `complete == false` after every non-clean
terminal. `Cancel` must return without waiting for a blocked read callback.

- [ ] **Step 2: Add failing database shutdown-order and lifetime tests.**

Install stage observers and assert this exact partial order:

```text
admission_closed
query_cancel_requested
query_tasks_joined
accepted_commits_drained
query_delta_stopped
projection_builders_stopped
maintenance_joined
scratch_cleaned
rocksdb_closed
```

Add a running analytical query, a queued projection build, an accepted commit,
an owned `QueryBatch`, and a copied `PreparedQuery`. `Database::Close` must
cancel and join bounded work before closing RocksDB; the batch remains readable,
while later Prepare/Execute/Next calls return `ShutdownInProgress`.

- [ ] **Step 3: Add failing Vacuum pin and generation-retirement tests.**

```cpp
TEST_F(QueryLifecycleTest, VacuumDoesNotCancelAnOlderQuery) {
  auto cursor = ExecuteAtSnapshot(CommitSeq{10});
  EXPECT_TRUE(database_->Vacuum(CommitSeq{11}).IsSnapshotPinned());
  EXPECT_EQ(cursor.Next().ValueOrDie()->row_count(), 1U);
  ASSERT_TRUE(cursor.Close().ok());
  EXPECT_TRUE(database_->Vacuum(CommitSeq{11}).ok());
}

TEST_F(QueryLifecycleTest, VacuumRetiresOldBaseForNewReadersOnly) {
  auto old_reader = AcquireGeneration(CommitSeq{10});
  ASSERT_TRUE(database_->Vacuum(CommitSeq{11}).ok());
  EXPECT_TRUE(old_reader.ReadPage(0).ok());
  EXPECT_FALSE(projections_->Acquire(CommitSeq{10}).has_value());
  EXPECT_TRUE(RunCanonicalWhileRebuildPending().ok());
}
```

Register each query Snapshot with the same oldest-readable pin registry used by
normal Snapshots. `Vacuum(B)` never trims valid time and never cancels readers.

- [ ] **Step 4: Run the lifecycle target and prove the contracts are absent.**

```bash
cmake --build build/query-debug -j2 --target test_query_lifecycle
```

Expected: compilation or assertions fail on terminal completeness, shutdown
ordering, query pins, and projection retirement.

- [ ] **Step 5: Implement one idempotent lifecycle state machine.**

Use atomic cancellation plus a mutex-protected terminal transition:

```cpp
class QueryExecutionState {
 public:
  void RequestCancel();
  bool cancelled() const;
  Status FinishClean();
  Status FinishFailed(Status status);
  QueryTerminalInfo terminal_info() const;
  Status Close();
};
```

Only `kRunning -> terminal` is legal. A failed/cancelled Cursor repeats its
first terminal Status; a clean Cursor repeats EOS. Release the engine Snapshot
and scratch exactly once after submitted tasks acknowledge cancellation.

- [ ] **Step 6: Integrate ordered Database Close and projection retirement.**

`Database::Impl` closes all admissions under the lifecycle mutex, snapshots
the active query registry, requests cancellation without holding that mutex,
joins query tasks, drains accepted commits, stops Delta/builders, stops Cedar
maintenance, cleans the current scratch instance, then closes the store. Add:

```cpp
Status QueryProjectionStore::RetireBefore(CommitSeq oldest_readable);
bool QueryProjectionStore::IsUsableBase(CommitSeq base) const;
```

New readers reject retired bases. Pinned `shared_ptr<const
ProjectionGeneration>` objects keep files alive; unlink begins only after the
last reader lease and cache lease release.

- [ ] **Step 7: Implement projection corruption fallback and canonical failure.**

On projection header/page/checksum failure, mark the exact region unavailable,
charge authoritative fallback against the current query budget, quarantine the
file after readers release, and enqueue a Cedar P4 rebuild. If fallback cannot
fit, return `ResourceExhausted` with the corrupt coverage in diagnostics.
Canonical CedarParquet/metadata corruption returns `Corruption` and moves the
Database into its existing recovery-required state; never quarantine or skip
authoritative facts.

- [ ] **Step 8: Add deterministic Open and crash-recovery tests.**

The subprocess test executable accepts:

```text
--query-crash-phase=segment_sync|manifest_sync|current_replace|delta_enqueue|scratch_write
--query-db=<absolute-test-directory>
--query-ready-fd=<descriptor>
```

For every phase, the parent waits for the ready byte, sends `SIGKILL`, reopens,
and compares canonical results. Assert old verified scratch and unpublished
`.tmp` segments are removed, a corrupt `PROJECTION-CURRENT` disables all
projections instead of selecting a manifest, identity/schema/base mismatches
disable that generation, and QueryDelta is rebuilt contiguously to visible
sequence before projection reads resume.

Open must finish RocksDB WAL recovery and validate Cedar visible/oldest-readable
watermarks before touching query manifests or reconstructing Delta. Add an
observer assertion for that order so a derived file can never influence
authoritative recovery.

- [ ] **Step 9: Run focused lifecycle and recovery regressions.**

```bash
cmake --build build/query-debug -j2 --target test_query_lifecycle test_query_crash_matrix test_vacuum test_kernel_lifecycle
ctest --test-dir build/query-debug --output-on-failure -R 'QueryLifecycle|QueryCrashMatrix|Vacuum|KernelLifecycle'
```

Expected: all terminal, Close, Vacuum, corruption, kill/reopen, and cleanup
tests pass without stale projection reads or leaked Snapshot pins.

- [ ] **Step 10: Commit lifecycle and recovery.**

```bash
git add tests/query/test_query_lifecycle.cc tests/recovery/test_query_crash_matrix.cc \
  include/cedar/query/result.h include/cedar/database.h \
  src/kernel/database_impl.h src/kernel/database.cc src/query/query_api.cc \
  src/query/runtime/query_runtime.* src/query/projection \
  src/query/resource/query_scratch.cc src/storage/facts/fact_store.h \
  src/storage/facts/vacuum.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: recover and retire query generations safely"
```

---

### Task 16: Query Statistics, Profiles, Metrics, and File Inspection

**Files:**
- Create: `src/query/observability/query_metrics.h`
- Create: `src/query/observability/query_metrics.cc`
- Create: `tests/query/test_query_observability.cc`
- Modify: `include/cedar/query/query.h`
- Modify: `include/cedar/query/result.h`
- Modify: `include/cedar/database.h`
- Modify: `include/cedar/storage_files.h`
- Modify: `src/kernel/database_impl.h`
- Modify: `src/kernel/database.cc`
- Modify: `src/query/query_api.cc`
- Modify: `src/query/planner/query_planner.cc`
- Modify: `src/query/runtime/query_runtime.cc`
- Modify: `src/query/projection/projection_format.h`
- Modify: `src/query/projection/projection_format.cc`
- Modify: `src/query/projection/projection_store.cc`
- Modify: `src/storage/rocks/storage_file_inspection.cc`
- Modify: `tests/tools/test_cedar_files.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 10 structured Explain, Task 11 accounting, Task 15 generation lifecycle, and current `InspectStorageFiles`.
- Produces: generation-bound `.cstats`, `RefreshQueryStatistics`, `QueryProfile`, bounded-label `QueryMetrics`, and inspection of every Cedar query file role.

- [ ] **Step 1: Add failing exact/approximate statistics tests.**

Define the immutable generation payload:

```cpp
struct QueryColumnStatistics {
  uint64_t rows = 0;
  uint64_t pages = 0;
  uint64_t bytes = 0;
  uint64_t interval_count = 0;
  uint64_t edge_count = 0;
  std::optional<EntityRange> entity_range;
  std::optional<ValidTimeInterval> valid_time_range;
  HllSketch distinct;
  std::vector<HistogramBucket> histogram;  // at most 128
  std::vector<TopValue> top_values;        // at most 64
  QuantileSummary fanout;
  QuantileSummary interval_length;
};
```

Define every supporting type in `query_metrics.h`:

```cpp
struct EntityRange {
  uint64_t min = 0;
  uint64_t max_exclusive = 0;
};
struct HllSketch {
  uint8_t precision = 14;
  std::vector<uint8_t> registers;
};
struct HistogramBucket {
  Value upper_bound;
  uint64_t cumulative_count = 0;
};
struct TopValue {
  Value value;
  uint64_t estimated_count = 0;
};
struct QuantilePoint {
  double quantile = 0;
  uint64_t value = 0;
};
using QuantileSummary = std::vector<QuantilePoint>;
```

Build statistics with a projection generation, reopen them, and assert exact
metadata equality plus bounded sketch sizes. A stale database/schema/generation
identity or checksum must make statistics unavailable, never invalidate facts
or the matching projection.

- [ ] **Step 2: Add failing planner uncertainty and refresh tests.**

`Database::RefreshQueryStatistics()` submits Cedar P4 work and returns a
move-only handle:

```cpp
class QueryMaintenanceHandle {
 public:
  QueryMaintenanceHandle(QueryMaintenanceHandle&&) noexcept;
  void Cancel();
  Status Await();
};
StatusOr<QueryMaintenanceHandle> Database::RefreshQueryStatistics();
```

Assert commits never update HLL/histograms, high QueryDelta dirty-chain ratio
lowers estimate confidence, and absent/untrusted stats select a conservative
plan without changing results.

- [ ] **Step 3: Add failing QueryProfile and global-metric tests.**

```cpp
struct QueryOperatorProfile {
  uint32_t operator_id = 0;
  uint64_t rows = 0;
  uint64_t batches = 0;
  uint64_t cpu_us = 0;
  uint64_t wall_us = 0;
  uint64_t queue_us = 0;
  uint64_t first_result_us = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t pages = 0;
  uint64_t delta_repairs = 0;
  uint64_t interval_fragments = 0;
  uint64_t spill_bytes = 0;
  uint64_t frontier_labels = 0;
};

struct QueryProfile {
  std::vector<QueryOperatorProfile> operators;
  QueryTerminalInfo terminal;
};
```

With `capture_profile=false`, assert no per-row clock call through a test clock.
With it enabled, compare totals with actual batches and terminal completeness.
Global metrics may label only bounded enums such as operator, lane, terminal,
fallback reason, and health; reject registration of QueryId, query text,
PropertyId, parameter, or user value labels.

- [ ] **Step 4: Run observability tests and observe missing interfaces.**

```bash
cmake --build build/query-debug -j2 --target test_query_observability test_cedar_files
```

Expected: compilation fails on statistics refresh/profile/metric types and
inspection assertions fail for Cedar query files.

- [ ] **Step 5: Encode generation-bound `.cstats` and feed planning.**

Use the common projection framing and CRC32C. The file header includes database
identity, schema fingerprint, projection generation, base sequence, coverage,
format version, payload length, and checksum. Publish `.cstats` in the same
projection manifest transaction as its generation; refresh builds a new
derived generation and never writes the RocksDB commit batch.

- [ ] **Step 6: Implement low-overhead profiles and bounded global metrics.**

`QueryMetrics` uses atomic counters and fixed histograms for admission,
terminal state, latency, projection hit/fallback, bytes, memory/scratch,
worker/I/O wait, Delta lag, projection health, adjacency pruning, and label
dominance. Operators add batch-level deltas at batch boundaries. Do not poll
generic RocksDB properties or start RocksDB/Cedar periodic statistics threads;
the existing Cedar sampler may take an explicit bounded query snapshot. Do not
log query text, parameters, QueryId, PropertyId, or values. An explicitly
enabled trace accepts only a caller-supplied redacted logical-plan description.

- [ ] **Step 7: Extend Cedar file inspection without treating files as SSTs.**

Add roles and formats:

```cpp
enum class StorageFileRole : uint8_t {
  kAuthoritativeFacts,
  kTransactionMetadata,
  kEngineInternal,
  kQueryProjection,
  kQueryStatistics,
  kQueryScratch,
};

enum class StorageTableFormat : uint8_t {
  kCedarParquet,
  kBlockBased,
  kCedarManifest,
  kCedarState,
  kCedarAdjacency,
  kCedarProperty,
  kCedarStatistics,
  kCedarScratch,
};

enum class StorageFileAuthority : uint8_t {
  kAuthoritative,
  kDerived,
  kTemporary,
  kEngineInternal,
};

struct QueryFileMetadata {
  StorageFileAuthority authority = StorageFileAuthority::kEngineInternal;
  std::optional<uint64_t> generation_id;
  std::optional<uint64_t> base_seq;
  std::string coverage;
  bool checksum_valid = false;
};
```

Add `std::optional<QueryFileMetadata> query_file` to `StorageFileInfo`; it is
unset for RocksDB files. `coverage` uses the manifest's canonical textual
PartID/entity/valid-time form and is never inferred from filenames.

Recognize `.cmanifest`, `.cstate`, `.cadj`, `.cprop`, `.cstats`, and
`.cscratch`. Report derived/temporary authority, generation, base sequence,
coverage, size, and checksum validity. Keep RocksDB live-file metadata and
Cedar-directory parsing separate so `.sst` classification cannot collide.

- [ ] **Step 8: Verify Explain, metrics, inspection, and hot-path isolation.**

```bash
cmake --build build/query-debug -j2 --target test_query_observability test_query_planner test_cedar_files
ctest --test-dir build/query-debug --output-on-failure -R 'QueryObservability|QueryPlanner|CedarFiles'
rg -n 'GetProperty|GetIntProperty|Statistics::Create|stats_dump_period_sec' src/query
```

Expected: tests pass; the final `rg` returns no query-module match. Physical
Explain contains estimates, confidence, projection/base/delta, fallback holes,
pushdowns, lane, algorithm, and spill permission; Profile contains actuals only
after execution.

- [ ] **Step 9: Commit observability and inspection.**

```bash
git add src/query/observability tests/query/test_query_observability.cc \
  include/cedar/query/query.h include/cedar/query/result.h \
  include/cedar/database.h include/cedar/storage_files.h \
  src/kernel/database_impl.h src/kernel/database.cc src/query/query_api.cc \
  src/query/planner/query_planner.cc src/query/runtime/query_runtime.cc \
  src/query/projection src/storage/rocks/storage_file_inspection.cc \
  tests/tools/test_cedar_files.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: expose bounded query observability"
```

---

### Task 17: Differential Correctness, Small-Threshold Debug, and Sanitizers

**Files:**
- Create: `tests/query/test_query_differential.cc`
- Modify: `tests/model/bitemporal_fact_oracle.h`
- Modify: `tests/recovery/test_query_crash_matrix.cc`
- Modify: `include/cedar/storage_options.h`
- Modify: `include/cedar/database.h`
- Modify: `src/kernel/database.cc`
- Modify: `src/query/projection/projection_format.cc`
- Modify: `src/query/projection/projection_manifest.cc`
- Modify: `src/query/projection/projection_store.cc`
- Modify: `src/query/projection/query_delta.cc`
- Modify: `src/query/resource/query_resource_pool.cc`
- Modify: `src/query/resource/query_scratch.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: the independent oracle from Task 2 and every completed canonical, projection, Delta, planner, relational, graph, recovery, and resource path.
- Produces: one reproducible randomized differential harness, exact Debug amplification capacities, complete fault-point coverage, and ASAN/UBSAN/TSAN/LSAN gates.

- [ ] **Step 1: Extend the independent oracle to the full query surface.**

Add direct-enumeration methods that share no production temporal/planner code:

```cpp
OracleRows Evaluate(const OracleLogicalQuery&, CommitSeq snapshot) const;
OracleRows Expand(const OracleExpandSpec&, CommitSeq snapshot) const;
OraclePath CoexistingShortestPath(const OraclePathSpec&, CommitSeq) const;
OracleJourney EarliestArrival(const OracleJourneySpec&, CommitSeq) const;
OracleJourney LatestDeparture(const OracleJourneySpec&, CommitSeq) const;
OracleJourney FastestDuration(const OracleJourneySpec&, CommitSeq) const;
```

Enumerate corrected facts, simple paths up to the same hop cap, and all bounded
journeys on graphs of at most eight vertices. Sort witnesses by objective, hop
count, then lexicographic `EdgeRef` sequence.

- [ ] **Step 2: Add the deterministic random-history generator.**

Every failure prints a replayable seed and serialized case. Generate same-time
corrections, deletes, disjoint intervals, Missing, schema epochs, self-loops,
parallel/cross-partition edges, cycles, hubs, duration changes, empty/touching
intervals, and arithmetic edge values. Use fixed suites:

```text
smoke seeds:       0..199
full Debug seeds:  0..4999
path seeds:        10000..10999
journey seeds:     20000..20999
```

- [ ] **Step 3: Compare every execution topology with the oracle.**

For every applicable generated query, normalize its typed rows and compare:

```text
reference evaluator
canonical-only
projection at base
projection plus short Delta
projection plus long Delta
partial-coverage fallback
restart-rebuilt Delta
interactive lane
analytical lane
hybrid lane
```

Assert both row equality and terminal `complete=true`. For injected budget,
cancel, deadline, projection corruption, or scratch corruption, compare every
already emitted batch with the correct Snapshot prefix and require
`complete=false`.

- [ ] **Step 4: Add and validate exact small-threshold Debug options.**

The test profile changes capacities only, never algorithms:

```cpp
struct QueryDebugThresholds {
  uint64_t memtable_bytes = 64ULL << 10;
  uint64_t projection_segment_bytes = 64ULL << 10;
  uint64_t projection_page_bytes = 4ULL << 10;
  uint64_t query_delta_soft_bytes = 64ULL << 10;
  uint64_t query_delta_hard_bytes = 128ULL << 10;
  uint64_t query_memory_bytes = 32ULL << 10;
  uint64_t scratch_run_bytes = 16ULL << 10;
  uint32_t delta_lag_soft_commits = 8;
  uint32_t delta_lag_hard_commits = 32;
  uint32_t manifest_commits_per_generation = 16;
};
```

Expose this only through `StorageProfile::kDebugSmallThresholds`. Assert the
same production classes perform MemTable flush/compaction, Delta rollover,
projection publication, spill, coverage switching, Vacuum, and orphan cleanup
multiple times in one test; no `#ifdef` test algorithm is allowed.

- [ ] **Step 5: Run the new tests first and preserve every discovered failure.**

```bash
cmake --build build/query-debug -j2 --target test_query_differential test_query_crash_matrix
build/query-debug/tests/test_query_differential --gtest_filter='*Smoke*'
build/query-debug/tests/test_query_differential --gtest_filter='*Full*'
```

Expected before fixes: at least the newly introduced threshold, topology, or
fault assertions fail. Reduce each discovered seed to a named non-random
regression in the owning `tests/query/test_*.cc` before fixing production code.

- [ ] **Step 6: Complete the crash/corruption matrix.**

Add fault hooks immediately before/after segment write, sync, rename, manifest
sync, CURRENT replacement, Delta enqueue/repair, scratch block write, Cursor
cancel/Close, projection-reader release, and Vacuum retirement. The real
subprocess matrix runs commits, queries, and projection builds concurrently;
kills at every hook; then validates visible commit sequence, canonical results,
projection eligibility, Delta continuity, scratch cleanup, and reopen.

Corrupt each projection file by bit flip, deletion, and truncation. Corrupt one
authoritative facts SST separately and assert the different recovery-required
outcome. Repeat Close races and Vacuum pins for 100 deterministic iterations.

- [ ] **Step 7: Fix production defects one regression at a time.**

For each named failure, run only that test, make the minimum owning-module
change, rerun it, then rerun its focused suite. Required assertions include
coverage non-overlap/continuity, Delta sequence continuity, reservation
non-underflow, Snapshot/generation/cache-lease lifetime, and exactly-once
scratch cleanup. Do not weaken the oracle, retry an unexpected Status, or widen
a test threshold to make a failure disappear.

- [ ] **Step 8: Run the complete Debug and randomized suites.**

```bash
cmake --build build/query-debug -j2
ctest --test-dir build/query-debug --output-on-failure
build/query-debug/tests/test_query_differential --gtest_filter='*Full*'
build/query-debug/tests/test_query_crash_matrix --gtest_repeat=5 --gtest_break_on_failure
```

Expected: all pre-existing and query tests pass; 5,000 general, 1,000 path, and
1,000 journey seeds match in every applicable topology; crash reopen repeats
pass without stale rows, invalid CURRENT selection, or leaked files.

- [ ] **Step 9: Run ASAN plus LeakSanitizer and UBSAN.**

```bash
cmake -S . -B build/query-asan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTS=ON -DCEDAR_ENABLE_ASAN=ON
cmake --build build/query-asan -j2
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 ctest --test-dir build/query-asan --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
cmake -S . -B build/query-ubsan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTS=ON -DCEDAR_ENABLE_UBSAN=ON
cmake --build build/query-ubsan -j2
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --test-dir build/query-ubsan --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
```

Expected: both profiles exit 0; ASAN/LSAN reports no cursor, batch lease,
generation, Snapshot, scratch, or label leaks; UBSAN reports no overflow,
misalignment, invalid enum, or lifetime error.

- [ ] **Step 10: Run TSAN concurrency stress.**

```bash
cmake -S . -B build/query-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTS=ON -DCEDAR_ENABLE_TSAN=ON
cmake --build build/query-tsan -j2
TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 ctest --test-dir build/query-tsan --output-on-failure -R 'QueryLifecycle|QueryDelta|ProjectionStore|QueryCrashMatrix|Vacuum'
```

Expected: exit 0 with no race or lock-order report during commit/Delta publish,
generation switch, cancellation, Close, and Vacuum.

- [ ] **Step 11: Commit the Debug acceptance harness and fixes.**

Review `git diff --name-only` and split any independent production defect into
its own tested commit before the harness commit. Then stage exact harness paths:

```bash
git add tests/query/test_query_differential.cc \
  tests/model/bitemporal_fact_oracle.h tests/recovery/test_query_crash_matrix.cc \
  include/cedar/storage_options.h include/cedar/database.h \
  src/kernel/database.cc src/query/projection src/query/resource \
  CMakeLists.txt tests/CMakeLists.txt
git commit -m "test: stress bitemporal queries under debug thresholds"
```

---

### Task 18: Kernel-Only Read, Write, and Space Benchmark Harness

**Files:**
- Create: `benchmarks/cedar_query_bench_options.h`
- Create: `benchmarks/cedar_query_bench_options.cc`
- Create: `benchmarks/cedar_query_bench_workload.h`
- Create: `benchmarks/cedar_query_bench_workload.cc`
- Create: `benchmarks/cedar_query_bench.cc`
- Create: `benchmarks/run_cedar_query_campaign.sh`
- Create: `tests/performance/test_query_bench_options.cc`
- Create: `tests/performance/test_query_benchmark_csv.cmake`
- Modify: `benchmarks/cedar_kernel_bench.cc`
- Modify: `benchmarks/cedar_kernel_bench_options.h`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `benchmarks/cedar_kernel_bench_workload.h`
- Modify: `benchmarks/cedar_kernel_bench_workload.cc`
- Modify: `tests/performance/test_kernel_bench_options.cc`
- Modify: `tests/performance/test_kernel_benchmark_csv.cmake`
- Modify: `tests/performance/test_kernel_bounded_benchmark.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: the user's existing dirty multi-fact Kernel benchmark edits, all query operations, profiles, metrics, inspection, and lifecycle verification.
- Produces: one Kernel write sweep, one Kernel query/mixed sweep, reproducible CSV/JSON metadata, reopen validation, and automated hard-gate classification.

- [ ] **Step 1: Audit and integrate the protected benchmark work.**

Before editing, save these exact diffs in the task review record:

```bash
git diff -- benchmarks/cedar_kernel_bench.cc \
  benchmarks/cedar_kernel_bench_options.cc \
  benchmarks/cedar_kernel_bench_options.h \
  benchmarks/cedar_kernel_bench_workload.cc \
  benchmarks/cedar_kernel_bench_workload.h \
  tests/performance/test_kernel_bench_options.cc \
  tests/performance/test_kernel_benchmark_csv.cmake \
  tests/performance/test_kernel_bounded_benchmark.cc
```

Preserve the facts-per-transaction implementation and tests unless a failing
contract proves a defect. Do not replace these files from another commit. Add
or revise tests first for every required integration change.

- [ ] **Step 2: Remove obsolete benchmark identities and compatibility.**

```bash
rg -n -i 'lean|legacy query|old query|query_v1|profile[ =,:]+lean' \
  benchmarks tests/performance CMakeLists.txt
```

Delete every matching executable, source, runner branch, CLI option, parser
fallback, and CSV column. The only write control is the same Cedar Kernel
binary with projection/statistics work paused. Add a CMake/CTest contract that
fails if a benchmark target or help text contains `lean`.

- [ ] **Step 3: Lock the write-sweep option and CSV contracts.**

The parser accepts exactly:

```text
--facts-per-txn=1|4|8|16|32|64|128|256|512|1024|2048
--writers=1|2|8|32|64
--projection-work=paused|active
--cache-state=cold|warm
--duration-seconds=<positive integer>
--reopen-verify=true
```

CSV and JSON include transactions/s, facts/s, MiB/s, group-fill distribution,
WAL-sync and end-to-end p50/p95/p99, write amplification, space amplification,
projection lag, terminal status, and reopen verification. Reject unknown enum
values and zero duration; do not silently clamp facts or writers.

- [ ] **Step 4: Add the typed query workload matrix.**

`cedar_query_bench_options` enumerates operations, never accepts free-form
query text:

```cpp
enum class QueryBenchmarkOperation : uint8_t {
  kStateAt, kHistory, kEvents, kChanges,
  kExpandOut, kExpandIn, kExpandBoth,
  kPropertyFilter, kTemporalAggregate, kIntervalJoin, kKHop,
  kCoexistingShortestPath, kEarliestArrival,
  kLatestDeparture, kFastestDuration,
};

enum class ProjectionState : uint8_t {
  kCanonicalOnly, kBase, kShortDelta, kLongDelta, kPartialCoverage,
};
```

Options cover degree `1,10,100,1000,10000`, property selectivity
`0.1,1,10,100` percent, readers `1,8,32`, cold/warm cache, max hops, result
limit, capture-profile, seed, and duration. Unsupported combinations fail
parsing instead of being skipped silently.

- [ ] **Step 5: Build deterministic datasets and reopen-verifiable answers.**

Generate vertices, parallel/cross-partition edges, skewed degrees, property
histograms, corrected valid-time chains, durations, and known path/journey
answers from a recorded seed. Bulk loading uses normal Kernel transactions and
the selected facts-per-transaction value. After load, optionally build
projections, apply exact short/long Delta mutations, or remove one manifest
coverage range through a benchmark-only setup helper before measurement.

Store expected row counts and a commutative typed-row checksum outside the
database directory. Every measured database is closed, reopened, queried
canonically, and checked against both values.

- [ ] **Step 6: Measure read latency and physical efficiency separately.**

For each run record QPS, facts/edges per second, MiB/s, first-result latency,
p50/p95/p99, CPU time, pages, physical/decoded bytes, cache hit state, Delta
repairs, fallback coverage, interval fragments, spill, frontier labels,
projection generation/base/Delta state, and terminal completeness. A cold run
uses a newly opened Database and benchmark-declared cache conditioning; a warm
run executes one unmeasured full workload pass. Never combine cold and warm
samples in one percentile.

- [ ] **Step 7: Encode artifact identity and space accounting.**

Each CSV row and JSON run header contains commit, dirty flag, compiler, build
type, sanitizer state, host CPU/RAM/storage, OS, dataset/seed, complete options,
cache state, query plan fingerprint, projection generation/base/Delta, start
time, duration, and raw sample path. Use `InspectStorageFiles` plus Cedar query
inspection to report authoritative, adjacency, property, statistics, scratch
peak, obsolete, WAL/MANIFEST, and total bytes without double counting.

- [ ] **Step 8: Add parser, schema, bound, and reopen tests.**

```bash
cmake --build build/query-debug -j2 --target test_kernel_bench_options test_kernel_bounded_benchmark test_query_bench_options
ctest --test-dir build/query-debug --output-on-failure -R 'KernelBench|QueryBench'
cmake -DCEDAR_BENCH=$PWD/build/query-debug/cedar_query_bench \
  -P tests/performance/test_query_benchmark_csv.cmake
```

Expected: option matrices accept only documented values; CSV/JSON have stable
column names and units; bounded smoke workloads terminate, report complete
results, close/reopen, and verify checksums.

- [ ] **Step 9: Implement the campaign runner and gate classifier.**

`run_cedar_query_campaign.sh` runs named phases in this order:

```text
release-calibration
write-idle-five-repeats
write-active-projection-five-repeats
read-cold
read-warm
mixed-30-minute
reopen-verification
space-audit
```

It writes one command manifest before execution, one raw CSV/JSON file per
case, and a summary that hard-fails: idle query overhead above 3% facts/s or 5%
WAL-sync p99; active projection overhead above 10% throughput or 15% p99;
StateAt full scan; adjacency work outside `O(log N + candidate/returned
degree)` evidence; profiling overhead above 2%; derived bytes above 1.5 times
authoritative live bytes; statistics above 2% of projections; incomplete,
stale, unexplained fallback, unbounded lag, leak, or reopen mismatch.

- [ ] **Step 10: Run Release smoke calibration without claiming capability.**

```bash
cmake -S . -B build/query-release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON
cmake --build build/query-release -j2
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase release-calibration \
  --duration-seconds 10 --output build/query-release/evidence/calibration
```

Expected: all smoke matrix rows exit 0 and reopen-verify. Label every number
`calibration`; do not publish it as Cedar capability and do not tune around one
sample before Debug/sanitizer gates remain green.

- [ ] **Step 11: Commit the Kernel-only benchmark harness.**

Review the combined diff with special attention to the eight originally dirty
files, then stage only benchmark/test/build paths:

```bash
git add benchmarks/cedar_kernel_bench.cc benchmarks/cedar_kernel_bench_options.* \
  benchmarks/cedar_kernel_bench_workload.* benchmarks/cedar_query_bench* \
  benchmarks/run_cedar_query_campaign.sh \
  tests/performance/test_kernel_bench_options.cc \
  tests/performance/test_kernel_benchmark_csv.cmake \
  tests/performance/test_kernel_bounded_benchmark.cc \
  tests/performance/test_query_bench_options.cc \
  tests/performance/test_query_benchmark_csv.cmake CMakeLists.txt tests/CMakeLists.txt
git add -u -- benchmarks tests/performance CMakeLists.txt tests/CMakeLists.txt
git commit -m "perf: add Kernel bitemporal query campaigns"
```

---

### Task 19: Final Installation, Release, Sustained, and Evidence Gate

**Files:**
- Create: `docs/superpowers/evidence/2026-08-21-cedar-bitemporal-query-acceptance.md`
- Modify: `tests/public/install_consumer/main.cc`
- Modify: `tests/test_install_consumer.cmake`
- Modify: `tests/test_public_header_contract.cmake`
- Modify: `README.md`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Consumes: Tasks 1-18 and all generated raw Debug/sanitizer/Release artifacts.
- Produces: installed public-query proof, a clean full-suite result, sustained read/write/space evidence, a reopen-verified capability statement, and an auditable final commit.

- [ ] **Step 1: Add failing installed-consumer and private-engine tests.**

The install consumer must open a Database, commit a vertex/property history,
prepare a typed `StateAt` query through `#include <cedar/query.h>`, execute it on
a consumed Snapshot, verify a typed result, close, reopen, and verify again.
Compile it using only installed include/library/package paths.

Extend the public/install scans to reject `rocksdb`, `src/query`, internal
headers, engine handles, and absolute source/build paths in installed headers,
targets, and package files.

- [ ] **Step 2: Run install and public-contract tests.**

```bash
cmake --build build/query-debug -j2 --target cedar_core
ctest --test-dir build/query-debug --output-on-failure -R 'PublicHeaderContract|InstallConsumer|EmbeddedEngineContract'
```

Expected: installed consumer passes without engine headers or direct engine
libraries, and the package exports only `Cedar::cedar` plus Cedar public types.

- [ ] **Step 3: Re-run the complete Debug and sanitizer gates from clean builds.**

```bash
cmake --build build/query-debug -j2
ctest --test-dir build/query-debug --output-on-failure
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 ctest --test-dir build/query-asan --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ctest --test-dir build/query-ubsan --output-on-failure -R 'Query|Projection|Temporal|Vacuum'
TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 ctest --test-dir build/query-tsan --output-on-failure -R 'QueryLifecycle|QueryDelta|ProjectionStore|QueryCrashMatrix|Vacuum'
```

Expected: every command exits 0. Any source change made after a sanitizer run
invalidates that sanitizer evidence and requires rebuilding/rerunning it.

- [ ] **Step 4: Run the complete write turning-point matrix.**

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase write-idle-five-repeats \
  --facts-per-txn 1,4,8,16,32,64,128,256,512,1024,2048 \
  --writers 1,2,8,32,64 --duration-seconds 60 \
  --output build/query-release/evidence/write-idle
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase write-active-projection-five-repeats \
  --facts-per-txn 1,4,8,16,32,64,128,256,512,1024,2048 \
  --writers 1,2,8,32,64 --duration-seconds 60 \
  --output build/query-release/evidence/write-active
```

Expected: five independent samples per point; the report identifies the actual
facts/transaction peak and later plateau/decline, not an assumed 64-fact
optimum. Idle/active overhead gates pass with WAL-sync and end-to-end tails.

- [ ] **Step 5: Run separate cold and warm read matrices.**

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase read-cold --duration-seconds 30 \
  --readers 1,8,32 --degrees 1,10,100,1000,10000 \
  --selectivities 0.1,1,10,100 \
  --projection-states canonical,base,short-delta,long-delta,partial \
  --output build/query-release/evidence/read-cold
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase read-warm --duration-seconds 30 \
  --readers 1,8,32 --degrees 1,10,100,1000,10000 \
  --selectivities 0.1,1,10,100 \
  --projection-states canonical,base,short-delta,long-delta,partial \
  --output build/query-release/evidence/read-warm
```

Expected: all StateAt/history/events/changes/adjacency/property/aggregate/join/
k-hop/path/journey cases finish completely. StateAt full-scan count is zero;
adjacency/property page and byte bounds pass; cold and warm artifacts remain
separate.

- [ ] **Step 6: Run and verify the 30-minute sustained mixed campaign.**

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase mixed-30-minute \
  --duration-seconds 1800 --writers 32 --readers 32 \
  --facts-per-txn auto-turning-point \
  --output build/query-release/evidence/mixed-sustained
```

Expected: elapsed time is at least 1,800 seconds and exit code is 0; no stale
or incomplete result, unexplained fallback, resource leak, uncontrolled write
stop, maintenance error, or unbounded projection/Delta lag. Report sustained
transactions/s, facts/s, MiB/s, each query family's QPS and latency, CPU,
memory/scratch peaks, WAL-sync tail, projection impact, and generation count.

- [ ] **Step 7: Reopen every database and enforce space gates.**

```bash
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase reopen-verification \
  --input build/query-release/evidence \
  --output build/query-release/evidence/reopen
benchmarks/run_cedar_query_campaign.sh \
  --build-dir build/query-release --phase space-audit \
  --input build/query-release/evidence \
  --output build/query-release/evidence/space
```

Expected: every database closes/reopens and matches stored row count/checksum.
Derived projection bytes target `<= 1.0x` authoritative live bytes and must be
`<= 1.5x`; `.cstats` must be `<= 2%` of projection bytes; scratch is zero after
Close/reopen; obsolete generation bytes are either zero or explained by a
currently pinned reader in the test.

- [ ] **Step 8: Write the acceptance evidence from raw artifacts.**

The evidence document records exact commands, commit, dirty status, host,
compiler, dataset, thresholds, artifact paths/checksums, test counts, random
seed ranges, sanitizer results, write turning point, cold/warm/peak/sustained
tables, hard-gate calculations, space breakdown, and reopen checks. Mark the
first Release run calibration-only. State Cedar capability only from the
30-minute run and only if every prior gate passed; otherwise label the exact
failed gate and do not summarize a partial run as capability.

- [ ] **Step 9: Run the final source and worktree audit.**

```bash
rg -n -i 'lean|legacy query|query_v1|QueryMemoryLimit|rocksdb::' \
  include benchmarks tests/performance README.md
git diff --check
git status --short --branch
```

Expected: no obsolete query/Lean/status or public `rocksdb::` match;
`git diff --check` is empty; only the intended evidence/public/install changes
remain. Generated database directories, binaries, and raw artifacts stay under
ignored `build/query-*` paths and are not committed.

- [ ] **Step 10: Commit public installation proof and final evidence.**

```bash
git add tests/public/install_consumer/main.cc tests/test_install_consumer.cmake \
  tests/test_public_header_contract.cmake README.md CMakeLists.txt \
  tests/CMakeLists.txt \
  docs/superpowers/evidence/2026-08-21-cedar-bitemporal-query-acceptance.md
git commit -m "docs: record bitemporal query acceptance"
```

- [ ] **Step 11: Record the implementation completion point.**

```bash
git log --oneline --decorate -20
git status --short --branch
```

Expected: Tasks 1-19 appear as reviewable commits, the acceptance commit is
HEAD, and the worktree is clean. Do not merge, delete the worktree, or remove
another branch without a separate explicit user instruction.
