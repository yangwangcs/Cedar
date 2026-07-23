# Cedar CBO HashJoin Dynamic Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a safe build-side hash-signature filter at the HashJoin pipeline breaker so unmatched probe rows are rejected before probe-row materialization and before spill writes.

**Architecture:** Every non-null encoded build key contributes its stable 64-bit signature to a query-local set. Probe rows first compute the normal encoded join key, then reject null keys or signatures absent from the build set; signature collisions are allowed false positives and are still validated by the existing exact hash lookup. The same filter runs in memory and before spilled probe rows are encoded, while final join multiplicity, ordering, null semantics, snapshot behavior, and exact key comparison remain unchanged.

**Tech Stack:** C++17, typed `PhysicalHashJoinResultStream`, multi-step `PhysicalMultiHashJoinPlan`, `QueryResultStream`, spill partitions, GoogleTest, EXPLAIN ANALYZE JSON.

## Global Constraints

- The filter is advisory: it may retain false positives but may never reject an exact build-key match.
- Stable-hash collisions always fall through to the existing exact `build_`/partition lookup.
- Null join keys remain non-matching and may be counted as dynamically rejected probe rows.
- Probe order and build-row multiplicity remain unchanged for surviving rows.
- The spill path filters before `StoredRowAt` and `AppendSpilledRow` so rejected probe rows do not consume spill I/O.
- The same `PhysicalHashJoinResultStream` implementation covers two-input and general multi-join steps.
- Broad memory-account normalization, hot-key spill fallback, fault injection, and sanitizer matrices remain deferred to the final constraint phase.
- Preserve the dirty worktree; do not reset, clean, commit, or push.

---

### Task 1: Reproduce unmatched probe work in memory

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `TcypherExecutionStats`, `ExecuteTcypher`, exact entity pattern `{id: 1}`.
- Produces: `TcypherExecutorTest.PhysicalHashJoinDynamicFilterRejectsUnmatchedProbeRows`.

- [ ] **Step 1: Write the failing test**

Create three vertices where only vertex `1` shares the exact-root build key:

```cpp
TEST(TcypherExecutorTest,
     PhysicalHashJoinDynamicFilterRejectsUnmatchedProbeRows) {
  CommitTimeline timeline("/tmp/cedar_unused_hash_dynamic_filter_timeline");
  auto stats = std::make_shared<TcypherExecutionStats>();
  TcypherQueryOptions options;
  options.statement_start_valid_time = 10;
  options.batch_capacity = 4;
  options.execution_stats = stats;
  TcypherExecutionContext context{
      timeline, 1, options, MakeTcypherBinderSchemaSnapshot(),
      {TemporalEvent::Put(LogicalKey::VertexExistence(1), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(2), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(3), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexProperty(1, 7), 0, 1, 1,
                          Value::String("target")),
       TemporalEvent::Put(LogicalKey::VertexProperty(2, 7), 0, 1, 1,
                          Value::String("other-2")),
       TemporalEvent::Put(LogicalKey::VertexProperty(3, 7), 0, 1, 1,
                          Value::String("other-3"))}};
  auto stream = ExecuteTcypher(
      "FOR VALID_TIME AS OF 10 MATCH (a {id: 1}) MATCH (b) "
      "WHERE a.name = b.name RETURN a, b;",
      std::move(context));
  ASSERT_TRUE(stream.ok()) << stream.status().ToString();
  ResultBatch result;
  ASSERT_TRUE(stream.ValueOrDie()->Next(&result).ok());
  ASSERT_EQ(result.batch().row_count(), 1U);
  EXPECT_EQ(*result.batch().ValueAt(0, 0), Value::Int64(1));
  EXPECT_EQ(*result.batch().ValueAt(1, 0), Value::Int64(1));
  EXPECT_TRUE(stream.ValueOrDie()->Next(&result).IsNotFound());
  EXPECT_EQ(stats->hash_join_dynamic_filter_input_rows, 3U);
  EXPECT_EQ(stats->hash_join_dynamic_filter_rejected_rows, 2U);
  EXPECT_EQ(stats->hash_join_dynamic_filter_output_rows, 1U);
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
cmake --build build --target test_correctness_kernel -j2
build/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.PhysicalHashJoinDynamicFilterRejectsUnmatchedProbeRows'
```

Expected: compilation fails because the dynamic-filter counters do not exist.

### Task 2: Publish and apply the in-memory build filter

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/tcypher/executor.cc`
- Modify: `src/observability/explain_analyze_profile.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Consumes: `PhysicalHashJoinResultStream::StableKeyHash`, `KeyAt`, `build_`, `TcypherExecutionStats`.
- Produces: `build_filter_hashes_`, `ProbePassesDynamicFilter`, four runtime/profile counters.

- [ ] **Step 1: Add execution and profile counters**

Add to both `TcypherExecutionStats` and `ExplainAnalyzeRuntimeProfile`:

```cpp
uint64_t hash_join_dynamic_filter_input_rows = 0;
uint64_t hash_join_dynamic_filter_rejected_rows = 0;
uint64_t hash_join_dynamic_filter_output_rows = 0;
uint64_t hash_join_dynamic_filter_spill_rows_avoided = 0;
```

Copy them in `PopulateExplainAnalyzeRuntimeProfile` and serialize them inside the existing `"join"` object.

- [ ] **Step 2: Retain build signatures**

In `PhysicalHashJoinResultStream`, add:

```cpp
std::set<uint64_t> build_filter_hashes_;
```

In `AddBuildRow`, immediately after a non-null encoded key is obtained, insert:

```cpp
build_filter_hashes_.insert(StableKeyHash(*key.ValueOrDie()));
```

This must happen before a possible `StartSpill()` so every build row contributes even when the first row triggers spill.

- [ ] **Step 3: Add the filter helper**

Add a helper that updates counters exactly once per probe row:

```cpp
bool ProbePassesDynamicFilter(
    const std::optional<std::string>& key, bool spill_path) {
  if (stats_) ++stats_->hash_join_dynamic_filter_input_rows;
  const bool passes = key.has_value() &&
      build_filter_hashes_.count(StableKeyHash(*key)) != 0;
  if (stats_) {
    if (passes) {
      ++stats_->hash_join_dynamic_filter_output_rows;
    } else {
      ++stats_->hash_join_dynamic_filter_rejected_rows;
      if (spill_path) {
        ++stats_->hash_join_dynamic_filter_spill_rows_avoided;
      }
    }
  }
  return passes;
}
```

- [ ] **Step 4: Filter before in-memory probe materialization**

In `Next`, compute the key, increment `probe_row_`, call `ProbePassesDynamicFilter(..., false)`, and continue immediately on rejection. Only then call `StoredRowAt` and the exact `build_.find` lookup. A signature collision may pass the filter but still produce no exact match.

- [ ] **Step 5: Run the focused test and verify GREEN**

Run the Task 1 command. Expected: one output row and counters `3/2/1`.

### Task 3: Filter probe rows before spill encoding

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Consumes: `ProbePassesDynamicFilter`, `PartitionProbe`, `AppendSpilledRow`.
- Produces: spill-path proof that rejected rows are not serialized.

- [ ] **Step 1: Write the failing spill test**

```cpp
TEST(TcypherExecutorTest,
     PhysicalHashJoinDynamicFilterAvoidsUnmatchedProbeSpillRows) {
  const std::string spill_directory =
      "/tmp/cedar_hash_dynamic_filter_spill";
  std::filesystem::remove_all(spill_directory);
  CommitTimeline timeline(
      "/tmp/cedar_unused_hash_dynamic_filter_spill_timeline");
  auto stats = std::make_shared<TcypherExecutionStats>();
  auto memory = std::make_shared<QueryMemoryAccount>(1, 8U << 20);
  TcypherQueryOptions options;
  options.statement_start_valid_time = 10;
  options.batch_capacity = 4;
  options.execution_stats = stats;
  options.memory_account = memory;
  options.spill_directory = spill_directory;
  TcypherExecutionContext context{
      timeline, 1, options, MakeTcypherBinderSchemaSnapshot(),
      {TemporalEvent::Put(LogicalKey::VertexExistence(1), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(2), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(3), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexProperty(1, 7), 0, 1, 1,
                          Value::String("target")),
       TemporalEvent::Put(LogicalKey::VertexProperty(2, 7), 0, 1, 1,
                          Value::String("other-2")),
       TemporalEvent::Put(LogicalKey::VertexProperty(3, 7), 0, 1, 1,
                          Value::String("other-3"))}};
  auto stream = ExecuteTcypher(
      "FOR VALID_TIME AS OF 10 MATCH (a {id: 1}) MATCH (b) "
      "WHERE a.name = b.name RETURN a, b;",
      std::move(context));
  ASSERT_TRUE(stream.ok()) << stream.status().ToString();
  ResultBatch result;
  ASSERT_TRUE(stream.ValueOrDie()->Next(&result).ok());
  ASSERT_EQ(result.batch().row_count(), 1U);
  EXPECT_EQ(*result.batch().ValueAt(0, 0), Value::Int64(1));
  EXPECT_EQ(*result.batch().ValueAt(1, 0), Value::Int64(1));
  EXPECT_TRUE(stream.ValueOrDie()->Next(&result).IsNotFound());
  EXPECT_EQ(stats->hash_join_spill_starts, 1U);
  EXPECT_EQ(stats->hash_join_dynamic_filter_input_rows, 3U);
  EXPECT_EQ(stats->hash_join_dynamic_filter_rejected_rows, 2U);
  EXPECT_EQ(stats->hash_join_dynamic_filter_output_rows, 1U);
  EXPECT_EQ(stats->hash_join_dynamic_filter_spill_rows_avoided, 2U);
  stream = Status::NotFound("test", "release stream");
  EXPECT_EQ(memory->used_bytes(), 0U);
  EXPECT_TRUE(std::filesystem::is_empty(spill_directory));
  std::filesystem::remove_all(spill_directory);
}
```

- [ ] **Step 2: Run the spill test and verify RED**

Expected: `spill_rows_avoided` remains zero because `PartitionProbe` still writes all non-null probe keys.

- [ ] **Step 3: Apply the filter before spill row copying**

In `PartitionProbe`, after `KeyAt` and ordinal assignment, call:

```cpp
if (!ProbePassesDynamicFilter(key.ValueOrDie(), true)) continue;
```

Only surviving rows may call `StoredRowAt` and `AppendSpilledRow`.

- [ ] **Step 4: Run both dynamic-filter tests**

Expected: in-memory and spill tests pass; spill cleanup returns the memory account to zero and removes temporary files.

### Task 4: Multi-join and observability regression

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: shared `PhysicalHashJoinResultStream` used by every `PhysicalMultiHashJoinStep`.
- Produces: multi-step and profile coverage plus the progress ledger update.

- [ ] **Step 1: Add a three-root regression**

```cpp
TEST(TcypherExecutorTest,
     PhysicalMultiHashJoinUsesDynamicFilterAtEachJoinBreaker) {
  CommitTimeline timeline(
      "/tmp/cedar_unused_multi_hash_dynamic_filter_timeline");
  auto stats = std::make_shared<TcypherExecutionStats>();
  TcypherQueryOptions options;
  options.statement_start_valid_time = 10;
  options.batch_capacity = 8;
  options.execution_stats = stats;
  TcypherExecutionContext context{
      timeline, 1, options, MakeTcypherBinderSchemaSnapshot(),
      {TemporalEvent::Put(LogicalKey::VertexExistence(1), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(2), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(3), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexExistence(4), 0, 1, 1,
                          Value::Binary("")),
       TemporalEvent::Put(LogicalKey::VertexProperty(1, 7), 0, 1, 1,
                          Value::String("target")),
       TemporalEvent::Put(LogicalKey::VertexProperty(2, 7), 0, 1, 1,
                          Value::String("target")),
       TemporalEvent::Put(LogicalKey::VertexProperty(3, 7), 0, 1, 1,
                          Value::String("Paris")),
       TemporalEvent::Put(LogicalKey::VertexProperty(4, 7), 0, 1, 1,
                          Value::String("other")),
       TemporalEvent::Put(LogicalKey::VertexProperty(1, 8), 0, 1, 1,
                          Value::String("Paris")),
       TemporalEvent::Put(LogicalKey::VertexProperty(2, 8), 0, 1, 1,
                          Value::String("Rome"))}};
  auto stream = ExecuteTcypher(
      "FOR VALID_TIME AS OF 10 MATCH (a {id: 1}) MATCH (b) MATCH (c) "
      "WHERE a.name = b.name AND b.city = c.name RETURN a, b, c;",
      std::move(context));
  ASSERT_TRUE(stream.ok()) << stream.status().ToString();
  ResultBatch result;
  ASSERT_TRUE(stream.ValueOrDie()->Next(&result).ok());
  ASSERT_EQ(result.batch().row_count(), 1U);
  EXPECT_EQ(*result.batch().ValueAt(0, 0), Value::Int64(1));
  EXPECT_EQ(*result.batch().ValueAt(1, 0), Value::Int64(1));
  EXPECT_EQ(*result.batch().ValueAt(2, 0), Value::Int64(3));
  EXPECT_TRUE(stream.ValueOrDie()->Next(&result).IsNotFound());
  EXPECT_GT(stats->hash_join_dynamic_filter_input_rows, 0U);
  EXPECT_GT(stats->hash_join_dynamic_filter_rejected_rows, 0U);
  EXPECT_GT(stats->hash_join_dynamic_filter_output_rows, 0U);
}
```

This proves every multi-join step inherits the same build-breaker filter.

- [ ] **Step 2: Extend EXPLAIN ANALYZE coverage**

Run the two-input selective query under `EXPLAIN ANALYZE` and assert the `join` JSON contains:

```json
"dynamic_filter_input_rows":3,
"dynamic_filter_rejected_rows":2,
"dynamic_filter_output_rows":1
```

- [ ] **Step 3: Run focused regression**

Run HashJoin, multi-join, spill, CBO, and EXPLAIN ANALYZE tests. Expected: all selected tests pass.

- [ ] **Step 4: Run normal regression and hygiene checks**

Run:

```bash
ctest --test-dir build --output-on-failure
git diff --check
```

Also scan the modified files for trailing whitespace. Do not run the deferred sanitizer/fault matrix.

- [ ] **Step 5: Update the progress ledger**

Record the exact focused and CTest counts, profile fields, spill-row avoidance behavior, deferred resource normalization, and that no commit was created.
