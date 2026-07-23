# Cedar General Multi-Root Join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace relationship-free three-or-more-root Cartesian materialization with a typed, cost-ordered, spill-capable physical hash-join chain.

**Architecture:** Lower bound equalities into an explicit logical join graph. Plan connected root inputs into a left-deep `PhysicalMultiHashJoinPlan`; use bounded dynamic programming for at most six roots and deterministic greedy attachment above that threshold. Execute every step through the existing spill-capable hash-join stream, retaining internal columns needed by later steps and projecting user columns only in the final step.

**Tech Stack:** C++17, Cedar typed T-Cypher IR, pinned VersionSet/catalog/statistics snapshot, vectorized `QueryResultStream`, GoogleTest.

## Global Constraints

- Temporal visibility is always resolved from the pinned `QuerySnapshot`; statistics can change cost but never correctness.
- Missing, incomplete, corrupt, or overlay-incompatible statistics use conservative estimates and deterministic tie-breaking.
- Every physical plan carries typed child ownership, key-column mappings, output layouts, plan identity, and validation.
- Hash joins retain null-never-matches inner-join semantics and spill or fail before their hard memory grant.
- Without `ORDER BY`, result order is unspecified. With `ORDER BY`, the final global SortSink establishes publication order.
- Existing two-root `PhysicalHashJoinPlan` remains supported during this slice.
- Handwritten `ExecuteMultiRootMatchAsOf` must no longer own any connected three-or-more-root query accepted by the typed planner.

---

### Task 1: Explicit Logical Join Graph

**Files:**
- Modify: `include/cedar/tcypher/logical_plan.h`
- Modify: `src/tcypher/logical_plan.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `BoundTcypherStatement::joins`, `BoundJoinEquality`, `BindingId`.
- Produces:

```cpp
enum class LogicalOperatorKind : uint8_t {
  // existing values...
  kJoin,
};

struct LogicalJoinEndpoint {
  BindingId binding_id;
  bool identity = false;
  std::optional<BoundPropertyId> property_id;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
};

struct LogicalJoinEdge {
  LogicalJoinEndpoint left;
  LogicalJoinEndpoint right;
};

struct LogicalPlan {
  FactDemandSet demand;
  std::vector<LogicalPlanNode> nodes;
  std::vector<LogicalJoinEdge> join_edges;
};
```

- [ ] **Step 1: Write the failing logical graph test**

Add `TcypherLogicalPlanTest.ThreeRootEqualitiesBecomeTypedConnectedJoinGraph`. Bind:

```text
FOR VALID_TIME AS OF 10
MATCH (a) MATCH (b) MATCH (c)
WHERE a.name = b.name AND b.name = c.name
RETURN a, b, c;
```

Assert exactly two typed edges, the expected three binding IDs, `kString` key types, and two `kJoin` nodes after the three scans.

- [ ] **Step 2: Run the RED test**

Run:

```bash
cmake --build build-v2 --target test_correctness_kernel -j4 && \
build-v2/tests/test_correctness_kernel \
  --gtest_filter=TcypherLogicalPlanTest.ThreeRootEqualitiesBecomeTypedConnectedJoinGraph
```

Expected: compile failure because `LogicalPlan::join_edges` and `LogicalOperatorKind::kJoin` do not exist.

- [ ] **Step 3: Implement typed lowering**

Map every `BoundJoinEquality` endpoint to binding identity or `BoundPropertyId`, preserve physical type/nullability, append one `LogicalJoinEdge`, and append one `kJoin` node per edge in deterministic binding-ID order.

- [ ] **Step 4: Verify logical tests**

Run the new test plus `TcypherLogicalPlanTest.*` and require zero failures.

---

### Task 2: Costed Multi-Root Physical Plan

**Files:**
- Modify: `include/cedar/tcypher/physical_plan.h`
- Modify: `src/tcypher/physical_plan.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `LogicalPlan::join_edges`, pinned `PhysicalHashJoinPlanningStats`-style estimates.
- The estimate map is supplied directly by planner tests in this task. Production per-binding estimate construction and executor routing belong only to Task 4.
- Produces:

```cpp
struct PhysicalMultiJoinColumn {
  BindingId binding_id;
  ReturnExpressionKind kind = ReturnExpressionKind::kBinding;
  std::optional<BoundPropertyId> property_id;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
  std::string name;
};

struct PhysicalMultiHashJoinStep {
  PhysicalOperatorSpec join;
  uint32_t input_index = 0;
  std::vector<uint32_t> accumulated_key_columns;
  std::vector<uint32_t> input_key_columns;
  std::vector<PhysicalHashJoinPlan::Output> outputs;
  std::vector<PhysicalMultiJoinColumn> output_layout;
  PhysicalHashJoinBuildSide build_side = PhysicalHashJoinBuildSide::kRight;
  PhysicalCardinalityEstimate accumulated_estimate;
  PhysicalCardinalityEstimate input_estimate;
};

struct PhysicalMultiHashJoinPlan {
  uint64_t plan_id = 0;
  uint64_t statistics_snapshot_id = 0;
  std::vector<std::shared_ptr<const PhysicalPlan>> inputs;
  std::vector<std::vector<PhysicalMultiJoinColumn>> input_layouts;
  std::vector<uint32_t> join_order;
  std::vector<PhysicalMultiHashJoinStep> steps;
  std::vector<std::string> output_names;
  std::vector<uint32_t> final_output_columns;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink;
  std::optional<PhysicalSortSinkSpec> sort_sink;
  std::vector<PhysicalOperatorSpec> post_join_operators;
  std::vector<PipelineDescriptor> pipelines;
};

bool CanPlanPhysicalMultiHashJoin(const BoundTcypherStatement& statement);
StatusOr<std::shared_ptr<const PhysicalMultiHashJoinPlan>> PlanPhysicalMultiHashJoin(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement,
    const std::map<BindingId, PhysicalCardinalityEstimate>& estimates,
    uint64_t statistics_snapshot_id);
Status ValidatePhysicalMultiHashJoinPlan(const PhysicalMultiHashJoinPlan& plan);
std::string FormatPhysicalMultiHashJoinPlan(const PhysicalMultiHashJoinPlan& plan);
```

- [ ] **Step 1: Write planner RED tests**

Add:

```text
TcypherPhysicalPlanTest.ThreeRootConnectedMatchBuildsTypedMultiJoinPlan
TcypherPhysicalPlanTest.MultiJoinDpChoosesLowestEstimatedConnectedOrder
TcypherPhysicalPlanTest.MultiJoinFallbackIsDeterministicWithoutStats
TcypherPhysicalPlanTest.MultiJoinValidatorRejectsDisconnectedOrInvalidLayouts
```

Use estimates `a=100`, `b=4`, `c=20`; assert the order starts at `b`, attaches a connected root, contains two steps, includes statistics identity in `plan_id` and EXPLAIN, and produces the original `RETURN a,b,c` layout.

- [ ] **Step 2: Run RED**

Run the four tests. Expected: compile failure because the multi-join plan APIs do not exist.

- [ ] **Step 3: Build root input plans**

Generalize the existing two-root child builder so each root input projects:

1. all final result columns owned by that binding;
2. every join key needed by any incident logical edge;
3. no duplicate physical column.

Store one `input_layouts[i]` beside every `inputs[i]`. `kind` is authoritative for identity/property/provenance ownership; `property_id` is populated only for `kProperty`. Every child clears aggregate/distinct/sort/post-result ownership. A child for `additional_matches[i]` uses `additional_match_scopes[i]` when nonempty and otherwise inherits the primary scopes.

- [ ] **Step 4: Implement bounded ordering**

Sort inputs by `BindingId` and assign dense ordinals `0..N-1`; DP masks use only those dense ordinals, never raw binding values. Normalize absent estimates to `PhysicalCardinalityEstimate{1000000, 0, true}`. For `N <= 6`, enumerate connected subsets with a 64-bit mask. The first root cost is its estimated rows; attaching an input adds `SaturatingMultiply(accumulated.rows, input.rows)` through `SaturatingAdd`, and the new accumulated estimate is `{saturated_product, min(confidence), accumulated.conservative || input.conservative}`. Keep one lowest cumulative cost per subset; ties compare the BindingId order vector lexicographically. For `N > 6`, start at the lowest `(estimated rows, BindingId)` and repeatedly attach the lowest connected candidate by that same tuple. Each step builds left only when the accumulated estimate is strictly smaller than the new input; ties build right. Reject a disconnected graph instead of silently forming a Cartesian product.

- [ ] **Step 5: Build typed steps and validation**

Each step must own a unique `PhysicalOperatorSpec{kHashJoin}`, join the accumulated layout to exactly one new input, map all equality columns connecting the two sets, retain final/future-key columns, and drop no later dependency. Define `final_output_columns/output_names` as the sink-input layout in original `statement.projections` order; the multi plan owns aggregate/distinct/sort exactly once. Validate input/layout cardinality and ownership, child post-result emptiness, connected attachment, key type compatibility, all output indices, final projection indices, sink indices, enum values, confidence bounds, and nonzero plan identity. Pipelines contain `N` child placeholders followed by `N-1` join pipelines: step zero depends on the first and second input pipelines; each later step depends on the prior join pipeline plus its new input pipeline. Only the final join pipeline appends the multi plan's post-result operators.

- [ ] **Step 6: Verify planner suite**

Run `TcypherPhysicalPlanTest.*` and require zero failures.

---

### Task 3: Spill-Capable Multi-Join Runtime

**Files:**
- Modify: `include/cedar/tcypher/runtime/query_runtime.h`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `include/cedar/tcypher/runtime/query_result.h`
- Modify: `src/tcypher/runtime/query_result.cc`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `PhysicalMultiHashJoinPlan`, `OpenPhysicalRootPointRuntime`, spill-capable physical hash join stream.
- Produces:

```cpp
StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalMultiHashJoinRuntime(
    std::shared_ptr<const PhysicalMultiHashJoinPlan> plan,
    QuerySnapshot snapshot, TcypherExecutionContext context);

class ProjectColumnsResultStream final : public QueryResultStream {
 public:
  ProjectColumnsResultStream(std::unique_ptr<QueryResultStream> input,
                             std::vector<uint32_t> columns,
                             std::vector<std::string> names);
  Status Next(ResultBatch* batch) override;
  Status terminal_status() const override;
};
```

- [ ] **Step 1: Write runtime RED tests**

Add:

```text
TcypherExecutorTest.PhysicalThreeRootJoinStreamsConnectedMatches
TcypherExecutorTest.PhysicalThreeRootJoinSpillsIntermediateAndPreservesRows
TcypherExecutorTest.PhysicalThreeRootJoinNullKeyDoesNotMatch
TcypherExecutorTest.PhysicalThreeRootJoinCancellationCleansSpillAndMemory
```

The first test uses asymmetric values so swapped layouts are visible. The spill test uses a low soft limit and custom directory, compares rows as an unordered multiset, observes files while the stream is alive, and asserts empty directory plus `used_bytes()==0` after destruction.

- [ ] **Step 2: Run RED**

Expected: the current executor selects `ExecuteMultiRootMatchAsOf`; execution stats show no typed multi-join plan and no physical pipelines.

- [ ] **Step 3: Reuse the physical hash join operator per step**

Open each root child from the same copied `QuerySnapshot`. Fold through `steps`: the accumulated stream and new root stream are passed to the existing physical hash join implementation with the step's build side, logical-side output mapping, memory account, cancellation, stats, and configured spill directory.

- [ ] **Step 4: Project and apply owned sinks**

After the last join, use `ProjectColumnsResultStream` for `final_output_columns`, then apply aggregate/distinct/sort sinks exactly once using the multi-plan's typed ownership.

- [ ] **Step 5: Verify runtime suite**

Run the four new tests plus all `PhysicalHashJoin*`, blocking spill, and result ownership tests.

---

### Task 4: Executor Routing and Fallback Removal

**Files:**
- Modify: `src/tcypher/executor.cc`
- Modify: `include/cedar/tcypher/executor.h`
- Test: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: pinned planning envelope and multi-join planner/runtime.
- Produces: typed execution for connected relationship-free `MATCH` counts of three or more.

- [ ] **Step 1: Write routing RED tests**

Add `TcypherExecutorTest.ThreeRootExplainReportsTypedJoinOrderAndStatistics` and `TcypherExecutorTest.ConnectedThreeRootQueryDoesNotUseHandwrittenMaterialization`. Add query-local counters `physical_multi_join_builds` and `legacy_multi_root_materialized_rows`; assert the former is one and the latter remains zero.

- [ ] **Step 2: Run RED**

Expected: EXPLAIN uses the logical fallback and the legacy materialization counter is nonzero or the physical counter is absent.

- [ ] **Step 3: Route planning and execution**

Build per-binding estimates from the pinned statistics snapshot, create `PhysicalMultiHashJoinPlan` before fallback dispatch, support ordinary EXPLAIN/EXPLAIN ANALYZE, and call `OpenPhysicalMultiHashJoinRuntime` for execution.

- [ ] **Step 4: Keep the compatibility fallback narrow**

`ExecuteMultiRootMatchAsOf` remains only for disconnected Cartesian queries and syntax not yet represented by the typed plan. Connected accepted shapes must not reach it.

- [ ] **Step 5: Full verification**

Run:

```bash
cmake --build build-v2 --target test_correctness_kernel -j4
build-v2/tests/test_correctness_kernel
ctest --test-dir build-v2 --output-on-failure
git diff --check
```

Require zero failures. Record constraint debt separately; do not mix the later relationship-input and plan-cache stages into this slice.
