# Cedar CBO Expand Dynamic Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep advisory index candidates in their binding domain so root predicates restrict root scans and downstream node/relationship predicates dynamically filter Expand work without false negatives.

**Architecture:** Resolve each physical predicate's owning `BindingId` from `PhysicalPropertySlot`. Select every legal indexed predicate independently, build candidate intersections per binding, and retain those exact candidate sets for the query lifetime. Apply the root binding set to `TemporalScanSpec::allowed_candidate_entity_ids`. Before target existence validation, filter Expand edge rows by the target binding's vertex candidates and the relationship binding's edge-ID candidates. Final PropertyGather and Filter continue validating every predicate.

**Tech Stack:** C++17, physical root runtime, immutable index sidecars, version-chain MemTable delta indexes, fixed/multi-hop `VectorExpand`, GoogleTest.

## Global Constraints

- Candidate IDs from one binding are never applied to another binding.
- Unindexed predicates remain ordinary gathered filters and do not disable usable indexes on other predicates.
- Root, relationship, and target candidate sets remain advisory; base temporal validation is mandatory.
- Point Expand is implemented first in this plan; range/change candidate pushdown remains on its existing non-index path.
- Sidecar/delta preparation, cancellation, memory leases, and cleanup remain query-owned.
- Full fault and sanitizer matrices remain deferred.
- This dirty worktree must not be reset, cleaned, committed, or pushed.

---

### Task 1: Reproduce binding-domain false negative

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

- [x] Build `1 -[:KNOWS]-> 2`, index `b.city`, and query `WHERE b.city='Paris'`.
- [x] Assert vertex `2` is returned.
- [x] Run focused test and observe `NotFound` because candidate `{2}` restricts root `a`.

### Task 2: Independent indexed predicate selection

**Files:**
- Modify: `src/tcypher/runtime/query_runtime.cc`

- [ ] Initialize selected definition slots for every plan predicate instead of push-order storage.
- [ ] Skip unsupported/unindexed predicates while retaining every legal indexed predicate.
- [ ] Resolve each indexed predicate's binding from `plan->predicate_properties()`.
- [ ] Validate SST and MemTable coverage only for selected definitions.

### Task 3: Per-binding candidate intersections

**Files:**
- Modify: `src/tcypher/runtime/query_runtime.cc`

- [ ] Track active predicate ordinal and count within each binding.
- [ ] Key candidate progress by `(binding, entity_id)`.
- [ ] Finalize exact candidate sets independently for every binding.
- [ ] Preserve density adaptation per predicate and never drop the last indexed predicate of a binding merely because another binding has an index.
- [ ] Install only the root binding candidate set on the root temporal scan.

### Task 4: Dynamic Expand filtering

**Files:**
- Modify: `include/cedar/tcypher/executor.h`
- Modify: `src/tcypher/runtime/query_runtime.cc`
- Modify: `include/cedar/observability/explain_analyze_profile.h`
- Modify: `src/observability/explain_analyze_profile.cc`

- [ ] Intersect single-hop target IDs with the target binding candidate set before target existence scans.
- [ ] Filter edge rows by relationship edge-ID candidates when present.
- [ ] Apply the same binding-domain filter at each fixed multi-hop Expand step.
- [ ] Count dynamic-filter input, rejected, and surviving rows in execution stats and EXPLAIN ANALYZE.

### Task 5: Regression

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify: `.superpowers/sdd/progress.md`

- [ ] Add source-plus-target indexed predicates proving independent domains.
- [ ] Add unindexed source predicate plus indexed target predicate.
- [ ] Add relationship-property/index candidate coverage if edge index identity supports exact edge IDs.
- [ ] Run focused advisory/Expand/CBO/Explain tests, normal CTest, and hygiene checks.
- [ ] Keep dynamic Join filters, next-pipeline reoptimization, and frontier repartitioning pending.
