# Temporal Index Candidate Completeness Oracle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic randomized corpus proving that indexed and hybrid candidate paths produce the same final rows and event provenance as an independent full-event scan across bitemporal histories.

**Architecture:** Build one query snapshot containing multiple immutable indexed sources plus a MemTable delta source, then execute each query twice: once with the index catalog/sources and once with those advisory sources removed. Compare canonicalized typed result rows for point, valid-time range, valid-time changes, system-time changes, and combined system/valid predicates. The base event stream remains authoritative in both executions.

**Tech Stack:** C++17, GoogleTest, Cedar T-Cypher executor, `IndexSidecar`, `MemtableDeltaIndex`, `CommitTimeline`.

## Global Constraints

- Preserve internal database format version `1`.
- Do not add old-layout readers, external V2/Vn names, or compatibility fallbacks.
- Use deterministic seed `0xCE4A20260723ULL` and print seed/case/query on failure.
- Use `-j1` for builds and tests.
- Preserve the dirty worktree; do not stage, commit, reset, clean, or push.

---

### Task 1: Randomized indexed-versus-full-scan oracle

**Files:**
- Modify: `tests/test_correctness_kernel.cc`
- Modify after verification: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify after verification: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: `ExecuteTcypher(const std::string&, TcypherExecutionContext)`, `BuildIndexCandidateSidecar`, `MemtableDeltaIndex::Rebuild`, `CommitTimeline::Append`.
- Produces: GoogleTest `TcypherExecutorTest.RandomizedIndexCandidatesMatchFullEventScanAcrossBitemporalQueries`.

- [x] **Step 1: Add the deterministic randomized oracle test**

Generate existence rows for at least 64 vertices and randomized property histories containing out-of-order `valid_from`, equal valid times with increasing `commit_seq`, PUT/DELETE/restore operations, two immutable sources, and one delta source. Build an active equality index and complete fragments for both immutable sources.

The test must run at least these query shapes with multiple point/range bounds:

```cpp
const std::vector<std::string> queries = {
    "AT VALID_TIME 15 MATCH (n) WHERE n.name = 'target' "
    "RETURN n, n.name, valid_from(n), system_time(n);",
    "FOR VALID_TIME BETWEEN 5 AND 25 MATCH (n) WHERE n.name = 'target' "
    "RETURN n, n.name, valid_from(n), valid_to(n), system_time(n);",
    "CHANGES FOR VALID_TIME BETWEEN 5 AND 25 MATCH (n) "
    "WHERE n.name = 'target' RETURN n, n.name, operation(n), "
    "valid_from(n), system_time(n);",
    "CHANGES FOR SYSTEM_TIME BETWEEN 0 AND 9999999999999999 MATCH (n) "
    "WHERE n.name = 'target' RETURN n, n.name, operation(n), system_time(n);",
};
```

Drain both streams into a stable textual representation containing every typed cell and compare the sorted row vectors. Assert the indexed execution actually processed nonzero candidates for at least one selective case and that the delta source was probed.

- [x] **Step 2: Run the focused test and treat any mismatch as a production defect**

Run:

```bash
cmake --build build-current -j1 --target test_correctness_kernel
./build-current/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.RandomizedIndexCandidatesMatchFullEventScanAcrossBitemporalQueries'
```

Expected: one test passes. If a mismatch occurs, preserve the printed seed/case/query and fix only the candidate completeness or base-validation path responsible for the false negative/incorrect provenance.

- [x] **Step 3: Run the temporal-index focused regression set**

Run:

```bash
./build-current/tests/test_correctness_kernel \
  --gtest_filter='TcypherExecutorTest.*Index*:DurableLogTest.Tcypher*Index*:IndexSidecarTest.*:MemtableDeltaIndexTest.*'
```

Expected: all selected tests pass with zero failures.

- [x] **Step 4: Update closure evidence accurately**

Mark Temporal Index completion item 3 implementation evidence COMPLETE only when the randomized oracle passes. Keep its release/artifact status MISSING until a durable corpus is archived. Record the seed, covered query shapes, focused counts, and the fact that no release artifact was claimed.

- [x] **Step 5: Verify hygiene**

Run:

```bash
git diff --check -- tests/test_correctness_kernel.cc \
  docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md \
  .superpowers/sdd/progress.md
```

Expected: exit code 0 and no output.
