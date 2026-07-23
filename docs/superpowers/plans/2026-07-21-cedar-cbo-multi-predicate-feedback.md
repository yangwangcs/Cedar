# Cedar CBO Multi-Predicate Feedback Bucketing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Isolate multi-predicate runtime feedback variants by the best legal indexed predicate selectivity instead of classifying every variant from the first predicate alone.

**Architecture:** Inspect every physical predicate under the pinned catalog/statistics envelope. Estimate equality/IN candidate cardinality for each legal active index, select the minimum as the single-index candidate estimate, and use it for `SelectivityBucket` classification. Preserve the full normalized predicate shape and all schema epochs in the feedback key. When two estimates are available, populate the bounded cost model's intersection inputs conservatively without changing correctness or forcing an intersection choice.

**Tech Stack:** C++17, pinned IndexCatalog/StatsSnapshot, RuntimeFeedbackStore, GoogleTest.

## Global Constraints

- Runtime feedback remains advisory and snapshot-scoped.
- Literal values are not retained in feedback keys.
- Missing or unsupported predicate estimates are ignored; no estimate means no feedback preparation.
- Final predicate validation and temporal visibility are unchanged.
- Full fault and sanitizer matrices remain deferred.
- This dirty worktree must not be reset, cleaned, committed, or pushed.

---

### Task 1: RED test for combined selectivity

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

- [x] Create 100 vertices where `name='Ada'` matches all rows and `city='Paris'` matches one row.
- [x] Register active equality indexes for both columns and execute the two-predicate query once.
- [x] Assert the runtime feedback key is `very_selective`, proving the second predicate participates in classification.
- [x] Run the focused test and observe the current `non_selective` bucket.

### Task 2: Multi-predicate candidate estimates

**Files:**
- Modify: `src/tcypher/executor.cc`

- [x] Parameterize candidate estimation by physical predicate index instead of always reading `predicates().front()`.
- [x] Find legal active index definitions for every predicate using column identity, schema epoch, canonical encoding, and required capability.
- [x] Use the minimum observed candidate count as `index_candidate_rows` and classify the feedback key from that value.
- [x] Populate two-input intersection estimate fields when at least two predicate estimates are known.
- [x] Run the RED test and existing selective/non-selective feedback tests to GREEN.

### Task 3: Regression and progress

**Files:**
- Modify: `.superpowers/sdd/progress.md`

- [x] Run focused runtime-feedback, advisory intersection, CostModel, and Explain Analyze tests.
- [x] Run normal CTest and hygiene checks.
- [x] Record multi-predicate feedback isolation and keep downstream dynamic filters, next-pipeline replanning, and frontier repartitioning pending.
