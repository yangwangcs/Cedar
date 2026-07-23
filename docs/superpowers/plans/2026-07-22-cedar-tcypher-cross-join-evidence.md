# Cedar T-Cypher Disconnected Cross-Join Evidence

Date: 2026-07-22
Branch: `codex/cedar-v2-unified`

## Scope

The disconnected point-state multi-root shape is now represented by explicit
`PhysicalOperatorKind::kCrossJoin` steps in the typed multi-join plan. Keyed
steps remain `kHashJoin`; an empty key vector is legal only for a cross step.
The planner composes connected and disconnected components, while the strict
validator rejects unknown operators, keyed cross joins, and empty-key hash
joins. EXPLAIN and EXPLAIN ANALYZE expose `HashJoin` and `CrossJoin` distinctly.

Execution uses the physical cross-join runtime, not the legacy materialization
stream. It chooses a replay/build side using bounded sampling, retains only the
replay side, streams Cartesian output in bounded batches, and supports soft-limit
spill, cancellation, cleanup, and memory-account lifetime through returned
result batches. Scalar, struct, list, empty-child, and composite rows are
covered, including spill replay and mixed hash/cross multi-join plans.

## Constraint coverage

- Owned scalar/struct/list payload size is estimated before reservation and copy.
- Struct/list/dictionary/slice vectors are referenced without staging copies in
  the spill path.
- Decoded spill batches are released before the next batch is consumed.
- Replay rows check cancellation and release per-row charges after destruction.
- Output accumulator, typed vectors, validity metadata, retained rows, and
  capacity growth are charged before allocation; the lease follows the returned
  `ResultBatch`.
- Spill files and retained containers release rows before their accounting
  owners; spill cleanup is exercised on success, cancellation, and failure.
- Cross-join runtime counters include builds, replay/stream rows, output rows,
  spill starts/bytes, and adaptive build-side switches.

## RED/GREEN evidence

- Two-root local predicate initially used legacy materialization (RED); the
  corrected physical child-filter routing now executes the typed physical plan
  (GREEN).
- Composite owned-payload estimator tests caught undercharging for struct/list
  values (RED); the estimator and reference accessors now pass (GREEN).
- Fixed-width and empty-list output peak tests initially returned OK under the
  hard limit (RED); pre-reserved output accounting now returns
  `QueryMemoryLimit` (GREEN).
- Retained-container capacity and outer-buffer growth tests reproduced leaked
  capacity/under-accounting (RED); reserve-before-growth and destruction-before-
  release now pass (GREEN).
- Near-hard-limit spill, cancellation, structured/list replay, and empty-child
  cases pass with deterministic fixed-seed coverage.

## Verification

The rebuilt correctness kernel and CTest matrix each report **656/656 tests
passed**. The same 656-test matrix passed under ASAN, UBSAN, and TSAN; TSAN
completed with no race report. `git diff --check` and explicit trailing-
whitespace scans over all touched cross-join/runtime/test files are clean.

## Remaining minor notes

1. `PrepareOutputAccumulator()` uses one `sizeof(std::vector<Value>)` charge for
   the three typed vector classes; this is equal on the current ABI but is not a
   cross-STL guarantee.
2. The conservative vector charge assumes a copy-constructed vector capacity no
   larger than its size; a stricter cross-implementation bound could charge
   capacity instead.
3. A dedicated cancellation injection during the initial spill migration is
   not yet present; cancellation during replay and normal cleanup are covered.

These are non-blocking release follow-ups and do not alter the confirmed design.
