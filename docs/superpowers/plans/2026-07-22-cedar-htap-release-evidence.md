# Cedar HTAP Correctness Kernel Release Evidence

Date: 2026-07-22

Scope: `docs/superpowers/specs/2026-07-17-cedar-htap-design.md`.

Status: functional correctness and recovery closure complete for the HTAP
kernel. Paper-level comparative performance measurements remain part of the
later Observability/Benchmark stage, so the persistent six-design Goal remains
active and advances next to T-Cypher physical execution.

## Transaction And Reservation Evidence

- `DurableLogTest.StrictTransactionsRejectDeterministicWriteSkew` proves that
  exact-key strict transactions do not admit the classic two-transaction
  write-skew cycle.
- `DurableLogTest.RandomStrictDependencyCyclesNeverCommitEveryParticipant`
  runs 24 dependency-cycle cases with fixed `std::mt19937_64` seed
  `0xceda7d3e5`; every failure reports seed, case, and cycle size. No complete
  dependency cycle commits.
- `DurableLogTest.CoordinatorWriteReservationsRespectHalfOpenIntervalMatrix`
  verifies coordinator-level overlapping and disjoint valid-time writes using
  half-open intervals.
- `PreparedStrictReadReservationRejectsSnapshotWriter`,
  `AbortedPreparedStrictTransactionReleasesReservation`, and
  `CrossShardConflictLeavesNoPartialReservation` cover read/write conflicts,
  abort cleanup, and validate-all-before-install reservation behavior.
- `CrossShardValidationUsesCanonicalOrderForReverseEventInput` proves that
  reverse event input still acquires and reports shard validation gates in
  canonical order `[0, 1]`.
- `IndependentShardValidationsRunConcurrently` proves independent shard
  validation is not serialized by the global commit mutex.

## Durable Decision And Installation Evidence

- DecisionLog append order, HLC allocation, and commit timeline publication
  remain serialized under `commit_mutex_`; participant installation happens
  only after a copied durable decision has been obtained.
- `LaterDecisionCanInstallAheadButWaitsForTheVisiblePrefix` proves a later
  decision may install ahead while `visible_seq` remains behind the missing
  predecessor.
- `EarlierInstallFailureWakesAheadOfPrefixCommitAsIndeterminate` proves an
  ahead-of-prefix waiter is awakened by predecessor failure and cannot report
  committed.
- `OneCrossShardDecisionInstallsParticipantsConcurrently` deterministically
  blocks the first participant until the second arrives, proving that one
  cross-shard durable decision installs its shard fragments concurrently.
- `InstallDecision` now uses two phases: it validates every PREPARE reference
  and rejects duplicate shard references before publication, then launches one
  worker per validated participant. `VisiblePrefix::MarkInstalled` runs only
  after all workers succeed.
- Live commit installation relies on `(txn_id, event_index)` idempotence and
  does not read the Flush-owned published watermark concurrently. Recovery
  replay remains single-threaded with respect to Flush and uses Manifest
  watermarks to avoid republishing checkpoint-covered events.

## Failure And Recovery Evidence

- `CrossShardPartialPrepareSuccessRecoversWithoutPublishingOrphans` injects a
  failure after one cross-shard PREPARE fsync. Reopen publishes no orphan
  fragment, reconstructs a reservation-free state, and starts commit sequence
  assignment at 1.
- `PartialParticipantInstallFailureReplaysEveryShardOnReopen` injects a
  participant-worker failure after the global decision is durable while the
  other shard worker completes. The original call returns `Indeterminate`,
  `visible_seq` stays 0, and reopen idempotently restores both shards at
  sequence 1.
- `AmbiguousDecisionAppendRequiresRecoveryAndResolvesOnReopen` now also proves
  that reopen clears stale reservations by successfully committing a
  replacement into the same logical event slot at sequence 2.
- Existing fault matrices cover partial/full PREPARE and DecisionLog writes,
  torn-tail truncation, missing/mismatched prepare references, durable
  publication failure, transaction outcome resolution, checkpointed outcome
  retention, missing/corrupt Manifest SSTs, and atomic SST/sidecar publication.

## MemTable, Temporal, Edge, And Snapshot Evidence

- `TemporalMemTable` stores complete immutable events in per-`LogicalKey`
  version chains ordered by valid time and commit sequence. Exact replay is
  idempotent and contradictory replay is corruption.
- Active/frozen generation pinning, copy-on-write installs, flush publication,
  tombstone retention, continuation blocks, compaction, and reopen are covered
  by the MemTable, SST, compaction, and randomized bitemporal oracle suites.
- Edge identity, EdgeOut/EdgeIn normalization, endpoint visibility, strict
  endpoint dependencies, schema epoch identity, and duplicate canonical edge
  rejection are covered by the LogicalKey and durable edge tests.
- Pinned MemTables and immutable VersionSnapshots keep analytical reads stable
  across ingestion, flush, compaction, cancellation, corruption, and database
  lifetime changes.

## Final Verification

- Normal full suite: 639/639 passed, 12.24 seconds.
- ASAN full suite: 639/639 passed, 21.06 seconds.
- UBSAN full suite: 639/639 passed, 22.37 seconds.
- TSAN full suite: 639/639 passed, 66.89 seconds.
- `git diff --check`: clean.
- The first `build-v2 -j2` rebuild after expanding the large test translation
  unit was killed with exit 137 and produced no compiler diagnostic. All
  authoritative rebuilds were rerun serially with `-j1` and passed.

## Deferred Paper Measurements

The correctness kernel no longer has a known functional or recovery gap that
blocks the next design stage. The following measurements are still required
before the overall six-design Goal can claim paper completion and belong in
the final benchmark artifact loop:

- single-shard versus multi-shard throughput scaling;
- strict conflict abort rate by dependency and interval shape;
- PREPARE fsync and DecisionLog fsync latency distributions;
- visible-prefix stall duration and lag distribution under injected skew;
- transaction-path write amplification under reproducible ingestion mixes.

These measurements must use real production counters, saved workload/config
metadata, fixed seeds, and artifact verification; they cannot be inferred
from passing correctness tests.
