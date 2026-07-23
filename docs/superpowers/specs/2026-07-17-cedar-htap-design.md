# Cedar HTAP Correctness Kernel Design

Date: 2026-07-17

Status: Approved authoritative design; functional implementation substantially complete; release/paper closure remains incomplete and is tracked in `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

## 1. Purpose

Cedar is a single-node temporal graph HTAP storage engine built around unified temporal events, in-memory version chains, Zone-Columnar SSTs, and low-write-amplification size-tiered compaction. This design defines the first implementation stage: a correctness kernel that makes those ideas durable and transactionally sound without replacing Cedar's data model.

The first stage adds:

- durable snapshot isolation by default;
- opt-in strict serializability for short, exact-key, multi-entity transactions;
- bitemporal visibility using `valid_time` and persisted `commit_seq`;
- sharded OCC, per-shard WALs, and a global transaction decision log;
- atomic flush, Manifest, recovery, tombstone, and compaction behavior;
- stable analytical snapshots across flush, compaction, and restart.

The design deliberately does not preserve the existing on-disk format. A database created with an older format version is rejected with a diagnostic instead of entering a compatibility path.

## 2. Motivation and Current Risks

The current repository demonstrates the intended storage layout, but its control plane cannot yet provide the required database guarantees. The first-stage design directly addresses these observed risks:

- ordinary writes can bypass WAL;
- flush can discard tombstones;
- range reads inspect only a bounded subset of relevant SSTs;
- compaction can delete overlapping files that were not merged;
- compaction groups are not consistently separated by column and entity type;
- disk reads can reconstruct edge records as vertex keys and lose `target_id`;
- the SST reader loads complete files into memory;
- the VSL insertion path is not the lock-free CAS algorithm its naming suggests;
- duplicated metadata and storage components provide no single durable source of truth;
- tests do not establish crash, reopen, compaction, tombstone, out-of-order, or concurrency correctness.

The first stage fixes correctness and ownership. Page-level I/O and execution optimizations follow in later stages.

## 3. Goals and Non-Goals

### 3.1 Goals

1. Never acknowledge a transaction whose committed data can be lost after restart.
2. Never expose a partial multi-shard transaction.
3. Provide strict serializability, including real-time ordering, for supported strong transactions.
4. Preserve out-of-order valid-time history and transaction-time visibility through flush, compaction, and restart.
5. Preserve tombstones until a future retention policy proves they are safe to remove.
6. Keep analytical scans on stable snapshots while ingestion and short transactions continue.
7. Retain the existing public columnar read/write shape during the correctness stage.
8. Preserve Cedar's immutable-event and low-write-amplification design intent.

### 3.2 Non-Goals

- Serializable range scans, adjacency expansion, or predicate reads.
- Online resharding or changing the shard hash after database creation.
- Backward-compatible reads or automatic migration of the old disk format.
- Page/granule encoding, vectorized execution, late materialization, or a buffer manager.
- Historical version or tombstone garbage collection.
- A claim of lock-free MemTable insertion.
- Replacing size-tiered compaction with leveled compaction.

## 4. Core Terms and Invariants

| Term | Meaning |
|---|---|
| `logical_key` | A stable key for one existence fact or property, such as `(Vertex, id, $existence)` or `(Vertex, id, property)` |
| `valid_from` | Application/event time at which an immutable event begins to apply |
| `commit_seq` | Globally ordered transaction-time sequence assigned to a durable commit decision |
| `snapshot_seq` | The `visible_seq` captured when a transaction or query begins |
| `visible_seq` | Largest continuous `commit_seq` prefix fully installed for reads |
| `txn_id` | Globally unique transaction identity, stable across retries and recovery |
| `PREPARE` | Durable per-shard transaction fragment that is not yet committed |
| `COMMIT` | Durable global decision referencing every participant's prepare record |

The following invariants are mandatory:

1. All versions of one `logical_key` map to one stable shard.
2. A transaction is committed only if its DecisionLog `COMMIT` and every referenced prepare fragment are durable and valid.
3. No prepared fragment is visible before its global commit decision.
4. A transaction is not acknowledged until `visible_seq >= commit_seq`.
5. Readers observe only a continuous committed prefix, never an arbitrary set of durable decisions.
6. Manifest state is the only authority for live SST files.
7. Every file removed by compaction was included in the merge represented by the same atomic Manifest edit.
8. Flush and compaction preserve `PUT`, `DELETE`, `valid_from`, and `commit_seq`.

## 5. Architecture

### 5.1 Component Boundaries

The current monolithic `LsmEngine` responsibilities are split into explicit components:

- `TransactionManager`: transaction lifecycle, snapshot capture, read/write sets, validation, and result reporting.
- `ShardDirectory`: stable hash from `logical_key` to shard and persisted shard configuration.
- `StorageShard`: one MemTable family, interval conflict index, prepared reservation table, shard WAL, and shard-local SST set.
- `CommitSequencer`: serial DecisionLog append order and global `commit_seq` assignment.
- `DecisionLog`: durable `COMMIT` and `ABORT` decisions for multi-shard transactions.
- `TxnOutcomeIndex`: durable `txn_id -> outcome` lookup retained across DecisionLog truncation.
- `VersionSet`: immutable view of all live SST metadata.
- `Manifest`: append-only, checksummed VersionSet edits and checkpoints.
- `RecoveryManager`: format validation, log replay, SST verification, and visible-prefix reconstruction.
- `SnapshotRegistry`: pins VersionSets and tracks live `snapshot_seq` values.

`CedarGraphStorage` remains the public facade. Existing single-operation APIs become autocommit transactions. An explicit transaction API supports atomic operations across vertices, edges, and properties.

### 5.2 Stable Sharding

The database format metadata persists:

- format version;
- shard count;
- hash algorithm and seed;
- shard WAL locations;
- DecisionLog and Manifest locations.

The first stage uses a fixed shard count. A shard has a mutation latch protecting the current version-chain MemTable and reservation structures. Different shards validate, persist, and install concurrently. This is sharded concurrency, not a lock-free VSL claim.

## 6. Temporal Data and Read Semantics

### 6.1 Immutable Events

The durable logical record, materialized only after a commit decision assigns its sequence, is:

```text
TemporalEvent {
  logical_key,
  valid_from,
  commit_seq,
  operation: PUT | DELETE,
  value
}
```

Before that decision, a prepare record contains `PendingEvent` values with the same logical key, valid time, operation, and value, but no `commit_seq`. The DecisionLog supplies the sequence when recovery or publication materializes the final `TemporalEvent`.

Existence and properties use separate logical keys. A multi-property change is atomic because the containing transaction commits all event records together.

### 6.2 Version-Chain MemTable Adaptation

Cedar retains the original CedarMemTable/VSL idea that one logical fact owns a complete in-memory history, but adapts its identity and ordering to the immutable bitemporal event model:

```text
TemporalMemTable {
  ordered_map<LogicalKey, TemporalVersionChain>
}

TemporalVersionChain order:
  valid_from DESC
  commit_seq DESC
```

The `LogicalKey` identifies the fact timeline and includes complete canonical vertex or edge identity. Directional `EdgeIn` and `EdgeOut` views of the same edge map to the same logical chain. `valid_from` and `commit_seq` identify an event within that chain; `schema_epoch`, operation, and value are immutable event content.

An exact replay of the same `(LogicalKey, valid_from, commit_seq)` and content is idempotent. The same identity with different schema, operation, value, or Blob content identity is corruption. A relocated Blob hint does not change event content identity.

The active and frozen MemTables retain complete `TemporalEvent` values, including tombstones and out-of-order events. They do not reuse the old `Descriptor`, 16-bit sequence, raw-pointer linked nodes, or `DeltaVersionChain` value compression. Delta and dictionary encoding belong to immutable SST/page codecs, where reconstruction cost and corruption boundaries can be controlled.

Readers pin immutable MemTable generations. If a pinned active generation exists, the next committed install uses copy-on-write before mutation. Freeze transfers one complete generation to the frozen slot and creates a new active generation; the frozen generation is released only after its SST is Manifest-live. Cursors traverse pinned generations directly in `(LogicalKey, valid_from DESC, commit_seq DESC)` order without materializing a second full snapshot.

This is the MemTable semantic contract. Its internal ordered container may later be replaced by a tested skip-list or arena-backed chain without changing event identity, visibility, replay, cursor, or flush behavior. No implementation may claim lock-free progress unless insertion and reclamation satisfy a separately verified lock-free algorithm.

### 6.3 Implicit Valid-Time Intervals

An event at `valid_from = t` applies over the half-open interval:

```text
[t, next_visible_valid_from)
```

The successor is derived from the next distinct `valid_from` for the same key in the selected transaction-time snapshot. An out-of-order event splits the previous interval logically; it never rewrites the previous event in place.

A `DELETE` event starts an interval of absence. A later `PUT` makes the key visible again. Flush and compaction retain both events.

### 6.4 Bitemporal Selection

For `Get(logical_key, valid_time)` at `snapshot_seq`, the read path merges candidates from the active MemTable, frozen MemTables, and every relevant SST, then applies:

```text
commit_seq <= snapshot_seq
valid_from <= valid_time
maximum valid_from
for equal valid_from, maximum commit_seq
apply PUT or DELETE
```

This rule must return the same result before and after flush, compaction, and restart.

### 6.5 Edge Visibility

At a requested valid time, an edge is logically visible only when all three facts are visible:

```text
edge existence
AND source vertex existence
AND target vertex existence
```

The edge's effective interval is their intersection. Vertex deletion does not cascade or rewrite adjacency data. Restoring a vertex can make an existing edge visible again. A strong exact-key edge read records read dependencies for the edge and both endpoint existence keys.

Disk records must preserve complete edge identity, including source, target, type/label, and edge identifier where applicable. No read path may reconstruct an edge as a vertex key.

## 7. Isolation and OCC Validation

### 7.1 Transaction Modes

`SNAPSHOT_ISOLATION` is the durable default. It uses the same atomic commit protocol but validates write/write conflicts only.

Snapshot-isolated transactions do not create read reservations, but every writer must honor an existing strong transaction's read and write reservations. This prevents a weaker transaction from overtaking and invalidating a prepared strict transaction. The strict guarantee covers a strong transaction relative to all committed writes; snapshot-isolated transactions are not thereby made mutually serializable.

`STRICT_SERIALIZABLE` additionally tracks and validates exact-key reads. It supports arbitrary valid-time reads and out-of-order writes. Commit order is determined only by `commit_seq`, not by `valid_from`.

Strong transactions reject range scans, adjacency expansion, and predicate reads with `UnsupportedSerializablePredicate`. The engine never silently downgrades isolation. Analytical scans and traversals use stable snapshot isolation.

### 7.2 Validation Records

Strong transactions record:

```text
ReadPoint(logical_key,
          valid_time,
          observed_event,
          predecessor_fence,
          successor_fence)

WriteInterval(logical_key,
              [valid_from, next_valid_from))
```

During prepare, the transaction enters validation gates in ascending shard order and re-evaluates each read and write boundary against the latest visible state plus prepared reservations.

A read conflicts with any later event that changes its result. Two writes conflict only when they address the same logical key and their derived valid-time intervals overlap. Disjoint intervals can validate and persist concurrently.

### 7.3 Cross-Shard Reservations

After successful validation, the transaction installs all read and write reservations while holding shard gates in canonical order. Partial installation is rolled back before any prepare WAL write. Once installed, reservations remain until abort or until a committed transaction enters the visible prefix. After a durable commit decision, they change from prepared to committed-pending reservations but continue to participate in validation.

The first-stage conflict policy is `first-prepared-wins`. A later conflicting transaction receives retryable `SerializationConflict` instead of waiting across WAL fsync operations. This prevents deadlock and cross-shard wait cycles.

Prepared reservations also prevent a conflicting transaction from receiving an earlier DecisionLog position and invalidating the serialization order. Non-conflicting transactions may overtake one another before decision logging because their relative order cannot change results.

## 8. Sharded WAL and Global Decision Protocol

### 8.1 Per-Shard Prepare Records

Each participating shard writes a checksummed record containing:

```text
PREPARE {
  txn_id,
  snapshot_seq,
  participant_shards,
  pending_events[],
  event_indices[],
  payload_checksum
}
```

The record's segment and LSN identify it during global decision and recovery.

### 8.2 Global Decisions

After every prepare record is fsynced, the sequencer allocates `commit_seq` in DecisionLog append order and writes:

```text
COMMIT {
  txn_id,
  commit_seq,
  [(shard_id, prepare_lsn, prepare_checksum), ...],
  record_checksum
}
```

Decision records may use group commit. All prepare fsync completions precede the corresponding DecisionLog fsync.

### 8.3 State Machine

```text
ACTIVE
  -> VALIDATING
  -> PREPARED_IN_MEMORY
  -> PREPARE_DURABLE
  -> DECISION_DURABLE
  -> INSTALLED
  -> VISIBLE
```

Before `DECISION_DURABLE`, a failure can produce an abort. After `DECISION_DURABLE`, the transaction is durably committed and must be recovered even if publication or the client response fails.

The fsynced DecisionLog record is the durable decision point and fixes serialization order. Inclusion in the installed continuous `visible_seq` prefix is the externally observable linearization point. It occurs before a successful response.

### 8.4 Parallel Installation and Visible Prefix

Committed fragments install into participant MemTables in parallel. Installation is idempotent using `(txn_id, event_index)` together with `commit_seq`.

If sequence 102 installs before 101, the engine cannot advance `visible_seq` to 102. A transaction waits for all preceding committed sequences to install before returning success. This head-of-line rule is required for snapshots to represent a total serial prefix.

Read-only strong transactions linearize when they capture `visible_seq`. They do not write WAL.

## 9. Commit Results and Failure Semantics

The API exposes three commit outcomes:

- `Committed(commit_seq)`: durable and included in the visible prefix.
- `Aborted(reason)`: no durable commit decision exists; retry is safe.
- `Indeterminate(txn_id)`: the durability outcome cannot be established by the caller's failed operation.

`ResolveTransaction(txn_id)` checks the live DecisionLog and the checkpointed `TxnOutcomeIndex`. A caller must resolve an indeterminate transaction instead of creating a different transaction that could duplicate effects. The first stage retains committed outcomes indefinitely; outcome retention is changed only by a later explicit policy.

Once a commit decision is durable, a publication failure cannot be reported as abort. The engine enters a recovery-required state if it cannot finish installation safely. It stops accepting operations that could observe or extend inconsistent in-memory state.

I/O errors before the global decision abort the transaction and release reservations after best-effort cleanup. Prepare records without decisions are harmless recovery-time garbage.

## 10. Recovery

Recovery performs these steps in order:

1. Read format metadata and reject unsupported versions.
2. Replay the Manifest to its last valid record and construct the live VersionSet.
3. Verify every referenced SST exists and passes structural/footer checks.
4. Load the most recent durable checkpoint, its transaction outcome index, and its global committed prefix.
5. Scan the retained DecisionLog suffix and validate record checksums and sequence continuity.
6. Resolve every commit reference against the corresponding shard prepare LSN and checksum.
7. Replay committed transaction fragments in `commit_seq` order, idempotently skipping data already covered by the checkpoint.
8. Discard prepare fragments without a retained commit decision.
9. Reconstruct interval indexes, reservation-free MemTables, and the final `visible_seq`.

Only an incomplete final record caused by a torn append may be truncated. Corruption in an earlier verified region, a missing live SST, or a retained `COMMIT` with a missing/mismatched prepare reference prevents normal startup. The engine reports corruption instead of guessing a repair.

Segment creation and rotation fsync both the file and containing directory as required by the filesystem durability contract.

## 11. Flush, Manifest, and WAL Checkpoints

### 11.1 MemTable Lifecycle

Each shard owns:

```text
ActiveMemTable -> FrozenMemTable queue -> SST files
```

Only committed events enter this lifecycle. Prepared fragments and reservations are separate and cannot be flushed.

### 11.2 Flush Protocol

```text
write temporary SST
write footer and checksum
fsync SST
atomic rename to final name
fsync directory
append Manifest AddFile edit
fsync Manifest
release FrozenMemTable
```

An SST contains complete temporal records and metadata for shard, entity kind, column group, key bounds, valid-time bounds, sequence bounds, and checksums.

On recovery, complete output files absent from the Manifest are orphans and can be deleted. Manifest-referenced files that are absent or invalid are corruption.

### 11.3 Safe Log Truncation

Each shard tracks a `wal_safe_lsn` representing a persisted prepare-log prefix whose committed events are covered by Manifest-referenced SSTs. A global checkpoint can advance to `checkpoint_seq` only when every transaction in that continuous commit prefix is covered on all participant shards.

The Manifest checkpoint persists:

- `checkpoint_seq`
- per-shard `wal_safe_lsn`
- DecisionLog safe position
- current VersionSet identity
- a checksummed immutable `TxnOutcomeIndex` file covering every decision in the truncated prefix

Only after this checkpoint and its outcome index are durable may covered shard WAL segments and the covered DecisionLog prefix be deleted. For every retained decision record, its referenced prepare record must remain available. `ResolveTransaction` searches both the retained suffix and checkpointed outcomes.

## 12. Compaction Correctness

The first stage retains size-tiered compaction. Candidate selection is partitioned by:

```text
(shard, entity_kind, column_group)
```

Files from different partitions never enter one merge. When a candidate overlaps files in its destination scope, the compactor computes the complete transitive overlap closure. Every file listed in `DeleteInputs` must have been opened, validated, and merged.

Compaction writes and fsyncs all output files before appending one atomic Manifest edit:

```text
AddOutputs + DeleteInputs
```

Old inputs are unlinked only after the Manifest edit is durable. Their physical deletion is additionally delayed while a pinned VersionSet references them.

The first stage does not discard overwritten versions or tombstones. Future garbage collection requires both an explicit historical retention policy and proof based on the oldest live snapshot and complete lower-level coverage.

Range queries enumerate every relevant file from the pinned VersionSet. No hidden maximum SST count is permitted.

## 13. Snapshot and File Lifetime

Every transaction and analysis query owns:

```text
ReadSnapshot {
  snapshot_seq,
  pinned_version_set
}
```

Compaction publishes a new immutable VersionSet without changing existing readers. Reference counting or epoch reclamation keeps old files alive until the last reader releases its VersionSet.

`SnapshotRegistry` records the oldest active `snapshot_seq`. The first stage uses it for observability and future-proofing, not version deletion.

Analytical scans and graph traversals can run concurrently with ingestion. They remain snapshot-isolated and do not install exact-key serializable reservations.

## 14. Format and File Ownership

A new database starts with an atomically created format metadata file. Suggested ownership is:

```text
FORMAT
manifest/
decision/
outcomes/
shards/<id>/wal/
shards/<id>/sst/
```

Every durable file has a magic value, format version, identity, length or footer boundary, and checksum. File naming is not authoritative; Manifest and log contents are.

The implementation must select one authoritative Manifest/VersionSet implementation and one authoritative WAL/Blob ownership model. Duplicate legacy metadata or Blob paths are not kept active behind runtime switches.

## 15. Relationship to Mainstream Column Stores

| System/design | Cedar adopts | Cedar intentionally keeps different |
|---|---|---|
| RocksDB | WAL sequencing, VersionSet/Manifest, snapshot watermarks, atomic compaction edits, conservative tombstone rules | Bitemporal graph events and size-tiered low-WA layout instead of ordinary row KV and default leveled compaction |
| ClickHouse | Immutable part lifecycle, marks/granules, statistics, atomic part publication | Continuous per-event transactions rather than batch-only part replacement |
| Parquet/ORC | Page checksums, codecs, dictionaries, and per-page statistics in stage two | Transaction visibility and version merging remain storage-engine responsibilities |
| DuckDB | Vectorized execution, late materialization, and buffer management in stage three | Cedar remains an LSM temporal engine rather than a single-file update architecture |
| Kudu/C-Store | Mutable delta layer plus stable columnar layer and scheduled maintenance | Cedar's immutable temporal events and VSL-to-SST flow remain primary |

The staged adoption plan is:

1. Correctness kernel: RocksDB-like control-plane rigor and ClickHouse-like immutable file ownership.
2. Columnar read kernel: granules/pages, bounded `pread`, page checksums, codecs, dictionaries, and data-skipping metadata.
3. HTAP execution: vectors, late materialization, buffer manager, batch adjacency expansion, and richer indexes.

This order absorbs mature mechanisms without replacing Cedar's research contribution.

## 16. Verification and Acceptance Criteria

### 16.1 Model and Property Tests

- Compare randomized out-of-order `PUT`, `DELETE`, correction, and resurrection histories with a simple bitemporal reference model.
- Check every selected `(snapshot_seq, valid_time)` before/after flush, compaction, and restart.
- Generate equal-`valid_from` corrections and ensure `commit_seq` tie-breaking is stable.
- Verify edge visibility equals the intersection of edge and endpoint existence.

### 16.2 Transaction Tests

- Overlapping valid-time intervals conflict; disjoint intervals can commit concurrently.
- Read/write, write/write, write-skew, and multi-shard dependency cycles cannot violate strict serializability.
- Real-time order is respected after a successful commit response.
- Snapshot-isolated transactions remain atomic but do not acquire serializable read reservations.
- Unsupported predicates in strong transactions fail explicitly.

### 16.3 Crash and Corruption Tests

- Inject a crash at every prepare WAL, DecisionLog, SST, rename, Manifest, and file-deletion boundary.
- Reopen after every injected crash and compare against the durable decision oracle.
- Verify prepare-only transactions disappear and committed transactions replay fully.
- Test partial installation, idempotent replay, torn tails, bad checksums, missing prepares, and missing SSTs.
- Resolve committed transaction outcomes after their original DecisionLog records have been checkpointed and deleted.
- Verify old format directories are rejected deterministically.

### 16.4 Compaction and Snapshot Tests

- Compact real overlapping files and verify every deleted input was merged.
- Hold old snapshots while publishing and deleting new VersionSets.
- Preserve all tombstones and historical versions.
- Read more than ten relevant SSTs and return complete results.

### 16.5 Concurrency and Tooling

- Run deterministic concurrency schedules and randomized stress histories through a serializability checker.
- Run ASan, UBSan, and TSan configurations.
- Measure single-shard and multi-shard scaling, conflict abort rate, prepare/decision fsync latency, visible-prefix stalls, and write amplification.

Correctness gates are mandatory. Performance results are reported comparatively until a specific hardware profile and workload define numeric targets.

## 17. Deferred Decisions

The following decisions belong to later design cycles and must not be inferred during first-stage implementation:

- retention duration and user-facing system-time history policy;
- page/granule size and encoding selection;
- buffer manager and cache replacement;
- data-skipping and adjacency index formats;
- online resharding;
- conversion from old disk formats;
- transaction outcome retention and compaction;
- promotion from first-prepared-wins to waiting or priority-based conflict handling;
- conditions for evolving from per-shard latches to a verified lock-free structure.

## 18. Completion Definition

The correctness kernel is complete only when:

1. every write path uses the transaction and durability protocol;
2. the reference-model, crash/reopen, real compaction, tombstone, and concurrency suites pass;
3. no legacy metadata path can independently publish or delete live files;
4. format rejection and corruption diagnostics are stable;
5. existing public autocommit operations work on the new kernel;
6. explicit exact-key transactions provide the approved strict-serializable semantics;
7. stable analytical snapshots run concurrently with ingestion.
