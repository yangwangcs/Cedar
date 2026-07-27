# Cedar Atomic Commit Design

## Objective

Replace Cedar's single-node shard PREPARE plus global Decision protocol with
**Cedar Atomic Commit (CAC)**:

> temporal-interval-aware optimistic concurrency control, one global
> `AtomicCommitLog`, adaptive group commit, and parallel idempotent shard
> installation.

The new database format is a clean break. Cedar will not open the old durable
transaction format and will not retain a dual-write or fallback path. A
successful synchronous commit remains durable and cross-shard atomic. A
Release benchmark on the target workstation must sustain at least 100,000
single-event durable transactions per second before CAC is accepted.

## Current State

Cedar currently writes one `PrepareRecord` to every participant shard's
`ShardPrepareLog`, fsyncs those logs, then appends and fsyncs a global
`CommitDecision` containing `PrepareReference` values. Recovery joins the
Decision Log to the shard Prepare Logs. `TransactionCoordinator::CommitInternal`
holds `commit_mutex_` across substantial validation, durability, sequencing,
and publication work. The implementation already has useful CAC foundations:

- temporal write-conflict validation and strict read identity checks;
- per-shard read/write reservations;
- `VisiblePrefix`, including out-of-order install completion;
- a monotonic `CommitTimeline` and durable transaction outcome index;
- Blob durable-before-reference rules and Manifest publication ordering;
- parallel commit-critical task execution and extensive fault-injection tests.

CAC changes the durable fact and commit pipeline while retaining those proven
building blocks where their invariants still apply.

## Alternatives Considered

### 1. Preserve PREPARE/Decision and tune group commit

This is the smallest change and preserves the current database format, but a
multi-shard transaction still requires multiple durable log facts and recovery
joins. It cannot remove the extra barrier or the protocol complexity, and it
makes the 100,000 tx/s target dependent on increasingly aggressive batching.

### 2. Add CAC beside the old protocol

A feature flag or format-dependent dual path reduces migration risk, but it
doubles recovery, checkpoint, fault-injection, statistics, and maintenance
logic. Because this repository is explicitly adopting a new incompatible
format, the compatibility cost has no product benefit.

### 3. Clean-break CAC

One complete transaction record becomes the only atomic commit fact. OCC and
reservations protect concurrency before logging; a single writer provides log
order and group fsync; installation happens after durability and may complete
out of order while visibility remains a continuous prefix.

**Selected:** clean-break CAC. It is the only approach that simultaneously
simplifies the failure proof, removes per-shard commit fsyncs, and exposes a
credible path to the required throughput without weakening synchronous
durability.

## Safety Model and Invariants

Cedar's failure domain is one database directory on one machine. CAC does not
claim atomicity across independent filesystems, hosts, or replicas.

The implementation must maintain these invariants:

1. A transaction is committed if and only if its complete
   `AtomicCommitRecord` is part of the durable log prefix.
2. Every durable record contains every participant shard event needed for
   replay; no other transaction log is required to prove or reconstruct it.
3. Durable records have contiguous `commit_seq` values and strictly monotonic
   `SystemHLC` values after the checkpoint prefix.
4. Blob payloads and their Manifest liveness are durable before a commit record
   references them.
5. No reader observes a commit above the continuous fully installed
   `VisiblePrefix`.
6. Validation reservations remain live from successful validation until the
   durable outcome is either known absent or installed/recovered.
7. Any append or fsync result that can no longer distinguish durable from
   non-durable stops the writer, marks the database `recovery_required`, and
   prevents a later `commit_seq` from crossing the uncertain position.
8. Returning `committed` implies the record is durable and the transaction's
   sequence has entered the visible prefix. A failure after possible durability
   returns `indeterminate` and is resolved by `txn_id` after reopen.

## Database Format

Increment `kCedarDatabaseFormatVersion`. Replace the format fields
`decision_log_location` and `shard_wal_locations` with an atomic commit-log
directory identity. Opening an old database returns `NotSupported`; no online
upgrade or legacy recovery is implemented. The v1 rejection path is read-only:
it must not open for append, truncate, rename, or delete any old PREPARE or
Decision file.

The log directory contains numbered segments:

```text
commit/COMMIT-00000001.log
commit/COMMIT-00000002.log
```

Each segment starts with a checksummed header containing at least:

- log magic and encoding version;
- Cedar database format version;
- database identity derived from the durable FORMAT identity;
- segment number;
- first `commit_seq` expected in the segment.

Creating the first record in a new segment requires creating the file,
persisting its header, and fsyncing the commit directory before the generation
can be acknowledged. Segment numbers and commit ranges must be contiguous.

## Atomic Commit Record

The logical record is:

```text
AtomicCommitRecord {
  record_format_version
  txn_id
  snapshot_seq
  commit_seq
  system_hlc
  transaction_mode          // snapshot | strict
  participant_count
  event_count
  shard_batches[] {
    shard_id
    events[] {
      logical_key
      valid_from
      schema_epoch
      operation
      inline_value | blob_ref
    }
  }
}
```

Participant batches are encoded in ascending `shard_id`. Events within a
batch use canonical `logical_key, valid_from` order. The encoder rejects zero
IDs, duplicate shard batches, empty batches, count mismatches, duplicate
transaction IDs in the retained/outcome history, invalid operations, invalid
Blob references, and records larger than the configured maximum.

The maximum transaction size is a new explicit CAC limit based on the complete
encoded record, not the old per-shard 4 MiB PREPARE limit or generic 64 MiB
frame assumption. Admission and encoding use the same overflow-checked sizing
function so a transaction cannot pass resource admission and then fail because
its canonical record is too large.

Each physical frame has a fixed magic/version, header length, payload length,
header checksum, payload checksum, and repeated frame length suffix. Recovery
uses the suffix to distinguish a torn final frame from a complete corrupt
frame:

- an incomplete final frame is truncated and the segment plus directory are
  synced before append resumes;
- a complete frame with a bad checksum is corruption;
- corruption before the physical tail is never truncated silently;
- a sequence gap, duplicate sequence, HLC regression, duplicate `txn_id`, bad
  segment header, or segment gap rejects open.

## Commit Queue and Adaptive Group Commit

Validated transactions enter one FIFO commit queue while holding their
reservations. One writer owns record ordering and the active segment.

For each generation the writer:

1. takes already-ready requests up to maximum record and byte limits;
2. allocates contiguous `commit_seq` values and `SystemHLC` values in FIFO
   order under the writer's ordering state;
3. encodes immutable records and appends them in the same order using a bounded
   `writev` or contiguous buffer;
4. performs one `fdatasync`/`fsync` covering the generation;
5. publishes durable success to every member only after that sync succeeds.

Transactions arriving after the generation closes join the next generation.
The writer does not impose a fixed microsecond sleep on a lone request. If
followers are already queued it merges them immediately; otherwise it may make
one scheduler yield before closing the generation. Batch count and bytes are
bounded to prevent large transactions or heavy concurrency from causing
unbounded latency or memory use.

`commit_seq` and `SystemHLC` allocation are not published to readers before the
generation is durable. The writer maintains private tentative ordering across
the generation and publishes an immutable outcome/timeline snapshot only after
durability. A definite pre-write failure may abort the
affected not-written requests and release their reservations. A partial write,
post-write failure, failed sync, or post-sync injected ambiguity makes every
request whose frame may be covered `indeterminate`, stops future generations,
and requires reopen.

The public API remains synchronous. Grouping changes physical sync sharing,
not the durable-return contract.

## OCC and Reservations

### Snapshot transactions

For each proposed temporal event, derive its affected valid-time interval from
the key's predecessor and successor boundaries. Abort if a committed event
with `commit_seq > snapshot_seq` overlaps that interval. Ordinary read sets are
not validated, preserving Cedar's existing snapshot-isolation semantics and
permitted write skew. Writes to the same logical key may proceed concurrently
when their valid-time intervals do not overlap.

### Strict transactions

Strict mode performs the snapshot write validation plus:

- verification that the observed event identity and predecessor/successor
  fences still match, including empty reads whose point value remains absent;
- temporary read reservations for every strict read point;
- write reservations for every derived write interval.

Reservation acquisition uses the canonical order
`shard_id, logical_key, valid_from, reservation_kind`. A transaction either
installs its complete reservation set or installs none. Reservation state
distinguishes validating, queued, durable-pending-install, and released so
failure handling cannot accidentally free a durable transaction's protection.
Strict read dependencies must be created by Cedar's capture APIs and carry a
validated identity token; callers cannot construct an identity-free strict
read that silently bypasses fence validation.

The global `commit_mutex_` is removed from the transaction body. Short locks
remain only for independent state: lifecycle/checkpoint exclusion, commit queue
ordering, per-shard validation/reservations, timeline publication, and test
hook replacement.

## Durable-After-Install Pipeline

Durable sync is the irreversible commit point. After a generation succeeds:

1. add each outcome to the in-memory durable outcome map and commit timeline;
2. schedule every transaction's shard batches independently;
3. install different shard batches in parallel;
4. mark the transaction installed only after all participant batches finish;
5. advance `VisiblePrefix` across consecutive installed sequences;
6. wake commit waiters whose sequence is now visible.

`StorageShard::InstallCommitted(txn_id, commit_seq, events)` must be idempotent.
It must accept exact replay, reject conflicting replay, update the memtable and
Blob reference catalog once, and preserve index-delta consistency. The install
coordinator uses explicit per-transaction completion state so one participant
failure cannot mark a partial transaction visible.

Installation may finish in sequence order 102, 100, 101; visibility remains
100, then 102 only after 101 is complete. Readers continue to choose snapshots
at or below `VisiblePrefix`.

An installation error after durability marks the database recovery-required
and returns `indeterminate`; reopen replays the complete record and finishes
idempotent installation.

The durable fact and the API result are deliberately distinct at this boundary:
after confirmed fsync the transaction can never become aborted, but an install
or visibility failure before the synchronous response remains indeterminate to
the caller. `Resolve(txn_id)` must then report committed after reopen. This
preserves Cedar's existing rule that `committed` is returned only after the
transaction joins the visible prefix.

## Recovery and Outcome Resolution

Open reads the Manifest checkpoint and outcome index first, restores the
checkpoint prefix, then scans commit segments strictly in segment and record
order. Each retained record above the checkpoint is decoded directly into a
replay transaction; there is no cross-log join.

Recovery:

1. validates segment identity and the continuous durable record prefix;
2. truncates and syncs only a torn physical tail;
3. restores `CommitTimeline` and the live `txn_id -> outcome` map;
4. replays every retained transaction idempotently into its shards;
5. marks each fully replayed sequence installed and advances visibility;
6. sets the next writer sequence and HLC after the recovered tail.

`Resolve(txn_id)` searches the retained live outcome map and then the immutable
checkpoint outcome index. A transaction whose final partial frame was
truncated resolves absent; a complete recovered record resolves committed.
The commit timeline and outcome map publish immutable snapshots, or provide an
equivalent internally synchronized lookup API; no caller may retain a raw
reference to a vector that the commit writer can mutate concurrently.

## Blob Handling

Blob materialization remains before commit-queue admission:

1. encode or deduplicate payloads in the Blob Store;
2. make the containing Blob segment durable;
3. ensure the segment is Manifest-live and the directory publication is
   durable;
4. only then enqueue an `AtomicCommitRecord` containing its `BlobRef`.

An abort may leave an unreachable Blob. Existing reconciliation and garbage
collection reclaim it. Inline values require no extra barrier. CAC resource
estimation accounts for Blob work separately from atomic-log bytes and rejects
oversized transactions before they hold queue capacity.

## Checkpoint and Segment Reclamation

Checkpoint targets
`C = min(captured continuous VisiblePrefix, continuous SST coverage frontier)`;
it never targets merely durable-but-uninstalled commits or relies only on the
maximum commit sequence observed in each shard. Coverage metadata must prove a
continuous prefix through `C` for every participant represented by the log.

1. freeze/flush every shard event with `commit_seq <= C` into checked SSTs;
2. fsync SST files and required directories;
3. create an immutable complete outcome index through `C` containing
   `txn_id`, `commit_seq`, and `SystemHLC`;
4. atomically publish the Manifest checkpoint and fsync the Manifest directory;
5. reclaim only whole commit segments whose `max_commit_seq <= C`;
6. fsync the commit directory after deletion.

After the new Manifest generation is durable, obsolete outcome-index files not
referenced by any retained Manifest generation are deleted and the checkpoint
directory is synced. The current all-history outcome index is retained for the
initial CAC implementation; incremental or layered indexes are deferred until
measurement proves full rewrite cost is material.

Any failure before Manifest publication leaves the old Manifest and log
segments authoritative. A failure after Manifest publication but before
deletion is safe and only retains extra log bytes. CAC never rewrites a live
log suffix merely to checkpoint it.

Checkpoint exclusion is narrow: it snapshots the visible target and prevents
reclamation races with segment rotation, but commits and installation may
continue into later segments while SST/outcome files are built.

## Lifecycle, Backpressure, and Shutdown

Commit admission estimates atomic-log bytes, Blob bytes, reservation memory,
and installation work before enqueueing. Queue count and bytes are bounded.
Pressure may reject a transaction before durability but never revoke a durable
generation.

Shutdown stops new admission, lets an active writer generation finish or
become recovery-required, drains durable installation work, and closes the log
only after waiters receive a terminal outcome. Checkpoint, close, and recovery
serialize segment mutation through the log lifecycle interface rather than a
transaction-wide mutex.

## Observability

Replace PREPARE/Decision-specific metrics with CAC metrics:

- atomic-log logical and physical bytes;
- queue depth and queued bytes;
- generation records, bytes, and leader/follower counts;
- physical sync count and sync latency;
- physical syncs per committed transaction;
- OCC validation and reservation latency;
- conflicts by snapshot-write, strict-read, and reservation class;
- durable-to-installed latency, participant install latency, and parallelism;
- visible-prefix lag and wait latency;
- aborted, committed, and indeterminate outcomes;
- recovery replayed records, truncated tail bytes, and reclaimed segments.

Benchmark artifacts and reports must use `AtomicCommitLog` terminology. Old
PREPARE and Decision counters, fault points, and storage estimates are deleted.

## Implementation Boundaries

Introduce focused units rather than growing `transaction_coordinator.cc`:

- `atomic_commit_record`: canonical validation, encoding, decoding, and byte
  estimates;
- `atomic_commit_log`: segment lifecycle, recovery scan, group append, outcome
  map, and reclamation;
- `commit_queue`: request/generation state and writer leadership;
- `transaction_validator`: temporal OCC and strict validation orchestration;
- `commit_installer`: parallel idempotent shard installation and visible-prefix
  completion.

Move `PendingEvent` to the record/intents layer so `StorageShard` does not
depend on the complete log implementation. The log append result carries the
calling transaction's exact immutable durable record and outcome; coordinator
code must never infer ownership from a shared `records().back()` accessor.

`TransactionCoordinator` owns these components and the cross-component state
machine, but delegates their internal mechanics. Existing `VisiblePrefix`,
`CommitTimeline`, Blob Store, VersionSet/Manifest, and outcome-index code are
retained or minimally adapted.

## Clean-Break Deletions

After CAC recovery and fault tests pass, delete:

- `PrepareRecord`, `PrepareReference`, `ShardPrepareLog`;
- per-shard PREPARE paths and format metadata;
- `CommitDecision.prepares` and Decision Log encoding;
- PREPARE/Decision append, recovery join, checkpoint truncation, metrics,
  resource estimates, and fault points;
- `RecoverCommittedTransactions`;
- synchronization and hooks that exist only for the old two-stage handoff.

No dead compatibility code remains in the final tree.

## Verification Strategy

Implementation is test-driven. Each delivery slice begins with failing focused
tests and ends with focused plus regression verification.

### Record and log tests

- canonical round-trip for every event/value/key form and Blob reference;
- invalid cardinality, ordering, duplicate ID, oversized record, and checksum;
- segment create/rotate/reopen and directory-sync failures;
- final-header, payload, checksum, and suffix partial writes;
- complete-tail corruption versus torn-tail truncation;
- segment gap, sequence gap/duplicate, HLC regression, and identity mismatch;
- group membership/order/bounds and leader/follower races;
- write, sync, post-sync, close, and recovery ambiguity propagation.

### OCC and installation tests

- same key overlapping interval conflict after snapshot;
- same key non-overlapping intervals commit concurrently;
- different keys/shards commit concurrently;
- strict event/fence changes and read/write reservation conflicts;
- deterministic acquisition under reversed input order;
- parallel participant install, out-of-order transaction completion, and
  continuous visibility;
- exact duplicate replay and conflicting replay;
- durable record followed by every participant-install fault boundary.

### Checkpoint, Blob, and lifecycle tests

- Blob sync/Manifest publication failure before commit append;
- abort leaves collectible Blob without a dangling durable reference;
- checkpoint target excludes durable-but-not-visible transactions;
- every SST/outcome/Manifest/reclamation fault boundary;
- retained suffix reopen, outcome resolution before/after reclamation, and
  directory-sync failures;
- concurrent commit, checkpoint, segment rotation, shutdown, and reopen.
- old FORMAT open rejection leaves every PREPARE/Decision file byte-for-byte
  unchanged;
- obsolete outcome indexes are reclaimed only after durable Manifest
  publication.

### Required verification gates

- focused CAC tests;
- complete CTest in clean Debug and Release builds;
- three clean Release A/B performance rounds;
- ASAN, UBSAN, and TSAN for changed code;
- deterministic crash campaign covering partial writes, fsync ambiguity,
  partial install, duplicate replay, checkpoint publication, and Blob faults;
- source inventory proving PREPARE/Decision implementation symbols and files
  are absent.

## Performance Contract

The acceptance workload is a reproducible Release build on local durable
storage with recorded CPU, memory, filesystem, mount, compiler, build flags,
instrumentation mode, shard count, value size, client count, queue bounds, and
dataset state. Loading and warm-up are outside the measurement interval.

Primary gate:

- at least **100,000 committed single-event durable transactions/second** over
  a sustained measurement window, with reopen verification of all acknowledged
  commits.

Secondary gates:

- target 100,000-300,000+ tx/s as concurrency scales;
- physical syncs per committed transaction <= 0.25, with < 0.1 preferred at
  high concurrency;
- one-shard and multi-shard transactions use one commit-log sync path and the
  multi-shard path adds no log fsync;
- every HTAP class makes progress; point-read and analytical throughput do not
  materially regress against the same Release baseline;
- p50/p95/p99 commit latency, generation size, queue delay, install lag, abort
  rate, and indeterminate rate are reported, not hidden behind throughput.

If the local storage cannot reach 100,000 synchronous durable tx/s even with
large safe generations, the implementation is not declared accepted. The
evidence must identify the physical sync ceiling and the remaining bottleneck;
the durability contract is not relaxed to manufacture a passing number.

## Delivery Sequence

1. Add the new format and atomic record codec with exhaustive tests.
2. Implement segmented recovery and single-request durable append.
3. Add commit-queue generations and adaptive group sync.
4. Move commit sequence/HLC allocation into the writer and connect outcome
   resolution.
5. Extract OCC/reservation orchestration and remove the transaction-wide lock.
6. Add parallel idempotent installation and continuous visibility waits.
7. Migrate Blob, checkpoint, outcome index, lifecycle, metrics, and benchmarks.
8. Switch coordinator/open paths exclusively to CAC and delete the old
   protocol.
9. Complete crash/sanitizer/full-suite gates and tune bounded batching until
   the 100,000+ tx/s Release gate passes.

## Non-Goals

- distributed commit, replication, quorum durability, or cross-host atomicity;
- backward-compatible database upgrade tooling;
- asynchronous acknowledgement or relaxed durability to meet throughput;
- changing snapshot mode into serializable isolation;
- unrelated query, optimizer, or storage-format refactoring.
