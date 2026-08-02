# Cedar Single-WAL Append Commit Design

**Date:** 2026-08-02
**Status:** Approved design, pending implementation plan
**Target:** durable append-heavy commits at 30,000+ transactions per second

## Purpose

Cedar currently implements durable asynchronous commit as two physical
RocksDB writes. The first synchronous write stores a full prepared batch. The
second write stores final facts, transaction metadata, and the visible
watermark, then deletes the prepare. The second write is not synchronized, but
it repeats the transaction payload in the WAL and MemTable path. The async
worker also creates a thread per transaction and sends each finalization back
through the group-commit queue.

This design replaces that path with one irrevocably decided group and one
synchronous WAL payload. RocksDB inserts that same payload into MemTables as
part of the same write. Cedar preserves two externally observable async stages:

1. `CommitAsync()` returns only after the group's unique WAL record is durable
   and the transaction's terminal decision cannot change.
2. `CommitHandle::Wait()` completes after the group is installed in MemTables
   and Cedar publishes the new visible prefix.

The design retains Cedar's explicit `commit_seq`, historical snapshots,
bitemporal version chains, strict reads, and ordered visible watermark. It does
not adopt RocksDB WritePrepared's commit map or hidden-version visibility
model.

## Goals

- Write each committed transaction payload to the WAL exactly once.
- Use one `fdatasync`-class durability barrier per physical group.
- Sustain at least 30,000 property-append transactions per second on the
  benchmark host with sufficient client concurrency.
- Preserve a strict, gap-free visible prefix of Cedar `commit_seq` values.
- Make every durable async acceptance recover to the same terminal decision.
- Keep ordinary synchronous `Commit()` fully durable.
- Preserve snapshot, temporal-neighborhood, delete, correction, edge identity,
  schema, vacuum, reopen, and historical-resolution semantics.
- Use bounded memory, explicit backpressure, and production-grade RocksDB
  flush, compaction, cache, WAL, and observability settings.
- Keep RocksDB-specific behavior behind a small Cedar storage seam.

## Non-Goals

- Distributed consensus, replication, or cross-process transactions.
- Relaxing snapshot immutability or Cedar's visible-prefix ordering.
- A second Cedar-owned WAL or a parallel recovery implementation.
- Multiple concurrent synchronous WAL epochs in the first implementation.
- Making every workload reach 30,000 transactions per second. The target is
  for append-heavy, predominantly independent transactions; hotspot and strict
  workloads retain correctness and may serialize.
- Enabling WAL compression in the latency-sensitive production profile.

## Selected Architecture

The selected architecture is a deep `AppendCommitPipeline` module. Its Cedar
interface accepts commit requests and returns durable acceptance and terminal
publication events. It hides queueing, adaptive epoch formation, validation,
sequence assignment, encoding, RocksDB callbacks, backpressure, and recovery
state.

Callers never become RocksDB group leaders. One dedicated durable-writer
thread owns engine submission; a bounded CPU pool performs preflight and
encoding. `Submit` enqueues and waits on the request's required stage, allowing
every async caller, including the request that caused an epoch to seal, to
return at the WAL-durable callback rather than at full MemTable completion.

The external storage seam is intentionally small:

```cpp
struct CommitTicket {
  TxnId txn_id;
  CommitMode mode;
};

StatusOr<CommitTicket> Submit(StoreCommitBatch batch, CommitMode mode);
StatusOr<StoreCommitResult> WaitPublished(const CommitTicket& ticket);
```

The concrete RocksDB adapter exposes one additional narrow interface:

```cpp
using WalDurableCallback = void (*)(void* context) noexcept;

Status WriteCommittedGroup(
    rocksdb::DB* db,
    const rocksdb::WriteOptions& options,
    rocksdb::WriteBatch* batch,
    WalDurableCallback on_wal_durable,
    void* callback_context);
```

`WriteCommittedGroup` uses RocksDB's existing `WriteBatch` encoding, WAL
fragmentation, checksums, write thread, synchronization, sequence assignment,
MemTable insertion, flush, MANIFEST, and recovery. It calls
`on_wal_durable` after WAL synchronization succeeds and before MemTable
insertion. It returns after MemTable insertion and RocksDB sequence publication
finish. The callback cannot fail, throw, re-enter RocksDB, or outlive the
synchronous `WriteCommittedGroup` call. It does not add a Cedar log file or a
new on-disk record type.

The adapter is implemented inside the maintained RocksDB build because
`DBImpl::WriteImpl` already has the required pre-release callback at exactly
this point. Cedar must not include RocksDB internal headers or downcast `DB` to
`DBImpl`.

## Why This Architecture

### Rejected: current durable prepare plus unsynchronized final write

This preserves a conventional prepare/final split, but writes the payload
twice, performs redundant point reads, and creates a second write-queue trip.
Removing `sync=true` from the final write does not remove its WAL append,
encoding, lock, or MemTable cost.

### Rejected: stock or adapted RocksDB WritePrepared

WritePrepared places data in WAL and MemTables during prepare, hides it with a
commit map, and later writes a small commit marker. It avoids a second large
payload but still requires a second WAL record. Its snapshot and commit-map
model also duplicates Cedar's explicit `commit_seq` and visible watermark.

### Deferred: WAL-only redo followed by WAL-disabled MemTable apply

This can use one payload, but it makes Cedar or a new RocksDB subsystem retain
the redo WAL until facts and metadata are atomically durable in SST and
MANIFEST. It requires custom WAL retention, cross-CF atomic flush, replay, and
garbage collection. A standard WAL-protected RocksDB write already provides
those guarantees with less state.

### Selected: decide first, then perform one standard durable write

The coordinator validates a whole epoch and fixes every terminal decision
before the write. The single final `WriteBatch` is therefore both the durable
decision and the redo data. A separate final marker is unnecessary.

## Commit Semantics

### Terminal decision before WAL

Before an epoch enters RocksDB, Cedar:

1. validates request shape and transaction identity;
2. evaluates snapshot, strict-read, temporal-neighborhood, and edge-identity
   dependencies against the stable visible prefix plus earlier decisions;
3. selects committed and aborted results in deterministic FIFO order;
4. assigns consecutive `commit_seq` values only to committed transactions;
5. encodes committed facts and metadata plus durable async-abort outcomes;
6. writes one final visible watermark equal to the last committed sequence.

If an epoch has no committed transaction, it omits the watermark update. An
async-abort-only epoch writes only terminal abort records. An epoch containing
only synchronous validation failures performs no RocksDB write.

After this point the result cannot be revalidated or changed. Recovery replays
the decision; it never decides the epoch again.

### Synchronous commit

`Commit()` enters the same coordinator as async requests. It returns only after
`WriteCommittedGroup` returns successfully and the new visible prefix has been
published in memory. A validation abort may return without a physical write
unless it shares an epoch that must durably record an async terminal abort.

### Asynchronous commit

`CommitAsync()` waits for the RocksDB WAL-durable callback. The callback runs
only after the WAL and required directory metadata have been synchronized.
At that moment every async request that will return accepted, and every
committed request, has a durable terminal decision:

- a winner has final fact, sequence, transaction-outcome, identity, and
  watermark data in the WAL;
- an accepted async loser has a durable async terminal-abort record in the
  same WAL batch.

The call then returns `CommitAcceptance::kAccepted` and a handle. The
`noexcept` callback does only bounded atomic state transitions and condition
notifications; it performs no user code, logging, allocation, lock acquisition
outside the epoch completion state, or blocking I/O.

If the engine returns an indeterminate WAL result before the callback fires,
`CommitAsync()` returns a handle whose acceptance is
`CommitAcceptance::kIndeterminate`. The handle preserves `txn_id` but does not
claim durable acceptance; `Wait()` returns an indeterminate/recovery-required
result and the caller resolves the ID after reopen. A definite pre-durability
failure still returns a non-OK `StatusOr` without a handle.

`CommitHandle::Wait()` completes after RocksDB has installed the batch in all
affected MemTables and Cedar has advanced its in-memory visible prefix. The
result is the terminal decision fixed before the WAL write.

The epoch retains encoded batch storage and minimum completion state until the
MemTable phase finishes. Transaction locks, validation overlays, request
payloads, and temporary version-chain nodes are released in bulk after that
second-stage confirmation. The WAL itself follows normal RocksDB retention;
it is not deleted by transaction completion.

### Visibility

The final visible watermark is stored in the same `WriteBatch` as facts and
metadata. Cedar does not expose RocksDB's internal sequence as its transaction
sequence. New Cedar snapshots use only the in-memory `visible_seq`, advanced
after the full RocksDB write returns.

`BeginSnapshot()` and publication synchronize through the visibility seam so
no snapshot can capture MemTable data from an epoch while also receiving an
older Cedar watermark. Existing snapshots remain bounded by their explicit
`commit_seq` and ignore newer versions even if RocksDB has installed them.
`ResolveTransaction()` also withholds a committed outcome whose `commit_seq`
exceeds the in-memory visible prefix; callers use the accepted handle to wait
for publication.

## Append Pipeline

### Stages

The pipeline contains the following internal stages:

1. **Admission:** enqueue into a byte-bounded MPSC queue and apply overload
   backpressure.
2. **Collection:** form an adaptive epoch using request count, encoded byte
   estimate, age, and RocksDB pressure.
3. **Preflight:** validate request shape, derive footprints, encode immutable
   portions, and calculate protection data in parallel.
4. **Sequencing:** evaluate dependencies in FIFO order, choose terminal
   outcomes, and assign consecutive Cedar sequences.
5. **Assembly:** build one final RocksDB `WriteBatch` and one watermark update.
6. **Durability:** the dedicated writer calls
   `WriteCommittedGroup(sync=true)`.
7. **Acceptance:** the WAL callback releases async callers with durable
   acceptance.
8. **Publication:** after MemTable insertion, advance `visible_seq`, complete
   handles, release epoch resources, and admit the next durable epoch.

### Bounded overlap

Only epoch N may be inside the RocksDB write path. While its WAL synchronization
or MemTable insertion is in progress, CPU workers may preflight, validate, and
assemble epoch N+1 against an immutable planning base containing:

- a RocksDB snapshot of the last published Cedar prefix;
- epoch N's irrevocable decisions represented by a pending-version overlay.

Epoch N+1 cannot enter RocksDB until N returns successfully. If N fails or is
indeterminate, N+1 is discarded without durable acceptance and the database
enters recovery-required state. This bound prevents pipelining from expanding
the set of transactions that might have reached durable storage.

The first implementation leaves RocksDB `enable_pipelined_write=false` because
only one epoch calls RocksDB at a time. A later measured optimization may allow
two ordered engine writes and enable it, but only with new crash-state tests.

### Pending-version overlay

The overlay contains only information required to validate the next epoch:

- fact identity and valid-time boundary;
- operation and selected value/schema identity where dependency checks need it;
- reserved Cedar `commit_seq`;
- edge-identity binding;
- transaction terminal decision.

It is immutable after sequencing and is deleted after the corresponding epoch
publishes. It is not a second canonical MemTable and is never used by public
reads. Deleting the module would force next-epoch validation to wait for the
previous epoch's MemTable publication, so the module provides real pipeline
depth rather than a pass-through seam.

### Append fast path

A request uses the append fast path when its footprint proves that it:

- creates a new valid-time boundary rather than correcting an existing one;
- does not overlap an earlier request in the epoch or pending overlay;
- has no strict read whose result can change inside the epoch;
- introduces no conflicting edge identity;
- does not require a historical delete/correction decision.

The fast path validates from the maintained version-chain/neighborhood state
and pending overlay without per-transaction RocksDB point reads. Blind appends
to distinct entities can be sequenced and encoded in parallel after their FIFO
sequence range is reserved.

Same-boundary corrections, deletes, strict reads, hotspot identities, duplicate
transaction IDs, and ambiguous footprints use the general version-chain path.
They retain identical semantics and may split the physical epoch. The scan
implementation remains a test-only temporal oracle.

## Adaptive Epoch Policy

The production starting policy is:

- minimum busy epoch target: 64 transactions;
- normal busy target: 128 transactions;
- maximum target: 256 transactions;
- hard encoded size limit: 2 MiB;
- low-load collection window: at most 200 microseconds;
- busy queue behavior: seal immediately at count or byte target;
- pressure behavior: reduce count and bytes before admitting more work.

The controller uses exponentially weighted observations of queue depth,
arrival rate, encoded bytes per transaction, WAL-sync latency, MemTable time,
p99 queue age, and RocksDB pressure. It changes targets gradually and never
exceeds configured hard bounds.

With a 3 ms durability barrier, ideal capacities are approximately:

| Transactions per sync | Ideal capacity |
| ---: | ---: |
| 64 | 21,333 txn/s |
| 128 | 42,667 txn/s |
| 256 | 85,333 txn/s |

The 30,000 txn/s production target assumes an average durable group of at least
128 and enough in-flight work. At 30,000 txn/s and 3 ms WAL latency, the bare
minimum is 90 in-flight requests; production testing uses 192 to 512 to absorb
scheduler and device variance.

## Backpressure and Fairness

Admission is bounded by both request count and estimated bytes. Byte accounting
includes staged values, encoded facts, metadata, overlay entries, and callback
state. A caller that reaches the soft limit waits within its deadline. At the
hard limit it receives `ResourceExhausted` before durable acceptance.

FIFO is preserved at epoch selection. A conflicting or slow-path request may
terminate the compatible prefix but cannot be bypassed indefinitely. Large
transactions are limited by a per-transaction byte ceiling and form their own
epoch when necessary.

RocksDB pressure feeds admission:

- soft pressure reduces epoch bytes, pauses speculative N+1 work, and delays
  new admission;
- hard pressure stops new admission until flush/compaction recovers;
- background error or write-stop transitions the store to an explicit error,
  never to unbounded queue growth.

## Failure and Recovery

### Before WAL submission

No transaction is durably accepted. Definite validation failures return abort
or invalid status. Planned sequence ranges are tentative and may be reused
because no WAL or external acceptance exists.

### Definite WAL failure before durability

No callback fires. The epoch and speculative successor fail without durable
acceptance. Cedar follows RocksDB's returned status and does not publish the
watermark.

### Indeterminate WAL result

If RocksDB cannot prove whether the synchronized record reached durable
storage, no later epoch enters RocksDB. Cedar marks the store
`RecoveryRequired`, returns indeterminate handles carrying their transaction
IDs, and requires a reopen. Recovery uses the final transaction outcome and
watermark in the standard WAL to resolve the epoch.

### Crash after WAL callback, before MemTable publication

Async callers may already hold accepted handles. The unique WAL record contains
the final terminal decisions and data. Standard RocksDB recovery replays it;
Cedar reloads and validates the visible watermark and committed sequence
records. The result cannot change.

### MemTable insertion failure after WAL durability

The epoch is durably decided but cannot be safely exposed by the running
process. Cedar stops publication, enters `RecoveryRequired`, and requires a
reopen. Accepted handles complete with recovery-required status. After reopen,
the caller uses `ResolveTransaction(txn_id)` to obtain the durable terminal
decision; an in-memory handle is not a cross-process object.

### Crash after publication

Recovery observes the same final batch in WAL or SST. Reapplying the batch is
idempotent under RocksDB recovery and Cedar's transaction/sequence keys.

### Corruption

Standard RocksDB record checksums, paranoid checks, WAL tracking in MANIFEST,
and Cedar codecs protect the path. A malformed final batch, missing sequence,
non-contiguous watermark, or mismatched transaction outcome fails open with
`Corruption`; Cedar never reconstructs winners by revalidation.

## Production RocksDB Profile

### Cedar-facing configuration

Cedar exposes a constrained `StorageProfile::kProductionAppend`, not arbitrary
RocksDB option pass-through. Its supported controls are:

```cpp
struct ProductionStorageOptions {
  uint64_t memory_budget_bytes;
  uint64_t block_cache_bytes;
  uint32_t max_background_jobs;
  uint32_t max_commit_batch_count = 256;
  uint64_t max_commit_batch_bytes = 2ULL * 1024ULL * 1024ULL;
  uint64_t compaction_rate_limit_bytes_per_sec;
};
```

For memory and worker counts, zero requests cgroup-limit derivation first and
host-limit derivation second. If neither limit is available, production mode
requires an explicit value instead of guessing. A zero compaction rate means
unlimited background throughput; deployment profiles set a limit when WAL and
compaction share an I/O service class. Cedar logs and exposes the fully
resolved profile at open. Production mode rejects a memory budget below 1 GiB,
insufficient file-descriptor limits, unsupported codec builds, or option
combinations that weaken WAL and snapshot guarantees. Tests and developer mode
retain explicit small profiles.

### Memory budget

The derived production budget is capped at 40% of the process/container memory
limit unless the operator supplies a lower explicit value. The starting split
within Cedar's RocksDB budget is:

- 55% shared block cache;
- 25% facts MemTables and immutable MemTables;
- 5% meta/default MemTables;
- 10% compaction, table-reader, and blob working headroom;
- 5% commit queues, encoded epochs, and pending overlays.

The shared `WriteBufferManager` enforces the aggregate MemTable limit. The
block cache uses strict capacity and a high-priority pool for index and filter
blocks. Commit admission uses the same budget rather than maintaining an
unaccounted independent limit.

### DB-wide baseline

The production baseline sets:

- `paranoid_checks=true`;
- `track_and_verify_wals_in_manifest=true`;
- `atomic_flush=false` because every cross-CF final batch is WAL protected;
- `allow_concurrent_memtable_write=true`;
- `enable_pipelined_write=false` for the single-engine-epoch design;
- `manual_wal_flush=false`;
- WAL compression disabled;
- `avoid_unnecessary_blocking_io=true`;
- `max_background_jobs=clamp(cpu_count / 2, 4, 16)` unless explicitly set;
- `max_subcompactions=min(2, max_background_jobs)`;
- `bytes_per_sync=1 MiB` for SST/compaction writeback smoothing;
- `wal_bytes_per_sync=1 MiB` without replacing explicit commit sync;
- `max_total_wal_size=max(1 GiB, 2 * aggregate MemTable capacity)`;
- RocksDB statistics, thread tracking, periodic stats, bounded log rotation,
  and background-error reporting enabled;
- `max_open_files=4096`, with production startup requiring an `RLIMIT_NOFILE`
  soft limit of at least 8192.

Direct I/O is not enabled by default. It becomes a host profile only after
device-specific sustained tests show improved tail latency without starving
the OS page cache or changing recovery behavior.

### Facts Column Family

The initial 4 GiB RocksDB budget resolves to:

- 128 MiB write buffer;
- four write buffers, with two eligible for merged flush;
- level compaction with dynamic level bytes;
- 128 MiB target L1 files;
- L0 compaction/slowdown/stop triggers of 8/24/36;
- soft pending-compaction limit of 8 GiB and hard limit of 32 GiB, scaled
  linearly for larger memory budgets;
- the fixed 12-byte fact-identity prefix extractor;
- a prefix-aware Bloom filter with 10 bits per key;
- cached index and filter blocks at high priority;
- VCSL MemTable representation when the maintained VCSL profile is selected;
- blob separation for values at or above 4 KiB, with garbage collection
  enabled, age cutoff `0.25`, force threshold `0.75`, and backlog metrics.

### Meta Column Family

Meta uses smaller, point-read-oriented settings:

- 32 MiB write buffers;
- three write buffers, with one sufficient for flush;
- whole-key filters;
- no blob files;
- high-priority cached index/filter blocks;
- leveled compaction and L0 compaction/slowdown/stop thresholds of 4/12/20.

The default Column Family remains small and unused for Cedar data. It does not
inherit the facts memory or blob policy.

### Compression and build reproducibility

The maintained production RocksDB build enables the pinned archived LZ4 and
Zstd sources as active build dependencies. Flush output and non-bottommost
levels use LZ4; the bottommost level uses Zstd level 3. WAL compression remains
disabled. Existing databases remain readable because every production binary
includes all codecs it may have used previously.

The reusable static-library fingerprint includes:

- RocksDB source revision and Cedar patch digest;
- LZ4 and Zstd source revisions;
- compiler, target, sanitizer, and build type;
- enabled codecs and relevant RocksDB feature flags.

A cache entry is complete only when its library, installed headers, manifest,
and codec linkage all match. Partial or mismatched entries are rebuilt under
the existing process lock.

### Compaction pressure feedback

The pipeline samples or listens for:

- active and immutable MemTable bytes;
- L0 file count;
- estimated pending compaction bytes;
- current delayed write rate and write-stopped state;
- running flushes and compactions;
- WAL sync, MemTable insertion, flush, and compaction latency;
- block-cache usage and hit rate;
- blob live/garbage bytes and GC backlog;
- background errors and available disk headroom.

The pressure controller uses hysteresis. It does not repeatedly oscillate
between full rate and stop. Safety signals such as background errors, hard disk
headroom, and RocksDB write-stop override throughput targets.

The initial facts-CF admission thresholds are:

- enter soft pressure at L0 >= 16 files, pending compaction >= 8 GiB, or
  immutable MemTable usage >= 75% of its budget;
- enter hard pressure at L0 >= 24 files, pending compaction >= 16 GiB,
  immutable MemTable usage >= 90%, or RocksDB write-stop;
- leave hard pressure only after L0 <= 16, pending compaction <= 8 GiB, and
  immutable usage <= 70%;
- leave soft pressure only after L0 <= 12, pending compaction <= 4 GiB, and
  immutable usage <= 60%.

For larger memory budgets, pending-compaction thresholds scale with the facts
soft limit; file-count and percentage thresholds remain fixed. Any background
error stops admission. Free filesystem space below the greater of 10 GiB or
10% enters soft pressure; below the greater of 4 GiB or 5% stops admission.
Meta enters soft pressure at L0 >= 8 and hard pressure at L0 >= 12; it leaves
those states at L0 <= 6 and L0 <= 4 respectively. The aggregate controller
uses the most severe state from any Column Family or DB-wide signal.

## Observability

Every benchmark and production metrics snapshot records:

- submitted, durably accepted, published, aborted, indeterminate, and rejected
  transactions;
- transactions and encoded bytes per epoch;
- collection, queue, validation, assembly, WAL-sync, MemTable, publication,
  and end-to-end latency;
- append-fast-path and general-path counts;
- physical WAL writes and syncs;
- transactions and bytes per sync;
- pending overlay size and N+1 discard count;
- admission wait/rejection and pressure state time;
- RocksDB stall, L0, pending compaction, cache, flush, compression, blob, and
  background-error metrics.

The metrics distinguish WAL callback latency from full `DB::Write` latency so
the async acceptance benefit and MemTable tail are visible independently.

## Testing

### Functional and model tests

- empty, single, and maximum-size epochs;
- FIFO ordering and incompatible-prefix splitting;
- append fast path differential tests against the general version-chain path;
- same-boundary correction, delete, out-of-order valid time, historical
  snapshot, strict read, edge identity, and schema conflicts;
- duplicate transaction IDs before acceptance, after acceptance, after
  publication, and after reopen;
- indeterminate async acceptance preserves `txn_id` and resolves after reopen;
- mixed sync and async requests sharing one decided epoch;
- durable async abort outcomes;
- temporal neighborhood differential tests against the scan oracle before and
  after reopen, flush, and compaction;
- VCSL and upstream MemTable profile equivalence.

### Crash matrix

Inject process death or I/O status at:

- before sequencing;
- after sequencing but before RocksDB submission;
- during WAL append;
- before and after WAL sync;
- inside the WAL-durable callback;
- before, during, and after MemTable insertion;
- before and after Cedar visible-prefix publication;
- while N+1 is planned but N is unresolved;
- after flush and during compaction/reopen.

For every durable acceptance, reopen must produce exactly the pre-decided
terminal result. No unaccepted N+1 request may appear after recovery. Sequence
and watermark validation must remain gap-free.

### RocksDB adapter tests

- callback fires once and only after successful WAL synchronization;
- callback never fires on definite pre-durability failure;
- callback precedes MemTable completion under deterministic sync points;
- the no-fail callback cannot suppress MemTable insertion or publish a partial
  batch;
- ordinary `DB::Write` behavior remains unchanged;
- WAL recovery, log rotation, flush, checkpoint, backup, and compaction retain
  the committed group;
- sanitizer and corruption tests cover the maintained RocksDB patch.

### Production profile tests

- resolved-option snapshot tests for developer and production budgets;
- facts/meta/default CF options differ as designed;
- invalid budgets, file limits, codecs, and durability-weakening combinations
  fail at open;
- aggregate MemTable and queue memory stay within budget;
- pressure hysteresis and admission recovery are deterministic;
- old uncompressed databases reopen under the codec-enabled binary;
- codec and patch changes produce different static-cache fingerprints.

## Performance Qualification

The primary append workload uses property puts to independent entities with
small values. It runs with 192, 256, and 512 in-flight requests and records a
matrix of epoch count, epoch bytes, and low-load collection window.

Qualification proceeds from focused to sustained:

1. 2,048-transaction smoke comparison against the current implementation;
2. 30-second warm benchmark with fixed database size;
3. 30- to 60-minute continuous append with flush, compaction, and database
   growth active;
4. reopen and correctness verification of the resulting database;
5. mixed append/correction/delete and hotspot degradation workloads.

The production acceptance gates are:

- sustained append throughput at least 30,000 committed transactions per
  second on the recorded reference host;
- one transaction payload in WAL and no prepare/final duplicate;
- average physical group at least 128 under saturated load;
- no persistent RocksDB write-stop or unbounded L0/pending-compaction growth;
- bounded memory and queue usage;
- no correctness, recovery, sanitizer, or temporal-oracle regression;
- p99 latency reported and stable over the sustained run rather than hidden by
  an open-loop throughput result.

The evidence records hardware, filesystem, storage device, OS, compiler,
RocksDB and patch fingerprint, codec versions, durability settings, database
size, in-flight count, and resolved production profile. A short run is never
used to claim production throughput.

## Rollout

1. Add the RocksDB durable-callback adapter and deterministic engine tests
   without changing Cedar's production path.
2. Add the decided-epoch model, final batch codec, and recovery tests behind an
   explicit experimental option.
3. Replace per-transaction finalizer threads with the bounded pipeline and run
   differential tests against the current durable async implementation.
4. Add adaptive epochs, append fast path, pressure feedback, and production
   RocksDB profile.
5. Run full crash, sanitizer, VCSL/upstream, and sustained-performance gates.
6. Make single-WAL append commit the default only after all gates pass.
7. Remove prepared-commit keys, codecs, scans, and old finalization code after
   cross-version migration/reopen coverage proves they are no longer needed.

During migration, open recognizes old durable prepare records and resolves them
through a compatibility recovery path before enabling the new coordinator. New
commits never create old prepare records. The durable database format records
the protocol capability so an older Cedar binary cannot silently open and
misinterpret a database written by the new path.
