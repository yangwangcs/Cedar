# Cedar Single-Node HTAP Resource and Maintenance Scheduling Design

Date: 2026-07-17

Status: Approved authoritative design; functional implementation substantially complete; release/paper closure remains incomplete and is tracked in `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

Depends on:

- `2026-07-17-cedar-htap-design.md`
- `2026-07-17-cedar-columnar-design.md`
- `2026-07-17-cedar-tcypher-vectorized-execution-design.md`
- `2026-07-17-cedar-temporal-index-cbo-design.md`

## 1. Purpose

This document defines Cedar's fifth design stage: a database-wide resource governor and maintenance scheduler for the single-node HTAP engine.

Cedar has two simultaneous obligations:

- commit writes durably and with predictable tail latency;
- run snapshot-stable vectorized graph analytics over immutable columnar files.

Those obligations compete for CPU, memory, page cache, disk bandwidth, I/O operations, file descriptors, and background worker slots. The current repository creates independent thread pools for query, compaction, batching, WAL flushing, and asynchronous indexes. Independent pools make local throughput look good while allowing global oversubscription, memory contention, page-cache pollution, and write stalls.

The new design supplies one control plane for these resources while preserving isolated lanes for correctness-critical commit work. It covers:

- workload classes and admission;
- hierarchical CPU, memory, and I/O budgets;
- fair morsel and maintenance scheduling;
- cache admission and scan resistance;
- MemTable, WAL, flush, compaction, index, and Blob GC pressure feedback;
- write backpressure and disk safety reserves;
- cancellation, shutdown, recovery, and observability;
- replacement of scattered legacy thread-pool and cache toggles.

## 2. Relationship to Earlier Designs

### 2.1 Authoritative Contracts

The correctness kernel remains authoritative for:

- transaction state transitions, prepare durability, DecisionLog order, HLC, and `visible_seq`;
- Manifest and VersionSet publication;
- snapshot and file lifetime;
- atomic flush and compaction edits;
- the rule that a successful commit cannot be made invisible by scheduler delay.

Columnar and T-Cypher V1 remain authoritative for:

- SST/Page and Blob I/O contracts;
- PageCache, BlobLocationCache, and BlobValueCache ownership;
- `QuerySnapshot`, `ColumnBatch`, morsel execution, query memory, spill, cancellation, and result streaming;
- index sidecar lifecycle, statistics snapshots, and CBO access paths.

This document decides who receives resources and when. It does not change which events are visible or which files are correct.

### 2.2 Scheduling Refinements

1. The database owns one `ResourceGovernor`, one `WorkScheduler`, one `MemoryGovernor`, and one `IoGovernor`. Components submit typed tasks instead of creating independent unbounded pools.
2. WAL/DecisionLog/Manifest commit work has a reserved critical lane. It is never queued behind analytical morsels, index builds, or Blob GC.
3. A transaction reserves the resources required to finish a durable prepare/decision path before entering a non-abortable commit phase.
4. Maintenance work is derived from Manifest, WAL, MemTable, and Blob state. Scheduler queues are not a second durability log.
5. Backpressure is explicit and stateful. It does not rely on arbitrary queue lengths or sleeping worker threads.

## 3. Current Resource Problems

The current codebase exhibits several independent resource owners:

1. `AdaptiveThreadPool` can create workers based on local pending tasks and does not reserve CPU or memory for other components.
2. `ThreadPoolQueryExecutor` uses hardware concurrency by default and owns a separate queue from compaction and transaction code.
3. Compaction and batch-processing paths create their own worker vectors and futures.
4. WAL group commit and `WalBatchWriter` create background threads without a database-wide I/O budget.
5. Async index construction can run against raw MemTable version nodes without a global maintenance grant.
6. Block, query, reader, version-chain, and Blob caches have overlapping memory budgets and no common eviction policy.
7. Configuration exposes many component-local thread and enable flags rather than one resource profile.
8. Query range and scan tasks can issue work without accounting for pinned PageCache bytes or Blob buffers.
9. Frozen MemTables and compaction debt do not provide a unified write-stall signal.
10. Disk free space, WAL queue latency, and pending Manifest work do not feed admission decisions.
11. There is no starvation guarantee: a continuous analytical workload can consume all workers, while maintenance can also flood workers and harm point-read latency.
12. Shutdown and cancellation semantics differ by component, so a background task can outlive its source snapshot or temporary output.

The new architecture removes these independent control loops.

## 4. Goals and Non-Goals

### 4.1 Goals

1. Keep durable commit tail latency isolated from analytical and optional maintenance work.
2. Prevent CPU, memory, I/O, and file-descriptor oversubscription.
3. Provide fair progress for foreground writes, point reads, analytical scans, and essential maintenance.
4. Turn MemTable, WAL, compaction, disk, and cache pressure into explicit admission and scheduling signals.
5. Keep already-prepared transactions from deadlocking because a scheduler later revokes their resources.
6. Bound query, maintenance, cache, and transaction memory while permitting controlled borrowing.
7. Protect PageCache and BlobValueCache from sequential scan pollution.
8. Schedule flush, compaction, index builds, statistics merges, and Blob GC according to correctness and debt.
9. Allow cancellation and shutdown to drain every task class deterministically.
10. Provide metrics sufficient to explain queue delay, pressure transitions, stalls, cache behavior, and write amplification.
11. Preserve stable snapshots and Manifest correctness under all scheduling decisions.

### 4.2 Non-Goals

This stage does not add:

- a distributed scheduler or cross-node quota protocol;
- Linux cgroup configuration as a required runtime dependency;
- a hard real-time guarantee;
- a new transaction isolation level;
- automatic history retention or valid-time garbage collection;
- a new query language workload hint beyond existing session APIs;
- dynamic shard resharding;
- changing size-tiered compaction into leveled compaction;
- making optional index sidecars a correctness requirement;
- using scheduler state as a recoverable database log.

## 5. Scheduling Alternatives and Selection

### 5.1 Separate Fixed Pools with Static Quotas

Each subsystem receives a fixed worker count and memory/I/O quota. This is simple, but unused query capacity cannot help a compaction burst, and fixed quotas do not react to FrozenMemTable debt, disk pressure, or analytical scans.

### 5.2 Unified Hierarchical Scheduler with Reserved Critical Lane

All work is represented as typed tasks and scheduled through one resource governor. Commit-critical operations have reserved CPU and I/O capacity. Other tasks share weighted fair queues with per-query and per-maintenance grants. Memory and I/O token buckets feed admission and backpressure.

This permits utilization without allowing optional work to consume the capacity needed for durability. It is the selected design.

### 5.3 External OS-Only Control

The engine could rely on cgroups, process priority, and the operating system scheduler. This is useful as a deployment boundary, but it cannot see VersionSet pins, MemTable debt, Blob GC liveness, query memory, or transaction phases. It remains an optional outer limit, not the Cedar control plane.

## 6. Workload Classes

Every task declares one class and a resource request:

```text
WorkClass {
  COMMIT_CRITICAL
  FOREGROUND_WRITE
  POINT_READ
  INTERACTIVE_QUERY
  ANALYTICAL_QUERY
  FLUSH
  COMPACTION_URGENT
  COMPACTION_NORMAL
  INDEX_BUILD
  STATS_MERGE
  BLOB_GC
  RECOVERY
  SHUTDOWN
}
```

### 6.1 Commit-Critical

`COMMIT_CRITICAL` includes:

- shard WAL prepare append and fsync;
- DecisionLog append and fsync;
- visible-prefix publication;
- Manifest edits required by an acknowledged commit;
- recovery of committed transactions;
- reservation release and commit outcome publication.

This lane has reserved resources and is not subject to ordinary query queueing. It may still fail on I/O errors, disk reserve exhaustion, or shutdown.

### 6.2 Foreground Work

`FOREGROUND_WRITE` covers pre-prepare mutation work, Blob durability preparation, MemTable insertion, and non-critical write batching. `POINT_READ` covers exact-key and small entity reads. `INTERACTIVE_QUERY` covers bounded T-Cypher plans with a user deadline. `ANALYTICAL_QUERY` covers scans, traversals, aggregates, and long result streams.

The scheduler uses query admission grants, not a promise that every foreground task receives a dedicated OS thread.

### 6.3 Maintenance

`FLUSH` is essential when FrozenMemTable debt approaches a limit. `COMPACTION_URGENT` protects read amplification, disk space, or write admission. `COMPACTION_NORMAL` improves layout when there is spare capacity. Index builds, statistics merges, and Blob GC are optional until their own safety thresholds are reached.

`RECOVERY` and `SHUTDOWN` temporarily supersede normal work according to the state machines below.

## 7. Resource Model

### 7.1 Resource Request

Every scheduled task carries:

```text
ResourceRequest {
  work_class,
  cpu_slots,
  memory_reservation,
  memory_peak_estimate,
  sequential_read_bytes,
  random_read_ops,
  write_bytes,
  file_descriptors,
  deadline,
  cancellation_token,
  snapshot_id,
  preemptible,
  non_revocable_after
}
```

The request is an estimate for admission and accounting. Actual consumption is metered at task and batch boundaries. A task may not allocate outside its grant without asking the governor for a bounded extension.

### 7.2 CPU Slots

At database open, Cedar discovers usable CPU capacity, process limits, and an optional deployment cap. It creates a fixed number of logical CPU slots rather than allowing every component to use `hardware_concurrency()` independently.

The default shared worker capacity is:

```text
shared_slots = max(1, usable_cpu_slots - critical_reserved_slots)
```

`critical_reserved_slots` is one slot when usable capacity is four or fewer and two slots otherwise. On a two-core system the critical lane shares one physical core through short bounded tasks, but still has scheduling priority and I/O reservation.

CPU slots are scheduling units, not a claim that the operating system will provide hard core affinity. Heavy tasks report their estimated weight so the governor can reduce concurrent decode, compaction, and hash work.

### 7.3 Memory Pools

The global database memory budget is the minimum of the explicit Cedar limit, an optional process/cgroup limit, and detected physical memory after an OS reserve of `max(1 GiB, 10% of physical memory)`. The default soft allocation is:

```text
MemTable and ValueArena       20%
Page/metadata/Blob caches     30%
Foreground query grants       25%
Maintenance and sidecars      10%
Transaction/WAL buffers        5%
Emergency and control         10%
```

These are soft pools. Unused capacity can be borrowed, but the emergency and commit-critical reserves cannot be borrowed by analytical work. Every allocation is charged once to the physical owner and, when pinned by a query, also counted against the query's pin allowance.

Memory revocation order is:

1. optional index, statistics, and Blob GC buffers;
2. revocable query intermediates and cache admissions;
3. analytical query concurrency and new analytical admission;
4. PageCache and BlobValueCache eviction;
5. normal compaction concurrency;
6. foreground write admission before prepare.

MemTables, prepared transaction state, and commit-critical buffers are non-revocable after their reservation point. The governor must stall or reject new work before violating their guarantees.

### 7.4 I/O Tokens

Each physical storage device has separate token buckets for:

```text
sequential read bytes
random read operations
sequential write bytes
metadata/fsync operations
```

WAL and DecisionLog fsyncs have a reserved metadata/commit lane. Foreground point reads have a bounded random-read share. Sequential analytical scans, flush, compaction, index builds, and Blob GC consume the remaining buckets according to work-class weights.

Tokens are replenished by a calibrated device profile. A task that exhausts tokens yields at a Page, Block, or Blob-block boundary; it does not sleep while holding a global mutex.

### 7.5 File Descriptors and Temporary Space

Open SST readers, sidecars, Blob segments, WAL files, and spill files consume a common descriptor budget. The governor reserves descriptors before opening a batch of files and uses bounded LRU reader handles.

Temporary-space budget covers query spill, index build outputs, compaction outputs, and Blob relocation staging. A task cannot begin a large output if the required reserve would cross the disk safety margin.

## 8. WorkScheduler

### 8.1 Task Queues

The scheduler has:

- a reserved critical queue;
- a foreground queue with subqueues for writes, point reads, and interactive queries;
- an analytical queue;
- an essential-maintenance queue;
- an optional-maintenance queue.

Tasks are small enough to yield at a vector batch, SST Block, posting Block, Blob Block, or compaction merge step. A long task is decomposed instead of relying on preempting arbitrary C++ code.

### 8.2 Fairness

Within a queue, Cedar uses weighted deficit round robin with deadline ordering for interactive work and aging for waiting tasks. Each admitted query and maintenance job has a concurrency cap. Work stealing is allowed only among tasks with compatible grants.

Fairness rules are:

- commit-critical tasks always have a reserved lane;
- essential flush and urgent compaction receive a minimum service share when debt exists;
- interactive queries receive a bounded latency share while admitted;
- analytical queries receive a minimum throughput share and cannot monopolize all shared slots;
- optional maintenance is the first class to yield under pressure;
- aging prevents an eligible task from waiting forever when its resource request is feasible.

### 8.3 Admission

Admission evaluates:

```text
available memory grant
available CPU slots
available I/O tokens
current pressure state
snapshot/file descriptor cost
deadline and queue age
```

An analytical query can be queued with a deadline, rejected with `AdmissionTimeout`, or admitted with a smaller worker grant. A query that has started does not lose its pinned snapshot; it may spill, slow down, or be cancelled by the client.

Foreground writes reserve their pre-prepare resources before entering validation. A transaction that reaches the non-revocable prepare phase receives a completion grant for required WAL/DecisionLog/Manifest work.

## 9. Pressure Controller

### 9.1 Pressure Signals

The controller samples:

- active and FrozenMemTable bytes/counts per shard;
- WAL queue depth, oldest request age, and fsync latency;
- DecisionLog and Manifest queue age;
- compaction debt, overlapping SST count, and read amplification;
- index sidecar coverage debt and build queue age as optional-maintenance signals;
- Blob orphan bytes, live-segment ratio, and GC queue age;
- free disk bytes and temporary-space reserve;
- global and pool memory utilization;
- PageCache hit rate, admission misses, and pinned bytes;
- query queue delay and tail latency;
- I/O token utilization and worker skew.

### 9.2 Pressure States

```text
NORMAL
SOFT_PRESSURE
HARD_PRESSURE
WRITE_STALL
DISK_EMERGENCY
RECOVERY
SHUTDOWN
```

Transitions use hysteresis and minimum dwell times so the system does not oscillate at a threshold.

### 9.3 Normal

All classes run under configured weights. Optional maintenance receives spare capacity. Caches admit according to their policies, and query admission uses ordinary grants.

### 9.4 Soft Pressure

Soft pressure is entered when a pool exceeds its soft target, a queue age crosses its warning threshold, or safety-relevant flush, compaction, or Blob debt rises above target. Index coverage and statistics debt influence spare-capacity scheduling but cannot trigger hard pressure or write stalls. The controller:

- reduces optional maintenance concurrency;
- tightens analytical query admission and PageCache admission;
- promotes and increases the specific flush, urgent-compaction, or Blob-GC work whose safety debt caused pressure;
- requests cache eviction and spill;
- preserves critical reserves and already admitted transaction grants.

### 9.5 Hard Pressure and Write Stall

Hard pressure is entered when FrozenMemTables, temporary files, compaction debt, or memory exceed a hard threshold. The controller stops admitting optional maintenance, may cancel queued analytical work, and gives essential flush/compaction a larger share.

`WRITE_STALL` is entered only when accepting another write could violate WAL, MemTable, prepared-transaction, or disk safety guarantees. New transactions fail or wait before prepare with retryable `WriteStalled`. Prepared transactions continue to completion or abort; they are never stranded behind a new stall.

### 9.6 Disk Emergency

When free disk or temporary space reaches the safety reserve:

- new writes and sidecar builds fail with `DiskReserveExhausted` unless they are required to complete an already prepared transaction;
- Blob orphan cleanup and safe file deletion receive priority;
- compaction may run only when its output reserve is available;
- reads remain available while files and snapshots can be safely pinned;
- the database refuses open or transitions to read-only if recovery cannot establish the reserve.

The controller never deletes a Manifest-live or snapshot-pinned file to recover space.

## 10. Write Path and Backpressure

### 10.1 Pre-Prepare Admission

Before a transaction enters OCC validation or writes a prepare record, it reserves:

- pending-event memory or query spill space;
- WAL append and fsync budget;
- durable Blob capacity for large values;
- expected MemTable installation bytes, including a safe FrozenMemTable handoff;
- required DecisionLog, visible-prefix, and outcome publication resources.

If a reservation cannot be obtained, the transaction returns `AdmissionTimeout`, `QueryMemoryLimit`, `WriteStalled`, or `DiskReserveExhausted` without installing a partial reservation.

### 10.2 Commit-Critical Completion

After all shard prepares are durable, the transaction owns a completion grant for:

```text
DecisionLog append and fsync
visible-prefix publication
outcome/index updates
reservation release
```

This grant cannot be revoked by analytical pressure. If an I/O device fails, the transaction reports the durable error and recovery resolves the state; it does not wait indefinitely for optional worker capacity.

### 10.3 MemTable Pressure

Each shard exposes:

```text
active_bytes
frozen_bytes
frozen_count
oldest_frozen_age
pending_prepare_bytes
```

Soft thresholds schedule flush. Hard thresholds reduce write admission. A flush failure raises debt and preserves the FrozenMemTable; it does not drop events or release the memory charge prematurely.

### 10.4 Read-Your-Writes and Maintenance

A transaction's pending events are accounted to the transaction until commit. They cannot be flushed or compacted before publication. After commit and MemTable installation, flush and compaction tasks may consume the data under normal maintenance grants.

## 11. Maintenance Scheduler

### 11.1 Maintenance Priority

The scheduler ranks eligible maintenance tasks:

1. recovery and shutdown safety;
2. flush required to release FrozenMemTable pressure;
3. urgent compaction required for write admission, disk safety, or severe read amplification;
4. Manifest/WAL/CommitTimeline/Stats checkpoints needed to advance safe truncation;
5. Blob GC required by orphan or disk thresholds;
6. normal compaction;
7. index sidecar repair/build;
8. statistics merge and optional prefetch warming.

Priority does not change compaction correctness. The size-tiered picker still selects complete partition/overlap closures from the pinned VersionSet.

### 11.2 Flush Tasks

A flush task includes:

- one FrozenMemTable ownership token;
- output SST and optional index sidecar requests;
- expected bytes and descriptor count;
- a cancellation token that is ignored after output publication begins unless the output can be safely abandoned;
- a Manifest publication continuation.

Flush output is temporary until fsync, rename, directory fsync, and Manifest edit complete. A failed flush retains the FrozenMemTable and retries with backoff.

### 11.3 Compaction Tasks

A compaction task includes:

- the complete input closure selected by the correctness-preserving compaction picker;
- pinned input VersionSet and file handles;
- output byte and temporary-space reservation;
- optional index sidecars and statistics fragments;
- a write-amplification estimate;
- a preemptible merge cursor.

Compaction yields between input Blocks and output Blocks. It cannot be cancelled after publishing only part of an output edit; cancellation before publication deletes complete temporary outputs and leaves inputs live.

### 11.4 Index and Statistics Tasks

Index sidecar builds and statistics merges are optional work with low priority by default. They may be paused at SST or posting-Block boundaries. A build does not block correct SST publication, and a missing sidecar creates a coverage gap handled by the CBO.

### 11.5 Blob GC Tasks

Blob GC is split into:

- catalog/reference analysis;
- candidate segment selection;
- copying live records;
- index mapping CAS and delta fsync;
- Manifest retirement and reader-epoch wait;
- physical deletion.

Only the analysis and copy phases are preemptible. After a new segment mapping is Manifest-live, the retirement protocol remains commit-critical for Blob correctness. GC never consumes the emergency disk reserve to create an unbounded relocation output.

## 12. Cache and Buffer Governance

### 12.1 One Cache Ownership Model

The database owns:

```text
MetadataCache
PageCache
BlobLocationCache
BlobValueCache
```

The old query-result, reader, version-chain, and block-cache implementations are not independently budgeted. A cache entry has an owner, byte charge, admission class, and eviction state.

### 12.2 PageCache Admission

PageCache uses segmented recency with scan-resistant admission:

- metadata and BlockIndex pages are high-priority and long-lived;
- sequential analytical pages enter a probation segment and are evicted after one pass unless reused;
- point-read pages may enter a protected segment after repeated hits;
- query hints can mark a read as streaming and bypass value-page admission;
- pinned pages cannot be evicted and count against the query's pin allowance.

The policy is observable through hit, bypass, promotion, eviction, and pinned-byte counters. A query cannot force the cache to retain its entire scan.

### 12.3 Blob Caches

`BlobLocationCache` stores hash-to-location hints and is cheap to rebuild. `BlobValueCache` is optional, bounded, and admits only values with repeated demand or explicit hotness feedback. Sequential scans do not populate it by default.

Blob payload bytes are charged to the query and cache owner until the consumer releases them.

### 12.4 Buffer Manager

The buffer manager exposes reservations rather than raw allocations:

```text
Reserve(kind, bytes, revocability, deadline)
CommitReservation(token)
ReleaseReservation(token)
TryRevoke(token)
```

Page decode, vector batches, compaction buffers, index postings, spill buffers, and Blob decompression all use this path. Large allocations have maximum-size checks before I/O or decompression.

## 13. I/O Scheduling

### 13.1 Request Classes

I/O requests declare:

```text
IoClass {
  COMMIT_FSYNC
  COMMIT_APPEND
  POINT_READ
  METADATA_READ
  ANALYTICAL_READ
  FLUSH_WRITE
  COMPACTION_READ
  COMPACTION_WRITE
  INDEX_READ
  INDEX_WRITE
  BLOB_READ
  BLOB_GC_WRITE
}
```

`COMMIT_FSYNC` and required Manifest edits use the reserved lane. Point and metadata reads receive latency protection. Large sequential work uses coalescing and lower-priority tokens.

### 13.2 Read Coalescing and Prefetch

The query runtime may coalesce adjacent page requests within a Block and prefetch the next Block only when the query's queue, memory pin, and I/O grants allow it. Prefetch is cancellable and revocable. It must not fill the cache or consume critical I/O tokens speculatively.

### 13.3 Write Ordering

The scheduler cannot reorder durability dependencies:

```text
prepare fsync
  -> DecisionLog fsync
  -> visible publication
```

For files:

```text
data write
  -> file fsync
  -> rename
  -> directory fsync
  -> Manifest edit fsync
```

Resource throttling may delay a step but cannot change its order.

## 14. Cancellation, Shutdown, and Recovery

### 14.1 Cooperative Cancellation

Every task checks cancellation at its natural yield boundary:

- vector batch;
- Page or Blob Block;
- SST input/output Block;
- index posting Block;
- WAL batch or maintenance phase.

Cancellation before a non-revocable durability boundary discards temporary work. Cancellation after that boundary completes the required protocol and reports the final status to the owner.

### 14.2 Graceful Shutdown

Shutdown transitions are:

```text
RUNNING
  -> QUIESCING
  -> DRAINING_COMMITS
  -> DRAINING_MAINTENANCE
  -> CHECKPOINTING
  -> CLOSED
```

During `QUIESCING`, new queries, writes, and optional maintenance are rejected. Existing read queries are cancelled or allowed to finish according to the close policy. Prepared transactions and commit-critical work drain first. Optional maintenance is cancelled at safe boundaries. Checkpointing advances only when its Manifest and outcome/timeline metadata are durable.

### 14.3 Crash Recovery

Scheduler queues are discarded on crash. Recovery reconstructs required work from:

- WAL and DecisionLog;
- Manifest and VersionSet;
- Frozen/active file identities;
- index catalog and sidecar health;
- BlobHashIndex and reference catalog.

Temporary outputs and spill files are removed when unreferenced. No scheduler checkpoint is needed to prove a committed transaction.

During `RECOVERY`, foreground writes and user queries are not admitted until format, Manifest, DecisionLog, CommitTimeline, Blob index, and visible-prefix checks establish a trusted read/write state. Recovery receives the critical lane and only the maintenance resources needed to verify or replay durable work. A recoverable optional sidecar or statistics task is deferred until normal operation.

## 15. Error Semantics

The resource layer returns structured, retry-aware errors:

```text
AdmissionTimeout
ResourceExhausted
WriteStalled
DiskReserveExhausted
QueryMemoryLimit
MaintenanceBackoff
MaintenanceCancelled
DeadlineExceeded
QueryCancelled
ShutdownInProgress
IOError
Corruption
```

`WriteStalled`, `AdmissionTimeout`, and `MaintenanceBackoff` are retryable when the pressure state changes. `DiskReserveExhausted` is retryable only after safe space is reclaimed or the configured reserve changes. Corruption and failed durability are not converted into successful writes.

An admitted query may slow, spill, or be cancelled, but it cannot lose its `QuerySnapshot` silently. A prepared transaction cannot receive `MaintenanceBackoff` after its non-revocable phase; it receives completion resources or a durable failure.

## 16. Observability

### 16.1 Global Metrics

The resource governor exports:

```text
resource_state
usable_cpu_slots / reserved_slots / active_slots
memory_budget / pool_usage / revocable_bytes / emergency_reserve
io_tokens / queue_depth / device_latency
open_descriptors / temporary_space
```

### 16.2 Per-Class Metrics

Every work class reports:

```text
submitted / admitted / rejected / cancelled
queue_delay / service_time / deadline_misses
cpu_slots / memory_peak / io_bytes / io_ops
preemptions / backpressure_events
```

### 16.3 Storage and HTAP Metrics

The engine additionally reports:

- WAL and DecisionLog fsync latency and queue age;
- active/frozen MemTable bytes and oldest age;
- compaction debt, output/write amplification, and overlap closure size;
- index coverage and repair/build debt;
- Blob orphan bytes, GC progress, and relocation bytes;
- PageCache admission/bypass/pin statistics;
- query latency by class, snapshot age, spill, and worker skew;
- write-stall duration and cause;
- visible-prefix publication lag.

Every pressure transition records the triggering signal and the actions taken. This is required to distinguish a real storage bottleneck from a scheduler bottleneck.

## 17. Configuration and Profiles

The old component-local thread and enable switches are replaced by one versioned `ResourceProfile`:

```text
ResourceProfile {
  memory_limit,
  cpu_limit,
  critical_reserved_slots,
  query_concurrency,
  analytical_concurrency,
  maintenance_concurrency,
  io_rate_limits,
  disk_safety_reserve,
  temporary_space_limit,
  pressure_thresholds,
  cache_pool_targets,
  fairness_weights,
  cancellation_defaults
}
```

Profiles are:

```text
BALANCED
INGESTION_PRIORITY
ANALYTICS_PRIORITY
```

The default is `BALANCED`. Profiles change quotas and weights, not transaction visibility, file ownership, or correctness guarantees. Runtime-safe tuning creates a new immutable profile snapshot; a task keeps the snapshot used for its admission until it yields.

## 18. Mainstream Comparison

| System family | Relevant mechanism | Cedar adaptation | Deliberate difference |
|---|---|---|---|
| RocksDB | background jobs, compaction debt, write stalls, rate limiter | explicit MemTable/compaction pressure and reserved commit lane | one scheduler also accounts for vector queries, Blob GC, and snapshots |
| ClickHouse | background pools, workload scheduling, memory trackers | class queues, resource requests, cache admission, maintenance debt | commits and Manifest durability have a stronger reserved lane |
| DuckDB | task scheduler, buffer manager, spilling, pipeline breakers | morsel grants, query memory, spill and PageCache pin accounting | long-lived VersionSet/snapshot pins and LSM flush debt are first-class |
| PostgreSQL | background writer, WAL, checkpoints, buffer management | WAL/Manifest lanes and explicit pressure states | no process-global buffer assumptions or untracked query allocations |
| Lucene/Pinot | segment-local maintenance and immutable segment lifecycle | sidecar/index/Blob maintenance under one VersionSet-aware governor | index work can never become a correctness dependency |
| Linux cgroups | external CPU/memory/IO limits | optional outer cap discovered at open | engine-level admission still sees temporal/storage state |

Cedar adopts rate limiting, compaction debt, buffer accounting, spill, and immutable maintenance. It adds a database-level model for prepared transactions, HLC/DecisionLog commit completion, snapshot age, and Blob reference safety.

## 19. Known Trade-Offs

1. A reserved critical lane can leave capacity idle during a read-only workload. This is intentional: it protects sudden durable commits and can be borrowed only through explicit governor policy, never by independent pools.
2. A unified scheduler is more complex than separate pools. The complexity is concentrated in one observable control plane instead of repeated local heuristics.
3. Pressure feedback can reduce analytical throughput during write bursts. It prevents write stalls and is reported with the exact cause.
4. Query pins can retain old files and PageCache bytes for a long time. Admission, pin quotas, and snapshot-age metrics make the trade-off visible; the scheduler never deletes pinned data.
5. Background sidecar/index and Blob GC work may remain incomplete under sustained ingestion. Base scans and safe orphan retention preserve correctness.

## 20. Verification Strategy

### 20.1 Scheduler Model Tests

Use a deterministic virtual clock and fake CPU/I/O/memory providers to test:

- weighted fairness and aging;
- reserved critical-lane service;
- admission timeouts and queue deadlines;
- no oversubscription beyond grants;
- task cancellation at every yield boundary;
- pressure hysteresis and transition dwell times;
- shutdown drain order.

### 20.2 Resource Invariants

Property tests assert:

- physical memory accounting never exceeds the global hard budget;
- emergency and commit-critical reserves are never borrowed by optional work;
- a prepared transaction either receives completion resources or reaches a durable failure state;
- I/O token usage never exceeds device budgets except for reserved commit operations;
- no task holds a global scheduler lock while waiting for I/O;
- every admitted task eventually completes, cancels, or returns a durable error;
- queue cancellation releases all reservations.

### 20.3 HTAP Workload Tests

Run concurrent mixes of:

- point and interactive reads;
- long valid-time graph scans and variable-length paths;
- continuous single-shard and multi-shard writes;
- flush and overlapping size-tiered compaction;
- index build/repair and statistics merges;
- Blob reads and Blob GC relocation;
- long snapshots retained across file publication.

Verify commit durability, visible-prefix behavior, result stability, write-stall causes, and tail latency by class.

### 20.4 Pressure and Failure Tests

Inject:

- memory allocation failure and pinned-page exhaustion;
- full WAL queues and fsync latency spikes;
- disk reserve exhaustion and temporary-space failure;
- compaction output failure after partial writes;
- index sidecar build cancellation;
- Blob GC interruption before/after mapping CAS;
- query cancellation during Page, Blob, and spill I/O;
- process crash in every shutdown and maintenance phase.

Recovery must reconstruct required work from durable state and never treat a scheduler queue as a commit record.

### 20.5 Cache Tests

- sequential scans do not permanently evict hot point-read metadata;
- pinned pages remain available until the query releases its Snapshot;
- BlobValueCache does not fill from one-pass scans;
- cache borrowing and revocation respect pool and query pin limits;
- cache metrics explain hit-rate changes after pressure transitions.

### 20.6 Structural Performance Acceptance

Completion requires evidence that:

1. commit-critical queue latency remains bounded under analytical load until physical I/O is actually saturated;
2. analytical work yields or slows under MemTable, disk, or memory pressure instead of silently oversubscribing;
3. flush/compaction debt causes measurable priority and write backpressure before hard failure;
4. no component creates an ungoverned worker pool or bypasses the memory/I/O governor;
5. query and maintenance spills stay within temporary-space limits;
6. PageCache and Blob caches resist scan pollution;
7. shutdown and crash leave no scheduler-owned correctness state unreconciled;
8. metrics can attribute tail latency, write stalls, cache misses, and read amplification to a resource cause.

Absolute QPS and latency targets remain benchmark-specific. The scheduler acceptance is structural and comparative until hardware and workload profiles are fixed.

## 21. Module Boundaries

The new ownership layout is:

```text
runtime/
  resource_profile
  resource_governor
  memory_governor
  io_governor
  work_scheduler
  pressure_controller
  task_context
  cancellation

maintenance/
  flush_scheduler
  compaction_scheduler
  index_scheduler
  blob_gc_scheduler
  checkpoint_scheduler

cache/
  cache_manager
  metadata_cache
  page_cache
  blob_location_cache
  blob_value_cache
  admission_policy

storage/
  write_admission
  memtable_pressure
  compaction_debt
  disk_reserve

observability/
  resource_metrics
  pressure_events
  scheduler_trace
```

Dependency rules:

- WAL, DecisionLog, Manifest, and transaction code request critical-lane resources through `ResourceGovernor` but do not implement their own global scheduler;
- query runtime submits morsel tasks and memory/I/O reservations through the governor;
- maintenance tasks own their source Snapshot and output reservations;
- caches are children of `CacheManager`, not independent global singletons;
- pressure signals are read from storage and caches, while policy decisions remain in `PressureController`;
- scheduler state is not a durability dependency;
- metrics do not acquire locks on every row or page.

## 22. Complete Legacy Resource Removal

The clean-break implementation must remove or replace:

- `AdaptiveThreadPool` as a general component-owned worker pool;
- `ThreadPoolQueryExecutor` and `ParallelQueryEngine` independent queues;
- parallel compaction worker pools that bypass the database scheduler;
- independent batch-processing thread vectors for production storage work;
- WAL/background flush loops that do not use the critical I/O lane;
- `QueryCache`, `SstReaderCache`, version-chain cache, and BlockCache budgets that are not owned by `CacheManager`;
- component-local `num_threads=0 means all hardware threads` behavior;
- runtime enable/disable switches that allow work to bypass resource accounting.

The new WAL and DecisionLog implementation may retain an internal append/fsync loop, but it must be a child of the critical lane and report reservations and metrics to the governor. Test-only utility threads are outside the production resource graph.

## 23. Implementation Dependency Order

The future implementation plan must follow this order:

1. Define `ResourceProfile`, task classes, requests, reservations, and structured errors.
2. Implement `MemoryGovernor`, `IoGovernor`, and descriptor/temporary-space accounting.
3. Implement `WorkScheduler` queues, fairness, aging, cancellation, and critical-lane reservations.
4. Implement pressure states, hysteresis, MemTable/WAL/compaction/disk signals, and write admission.
5. Move PageCache, metadata, Blob, and query memory into `CacheManager` and governor budgets.
6. Adapt WAL, DecisionLog, Manifest, and transaction completion to the critical lane.
7. Adapt flush, compaction, index, statistics, and Blob GC tasks to maintenance scheduling.
8. Adapt T-Cypher morsel pipelines, query admission, spill, and snapshot pins.
9. Add shutdown/recovery work reconstruction and fault injection.
10. Add metrics, scheduler traces, pressure diagnostics, and HTAP workload benchmarks.
11. Remove all legacy component-owned pools, caches, and bypass switches.
12. Run fairness, pressure, crash, snapshot, cache, and structural performance acceptance.

This is architectural sequencing, not a code implementation plan. Each implementation task must be test-first and independently reviewable after explicit authorization.

## 24. Completion Definition

The single-node HTAP resource stage is complete only when:

1. every production task is admitted and metered by one ResourceGovernor;
2. commit-critical WAL/DecisionLog/Manifest work has reserved CPU, I/O, and completion resources;
3. queries, flush, compaction, index builds, statistics, and Blob GC use typed queues and bounded grants;
4. memory, I/O, descriptors, and temporary space have hierarchical budgets and emergency reserves;
5. pressure states produce deterministic admission, throttling, and write-stall behavior;
6. prepared transactions cannot be stranded by pressure or optional maintenance;
7. cache admission prevents sequential scans from permanently polluting point-read working sets;
8. snapshots, PageCache pins, sidecars, Blob GC, and compaction remain safe across cancellation and shutdown;
9. scheduler queues are reconstructible from durable storage state after crash;
10. observability attributes queue delay, stalls, tail latency, cache behavior, and write amplification to resource causes;
11. deterministic scheduler, HTAP stress, pressure, fault, recovery, and cache tests pass;
12. all legacy ungoverned pools, caches, and bypass switches are removed;
13. correctness-kernel, columnar, T-Cypher, and temporal-index invariants remain intact.
