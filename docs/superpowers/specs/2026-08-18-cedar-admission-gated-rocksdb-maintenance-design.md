# Cedar Admission-Gated RocksDB Maintenance Design

**Date:** 2026-08-18

**Status:** Approved by the user's standing authorization; implementation not started

**Depends on:**

- `2026-08-03-cedar-authoritative-columnar-facts-complete-design.md`
- `2026-08-17-cedar-owned-rocksdb-runtime-design.md`
- `2026-08-17-cedar-bounded-async-executor-design.md`

## 1. Purpose

This design sharpens the Cedar/RocksDB maintenance seam without changing the
database architecture. Cedar continues to use one RocksDB WAL and one RocksDB
recovery path. Facts remain authoritative only in RocksDB MANIFEST-managed
Cedar Parquet files. RocksDB continues to own sequence allocation, MemTable
lifecycle mechanics, flush and compaction mechanics, VersionSet, MANIFEST,
snapshots, checkpoints, backups, and obsolete-file deletion.

The adjustment is limited to policy ownership:

```text
RocksDB discovers debt and maintains native candidate queues
                         |
                         v
Cedar samples one coherent DB-wide maintenance snapshot
                         |
                         v
Cedar grants a bounded flush or compaction token
                         |
                         v
RocksDB consumes the token through its native picker and background job
                         |
                         v
RocksDB returns progress, remaining debt, yield reason, and errors
```

RocksDB is therefore not autonomous in Kernel Mode, but Cedar also does not
reimplement RocksDB maintenance. Cedar wraps and admits the existing RocksDB
mechanism.

## 2. Non-negotiable invariants

The implementation must preserve all of the following:

1. One Cedar epoch is encoded as one RocksDB WriteBatch, one RocksDB WAL record,
   and one configured durable synchronization.
2. `disableWAL=true` is prohibited for production commits. Cedar does not create
   a second WAL or a second replay protocol.
3. RocksDB owns WAL framing, checksums, append, rotation, recovery, log-number
   tracking, and WAL-to-MANIFEST coordination.
4. The WAL durable callback remains after successful WAL durability and before
   MemTable publication becomes client-visible.
5. The facts column family uses `PartitionedVersionRadixMemTable` and
   MANIFEST-installed Cedar Parquet v2 tables as its only immutable authority.
6. The meta and default column families remain RocksDB BlockBasedTable column
   families. There is no Parquet side manifest and no Cedar file garbage
   collector.
7. RocksDB owns MemTable switching, immutable-list mutation, table construction,
   compaction picking, VersionEdit installation, VersionSet, MANIFEST, and
   obsolete-file deletion.
8. Cedar owns commit admission, group sizing, the N+1 pipeline, sampling cadence,
   maintenance priority, maintenance concurrency, and maintenance budgets.
9. Recovery flushes and recovery-required transitions may not depend on a live
   Cedar policy thread. Reopen must reconstruct all maintenance debt from
   RocksDB state.
10. Generic and Lean profiles remain rollback paths and can reopen a database
    last written by Kernel Mode without format conversion.

## 3. Current implementation defects addressed here

### 3.1 Column-family scope mismatch

Kernel Mode currently suppresses autonomous maintenance for the entire DBImpl,
but Cedar samples and runs maintenance only against `facts_cf`. Every committed
epoch also writes visible-watermark and transaction metadata to `meta_cf`, and
all column families share the production WriteBufferManager. Meta or default
MemTables can therefore stop DB-wide writes while the facts-only snapshot shows
no immutable debt.

The new seam is DB-wide. It reports per-column-family debt for observability,
but Cedar does not pick storage files or encode RocksDB picker rules.

### 3.2 Synchronous manual API re-entry

The current `RunCedarMaintenance` calls synchronous manual `FlushMemTable()` and
`CompactFiles()`. A manual flush joins RocksDB's WriteThread through
`EnterUnbatched()`. If a foreground writer is already sleeping in `DelayWrite()`
under write stop, the maintenance caller can wait behind the writer that only
maintenance can unblock.

The replacement uses RocksDB's native background flush and compaction queues.
It never uses the public synchronous manual-flush path to recover from write
stop.

### 3.3 Lost progress feedback

The current Cedar adapter discards `CedarMaintenanceResult`. The controller
cannot distinguish progress from input-budget, output-budget, deadline,
WAL-priority, conflict, or shutdown yields. The replacement makes the result a
required controller input and records every grant and completion.

### 3.4 One blocking maintenance lane

The current Cedar maintenance worker serializes flush and compaction. A long
compaction can prevent an emergency flush. The replacement provides exactly one
Cedar flush lane and one Cedar compaction lane by default. Each lane may have at
most one outstanding RocksDB job. More lanes require separate sustained
qualification.

## 4. Alternatives

### 4.1 Selected: admission-gated native RocksDB scheduler

RocksDB continues to discover debt, populate native queues, choose files, run
background jobs, install VersionEdits, and delete obsolete files. In Kernel
Mode, the scheduling point requires a matching Cedar grant before it submits a
flush or compaction job to the existing RocksDB environment pool.

Benefits:

- reuses the paths already coordinated with WriteController, WBM, VersionSet,
  error recovery, and shutdown;
- covers facts, meta, and default column families coherently;
- prevents Cedar from duplicating compaction-picker correctness;
- keeps the fork narrow and reviewable;
- permits Cedar to control concurrency, timing, and byte/deadline budgets.

This is the selected approach.

### 4.2 Rejected: synchronous public/manual wrapper

Cedar calls public `Flush()` and `CompactFiles()` and treats those calls as
Cedar-owned maintenance. This is close to the current implementation. It is
rejected because manual flush re-enters WriteThread, the caller must choose a
column family, total output bytes are not a hard `CompactFiles()` bound, and a
blocking compaction occupies the only maintenance caller.

### 4.3 Deferred: Cedar-owned flush and compaction executors

RocksDB exposes extracted low-level jobs and Cedar runs them on Cedar-created
threads. This offers maximum operating-system scheduling control, but it moves
thread lifetime, job cancellation, job-context cleanup, SuperVersion pinning,
and error-recovery coordination into a much larger fork seam. It is deferred
unless the selected approach leaves measured scheduler overhead after the
durable-sync and I/O-interference limits are isolated.

## 5. Profile model

### 5.1 Generic profile

The developer/oracle profile keeps upstream-style RocksDB behavior and
statistics. It exists for differential correctness and emergency diagnosis, not
for production throughput claims.

### 5.2 Cedar Lean Profile

Lean uses the production memory layout, WAL parameters, explicit RocksDB worker
counts, disabled periodic statistics/info-log work, cached Cedar sampling, and
the bounded Cedar commit executor. RocksDB still performs normal automatic
flush and compaction admission.

Lean is the control profile for determining whether Kernel Mode improves or
regresses write throughput, read amplification, or space amplification.

### 5.3 Cedar Kernel Mode

Kernel includes all Lean settings and enables admission-gated maintenance.
RocksDB may discover and queue debt but may submit no ordinary flush or
compaction job without a Cedar grant. Recovery and shutdown exceptions are
explicitly enumerated and measured.

Kernel Mode is not the default until the complete correctness, recovery,
sanitizer, read, write, space, and 30-minute sustained gates pass.

## 6. Deep maintenance module

The RocksDB fork exposes one deep module at the DBImpl seam. Cedar learns debt
and grants work classes; RocksDB hides column-family selection, memtable IDs,
file lists, picker rules, VersionEdits, job contexts, and background-pool
details.

The conceptual interface is:

```cpp
enum class CedarMaintenanceKind : uint8_t {
  kFlush,
  kCompaction,
};

enum class CedarMaintenancePriority : uint8_t {
  kNormal,
  kEmergency,
};

enum class CedarMaintenanceYield : uint8_t {
  kNone,
  kNoDebt,
  kStaleGeneration,
  kInputBudget,
  kOutputBudget,
  kDeadline,
  kWalSync,
  kManualConflict,
  kRecovery,
  kShutdown,
  kInvariantViolation,
};

enum class CedarColumnFamilyRole : uint8_t {
  kDefault,
  kFacts,
  kMeta,
  kOther,
};

struct CedarColumnFamilyDebt {
  uint32_t id = 0;
  CedarColumnFamilyRole role = CedarColumnFamilyRole::kOther;
  uint64_t active_memtable_bytes = 0;
  uint64_t immutable_memtable_bytes = 0;
  uint64_t immutable_memtable_count = 0;
  uint64_t oldest_immutable_age_us = 0;
  uint64_t l0_files = 0;
  uint64_t pending_compaction_bytes = 0;
  bool flush_pending = false;
  bool compaction_pending = false;
};

struct CedarMaintenanceSnapshot {
  uint64_t generation = 0;
  uint64_t sampled_at_us = 0;
  uint64_t total_active_memtable_bytes = 0;
  uint64_t total_immutable_memtable_bytes = 0;
  uint64_t total_immutable_memtable_count = 0;
  uint64_t write_buffer_manager_bytes = 0;
  uint64_t write_buffer_manager_limit_bytes = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_pending_compaction_bytes = 0;
  uint64_t running_flushes = 0;
  uint64_t running_compactions = 0;
  uint64_t background_errors = 0;
  bool write_delayed = false;
  bool write_stopped = false;
  bool manual_conflict = false;
  bool recovery_in_progress = false;
  bool shutting_down = false;
  std::vector<CedarColumnFamilyDebt> column_families;
};

struct CedarMaintenanceGrant {
  uint64_t snapshot_generation = 0;
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  CedarMaintenancePriority priority = CedarMaintenancePriority::kNormal;
  uint64_t max_input_bytes = 0;
  uint64_t max_output_bytes = 0;
  uint64_t deadline_us = 0;
  bool yield_for_wal_sync = true;
  const std::atomic<bool>* wal_sync_critical = nullptr;
};

struct CedarMaintenanceResult {
  uint64_t grant_id = 0;
  uint64_t selected_column_family_id = 0;
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t remaining_smallest_complete_unit_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint64_t elapsed_us = 0;
  CedarMaintenanceYield yield = CedarMaintenanceYield::kNone;
  CedarMaintenanceSnapshot remaining;
};

Status PollCedarMaintenance(DB* db, CedarMaintenanceSnapshot* snapshot);

Status RunCedarMaintenance(DB* db,
                           const CedarMaintenanceGrant& grant,
                           CedarMaintenanceResult* result);
```

`RunCedarMaintenance` may wait for the one admitted native background job to
finish, but it does not execute the public manual operation inline. Cedar calls
it from separate flush and compaction lanes, so one compaction cannot occupy the
flush lane.

The snapshot generation prevents a grant based on stale debt from being applied
to a materially different queue. A stale generation yields without consuming a
job token; Cedar immediately resamples.

## 7. RocksDB scheduling state machine

### 7.1 Debt discovery

All existing RocksDB call sites may continue to detect flush or compaction need
and enqueue native candidates. Kernel Mode changes only job submission. It does
not skip queue construction, WBM accounting, WriteController updates, compaction
scores, or recovery state.

### 7.2 Grant installation

`RunCedarMaintenance` validates the generation and budget under DBImpl mutex,
installs exactly one ephemeral grant, invokes the normal scheduler, and waits on
a grant-specific completion condition. A matching native job atomically
consumes the grant before submission. No second job can reuse it.

### 7.3 Candidate selection

For flush grants, RocksDB selects the candidate that most directly releases
write stop or WBM pressure, then the oldest immutable MemTable. It may choose
facts, meta, or default. Cedar does not pass a column-family handle.

For compaction grants, RocksDB uses its native picker and compaction score among
eligible column families. Emergency L0 debt outranks normal pending bytes.
Within the selected job, all required overlapping files remain included even if
that means the candidate cannot fit the current grant; in that case the job is
not started and the result yields `kInputBudget` or `kOutputBudget`.

### 7.4 Active MemTable handling

Normal writes continue to perform RocksDB's safe MemTable switch when a buffer
fills or WBM requests a flush. Cedar grants flush capacity before the configured
stop thresholds so an immutable candidate is runnable before writers sleep.

If WAL retention or idle-time policy requests an active-only flush while writes
are not stopped, RocksDB may switch the MemTable through an extracted internal
safe-point helper. It must not call the public manual flush wrapper. If RocksDB
reports write stop with DB-wide active bytes but no immutable candidate, the
result is `kInvariantViolation`; Cedar stops admission and reports the state
instead of waiting indefinitely.

### 7.5 Completion

The native background job records actual input bytes, output bytes, elapsed
time, selected column family, yield reason, errors, and the remaining DB-wide
snapshot. RocksDB signals the matching `RunCedarMaintenance` waiter after job
cleanup and before the grant state is destroyed.

## 8. Cedar controller

The Cedar controller contains policy only. It has four modules:

1. `RuntimeSampler` publishes the immutable DB-wide snapshot at 50 ms normal,
   10 ms soft, and 5 ms hard/emergency cadence.
2. `MaintenancePolicy` is a pure function from snapshot plus recent completion
   history to zero or one flush decision and zero or one compaction decision.
3. `FlushLane` owns one thread and at most one outstanding flush grant.
4. `CompactionLane` owns one thread and at most one outstanding compaction
   grant.

The policy uses completion feedback. `kWalSync`, `kStaleGeneration`, and
`kManualConflict` cause bounded resampling/backoff. Budget yields raise only the
next grant needed to cover one complete atomic input set; they do not remove the
global hard memory and disk bounds.

## 9. High-performance write policy

### 9.1 Foreground path

- Keep one exclusive Cedar commit worker and one immutable N+1 preparation
  slot.
- Keep one WAL record and one full durable sync per epoch.
- Keep production grouping bounded by 512 transactions and 2 MiB encoded bytes.
- Grow the busy target from 128 to 256 only under normal pressure; use the
  existing 200 microsecond collection window unless measurement selects a
  different production candidate.
- Never query RocksDB properties on a foreground commit thread.
- Preserve the durable callback and release mailbox reservations at the
  existing durability boundary.

### 9.2 N+1 protocol

The current double-slot rules remain: N+1 freezes one exact queue prefix while N
is in WAL append/sync, uses N's only legal successor base, and promotes only
after N publishes successfully. Later arrivals do not invalidate the prefix.

The metrics are corrected to count both epochs and transactions:

- preflight epochs/transactions;
- decided epochs/transactions;
- eligible epochs/transactions;
- promoted epochs/transactions;
- discarded epochs/transactions by reason;
- validation and assembly CPU microseconds hidden behind WAL sync.

Steady independent append qualification requires at least 95% of eligible
epochs promoted and less than 1% non-fault/non-cancellation discard.

### 9.3 WAL priority

The commit worker sets `wal_sync_critical` before WAL append and clears it only
after the RocksDB write result has crossed its required publication/error
boundary. While set:

- no normal compaction grant starts;
- no normal flush grant starts;
- an emergency flush may start only to avoid memory, WAL-retention, or write-stop
  failure;
- an already-running compaction is not interrupted mid-job; it completes its
  current native input overlap and atomic VersionEdit/MANIFEST installation,
  then reports elapsed time and any aggregate estimate overrun;
- no record, page, file install, VersionEdit, or MANIFEST write is interrupted.

### 9.4 WAL options

The initial production values remain:

```text
manual_wal_flush       = false
use_fsync              = false
wal_bytes_per_sync     = 0
recycle_log_file_num   = 0
max_total_wal_size     = Cedar budget-derived hard limit
```

On macOS, this does not remove `F_FULLFSYNC`. WAL recycling and
`F_BARRIERFSYNC` remain separate, opt-in experiments and are excluded from the
production qualification in this design.

## 10. High-performance read policy

Maintenance control must not improve foreground write throughput by allowing
read amplification to grow without bound.

### 10.1 Point reads

- Facts Get remains RocksDB file selection followed by embedded Parquet Bloom,
  row-group index, page index, and fixed-width key-page binary search.
- A point Get must not decode a complete row group.
- Meta/default point reads retain BlockBasedTable Bloom/index/cache behavior.
- Kernel Mode may not reduce the configured block/page cache to fund unbounded
  maintenance scratch memory.

### 10.2 Projected scans

- Event and state scans continue to prune files, row groups, and pages using
  safe key ranges and Parquet indexes.
- Only selected projection pages are decoded after MVCC/key-page selection.
- Maintenance reads and compaction construction must not populate the
  foreground page cache with one-use input pages.

### 10.3 Read-amplification control

- Normal compaction becomes eligible before L0 reaches the RocksDB slowdown
  trigger.
- Emergency L0 compaction is admitted at the configured emergency threshold,
  even under sustained writes, after any write-stop-relieving flush.
- The final sustained interval must show bounded L0 files and non-monotonic
  pending compaction debt.
- Identical-seed point Get, MultiGet, narrow projection, full projection, and
  mixed read/write comparisons are required across Lean and Kernel profiles.

Kernel passes the read gate when no workload regresses p99 latency or throughput
by more than 10% versus Lean on the same seed and host, unless the report shows a
corresponding, explicitly accepted space reduction. Correctness and bytes-read
pruning may never regress.

## 11. Space-efficiency policy

### 11.1 No duplicate authority

The design adds no sidecar facts files, second manifest, second WAL, secondary
projection authority, or Cedar-owned obsolete-file list. This preserves the
largest structural space saving: every immutable facts byte has one
MANIFEST-owned authoritative representation.

### 11.2 Compression

- RocksDB outer compression remains disabled for Cedar Parquet facts.
- Cedar Parquet pages retain `LZ4_RAW` for the initial production profile.
- Meta/default retain their existing qualified BlockBasedTable compression.
- Bottommost Zstd is not enabled by this implementation. It may be a later
  profile only after format interoperability, read CPU, compaction CPU, and
  sustained space-amplification evidence.
- WAL compression remains disabled on the latency-sensitive path.

### 11.3 File sizing and compaction

- Facts target files remain 128 MiB with independently bounded row groups and
  pages.
- A compaction grant's `max_output_bytes` is an aggregate admission budget, not
  merely `output_file_size_limit` per output file. RocksDB estimates the complete
  picked job before submission; an estimate over budget yields without starting
  the job and keeps the overlap debt queued.
- Once submitted, the native job completes its full input overlap and one atomic
  VersionEdit/MANIFEST installation. Compression variance may cross the budget;
  Cedar reports the atomic overrun and uses it to size the next grant. Cedar does
  not publish a partial overlap compaction.
- Obsolete-file deletion remains RocksDB-owned and is included in ending space
  metrics.

### 11.4 Retention

Compaction may remove only storage versions that RocksDB semantics and Cedar's
explicit vacuum/oldest-readable policy permit. It may not discard bitemporal
history merely to improve a benchmark. WAL retention is released by DB-wide
flush progress, including meta/default column families.

### 11.5 Space acceptance

For identical logical data and vacuum watermark:

- Kernel and Lean must produce the same visible facts and transaction outcomes;
- Kernel ending live SST bytes must be within 5% of Lean unless file-boundary
  effects are itemized;
- retained WAL must end below the 768 MiB soft threshold;
- pending compaction must end below the 8 GiB soft threshold;
- no L0, WAL, obsolete-file, or temporary-output class may grow monotonically
  through the final ten minutes;
- the report includes logical facts bytes, live SST bytes, WAL bytes, temporary
  peak bytes, obsolete bytes, compression input/output, and space amplification.

## 12. Maintenance priorities and bounds

The policy order is fixed:

1. Background/recovery error: stop new admission and expose the error.
2. Emergency DB-wide flush: write stop, WBM at or above 85%, hard WAL retention,
   or a column family at its last safe immutable buffer.
3. Normal DB-wide flush: any immutable candidate, WBM at or above 70%, active
   MemTable at or above 75% of its configured buffer, or soft WAL retention.
4. Emergency L0 compaction: any column family at its configured slowdown
   threshold or DB-wide L0 emergency threshold.
5. Normal compaction: native compaction score requests work, facts L0 reaches
   four files, meta/default reach their normal trigger, or pending compaction is
   nonzero.

Initial bounds retain the existing production safety constants:

| Signal | Soft/normal | Hard/emergency |
| --- | ---: | ---: |
| Retained WAL | 768 MiB | 1 GiB / configured max |
| Pending compaction | 8 GiB | 32 GiB |
| Facts L0 | 4 Cedar trigger | configured slowdown/stop |
| Immutable MemTables | 2 | 4 or CF-specific last safe buffer |
| WBM occupancy | 70% | 85% |
| Runtime snapshot age | adaptive cadence | 250 ms stale |

Flush grants cover one complete selected MemTable set. Compaction grants begin at
8 MiB but may increase to the smallest complete native overlap set, capped by
the configured safety maximum. Cedar never asks RocksDB to compact an unsafe
partial overlap set.

## 13. Error, recovery, and shutdown

### 13.1 Errors

Any WAL, MemTable, table-builder, compaction, VersionSet, MANIFEST, filesystem,
or background error is returned through the RocksDB status and DB-wide snapshot.
Cedar stops increasing admission. Indeterminate foreground writes enter
recovery-required and are not retried in-process.

### 13.2 Recovery

Grants and controller state are process-local and not persisted. On reopen,
RocksDB reconstructs WAL, MemTables, VersionSet, and maintenance queues. Recovery
flushes required for opening or error recovery bypass ordinary Cedar admission
through an explicit RocksDB-owned `kRecovery` path. This exception increments a
counter and cannot run ordinary policy compaction.

The same database must reopen under Generic, Lean, or Kernel profile. Kernel
selection does not change durable bytes.

### 13.3 Shutdown

Shutdown order is:

1. stop accepting new commits;
2. cancel or resolve queued mailbox and append requests;
3. join N+1 preparation;
4. finish the active commit or enter recovery-required;
5. join the commit worker;
6. stop new maintenance grants;
7. cancel at safe boundaries and join compaction, then flush lanes;
8. publish a final DB-wide snapshot and maintenance completion set;
9. close RocksDB and let RocksDB finish MANIFEST/log/file cleanup.

No completion waiter, callback, or token may outlive DBImpl or the Cedar
controller.

## 14. Metrics

The production snapshot and benchmark must record:

- profile, kernel-gate state, configured and actual thread counts;
- WAL directory/device, durability primitive, WAL options, rotation count,
  append/sync/close latency;
- per-CF active/immutable bytes and count, WBM usage/limit, oldest immutable age;
- per-CF and total L0 files and pending compaction bytes;
- grants requested/accepted/stale, selected CF, queue delay, input/output bytes,
  elapsed time, atomic overrun, and every yield reason;
- autonomous schedule attempts, jobs admitted by Cedar, recovery exceptions,
  and unexplained background jobs;
- point/MultiGet file, row-group, page, cache, and bytes-read metrics;
- projected scan input/pruned groups/pages and decoded bytes;
- logical facts bytes, live SST, WAL, obsolete, temporary peak, compression
  input/output, and space amplification;
- N+1 epoch and transaction promotion/discard metrics;
- submitted, durable, published, aborted, indeterminate, rejected, and reopen
  counts;
- pressure-state time and maximum runtime-snapshot age.

Production qualification requires zero unexplained ordinary RocksDB maintenance
jobs in Kernel Mode. Recovery exceptions must be named and countable.

## 15. Verification strategy

### 15.1 Deterministic unit tests

- pure Cedar policy tests for priority, hysteresis, DB-wide WBM thresholds,
  WAL-critical yielding, and result-driven retry;
- RocksDB token tests proving zero jobs without a grant and exactly one native
  job per matching grant;
- stale generation, zero budget, full overlap set, output overrun, deadline,
  manual conflict, recovery, error, and shutdown tests;
- per-CF aggregation tests where only meta or default has debt;
- N+1 generation/base/cancellation/failure/indeterminate/shutdown tests with
  epoch and transaction metrics.

### 15.2 Integration tests

- a real multi-CF WriteBufferManager fixture that fills meta while facts remains
  below its threshold and proves Cedar flushes the correct CF;
- a blocking foreground writer fixture that reaches write stop and proves a
  Cedar-admitted native flush resumes it without `EnterUnbatched` deadlock;
- independent flush/compaction lanes where a blocked compaction does not prevent
  an emergency flush;
- no-autonomous-work counters across active writes, flush debt, compaction debt,
  checkpoint, backup, and close;
- Generic/Lean/Kernel reopen and snapshot equivalence.

### 15.3 Crash and sanitizer tests

Crash boundaries cover before/after WAL append, sync, callback, MemTable insert,
flush construction, VersionEdit/MANIFEST install, and obsolete cleanup. ASAN,
UBSAN, and TSAN cover commit, N+1, sampler, grant state, completion waiters,
shutdown, and recovery exceptions.

### 15.4 Performance campaign

Campaigns use identical seeds and exact profile/configuration output:

1. 2,048-operation smoke for correctness and immediate rejection behavior.
2. 30-second warm Generic/Lean/Kernel comparisons.
3. 60-second and 300-second bounded preflights. A hang, write stop, rejection,
   background error, debt growth, or reopen failure stops the campaign.
4. A 30-minute bounded Kernel run only after the 300-second preflight passes.
5. Matching Lean and production-device runs for comparison.

Runs shorter than 1,800 seconds remain `warm_not_sustained`. Early failure may
shorten a failed campaign; a successful production qualification may not shorten
the 30-minute observation window.

There is no fabricated 30,000 transactions/s pass condition. The report must
show transactions per sync, measured durable-sync rate, the corresponding
physical ceiling, software-stage costs, read results, and space results. Kernel
passes only when correctness gates pass, maintenance remains bounded, N+1 meets
its promotion target, no measured read/write regression exceeds the stated
comparison bounds, and the remaining limit is honestly attributed.

## 16. Rollout and rollback

1. Implement DB-wide observation while retaining existing Lean automatic
   maintenance.
2. Implement the token gate behind `kernel_mode` with Kernel disabled by
   default.
3. Differentially verify native jobs with the gate off and on.
4. Enable Kernel only in tests and explicit benchmarks.
5. Run correctness, crash, sanitizer, read, write, and space campaigns.
6. Publish the qualification record.
7. Make Kernel the production default only after review of the 30-minute and
   production-device evidence.

Rollback selects Lean or Generic at open. It does not rewrite files or replay a
Cedar-specific log.

## 17. Ownership after this adjustment

| Concern | Owner |
| --- | --- |
| Transaction decision, grouping, N+1, Cedar commit sequence | Cedar |
| Commit and maintenance admission policy | Cedar |
| Sampling cadence and immutable runtime snapshot | Cedar |
| Flush/compaction grant priority, concurrency, byte/deadline budgets | Cedar |
| WAL format, append, sync primitive, rotation, replay | RocksDB |
| Sequence allocation and MemTable insertion/switch mechanics | RocksDB |
| Debt discovery, native candidate queues, picker, background jobs | RocksDB, gated by Cedar in Kernel Mode |
| Facts Parquet bytes, page indexes, codecs, projected scans | Cedar RocksDB modules |
| VersionSet, MANIFEST, snapshots, checkpoints, backups, obsolete files | RocksDB |
| Recovery-required semantics and profile-independent reopen | RocksDB plus Cedar status mapping |

This seam preserves the original design idea: Cedar owns performance-sensitive
policy while RocksDB remains the single storage and recovery kernel.
