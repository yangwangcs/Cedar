# Cedar-Owned RocksDB Runtime Design

**Date:** 2026-08-17

**Status:** Approved for implementation

**Depends on:** `2026-08-03-cedar-authoritative-columnar-facts-complete-design.md`

## 1. Purpose

This design removes avoidable RocksDB runtime policy from Cedar's production
append path without changing the authoritative columnar facts architecture.
RocksDB remains Cedar's storage kernel for WAL encoding and recovery, sequence
numbers, MemTable lifecycle, table construction and reading, VersionSet,
MANIFEST, snapshots, checkpoints, backups, and live-file reclamation. Cedar
owns when foreground commits and background maintenance execute.

The target data path remains:

```text
one Cedar decided epoch
        |
        v
one RocksDB WAL record and one durable sync
        |
        +-> meta/default MemTables and BlockBasedTable files
        |
        +-> facts PartitionedVersionRadixMemTable
                       -> Cedar authoritative columnar facts files
        |
        v
RocksDB VersionSet and MANIFEST
```

This design changes policy ownership, not the durable representation or the
single recovery path.

## 2. Evidence and problem statement

The latest production append measurement used 512 asynchronous workers and a
maximum group size of 256 for 30.0104 seconds. It committed and published
155,922 transactions through 743 durable synchronizations: 209.855
transactions per synchronization and 5,195.61 transactions per second. Reopen
verification succeeded with no background error or hard-pressure event.

At approximately 25 durable synchronizations per second, a group limit of 256
has a sync-only ceiling near 6,400 transactions per second. Parameter tuning
cannot honestly claim 30,000 transactions per second under that physical sync
rate and group limit. Software optimization must first remove CPU, scheduling,
and I/O interference, then report the remaining physical limit explicitly.

Four software defects or ownership problems are established:

1. A Cedar-decided exclusive group still enters RocksDB's generic `WriteImpl`,
   including generic write-thread leadership, preprocessing, admission,
   statistics, WAL, MemTable, flush, and compaction scheduling machinery.
2. Cedar polls RocksDB pressure and runtime properties on foreground epochs and
   on 5 ms preflight-worker timeouts instead of reading cached Cedar state.
3. RocksDB starts periodic statistics, info-log, flush, and compaction policy
   work that Cedar did not explicitly admit. Background SST I/O can increase
   macOS `F_FULLFSYNC` latency by filling the same device queue.
4. The N+1 overlap is functionally ineffective. A measured run produced 31,362
   speculative decisions and discarded 31,355 of them. Validation and encoding
   are therefore not hidden by the preceding WAL synchronization.

## 3. Goals

- Preserve one RocksDB WAL record and one durable synchronization per committed
  Cedar epoch.
- Preserve WAL-before-MemTable ordering and the existing durable-callback
  contract.
- Remove redundant generic write grouping from Cedar's exclusive writer path.
- Make all production thread counts and background work admission explicit in
  Cedar configuration.
- Prevent new flush or compaction I/O from being submitted during the WAL sync
  critical interval.
- Replace foreground RocksDB property polling with cached Cedar metrics.
- Promote valid N+1 speculative epochs in steady append workloads instead of
  rebuilding them.
- Preserve bounded memory, WAL retention, L0 debt, shutdown, and error behavior
  when autonomous RocksDB maintenance is disabled.
- Measure every important foreground stage and distinguish software cost from
  media synchronization cost.
- Keep the RocksDB fork narrow, documented, tested, and straightforward to
  rebase.

## 4. Non-goals

- Replacing the RocksDB WAL, WAL recovery, MANIFEST, or VersionSet.
- Writing a Cedar WAL and invoking RocksDB with `disableWAL=true`.
- Weakening the default promise that a successful durable callback represents
  the configured production durability boundary.
- Claiming that `use_fsync`, `manual_wal_flush`, or `wal_bytes_per_sync` removes
  a synchronous macOS `F_FULLFSYNC`.
- Making `F_BARRIERFSYNC` the default before its power-loss behavior is proven
  on supported hardware and filesystems.
- Adding additional WAL shards or changing the single-WAL architecture in this
  delivery.
- Tuning benchmark-only shortcuts that are not valid for sustained production
  execution and reopen recovery.

## 5. Selected approach

Implementation has two cumulative stages. Stage A establishes a Cedar lean
profile using public or low-risk RocksDB controls. Stage B introduces a narrow
RocksDB kernel interface and Cedar-owned maintenance scheduling. Stage A remains
enabled in Stage B.

### 5.1 Rejected alternative: Cedar-owned WAL

A Cedar-owned WAL followed by RocksDB writes with WAL disabled would provide
maximum scheduling control, but it would duplicate record framing, checksums,
rotation, replay, sequence mapping, partial-record handling, checkpoint
coordination, and the crash boundary between replay and MANIFEST state. This
would create a second recovery protocol and violate the single-recovery-path
design. It is not part of this implementation.

## 6. Stage A: Cedar lean profile

### 6.1 Periodic and statistics work

The production storage profile sets RocksDB statistics dump and persistence
periods to zero. Cedar does not construct RocksDB `Statistics` in production
unless an explicit diagnostic profile requests it. Production also disables
periodic info-log flushing and time-trigger compaction polling through the
smallest tested fork hook required to prevent registration. Error logging
remains available and shutdown explicitly flushes the log when configured.

No diagnostic feature may silently re-enable production periodic work. The
diagnostic profile must be opt-in and exposed in runtime metrics.

### 6.2 Explicit background resources

The production profile no longer derives four to sixteen RocksDB background
jobs from host concurrency. Cedar configuration explicitly assigns:

| Role | Default | Owner |
| --- | ---: | --- |
| Commit worker | 1 | Cedar |
| N+1 preparation worker | 1 | Cedar |
| Metrics/pressure sampler | 1 | Cedar |
| Flush worker | 1 | Cedar |
| Compaction worker | 1 | Cedar |
| File purge execution | 0 or 1 | RocksDB kernel, explicitly configured |

The flush and compaction workers are created in Stage B. Stage A fixes RocksDB
thread counts and records their actual values so benchmarks never depend on an
implicit host-size heuristic.

### 6.3 WAL parameters

The initial production WAL profile is:

```text
manual_wal_flush       = false
use_fsync              = false
wal_bytes_per_sync     = 0
recycle_log_file_num   = 0 initially
max_total_wal_size     = Cedar budget-derived hard limit
```

On the supported macOS path, RocksDB `Sync()` and `Fsync()` both call
`fcntl(F_FULLFSYNC)`. Therefore `use_fsync` remains false for clarity but is not
treated as a performance optimization. Each synchronous Cedar epoch still
performs its required full synchronization.

`wal_bytes_per_sync` is initially zero because the explicit synchronous epoch
already establishes the durability boundary; additional range synchronization
between epochs must demonstrate a sustained benefit before being enabled.

WAL recycling is a separate experiment. Values from two through four may be
qualified only after tests cover rotation, reopen, corrupted/truncated tails,
crash recovery, checkpoint, and backup behavior. It is not enabled merely from
a favorable short benchmark.

WAL preallocation and rotation metrics record file creation, allocation, roll,
append, sync, directory sync where applicable, and error latency. Cedar avoids
voluntary rotation while a foreground sync is active, but all size and recovery
invariants remain authoritative.

### 6.4 Cached runtime state

One Cedar sampler publishes an immutable atomic snapshot used by commit,
preflight, admission, and reporting paths. It samples every 50 ms in normal
state, every 10 ms in soft pressure, and every 5 ms in hard or emergency
pressure. State transitions may wake the sampler immediately. Foreground paths
must not query RocksDB properties.

The snapshot contains at least:

```cpp
struct CedarRuntimeSnapshot {
  uint64_t sampled_at_us;
  uint64_t immutable_memtable_bytes;
  uint64_t active_memtable_bytes;
  uint64_t block_cache_bytes;
  uint64_t retained_wal_bytes;
  uint64_t l0_files;
  uint64_t pending_compaction_bytes;
  uint64_t background_errors;
  PressureState pressure;
};
```

A stale snapshot is conservative. After 250 ms without a successful sample,
Cedar stops increasing admission and samples synchronously on the control
thread; it does not make foreground workers call arbitrary RocksDB properties.

## 7. Stage B: Cedar kernel mode

### 7.1 Fork boundary

The RocksDB fork exposes a Cedar-specific header with three conceptual entry
points. Exact C++ ownership types may follow existing RocksDB conventions, but
the behavioral surface must remain equivalent to:

```cpp
CedarCommitResult WriteCedarEpoch(CedarEpoch&& epoch);
MaintenanceState PollCedarMaintenance();
MaintenanceResult RunCedarMaintenance(const MaintenanceBudget& budget);
```

These are storage-mechanism interfaces, not policy interfaces. They do not
choose group size, collection windows, thread counts, pressure thresholds, or
maintenance priority.

`WriteCedarEpoch` is valid only when Cedar owns the single exclusive production
writer and kernel mode was selected at database open. Other write entry points
are rejected for Cedar-managed column families while this mode is active.

The implementation must extract or reuse RocksDB primitives for WAL append,
sequence allocation, MemTable insertion, error propagation, and recovery state.
It must not copy a private snapshot of `WriteImpl` that can drift from RocksDB
correctness fixes.

### 7.2 Foreground commit contract

For one already-decided epoch, `WriteCedarEpoch` performs this ordered state
machine:

```text
validate kernel-mode invariants
  -> reserve one contiguous RocksDB sequence interval
  -> encode and append one WAL record
  -> execute the configured durable sync
  -> invoke the WAL durable callback exactly once
  -> insert the same batch into the selected MemTables
  -> publish the RocksDB sequence boundary
  -> report maintenance debt without scheduling it
```

The callback remains after successful WAL durability and before client-visible
publication. A failure before the durable boundary returns a determinate error
when RocksDB can prove no durable record exists. A failure whose durable state
cannot be proven enters recovery-required. A failure after the WAL durable
callback also enters recovery-required; Cedar does not retry the epoch in the
same process. Reopen and WAL recovery decide the result.

Kernel mode retains RocksDB protection information, WriteBatch validation,
column-family identity validation, log-number references, sequence overflow
checks, write-stall safety, and background-error integration. Generic grouping,
leader election, redundant admission policy, per-write property sampling, and
autonomous maintenance scheduling are bypassed.

### 7.3 N+1 double-slot protocol

The commit pipeline owns exactly two immutable slots:

```cpp
struct DecidedEpochSlot {
  uint64_t generation;
  CommitSeq base_visible_seq;
  std::vector<std::shared_ptr<AppendCommitRequest>> requests;
  std::unique_ptr<internal::DecidedEpoch> epoch;
  SlotState state;
};
```

Slot N is frozen and executing `WriteCedarEpoch`. During its WAL append and
sync, the preparation worker freezes a precise queue prefix for slot N+1. N+1
uses the only legal successor base: N's base plus N's committed transaction
count. Queue arrivals after the prefix do not invalidate it.

N+1 is promoted only when all conditions hold:

- N reached the durable and publishable boundary successfully.
- N+1 generation and successor base match N's published result.
- Every frozen N+1 request remains present in the same prefix and is not
  cancelled.
- Shutdown or recovery-required has not begun.

N+1 is discarded only for predecessor failure or indeterminacy, explicit
request cancellation, generation/base mismatch, or shutdown. The normal arrival
of later requests is not a discard reason. Slot transitions occur under one
pipeline mutex, while validation and encoding execute outside it against
immutable inputs.

Steady independent append workloads must promote at least 95% of eligible N+1
epochs and discard less than 1%, excluding explicit fault injection, request
cancellation, and shutdown. Metrics report eligible, started, decided, promoted,
discarded-by-reason, and CPU time hidden behind WAL sync.

### 7.4 Maintenance state and budgets

RocksDB kernel mode marks work as debt but does not autonomously submit flush or
compaction jobs. `PollCedarMaintenance` returns a coherent snapshot containing
at least immutable MemTable count and bytes, oldest immutable age, retained WAL
bytes, L0 file count, pending compaction bytes, manual-compaction conflict,
background error, and shutdown state.

The Cedar maintenance controller grants work using:

```cpp
struct MaintenanceBudget {
  uint64_t max_input_bytes;
  uint64_t max_output_bytes;
  uint64_t deadline_us;
  bool allow_flush;
  bool allow_compaction;
  bool yield_for_wal_sync;
};
```

`RunCedarMaintenance` executes no work outside the budget except a small,
documented atomic install or cleanup boundary required for correctness. It
returns consumed bytes, elapsed time, remaining debt, yield reason, and error.

### 7.5 WAL-priority I/O scheduling

The commit worker announces a sync-critical interval before WAL append. During
that interval the controller submits no new compaction work. Flush work is also
held unless memory or WAL retention has entered emergency state. Work already
executing checks the Cedar yield token at table-builder or bounded-byte
boundaries. RocksDB file-format invariants are never interrupted mid-record or
mid-install.

After the interval, Cedar distributes an I/O budget in this order:

1. Emergency flush required to preserve bounded memory or WAL retention.
2. Normal flush required to keep immutable age and bytes bounded.
3. Emergency L0 compaction required to avoid read or file-count collapse.
4. Normal compaction from the remaining byte and deadline budget.

The controller uses hysteresis. It does not oscillate between foreground and
maintenance on each sample. Budget decisions and reasons are observable.

### 7.6 Hard safety limits

Moving policy into Cedar must not remove RocksDB safety behavior. Cedar stops or
rejects new admission when any hard condition is reached:

- Immutable MemTable count or bytes exceed the configured hard bound.
- Retained WAL bytes reach `max_total_wal_size`.
- L0 reaches the configured emergency stop threshold.
- The runtime snapshot is stale and a control-thread refresh cannot succeed.
- RocksDB reports a background, MANIFEST, WAL, filesystem, or recovery error.

Emergency maintenance is explicit and measured. If it cannot make progress, the
database returns pressure or recovery-required instead of silently allowing
unbounded state.

### 7.7 Shutdown

Shutdown executes in this order:

1. Stop accepting new commit requests.
2. Resolve or fail queued requests.
3. Join the preparation worker.
4. Allow the active commit to finish or enter recovery-required.
5. Join the commit worker.
6. Stop maintenance admission and join flush/compaction workers.
7. Take a final runtime and error snapshot.
8. Invoke RocksDB close, MANIFEST, log, and file cleanup behavior.

No Cedar callback may execute after its owner is destroyed. No RocksDB
background job may outlive the corresponding Cedar controller in kernel mode.

## 8. Metrics and benchmark contract

Every epoch records queue, collection, validation, assembly/encoding, generic
kernel preprocessing that remains, WAL append, WAL sync, durable callback,
MemTable insertion, publication, and total database-write latency. WAL rotation
and MANIFEST latency are separately attributed rather than hidden in sync.

Maintenance records requested and consumed input/output bytes, queue delay,
execution time, yield count and reason, WAL-critical overlap, flush debt,
compaction debt, and all pressure transitions.

Benchmark reports must include:

- Wall-clock duration, submitted, committed, published, and reopened counts.
- Durable sync count and transactions per sync.
- Throughput plus latency p50, p95, p99, and maximum.
- N+1 promotion and discard-by-reason counts.
- Foreground stage timings and WAL rotation counts.
- Flush/compaction input and output rates and ending debt.
- Actual thread counts, WAL parameters, hardware, filesystem, and durability
  primitive.
- Background errors, hard-pressure time, and reopen verification.

No short run, unflushed database, weaker durability mode, or missing reopen
verification may be reported as production throughput.

## 9. Durability experiments

A raw append plus `F_FULLFSYNC` microbenchmark measures the hardware floor at
realistic Cedar group sizes before and after background-I/O isolation. It does
not replace end-to-end qualification.

`F_BARRIERFSYNC` may be implemented only as an explicitly named experimental
durability profile. It must never share the production profile name or results.
Promotion requires a written supported-filesystem matrix, documented ordering
and power-loss semantics, crash and forced-power-loss evidence on target
hardware, and explicit user selection. Until then, successful barrier results
mean only that the experimental boundary completed.

## 10. Delivery sequence

1. Add missing stage metrics and raw sync microbenchmarks without changing
   production behavior.
2. Implement and qualify the Stage A lean profile and cached sampler.
3. Replace the current N+1 handoff with the double-slot protocol and prove its
   promotion rate.
4. Add the narrow `WriteCedarEpoch` kernel path while retaining the generic
   implementation as a test oracle and rollback profile.
5. Add maintenance polling and manual flush execution.
6. Add budgeted manual compaction and WAL-priority yielding.
7. Disable autonomous RocksDB maintenance only after Cedar hard-limit and
   shutdown tests pass.
8. Run the full correctness, sanitizer, crash, sustained-load, and reopen gates.
9. Compare lean, kernel, and generic control profiles from identical database
   seeds and publish the limiting stage honestly.

Each step is independently reviewable. Kernel mode does not become the default
until all gates pass. The generic profile remains available for differential
tests and emergency rollback during this delivery.

## 11. Verification gates

### 11.1 Correctness

- Existing complete Cedar correctness suite, currently 323 tests.
- WAL record and recovered database equivalence between generic and kernel
  profiles for the same decided epochs.
- Deterministic N+1 tests across thousands of epochs, including arrivals during
  sync, cancellations, predecessor conflicts, failure, indeterminacy, and
  shutdown.
- Tests proving no RocksDB-owned periodic, flush, or compaction task executes
  after kernel-mode open.
- Explicit maintenance budget, yield, hard-limit, error, and shutdown tests.
- Checkpoint, backup, reopen, WAL rotation, and optional recycling tests.

### 11.2 Crash boundaries

Fault injection covers at least:

```text
before WAL append
after append and before sync
after sync and before durable callback
after callback and before MemTable insertion
during MemTable insertion
after MemTable insertion and before publication
during flush file construction
after file construction and before MANIFEST install
after MANIFEST install and before obsolete-file cleanup
```

Every boundary has an expected determinate, indeterminate/recovery-required, or
recoverable outcome and verifies it after reopen.

### 11.3 Dynamic analysis

- ASAN and UBSAN on the complete relevant suite.
- TSAN on commit, N+1 slot, sampler, maintenance, and shutdown stress tests.
- No ignored sanitizer error and no test-only synchronization in production
  code.

### 11.4 Performance

- Raw durable-sync benchmark with idle device and controlled background I/O.
- 2,048-worker smoke test.
- 30-second warm measurement.
- 30-to-60-minute sustained measurement with stable memory, WAL retention, L0,
  and compaction debt.
- Identical generic, lean, and kernel profile comparison.
- Near-zero eligible N+1 discard and at least 95% promotion.
- Zero unexplained background work, background errors, or reopen mismatch.

There is no fabricated 30,000-transactions-per-second acceptance gate. The
implementation passes when it demonstrably removes the specified software
costs, preserves all correctness gates, sustains bounded maintenance, and
reports the remaining physical WAL limit. Any later 30,000 target must state the
required durable-sync rate, transactions per sync, or explicitly approved
architecture change.

## 12. Compatibility and rollback

This delivery does not change Cedar keys, facts values, Parquet table bytes, WAL
format, or MANIFEST format. Databases created by the same authoritative
columnar-facts format remain reopenable by the generic profile.

Kernel mode is selected at open and recorded in runtime evidence, not persisted
as a new database format. If the kernel path fails qualification, Cedar reverts
to the generic execution profile without data conversion. A recovery-required
process must reopen before changing profiles.

## 13. Ownership summary

| Concern | Owner after this design |
| --- | --- |
| Transaction decision, grouping, and Cedar commit sequence | Cedar |
| N+1 validation and encoding pipeline | Cedar |
| WAL format, checksum, append primitive, and replay | RocksDB kernel |
| Durable-sync scheduling and foreground priority | Cedar policy using RocksDB I/O primitive |
| RocksDB sequence and MemTable insertion | RocksDB kernel |
| Mutable/immutable MemTable lifecycle state | RocksDB kernel |
| Flush and compaction admission and budgets | Cedar |
| Flush and compaction storage mechanics | RocksDB kernel and Cedar facts table modules |
| Facts Parquet bytes and indexes | Cedar facts table modules |
| VersionSet, MANIFEST, snapshots, and live files | RocksDB kernel |
| Runtime sampling, pressure, threads, and shutdown order | Cedar |

This boundary gives Cedar control over performance-sensitive policy while
continuing to reuse the RocksDB mechanisms whose replacement would create a
second durability and recovery system.
