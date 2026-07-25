# Cedar Six-Design Completion Matrix

Date: 2026-07-25

Status: Audit baseline for the active release/paper closure goal.

This matrix records the current evidence boundary. A green implementation test
does not by itself satisfy a release or paper gate; those gates also require a
reproducible artifact, provenance, and the declared statistical protocol.

## Summary

| Design | Current state | Remaining closure |
|---|---|---|
| Columnar | Typed SST/Blob, bounded reads, corruption, relocation and compaction have current format-1 focused evidence; the real `maintenance-cycle` campaign executes a two-SST merge and publishes nonzero compaction/cache peaks; current sanitizers pass | Produce production-scale observed-peak, full persistence-boundary and release capability artifacts |
| HTAP correctness kernel | Transaction, reservation, visible-prefix, MemTable, flush/compaction and recovery protocols have schema-3 focused evidence plus final fault/oracle and sanitizer gates | Produce production-scale conflict/serializability/concurrent-ingestion and every-boundary crash evidence |
| HTAP resource scheduling | Typed shared admission is complete; `scheduler-saturation` verifies 4:2:2:1 saturated fairness, EDF, a real worker-saturated deadline miss, and nonzero queue/service/CPU-grant activity for all 13 work classes; current sanitizers pass | Produce the same distributions at production scale and under concurrent HTAP load |
| T-Cypher vectorized execution | Typed execution and approved query shapes have current focused evidence; one Blob-bearing corpus now crosses active MemTable, a snapshot pinned across the frozen handoff, one/many SST, flush, compaction, reopen, real Blob relocation and reopen-after-relocation; typed spill faults and current sanitizers pass | Retain two approved deterministic exclusions and produce production concurrent-snapshot/disk-pressure artifacts |
| Temporal index/CBO | A public `index-path-matrix` campaign fail-closes unless all six required physical choices execute; a single lifecycle now compacts, repairs a corrupt sidecar, drops, reopens and proves index-ID non-reuse; CI emits nonzero candidates and current sanitizers pass | Produce production-scale plan-choice/resource-cost distributions |
| Observability/benchmark | The versioned production metric census, fail-closed activity validator, CI workload artifacts, cache modes, fault producers, paired driver, minimal-instrumentation profile and dedicated 2% throughput / 5% p99 overhead gate are implemented | Archive an approved production-scale paired comparison and workstation/stress release evidence; paper and external LDBC are excluded from this goal |

## Completion Evidence by Design

### Columnar

Authoritative completion definition: `docs/superpowers/specs/2026-07-17-cedar-columnar-design.md`, section 32.

Current evidence includes typed SST/page and Blob implementations under
`src/columnar/`, `src/blob/`, and `src/storage/`; corruption, publication,
compaction, Blob relocation, bounded-memory, reopen, and sanitizer tests in
`tests/test_correctness_kernel.cc`; and the columnar release evidence plans.

Open implementation/evidence items: page format now includes real
frame-of-reference, bitmap, bit-packing, delta-of-delta and Float32/Float64 XOR
codecs. GranuleBlock compares the integer candidates against Delta where legal,
and bitmap against RLE for boolean operation pages before persisting the selected
codec. Float32/Float64 inline values now use a fixed-width raw-bit layout;
GranuleBlock requests XOR for production float pages and the page codec retains
it only when the encoded payload clears the existing 12.5% benefit threshold.
Full Granule decode and selective SST reads share the same typed decoder, with
production-selection, point-read, malformed-codec, golden-header and sanitizer
focused evidence. The current layout uses `GBK5/5` and a `PDR3` page directory
whose entries carry encoded-page BLAKE3 hashes; older block layouts are not accepted. LZ4 1.10.0 and Zstd 1.5.7 are now
pinned source snapshots under `third_party/`, with retained licenses and release
archive SHA-256 metadata in `third_party/CODECS.md`; default builds link the
static bundled targets and have no dynamic host LZ4/Zstd dependency. An explicit
`CEDAR_ENABLE_ZSTD=OFF` build still returns deterministic `NotSupported` instead
of silently changing the requested format. `CedarDatabase::Open()` runs a
bounded codec round-trip self-test before telemetry, workers, or persistent-state
recovery and exports bounded success/failure counters.
The remaining codec release item is a durable capability artifact, not source
vendoring.
GranuleBlock now persists independent Inline/Blob presence bitmap pages, uses
bitmap rank while mapping block rows to dense value streams, validates the
bitmaps against ValueClass in full and selective SST reads, and exports bounded
per-page metrics for both new page types. The current SST layout now persists
storage-shard and logical/physical type identity, stable sort/hash/encoding/
compression/checksum IDs, a BLAKE3 file identity repeated in header/footer, and
a bounded checksummed statistics region with full key/time/commit ranges and
row/value-class counts. Oversized variable-width typed min/max values are
explicitly marked incomplete rather than copied into unbounded metadata.
Blob-backed values also mark file typed min/max incomplete, preventing unsafe
pruning without reading their payloads. Footer ranges use checked arithmetic
and hard bounds before I/O; BlockIndex length is derived exactly from
`block_count`. Fixed-size BlockIndex entries persist row counts and block
content commitments, metadata-only open recomputes the file identity root,
and full/selective reads verify block/page commitments without forcing a whole
block read for point lookup. Metadata/page/block cache keys are scoped by the
same persisted file identity.
In-memory, file, cursor, ordinal, selective and streaming paths use one footer
decoder. `SstFileMeta` persists the same ownership fields through flush,
streaming compaction, Manifest round-trip and metadata-only reopen validation.
The current Blob layout uses numeric block format 1 with `CBB1` magic, an
88-byte checksummed header, a hash-bearing fixed directory, 64-bit block and
record boundaries, approximately 1 MiB ordinary blocks, and dedicated
oversized blocks. Transaction commits call `PutBatch`, so multiple small Blob
records in one durable batch share one block and one segment fsync; INDEX
deltas map hashes to block starts. Reopen validates every distinct referenced
block once, and ordinary Put plus GC relocation share one block writer. Direct
write estimates match actual segment, INDEX and ACTIVE bytes.
The latest dirty-working-tree normal correctness matrix passes 928/928. The focused Blob gate at
its archived sanitizer checkpoint passes 49/49 under
ASAN, UBSAN and TSAN; the earlier focused SST/Manifest/page-format gate passes
74/74 under each sanitizer. Schema registration now belongs to the clean-break
`MSC1` VersionSet Manifest: `VersionSnapshot.schemas` is canonical and
cross-validates every live SST, index definition and index-fragment source
partition. Registration uses generation CAS and exact Manifest rewrite
admission without consuming the prepared-transaction completion reserve.
Post-rename uncertainty from schema, index or SST publication gates every
mutation until reopen. Recovery rejects a Manifest larger than 256 MiB before
reading it and preflights declared entry counts before container reservation;
encoding obeys the same byte limit.
The standalone `SCH1`/`manifest/SCHEMA` runtime and the previous `MV10` reader
are absent. The schema/Manifest focused gate passes 49/49 under ASAN, UBSAN and
TSAN. A fresh `CEDAR_ENABLE_ZSTD=OFF` build passes 4/4 focused tests covering
registration rejection, checksummed `MSC1` reopen rejection, codec behavior,
and schema admission. A release
artifact must additionally enumerate zero whole-SST reads, zero Blob payload
reads in reference copy, cache/compaction bounds, and format/legacy removal.
The twelve artifacts under `results/columnar-closure-20260722/` and its
`-r2`, `-r3`, and `-r4` successors are explicitly rejected as release evidence:
each persists `database_format_version: 2` and a `cedar-v2-*` workload identity.
The current strict reader deterministically returns `NotSupported` for them;
no compatibility reader or artifact migration is permitted. Columnar release
evidence must be regenerated with database format 1 and current workload names.

### HTAP Correctness Kernel

Authoritative completion definition: `docs/superpowers/specs/2026-07-17-cedar-htap-design.md`, section 18.

Current evidence is summarized in
`docs/superpowers/plans/2026-07-22-cedar-htap-release-evidence.md`, including
strict transactions, concurrent participant install, visible-prefix recovery,
MemTable/version pinning, compaction, crash/reopen, and four sanitizer matrices
for the recorded checkpoint.

Current schema-3 evidence records prepare, decision, DecisionLog-fsync and
visible-prefix measurement distributions without substituting zero values for
unavailable measurements. Deterministic conflict-abort and visible-prefix stall
paths are also bound to the focused log. The remaining measurement item is a
production-scale workload with nonzero conflict, stall and write-amplification
distributions, not absence of the measurement producers.

### HTAP Resource Scheduling

Authoritative completion definition: `docs/superpowers/specs/2026-07-17-cedar-htap-resource-scheduling-design.md`, section 24.

Current implementation is distributed across
`include/cedar/runtime/`, `src/runtime/`, `src/cache/`, database wiring in
`src/db/cedar_database.cc`, and focused scheduler/pressure/cache/I/O tests.

The current call-site audit proves that `MaintenanceExecutor` routes compaction,
index build, statistics merge, Blob GC, checkpoint, flush, and Blob rotation
through shared typed admission, and T-Cypher morsels use
`WorkExecutionService`. Transaction PREPARE and participant installation now
submit one `WorkClass::kCommitCritical` task per shard through the shared
service, retain the transaction-owned completion grant, and use nested-progress
waiting rather than direct production threads. Direct coordinators create a
bounded owned service; database coordinators bind the shared service through a
status-returning pre-open-only API that rejects null, replacement, successful-
open rebinding, and failed-open owned-service replacement. Public
`CedarDatabase::Get()` now submits one `WorkClass::kPointRead` task with a one-
slot CPU grant, waits through the nested-progress path, and preserves cache,
I/O, status, metric, and trace semantics. Internal `GetChecked()` remains an
unscheduled primitive for batched runtime use. Critical and noncritical
ResourceGovernor usage are tracked separately so an occupied critical reserve
is not subtracted twice from the shared pool. `TransactionCoordinator::Open()`
now submits the complete durable recovery/rebuild body as one synchronous
`WorkClass::kRecovery` task with a one-slot CPU request. Successful and failed
open paths preserve their exact typed status, and one-worker recovery can make
nested progress while rebuilding index/statistics queues. Focused regressions
prove the recovery body runs away from the `Open()` caller and that both a
successful open and a clean-break `NotSupported` rejection are submitted,
admitted, and completed as recovery work. `CedarDatabase::Close()` now quiesces
public admission, drains already-entered commit/read/query/maintenance leases,
cancels or explicitly drains registered lazy streams, reclaims idle query
grants, and submits the real flush/checkpoint body as one
`WorkClass::kShutdown` task. Worker-pool stop and telemetry teardown remain
outside that callback. The destructor selects cancel-close. Concurrent close
callers share one result, worker-originated blocking close is rejected before
lifecycle mutation, and each database object permits only one recovery attempt.
Database-created T-Cypher sessions share lifecycle ownership, retained sessions
cannot dereference a destroyed coordinator, and an already-entered session
commit drains before checkpoint. Public visible-sequence, cache/storage,
benchmark-storage and session-creation APIs now return typed lifecycle
rejection after quiescing; metrics and traces remain snapshot-readable.

Normal compaction, index build, statistics merge and Blob GC are optional and
cancelled by close whether queued or already running, while urgent compaction
and correctness-critical work remain non-cancellable. `WorkExecutionService`
keeps a bounded running-task registry, signals each task-scoped token exactly
once, and closes later preemptible admission for the class while shutdown
drains. Production algorithms check only at declared safe boundaries;
unpublished SST/index output is removed, partial Blob relocation remains safe,
and Manifest publication is non-revocable once it begins. Completion items 1,
8, and 12 are now functionally closed. Index/statistics queue reconstruction is covered by
`DurableLogTest.ReopenReconstructsIndexAndStatsMaintenanceQueue`; recovery typed
admission itself is covered by
`DurableLogTest.OpenRunsRecoveryThroughTypedScheduler` and
`DurableLogTest.FailedOpenStillCompletesTypedRecoveryTask`. Commit
PREPARE/install, public point-read admission, recovery admission, and shutdown
ordering now have deterministic coverage for cross-shard overlap, thread
identity, partial failure, reopen, completion-grant, binding lifetime, typed
counters, nested-worker progress, one-worker shutdown, query/session lifetime,
and urgent-versus-optional maintenance ordering. The archived concentrated
resource/shutdown/maintenance selection passes 48/48. The latest dirty working
tree has one full normal, ASAN, UBSAN and TSAN execution each; all four pass
928/928 with `-j1`, with no sanitizer or race report. These latest logs are
local verification only. The preceding self-contained 916/916 checkpoint
remains archived under `results/release-closure-20260725-final-matrix-r10/`;
its directory verifier passes all manifest, binary and log SHA-256 bindings.

Index build and statistics merge now admit one immutable source SST per task.
Columnar, index, statistics and Manifest owners provide checked estimates;
grants include nonzero peak memory, source/sidecar reads, temporary bytes,
artifact/checkpoint/Manifest writes, descriptors, CPU and metadata operations.
Resource/I/O rejection precedes source reads and filesystem mutation, pressure
refusal returns typed `MaintenanceBackoff`, and each grant is released before
the next SST. Stats publication uses expected generation plus projected-map
write/fsync/rename/parent-directory-fsync before in-memory swap. Blob-backed
distinct statistics use `BlobRef` content identity without payload reads.
Focused admission/fault coverage passes 18/18. The current CI scheduler artifact
`results/goal-current-scheduler-saturation/6ffee3ad18e4836d88c17b48b5ef045eab81c517ece2ae459e265da95f5e8218`
passes strict reading and records all 13 queue/service/CPU-grant labels plus one
interactive deadline miss. It verifies exact 4:2:2:1 fairness and EDF inside
the workload, but is not production-scale evidence. Production-scale
concurrent HTAP distributions remain open.

### T-Cypher Vectorized Execution

Authoritative completion definition: `docs/superpowers/specs/2026-07-17-cedar-tcypher-vectorized-execution-design.md`, section 26.

Current evidence covers the typed execution path, QuerySnapshot, temporal
point/range/change semantics, fixed and variable Expand, joins, aggregate/
DISTINCT/sort/COLLECT, spill, cancellation, cross-join, DML, restart and the
independent oracle. Main test evidence is in the temporal/Expand/range/change,
physical-plan, ExplainAnalyze, spill and result-stream sections of
`tests/test_correctness_kernel.cc`.

The physical point/range/change EXPLAIN ANALYZE dispatcher is complete for the
current physical candidates. Mixed fixed/variable point and valid-time range
paths use ordered typed Expand steps, global `TRAIL`, typed relationship/path
projection, bounded spill, cancellation, per-hop attribution, and independent
oracle coverage. The authoritative support matrix is
`docs/superpowers/plans/2026-07-23-cedar-tcypher-support-matrix.md`.
Two shapes remain approved release exclusions with exact negative regressions:
mixed fixed/variable `CHANGES`, whose event/path semantics are not approved,
and property access on a variable relationship path, whose binding is a list
rather than a scalar relationship. The remaining T-Cypher closure is artifact
evidence, not a generic logical-runtime fallback.

Typed spill-failure campaigns now exercise the actual `QuerySpillFile` write
boundary and checksum reader. CI artifacts
`results/goal-current-spill-fault/0fcd9d1b01d3d4a60e0bb8b9c7bdc2bf2e1422e9f7c845061060679f6ae73e54`
and
`results/goal-current-spill-fault/52cf4501a25d9d0b6101d3b5141e60c916ff268f57d325a0da7a37d31f308011`
respectively inject ENOSPC and corrupt a valid spill record. Both pass strict
reading, cleanup, reopen and persisted-value verification. Production-scale
concurrent snapshots and disk-pressure campaigns remain open.

### Temporal Index and CBO

Authoritative completion definition: `docs/superpowers/specs/2026-07-17-cedar-temporal-index-cbo-design.md`, section 26.

Current evidence covers Manifest-owned `IndexCatalog`, CSI3 encoding matrix,
snapshot-pinned sidecars, MemTable deltas, hybrid fallback, repair/drop,
statistics snapshots, runtime feedback and index lifecycle tests.

Large String/Binary equality and `IN` now use the content-address identity
`BLAKE3-256 + raw_length` in immutable sidecars, MemTable delta indexes,
candidate validation and predicate property gather. Ordered range and prefix
paths reject hash postings. Predicate-only root and fixed-Expand gathers emit a
matching typed literal or NULL without reading Blob payloads; a separately
demanded projection owns a distinct physical slot and materializes the actual
payload exactly once. This deliberately treats the content-address identity as
the Blob equality contract rather than performing a secondary payload read.
Exact range/prefix evaluation over Blob-backed strings retains payload
materialization, including a regression covering both predicate kinds. Legacy
graph projection helpers now resolve the visible event and materialize BlobRefs
through the transaction coordinator instead of exposing the placeholder value.
MemTable lower-only range cursors stop at the requested physical type's inline
partition rather than scanning Blob-hash or later-type groups. Blob equality/
`IN` probe bytes are reserved from the query memory account before hashes and
literal copies are allocated, retained by the probe lifetime, and exported as
`blob_predicate_probe_bytes_reserved`.

Graph-order functionality is now implemented: `GraphOrder` cost alternatives
cover index-first and adjacency-first starts, multi-root physical planning
uses the selected alternative during DP startup, and the selected order is
persisted in the physical-plan fingerprint/validation and EXPLAIN formatting.
Deterministic cost/planner regressions cover both choices and budget fallback.
Production point planning now chooses base, index, hybrid or two-index
intersection before the first execution, applies confident feedback to the
same pinned estimate, and records selected and actually executed paths. Runtime
adaptive intersection drops and advisory fallback update the executed path.
Root predicate identities are carried with the decision, so a base root choice
does not disable independent target/relationship dynamic filters and non-root
predicates cannot masquerade as a root access-path choice. Validated multi-root
runtime publishes the selected and executed graph order from the exact opened
plan. The 60/60 focused and 855/855 normal evidence is archived under
`results/release-closure-20260723-cbo-executed-choices/`.

This closes real access-path/graph-order execution, bounded EXPLAIN ANALYZE,
atomic drop/reopen, automatic health-event repair, concurrent feedback
generation isolation, and functional per-SST index/statistics resource
admission. Remaining gates are production-scale resource-accounting artifacts,
production-scale HTAP/fairness, and release/paper benchmark
evidence.

The public CI workload artifact
`results/goal-current-index-path-matrix/4fbeaaa84d1eea590c3a01fa58bf231f682b4372b2bc0e6727278393f3d742ae`
passes strict reading with 6/6 verified paths, nonzero indexed candidates,
interactive service/CPU grants, and successful load/result/reopen phases. It
is functional CI evidence only, not a production-scale cost distribution.

### Observability and Benchmark

Authoritative completion definition: `docs/superpowers/specs/2026-07-17-cedar-observability-benchmark-design.md`, section 24, with release and paper gates in section 22.

Current evidence includes versioned manifests, Cedar-TG and LDBC-derived input,
durability/recovery/fault workloads, physical-byte/cache/maintenance producers,
strict artifact validation, and offline report regeneration.

The paired regression gate now rejects fewer than five valid repetitions by
default and serializes `minimum_pair_count` into the gate policy, with
normal/ASAN/UBSAN/TSAN regression tests covering the constraint. The
`cedar_bench_pair` executable now launches two explicit binaries in alternating
order, rejects identical hashes, validates every child artifact through
`ReadBenchmarkArtifact`, and writes `regression-gate.json` plus
`paired-runs.json` with run-id and artifact provenance. A five-pair smoke run
using distinct current/alternate binaries is archived as
`results/release-closure-20260723-paired-smoke-r1/`. Its manifest and all three
bound top-level files, all three executable SHA-256 values, and all ten child
verification/report hashes have been independently rechecked. The internal
statistical gate reports PASS, while the enclosing manifest deliberately sets
`classification=smoke_only`, `approved_production_baseline=false`, and
`release_gate_eligible=false`; it proves the paired process driver, not a Cedar
release comparison. A sanitizer binary carrying a different workload/resource
key correctly produced `INCOMPATIBLE`. The
historical `results/release-closure-20260723-r2/` corpus predates the current
required profile/count manifest fields and is intentionally rejected by the
strict reader; no compatibility fallback was added. A replacement current-
schema corpus has been run five times per workload in
`results/release-closure-20260723-r3/` (50 artifacts, index at
`results/release-closure-20260723-r3/run-index.tsv`). The set covers bitemporal point read, analytical vertex
count, valid-time range, graph one-hop, blob projection, durable ingestion,
maintenance cycle, HTAP balanced, recovery, and index equality. Every run has
`database_format_version: 1`, `dataset_profile_id: ci`, actual dataset counts,
durable mode, `cold_process_and_database`, source/binary/dataset provenance, a
PASS verification record, and an offline report regeneration pass (`50/50`).
The workload families include open-loop, closed-loop, and mixed drivers. Five
additional current-schema artifacts under
`results/release-closure-20260723-cache-modes/` cover every named cache mode;
all five pass strict reading, result/reopen verification, and offline report
regeneration.
The offline regeneration gate is now durably archived as artifact
`release-closure-20260723-report-regeneration`: the current strict
`cedar_bench_report` binary regenerated all 50 r3, 5 cache-mode, and 8 fault-r5
reports twice. All 63 inputs retained PASS verification and both regenerated
report SHA-256 values matched for every run; the per-run transcript is
`results/release-closure-20260723-report-regeneration/run-index.tsv`.

This closes the paired-driver implementation and fixed-corpus/repetition/report-
reproducibility evidence items, but not the release comparison itself: the
archived 50-run batch intentionally uses one candidate binary and has no
approved distinct baseline binary or release confidence/regression artifact.
Authoritative fault/recovery evidence is
`results/release-closure-20260723-fault-r5/run-index.tsv`. Its eight current-
schema artifacts are the sole accepted fault corpus: `commit_after_prepare_durable`
(seed 261201, `a6e4e2a5082f79a9ac5aa504579114f31d038852680894ea805c082035bd3d40`),
`commit_after_decision_durable` (261202,
`6b773fb72b15eacd36d9875cc74914ed45564f01b1e37d1ec72aac513c290dd5`),
`manifest_after_rename` (261203,
`5616c635b8edaf83dd317102e01667d1db3f4b88285e36bef90e4a3ea9efe6c9`),
`blob_index_partial_write` (261204,
`8bc5a04d2626e7af771f90213b9a041ab975f88f72e0e36e9bfc9ea26b8cab80`),
`sst_after_rename` (261205,
`6dc579942d98578b95269869a9d31b83666e3779dc3c998ff7790b6aff0bf28e`),
`sidecar_after_rename` (261206,
`853c3d55d12cf73c5525d52c647a93b0c158b2060917788e59cac4b6e848cd58`),
`blob_gc_after_manifest_rename` (261207,
`0df6ed108d459a9ec9021e67354c0261ee5ddaab196949ef39e787bdaaaf8e33`),
and `accepted_work_shutdown` (261208,
`618b328de6d08cf7251298a3ac3232209660ce845111b7e6a156ac5b6d9ebba7`).
Every artifact records PASS, a complete protocol, successful load/result/reopen
verification, scenario-specific durable-reopen/value verification, and an
offline-regenerated report.

`results/release-closure-20260723-fault-r4/` is explicitly rejected and must
not be cited as release evidence: its `accepted_work_shutdown` artifact
(`c8cc7cc7eba2ee769237ebdea87cb667dfa0b0ce61dab7d6b9072a923751e464`)
is INVALID because the injected fault did not stop at the selected durability
boundary. Earlier r3/r4 fault corpora are superseded by r5 for this gate.

The latest dirty-working-tree correctness baseline passes 928/928 in normal,
ASAN, UBSAN and TSAN modes with `-j1`. No sanitizer or race diagnostic is
present. This current matrix is not release evidence because the source is
dirty and the logs are not sealed into a release root. The preceding stable
logs, binary/log SHA-256 bindings, source commit, database format `1`, host
identity, exact commands and elapsed times remain archived with all four
corresponding binaries under
`results/release-closure-20260725-final-matrix-r10/` at 916/916 plus the
independent provenance regression 1/1.
Workstation and stress resource profiles plus an approved production
baseline/candidate comparison remain evidence-missing. Paper and externally
derived LDBC artifacts are outside the active functional release goal.

The current repository history does not contain an approvable prior production
binary for the clean-break architecture. `main` and the current branch point at
the design-only checkpoint, while `origin/main` contains the removed legacy
runtime and cannot be restored as a baseline. `cedar_bench_alt` remains a
smoke-only distinct binary and is not release evidence. A release comparison
therefore requires an explicitly approved, independently preserved production
baseline; it must not be synthesized from the legacy runtime or the ablation
target. This workstation reports 16 GiB physical memory and 22 GiB available
filesystem capacity. No workstation/paper/stress run is claimed from that
environment: the declared paper and stress profiles contain respectively
10M/40M and 25M/100M vertices/edges and require a separately provisioned run
host whose environment and resource limits are archived in each artifact.

## Requirement-Level Completion Audit

Status vocabulary:

- `COMPLETE`: current production code and focused tests implement the item;
- `PARTIAL`: only a subset of the required behavior or evidence is present;
- `MISSING`: current production code or the artifact inventory directly lacks
  the required behavior;
- `APPROVED-OOS`: an explicit approved exclusion with a deterministic tested
  failure contract. No mandatory completion item currently has this status.

An implementation status does not close its release gate. The final column
identifies the artifact boundary still required.

### Columnar completion definition

| # | Implementation | Precise code and test evidence | Release/artifact status |
|---|---|---|---|
| 1 | COMPLETE | `BuildSst`, `BlobStore::PutBatch`, `FlushFrozenShard`; `FlushesCommittedShardEventsIntoPartitionedSstFiles`, `CoordinatorExternalizesLargeBinaryBeforePrepareAndRestoresIt` | PROVEN for static ownership by `release-closure-20260725-source-contract-r1`: 11 database durable-mutation owners are separated from benchmark/result and temporary-spill writers by an exact fail-closed inventory; runtime workload evidence remains in r3/r10 |
| 2 | COMPLETE | `ReadSstFileMetadata`, `OpenSstEventCursor`, `ReadSstEventsAtOrdinals`; `SstMetadataOpenValidatesOwnershipWithoutDecodingDataBlocks`, `PointReadFetchesOnlyTheSelectedValuePageFragment` | PROVEN at focused scale by the self-contained r10 selective-read root; production-scale residency distributions remain row 7 |
| 3 | COMPLETE | `TemporalReadMerger`, `GetChecked`; `RandomMultiSstSchemaEpochHistoryMatchesOracleAcrossCompactionAndReopen`, `RandomMultiSstEdgePathProvenanceMatchesOracleAcrossCompactionAndReopen` | PROVEN at focused scale by r10, including full fixed-seed key/provenance oracle assertions across compaction and reopen |
| 4 | COMPLETE | `DecodePage`, `ReadSstFileInternal`, `BlobStore::Get`; `RejectsCorruptPayloadSizesAndDirectoryOffsets`, `BlobPayloadDetectsCrcAndBlake3Corruption` | PROVEN at focused scale by the current-format r10 page/granule/SST/Blob corruption corpus; exhaustive injected persistence boundaries remain row 8 |
| 5 | COMPLETE | `CompactSstPartition`; `DatabaseExportsRealColumnarBlobCompactionAndGcMetrics`, `ExposesExactSortedBlobReferencesWithoutReadingPayloads` | PROVEN at focused scale by r10 reference-copy compaction and zero Blob-payload-read assertions |
| 6 | COMPLETE | `RelocateLiveHashes`, `BlobGarbageCollector::Collect`, `BlobReferenceCatalog`; `BlobGcConcurrentWriterRespectsLongSnapshotPinAndStaleReferences` | PARTIAL: r5 Blob-GC publication fault does not replace concurrent relocation/stale-SST-reference evidence |
| 7 | PARTIAL | `CacheManager`, streaming compaction; `CompactionStreamsSortedSstBlocksWithBoundedEventBuffering`, `BypassesFirstScanAndProtectsPinnedSnapshotHandles`; the real `maintenance-cycle` campaign archives nonzero compaction input/output, compaction peak and cache peak at CI scale | PARTIAL: production-scale configured-limit and observed-peak distributions remain missing |
| 8 | PARTIAL | VersionSet/Blob/SST recovery and concurrency tests; r5 eight-scenario corpus; sanitizer artifact `release-closure-20260723-sanitizers` | PARTIAL: not every file-deletion/index/Blob/SST persistence boundary has artifact coverage |
| 9 | COMPLETE | old Descriptor/v1/duplicate ownership sources are removed; old-format rejection tests include `DatabaseRejectsOldFormatMagicWithoutMutatingDirectory` and `SstTest.RejectsOldHeaderMagic` | PROVEN: artifact `release-closure-20260723-clean-break-scan` records zero forbidden external names, parallel runtimes and old-source references, with every retained term justified |
| 10 | PARTIAL | strict transaction/snapshot tests such as `StrictTransactionsRejectDeterministicWriteSkew` and `DatabaseSessionReadsUseTheSnapshotCapturedAtBegin` | PARTIAL: depends on the still-open HTAP/resource completion rows |

Columnar verification strategy §30 remains `PARTIAL`: golden/round-trip,
continuation, corruption, multi-SST/schema, Blob, compaction and oracle tests
exist. The current format-1 focused root covers selective I/O, reference-copy
Blob avoidance, concurrent relocation, corruption, codec startup and the real
CI maintenance peak campaign. Production-scale cache/compaction peak
distributions and every persistence boundary remain missing. The
static clean-break gate and the exact durable-writer/source-contract gate are
archived. All twelve artifacts below
`results/columnar-closure-20260722*` are rejected old format-2/V2 evidence.

`results/release-closure-20260725-columnar-functional-r10` retains a current-source
74/74 focused log for selective and ordinal-block reads,
metadata-only open, zero-payload reference-copy compaction assertions, bounded
streaming, fixed-seed oracles, concurrent Blob publication/relocation,
corruption, codec startup capability checks, the real two-SST maintenance-cycle
peak campaign, the fail-closed activity gate and clean-break rejection. Its
binary, focused log, manifest, and ledger are self-contained and pass the
current directory-level `cedar_evidence_verify`. Production-scale
cache/compaction peak distributions, broader persistence boundaries, and a
release-scale artifact remain absent.

### HTAP correctness-kernel completion definition

| # | Implementation | Precise code and test evidence | Release/artifact status |
|---|---|---|---|
| 1 | COMPLETE | `CedarDatabase::Put/Delete`, `TransactionSink::Submit`, `CommitInternal`; `TypedFacadeUsesOnlyNewTransactionAndSchemaContracts`, `TransactionSinkConvertsTypedMutationsAndCommitsAtomically` | PARTIAL: `release-closure-20260725-source-contract-r1` archives the exact filesystem-mutation owner inventory, but a public-entry-to-writer call-path map remains missing |
| 2 | PARTIAL | model, recovery, compaction, tombstone and concurrency tests including `CommitFaultMatrixReopensAndPreservesOnlyDurableOutcomes` and `RandomStrictDependencyCyclesNeverCommitEveryParticipant` | PARTIAL: r5 and sanitizer artifacts exist, but every-boundary crash and serializability-stress artifacts do not |
| 3 | COMPLETE | Manifest publication through `VersionSet::ApplyEdit`; `CoordinatorPublishesSchemaOnlyThroughManifest`, `PublicationFaultMatrixCleansUnmanifestedSstAndSidecarOutputs` | PROVEN for static ownership by `release-closure-20260725-source-contract-r1`: five `ApplyEdit` owners and all direct-delete owners are exact fail-closed inventories; the six persistent-delete owners are the reviewed semantic subset |
| 4 | COMPLETE | database-format, Manifest, SST and Blob decoders; `CoordinatorRejectsUnknownPersistentFormatVersion`, `VersionSetRejectsUnframedManifestAsCorruption` | PARTIAL: no diagnostic-code/message stability corpus |
| 5 | COMPLETE | public autocommit facade and T-Cypher DML; `ExecuteTcypherCreateCommitsExistenceAndPropertyAtomically` | PARTIAL: r3 covers finite workloads rather than the full public contract matrix |
| 6 | COMPLETE | strict reservations in `CommitStrict`/`StorageShard`; write-skew, half-open interval, prepared-read and dependency-cycle tests | PROVEN at deterministic focused scale by r10; production serializability stress remains missing |
| 7 | COMPLETE | visible prefix and pinned immutable snapshots; `PinsImmutableMemtableGenerationAcrossWritesAndFlush`, `RetainedScheduledQueryStreamIsCancelledAfterDatabaseDestruction` | PROVEN for functional pinned/frozen/session behavior by r10; production concurrent snapshot-vs-ingestion distributions remain missing |

HTAP verification strategy §16 is `PARTIAL`. Model, transaction, crash,
compaction, deterministic serializability, transaction measurement and final
sanitizer sources are archived. Production-scale durability-boundary coverage,
multi-shard scaling, production-scale conflict/stall distributions and write
amplification remain release evidence gaps.

`results/release-closure-20260725-htap-functional-r10` is a self-contained
format-1 focused root with the current test binary and a `-j1` 36/36 corpus
for serializability/reservations, visible-prefix ordering, immutable MemTable
pinning, commit faults, the unified fault campaign, flush/compaction, reopen and
independent oracles. Its
ledger and manifest bindings pass the directory-level verifier. It remains
non-release evidence because production-scale conflict/serializability,
visible-prefix stall, all-boundary crash and multi-shard stress artifacts are
absent. A real CI HTAP artifact at
`results/goal-current-smoke/3898450af52a42e4bcc831bce8aab92b8b5ea0be9d4075a408311c01dcf17f27`
records 102 started transactions, 101 commits, one strict conflict abort, 101
nonzero visible-prefix waits and nonzero analytical/commit/flush/compaction/
point-read/statistics CPU grants. The self-contained archived checkpoint is the
normal/ASAN/UBSAN/TSAN 916/916 matrix under
`results/release-closure-20260725-final-matrix-r10/`; the latest dirty working
tree additionally passes 928/928 in all four modes but is not a release root.

### HTAP resource-scheduling completion definition

| # | Implementation | Precise code and test evidence | Release/artifact status |
|---|---|---|---|
| 1 | COMPLETE | maintenance/query morsels, commit PREPARE/install, public reads/snapshots, recovery, and the real close body use typed shared admission; `scheduler-saturation` covers every work class | PROVEN at CI scale; production-scale admission artifact remains open |
| 2 | COMPLETE | commit completion grant and critical `IoGovernor` reservation; `CommitAdmissionUsesComponentOwnedFramedLogBytes`, `BlobCommitAdmissionBudgetsProtectedBlobWrites` | PROVEN for the two commit durability boundaries by r5 prepare/decision artifacts |
| 3 | PARTIAL | `Flush`, `CompactWithClass`, index/stats scheduling, Blob GC and `QueryRuntimeState::ScheduleAndWait`; queued/running registry plus cooperative cancellation safe points; all 13 queue/service/grant labels are active in the CI saturation artifact | MISSING: production-scale contention artifact |
| 4 | PARTIAL | `ResourceGovernor` separately tracks total and noncritical-pool usage, `IoGovernor`, `QueryMemoryAccount`, spill files; atomic reserve/release and critical/shared-pool tests | MISSING: hierarchical-budget/emergency-reserve property artifact |
| 5 | PARTIAL | `PressureController::Update`, `AdmitQuery`, pressure actions; pressure transition/write-stall tests | MISSING: end-to-end hysteresis, deadline and stall-cause artifact |
| 6 | PARTIAL | commit obtains completion resources before PREPARE; commit admission/fault tests | MISSING: starvation/property proof across every prepared completion branch |
| 7 | PARTIAL | `CacheManager`; `BypassesFirstScanAndProtectsPinnedSnapshotHandles` | MISSING: hot-point versus sequential-scan release comparison |
| 8 | COMPLETE | scheme C quiesces admission, safely cancels/drains lazy streams, drains accepted session/commit/read work, cancels queued/running optional maintenance with task-scoped tokens, preserves urgent/correctness-critical work, checkpoints, and tears down idempotently; exact safe-boundary/publication-fence tests plus real Blob-GC close/reopen verification | PROVEN by `release-closure-20260723-maintenance-cancellation`; production-scale stress remains row 11 |
| 9 | COMPLETE | WAL/decision/Manifest/Blob/SST recovery plus `ReopenReconstructsIndexAndStatsMaintenanceQueue`, typed recovery accounting, shutdown-checkpoint fault recovery, and partial Blob relocation reopen verification | PROVEN for the current implementation by the focused shutdown/reopen log and current 916-test matrices; broader crash-boundary corpus remains an HTAP correctness release gate |
| 10 | PARTIAL | scheduler/resource/pressure metrics and `DatabaseExportsSchedulerResourceAndPressureMetrics` | MISSING: complete tail/stall/cache/read-amplification cause artifact |
| 11 | PARTIAL | component scheduler/pressure/cache tests plus exact 4:2:2:1 fairness, EDF and deadline-miss CI artifact | MISSING: production-scale HTAP stress corpus |
| 12 | COMPLETE | transaction/storage/Blob/index/statistics/database production sources have no direct `std::thread`, `std::jthread` or `std::async`; PREPARE/install, public point reads, recovery and shutdown use the shared service | PROVEN at current-source focused scale by `release-closure-20260725-production-thread-gate-r1`; production scheduler distributions remain row 13 |
| 13 | PARTIAL | concentrated resource/shutdown/maintenance, current 57/57 focused evidence, latest dirty-working-tree normal/ASAN/UBSAN/TSAN 928/928 matrix and strict all-class saturation artifact | PARTIAL: production-scale distributions remain open |

Resource verification strategy §20 is `PARTIAL`, with structural acceptance
§20.6 covered at focused scale. Virtual-clock weighted fairness/aging/deadlines, allocation and
disk/temp-space failures, all-class concurrent HTAP workload, scan-resistance,
commit latency under analytical saturation, no-bypass proof, and production-
scale fairness/resource evidence have not been produced.

`results/release-closure-20260725-resource-functional-r10` retains a current-source
`-j1` 57/57 focused corpus for production entry points, scheduler ownership,
per-SST nonzero admission, rejection-before-I/O, release, cancellation,
recovery, shutdown, exact saturated fairness, EDF, a real worker-saturated
deadline miss and all 13 queue/service/grant labels. Its binary, focused log,
manifest, and ledger are self-contained and pass the current directory-level
verifier. Production-scale concurrent HTAP distributions remain open.

### T-Cypher completion definition

| # | Implementation | Precise code and test evidence | Release/artifact status |
|---|---|---|---|
| 1 | COMPLETE | `CedarDatabase::ExecuteTcypher`/`cedar::ExecuteTcypher`; `ExecuteTcypherReturnsCommittedVertexBindingsThroughTheNewEntryPoint` | PROVEN by the zero-hit forbidden-runtime scan in `release-closure-20260723-clean-break-scan` |
| 2 | COMPLETE | new tokenizer/parser/binder/physical plan/runtime/result contracts and their focused tests | PARTIAL: no V1 clause/scope golden artifact |
| 3 | COMPLETE | production exchanges `ResultBatch`/vectors, not old Record/per-row operators | PROVEN by focused contracts plus the archived forbidden-runtime scan |
| 4 | PARTIAL | valid-time point/range/change implementation and independent oracle tests | MISSING: full supported-shape oracle artifact |
| 5 | COMPLETE | durable `CommitTimeline`; clock-tie/checkpoint/system-change tests | PARTIAL: no dedicated system-change release artifact |
| 6 | COMPLETE | `BuildPhysicalQuerySnapshot`/visible-prefix cap and snapshot-pin tests | PARTIAL: no concurrent release artifact |
| 7 | PARTIAL | demand analysis/property gather/selective reads; focused Page/Blob tests | MISSING: complete demand-driven Page/Blob artifact |
| 8 | COMPLETE | `VectorExpand` and interval intersection; endpoint visibility/range tests | PARTIAL: r3 graph run covers only one-hop behavior |
| 9 | PARTIAL | variable path frontier, TRAIL, spill and cancellation tests | MISSING: resource-limit/disk-failure release artifact |
| 10 | COMPLETE | `TransactionSink::Submit`; atomic CREATE/SET/DELETE tests | PARTIAL: no full temporal DML corpus artifact |
| 11 | PARTIAL | memory/spill/backpressure/cancellation tests plus typed ENOSPC and checksum-corruption campaigns through the real `QuerySpillFile` boundary | PROVEN at CI fault-campaign scale for cleanup/reopen/durable-value verification; MISSING: production disk pressure and all-stage cancellation corpus |
| 12 | COMPLETE | `TcypherCorpusMatchesAcrossMemtableFrozenSstCompactionBlobRelocationAndReopen` runs one Blob-bearing corpus through active MemTable, a query snapshot pinned across the frozen handoff, one/many live VersionSet SSTs, repeated flushes, explicit compaction with increased input/output counters and reduced live-file count, reopen, real nonzero Blob relocation and reopen-after-relocation | PROVEN at focused scale by the self-contained r10 lifecycle root; production concurrency and disk pressure remain rows 6 and 11 |
| 13 | PARTIAL | EXPLAIN ANALYZE serializers/counters and focused tests | MISSING: supported point/range/change/Expand/join/spill counter artifact matrix |
| 14 | COMPLETE | old Cypher API/Record/fallback sources removed | PROVEN by `release-closure-20260723-clean-break-scan`; retained fallback terms are approved base-scan/value fallbacks, not an old executor |
| 15 | PARTIAL | cross-design tests and sanitizer matrices | PARTIAL: resource/index/observability rows remain open |

Approved shape exclusions are not completion-item exclusions: mixed
fixed/variable `CHANGES` and scalar property access on a variable relationship
path return deterministic `NotSupported` and are covered respectively by
`MixedChangePathHasAnExactUnsupportedContract` and
`VariableRelationshipPropertyHasAnExactUnsupportedContract`.

T-Cypher verification strategy §23 is `PARTIAL`; §23.9 production end-to-end
corpus is `MISSING`. The current focused root covers supported point/range/
change, historical DML, temporal aggregates, bounded mixed paths, joins/spill,
cancellation and EXPLAIN ANALYZE, but no single production-scale corpus carries
those shapes across concurrent snapshots and sustained disk pressure.

`results/release-closure-20260725-tcypher-functional-r10` retains a current-source `-j1`
416/416 support log for point/range/change, fixed/variable/mixed Expand, joins,
spill, TRAIL, EXPLAIN ANALYZE and the unified typed spill-fault campaign, with
the two approved `NotSupported`
exclusions recorded explicitly. Its binary, focused log, manifest, and ledger
are self-contained and pass the current directory-level verifier.
Production-scale concurrent-snapshot and disk-failure corpus remain open.

### Temporal index/CBO completion definition

| # | Implementation | Precise code and test evidence | Release/artifact status |
|---|---|---|---|
| 1 | COMPLETE | Manifest-owned `IndexCatalog`; `IndexCatalogValidatesSchemaAndPublishesLifecycleEdits` | PROVEN at focused scale by the self-contained r10 catalog/lifecycle root |
| 2 | COMPLETE | `BuildIndexSidecar`, verified reader, snapshot pins/source identity; sidecar round-trip/source mismatch tests | PARTIAL: r5 sidecar rename covers only publication fault |
| 3 | COMPLETE | optional candidates plus base validation; incomplete-coverage fallback tests; deterministic randomized `RandomizedIndexCandidatesMatchFullEventScanAcrossBitemporalQueries` compares indexed/hybrid execution with an independent full-event scan across out-of-order/equal valid times, PUT/DELETE/restore, two immutable sources, a MemTable delta source, point/range/valid-change/system-change/combined scopes, and different batch capacities | MISSING: archive the randomized completeness run as a durable release artifact |
| 4 | COMPLETE | postings store PUT candidate facts, not authoritative `valid_to`; successor-delete regression | PARTIAL: archive static format proof |
| 5 | COMPLETE | `IndexCanonicalKind::kBlobHash`, CSI3 sidecars, Blob-aware `MemtableDeltaIndex`, source validation, pre-admitted predicate hash probes and legacy graph Blob materialization; `BlobEqualityUsesHashWithoutRangeOrPrefixExposure`, `BlobEqualityUsesHashAlongsideInlineValue`, bounded-work `RangeCursorNeverReturnsBlobHashPostings`, `TcypherIndexedBlobEqualityAvoidsPredicatePayloadReads`, `TcypherBlobEqualityAvoidsPredicatePayloadReadsWithoutIndex`, `TcypherBlobStringRangePredicatesMaterializePayload`, fixed-Expand predicate/projection plus legacy fallback coverage, and `AdvisoryLargeInDeltaCursorSeeksAcrossKeyQuanta` | MISSING: archive a current release artifact proving nonzero sidecar and MemTable hash candidates, zero predicate payload reads, one demanded projection read, range/prefix exclusion/materialization, bounded range work and probe-memory admission |
| 6 | COMPLETE | hybrid/fallback and corrupt-sidecar repair tests | PROVEN at focused scale by r10, including real corrupt-sidecar repair and nonzero index candidates |
| 7 | COMPLETE | atomic `DropIndex()` removes the definition and all fragments in one generation-CAS Manifest edit; the durable `next_index_id` high-water mark prevents replacement-ID/sidecar collision; indeterminate publication gates mutations until reopen; pinned pre-drop snapshots defer old-sidecar reclamation; `IndexLifecycleCompactsRepairsDropsAndReopensWithoutIdReuse` combines compaction with verified input/output counter growth and live-file reduction, real sidecar corruption/repair, drop, reopen and replacement-ID monotonicity | PROVEN at focused scale by the self-contained r10 lifecycle artifact; production-scale lifecycle stress remains row 14 |
| 8 | COMPLETE | `StatsSnapshotStore`; pinned live-fragment and restart/corruption tests | MISSING: stats generation/rebuild artifact |
| 9 | COMPLETE at CI scale | access-path and graph-order cost decisions plus budget tests; `index-path-matrix` fail-closes unless base, index, hybrid, intersection, index-first and adjacency-first all execute through the public API | PROVEN by the strict CI artifact and current focused root; MISSING: production-scale plan-choice distributions |
| 10 | COMPLETE at CI scale | `CostVector` and runtime counters include resource dimensions; the strict path matrix emits nonzero candidates and interactive scheduler service/CPU grants | PROVEN for CI path activity; MISSING: production interval-fragmentation, Blob-avoidance and resource-cost distributions |
| 11 | COMPLETE | `RuntimeFeedbackStore` implements two-observation confidence, 64-epoch decay, 256-epoch expiry and LRU separation; `RuntimeFeedbackKey` includes plan shape, schema epochs, Manifest/catalog generation and statistics snapshot ID; `PopulateTcypherContext` pins coherent VersionSet/catalog/statistics identities; `DurableLogTest.RuntimeFeedbackCompletionStaysInPinnedCatalogAndStatsGeneration` proves an old stream publishes only to its pinned generation while a concurrent drop/rebuild/flush query observes a distinct fresh key | PARTIAL: the functional concurrent generation-isolation regression is complete; a plan-choice feedback release artifact remains missing |
| 12 | COMPLETE | index build and statistics merge use typed, cancellable, one-source-SST-per-task scheduling with component-owned checked estimates for nonzero peak memory, source/sidecar reads, temporary/output bytes, artifact/checkpoint/Manifest writes, descriptors, CPU and metadata operations; admission rejection precedes source I/O or mutation, pressure is refreshed between SSTs with typed `MaintenanceBackoff`, grants are released before the next source, and stats publication uses expected-generation copy-project checkpointing | PARTIAL: focused regressions and the real indexed CI artifact prove nonzero CPU/memory/read/temp/write/descriptor/metadata dimensions; production-scale distributions remain missing |
| 13 | COMPLETE | old index sources and switches are deleted | PROVEN by the zero-hit old-source and external-name scans in `release-closure-20260723-clean-break-scan` |
| 14 | PARTIAL | focused cross-design tests and final sanitizer artifact | PARTIAL: preceding design requirements remain open |

Temporal-index verification strategy §23 is `PARTIAL`; randomized candidate
completeness and Blob-hash equality have focused implementation tests. The real
CI path artifact at
`results/goal-current-index-path-matrix/4fbeaaa84d1eea590c3a01fa58bf231f682b4372b2bc0e6727278393f3d742ae`
passes strict reading, executes all six required physical choices and records
nonzero `cedar_index_candidate_rows_total[all]`, interactive service and CPU
grants. Required production artifacts must still prove plan-choice and cost
distributions at release scale. The
atomic-drop focused selection passes 34/34 and the subsequent fresh normal
matrix is superseded for local verification by the current 928/928 matrix;
these dirty-working-tree results do not replace the still-missing production
release artifact. The latest self-contained archived matrix remains r10 at
916/916.

`results/release-closure-20260725-temporal-index-cbo-functional-r10` retains a
current-source `-j1` 57/57 focused log for real base/index/hybrid/intersection/
index-first/adjacency-first execution, random candidate completeness, nonzero
candidate/activity gates, Blob-hash bounds, automatic
repair/drop/reopen, feedback generation isolation and per-SST resource
accounting. Its binary, focused log, manifest, and ledger are self-contained
and pass the current directory-level verifier. Production access-path/resource
distributions remain open.

### Observability/benchmark completion definition

| # | Implementation | Precise code and test evidence | Release/artifact status |
|---|---|---|---|
| 1 | COMPLETE | `ProductionMetricDefinitions`, `MetricRegistry`, `TelemetryAggregator`, registered database/scheduler/storage/index metrics, bounded-label schema audit and database registration regression | PROVEN at focused scale; production workload activity remains row 3 |
| 2 | COMPLETE | `CEDAR_MINIMAL_INSTRUMENTATION` builds `tier0-minimal`; manifests persist `instrumentation_profile_id`; `CompareInstrumentationOverheadRuns`, `WriteInstrumentationOverheadGate`, and `cedar_bench_pair --instrumentation-overhead` require a `tier0-minimal` baseline and `tier0-tier1` candidate, at least five valid pairs, and apply direct 2% median throughput / 5% p99 thresholds; `results/release-closure-20260723-observability-final` binds ten frozen schema-3 strict-reader-validated paired runs | PARTIAL: the tiny smoke gate is `NOISY` and is not release evidence; archive an approved production-scale paired comparison |
| 3 | COMPLETE structure, PARTIAL production execution | runtime profile contains row/interval/page/Blob/index/memory/spill/scheduler dimensions; fail-closed activity validation covers the corrected HTAP, maintenance, scheduler-saturation and index-path CI campaigns | PROVEN across real CI campaigns; PARTIAL: production-scale target workloads and distributions remain missing |
| 4 | COMPLETE | `BenchmarkRunManifest`, writer/reader strict provenance tests | PROVEN for r3/r5 current-format artifacts |
| 5 | COMPLETE | deterministic Cedar-TG and independent small oracle tests | PROVEN for r3 dataset hashes/checksums |
| 6 | COMPLETE at CI scale | public workload driver and r3 10-workload × 5 corpus | PROVEN only for CI scale, not workstation/release |
| 7 | COMPLETE | open-loop arrival scheduler and intended/admitted/start/completion timestamps | PROVEN for r3 open-loop summaries |
| 8 | COMPLETE | explicit `BenchmarkRatio` numerator/denominator serialization tests | PROVEN for r3 summaries |
| 9 | PARTIAL | paired comparator plus fail-closed production orchestrator, binary-provenance preflight, strict resume validation and incompatibility/minimum-pair tests; ten schema-3 strict-reader-validated frozen pairs | MISSING: approved production baseline/candidate `regression-gate.json` and `paired-runs.json` (current gate is `NOISY`) |
| 10 | PARTIAL | `cedar_production_campaign` freezes 65 paired workload/cache commands and ten typed fault/reopen commands, archives the three executables, and seals protocol/report files with SHA-256; r5 faults, scheduler/HTAP aggregate 80/80, benchmark aggregate 28/28 and frozen four-matrix evidence exist | MISSING: execute the runner on qualified workstation/stress hosts and archive the production-scale scheduler/HTAP stress gate before performance aggregation |
| 11 | PARTIAL | artifact IDs and provenance exist for current CI/fault numbers | MISSING: published-claim-to-artifact ledger and release comparison |
| 12 | PARTIAL | `release-closure-20260725-source-contract-r1` archives 16 retained `*Stats` views and zero direct `printf/fprintf`, ad-hoc latency/cache metric identifiers, or unbound numeric performance claims, with nine fail-closed negative fixtures plus stranded-output recovery coverage | PARTIAL: the static Stats/direct-diagnostic/performance-claim scan is proven; a separate duplicate-trace inventory remains missing |
| 13 | COMPLETE | offline report reader/regenerator and focused test | PROVEN: artifact `release-closure-20260723-report-regeneration` records 63/63 PASS and deterministic two-pass report hashes |
| 14 | MISSING | this matrix still contains functional and evidence gaps | MISSING until all preceding design rows close or receive explicit approved exclusions |

Observability verification strategy status: metric schema `COMPLETE`,
instrumentation overhead implementation `COMPLETE` but production evidence
`PARTIAL`, reproducibility `COMPLETE` for the accepted r3/cache/fault-r5
corpora, correctness/fault `PARTIAL`, and regression harness `PARTIAL`. The
tiny smoke's minimal binary does not replace an approved production baseline.

`results/release-closure-20260725-observability-functional-r10` is a self-contained
format-1 focused root with the current test binary and a `-j1` 58/58 corpus
covering bounded metric registration/export, telemetry, database
resource/storage metrics, the real maintenance/scheduler/index campaigns,
benchmark-artifact provenance, byte-bound executable snapshots, namespaced-cgroup
fail-close and paired-gate contracts. Its ledger and
manifest bindings pass the directory-level verifier.
It deliberately remains `release_gate_eligible: false`: the versioned census
and CI target-workload activity gate are closed, while an approved
production-scale paired baseline and production workload distributions remain
missing.

The production gate itself is now executable rather than documentary. Named
workstation/stress profiles use exact dataset and worker contracts; runtime
resource probes fail closed when process CPU/RAM limits or storage
device/filesystem provenance are incomplete; production artifacts require a
clean full source commit; and paired mode requires an externally approved,
distinct baseline SHA-256. The self-contained negative root
`results/release-closure-20260725-production-preflight-r2` proves rejection
occurs before result-root creation for undersized workstation/stress hosts,
missing approval, and a locally approved baseline on an undersized host. This
does not replace positive production-scale executions or the missing approved
baseline.

`cedar_production_campaign` now makes that external execution deterministic and
resumable: it covers all 13 public workloads across all five cache modes with
at least five approved-baseline/candidate pairs, then all ten typed fault/reopen
scenarios. It archives immutable baseline, candidate, and pair-runner bytes and,
on Linux, executes the already verified snapshot descriptors. Resume requires
the exact configuration and binary hashes, reconstructs both paired sample sets
from strict-read child artifacts, reruns the regression comparison, and checks
production identity/metrics plus offline report regeneration. The campaign
index binds full argv, approval, complete host provenance, child paths and child
hashes. The scoped campaign `SHA256SUMS` excludes database directories but binds
the executables, configuration, states, gates, and artifact protocol/report
files; write completion is followed by a safe exact-path-set readback. No
qualified-host campaign has run yet, so all production evidence rows remain
`PARTIAL` or `MISSING` as stated above.

### Observability release and paper gates

| Gate | Status | Current evidence and exact gap |
|---|---|---|
| PR | MISSING | no archived combined unit/format/model/scheduler, crash subset, CI workload and overhead-smoke gate |
| Nightly | MISSING | no workstation random-model/sanitizer/crash/paired/regeneration run |
| Release crash/recovery | PARTIAL | r5 eight-scenario corpus is authoritative but not every preceding persistence boundary |
| Release six-design audit | PARTIAL | current matrix, final-matrix manifest and claim-to-artifact ledger record focused proof and every remaining production/external boundary |
| Release cold/warm/durable/HTAP | PARTIAL | r3 is durable/cold and cache corpus covers one workload; no workstation/release profile |
| Release five repetitions | PARTIAL | r3 has five candidate repetitions per CI workload; no approved paired production comparison |
| Release checksums/stalls/telemetry/claims | PARTIAL | verification checksums exist; headline stall/drop and claims-link gates do not |
| Paper/external LDBC | APPROVED-OOS | excluded from the active functional production-release goal by user direction |

## Required Final Evidence

The active goal is complete only after this matrix is updated with artifact IDs
and command output for every row, or an explicit approved out-of-scope decision
with a tested failure contract. Normal, ASAN, UBSAN, TSAN, fault/recovery/oracle,
scheduler/HTAP stress and benchmark reproducibility must all be run with `-j1`.
No old runtime, external V2/Vn name, or legacy disk-layout
compatibility path may be reintroduced.

Baseline and execution-host requirements are specified in
`docs/superpowers/plans/2026-07-23-cedar-external-release-evidence-contract.md`;
smoke, ablation and removed-runtime substitutes are explicitly prohibited.
External LDBC and paper evidence are outside this active goal.
