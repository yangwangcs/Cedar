# Cedar Immutable Maintenance Resource Admission Design

Date: 2026-07-23

Status: Implemented and verified in the normal correctness matrix

Extends:

- `2026-07-17-cedar-temporal-index-cbo-design.md`
- `2026-07-17-cedar-htap-resource-scheduling-design.md`

## 1. Purpose

Index sidecar builds and statistics merges already run as typed, cancellable
optional maintenance. Their current requests account for SST read bytes but
declare zero memory and zero write bytes even though both paths decode a full
SST and publish an immutable artifact. This violates the resource contract and
allows optional maintenance to allocate or write outside its grant.

This design makes one immutable SST the admission and yielding unit for both
work classes. A task obtains its complete conservative grant before reading the
source SST, creating a temporary file, detaching coverage, or publishing any
durable state.

## 2. Decisions

1. `INDEX_BUILD` and `STATS_MERGE` are scheduled one source SST at a time.
2. A work item captures the source identity and the catalog/statistics inputs
   used for estimation. It does not discover additional work after admission.
3. The component that owns an allocation or file format owns its estimate.
4. Estimates are conservative upper bounds, not historical averages.
5. Resource and I/O admission complete before the callback performs source I/O
   or filesystem mutation.
6. Each SST task releases its grant before the next SST is considered.
7. Optional maintenance rechecks pressure between SST tasks. A refusal leaves a
   correct coverage/statistics gap and returns a retryable resource status.
8. This changes no database format, Manifest magic, feature bits, visibility
   rule, index sidecar format, or statistics checkpoint format.
9. Blob-backed statistics canonicalize the persisted `BlobRef` content hash;
   they never treat the placeholder event value as the indexed value and never
   read a Blob payload.
10. Pressure-policy refusal returns typed `MaintenanceBackoff`. A concrete
    governor dimension that cannot satisfy the declared request retains its
    existing `ResourceExhausted` or `QueryMemoryLimit` status.

## 3. Rejected Alternatives

### 3.1 One grant for the complete file batch

This is a smaller coordinator edit, but it holds the sum of all I/O and output
charges while memory consumption is only per-file. Large batches create
head-of-line blocking and do not yield to ingestion between SSTs.

### 3.2 Read first, then extend the grant

This gives a tighter estimate after decoding, but source I/O and decoded event
allocation occur before the task owns the resources needed to finish. It also
requires exposing the execution service's internal lease through maintenance
callbacks. The current component metadata is sufficient for a safe preflight
bound, so the extra protocol is not justified.

## 4. Work Units

### 4.1 Index work item

An index work item contains:

```text
source SstFileMeta
captured IndexDefinition
captured Manifest generation
existing fragment metadata, when present
deterministic target relative path
```

Only property SSTs with one matching `BUILDING` or `ACTIVE` definition produce
work. The catalog already forbids two live definitions for the same entity,
column, and schema epoch, so one source SST has at most one index work item.

The callback verifies that the source and definition are still live and
unchanged. A stale work item returns `Conflict` without reading or writing and
is rebuilt from a later snapshot by the existing maintenance reconstruction
boundary.

### 4.2 Statistics work item

A statistics work item contains one property `SstFileMeta` and the captured
statistics generation used to estimate the projected checkpoint rewrite. It
builds one fixed-format `StatsFragment` and calls the statistics store only
after the complete task has been admitted.

## 5. Component-Owned Estimates

All arithmetic is checked or saturating. Overflow returns `ResourceExhausted`
before task submission; it must not wrap into a smaller request.

### 5.1 SST decode bound

The coordinator does not estimate C++ object layout itself. The columnar
component exposes a helper that derives a conservative full-file decode peak
from `SstFileMeta.statistics`, physical type, and the registered schema's
inline-value bound.

The estimate covers:

- the retained `vector<TemporalEvent>` and its capacity;
- fixed event, key, value, optional BlobRef, and allocator overhead;
- inline string/binary bytes up to the schema `blob_threshold`;
- one granule/block decode transient;
- compressed and decoded page buffers that may coexist during block decode.

Fixed-width physical values use their fixed canonical bound. Blob-backed rows
use the fixed `BlobRef`/hash bound and never budget Blob payload materialization.

### 5.2 Index sidecar bound

The index component exposes an estimate for the captured definition and SST
statistics. It covers the worst case in which every `PUT` row emits a posting:

- posting vector capacity and canonical-value storage;
- sorting and the by-value posting copy used by sidecar encoding;
- dictionary, bitmap, sorted-delta, or plain encoding intermediates;
- the encoded checksum-bearing sidecar retained by the caller;
- the second encoded buffer currently created by atomic file publication;
- temporary and target descriptors;
- sidecar bytes written, file fsync, rename, and directory fsync metadata ops.

The encoded byte bound is format-aware. It uses the source row count for bitmap
ordinal space and the schema inline bound for canonical values. It rejects any
bound above the existing maximum sidecar size rather than admitting a task that
the writer must later reject.

When an existing fragment is present, the same bound also covers reading and
decoding that sidecar for the health check. Preflight must not call `file_size`
or read the sidecar before admission. The fragment's persisted posting count,
the deterministic format, the source row count, and the schema inline bound are
sufficient to derive a conservative existing-sidecar read and decode charge.

The reader accepts that computed maximum as an allocation bound. Inside the
admitted callback it opens and `fstat`s the file, then returns `Corruption`
without allocating the body when the physical size exceeds the maximum.
Persisted fragment metadata with `source_row_count` different from the source
SST or `indexed_put_count` greater than the source `put_count` is unhealthy and
is rebuilt without reading the old sidecar. The `fstat` and possible bounded
sidecar read are included in the task's metadata and sequential-read request.

### 5.3 Manifest rewrite bound

`VersionSet` estimates the exact framed Manifest rewrite for the captured edit.
Index repair writes the replacement sidecar and replaces the fragment in one
generation-CAS Manifest edit. It does not publish an intermediate detached
generation. The task request includes the exact projected replacement rewrite,
including temporary Manifest bytes, written bytes, descriptors, rename/fsync
work, and metadata operations.

A generation conflict before either edit returns `Conflict`. An indeterminate
Manifest publication retains the existing reopen-required semantics; resource
accounting must not translate it into cancellation or retry.

### 5.4 Statistics build and checkpoint bound

The statistics component estimates:

- the distinct-value set at a maximum of `put_count` entries;
- copied inline canonical values or fixed-width/Blob-hash values;
- the fixed-size `StatsFragment` and map node;
- the complete projected checkpoint body and framed checksum buffer;
- temporary and target descriptors;
- checkpoint bytes written, file fsync, rename, parent-directory fsync, and
  metadata operations.

`BuildStatsFragment()` uses `EncodeIndexBlobHash(*event.blob_ref())` for a
Blob-backed `PUT` and `EncodeIndexCanonicalValue(event.value())` for an inline
`PUT`. This corrects the current placeholder-value behavior and makes the
resource estimate and distinct-value statistics use the same authoritative
canonical identity.

`StatsSnapshotStore` owns the projected checkpoint calculation because it owns
the in-memory fragment map and checkpoint format. The estimate is taken under
the store mutex against the captured generation. `Upsert` rejects a stale
expected generation before mutation, so concurrent statistics work cannot
silently exceed an older grant.

The new expected-generation upsert constructs and encodes a projected map
without publishing it, atomically replaces the checkpoint, and only then swaps
the projected map and advances the in-memory statistics generation. An encode,
write, file-fsync, or pre-rename failure leaves the old disk checkpoint,
in-memory map, and generation unchanged.

After rename, the checkpoint publisher opens and fsyncs the parent directory
before publishing the projected map in memory. The directory descriptor and
fsync are included in both resource and I/O metadata dimensions. Because the
checkpoint is advisory, a post-rename directory-fsync error returns the exact
I/O failure and leaves the current in-memory snapshot unchanged; reopen may
accept the checksum-valid newer checkpoint or discard it conservatively.

## 6. Admission And Execution Flow

For each eligible SST:

1. capture the immutable work item from one pinned snapshot;
2. ask the owning components for their checked estimates;
3. combine them into one `ResourceProfile` and `IoTokenRequest`;
4. recheck pressure and submit one typed preemptible maintenance task;
5. let `MaintenanceExecutor` acquire the resource grant and I/O tokens;
6. enter the callback, revalidate the captured identities, then read the SST;
7. build and publish the artifact with the existing cancellation checkpoints;
8. return the exact typed status and release the task grant by RAII;
9. only then consider the next SST.

The resource request includes one CPU slot. Descriptor, memory, temporary-byte,
sequential-read, write-byte, and metadata-op dimensions must all be nonzero
when the selected work performs that operation. `IoTokenRequest` mirrors the
physical read/write/metadata dimensions of the `ResourceProfile`.

For index repair, sequential-read bytes include both the source SST and the
conservative existing-sidecar bound. A missing-fragment build includes only the
source SST read.

## 7. Failure Semantics

- Resource or I/O rejection occurs before `ReadSstFile`, directory creation,
  fragment detachment, checkpoint mutation, or temporary-file creation.
- Rejection leaves the source SST and all existing Manifest/statistics state
  unchanged. Missing or corrupt index coverage continues to use the base path.
- Cancellation before publication removes unpublished sidecar/checkpoint
  temporary output using the existing cancellation protocol.
- Cancellation is not checked after an irreversible rename/Manifest boundary
  in a way that could misreport a possibly published artifact as cancelled.
- `Conflict` means the captured work item is stale and may be reconstructed.
- `MaintenanceBackoff` means pressure policy declined optional work before
  task admission. It is retryable after pressure changes and is distinct from
  an infeasible resource request.
- `Indeterminate` and `RecoveryRequired` retain their existing meanings and
  take precedence over optional-maintenance retry.
- A statistics failure remains advisory and cannot affect query visibility or
  authoritative storage state.

## 8. Pressure And Fairness

The scheduler does not enqueue the entire batch under one grant. Between SSTs,
the coordinator refreshes pressure and gives foreground writes, flush,
commit-critical work, recovery, shutdown, and urgent compaction an admission
opportunity. Normal optional work stops when the pressure policy disallows it.

This design does not promise strict wall-clock fairness; that remains a release
stress-evidence requirement. It establishes the functional prerequisite: a
bounded yielding quantum with a complete, releasable per-SST grant.

## 9. Tests

Implementation follows RED/GREEN order and adds focused production-path tests:

1. Index build with insufficient memory is rejected before the source SST is
   read and before any index directory, temp file, fragment detach, or Manifest
   generation change.
2. Index build with insufficient write or metadata budget has the same zero-
   mutation behavior.
3. An oversized existing sidecar and impossible persisted posting counts are
   classified as unhealthy without allocating or reading beyond the admitted
   legal encoding bound, then rebuilt from the immutable source.
4. A successful index task observes nonzero memory/read/write/metadata usage
   during its callback and releases every dimension afterward.
5. Two source SSTs produce two admissions; the first task's full grant is
   released before the second begins.
6. Pressure raised after the first SST prevents the second SST from mutating
   state while preserving the first published fragment and returns typed
   `MaintenanceBackoff`.
7. Statistics merge with insufficient memory/write/metadata budget performs no
   source read and does not create or replace its checkpoint.
8. A successful statistics task accounts for the projected full checkpoint
   rewrite and releases its complete grant.
9. A stale statistics generation and stale index work item return `Conflict`
   before mutation.
10. Distinct statistics count two different Blob hashes separately, coalesce
   repeated references to the same hash, and perform zero Blob payload reads.
11. Cancellation, post-rename `Indeterminate`, reopen, and existing base-fallback
   tests continue to preserve their exact statuses.
12. Statistics checkpoint publication accounts for and performs parent-
   directory fsync; injected pre-rename failure leaves the old checkpoint and
   in-memory generation unchanged.

Focused tests precede the later consolidated normal/sanitizer/release matrices,
consistent with the feature-first closure order.

## 10. Completion Boundary

This slice is functionally complete when both production paths have no
zero-memory or zero-write declaration, resource rejection is proven to precede
all read/write mutation, one SST is the release/yield quantum, and every grant
is released on success, conflict, cancellation, and failure.

It does not close automatic corrupt-fragment health-event scheduling,
concurrent runtime-feedback generation tests, production-scale fairness,
sanitizer refresh, or release/paper artifacts. Those remain explicit items in
the six-design completion matrix.

## 11. Verification Record

The implementation adds typed `MaintenanceBackoff`, BlobRef-aware statistics,
bounded sidecar reads, component-owned SST/index/statistics estimates, exact
projected Manifest/checkpoint estimates, generation-CAS publication, and one
grant per source SST. The focused closure selection passes 18/18; additional
stats-release, dynamic between-SST pressure, and reopen-queue regressions pass
individually. The fresh normal matrix passes 873/873 with `-j1` after the final
accounting correction. Sanitizer and release/paper artifacts were intentionally
not refreshed in this feature batch.
