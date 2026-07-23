# Cedar Clean-Break Naming and Legacy Removal Design

**Status:** Approved

**Date:** 2026-07-22

## 1. Objective

Cedar has one current architecture. Production APIs, source files, include
paths, durable paths, test names, and documentation therefore use canonical
names without `V2`, `V8`, `_v2`, `-v2`, or similar release-stage suffixes.
Pre-rearchitecture implementations and compatibility surfaces are removed
rather than retained behind aliases, readers, switches, or fallback paths.

The clean break does not remove format self-description. Durable encodings
continue to carry numeric format versions, magic values, checksums, required
feature bits, schema epochs, and benchmark protocol versions. Those fields
validate the current format and reject unknown or obsolete input; they never
select an old runtime.

## 2. Authoritative Architecture

After this change, the only production stack is:

```text
CedarDatabase
  -> TransactionCoordinator
  -> VersionSet + Manifest
  -> TemporalMemTable version chains
  -> SST + BlobStore + immutable index sidecars
  -> physical T-Cypher runtime
  -> shared resource, I/O, cache, and telemetry control planes
```

There is no public compatibility facade and no second metadata, storage,
transaction, query, or benchmark implementation.

## 3. Naming Policy

### 3.1 Canonical production names

The following mapping is normative:

| Current name | Canonical name |
|---|---|
| `CedarDatabaseV2` | `CedarDatabase` |
| `cedar_database_v2.h/.cc` | `cedar_database.h/.cc` |
| `DatabaseFormatV2` | `DatabaseFormat` |
| `MakeDatabaseFormatV2` | `MakeDatabaseFormat` |
| `VersionSetV2` | `VersionSet` |
| `VersionSnapshotV2` | `VersionSnapshot` |
| `VersionEditV2` | `VersionEdit` |
| `SstFileMetaV2` | `SstFileMeta` |
| `BlobSegmentMetaV2` | `BlobSegmentMeta` |
| `BlobSegmentKeyV2` | `BlobSegmentKey` |
| `IndexFragmentV2` | `IndexFragment` |
| `IndexFragmentKeyV2` | `IndexFragmentKey` |
| `DurableCheckpointV2` | `DurableCheckpoint` |
| `version_set_v2.h/.cc` | `version_set.h/.cc` |
| `BlobRefV2` | `BlobRef` |
| `SstV2*` | corresponding `Sst*` name |
| `sst_v2.h/.cc` | `sst.h/.cc` |
| `FlushResultV2` | `FlushResult` |
| `CompactionResultV2` | `CompactionResult` |
| `sst_flush_v2.h/.cc` | `sst_flush.h/.cc` |
| `sst_compaction_v2.h/.cc` | `sst_compaction.h/.cc` |
| `CEDAR_V2_SOURCES` | `CEDAR_SOURCES` |
| `kIndexCanonicalEncodingV1*` | encoding names without release-stage suffixes |
| historical `2026-07-17-cedar-columnar-v2-design.md` | `2026-07-17-cedar-columnar-design.md` |

No `using`, `typedef`, forwarding header, deprecated wrapper, alternate
namespace, or duplicated build target preserves the old name.

### 3.2 Durable paths

The current database layout becomes:

```text
FORMAT
manifest/MANIFEST
shards/<id>/sst/<file-number>.sst
indexes/<index-id>/<source-id>.idx
```

`manifest/MANIFEST-V2`, `.sst2`, `.sst2.tmp`, `.idx1`, and `.idx1.tmp` are old
layouts. Open and recovery must not search them as alternatives, rename them,
or infer current state from them.

Because this is a clean break rather than a continuation of the old format
line, the current authoritative database format starts at internal version
`1`. The `FORMAT` header uses a new version-neutral magic such as `CDFM`
instead of the old `FMT2` magic. The benchmark manifest's
`database_format_version` follows the same value; its benchmark protocol
version does not change. The database format number remains an internal
validation field and is not embedded in public class names or durable
filenames. SST, page, Blob, WAL, Manifest, statistics, spill, and sidecar
encodings keep their existing internal version values unless their encoded
bytes or required semantics change.

### 3.3 Internal format identifiers

The following are retained or normalized behind unversioned constant names:

- binary magic values such as the SST header/footer magic;
- `format_version`, `protocol_version`, and `database_format_version` fields;
- required and optional feature-bit masks;
- schema epochs and catalog generations;
- standard algorithm names such as FNV-1a;
- benchmark artifact schema versions used for strict parsing.

Raw magic literals such as `"CSI2"` should be named `kIndexSidecarMagic` in
code. The persisted bytes may remain unchanged. Tests describe the current
header or sidecar rather than using `V2` or `V8` in test names.

## 4. Legacy Code Removal

### 4.1 Already removed legacy tree

The deleted pre-rearchitecture API, Frond, graph facade, legacy transaction,
legacy storage, old CedarKey, old query engine, and associated examples/tests
remain deleted. The build must not reference them, and no replacement shim is
created.

### 4.2 Scheduler compatibility queue

`CompatibilityWorkId`, `ScheduledCompatibilityWork`, the compatibility queue,
and its enqueue/cancel/dequeue APIs are test-only compatibility surfaces. They
are deleted. Scheduler tests use executable task IDs and the production queue.

### 4.3 Materializing T-Cypher runtime

`ExecuteMultiRootMatchAsOf`, `ExecuteMultiFixedMatchAsOf`,
`CrossJoinRootResultStream`, `RootMatchBinding`, and the
`legacy_multi_root_materialized_rows` counter form a parallel materializing
runtime. They cannot simply be removed if an accepted query still reaches
them.

Before deletion, tests must enumerate every currently declared multi-root and
multi-relationship shape. Each accepted shape must produce a physical plan and
run through the bounded physical runtime. A shape outside the declared support
matrix must be rejected during binding or planning, before execution. There is
no executor fallback after physical planning fails.

### 4.4 Correctness fallbacks that remain

The following are not legacy compatibility and remain:

- advisory index corruption or incomplete-coverage fallback to authoritative
  base temporal data;
- deterministic conservative cost estimates when statistics are unavailable;
- bounded spill algorithms selected under memory pressure;
- recovery truncation of incomplete log tails where the durable protocol
  explicitly permits it.

These mechanisms should use names such as `base_scan_fallback` or
`conservative_estimate` so they cannot be confused with an old runtime.

## 5. Old-Format Rejection

Clean-break tests are written before the layout change and must initially fail
for the expected reason. They cover:

1. a database with no current `FORMAT` and old metadata files;
2. a `FORMAT` containing the old `FMT2` magic, previous database format number,
   or old Manifest location;
3. `manifest/MANIFEST-V2` without `manifest/MANIFEST`;
4. old `.sst2` and `.idx1` files in otherwise current directory structures;
5. recognizable pre-rearchitecture Manifest magic;
6. obsolete SST or sidecar magic copied to a current filename;
7. benchmark manifests missing the current strict provenance/schema fields.

Every rejection test snapshots the directory before and after open. Rejection
must not create `FORMAT`, `MANIFEST`, temporary files, renamed files, repaired
sidecars, or migrated SSTs.

Recognizable obsolete formats return `NotSupported`. Malformed current-format
files return `Corruption`. Missing Manifest-owned current files remain
corruption.

## 6. Implementation Boundaries and Order

### Phase 1: Rejection contract

Add the old-path and no-mutation RED tests. Centralize current durable names in
unversioned storage-layout constants so database, recovery, flush, compaction,
Blob, index, benchmark, and tests cannot diverge.

### Phase 2: Core value and metadata names

Rename `BlobRefV2`, database format types, VersionSet metadata structures, and
their call sites. This phase changes names, not encoded bytes or semantics.

### Phase 3: SST and Manifest authority

Rename SST/flush/compaction/VersionSet files and APIs. Switch current output to
`MANIFEST`, `.sst`, and `.idx`. Establish the clean-break database format as
internal version `1` with version-neutral `CDFM` magic. Recovery recognizes old
magic and paths only to reject the database before mutation.

### Phase 4: Database and consumers

Rename `CedarDatabaseV2` and update benchmark, Blob, index, T-Cypher,
observability, tests, README, CMake, and public includes. No old header remains.

### Phase 5: Parallel runtime deletion

Convert any remaining accepted T-Cypher shapes to the physical runtime, then
delete the materializing multi-root runtime and its counter. Delete the
scheduler compatibility queue and convert its tests to production APIs.

### Phase 6: Documentation and residual audit

Update the six current design documents to use canonical names. Historical
plans may mention old names only when labeled as historical evidence. Run a
full residual scan for version suffixes, legacy, compatibility, migration, and
fallback terms. Every retained hit must be recorded with its justification.
Local untracked build-directory names such as `build-v2` are build artifacts,
not production interfaces; new verification trees use canonical names, but
existing build directories are not deleted because the worktree-cleaning
constraint forbids destructive cleanup.

## 7. Testing and Evidence

Each phase follows RED then GREEN and builds with `-j1`.

Required evidence:

- clean-break rejection and no-directory-mutation tests;
- current create, write, flush, compact, checkpoint, close, and reopen tests;
- Manifest/SST/index publication fault matrices;
- crash, orphan cleanup, missing-live-file, and checksum corruption tests;
- fixed-seed temporal and T-Cypher oracle tests;
- all physical multi-root/relationship/spill/cancellation tests;
- benchmark artifact generation and offline regeneration;
- normal, ASAN, UBSAN, and TSAN full suites;
- `git diff --check` and explicit trailing-whitespace scan;
- residual `rg` report with a reason for every retained version/legacy term.

The final evidence package contains:

- old-to-canonical rename map;
- deleted production file and symbol list;
- retained internal format-field and magic list;
- old-format rejection matrix;
- exact verification commands and results.

## 8. Non-Goals

- No in-place migration tool.
- No automatic export/import path.
- No compatibility build flag.
- No deprecated API grace period.
- No changes to LogicalKey, TemporalEvent, bitemporal visibility, transaction
  durability, physical-query semantics, or resource-governance guarantees
  except those required to remove a parallel legacy path.
