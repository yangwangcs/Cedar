# Cedar Public Documentation

This directory contains the public documentation and measured results for the
Cedar Kernel bitemporal graph database.

## Start Here

1. Read the [Cedar overview](../README.md) for the product model, LSM-backed
   columnar architecture, bitemporal fact definition, and public C++ API.
2. Read the [Cedar User Guide](user-guide.md) for build instructions, the
   embedded C++ API, T-Cypher syntax and examples, Bolt usage, schema manifests,
   transactions, and operational limits.
3. Read the [query acceptance summary](query-acceptance.md) for correctness,
   recovery, sanitizer, projection, and space gates.
4. Read the [development-host performance report](query-performance.md) for
   measured write, read, and sustained mixed-workload results.

## Public Storage Model

Cedar combines:

- one embedded LSM engine and one WAL;
- CedarParquet authoritative bitemporal columnar facts;
- engine-owned WAL recovery, MemTables, VersionSet, and MANIFEST; and
- Cedar-owned temporal and adjacency projections that can be rebuilt or
  bypassed through canonical fallback.

The public storage contract is described in the root README and is reflected
in the acceptance and performance evidence linked above.

## Opt-in Meta Sequence v2 (One-Way Format Gate)

Cedar's meta column family stores one `sequence/<seq>` record per commit.
The default **v1** record (`kind 7`) repeats every 32-byte fact key. **Meta
sequence v2** (`kSequenceV2`, on-disk record kind **9**) is an opt-in writer
format that stores a 23-byte descriptor per fact instead; the reader rebuilds
the 32-byte fact key deterministically from the descriptor and the record key's
commit sequence. The saving is `6 + 9 * fact_count` bytes per sequence record.
Record framing, meta version, CRC32C, and all other record kinds are unchanged,
so v1 and v2 records coexist in one database and every read path dual-reads
both.

### Durable marker and opt-in

- The durable marker is the meta-CF key
  `format/sequence-record-format`: a normal framed meta record (`kind 10`)
  whose 4-byte big-endian payload is `1` (v1) or `2` (v2). Any other value is
  rejected at open as corruption.
- An open that opts into format 2 writes the marker with `Sync=true` before it
  accepts any commit, so a crash cannot leave a v2 record that a later v1 open
  would not expect. The marker is authoritative: a database that recorded 2
  keeps writing v2 even when a later open uses the default option.
- `DatabaseOptions::meta_sequence_format` selects the writer format for the
  kernel (`1` is the default). `Database::Open` validates it and rejects any
  value other than 1 or 2 with `InvalidArgument`. The storage-internal
  `FactStoreOptions::meta_sequence_format` has the same meaning.
- The default (1) writes no marker and keeps new v1 records byte-identical to
  previous releases. Enabling 2 changes only newly committed records; existing
  v1 records stay v1 until reclaimed.

```cpp
cedar::DatabaseOptions options;
options.path = "/path/to/db";
options.meta_sequence_format = 2;  // kMetaSequenceFormatV2
auto database = cedar::Database::Open(std::move(options));
```

Rollout order matters: first deploy the dual-read release, then opt the
database in. Do not enable format 2 while any older binary can open the
database.

### Reclaiming existing v1 records (offline)

The bundled migration tool rewrites existing `sequence/<seq>` records in place:

```text
cedar-meta-v2-reclaim --db=PATH [--batch=N] [--sync] [--no-verify]
```

- The database must already record marker 2. Opt it in and open it once
  (format 2) before running the tool; it refuses a missing or v1 marker before
  writing anything.
- The database must be offline: the caller must ensure no Cedar process has it
  open, and the tool fails if the engine is already locked.
- Each batch of at most `--batch` records (default 1024) is one atomic write.
  The run is resumable and idempotent: a stopped or crashed run leaves a
  consistent mixed database (completed batches are v2, later records are v1),
  and a rerun skips records that are already v2.
- `--no-verify` is not recommended. By default every rewritten record is
  decoded again, including every rebuilt 32-byte fact key, and compared with
  the v1 source before the batch is written.
- The tool prints `scanned`, `rewritten`, `already_v2`, `bytes_before`, and
  `bytes_after`. Per rewritten record it saves exactly `6 + 9 * facts` bytes.
- Current assumption: the tool opens with Cedar's default storage profile and
  does not accept custom production-profile or WAL-directory placement
  options. Production-profile/custom-WAL support is a follow-up; use the tool
  only on databases created and opened with the default storage layout.

### ONE-WAY GATE: old binaries reject v2 records

> **WARNING: enabling meta sequence format 2 is a one-way upgrade.**
> Once the format marker is 2 and/or v2 records exist, binaries that predate
> the dual-read release cannot open the database.

- An old binary's v1-only sequence decoder sees record kind 9, fails the
  framing check (`kSequence` expected), and reports `Corruption`. Recovery
  fails at open; it does not silently skip or misread sequence records.
- The dual-read release keeps a v1-only API (`DecodeSequenceRecord(encoded)`)
  for callers that do not supply the record key. It rejects a v2 record with
  the actionable
  `Corruption: sequence record: v2 sequence record requires its commit sequence`
  instead of guessing `commit_seq` and rebuilding wrong fact keys. This
  expected failure is pinned by
  `FactStoreMetaCodecSequenceV2Test.V1OnlyDecoderRejectsV2RecordWithDocumentedCorruption`.
- **There is no downgrade path in this milestone.** Returning a v2 database to
  v1 requires rewriting every v2 record back to v1 with a dedicated offline
  tool that does not exist yet. Restoring a v1-era binary over a v2 database is
  unsupported and fails open rather than degrading silently. Back up before
  opting in, and treat the opt-in as irreversible.
- Databases that never opt in remain fully readable by old binaries; the
  default writer format is unchanged.

### Sequence-record retention below the Vacuum watermark — deferred

Decision record (F10 Task 13, audit #11 follow-up): **deferred**, not
implemented in this milestone.

- No code prunes `sequence/*` records at or below the Vacuum watermark.
  Reclamation only rewrites v1 records to v2 in place; it never deletes a
  sequence record, and `Vacuum` continues to remove only obsolete fact
  versions.
- Why deferred: correct pruning needs (a) a crash matrix covering pruning
  against visible/oldest-readable watermark publication and Vacuum resume,
  (b) an explicit old-binary/interop and downgrade story, and (c) retention
  policy semantics (minimum retained history, `as_of` reads, and
  `ReadSequenceRange` behavior below the boundary). The v2 space saving is
  available without changing any retention contract.
- What a future implementation must guarantee: never prune above the
  published `oldest_readable` boundary or any pinned reader's sequence; keep
  pruning and the durable watermark update atomic/crash-safe so reopen sees
  either the pre-prune or the post-prune consistent state; bound and resume
  deletion work in batches; make read/repair paths report an explicit error at
  or below the retained boundary instead of silently shortening a range; and
  document the minimum-retention, old-binary, and reclaim-interaction
  behavior with tests.

## Evidence Interpretation

The published results are tied to their documented build, workload, host, and
admission parameters. They are intended to make correctness and performance
measurements reproducible, not to define a hardware-independent SLA.

- A sustained qualification requires at least 1,800 seconds of actual elapsed
  workload time.
- `facts/s` is the primary write metric when transactions contain multiple
  facts; `txn/s` is derived from the configured transaction size.
- Projection results are compared with authoritative canonical facts.
- Missing, stale, corrupt, or partial projections must fall back to canonical
  facts without returning silently stale results.
- Generated database directories and host-specific raw benchmark artifacts are
  not published in the source tree.

## Documentation Boundary

Detailed implementation plans, historical design drafts, and internal
execution records are maintained as local development material. They are not
part of the public GitHub documentation surface. The files in this directory,
the root README, and the source/tests are the public contract.
