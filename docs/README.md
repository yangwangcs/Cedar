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
5. Read the [follow-up release notes](release-notes.md) for the F11-F19 changes
   and the F14 retention milestone, including the default v2 writer, the
   reopen-validation default switch, and their rollback/opt-out options.

## Public Storage Model

Cedar combines:

- one embedded LSM engine and one WAL;
- CedarParquet authoritative bitemporal columnar facts;
- engine-owned WAL recovery, MemTables, VersionSet, and MANIFEST; and
- Cedar-owned temporal and adjacency projections that can be rebuilt or
  bypassed through canonical fallback.

The public storage contract is described in the root README and is reflected
in the acceptance and performance evidence linked above.

## Meta Sequence v2 (Default Writer Format, One-Way Gate)

Cedar's meta column family stores one `sequence/<seq>` record per commit.
The default writer format is now **v2** (`kSequenceV2`, on-disk record kind
**9**): it stores a 23-byte descriptor per fact instead of repeating the
32-byte fact key, and the reader rebuilds the fact key deterministically from
the descriptor and the record key's commit sequence. The saving is
`6 + 9 * fact_count` bytes per sequence record versus the v1 `kSequence`
record (`kind 7`). Record framing, meta version, CRC32C, and all other record
kinds are unchanged, so v1 and v2 records coexist in one database and every
read path dual-reads both.

### Durable marker and default

- The durable marker is the meta-CF key
  `format/sequence-record-format`: a normal framed meta record (`kind 10`)
  whose 4-byte big-endian payload is `1` (v1) or `2` (v2). Any other value is
  rejected at open as corruption.
- Since F19 the default is 2. An open with default options writes the marker
  with `Sync=true` before it accepts any commit, so a crash cannot leave a v2
  record that a later v1 open would not expect.
- **Existing v1 databases are upgraded in place** when opened with default
  options: the marker is written as 2 (before the next commit), all new commits
  use `kSequenceV2`, and existing v1 records remain readable. This is the
  accepted one-way upgrade. Back up the database before opening it with a
  v2-default build, and do not do so while any older binary may still need to
  open the database.
- The marker is authoritative: a database that recorded 2 keeps writing v2
  even when a later open asks for 1. There is no downgrade path in this
  milestone; `cedar-meta-v2-reclaim` rewrites v1 records forward, never back.
- `DatabaseOptions::meta_sequence_format` selects the writer format for the
  kernel. `Database::Open` validates it and rejects any value other than 1 or 2
  with `InvalidArgument`. The storage-internal
  `FactStoreOptions::meta_sequence_format` has the same meaning.
- **Explicit v1 opt-out:** set `meta_sequence_format = 1` to keep a database
  (or a new database) on the v1 writer. That writes no marker and keeps new v1
  records byte-identical to previous releases. It only works while the durable
  marker is absent or 1; once a database records 2, asking for 1 is ignored.
- The v1-only compatibility build (F17) defaults to 1, because it simulates a
  pre-v2 binary.

```cpp
// Default since F19: write (or upgrade to) meta sequence v2.
cedar::DatabaseOptions options;
options.path = "/path/to/db";
auto database = cedar::Database::Open(std::move(options));

// Explicit v1 opt-out for a database that must stay v1-readable by old binaries.
cedar::DatabaseOptions v1_options;
v1_options.path = "/path/to/db";
v1_options.meta_sequence_format = 1;  // kMetaSequenceFormatV1
auto v1_database = cedar::Database::Open(std::move(v1_options));
```

A build that defaults to v2 must not open a database that any older v1-only
binary still needs to read; the first default open makes that database a v2
database.

### Reclaiming existing v1 records (offline)

The bundled migration tool rewrites existing `sequence/<seq>` records in place:

```text
cedar-meta-v2-reclaim --db=PATH [--batch=N] [--sync] [--no-verify]
                      [--profile=NAME] [--wal-dir=PATH]
```

- The database must already record marker 2. A default open (F19) writes it, so
  upgraded databases are ready for reclamation; an explicit v1 database still
  has no marker and the tool refuses it before writing anything.
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
- The tool accepts the database's storage layout. `--profile` selects the
  `developer` (default), `kernel-test`, `debug-small`, or `production` profile
  the database was written with, and `--wal-dir=ABSOLUTE_PATH` names a WAL
  directory outside the database directory; the offline open recovers it
  exactly like a Cedar process does. `MetaV2ReclaimOptions::storage_profile`
  and `MetaV2ReclaimOptions::production` expose the same settings to callers.
  A non-empty WAL directory must be absolute, and the default layout (Developer
  profile, WAL inside the database directory) is unchanged.
- Omitting a matching WAL directory is unsafe: the format marker and recent
  sequence records may still live in the WAL, so the tool can see a stale
  SST-only view (and refuses a WAL-only marker before writing anything).

### ONE-WAY GATE: old binaries reject v2 records

> **WARNING: meta sequence format 2 is the default and is a one-way upgrade.**
> Once the format marker is 2 and/or v2 records exist, binaries that predate
> the dual-read release cannot open the database. A default open upgrades the
> database immediately, so back up and complete the binary rollout first.

- A real pre-v2 binary's v1-only sequence decoder sees record kind 9, fails the
  framing check (`kSequence` expected), and reports `Corruption`. Recovery
  fails at open; it does not silently skip or misread sequence records.
- The repository ships a build profile that reproduces such a binary for
  end-to-end verification: `-DCEDAR_META_SEQUENCE_V1_ONLY=ON` (see the next
  subsection).
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
  the first default open, and treat the upgrade as irreversible.
- A database opened with the explicit v1 opt-out (`meta_sequence_format = 1`)
  and never upgraded remains fully readable by old binaries.

### Verifying the gate with a v1-only compatibility build

`CEDAR_META_SEQUENCE_V1_ONLY=ON` compiles a binary that refuses to read meta
sequence format 2, mirroring a pre-v2 release while leaving the default build
unchanged:

```bash
cmake -S . -B build-meta-v1-only -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DBUILD_TESTS=ON -DCEDAR_META_SEQUENCE_V1_ONLY=ON
cmake --build build-meta-v1-only --target test_meta_compat
ctest --test-dir build-meta-v1-only -R '^MetaCompat' --output-on-failure
```

- Opening a database whose durable marker records 2 fails with an actionable
  `Corruption`: the message names the rejected format, the
  `CEDAR_META_SEQUENCE_V1_ONLY` build switch, and the recovery action
  ("reopen the database with a binary built with meta sequence v2 support").
- A stray `kSequenceV2` record in a database whose marker is missing is
  rejected the same way. Reopen validation never decodes the descriptor
  payload, so wrong 32-byte fact keys cannot be rebuilt silently.
- v1 databases still open, read their contiguous history, and accept new v1
  commits under the v1-only build; the gate is not a generic sequence-record
  rejection. That build defaults to `meta_sequence_format = 1`, mirroring a
  pre-v2 release.
- The v1-only build also refuses to opt a database into format 2
  (`InvalidArgument` before any engine write), so it cannot leave behind a
  marker it will be unable to read back.
- The default build is v2-capable and unchanged. `tests/storage/test_meta_compat.cc`
  runs the same end-to-end checks in every normal build by enabling the gate
  through an internal test hook, and
  `tests/storage/run_meta_compat_v1_only_ci.sh` runs them against the real
  compatibility build in CI.

### Sequence-record retention below the Vacuum watermark — opt-in

Implemented in F14 as an **opt-in** reclamation step; the default is unchanged,
which keeps every `sequence/*` record and the historical contiguous
`[1..visible]` reopen contract.

- **Enable:** `DatabaseOptions::prune_sequence_records_on_vacuum = true`
  (`FactStoreOptions::prune_sequence_records_on_vacuum` at the storage layer).
  Default `false`. `Database::Vacuum` then advances the newest durable boundary
  and removes older committed sequence records; an open with the option enabled
  also finishes a prune a crash interrupted.
- **What is deleted:** only records with `commit_seq < oldest_readable`, the
  durable Vacuum boundary. One baseline per valid-time chain and every fact at
  or above the boundary stay. The reclamation tool's v1→v2 rewrite is
  independent and never deletes sequence records.
- **Durable watermark:** the boundary is persisted as
  `watermark/sequence-retention` (the same watermark record encoding) with
  `Sync=true` *before* the first delete. Reopen validation starts at that
  watermark: missing records below it are expected, while any gap at or above
  it (and any record above the visible watermark) still fails the open with
  `Corruption`. A watermark above the oldest-readable boundary is rejected as
  corruption.
- **Crash safety:** a crash before the watermark write leaves the old
  contiguous history; a crash after it (including inside a delete batch) leaves
  a watermark that exactly excuses the records that may be missing, and the
  surviving records at or above it are untouched. Deletes run in idempotent
  batches of at most 1024 keys; the next `Vacuum` or opted-in open resumes the
  remaining work. There is no separate prune cursor to corrupt.
- **Read/repair semantics:** `ReadSequence` and `ReadSequenceRange` report
  `SnapshotExpired` for history below the durable boundary instead of silently
  shortening the range; a missing record at or above it is `Corruption`. The
  transaction-ID allocator cold path starts its rebuild at the watermark.
  `BeginSnapshot`/`GetPointAtAt` already reject `as_of` below
  `oldest_readable`.
- **Compatibility:** the extra meta key is invisible to binaries that predate
  it, and a database kept on the explicit v1 opt-out is byte-compatible and
  fully readable by them. (The default v2 writer is itself a one-way gate; see
  above.) Once pruning removes sequence history, however, an old binary's
  strict `[1..visible]` reopen (or a new binary opened without ever reading the
  watermark) would report missing durable sequence records. Treat enabling
  retention as a one-way operational choice for the retention policy, just like
  the v2 writer gate; keep a backup if old binaries must still open the
  database.
- **Tests:** `tests/storage/test_sequence_retention.cc` covers the default
  no-op, bounded/idempotent pruning, exact deleted-byte accounting, retained
  `ReadSequenceRange` semantics, gaps at/above the watermark, actual process
  crashes before the watermark and inside the first delete batch, a crash
  after fact-cleanup resumes both phases, and kernel option plumbing.

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
