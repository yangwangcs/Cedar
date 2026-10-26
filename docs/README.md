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
5. Read the [follow-up release notes](release-notes.md) for the F11-F20 changes
   and the F14 retention milestone, including the v2-only meta sequence
   contract, the full-history reopen-validation default, and the explicit
   bounded-validation option.
6. Read the [columnar cache deployment notes](columnar-cache-and-deployment.md)
   for cache ownership, reproducible validation, and current evidence limits.

## Public Storage Model

Cedar combines:

- one embedded LSM engine and one WAL;
- CedarParquet authoritative bitemporal columnar facts;
- engine-owned WAL recovery, MemTables, VersionSet, and MANIFEST; and
- Cedar-owned temporal and adjacency projections that can be rebuilt or
  bypassed through canonical fallback.

The public storage contract is described in the root README and is reflected
in the acceptance and performance evidence linked above.

## Meta Sequence v2 (The Only Supported Format)

Cedar's meta column family stores one `sequence/<seq>` record per commit. The
only supported encoding is **v2** (`kSequenceV2`, on-disk record kind **9**): it
stores a 23-byte descriptor per fact instead of repeating the 32-byte fact key,
and the reader rebuilds the fact key deterministically from the descriptor and
the record key's commit sequence. The saving is `6 + 9 * fact_count` bytes per
sequence record versus the retired v1 `kSequence` record (kind 7). Record
framing, meta version, CRC32C, and all other record kinds are unchanged.

### v2-only contract

- **Writer:** every commit writes a kind-9 `kSequenceV2` record. There is no
  v1 writer and no format-selection parameter in the commit path.
- **Durable marker:** the meta-CF key `format/sequence-record-format` is a
  normal framed meta record (kind 10) whose 4-byte big-endian payload is the
  format value. A fresh database writes marker **2** with `Sync=true` in the
  same initialization batch that writes the system identity and watermarks,
  before the store can accept any commit. A database written by this release
  therefore always has marker 2.
- **Reopen:** the marker must decode and record 2. A missing marker, a
  marker recording 1 (v1), or any kind-7 `kSequence` record found by reopen
  validation fails the open as unsupported; v1 records are never decoded. Other
  marker values fail as `Corruption`.
- **No downgrade:** there is no path that returns a v2 database to v1. A v1
  binary cannot read a v2 database, and this release cannot read a v1 database.
- **Retention is independent:** the opt-in sequence-record retention policy
  below still applies and does not change the record encoding.

### Option compatibility

`FactStoreOptions::meta_sequence_format` and
`DatabaseOptions::meta_sequence_format` are kept for source compatibility, but
v2-only validation applies: the only accepted value is **2** (the default).
Passing **1** fails with `InvalidArgument` explaining that v1 is no longer
supported; any other value also fails with `InvalidArgument`. The option no
longer selects the writer; it only validates the caller's assumption.

```cpp
// The default (and only) format.
cedar::DatabaseOptions options;
options.path = "/path/to/db";
auto database = cedar::Database::Open(std::move(options));
```

### Legacy v1 databases are unsupported

A database written by a release that predates v2 (no marker, marker 1, or
kind-7 sequence records) is rejected at open with a `NotSupported` error such
as:

```text
NotSupported: meta sequence: database has no durable meta sequence format
marker; this Cedar release supports only meta sequence format 2 (v2); v1
sequence records are unsupported, and migration/downgrade is not supported.
Delete and recreate the database, or restore a format-2 backup.
```

v1 is unsupported: this release ships no migration tool and has no downgrade
path. Delete and recreate the database, or restore a format-2 (v2) backup.

### Removed v1 compatibility machinery

- The v1 encoder `EncodeSequenceRecord` and the v1-only decoders
  `DecodeSequenceRecord(encoded)` / `DecodeSequenceRecordView(encoded)` were
  removed. All decoding uses the key-aware v2 decoder
  `DecodeSequenceRecord(encoded, commit_seq)` /
  `DecodeSequenceRecordView(encoded, commit_seq)`.
- The `CEDAR_META_SEQUENCE_V1_ONLY` build option, the
  `meta_sequence_compat.h` read gate, `test_meta_compat`, and
  `run_meta_compat_v1_only_ci.sh` were removed. The v1-only compatibility
  profile no longer exists.
- The v1→v2 reclamation API, CLI tool, and tests were removed with v1 support;
  this release has no migration tool.

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
  or above the boundary stay. Sequence-record retention is independent of the
  record encoding and never rewrites or deletes v2 records above the boundary.
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
  same expiry applies to transaction resolution and idempotent commit retries.
  Concurrent resolution uses a single RocksDB snapshot, so it can still return
  the historical result when its snapshot predates the prune.
  The transaction-ID allocator cold path starts its rebuild at the watermark.
  `BeginSnapshot`/`GetPointAtAt` already reject `as_of` below
  `oldest_readable`.
- **Compatibility:** the extra meta key is invisible to binaries that predate
  it, but only v2-capable binaries can read the v2 sequence records this release
  writes. Once pruning removes sequence history, an older binary's strict
  `[1..visible]` reopen (or a v2 binary opened without ever reading the
  watermark) would report missing durable sequence records. Treat enabling
  retention as a one-way operational choice for the retention policy, just like
  the v2-only format; keep a backup if old binaries must still open the
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
