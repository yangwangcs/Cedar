# Cedar Storage File Inspection Design

**Date:** 2026-08-21

## Purpose

Cedar databases contain RocksDB-owned `.sst` files. The shared suffix is
intentional: it preserves RocksDB's file-name, recovery, backup, repair, and
obsolete-file lifecycle protocol. It is not a statement that every file has
the same table format or logical role.

Expose a Cedar-owned, read-only inspection surface that maps each *live* SST
to its Cedar meaning. Operators should be able to answer which files are
authoritative columnar facts and which are transaction metadata without
changing the on-disk filename protocol.

## Non-goals

- Do not rename `.sst` files or modify RocksDB filename parsing.
- Do not scan, recover, repair, delete, or otherwise mutate database files.
- Do not report obsolete SSTs, WAL history, or archival logs in version one.
  Their lifecycle and retention semantics are separate from the live
  VersionSet view.
- Do not expose RocksDB types in Cedar public headers.
- Do not add BlobDB support. Kernel stores do not enable blob files.

## Public Model

Add a public header `cedar/storage_files.h` containing a value model:

```cpp
enum class StorageFileRole : uint8_t {
  kAuthoritativeFacts,
  kTransactionMetadata,
  kEngineInternal,
};

enum class StorageTableFormat : uint8_t {
  kCedarParquet,
  kBlockBased,
};

struct StorageFileInfo {
  std::string relative_filename;
  std::string column_family_name;
  StorageFileRole role;
  StorageTableFormat table_format;
  int level;
  uint64_t size_bytes;
  uint64_t smallest_seqno;
  uint64_t largest_seqno;
  std::string smallest_key_hex;
  std::string largest_key_hex;
};
```

`Database::InspectStorageFiles()` returns
`StatusOr<std::vector<StorageFileInfo>>`. It operates only on an open
`Database`, so it uses exactly the same validated Cedar profile and column
family configuration as the active database. It neither opens a second DB nor
exposes an engine handle.

The results are sorted deterministically by column family name, level, then
relative filename. Empty or unavailable key/sequence bounds remain empty/zero
rather than being fabricated.

## Source of Truth and Translation

`FactStore` obtains the current live SST list from RocksDB
`GetLiveFilesMetaData()`. This is the VersionSet's current view: an SST is
reported only when it is live in the manifest-backed database version.

Each RocksDB item is translated as follows:

| Column family | Cedar role | Table format |
| --- | --- | --- |
| `facts` | `kAuthoritativeFacts` | `kCedarParquet` |
| `meta` | `kTransactionMetadata` | `kBlockBased` |
| `default` | `kEngineInternal` | `kBlockBased` |

An unknown column family is an invariant error, rather than being silently
misclassified. Cedar's known three-column-family layout is part of the Kernel
storage contract.

The diagnostic output records encoded key bounds as lowercase hexadecimal.
This is lossless and safe for arbitrary binary internal keys; it does not
mistake them for user-facing temporal graph values.

## Command Frontend

Add the small `cedar` executable as a product tool, with one initial command:

```text
cedar files --path DB_PATH
cedar files --path DB_PATH --json
```

The command opens the database through `Database::Open`, invokes
`InspectStorageFiles`, prints a fixed human-readable table by default, and
prints a stable JSON document with a `files` array under `--json`. The command
returns a non-zero status for invalid arguments, database-open failure, or an
inspection invariant failure. It makes no writes and does not schedule Cedar
maintenance work.

The human output contains filename, CF, role, format, level, size, and
sequence range. Key ranges are available in JSON, keeping ordinary terminal
output compact and avoiding unhelpful binary key rendering.

Example:

```text
FILE        CF      ROLE                    FORMAT         LEVEL  SIZE    SEQ
000123.sst  facts   authoritative-facts     CedarParquet   L0     68 MiB  120..340
000124.sst  meta    transaction-metadata    BlockBased     L0     12 MiB  121..341
```

## Layering

`Database` delegates to `FactStore`; only `FactStore` knows its private
RocksDB DB instance. A focused implementation unit performs the Cedar-model
translation. The CLI depends only on public Cedar headers and `cedar_core`.

This gives future tools, embedded users, monitoring, and tests one stable
Cedar inspection API rather than each caller depending directly on a RocksDB
handle. RocksDB remains the owner of WAL, MemTables, SST lifecycle,
VersionSet, MANIFEST, and recovery.

## Error Handling

- A closed `FactStore` returns Cedar `InvalidArgument`, matching existing
  sampling methods.
- An unknown CF returns Cedar `InvariantViolation` and names the offending CF.
- Inspection does not inspect directory entries, so inaccessible obsolete
  files and temporary outputs cannot make the live report incorrect.
- The CLI reports Cedar `Status::ToString()` on stderr and writes no partial
  JSON document after an error.

## Tests

1. Unit-test translation of all three known CF names and rejection of an
   unknown name.
2. Integration-test an opened temporary Kernel database: write facts, trigger
   a Cedar-controlled flush, call `Database::InspectStorageFiles`, and assert
   a live `facts` file is classified as `CedarParquet` and no RocksDB type
   appears in the public contract.
3. Integration-test the CLI text and JSON output against that database;
   confirm deterministic ordering and valid JSON.
4. Run the existing Debug, ASAN, and recovery suites. Since the feature is
   observational, it must not change writes, maintenance, WAL count, or
   recovery results.

## Acceptance Criteria

- A user can identify every live SST's CF, Cedar role, and physical table
  format using `cedar files`.
- Facts are always shown as `authoritative-facts / CedarParquet`; metadata and
  default files are shown separately as Block-Based.
- Output is derived from the current RocksDB VersionSet and never inferred
  from an SST suffix.
- No RocksDB source file, filename suffix, MANIFEST format, or ownership
  boundary changes.
