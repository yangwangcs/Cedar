# Cedar Zone-Columnar SST and Content-Addressed Blob Design

Date: 2026-07-17

Status: Approved authoritative design; functional implementation substantially complete; release/paper closure remains incomplete and is tracked in `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

Depends on: `2026-07-17-cedar-htap-design.md`

## 1. Purpose

This document defines Cedar's second design stage: a clean-break Zone-Columnar SST and Blob storage layer. It evolves Cedar's original ideas rather than replacing them:

- entity-major temporal ordering;
- immutable Zone-Columnar SSTs;
- one logical property or edge type per SST partition;
- large-value separation through Blob storage;
- size-tiered compaction and low write amplification;
- bitemporal visibility based on `valid_from` and `commit_seq`.

The design replaces the current whole-file zone layout with independently readable, encoded, compressed, and checksummed pages. It also unifies the duplicate Blob implementations into a content-addressed, sharded, append-only Blob log.

This is a clean format and API break. No v1 reader, writer, Descriptor compatibility layer, or old-format migration path remains active.

## 2. Relationship to the Correctness Kernel

The correctness-kernel design remains authoritative for:

- transaction isolation and strict serializability;
- `valid_time`, `commit_seq`, `snapshot_seq`, and `visible_seq` semantics;
- sharded transaction WALs and the global DecisionLog;
- Manifest and VersionSet ownership;
- flush and compaction atomicity;
- snapshot pinning and file lifetime;
- crash recovery and corruption policy.

This document refines the physical representation and read path. Where this document introduces SST, Blob, schema, or page metadata, those files are published and retired only through the Manifest protocol already defined by the correctness kernel.

## 3. Current Problems Being Replaced

The current implementation has useful encoding experiments, but it does not yet form a reliable columnar storage format:

1. `FlushBlock()` accumulates encoded chunks in memory and `Finish()` concatenates them into whole-file zones. The blocks are not independent I/O or corruption units.
2. `ZoneColumnarSstReader::Open()` reads the complete SST, then copies zones into additional strings.
3. Zone maps are written from only the last flushed builder state, while restart points reuse the first file key rather than each block's first key.
4. Dictionary encoding is selected and then forced back to raw Descriptor values.
5. Compression and checksum fields exist in metadata without a complete write/read verification path.
6. Point and range reads can ignore edge target identity or reconstruct edge rows as vertex keys.
7. The format has a 16-bit sequence field but no durable 64-bit `commit_seq` column.
8. The flush path can discard tombstones before they reach SST.
9. The active per-SST Blob reader and writer disagree about the record-length boundary.
10. Blob write failures can become default tombstones instead of hard errors.
11. SST and companion Blob durability are not one atomic protocol.
12. Every value above a few bytes can consume a 4 KiB-aligned Blob record.
13. Multiple compiled Blob, column-codec, index, cache, and GC implementations have overlapping ownership.
14. There are no direct SST/Blob format, corruption, reopen, or real compaction tests.

SST and Blob replace these paths. They do not wrap or extend them in place.

## 4. Design Decisions

The following decisions are approved:

1. Entity and adjacency queries are the primary clustering workload.
2. Cedar uses one entity-major physical projection, not a duplicated time-major copy.
3. A granule is aligned to logical-key boundaries when possible.
4. One granule corresponds to one independently readable physical block.
5. Every block contains independently encoded and compressed column pages.
6. Every user column has a registered logical and physical type.
7. Fixed-width, medium variable-length, and large Blob values use separate placement tiers.
8. Large Blob identity is the full BLAKE3-256 content hash.
9. Blob payloads are stored in hash-sharded append-only immutable segments.
10. A physical Blob location in an SST is a non-authoritative hint.
11. SST compaction copies Blob references without reading or rewriting Blob payloads.
12. Old Descriptor APIs, old formats, and duplicate Blob paths are removed.

## 5. Goals and Non-Goals

### 5.1 Goals

- Point and entity-time-range reads perform bounded page I/O instead of loading whole SSTs.
- Analytical scans decode only predicate and projected pages.
- SST files preserve complete vertex and edge identity.
- SST pages carry full `valid_from`, `commit_seq`, and `PUT`/`DELETE` state.
- Page-local encoding and compression are selected from typed statistics.
- Large values do not get rewritten by ordinary SST compaction.
- Equal large values are physically deduplicated across the database.
- Blob relocation does not require rewriting immutable SSTs.
- All metadata, offsets, checksums, and codec identifiers are portable and versioned.
- Memory use is bounded by metadata, cache capacity, and maximum block size.

### 5.2 Non-Goals

- Backward-compatible disk or C++ APIs.
- An old-format migration utility.
- A second time-major physical projection.
- Wide relational rows with mandatory cross-property row alignment.
- Global dictionaries shared across SSTs.
- Leveled compaction.
- Content addressing for small and medium values.
- Persisting Arrow buffers as the SST file format.
- Online changes to Blob hash algorithm or hash-shard count.
- Historical value collection before a retention policy exists.

## 6. Core Types

### 6.1 Physical and Logical Types

The current format supports these physical types:

```text
BOOL
INT32
INT64
FLOAT32
FLOAT64
TIMESTAMP64
STRING
BINARY
```

Logical types may refine physical interpretation, but a page always has exactly one physical type. Graph-property absence is represented by a temporal `DELETE`, not by storing a null value.

### 6.2 Value

The new public and internal value contract is typed:

```text
Value {
  logical_type,
  physical_type,
  payload
}
```

`Descriptor`, `EntryKind`, `ExternalRef`, `EdgeRef`, and `DESCRIPTOR64` are not part of the current protocol.

### 6.3 Temporal Event

```text
TemporalEvent {
  logical_key,
  valid_from,
  commit_seq,
  operation: PUT | DELETE,
  value
}
```

The logical key contains complete vertex or edge identity. `commit_seq` replaces the old 16-bit sequence field.

### 6.4 Column Schema

```text
ColumnSchema {
  entity_type,
  column_id,
  schema_epoch,
  logical_type,
  physical_type,
  blob_threshold,
  encoding_policy,
  compression_policy
}
```

Every column must be registered in the persistent `SchemaRegistry`. A physical type change creates a new schema epoch. Type-mismatched writes fail with `SchemaMismatch`; no generic storage fallback exists.

Schema registration is a durable Manifest edit. The edit must be fsynced before a transaction may use the new epoch. A query spanning epochs performs only conversions explicitly registered as lossless or requested by an explicit cast; otherwise it fails with `SchemaMismatch`.

The current implementation stores the canonical schema catalog directly in
the clean-break `MSC1` VersionSet Manifest. `SchemaRegistry` is an in-memory
catalog installed from the published `VersionSnapshot`; it has no standalone
schema log. Registration proposes the next contiguous epoch and publishes it
with an `expected_generation` Manifest edit. A post-rename indeterminate result
from schema, index, flush, compaction, Blob or checkpoint publication gates
every database mutation until reopen. Manifest validation rejects duplicate or
non-contiguous identities and requires every live SST and index definition to
reference a registered epoch; an SST physical type must match that epoch, and
each index fragment must reference a property SST with the definition's exact
entity/column/epoch identity. Recovery checks a 256 MiB Manifest cap before
reading, preflights declared counts before reservation, and applies the same
cap while encoding. Schema publication admits the exact projected rewrite
through the ResourceGovernor and IoGovernor without consuming the completion
reserve for already-prepared transactions. No standalone schema catalog or
previous Manifest reader is retained.

## 7. Partitioning and Sort Order

An SST belongs to exactly one partition:

```text
(storage_shard_id, entity_type, column_id, schema_epoch)
```

Vertex, EdgeOut, and EdgeIn remain separate. Multiple property columns do not share a forced row spine.

Rows use entity-major order:

```text
entity_id
logical-key suffix / edge target
valid_from DESC
commit_seq DESC
```

This order keeps versions of a logical key adjacent and preserves efficient point, temporal, and adjacency access. Analytical scans use page pruning, selection vectors, and column projection rather than a second sorted copy.

## 8. SST File Layout

```text
FileHeader
GranuleBlock 0
GranuleBlock 1
...
GranuleBlock N
FileBloomAndStatistics
BlockIndex
Footer
```

### 8.1 File Header

The header contains:

- magic and format version;
- required and optional feature flags;
- storage shard, entity type, column ID, and schema epoch;
- logical and physical type IDs;
- sort-order ID;
- hash, encoding, compression, and checksum algorithm IDs;
- file identity.

The header is at the beginning of the file. The fixed-size footer is at the end and identifies every variable metadata region.

### 8.2 Portable Encoding

All offsets and lengths are 64-bit. Fixed-width integers use explicitly documented little-endian encoding. Builders do not persist C++ structs, native enums, padding, or host byte order.

Every format enum has a stable numeric ID and version. Unknown optional metadata can be skipped by length. Unknown required features cause `UnsupportedFormat`.

### 8.3 Footer

The footer contains:

- BlockIndex offset and length;
- file Bloom/statistics offset and length;
- exact BlobRefSet offset and length;
- row and block counts;
- file identity and format version;
- checksums for footer and metadata directories.

Data corruption is localized by page checksums. A metadata checksum protects the directories needed to find those pages.
Every footer range is validated with checked addition against the metadata
envelope before allocation or I/O. BlobRefSet, Bloom, statistics, and
BlockIndex regions have independent hard bounds; the fixed BlockIndex length is
derived exactly from the persisted block count.

## 9. Granule Block

One logical granule is one physical block:

```text
BlockHeader
EntityId Page(s)
TargetId Page(s)
ValidFrom Page(s)
CommitSeq Page(s)
Operation Page(s)
ValueClass Page(s)
TypedValue Page(s)
BlobRef Page(s)
PageDirectory
```

Block is the independent random-read, cache, and metadata-corruption unit. Pages are the independent encoding, compression, and data-corruption units.

### 9.1 Adaptive Boundaries

Default targets are:

```text
target_rows_per_block      = 8192
target_uncompressed_block  = 1 MiB
hard_max_block             = 4 MiB
target_compressed_page     = 64 KiB
hard_max_page              = 256 KiB
```

A builder closes a block after reaching the row or byte target at the next logical-key boundary. A single oversized version chain may cross the hard limit; continuation flags identify the split.

Supernodes may span multiple blocks at neighbor-key boundaries. Normal logical-key version chains remain within one block whenever possible.

### 9.2 Block Metadata

BlockHeader and BlockIndex record:

- first and last logical keys;
- minimum and maximum entity ID and target ID;
- minimum and maximum valid time and commit sequence;
- row count;
- continuation-before and continuation-after flags;
- page-directory location;
- a BLAKE3 block content commitment;
- typed value statistics;
- PUT, DELETE, inline-value, and Blob-reference counts.

## 10. Page Format

Every PageHeader contains:

```text
page_type
physical_type
encoding_id/version
compression_id/version
first_row
row_count
value_count
uncompressed_size
compressed_size
payload_crc32c
required_flags
```

PageDirectory entries contain page type, ordinal, 64-bit block-relative offset
and length, and the BLAKE3 hash of the complete encoded page. A logical column
may use several page fragments in one block; each fragment covers a continuous
row range. The block commitment hashes the BlockHeader and complete
PageDirectory, thereby committing the SST identity to every persisted page
hash without forcing point reads to fetch the whole block.

CRC32C covers the stored page payload and is verified before decompression. Readers validate decompressed size, row count, offsets, dictionary indexes, rank/select bounds, and maximum allocation before materialization.

## 11. Page Schema and Sparse Values

All system pages in one block share row numbers:

```text
row i:
  EntityId[i]
  TargetId[i]
  ValidFrom[i]
  CommitSeq[i]
  Operation[i]
  ValueClass[i]
```

Entity type, column ID, schema epoch, and physical type are file constants. TargetId pages may be omitted for a partition in which all target values are absent.

Value storage is dense by class:

```text
Operation Page
ValueClass Page: NONE | INLINE | BLOB
InlinePresence Bitmap
BlobPresence Bitmap
TypedValue Page for INLINE rows
BlobRef Page for BLOB rows
```

Bitmap rank maps a block row to its dense typed-value or Blob-reference index. DELETE rows consume no value slot.

## 12. Three-Tier Value Placement

### 12.1 Fixed-Width Values

Bool, integer, float, and timestamp values stay typed through API, WAL, MemTable, and SST page encoding.

### 12.2 Medium Variable-Length Values

String and binary values at or below the column's Blob threshold are stored in a shard-local immutable ValueArena. The WAL persists the complete payload. Flush writes those values to offset/length and byte-data pages.

The default Blob threshold is 4 KiB. It is configurable per schema epoch and does not change inside an epoch.

### 12.3 Large Values

Values above the threshold enter BlobStore before transaction prepare. The transaction WAL and MemTable retain only a durable `BlobRef`.

```text
BlobRef {
  content_hash[32],
  raw_length,
  hint_segment_id,
  hint_offset
}
```

The full BLAKE3-256 hash is authoritative. The physical location is only a read optimization.

Frozen MemTables own their ValueArena until their SST and Manifest edit are durable.

## 13. Page Encoding and Compression

Encoding is selected per page from typed statistics:

| Page/type | Candidate encodings |
|---|---|
| Entity ID | RLE, delta, frame-of-reference, bit packing |
| Target ID | entity-reset delta, frame-of-reference, bit packing |
| Valid time | logical-key-reset signed delta or delta-of-delta |
| Commit sequence | frame-of-reference and bit packing |
| Operation/value class | bitmap or RLE |
| Integer/time value | delta, frame-of-reference, bit packing |
| Float value | raw or XOR |
| Bool value | bitmap |
| String value | dictionary or plain offsets and bytes |
| Binary value | plain offsets and bytes |
| Blob hash | raw full hash |
| Blob location hint | segment RLE and offset delta |

Commit sequence encoding must not assume it is ordered with valid time. Out-of-order temporal writes make those dimensions independent.

Compression is applied after encoding:

- hot and L0 system pages prefer none or LZ4;
- colder levels and variable-length value pages may use Zstd;
- compressed output is rejected when it saves less than approximately 12.5%;
- dictionary scope is page or block, never global across SSTs;
- codec availability and version are explicit format requirements.

Existing RLE, delta-of-delta, bitmap, compression, and CRC32C code may be migrated only behind the new page-codec interface and after direct round-trip and corruption tests.

## 14. File, Block, and Page Indexes

### 14.1 File-Level Filtering

Each SST stores:

- full logical-key range;
- valid-time and commit-sequence ranges;
- a Bloom filter over complete logical keys, including edge target identity;
- file-level typed statistics;
- exact sorted unique BlobRefSet metadata.

BlobRefSet is stored as exact, sorted, prefix-compressed hash metadata. A Bloom filter is not sufficient because GC requires exact liveness.

The old entity-only Bloom filter is not retained.

### 14.2 Block Index

BlockIndex entries are fixed-size, sorted by first logical key, and persist the
row count plus block content commitment. Point and entity-range readers
binary-search this index and follow continuation flags when a logical key or
entity spans blocks.

Typed min/max, dictionary membership, optional string Bloom, valid-time range, and commit-sequence range support block pruning. A block with `min_commit_seq > snapshot_seq` is invisible to that snapshot and can be skipped.

### 14.3 No Full Position Index

The reader does not build an unbounded `entity_id -> all row positions` hash map. Entity-major order and BlockIndex marks provide bounded lookup without duplicating every row position in memory.

## 15. Read Path

Opening an SST reads only:

```text
FileHeader
Footer
BlockIndex
file Bloom/statistics
```

No data page is read at open.

Open verifies the checksummed metadata regions and recomputes the file identity
root from the persisted block commitments. Full reads verify the block
commitment and every page hash. Selective reads verify the BlockHeader plus
PageDirectory commitment and only the encoded pages they consume. Metadata,
page, and whole-block cache keys include the file identity, so path reuse or a
same-size replacement cannot return bytes owned by another SST.

### 15.1 Point and Temporal Read

```text
VersionSet selects relevant SSTs
file range/Bloom pruning
BlockIndex locates candidate blocks
reader fetches system pages
valid_time and snapshot_seq produce candidate rows
reader fetches selected typed-value or BlobRef pages
BlobStore reads only surviving projected Blob values
TemporalReadMerger resolves MemTable and all SST candidates
```

An individual SstReader returns candidate temporal events. It does not claim to produce a final value across files. `TemporalReadMerger` applies the correctness-kernel visibility rule across active/frozen MemTables and every relevant SST.

### 15.2 Range and Adjacency Read

Entity time-range and adjacency reads decode Entity, Target, ValidFrom, CommitSeq, and Operation pages first. They construct a selection vector, then decode only projected values.

Edge rows preserve source, target, direction, edge type, valid time, commit sequence, and operation. No path reconstructs an edge as a vertex.

### 15.3 Analytical Scan

Analytical scans use block statistics and dictionaries for pruning, decode predicate columns first, and materialize projected columns after filtering. Arrow-compatible typed batches may be used as an execution exchange format, but they are not persisted directly.

## 16. I/O and Cache Ownership

The database instance owns three bounded caches:

- `MetadataCache`: file headers, footers, BlockIndexes, and file filters;
- `PageCache`: immutable decoded or encoded pages keyed by file/block/page identity;
- `BlobLocationCache`: content hash to current physical location.

An optional `BlobValueCache` stores only admitted hot values by hash. Sequential scans do not automatically pollute point-read caches.

PageCache keys include:

```text
(file_id, block_id, page_type, page_ordinal, format_version)
```

The reader uses `RandomAccessFile`/`pread`. Adjacent requested pages inside one block may be coalesced into one physical read. Cache misses never fall back to whole-file loading.

Global singleton caches are removed. Reader handles and metadata remain tied to VersionSet file lifetime.

## 17. Content-Addressed Sharded Blob Store

### 17.1 Logical and Physical Organization

Blob shards are chosen by a persisted prefix of BLAKE3-256. Hash-shard count and hash algorithm are fixed at database creation.

Each Blob shard contains:

```text
Active BlobSegment
Sealed BlobSegments
BlobHashIndex MemTable
BlobIndexDeltaLog
Immutable BlobHashIndex checkpoints
per-hash in-flight reservations
```

Blob shards are independent of StorageShards. This enables database-wide deduplication without a single global append lock.

### 17.2 Blob Record

```text
BlobRecord {
  content_hash[32],
  raw_length,
  stored_length,
  codec,
  payload_crc32c,
  compressed_payload
}
```

Several records are packed into Blob blocks. Records are not individually padded to 4 KiB. Blob blocks and segment indexes define random-read boundaries.

Default targets are 1 MiB per Blob block and 256 MiB per Blob segment. A record larger than the block target receives a dedicated oversized block. Every offset and length is 64-bit.

The current clean-break implementation persists numeric Blob block format 1
with `CBB1` magic. Its fixed 88-byte header owns the 64-bit block length,
record count, directory offset and directory length, plus directory CRC32C,
BLAKE3 block identity and header CRC32C. Each fixed directory entry binds the
full content hash to 64-bit record offset/length and raw/stored length, codec
and payload checksum. A durable location hint points to the block start; the
full content hash selects the record from the verified directory. Transaction
Blob batches and GC relocation use the same block packer and group-fsync path.

The content hash is computed over canonical uncompressed bytes. Compression changes do not change identity.

BLAKE3 comes from a vetted upstream implementation. Cedar does not implement its own cryptographic hash.

## 18. Blob Write and Deduplication Protocol

```text
compute BLAKE3-256 over raw payload
route to hash shard
query BlobHashIndex
on hit, reuse durable record
on miss, install per-hash reservation
encode and append BlobRecord
group-fsync BlobSegment
append and fsync BlobIndexDeltaLog
publish in-memory hash mapping
return DurableBlobRef
```

Only the reservation owner writes a missing hash. Other writers wait for or reuse that result.

An active Blob segment is append-only and must already be Manifest-live before any durable index entry can reference it. Rotation seals the old segment, publishes a new active segment through Manifest, and only then accepts appends to the new identity.

A transaction's shard PREPARE waits for every `DurableBlobRef`. Therefore an acknowledged commit cannot reference an unflushed Blob record.

Crash cases are deterministic:

- an unflushed Blob record is invisible;
- a durable record without a durable index delta is an orphan and is safe;
- a durable index delta may reference only an already durable record;
- a durable transaction commit must resolve every Blob hash by a valid hint or BlobHashIndex mapping.

Aborted transactions may leave orphan records. They are not visible and are reclaimed by Blob GC.

## 19. Blob Read and Integrity

Reader behavior is:

1. Try `location_hint`.
2. Read and validate record header and stored payload CRC32C.
3. Decompress with bounded output size.
4. Verify BLAKE3-256 over raw output.
5. If hint is absent, stale, or points to a different hash, query BlobHashIndex and retry.

A stale location hint is legal. The full hash remains the identity. A hash that cannot be resolved by either hint or index is corruption, never an empty value.

## 20. Blob Reference Catalog

Every SST stores an exact sorted unique BlobRefSet. It is metadata for recovery and GC, not a query index.

`BlobReferenceCatalog` accounts for:

- committed WAL/MemTable references not yet checkpointed into SSTs;
- SSTs in the current VersionSet;
- retired SSTs still pinned by old snapshots;
- flush and compaction outputs not yet published.

The catalog is rebuildable from WAL, Manifest, VersionSets, and SST BlobRefSets. An ad hoc reference-count file is not a source of truth.

Flush transfers protection from committed WAL/MemTable state to the Manifest-published SST without a gap. Compaction registers outputs before retiring inputs. References from retired inputs remain protected until the last pinned VersionSet releases those files.

## 21. SST Compaction and Blob Behavior

Ordinary SST compaction:

```text
read BlobRef
copy content hash and optional location hint
do not read Blob payload
do not decompress Blob payload
do not rewrite Blob payload
```

This rule preserves Cedar's low-write-amplification purpose for large values. Location hints may become stale after Blob GC and remain correct because readers fall back to BlobHashIndex.

Compaction still obeys the correctness-kernel rules for shard/entity/column partitioning, complete overlap closure, tombstone retention, output durability, one atomic Manifest edit, and snapshot-pinned input lifetime.

## 22. Blob Garbage Collection

Only sealed segments are GC candidates. Active segments are never relocated.

GC computes live bytes from BlobReferenceCatalog and BlobHashIndex. It selects sufficiently old segments with low live-byte ratio, then performs:

```text
copy referenced BlobRecords into a new segment
fsync and Manifest-add the new segment
CAS hash mappings from old location to new location
fsync BlobIndex delta
write index tombstones for still-unreferenced hashes
Manifest-retire old segment
wait for Blob reader epochs
physically delete old segment
```

The index CAS prevents relocation from overwriting a newer mapping. Hash reservations and a final reference recheck prevent GC from deleting a record concurrently reused by a new transaction.

BlobHashIndex must not point to a new segment before that segment is Manifest-live. An old segment remains physically available while any reader has pinned it. Time-delay-only deletion is not a correctness mechanism and is removed.

The current no-history-GC policy means first-stage Blob GC primarily removes aborted transaction orphans and duplicate physical records. Historical Blob reclamation follows the future unified retention policy.

## 23. Manifest and File Lifecycle

Manifest owns:

- live SST files;
- sealed and active Blob segment identities;
- BlobHashIndex checkpoint files;
- index-delta safe positions;
- schema registry versions;
- retired files awaiting snapshot or reader release.

SST flush follows:

```text
write and checksum pages/blocks
write BlockIndex and footer
fsync temporary SST
rename and fsync directory
Manifest AddSst and fsync
release FrozenMemTable
```

Large-value Blob records already satisfy their independent durability protocol before transaction prepare. SST flush persists references; it does not create Blob payloads.

Blob segment and BlobHashIndex checkpoint creation use the same temporary-file, fsync, rename, directory-fsync, and Manifest-publication discipline.

A BlobIndexDeltaLog prefix can be removed only after an immutable checkpoint covering that prefix is fsynced, renamed, and made Manifest-live.

Manifest-referenced missing or invalid files are corruption. Complete unreferenced outputs are orphans and may be deleted during recovery.

## 24. Recovery

After correctness-kernel transaction and Manifest recovery, columnar recovery:

1. Loads SchemaRegistry epochs referenced by live SSTs.
2. Verifies live SST headers, footers, metadata directories, and file identities.
3. Loads BlobHashIndex checkpoints and replays retained delta logs.
4. Verifies index locations reference Manifest-live Blob segments.
5. Rebuilds BlobReferenceCatalog from committed WAL state and live/pinned SST metadata.
6. Reconciles durable Blob records absent from the index as safe orphans.
7. Rejects any committed BlobRef that cannot resolve to verified content.
8. Removes unreferenced temporary or orphan output files when safe.

Recovery does not scan or decode every SST data page. Pages are verified lazily when read, with optional paranoid full verification as an administrative operation.

## 25. Public API

The new API is typed and schema-driven:

```text
RegisterColumn(ColumnSchema)

txn.Put(logical_key, valid_from, Value)
txn.Delete(logical_key, valid_from)
txn.Commit()

Get(snapshot, logical_key, valid_time)
Scan(snapshot, predicate, projection)
Expand(snapshot, vertex_id, edge_type, valid_time)
```

Old `Put(..., Descriptor)`, Descriptor-returning getters, and generic format switches are removed rather than wrapped.

Strong-transaction restrictions remain those in the correctness-kernel design: exact-key operations may be strict-serializable; range scans and adjacency expansion use snapshot-isolated analytical snapshots.

## 26. Module Boundaries

The intended ownership is:

```text
types/
  value

schema/
  schema_registry

columnar/
  page_format
  page_codec
  granule_builder
  block_index
  sst_builder
  sst_reader
  temporal_read_merger
  page_cache

blob/
  blob_store
  blob_segment
  blob_hash_index
  blob_reference_catalog
  blob_gc

storage/
  manifest/version_set
  transaction/WAL
  memtable/value_arena
```

Dependency rules:

- SstBuilder receives typed events and already durable BlobRefs; it does not call BlobStore.
- BlobStore does not depend on SST implementation.
- SstReader handles one file and returns candidate events.
- TemporalReadMerger owns cross-MemTable/SST visibility resolution.
- Manifest owns every durable file lifecycle.
- caches are per database instance and do not own file lifetime.

## 27. Legacy Removal

The implementation removes or replaces all callable v1 paths:

- Descriptor and its serialization protocol;
- v1 ZoneColumnar builder, reader, and iterator;
- SimpleSSTBlobManager;
- both old BlobFileWriter/BlobFileManager ownership paths;
- AutoBlobStorage;
- delay-based BlobGCManager;
- whole-file zone caches and global BlockCacheManager;
- disabled duplicate entity-index and column-codec paths;
- `use_zone_columnar_format` and legacy Frond runtime switches.

Algorithms may move into current codecs only after direct tests. Old classes do not remain as hidden fallback implementations.

## 28. Relationship to Mainstream Designs

| System/design | Cedar adopts | Cedar keeps different |
|---|---|---|
| ClickHouse | marks, granule statistics, immutable publication, selected-column reads | Block equals granule; storage remains a bitemporal LSM |
| Parquet | row-group/page hierarchy, page-local codecs, dictionaries, statistics, checksums | SST participates in transaction visibility and compaction |
| ORC | stream indexes, typed streams, selective reads, Bloom filtering | properties do not share a mandatory relational row spine |
| DuckDB | typed batches, selection vectors, late materialization, bounded caching | persistence remains immutable SST plus Blob segments |
| RocksDB/Titan/WiscKey | value separation, reference-copy compaction, Blob GC | full content hash is the stable Blob identity |
| Kudu/C-Store | mutable delta/memory layer plus stable columns | Cedar retains immutable temporal events and implicit valid intervals |
| Arrow | typed in-memory exchange and bitmap conventions | Arrow is not the disk format |

The design adopts mature physical and execution principles without changing Cedar's research contribution.

## 29. Error Semantics

Errors are explicit and contextual:

- malformed page metadata or checksum failure: `Corruption(file, block, page)`;
- missing or invalid Blob content: `BlobCorruption(hash)`;
- schema/type mismatch: `SchemaMismatch`;
- unknown required feature or codec: `UnsupportedFormat`;
- bounded-cache miss: normal uncached page read;
- allocation beyond configured bounds: resource-limit error, never unchecked allocation;
- partially written file outside Manifest: orphan, never readable;
- referenced missing file: corruption.

No path converts an error into tombstone, empty bytes, default value, or `nullopt`.

## 30. Verification and Acceptance Criteria

### 30.1 Format and Codec Tests

- Golden-byte tests for FileHeader, Footer, BlockHeader, PageDirectory, PageHeader, BlobRecord, and BlobIndex records.
- Random round trips for every physical type, encoding, and compression combination.
- Cross-page and continuation-boundary tests.
- Truncation, bit flip, bad size, decompression bomb, invalid offset, bad dictionary index, and checksum tests.
- Explicit little-endian and 64-bit offset tests.

### 30.2 Granule and SST Tests

- Normal version chains remain in one block.
- Oversized chains and supernodes produce correct continuation metadata.
- Vertex, EdgeOut, and EdgeIn identities round-trip exactly.
- Valid time, commit sequence, PUT, and DELETE produce identical results before and after flush, compaction, and reopen.
- Typed predicates and projections read only required pages.
- More than ten SSTs and multiple schema epochs return complete results.

### 30.3 Blob Tests

- Concurrent equal-content writes produce one authoritative hash mapping.
- Distinct values do not deduplicate.
- Medium values remain in SST pages; large values enter BlobStore.
- Blob record, index delta, and transaction prepare crash boundaries are fault-injected.
- Stale hints fall back to BlobHashIndex.
- CRC and BLAKE3 corruption are both detected.
- GC relocation remains correct under concurrent reads, writes, and snapshot pins.
- Aborted transaction orphans become reclaimable.

### 30.4 Compaction and Resource Tests

- SST compaction copies BlobRefs without reading Blob payload bytes.
- Reader memory remains bounded by metadata, cache capacity, and maximum block size.
- Compaction memory remains bounded by input stream count and one output block.
- SST open reads only metadata and BlockIndex.
- Sequential scans do not force whole-file residency or permanently pollute point caches.

### 30.5 End-to-End and Tooling

- Reference-model comparison across random bitemporal event histories.
- Crash/reopen at every SST, Blob, index, Manifest, and file-deletion persistence boundary.
- ASan, UBSan, and TSan runs.
- Static scan proving no callable Descriptor, v1 reader/writer, old Blob path, or dual-format switch remains.

### 30.6 Performance Observability

The engine reports:

```text
bytes read per point/range query
pages decoded and skipped
metadata/page/Blob cache hit rate
Blob deduplicated byte ratio
Blob index lookup latency
compression ratio by page type
GC rewritten bytes and live bytes
Blob payload bytes read by SST compaction
```

Correctness and bounded-resource behavior are release gates. Hardware-specific throughput targets are set only after a reproducible benchmark profile exists.

## 31. Implementation Dependency Order

The later implementation plan must preserve this dependency order:

1. typed Value, LogicalKey, and persistent SchemaRegistry;
2. Blob segment, BlobHashIndex, durable BlobRef, and direct Blob tests;
3. page format and codec registry;
4. granule/block builder and SST format tests;
5. SST reader, indexes, cache, and TemporalReadMerger;
6. WAL/MemTable/flush integration with the correctness kernel;
7. compaction and Blob reference catalog;
8. Blob GC and crash injection;
9. public typed API migration and removal of all v1 code.

No compatibility shim is inserted to change this order.

## 32. Completion Definition

Columnar is complete only when:

1. all durable data uses typed SST pages or content-addressed Blob records;
2. no normal read opens or copies a whole SST;
3. bitemporal point, range, and adjacency reads preserve full key identity;
4. page and Blob corruption are detected and reported explicitly;
5. SST compaction performs zero Blob payload reads in the reference-copy path;
6. Blob relocation leaves immutable SST references valid;
7. caches and compaction memory obey configured bounds;
8. crash/reopen and concurrency tests pass;
9. Descriptor, v1 format, duplicate Blob ownership, and format switches are removed;
10. the approved correctness-kernel transaction and snapshot semantics remain intact.
