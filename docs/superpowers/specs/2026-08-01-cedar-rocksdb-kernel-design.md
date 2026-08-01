# Cedar RocksDB Kernel Design

Date: 2026-08-01

Status: Proposed for final user review

## 1. Objective

Cedar becomes a trustworthy embedded bitemporal graph database kernel instead
of a complete research HTAP system. The first release prioritizes explicit
transactions, durable bitemporal facts, crash recovery, stable snapshots, and
a small C++ interface. T-Cypher, the optimizer, vector execution, benchmark
campaigns, and production telemetry move outside the kernel.

The durable store is standard RocksDB. Cedar does not modify RocksDB, implement
a custom `TableFactory`, or maintain a second canonical MemTable, WAL, Manifest,
or SST format. Cedar's columnar format remains a rebuildable analytical
projection over canonical RocksDB facts.

This is a clean break in both the C++ interface and disk format. Old Cedar
databases are identified and rejected without mutation.

## 2. Decisions

1. Pin RocksDB v11.1.2 and build it statically from a repository-owned source
   snapshot. Upgrade Cedar from C++17 to C++20, matching the dependency's
   supported build.
2. RocksDB owns the only WAL, mutable MemTable, immutable MemTable, SST,
   Manifest, compaction, cache, blob-file, and physical recovery path.
3. Cedar retains the bitemporal version-chain semantics as a canonical key
   ordering and resolver, not as a second data structure.
4. `commit_seq` is a Cedar logical sequence encoded in user keys. RocksDB's
   internal sequence and Snapshot are physical implementation details.
5. All writes use explicit `Transaction` objects. `Database::Put` and
   `Database::Delete` are not part of the new public interface.
6. Snapshot isolation is the default. Exact-key strict serializability is an
   explicit transaction option. Unsupported strict predicates return a typed
   error and never downgrade silently.
7. The public mutation model distinguishes entity existence from properties:
   `Assert`, `Retract`, `Set`, and `Unset`. Existence facts do not expose a fake
   schema or empty value.
8. The database allocates vertex and edge IDs from independent durable,
   monotonic, never-reused sequences. Allocation is outside transaction
   atomicity; gaps are valid.
9. An `edge_id` permanently binds source, target, and edge type. Edge state and
   properties reference only that ID. `EdgeOut` and `EdgeIn` are not canonical
   facts.
10. Edge visibility is the intersection of edge state and both endpoint vertex
    states. Vertex retraction does not cascade; reassertion can reveal an old
    still-valid edge.
11. Full bitemporal history is retained by default. Only explicit
    `Vacuum(commit_seq_boundary)` advances the earliest readable system
    snapshot and physically removes history.
12. Columnar segments, adjacency indexes, property indexes, and statistics are
    derived projections. They may lag, be deleted, or be rebuilt; correctness
    always falls back to canonical facts.

## 3. Module Structure

The kernel exposes four deep modules.

### 3.1 `Database`

`Database` owns open/close, schema registration, durable ID allocation,
transaction creation, snapshot creation, Vacuum, and optional maintenance. It
is an assembly root, not a coordinator containing query, benchmark, and
telemetry implementations.

```cpp
struct DatabaseOptions {
  std::string path;
  uint64_t write_buffer_bytes = 64ULL * 1024 * 1024;
  uint64_t block_cache_bytes = 256ULL * 1024 * 1024;
  uint64_t blob_threshold_bytes = 4096;
};

struct SnapshotOptions {
  std::optional<CommitSeq> as_of;  // empty means the latest visible sequence
};

class Database {
 public:
  static StatusOr<std::unique_ptr<Database>> Open(DatabaseOptions);
  Status Close();
  StatusOr<VertexId> AllocateVertexId();
  StatusOr<EdgeId> AllocateEdgeId();
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition);
  StatusOr<std::unique_ptr<Transaction>> BeginTransaction(
      TransactionOptions = {});
  StatusOr<Snapshot> BeginSnapshot(SnapshotOptions = {}) const;
  Status Vacuum(CommitSeq oldest_readable);
};
```

### 3.2 `Transaction`

`Transaction` captures one logical snapshot, accumulates mutations, records
strict read dependencies where requested, and produces one typed commit
result. A transaction is move-only and becomes terminal after commit or
rollback.

```cpp
enum class IsolationLevel : uint8_t { kSnapshot, kStrict };

class Transaction {
 public:
  StatusOr<bool> Exists(EntityFact, ValidTime);
  StatusOr<std::optional<Value>> Get(PropertyFact, ValidTime);
  Status Assert(EntityFact, ValidTime);
  Status Retract(EntityFact, ValidTime);
  Status Set(PropertyFact, ValidTime, Value);
  Status Unset(PropertyFact, ValidTime);
  StatusOr<CommitResult> Commit();
  Status Rollback();
};
```

Strict mode supports only exact `Exists` and `Get` reads. Range, adjacency, and
predicate reads return `UnsupportedSerializablePredicate`.

### 3.3 `Snapshot`

`Snapshot` is an RAII value containing one Cedar logical sequence, one pinned
RocksDB Snapshot, the corresponding schema view, and the observed retention
watermark. A caller cannot construct its fields or obtain a raw RocksDB
Snapshot. It provides read-only `Exists`, `Get`, and bounded fact-family `Scan`
operations.

### 3.4 `FactStore`

`FactStore` is the only module whose implementation includes RocksDB headers.
It hides key/value encodings, Column Families, snapshots, iterators, batching,
blob configuration, and engine status translation.

```cpp
class FactStore {
 public:
  Status Open();
  StatusOr<StoreSnapshot> BeginSnapshot() const;
  StatusOr<std::optional<FactEvent>> Read(
      const StoreSnapshot&, FactRef, ValidTime) const;
  Status Scan(const StoreSnapshot&, FactPrefix, FactVisitor) const;
  StatusOr<StoreCommitResult> Commit(StoreCommitBatch);
  StatusOr<IdLease> LeaseIds(IdKind, uint64_t count);
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition);
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      const StoreSnapshot&, PropertyId, uint32_t schema_epoch = 0) const;
  StatusOr<std::optional<EdgeIdentity>> LookupEdgeIdentity(
      const StoreSnapshot&, EdgeId) const;
  StatusOr<std::optional<StoreCommitResult>> ResolveTransaction(TxnId) const;
  Status Vacuum(CommitSeq oldest_readable);
};
```

Tests and callers cross this same seam. An in-memory adapter is permitted only
for focused domain tests; all durability and recovery tests use RocksDB.

## 4. Canonical Data Model

The canonical families are:

```text
VertexState(vertex_id)
VertexProperty(property_id, vertex_id)
EdgeIdentity(edge_id -> source_vertex_id, target_vertex_id, edge_type)
EdgeState(edge_id)
EdgeProperty(property_id, edge_id)
```

`EdgeIdentity` is immutable and non-temporal. Its first assertion and the first
`EdgeState` event are committed in the same RocksDB WriteBatch. Reasserting the
same binding is idempotent; any different binding is `IdentityConflict`.

Entity existence uses no property schema. Properties are registered definitions
with a stable `property_id`, name, entity kind, physical type, blob threshold,
and schema epoch history. `Transaction::Set` resolves the latest schema at the
transaction snapshot and records the exact epoch in the durable event.

## 5. RocksDB Layout

The database contains exactly two Cedar-owned Column Families.

### 5.1 `facts`

`facts` stores every immutable temporal fact. Properties do not receive their
own Column Families because user schema cardinality must not control engine
open time, compaction topology, or backup complexity.

Key codec v1 is fixed-width and bytewise ordered:

```text
format_version       u8
fact_family          u8
property_id          u16 big-endian; zero for state families
entity_identity      u64 big-endian; vertex_id or edge_id
valid_from_desc      bitwise-not(u64 big-endian valid_from)
commit_seq_desc      bitwise-not(u64 big-endian commit_seq)
```

The order is therefore:

```text
family, property_id, entity_id, valid_from DESC, commit_seq DESC
```

Key encoding never serializes native structs, host endianness, enum layout, or
variable-length user strings. Unknown format or family bytes are corruption.

Event value codec v1 is:

```text
value_format_version u8
operation            u8  // PUT or DELETE
schema_epoch         u32 big-endian; zero for entity state
value_kind           u8
payload_length       u32 big-endian
payload              bytes
crc32c               u32 big-endian
```

Logical deletes are durable Cedar DELETE events. Ordinary transactions never
call RocksDB `Delete` for facts. Only Vacuum physically deletes fact keys.

### 5.2 `meta`

`meta` stores versioned, checksummed records under stable byte prefixes:

```text
format/current
schema/<property_id>/<schema_epoch>
edge/<edge_id>
allocator/vertex_next
allocator/edge_next
txn/<txn_id>
sequence/<commit_seq>
watermark/visible
watermark/oldest_readable
vacuum/state
projection/<projection_id>
```

`sequence/<commit_seq>` stores the transaction ID, system HLC, and canonical
fact keys changed by that commit. It is the durable logical commit index and
incremental input for derived projections. Version 1 retains all sequence and
transaction-outcome records permanently. This makes outcome resolution and
contiguous reopen verification exact; a later format may add a separately
designed checkpoint index. Facts remain authoritative for data reads.

RocksDB integrated blob files are enabled for the `facts` Column Family. Cedar
does not maintain its current BlobStore, Blob Manifest, or Blob GC. Blob
placement is not part of Cedar event identity. Cedar configuration pins the
blob threshold and enables RocksDB blob garbage collection.

## 6. Snapshot and Read Semantics

`FactStore::BeginSnapshot` holds the short commit publication mutex while it:

1. reads the in-memory continuous `visible_seq`;
2. acquires a RocksDB Snapshot;
3. chooses the requested logical `as_of` sequence or the latest visible
   sequence and verifies it is within
   `[oldest_readable_seq, visible_seq]`;
4. captures the immutable schema view and `oldest_readable_seq`;
5. registers the chosen logical sequence in the active snapshot registry.

The lock prevents a commit from becoming physically visible between the
logical-sequence capture and physical Snapshot capture. The returned pair is a
consistent upper bound even though RocksDB uses its own internal sequence.

To read `(fact, valid_time)` at `snapshot_seq`, the resolver:

1. rejects `snapshot_seq < oldest_readable_seq` with `SnapshotExpired`;
2. seeks to the fact prefix and requested valid-time bound;
3. iterates only while the fixed fact prefix matches;
4. ignores events with `valid_from > valid_time`;
5. for the first eligible valid time, chooses the greatest
   `commit_seq <= snapshot_seq`;
6. applies PUT or DELETE.

All reads use the captured RocksDB Snapshot and still filter Cedar
`commit_seq`. RocksDB internal sequence is never exposed as application time.

Edge reads additionally resolve EdgeIdentity, EdgeState, source VertexState,
and target VertexState at the same Snapshot and valid time.

## 7. Transaction and Commit Protocol

Transactions have Cedar-generated persistent `txn_id` values. A commit request
is canonicalized by fact key and valid time; duplicate contradictory mutations
are rejected before validation.

### 7.1 Validation

Snapshot transactions validate only temporal write conflicts. For each pending
event Cedar derives the affected half-open valid-time interval from predecessor
and successor boundaries. A later committed event overlapping that interval
causes `Conflict`.

Strict transactions also retain exact observed event identity and predecessor/
successor fences. Commit revalidates those dependencies while holding the same
publisher mutex used through the durable WriteBatch. Because the first
implementation serializes validation and publication, no prepared reservation
index is required. If publication is later parallelized, reservations or an
equivalent validated-generation protocol must be designed and proven before the
mutex is relaxed.

### 7.2 Durable Commit

A serialized commit publisher performs:

1. validate against the latest published sequence while holding the publisher
   mutex;
2. allocate the next contiguous Cedar `commit_seq` and System HLC;
3. encode all fact events;
4. add immutable EdgeIdentity bindings if first asserted;
5. add `txn/<txn_id>`, `sequence/<commit_seq>`, and `watermark/visible`;
6. call one RocksDB `DB::Write` with one WriteBatch and `sync = true`;
7. only after success, publish the in-memory visible sequence and release the
   publisher mutex.

The default first implementation uses one publisher mutex. RocksDB's internal
write grouping may share physical syncs across concurrent writers, but Cedar
does not add a second commit log or acknowledge before synchronous durability.
If later benchmarks require a Cedar-side group queue, it must preserve the same
WriteBatch fact and outcome model.

The prior PREPARE/Decision protocol and the planned Cedar Atomic Commit log are
removed. One RocksDB WriteBatch recorded by the RocksDB WAL is the sole durable
transaction fact.

### 7.3 Outcomes and Ambiguity

`CommitResult` is one of `Committed`, `Aborted`, or `Indeterminate`. A definite
validation or pre-write failure is `Aborted`. A successful synchronous write is
`Committed`. Any engine result or injected failure for which Cedar cannot prove
absence of the batch becomes `Indeterminate`; Cedar enters `recovery_required`
and rejects further writes until reopen.

Reopen reads `watermark/visible`, verifies contiguous retained sequence records,
and rebuilds the live transaction-outcome cache. `Resolve(txn_id)` reports the
durable `txn/<txn_id>` record or absence. No post-WAL shard installation exists.

## 8. ID Allocation

Vertex and edge IDs use separate allocators. To avoid one fsync per ID, each
allocator durably leases ranges in `meta` with a synchronous WriteBatch, then
serves IDs from memory. The default range is 4096. Reopen starts after the end
of the last durable lease; unused IDs become gaps. IDs are never reused.

Allocation does not assert an entity and is not rolled back with a transaction.

## 9. Vacuum

All history is retained until an explicit `Vacuum(B)` call. `B` is a Cedar
commit sequence and becomes the earliest readable system snapshot. Vacuum does
not trim the valid-time axis.

Vacuum first verifies:

- `oldest_readable_seq <= B <= visible_seq`;
- no active Snapshot has `snapshot_seq < B`;
- no other Vacuum is active.

It then writes `vacuum/state = {target:B, phase:prepared}` and advances
`watermark/oldest_readable = B` synchronously before deleting data. This makes
the loss of old snapshot readability durable and monotonic. After that point a
crash resumes cleanup; it never restores the older boundary.

For every `(fact prefix, valid_from)`, Vacuum retains:

- the greatest `commit_seq <= B` as the baseline, if any;
- every version with `commit_seq > B`.

Older versions are deleted in bounded WriteBatches. When all facts are
processed, Vacuum writes a completed state. Version 1 does not delete sequence
or transaction-outcome metadata.
Logical reads remain correct throughout cleanup because the boundary is already
advanced and retained baselines cover every readable snapshot.

Vacuum never cancels readers. If an old Snapshot exists, the call returns
`SnapshotPinned`.

## 10. Columnar Analytical Projection

The optional projection module reads canonical facts and emits Cedar columnar
segments. It is outside `FactStore` durability and uses a separate directory.

A projection manifest records:

```text
projection format version
schema/property family
source sequence range
source checksum/fingerprint
segment identity and checksum
coverage watermark
```

The builder consumes `sequence/<commit_seq>` in order and fetches canonical
events at a pinned Snapshot. It may build row groups by property and identity
range using Cedar's typed codecs, zone maps, and vector-friendly pages.

Coverage is never inferred from file presence. A scan can use a projection only
when its manifest proves complete coverage for the requested schema, key range,
and snapshot. Otherwise it reads canonical RocksDB facts or combines a covered
prefix with a canonical suffix. Corruption deletes the projection and schedules
rebuild; it does not make the database recovery-required.
Vacuum invalidates projection coverage below the new oldest-readable sequence.
A projection that has fallen behind that boundary rebuilds from retained
baselines rather than replaying deleted fact versions.

Adjacency and secondary property indexes follow the same derived-artifact rule.
`EdgeOut` and `EdgeIn` may exist in an adjacency projection but never in
canonical facts.

## 11. Lifecycle and Maintenance

The lifecycle tracks only transactions, snapshots, commits, and maintenance.
Query-result registrations and T-Cypher cancellation are removed from the
kernel. Close stops new transactions, waits for active commit calls, waits for
or invalidates only explicitly configured maintenance, releases RocksDB
Snapshots, flushes as configured, and closes RocksDB.

Maintenance is limited to Vacuum, projection build/rebuild, optional RocksDB
manual compaction, checkpoint/export, and integrity verification. Cedar does
not duplicate RocksDB's cache, IO governor, pressure controller, or compaction
scheduler in the first kernel release. Resource limits map to RocksDB options
and bounded Cedar maintenance batches.

## 12. Error Mapping

`FactStore` converts RocksDB Status values to Cedar typed statuses. RocksDB
strings never become a caller contract. Required Cedar classes include:

```text
InvalidArgument
NotFound
Conflict
IdentityConflict
SchemaMismatch
SnapshotExpired
SnapshotPinned
Corruption
IOError
ResourceExhausted
ShutdownInProgress
RecoveryRequired
Indeterminate
UnsupportedSerializablePredicate
```

Corruption in canonical RocksDB state fails Open or marks the database
recovery-required. Corruption in a derived projection only disables and rebuilds
that projection.

## 13. Build and Distribution

RocksDB v11.1.2 is pinned as a repository dependency and built statically.
RocksDB tests, tools, benchmarks, JNI, and shared libraries are disabled. Cedar
controls the enabled compression libraries and exposes their exact versions in
its format/provenance report. No system RocksDB fallback is used in release
builds.

Only `cedar_core` and its public headers are installed. Query, benchmark, and
projection targets link to `cedar_core` but are not compiled into it.

## 14. Migration and Deletion

The implementation is staged so each boundary remains testable, but the final
format has one path only.

1. Add RocksDB and `FactStore` behind focused codec/store tests.
2. Add new `Database`, `Transaction`, Snapshot, fact, schema, and ID interfaces.
3. Route all kernel reads/writes through `FactStore`.
4. Add strict validation, outcomes, reopen, Vacuum, and fault tests.
5. Add the rebuildable columnar projection and canonical fallback.
6. Remove TransactionCoordinator's storage ownership, DecisionLog, shard WALs,
   TemporalMemTable, VersionSet, canonical Cedar SST, BlobStore, cache manager,
   governors, and query dependencies from the kernel.
7. Move T-Cypher, optimizer, observability, and benchmark code to optional
   targets or delete obsolete paths.
8. Replace the monolithic correctness test with focused test targets.
9. Reject old FORMAT versions read-only and publish the new format version.

There is no automatic migration or dual-write bridge. A future offline export/
import tool may read an old database with an old binary and write facts through
the new public interface.

## 15. Verification

Acceptance requires:

- key/value codec golden tests and malformed-input fuzz/property tests;
- exact bitemporal oracle equivalence before/after reopen and compaction;
- out-of-order valid-time writes and same-valid-time corrections;
- snapshot write conflicts and strict exact-read/write-skew tests;
- edge identity, endpoint intersection, retraction, and reassertion tests;
- multi-fact atomic WriteBatch crash tests at every injected boundary;
- indeterminate outcome resolution after reopen;
- durable ID lease tests with crash gaps and no reuse;
- active Snapshot/Vacuum exclusion and resumable Vacuum crash tests;
- projection absence, lag, corruption, rebuild, and canonical fallback tests;
- RocksDB format/options compatibility checks;
- Debug and Release CTest, ASAN, UBSAN, and TSAN;
- a clean source inventory proving PREPARE/Decision/CAC logs and the old
  canonical store are not in the production path;
- durable write and temporal point-read benchmarks compared with the current
  baseline, with performance reported rather than used to weaken durability.

## 16. Rejected Alternatives

### Continue the custom canonical Columnar LSM

This offers maximum format control but requires Cedar to own WAL, Manifest,
leveled compaction, crash recovery, caching, rate limiting, and space
reclamation. That conflicts with the chosen trustworthy-kernel priority.

### Keep a Cedar version-chain MemTable beside RocksDB

This duplicates mutable canonical state and creates two snapshot and flush
lifecycles. Recovery must rebuild one from the other. The version-chain semantic
is instead represented in RocksDB key ordering.

### Implement a custom RocksDB `TableFactory`

It can make canonical SSTs columnar, but it couples Cedar to lower-level table
reader/builder contracts and makes upgrades costly. It is not justified before
the standard adapter and projection path are measured.

### Use RocksDB user-defined timestamps immediately

This could map Cedar `commit_seq` to an engine timestamp, but it would couple
Cedar Vacuum and history semantics to RocksDB timestamp retention. The first
format keeps `commit_seq` explicitly in Cedar keys.

### Use one Column Family per property

User schema growth would create unbounded engine topology, open-time, and
compaction overhead. A single `facts` family keeps property grouping in the
ordered key.

### Preserve T-Cypher in the kernel

The current query runtime and session create circular dependencies through the
transaction coordinator. A future query module will consume Snapshot/Scan and
remain outside the kernel.
