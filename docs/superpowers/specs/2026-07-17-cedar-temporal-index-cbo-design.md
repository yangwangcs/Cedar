# Cedar Temporal Index Catalog and Cost-Based Optimizer Design

Date: 2026-07-17

Status: Approved authoritative design; functional implementation substantially complete; release/paper closure remains incomplete and is tracked in `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`

Depends on:

- `2026-07-17-cedar-htap-design.md`
- `2026-07-17-cedar-columnar-design.md`
- `2026-07-17-cedar-tcypher-vectorized-execution-design.md`

## 1. Purpose

This document defines Cedar's fourth design stage: one manifest-owned temporal index catalog, immutable per-SST index sidecars, mergeable statistics, and a budgeted cost-based optimizer for Cedar T-Cypher.

The stage fills the gap deliberately left by T-Cypher V1. V1 can use SST and Block statistics and deterministic rules, but it has no persistent logical index definition, no selective property access path, and no cost model for choosing between a base scan, an index candidate scan, an adjacency expansion, and their combinations.

The design keeps Cedar's research purpose intact:

- immutable temporal events remain the source of truth;
- valid-time intervals remain implicit and are never materialized as mutable index state;
- `commit_seq` and the HLC CommitTimeline remain the visibility authority;
- SSTs remain entity-major, column-partitioned, and size-tiered;
- a secondary index is an optional candidate accelerator, not a correctness dependency;
- no second global secondary LSM and no time-major data projection are introduced.

## 2. Relationship to Earlier Designs

### 2.1 Authoritative Contracts

The correctness kernel owns:

- transaction isolation, OCC, shard WALs, DecisionLog, and the visible commit prefix;
- Manifest and VersionSet publication;
- snapshot pinning and file lifetime;
- flush, compaction, recovery, and tombstone retention.

Columnar owns:

- SST format, Block/Page layout, typed schema epochs, and file identities;
- file and Block Bloom filters, BlockIndex, zone maps, and typed page statistics;
- BlobHashIndex, BlobRefSet, Blob GC, and the rule that Blob hashes are authoritative.

T-Cypher V1 owns:

- the AST, binding, temporal scopes, demand analysis, and vectorized execution contracts;
- `QuerySnapshot`, `ColumnBatch`, `PropertyGather`, `VectorExpand`, and pipeline scheduling;
- the rule that strict transactions reject predicate scans and graph expansion.

This stage adds a logical property-index catalog and optimizer. It does not replace primary SST metadata, BlobHashIndex, or CommitTimeline with a second index ownership model.

### 2.2 Normative Refinements

1. A secondary index stores immutable `PUT` event candidates. It does not store derived `valid_to` or claim a final visible value.
2. Index sidecars are bound to immutable source SST identities and are published, retired, and snapshot-pinned through Manifest and VersionSet.
3. Statistics are immutable per-file fragments and rebuildable VersionSet snapshots. They are estimates, not a source of truth.
4. A missing, stale, or corrupt secondary sidecar may force a base scan, but it may never cause a false negative.

## 3. Current Index and Planning Problems

The current repository contains multiple unrelated experiments rather than one index subsystem:

1. `legacy schema header` exposes an `IndexType` enum and template flags, but no durable index definition or query contract consumes them.
2. `VersionChainIndex` and `AsyncIndexBuilder` build per-entity version-chain skip structures, not property predicate indexes.
3. `TemporalBloomFilter` and `SSTTemporalFilter` have overlapping ownership and are not tied to Manifest or the new visible-prefix snapshot.
4. The old `ZoneColumnar` Bloom and zone maps are whole-file experiments, not the portable SST sidecar format.
5. Asynchronous index tasks reference raw MemTable version nodes and can outlive the state they describe; there is no immutable source identity or crash protocol.
6. Existing index settings are runtime toggles rather than SchemaRegistry or IndexCatalog state.
7. The current planner has no statistics provider, cost budget, or alternative physical access paths.
8. There is no plan-level accounting for index coverage gaps, base validation cost, temporal version churn, or Blob avoidance.
9. Existing structures do not establish the required no-false-negative property under out-of-order valid-time writes and compaction.
10. There are no direct tests for index sidecar corruption, partial coverage, snapshot pinning, or plan/result equivalence with and without an index.

The new design removes these legacy index paths rather than composing them behind additional switches.

## 4. Goals and Non-Goals

### 4.1 Goals

1. Support typed equality, `IN`, ordered range, and binary-prefix predicates on indexed scalar and medium variable-length properties.
2. Support exact equality on large Blob values through their BLAKE3-256 identity without reading Blob payloads.
3. Preserve all historical `PUT` candidates needed by arbitrary valid-time and system-time reads.
4. Keep all index fragments aligned with immutable SST publication and compaction.
5. Allow partial index coverage and hybrid index/base execution without changing results.
6. Choose among base scans, index candidates, index intersections, joins, gathers, and graph expansion using bounded cost search.
7. Estimate temporal selectivity, event churn, tombstones, interval fragmentation, and Blob avoidance.
8. Keep statistics mergeable, rebuildable, versioned, and independent from correctness.
9. Reoptimize safely when data, index coverage, schema epochs, parameters, or runtime observations change.
10. Expose explainable access-path and cost decisions in `EXPLAIN` and `EXPLAIN ANALYZE`.
11. Limit index maintenance resources so ingestion, WAL, flush, and compaction retain priority.

### 4.2 Non-Goals

This stage does not add:

- a global secondary LSM per indexed property;
- composite indexes in V1;
- full-text, regular-expression, linguistic-collation, spatial, or vector indexes;
- a `VALID_TIME` time-major projection or a global temporal event index;
- materialized visible-state intervals in an index;
- a user-facing `CREATE INDEX` T-Cypher clause in `CEDAR_TCypher_V1`;
- strict-serializable predicate queries;
- a generic unbounded Cascades optimizer or unrestricted plan enumeration;
- a statistics value used as a correctness condition;
- a migration or compatibility path for the old index implementations.

Multiple single-column indexes can be intersected by the executor. Composite index design is deferred until real workloads demonstrate that intersection is insufficient.

## 5. Design Alternatives and Selection

Three families were considered:

### 5.1 Global Secondary LSM

Each indexed property would have a second LSM with its own MemTables, WAL, compaction, Manifest edits, and recovery. It would reduce the number of fragments a query probes, but every event would be written and compacted twice. Coordinating the base SST and the secondary LSM would add a second cross-component atomic protocol and materially increase write amplification.

This conflicts with Cedar's low-write-amplification and immutable-event purpose.

### 5.2 SST-Aligned Sidecars plus Budgeted CBO

Each immutable SST has an optional immutable sidecar. The sidecar stores candidate postings over that SST's `PUT` events. MemTables have rebuildable delta indexes. The IndexCatalog describes definitions and coverage, while a CBO chooses which fragments and base paths to use.

This keeps publication, compaction, and snapshots aligned with the existing VersionSet. It increases read-side fragment probing, but the morsel scheduler can probe fragments in parallel and the planner can choose a base scan when candidate density is high.

This is the selected design.

### 5.3 Scan-Only Adaptive Execution

The engine would keep only file and Block statistics, then use runtime filtering and adaptive scans. This has the smallest write cost, but selective equality and range predicates still read too many Blocks, and historical property filters have no direct candidate path.

It is retained as the fallback path and as the baseline for index benchmarks, not as the complete design.

## 6. Temporal Index Semantics

### 6.1 Event Candidates, Not State Intervals

For a property logical key, the source event stream is:

```text
PUT(value=v, valid_from=t, commit_seq=s)
DELETE(valid_from=t2, commit_seq=s2)
```

The index stores the `PUT` candidate `(v, logical_key, t, s)`. It does not store the interval end. A later event, including a `DELETE`, may make that candidate invisible at a requested valid/system snapshot.

An index lookup therefore has two phases:

```text
Index candidate lookup
  -> base TemporalReadMerger
  -> successor/DELETE/commit visibility validation
  -> demanded property and graph interval intersection
```

A query never trusts an index candidate as a final value. This permits out-of-order valid-time inserts without rewriting an older index entry.

### 6.2 Candidate Completeness Invariant

For every committed indexed `PUT` event whose column schema and canonical value match a predicate, at least one live source SST, MemTable, or FrozenMemTable candidate path covers it, either through an index candidate or through the query's base scan for that source. Flush overlap may expose the same logical event in more than one physical source temporarily; the temporal merger must deduplicate it by logical identity.

The index may return false positives because an event can be shadowed by a later value or delete. It may not omit a matching `PUT` event. Candidate completeness is verified against an independent event scan in tests.

### 6.3 Temporal Predicate Mapping

- `AS OF valid_time` uses value candidates, then selects the maximum visible valid event at or before the point.
- `BETWEEN valid_time` uses matching candidates plus predecessor and successor fences to reconstruct actual implicit intervals.
- `CHANGES FOR VALID_TIME` can use the index to find matching value events in the valid range.
- `SYSTEM_TIME AS OF` prunes postings above the resolved `snapshot_seq` and then performs base validation.
- `CHANGES FOR SYSTEM_TIME` can use an index when the event also has an indexed value predicate; the HLC range remains a filter on commit metadata.

An index cannot accelerate a pure valid-time scan without a value predicate. The entity-major Block and valid-time metadata remain the intended path for that workload.

### 6.4 Logical Identity

The logical index identity is:

```text
(index_id, logical_key, valid_from, commit_seq)
```

A physical posting may store only a source-local row ordinal because its sidecar names an immutable `source_sst_id`. The executor resolves that ordinal to the full system row and verifies the logical identity. No physical offset becomes a durable identity.

## 7. Index Definition and Catalog

### 7.1 IndexDefinition

The durable logical definition is:

```text
IndexDefinition {
  index_id,
  entity_type,
  column_id,
  schema_epoch,
  capabilities: EQUALITY | ORDERED_RANGE | PREFIX,
  physical_policy: ADAPTIVE,
  blob_policy: HASH_EQUALITY_ONLY,
  canonical_encoding_id,
  state,
  generation,
  definition_checksum
}
```

V1 supports one property column per definition. Entity existence, label, relationship type, source, target, and direction are already governed by the SST partition and primary adjacency layout. They do not create redundant property indexes.

The definition is registered through an administrative storage API, not a V1 T-Cypher clause:

```text
RegisterIndex(IndexDefinition)
GetIndexStatus(index_id)
DropIndex(index_id)
```

Registration and removal are durable Manifest edits. The API rejects a type or collation mismatch with the SchemaRegistry. A physical type change creates a new schema epoch and requires a new index definition.

### 7.2 Index State

The persisted catalog state machine is:

```text
DECLARED -> BUILDING -> ACTIVE
                \-> FAILED
```

`BUILDING` definitions are not used by cached plans. A definition enters `FAILED` when its build cannot satisfy schema or format requirements. `ACTIVE` permits partial coverage: the planner sees the exact coverage map and builds a hybrid plan for gaps. An individual failed or corrupt Fragment is removed from the usable coverage set and scheduled for rebuild without changing the whole definition to `FAILED`.

Drop does not persist transitional `DROPPING` or `RETIRED` definitions. One
generation-CAS Manifest edit removes the definition and all of its fragments;
pre-drop snapshots provide the retirement lifetime for physical sidecars.

### 7.3 IndexCatalogSnapshot

Queries pin:

```text
IndexCatalogSnapshot {
  catalog_generation,
  index_definitions,
  source_sst_to_fragment_map,
  coverage_generation,
  statistics_snapshot_id
}
```

The catalog snapshot is selected alongside the query's VersionSet. A cached plan refers to logical `index_id` and capabilities, not to a physical sidecar name. Runtime resolution uses the pinned catalog snapshot and cannot observe a later attach, drop, or fragment replacement.

## 8. Sidecar Format

Each source SST may have one sidecar per active `IndexDefinition`:

```text
IndexSidecarHeader
ValueDirectory
PostingBlock 0
PostingBlock 1
...
StatisticsDirectory
IndexSidecarFooter
```

The header contains:

- sidecar format and feature version;
- `source_sst_id`, `index_id`, schema epoch, and catalog generation;
- canonical value and comparator IDs;
- source row count and indexed PUT count;
- physical encoding IDs;
- required flags and checksums.

The footer contains 64-bit offsets and lengths, value-directory checksum, posting checksum, statistics checksum, and a sidecar identity checksum over the source and definition identities.

### 8.1 Value Directory

The value directory is sorted by canonical value. An entry stores:

```text
value_start
value_length
posting_start
posting_length
posting_row_count
min_valid_from
max_valid_from
min_commit_seq
max_commit_seq
```

Equality seeks one value. `IN` performs multiple seeks and deduplicates source row ordinals. Ordered ranges binary-search the first and last value and stream the covered posting ranges. Binary-prefix predicates use the lexicographic directory range.

### 8.2 Posting Blocks

Posting rows are sorted by source row ordinal within a value range. Each block stores:

- delta-coded source row ordinals or a bitmap;
- row count and byte bounds;
- valid-time and commit-sequence min/max;
- checksum and continuation flags.

The source SST supplies complete Entity, Target, ValidFrom, CommitSeq, Operation, and value metadata. The sidecar does not duplicate full graph identity unless a future physical encoding proves that the duplicate reduces total I/O without changing ownership.

## 9. Adaptive Physical Encoding

The logical capability is stable even when each fragment chooses a different encoding.

### 9.1 Bitmap Dictionary

Low-cardinality values use a dictionary of canonical values and compressed bitmaps over source row ordinals. The builder computes complete byte estimates for bitmap and sorted-posting representations from the source counts. It selects bitmap only when the estimated stored size is at most 87.5% of sorted postings; otherwise it selects sorted postings. The chosen encoding is persisted in the Sidecar header. Bitmap blocks support fast equality, `IN`, and candidate-union operations.

The bitmap is a candidate set. Base validation still determines whether an event is visible.

### 9.2 Sorted Delta Posting

The general representation is a sorted value directory followed by delta-coded postings. It supports equality, `IN`, ordered numeric/time ranges, and binary string prefixes. Posting blocks include valid-time and commit-sequence bounds for additional pruning.

This is the mandatory representation for an index with `ORDERED_RANGE` capability when bitmap encoding is not profitable.

### 9.3 Canonical Values

Canonical encoding is supplied by SchemaRegistry:

- integers and timestamps use fixed-width ordered encoding;
- floats use an explicit schema comparator ID; ordinary equality and range predicates exclude NaN, and `-0` is normalized to `+0`; a future semantic change requires a new comparator ID and schema epoch;
- strings and binary values use raw bytes and binary collation;
- medium strings and binary values use their canonical uncompressed bytes;
- large values use `(BLAKE3-256, raw_length)` for equality only;
- null or absent properties are not indexed in V1.

`IS NULL` and absent-property predicates use existence and property event scans. The planner must not treat a missing null index as a false negative.

No generic scalar hash table is persisted. BLAKE3 remains the authoritative hash identity only for Blob content, not for arbitrary scalar ordering.

## 10. Index Build and Publication

### 10.1 Build Inputs

An index builder reads a pinned immutable source SST or a frozen source snapshot. It decodes only system columns and the indexed value column. It does not resolve Blob payloads; for large values it reads BlobRef hashes.

For each indexed `PUT`, the builder emits a canonical value and source row ordinal. DELETE rows do not emit postings. The builder verifies row count, schema epoch, source identity, and sorted event order before sealing the sidecar.

### 10.2 Online Build

Registration follows:

```text
Manifest AddIndexDefinition(state=DECLARED)
capture VersionSet/catalog fence
Manifest SetIndexState(BUILDING)
build sidecars for fenced live SSTs
build sidecars for new SSTs created during the build
publish usable coverage and statistics generation
Manifest SetIndexState(ACTIVE)
```

The catalog may activate with partial coverage. The planner explicitly accounts for uncovered source files and uses a base scan for them. There is no false-negative window.

Build progress is not a correctness log. An incomplete temporary sidecar is deleted after restart and rebuilt from the immutable source. A complete sidecar is durable only after temp-file fsync, rename, directory fsync, and Manifest publication.

### 10.3 Flush and Compaction

For an active index definition:

- flush may publish an SST without a sidecar if sidecar construction fails, but the catalog records the gap and queries fall back;
- when available, the output SST and sidecar are added by one Manifest edit;
- compaction reads candidate values from input SSTs and writes a new sidecar for the output without reading Blob payloads;
- input SSTs and their sidecars retire together under snapshot pinning.

Compaction never updates a sidecar in place. A sidecar is immutable and source-bound.

### 10.4 Drop and Repair

Dropping an index creates a new catalog generation with the definition removed. Existing queries may continue using the old generation until their VersionSet and catalog snapshot release. Physical sidecars then become Manifest-retired files.

Repair rebuilds a disabled sidecar from its live source SST and attaches it in a new catalog generation. A repair task does not modify query results because the base path remains available.

## 11. MemTable Delta Index

Each active indexed column may maintain a mutable delta index over committed events in active and FrozenMemTables. It is keyed by canonical value and stores logical identity plus valid/commit metadata.

The delta index is:

- protected by the owning StorageShard mutation latch;
- visible only through the transaction/query snapshot rules;
- rebuildable from MemTable events;
- not a durable source of truth;
- never required for correctness.

If the delta index cannot be read at its snapshot generation, the query scans the corresponding MemTable event range. Frozen MemTable lifetime is controlled by the correctness kernel, not by the index builder.

## 12. Query Access Paths

The optimizer exposes four logical access choices:

```text
BaseTemporalScan
IndexFragmentScan + BaseTemporalValidate
HybridIndexOrBaseScan
IndexIntersection + BaseTemporalValidate
```

These predicate access paths execute under snapshot isolation. They are not eligible in the correctness kernel's exact-key strict-serializable mode.

### 12.1 Index Fragment Scan

For every pinned source SST with a usable fragment, the executor probes the value directory and emits candidate row ordinals. It schedules probes as morsels. The base storage cursor then reads complete system rows and applies visibility, successor, DELETE, and interval rules.

### 12.2 Hybrid Coverage

The source set is partitioned into:

```text
covered SSTs     -> index candidate probes
uncovered SSTs   -> base scans
MemTables        -> delta index or base scan
```

The resulting candidates are merged by logical event identity and passed through one `TemporalReadMerger`. An index candidate is never returned directly to the user.

If a posting read fails after a Fragment has emitted some source row ordinals, the per-source scan records those ordinals in a compressed candidate bitmap, switches the remainder of that source to a base scan, and skips already emitted ordinals. This preserves complete coverage without producing the same physical event twice.

### 12.3 Index Intersection

For predicates such as:

```cypher
WHERE n.country = 'CN' AND n.age >= 18
```

the planner may probe both single-column indexes and intersect entity bindings before property gather. Because the property columns have different logical keys and their SSTs are not row-aligned, intersection uses vertex identity or complete edge identity plus a possible temporal-domain overlap. Each property's event provenance remains attached separately until base validation and `IntervalAlign` establish the true common interval.

The planner compares intersection cost with using the most selective index followed by a base property gather. It may choose one index when the second predicate is cheap to evaluate after candidate reduction.

### 12.4 Index and Expand

For a connected graph pattern, the planner compares:

- property-index start followed by adjacency Expand;
- adjacency-first Expand followed by property index/filter;
- bound-endpoint `ExpandInto` followed by indexed property validation;
- two indexed property candidates joined before expansion.

The temporal interval domain is preserved in every alternative. Index selection cannot move a predicate across an interval intersection in a way that changes visible results.

## 13. Temporal Validation Cost

An indexed event candidate can be cheap to find but expensive to validate if the logical key has many versions. The estimator and executor account for:

- candidate posting rows;
- distinct logical keys after candidate deduplication;
- versions per key;
- DELETE ratio and restoration frequency;
- expected successor reads for a range query;
- interval fragmentation after property alignment;
- property gather bytes;
- Blob payloads avoided or required.

An index plan with a high candidate-to-surviving ratio may cost more than a sequential Block scan. The CBO therefore does not treat index existence as a reason to use it.

## 14. Statistics System

### 14.1 Immutable Statistics Fragment

Every SST and usable index sidecar exposes a `StatsFragment`:

```text
StatsFragment {
  source_identity,
  row_count,
  put_count,
  delete_count,
  absent_value_count,
  distinct_value_estimate,
  exact_min_max_when_available,
  quantile_sketch,
  top_k_values,
  posting_length_histogram,
  valid_time_histogram,
  commit_sequence_histogram,
  versions_per_key_histogram,
  delete_and_restore_rates,
  interval_fragmentation_estimate,
  confidence,
  stats_format_version
}
```

Numeric and timestamp columns use mergeable quantile sketches. Cardinality uses a mergeable distinct estimator. Frequent values use a bounded top-k sketch. Exact file/block min/max and counts remain exact metadata.

### 14.2 Temporal Correlation

Independent value and time histograms can badly estimate historical predicates. Indexed columns therefore record a bounded correlation synopsis:

- equi-depth value buckets crossed with valid-time buckets;
- commit-sequence visibility buckets;
- exact per-value valid-time summaries for top-k values.

The synopsis is used only for selectivity estimates. It is not used to omit a candidate or declare a row invisible.

### 14.3 VersionSet Statistics Snapshot

Compaction removes input files and adds output files atomically. HLL, KLL, and top-k sketches are not inverted in place. Instead:

1. each immutable file and sidecar carries its own StatsFragment;
2. a `StatsSnapshot` references the exact live source identities in a pinned VersionSet;
3. mergeable sketches are combined on demand or cached by `version_set_id`;
4. compaction invalidates the old aggregate cache by VersionSet identity;
5. an optional checkpoint accelerates future merges but is never authoritative over live file metadata.

This avoids stale aggregate statistics caused by subtracting non-invertible sketches.

### 14.4 Runtime Feedback

`EXPLAIN ANALYZE` and completed queries may write bounded feedback records keyed by:

```text
normalized plan shape
schema epoch set
index catalog generation
parameter selectivity bucket
data statistics generation
```

Feedback stores observed candidate counts, surviving rows, interval splits, page bytes, and Blob reads. It uses an expiry and confidence decay. Feedback can adjust cost estimates but cannot affect temporal visibility or force an index plan without a valid catalog snapshot.

## 15. Cost-Based Optimizer

### 15.1 Optimizer Architecture

The optimizer is a bounded Cascades-style memo with Cedar-specific equivalence and physical properties. It is not an unrestricted search engine.

Logical expressions are grouped by semantic equivalence. Physical alternatives attach properties and costs. Search is stopped by a planning deadline, group limit, and rule-application budget; the best verified plan found so far is returned.

### 15.2 Logical Transformations

The V1 rule set includes:

- temporal scope and predicate pushdown;
- property demand and late materialization;
- base scan versus index candidate scan;
- index intersection and semi-join reduction;
- adjacency orientation selection;
- `ExpandInto` when both endpoints are bound;
- connected-pattern join and Expand reordering;
- hash join versus interval join;
- aggregate and distinct placement after safe reduction;
- safe limit and ordering pushdown;
- temporal interval alignment and coalescing placement.

Rules that could change graph path multiplicity, temporal interval boundaries, or DELETE visibility are not legal unless their equivalence proof includes those domains.

### 15.3 Physical Properties

Memo alternatives carry:

```text
TemporalMode: AS_OF | BETWEEN | CHANGES
Ordering: logical-key | valid-time | none | requested-order
Partitioning: shard | source-sst | hash-key | single
Materialization: system-only | predicate-columns | projected-columns | complete-entity
CandidateSource: base | index | hybrid | intersection
VectorShape: flat | dictionary-factorized | frontier
```

An alternative that violates a required ordering, snapshot, or materialization property is rejected rather than repaired with an unaccounted copy.

### 15.4 Cost Model

The cost model tracks a vector:

```text
Cost {
  cpu_ns,
  sequential_bytes,
  random_reads,
  decoded_bytes,
  output_rows,
  output_intervals,
  memory_peak,
  spill_bytes,
  blob_bytes,
  confidence
}
```

The session workload profile converts this vector into a score using calibrated sequential bandwidth, random-read latency, decode throughput, CPU cost, memory pressure, and spill cost. Low-confidence estimates receive a risk penalty so an apparently cheap but uncertain index plan does not routinely beat a stable scan.

The estimator includes:

- covered and uncovered source SST counts;
- posting candidate density and duplicate-key ratio;
- base validation versions per candidate;
- temporal fence and interval alignment work;
- property gather page locality;
- adjacency degree and path frontier growth;
- Blob payload avoidance;
- worker parallelism and expected skew.

### 15.5 Planning Budget and Fallback

The optimizer has explicit limits for elapsed planning time, memo groups, alternatives per group, and statistics merge bytes. On budget exhaustion it returns the best legal plan already found, falling back in this order:

```text
bounded indexed/hybrid plan
deterministic index-aware plan
deterministic base vectorized plan
```

Every fallback remains snapshot-correct. There is no row-at-a-time legacy fallback.

## 16. Parameter-Sensitive and Adaptive Planning

### 16.1 Parameter Variants

A cached normalized T-Cypher plan does not freeze literal values. For indexed predicates, the cache may retain a small number of selectivity variants:

```text
very_selective -> index candidate
moderate       -> hybrid
non_selective  -> base scan
```

The binder uses parameter type and runtime value; the optimizer uses the pinned `StatsSnapshot`. Variant count is bounded and least-used variants are evicted.

### 16.2 Runtime Adaptation

At a pipeline breaker or after a bounded number of source morsels, the executor compares observed candidate/survivor ratios with the plan estimate. If the ratio crosses a configured reoptimization threshold, it may:

- stop scheduling unopened index fragments;
- switch remaining uncovered fragments to a base scan;
- insert a dynamic filter for a downstream join or Expand;
- repartition a path frontier;
- request a bounded reoptimization for the next pipeline.

Already emitted rows retain their QuerySnapshot semantics. Runtime adaptation changes only future work and cannot change a result already validated by the base event merger.

## 17. Resource and Maintenance Scheduling

Index sidecar builds and statistics merges are background maintenance tasks. They use:

- a bounded worker quota;
- an I/O token bucket;
- a memory grant separate from foreground query memory;
- cancellation at SST and posting-block boundaries;
- backoff when WAL latency, flush queues, or compaction debt exceed configured thresholds.

Index maintenance never blocks a correct SST publication solely because an optional sidecar is unavailable. It may delay or reduce coverage. A later HTAP scheduling design may add workload classes and admission priorities; this stage defines only the resource contract needed to avoid starving ingestion.

## 18. Error, Corruption, and Fallback Rules

### 18.1 Sidecar Errors

The following make a Fragment unusable:

- source SST identity mismatch;
- index definition or schema epoch mismatch;
- invalid canonical comparator;
- bad offset, posting boundary, dictionary reference, or checksum;
- impossible row ordinal or statistics range.

The query marks the Fragment unavailable and scans the source SST. The catalog records the health event and schedules repair. If the source SST itself is corrupt, the base storage error remains a hard database error.

### 18.2 No Silent False Negatives

An index builder must either emit a complete Fragment or mark it unavailable. A partial temp file is never published as a usable Fragment. Query code cannot treat an empty posting list as proof that a value is absent unless the Fragment health and source coverage are verified.

### 18.3 Statistics Errors

Malformed or stale statistics are discarded and replaced by conservative defaults. A statistics failure can change a plan but cannot change a result. Stats snapshots are never used to bypass base temporal validation.

## 19. Module Boundaries

The new ownership layout is:

```text
index/
  index_definition
  index_catalog
  index_catalog_snapshot
  index_sidecar_format
  index_sidecar_builder
  index_sidecar_reader
  index_posting_codec
  index_canonical_value
  memtable_delta_index
  index_health

statistics/
  stats_fragment
  stats_snapshot
  quantile_sketch
  cardinality_sketch
  topk_sketch
  temporal_selectivity
  runtime_feedback

optimizer/
  memo
  rewrite_rules
  physical_properties
  cost_model
  plan_variants
  adaptive_reoptimizer

manifest/
  index_edits
  stats_checkpoint_metadata

tcypher/physical/
  index_scan
  hybrid_scan
  index_intersection
  stats_provider
```

Dependency rules:

- index readers depend on SST system-column and schema contracts, not on T-Cypher AST;
- optimizer depends on logical index definitions and stats snapshots, not on physical sidecar file names;
- sidecar builders depend on immutable SST readers and BlobRef hashes, never on Blob payload reads;
- Manifest owns index definition, fragment attachment, coverage, and retirement;
- statistics are rebuildable and cannot be required for recovery correctness;
- `BlobHashIndex` remains in `blob/` and is not merged into property IndexCatalog;
- CommitTimeline remains in transaction/recovery ownership and is not treated as a property index.

## 20. Complete Legacy Index Removal

The clean-break requirement applies to index code as well. The implementation must remove or replace:

- `legacy schema header` template index flags and old `IndexType` declarations;
- `AsyncIndexBuilder` and its raw `TemporalVersionNode*` task protocol;
- `VersionChainIndex` as a query-facing secondary index;
- `TemporalBloomFilter` and `SSTTemporalFilter` duplicate ownership;
- old runtime index enable/disable switches in `CedarConfig` and `CedarOptions`;
- entity-only or old-format Bloom paths;
- any index API that returns raw row pointers or assumes mutable version chains;
- any planner branch that relies on a build threshold without an IndexCatalog snapshot.

The new primary SST Bloom, BlockIndex, zone maps, BlobHashIndex, and CommitTimeline remain because they have distinct ownership and semantics. They are not compatibility wrappers around the removed modules.

## 21. Mainstream Comparison

| System family | Relevant strength | Cedar adaptation | Deliberate difference |
|---|---|---|---|
| ClickHouse data-skipping indexes | immutable segment metadata, min/max, Bloom, set/bitmap skips | sidecar fragments, Block pruning, adaptive bitmaps, immutable publication | no MergeTree-style global projection or time-major copy |
| Apache Pinot/Druid | segment-local inverted and bitmap indexes | per-SST sorted postings and compressed bitmaps with hybrid gaps | candidate results must pass bitemporal base validation |
| Lucene | immutable segment dictionaries and postings, segment lifecycle | sorted value directory, posting blocks, source-bound sidecars | Cedar indexes events and logical identities, not independently authoritative documents |
| DuckDB | zonemaps, typed statistics, adaptive scan choices, vector execution | VersionSet stats snapshots and CBO alternatives feed morsel pipelines | no mutable ART index in the primary V1 path |
| PostgreSQL B-tree/BRIN | ordered access and lightweight range summaries | ordered per-SST value directories plus Block/temporal summaries | no global page-addressed index over mutable table pages |
| Kuzu and graph engines | property access and adjacency-aware planning | index start versus adjacency start alternatives, ExpandInto, degree estimates | endpoint and path temporal intersections are part of every candidate plan |

Cedar absorbs immutable segment indexing, typed statistics, bitmap/inverted access, and cost-based alternatives. It does not absorb mutable interval indexes, a second LSM, or an index that can bypass event visibility.

## 22. Known Trade-Offs

1. Per-SST sidecars require probing multiple fragments under size-tiered compaction. The morsel scheduler and coverage-aware CBO make this parallel and measurable.
2. Event candidate indexes can return shadowed historical PUTs. Base validation is mandatory, and churn statistics are part of cost estimation.
3. Index coverage may be incomplete while a build or repair runs. Hybrid plans preserve correctness but can lose selectivity.
4. Single-column indexes may require candidate intersection or a property gather for multi-predicate queries. This avoids composite-index write amplification in V1.
5. A pure time scan remains a Block/valid-time metadata workload, not a secondary value-index workload.
6. Statistics are approximate and can choose a slower plan. They never alter visibility, and runtime feedback can correct recurring misestimates.

## 23. Verification Strategy

### 23.1 Sidecar Format

- golden-byte tests cover header, value directory, posting blocks, statistics, footer, and all format IDs;
- round trips cover integer, timestamp, float, string, binary, and Blob-hash canonical values;
- corruption tests cover truncation, invalid ordinals, invalid ranges, bad dictionaries, bad checksums, and source identity mismatch;
- large postings and oversized values obey allocation and block bounds.

### 23.2 Candidate Completeness

For randomly generated histories, compare an indexed query with a full event scan across:

- out-of-order valid-time writes;
- equal valid times with different `commit_seq`;
- PUT, DELETE, and restore events;
- multiple overlapping SSTs;
- MemTable plus SST mixtures;
- `AS OF`, `BETWEEN`, valid-time changes, system-time changes, and combined predicates.

The indexed path may produce more candidates before validation, but the final result and event provenance must be identical.

### 23.3 Lifecycle and Crash Boundaries

- index definition registration, build, attach, activation, drop, and repair are fault-injected;
- crash before and after sidecar fsync, rename, directory fsync, and Manifest edit is covered;
- orphan sidecars are cleaned while Manifest-referenced sidecars are never deleted;
- compaction atomically retires input SST/sidecar pairs and publishes output pairs;
- old query snapshots retain old fragments until release;
- partial coverage produces a hybrid plan rather than a false negative.

### 23.4 Encoding and Blob Tests

- bitmap and sorted posting encodings return identical candidate sets;
- equality, `IN`, ordered range, and binary-prefix bounds are tested at page and block boundaries;
- large Blob equality probes hash identity without Blob payload reads;
- stale or corrupt sidecars fall back to base scans;
- null and absent-property predicates never rely on an absent index.

### 23.5 Statistics and Optimizer

- mergeable sketches are tested against exact fragment statistics;
- VersionSet changes invalidate aggregate caches without subtracting non-invertible sketches;
- statistics corruption falls back to conservative defaults;
- plan golden tests cover base, index, hybrid, intersection, adjacency-first, and index-first alternatives;
- optimizer budget exhaustion returns a legal deterministic plan;
- parameter variants choose different legal paths without changing results;
- runtime feedback and adaptive switching are tested at pipeline breakers;
- `EXPLAIN` reports index definitions, coverage, statistics generation, estimates, and chosen alternatives.

### 23.6 Concurrency and Resource Tests

- index build runs with writes, flush, compaction, and Blob GC;
- foreground queries remain snapshot-stable while coverage changes;
- maintenance I/O and memory budgets yield under ingestion pressure;
- cancellation releases sidecars, temp files, catalog snapshots, and source pins;
- index repair and drop do not block correct base reads.

## 24. Structural Performance Acceptance

Completion requires measurable evidence that:

1. selective predicates can avoid reading unrelated base Blocks when a healthy Fragment is covered;
2. non-selective predicates choose a base scan rather than blindly probing all postings;
3. historical validation cost is visible in `EXPLAIN ANALYZE`;
4. index probes, base validation, property gathers, and Blob reads are separately counted;
5. sidecar reads use bounded metadata/page I/O rather than whole-file loading;
6. hybrid gaps do not create duplicate logical results;
7. index intersections reduce candidates only when their join cost is justified;
8. plan search remains within configured time and group budgets;
9. background index construction does not starve WAL, flush, or compaction;
10. statistics generations match the VersionSet used by the query.

No absolute QPS claim is made before a fixed benchmark corpus and workload mix are defined. The required counters make those later benchmarks reproducible.

## 25. Implementation Dependency Order

The future implementation plan must follow this order:

1. Define canonical typed value comparators and index format IDs.
2. Add Manifest index edits, catalog generations, and snapshot metadata.
3. Implement immutable Sidecar builder/reader and bitmap/sorted posting codecs.
4. Implement candidate completeness checks and base validation integration.
5. Implement MemTable delta indexes and hybrid coverage resolution.
6. Implement immutable StatsFragment, mergeable sketches, and VersionSet StatsSnapshot.
7. Implement IndexCatalog administrative APIs and asynchronous build/repair scheduling.
8. Add `IndexScan`, `HybridScan`, and `IndexIntersection` physical operators.
9. Add optimizer memo, temporal rules, physical properties, and cost model.
10. Add parameter variants, runtime feedback, and bounded adaptive reoptimization.
11. Add EXPLAIN/ANALYZE metrics and failure/concurrency tests.
12. Remove all old index headers, sources, settings, and runtime switches.
13. Run clean-break, crash/reopen, temporal oracle, and structural performance acceptance.

This is architectural sequencing, not a code implementation plan. Each implementation task must be test-first and independently reviewable after the user authorizes implementation.

## 26. Completion Definition

The temporal index and CBO stage is complete only when:

1. one Manifest-owned IndexCatalog describes all usable property indexes;
2. index definitions and sidecars are versioned, checksummed, snapshot-pinned, and source-bound;
3. every secondary index candidate path is complete or falls back to a base scan;
4. no secondary index stores mutable or authoritative `valid_to` state;
5. equality, `IN`, ordered range, binary-prefix, and Blob-hash equality behavior matches full scans;
6. partial coverage and sidecar corruption produce correct hybrid/fallback execution;
7. MemTable deltas, SST sidecars, compaction, repair, and drop follow one lifecycle protocol;
8. StatsSnapshots match the exact VersionSet and remain rebuildable after restart;
9. the CBO chooses among base, index, hybrid, intersection, and graph-order alternatives within budget;
10. temporal validation, interval fragmentation, Blob avoidance, and resource costs appear in estimates and metrics;
11. runtime feedback and adaptation never change snapshot or event visibility;
12. index maintenance respects foreground ingestion and storage resource budgets;
13. all old index implementations, switches, and compatibility paths are removed;
14. the correctness-kernel, columnar, and T-Cypher V1 invariants remain intact.
