# Temporal Index Blob-Hash Equality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute equality and `IN` predicates over large Blob-backed string/binary properties through BLAKE3-256 index identity without reading Blob payloads during candidate lookup or predicate validation.

**Architecture:** Tag canonical index values as inline values or Blob hashes. Sidecars and MemTable delta indexes store `BlobRef.content_hash` for Blob-backed PUTs and equality probes search both the inline literal and its Blob hash; ordered range and prefix paths ignore hash entries. Predicate property gather compares a visible BlobRef against precomputed literal hashes and emits the matching typed literal without reading the Blob, while projection gather retains normal payload materialization.

**Tech Stack:** C++17, GoogleTest, Cedar sidecar format, MemTable delta index, T-Cypher vector runtime, BLAKE3 Blob identity.

## Global Constraints

- Keep database format version `1`.
- Upgrade the current sidecar magic from `CSI2` to `CSI3`; reject older sidecars without a compatibility reader.
- Blob hashes are equality-only and never participate in ordered range or prefix lookup.
- Match both `content_hash` and `raw_length` when validating a literal against a BlobRef.
- Never read or install external dependencies at database startup.
- Build and test with `-j1`; preserve the dirty worktree and do not stage, commit, reset, clean, or push.

---

### Task 1: RED contracts for Blob hash candidates

**Files:**
- Modify: `tests/test_correctness_kernel.cc`

**Interfaces:**
- Consumes: `BuildIndexCandidateSidecar`, `LookupIndexEquality`, `MemtableDeltaIndex::Lookup`, `CedarDatabase::ExecuteTcypher`.
- Produces: sidecar, delta-index, and end-to-end no-payload-read regressions.

- [x] Add a sidecar test using `TemporalEvent::PutBlob` and assert equality by the original typed payload returns the Blob posting while range/prefix do not.
- [x] Add a MemTable delta test with the same inline-plus-Blob equality contract.
- [x] Add a database test with a Blob-backed property, active index, `WHERE n.payload = <large literal> RETURN n`, nonzero index candidates, and `blob_payload_reads == 0`; also assert `RETURN n.payload` reads exactly one payload.
- [x] Run the focused RED filter and confirm failures are caused by Blob refs being skipped/falling back/materialized.

### Task 2: Tagged canonical values and current sidecar format

**Files:**
- Modify: `include/cedar/index/canonical_value.h`
- Modify: `src/index/canonical_value.cc`
- Modify: `src/index/index_sidecar.cc`
- Modify: `include/cedar/index/memtable_delta_index.h`

**Interfaces:**
- Produce: `IndexCanonicalKind::{kInline,kBlobHash}`, `EncodeIndexBlobHash(const BlobRef&)`, `EncodeIndexBlobHash(const Value&)`.

- [x] Add the canonical kind to comparisons and transparent lookup keys.
- [x] Encode/decode the kind byte under `CSI3`; keep all older sidecars rejected.
- [x] Index Blob PUTs by hash in sidecars and delta indexes.
- [x] Probe both inline and hash keys for equality/IN and exclude hash keys from range/prefix.
- [x] Run canonical/sidecar/delta focused tests to GREEN.

### Task 3: Predicate gather hash validation

**Files:**
- Modify: `include/cedar/tcypher/storage/temporal_scan.h`
- Modify: `src/tcypher/storage/temporal_scan.cc`
- Modify: `include/cedar/tcypher/storage/property_gather.h`
- Modify: `src/tcypher/storage/property_gather.cc`
- Modify: `src/tcypher/runtime/query_runtime.cc`

**Interfaces:**
- Produce: a predicate-only Blob hash probe carrying typed literals and their BLAKE3 identities.

- [x] Precompute hash probes only for equality/IN string or binary predicates.
- [x] During predicate gather, compare the visible BlobRef to the probes; emit the matching typed literal or NULL without calling the Blob materializer.
- [x] Keep projection gather unchanged so demanded Blob values are materialized and attributed.
- [x] Update sidecar/delta source validation to accept hash postings only when they match the source BlobRef.
- [x] Run the end-to-end no-payload-read test to GREEN.

### Task 4: Closure verification

**Files:**
- Modify: `docs/superpowers/plans/2026-07-22-cedar-six-design-completion-matrix.md`
- Modify: `.superpowers/sdd/progress.md`

- [x] Run Blob/index/T-Cypher focused tests.
- [x] Run the full normal CTest matrix with `ctest --test-dir build-current -j1 --output-on-failure` (`805/805 PASS`, 41.80 seconds).
- [x] Run `git diff --check` on all touched files.
- [x] Mark Temporal Index item 5 implementation COMPLETE only if equality equivalence and zero predicate payload reads are proven; keep release artifact status open until archived evidence exists.

Post-review hardening also proves that Blob-backed string range/prefix
predicates materialize payloads for exact evaluation, legacy graph projection
paths materialize BlobRefs instead of returning their placeholder value,
lower-only MemTable ranges stop at the inline physical-type boundary, and
large equality/`IN` probe storage is admitted before allocation. These changes
do not close the still-missing durable release artifact.
