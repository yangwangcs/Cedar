# Cedar RocksDB Stage A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the pinned RocksDB dependency and a durable, independently tested Cedar `FactStore` while leaving the current public database path intact.

**Architecture:** `FactStore` is a deep pimpl-based module; RocksDB headers remain in `src/fact_store`. Fixed Cedar codecs order immutable facts by family/property/entity, then valid and commit time descending. A single publisher mutex validates contiguous sequence metadata and writes facts plus metadata in one synchronous RocksDB WriteBatch.

**Tech Stack:** C++20, CMake, RocksDB v11.1.2, GoogleTest, CRC32C.

## Global Constraints

- Follow the master plan's Global Constraints.
- Stage A does not route `CedarDatabase` or `TransactionCoordinator` through RocksDB.
- Every new durable encoding has a format version and bounded decoder.
- Tests use real temporary RocksDB directories for durability assertions.

---

### Task A1: Pin RocksDB and Establish the Build Boundary

**Files:**
- Create: `.gitmodules`
- Add dependency: `third_party/rocksdb` at tag `v11.1.2`
- Modify: `CMakeLists.txt`
- Create: `cmake/CedarRocksDB.cmake`
- Modify: `third_party/CODECS.md`
- Create: `tests/test_rocksdb_dependency.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces target `cedar_rocksdb` linked privately by future `cedar_fact_store`.
- Produces compile definition `CEDAR_ROCKSDB_VERSION="11.1.2"`.

- [ ] **Step 1: Add a failing dependency identity test**

```cpp
#include <gtest/gtest.h>
#include <rocksdb/version.h>

TEST(RocksDbDependencyTest, UsesPinnedVersion) {
  EXPECT_EQ(ROCKSDB_MAJOR, 11);
  EXPECT_EQ(ROCKSDB_MINOR, 1);
}
```

- [ ] **Step 2: Run RED**

Run: `cmake -S . -B build-rocks -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug && cmake --build build-rocks --target test_rocksdb_dependency -j2`

Expected: configuration or compilation fails because the RocksDB target/header is absent.

- [ ] **Step 3: Add the pinned dependency and isolated CMake adapter**

Configure RocksDB with C++20, static libraries, no tests/tools/benchmarks/JNI, no system dependency fallback, and a repository target alias. Link platform dependencies transitively through `cedar_rocksdb`; do not add global RocksDB include directories.

```cmake
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_JNI OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/rocksdb EXCLUDE_FROM_ALL)
add_library(cedar_rocksdb INTERFACE)
target_link_libraries(cedar_rocksdb INTERFACE rocksdb)
target_compile_definitions(cedar_rocksdb INTERFACE
  CEDAR_ROCKSDB_VERSION="11.1.2")
```

- [ ] **Step 4: Run GREEN**

Run: `cmake --build build-rocks --target test_rocksdb_dependency -j2 && ./build-rocks/tests/test_rocksdb_dependency`

Expected: one passing test and reported RocksDB major/minor `11.1`.

- [ ] **Step 5: Commit**

```bash
git add .gitmodules third_party/rocksdb CMakeLists.txt cmake/CedarRocksDB.cmake third_party/CODECS.md tests/CMakeLists.txt tests/test_rocksdb_dependency.cc
git commit -m "build: pin RocksDB storage dependency"
```

### Task A2: Add Cedar Kernel Status Classes and Fact Domain Types

**Files:**
- Modify: `include/cedar/core/status.h`
- Modify: `src/core/status.cc`
- Create: `include/cedar/fact/fact.h`
- Create: `src/fact/fact.cc`
- Create: `tests/test_fact_model.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces strong types `VertexId`, `EdgeId`, `PropertyId`, `CommitSeq`, `TxnId`, and `ValidTime`.
- Produces `EntityFact`, `PropertyFact`, `FactRef`, `EdgeIdentity`, `FactEvent`, `PendingFactMutation`, and `FactFamily`.
- Produces typed Status constructors/predicates for `IdentityConflict`, `SnapshotExpired`, `SnapshotPinned`, and `UnsupportedSerializablePredicate`.

- [ ] **Step 1: Write failing model and status tests**

```cpp
TEST(FactModelTest, SeparatesEntityStateFromProperties) {
  const auto vertex = cedar::EntityFact::Vertex(cedar::VertexId{7});
  const auto property = cedar::PropertyFact::Vertex(
      cedar::VertexId{7}, cedar::PropertyId{9});
  EXPECT_EQ(vertex.ref().family(), cedar::FactFamily::kVertexState);
  EXPECT_EQ(property.ref().family(), cedar::FactFamily::kVertexProperty);
}

TEST(StatusTest, SnapshotExpiredIsTypedAndStable) {
  const cedar::Status status = cedar::Status::SnapshotExpired(
      "snapshot", "sequence is below retention boundary");
  EXPECT_TRUE(status.IsSnapshotExpired());
  EXPECT_EQ(status.ToString(),
            "SnapshotExpired: snapshot: sequence is below retention boundary");
}
```

- [ ] **Step 2: Run RED**

Run: `cmake --build build-rocks --target test_fact_model -j2`

Expected: missing fact types and Status constructors.

- [ ] **Step 3: Implement value-semantic fact types and Status mapping**

All IDs reject zero at storage boundaries. `EdgeIdentity` validates nonzero IDs and stable source/target/type. Existence mutations use schema epoch zero and no `Value`; property mutations always carry a nonzero property ID and schema epoch.

- [ ] **Step 4: Run GREEN**

Run: `cmake --build build-rocks --target test_fact_model -j2 && ./build-rocks/tests/test_fact_model`

Expected: all fact model and typed status tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/core/status.h src/core/status.cc include/cedar/fact/fact.h src/fact/fact.cc tests/test_fact_model.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: define clean-break fact model"
```

### Task A3: Implement Fixed Fact and Metadata Codecs

**Files:**
- Create: `include/cedar/fact/fact_codec.h`
- Create: `src/fact/fact_codec.cc`
- Create: `include/cedar/fact/meta_codec.h`
- Create: `src/fact/meta_codec.cc`
- Create: `tests/test_fact_codec.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces `std::string EncodeFactKey(const FactRef&, ValidTime, CommitSeq)`.
- Produces `StatusOr<DecodedFactKey> DecodeFactKey(Slice)`.
- Produces `StatusOr<std::string> EncodeFactValue(const FactEvent&)` and `DecodeFactValue`.
- Produces typed codecs for format, watermark, sequence, transaction outcome, edge identity, ID lease, and property schema metadata.

- [ ] **Step 1: Write golden and ordering tests**

```cpp
TEST(FactCodecTest, OrdersValidAndCommitTimeDescending) {
  const auto ref = cedar::EntityFact::Vertex(cedar::VertexId{7}).ref();
  EXPECT_LT(cedar::EncodeFactKey(ref, cedar::ValidTime{20}, cedar::CommitSeq{9}),
            cedar::EncodeFactKey(ref, cedar::ValidTime{10}, cedar::CommitSeq{99}));
  EXPECT_LT(cedar::EncodeFactKey(ref, cedar::ValidTime{20}, cedar::CommitSeq{9}),
            cedar::EncodeFactKey(ref, cedar::ValidTime{20}, cedar::CommitSeq{8}));
}
```

Add exact hexadecimal golden expectations, round trips for every family/value type, truncated length tests, CRC corruption tests, invalid enum tests, and maximum payload rejection.

- [ ] **Step 2: Run RED**

Run: `cmake --build build-rocks --target test_fact_codec -j2`

Expected: missing codec headers and functions.

- [ ] **Step 3: Implement bounded explicit-endian codecs**

Use byte appends/reads, not native struct serialization. The key is exactly 28 bytes. Descending u64 fields are encoded as bitwise-not before big-endian emission. Values include format, operation, schema epoch, kind, u32 length, payload, and CRC32C.

- [ ] **Step 4: Run GREEN and malformed corpus**

Run: `cmake --build build-rocks --target test_fact_codec -j2 && ./build-rocks/tests/test_fact_codec --gtest_repeat=2`

Expected: ordering, golden, round-trip, and corruption tests pass twice.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/fact/fact_codec.h src/fact/fact_codec.cc include/cedar/fact/meta_codec.h src/fact/meta_codec.cc tests/test_fact_codec.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add versioned fact store codecs"
```

### Task A4: Implement FactStore Open, Snapshot, Read, and Scan

**Files:**
- Create: `include/cedar/fact/fact_store.h`
- Create: `src/fact/fact_store_impl.h`
- Create: `src/fact/fact_store.cc`
- Create: `tests/test_fact_store.cc`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces move-only `StoreSnapshot` with logical sequence, retention boundary, and hidden RocksDB Snapshot.
- Produces `FactStore::Open`, `Close`, `BeginSnapshot`, `Read`, and bounded `Scan`.
- RocksDB types remain absent from public headers.

- [ ] **Step 1: Write failing empty/open/read tests**

```cpp
TEST_F(FactStoreTest, OpensTwoColumnFamiliesAndReturnsEmptySnapshot) {
  cedar::FactStore store({path_});
  ASSERT_TRUE(store.Open().ok());
  auto snapshot = store.BeginSnapshot({});
  ASSERT_TRUE(snapshot.ok());
  EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), cedar::CommitSeq{0});
  EXPECT_EQ(snapshot.ValueOrDie().oldest_readable_seq(), cedar::CommitSeq{0});
}
```

Add tests that RocksDB's default CF is not used for Cedar records, prefix-bounded scans never escape a fact, and requested historical snapshots outside `[oldest, visible]` are rejected.

- [ ] **Step 2: Run RED**

Run: `cmake --build build-rocks --target test_fact_store -j2`

Expected: missing `FactStore`.

- [ ] **Step 3: Implement pimpl store and resolver**

Open/create exactly `default`, `facts`, and `meta`; reserve `default` empty for RocksDB compatibility. Pin RocksDB options, comparator, blob configuration, atomic flush behavior, and prefix extractor in one implementation function. `BeginSnapshot` captures logical and physical boundaries under one publisher mutex. `Read` seeks the 12-byte fact prefix plus requested valid bound and chooses the first event satisfying both time predicates.

- [ ] **Step 4: Run GREEN**

Run: `cmake --build build-rocks --target test_fact_store -j2 && ./build-rocks/tests/test_fact_store`

Expected: open, snapshot, empty read, and bounded scan tests pass.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/fact/fact_store.h src/fact/fact_store_impl.h src/fact/fact_store.cc tests/test_fact_store.cc CMakeLists.txt tests/CMakeLists.txt
git commit -m "feat: add RocksDB fact store reads"
```

### Task A5: Add Atomic Durable Commit and Outcome Recovery

**Files:**
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `src/fact/fact_store.cc`
- Create: `tests/test_fact_store_commit.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces `StoreCommitBatch`, `StoreCommitResult`, `FactStore::Commit`, `visible_seq`, and `ResolveTransaction`.
- One commit sequence record contains the exact encoded fact keys written by its transaction.

- [ ] **Step 1: Write failing atomicity/reopen tests**

```cpp
TEST_F(FactStoreCommitTest, CommitsFactsOutcomeSequenceAndWatermarkAtomically) {
  auto result = store_->Commit(BatchForTwoVertexFacts(cedar::TxnId{9}));
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().commit_seq, cedar::CommitSeq{1});
  Reopen();
  EXPECT_EQ(store_->visible_seq(), cedar::CommitSeq{1});
  EXPECT_EQ(store_->ResolveTransaction(cedar::TxnId{9}).ValueOrDie()->commit_seq,
            cedar::CommitSeq{1});
}
```

Add duplicate `txn_id` idempotence/conflict, multi-fact all-or-none, contiguous sequence, invalid batch, and corrupted/missing watermark/sequence reopen tests.

- [ ] **Step 2: Run RED**

Run: `cmake --build build-rocks --target test_fact_store_commit -j2`

Expected: missing Commit and outcome interfaces.

- [ ] **Step 3: Implement synchronous WriteBatch publication**

Under the publisher mutex, allocate `visible + 1`, encode every immutable fact key, add edge/meta records supplied by the batch, add `txn`, `sequence`, and `watermark/visible`, then call `DB::Write` with `sync=true`. Publish memory state only after success. Translate ambiguous engine outcomes to `Indeterminate` and set `recovery_required`.

- [ ] **Step 4: Run GREEN and reopen loop**

Run: `cmake --build build-rocks --target test_fact_store_commit -j2 && ./build-rocks/tests/test_fact_store_commit --gtest_repeat=5`

Expected: atomicity and reopen tests pass five times.

- [ ] **Step 5: Commit**

```bash
git add include/cedar/fact/fact_store.h src/fact/fact_store.cc tests/test_fact_store_commit.cc tests/CMakeLists.txt
git commit -m "feat: commit canonical facts through RocksDB"
```

### Task A6: Add Durable ID Leases and Property Registration

**Files:**
- Modify: `include/cedar/fact/fact_store.h`
- Modify: `src/fact/fact_store.cc`
- Create: `tests/test_fact_store_metadata.cc`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces `LeaseIds(IdKind, count)` and durable property definition registration/lookup.
- Vertex and edge allocators lease separate ranges; property IDs/epochs are monotonic and stable.

- [ ] **Step 1: Write failing lease and schema tests**

Test 4096-ID default lease persistence, unused-gap behavior after reopen, independent vertex/edge ranges, exhaustion, idempotent property registration, epoch increment, type mismatch, and corrupted metadata rejection.

- [ ] **Step 2: Run RED**

Run: `cmake --build build-rocks --target test_fact_store_metadata -j2`

Expected: missing metadata methods.

- [ ] **Step 3: Implement synchronous metadata WriteBatches**

Lease range endpoints and schema records with `sync=true`. Cache current leases and immutable schema snapshots in memory only after durable success. Never reuse a returned or durably reserved ID.

- [ ] **Step 4: Run GREEN and Stage A regression**

Run: `cmake --build build-rocks --target test_fact_store_metadata test_correctness_kernel -j2 && ctest --test-dir build-rocks --output-on-failure`

Expected: all Stage A targets and the unchanged legacy correctness target pass.

- [ ] **Step 5: Commit and record Stage A boundary**

```bash
git add include/cedar/fact/fact_store.h src/fact/fact_store.cc tests/test_fact_store_metadata.cc tests/CMakeLists.txt
git commit -m "feat: persist Cedar metadata in RocksDB"
```
