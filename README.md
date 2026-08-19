# Cedar

Cedar is an embedded C++20 bitemporal graph database kernel. Standard RocksDB
is its sole canonical WAL, MemTable, manifest, and LSM store. Cedar stores
immutable bitemporal facts there; columnar and adjacency structures are
rebuildable analytical projections rather than a second source of truth.

Every write occurs in an explicit transaction. Snapshot reads bind a logical
commit sequence and a RocksDB Snapshot, so a fact is resolved by both the
requested valid time and the snapshot's system time. New databases use the
clean-break RocksDB format; legacy Cedar formats are rejected without mutation.

## Build

LZ4 1.10.0 and Zstd 1.5.7 are pinned under `third_party/` and compiled
statically by default. Building and starting Cedar does not install packages or
require host codec libraries.

```bash
cmake -S . -B build -DBUILD_TESTS=ON
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

Only `cedar_core` and Cedar's public headers are installed. Columnar and
adjacency projections are optional derived targets; they are deliberately not
installed as part of the embedded kernel.

## Format and migration

Format version `1` is stored, checksummed, at `meta/format/current` in the
RocksDB `meta` Column Family. Cedar verifies the exact version, the `facts` and
`meta` Column Family layout, watermarks, and contiguous sequence metadata at
open. It rejects old Cedar directories, missing or corrupt format records, and
future versions without modifying them.

This release has no dual-write or online migration path. Export an old database
using a binary that understands its old format, then import its facts through
the explicit Cedar `Transaction` API into a fresh version-1 directory.

## Verification and benchmark

The standard verification profiles are Debug/Release CTest plus ASAN, UBSAN,
and TSAN. Cedar builds the pinned RocksDB source once into a profile-keyed
static-library cache, then every Cedar build links that library. The cache key
includes the RocksDB revision, compiler, target, and sanitizer profile; set
`CEDAR_ROCKSDB_CACHE_DIR` to relocate it. UBSAN enables the pinned RocksDB
source's upstream UBSAN mode, which suppresses only its documented intentional
unaligned checksum loads.

```bash
cmake -S . -B build-bench -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-bench --target cedar_kernel_bench -j2
./build-bench/cedar_kernel_bench --path /tmp/cedar-kernel-bench \
  --workload mixed-90-write-10-point-read --operations 10000 \
  --read-operations 10000 --campaign warm --duration-seconds 30 \
  --profile kernel
```

`cedar_kernel_bench` is the only supported benchmark. It exercises Cedar's
single-WAL Kernel write, read, mixed, and authoritative columnar paths and
emits Cedar-owned runtime and space metrics. `--campaign sustained` rejects
durations below 1,800 seconds and only a run that actually reaches that
duration can receive a sustained qualification status.

### Durable asynchronous commit

`Transaction::Commit()` remains the synchronous API. `CommitAsync()` returns a
`CommitHandle` only after the complete transaction has been durably written as
a CRC-protected prepare record with `sync = true`; it does not merely mean that
the request reached an in-memory queue. `Wait()` returns the eventual
`Committed` or `Aborted` result. On reopen Cedar resumes any durable prepares
before accepting new async work.

```cpp
auto accepted = transaction->CommitAsync();
if (!accepted.ok()) return accepted.status();
const cedar::CommitResult result = accepted.ValueOrDie().Wait().ConsumeValueOrDie();
```

The prepare and final-publish paths each use bounded group commit. This API
reduces how long callers wait for a durable acceptance boundary, but it does
not remove synchronous persistence: each ultimately committed async batch has
one durable prepare write and one durable final publish.

## API shape

```cpp
#include <memory>
#include "cedar/database.h"

auto opened = cedar::Database::Open({.path = "/data/cedar"});
if (!opened.ok()) return opened.status();
std::unique_ptr<cedar::Database> database =
    std::move(opened).ConsumeValueOrDie();

const cedar::VertexId vertex =
    database->AllocateVertexId().ConsumeValueOrDie();
auto begun = database->BeginTransaction();
if (!begun.ok()) return begun.status();
std::unique_ptr<cedar::Transaction> transaction =
    std::move(begun).ConsumeValueOrDie();
transaction->Assert(cedar::EntityFact::Vertex(vertex), cedar::ValidTime{1000});
const cedar::CommitResult committed =
    transaction->Commit().ConsumeValueOrDie();
if (committed.outcome != cedar::CommitOutcome::kCommitted) {
  return committed.status;
}

auto begun_snapshot = database->BeginSnapshot();
if (!begun_snapshot.ok()) return begun_snapshot.status();
cedar::Snapshot snapshot = std::move(begun_snapshot).ConsumeValueOrDie();
const bool exists = snapshot.Exists(
    cedar::EntityFact::Vertex(vertex), cedar::ValidTime{1000}).ConsumeValueOrDie();
```

`AllocateVertexId` and `AllocateEdgeId` use separate durable leases; gaps after
a restart are intentional and IDs are never reused. `Close()` rejects new
operations, waits for active commits, and returns `SnapshotPinned` until caller
owned Snapshots have been released.
