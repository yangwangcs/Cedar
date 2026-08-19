# Cedar embedded engine design

## Decision

Cedar embeds its storage engine source in the Cedar repository. The former
`third_party/rocksdb` Git submodule is replaced by `src/engine/rocksdb`; it is
not fetched, published, or versioned as a separate Cedar repository. Cedar
does not create a `cedar-rocksdb` remote as part of this design.

The engine is Cedar-controlled source code. RocksDB 11.1.2 is its recorded
provenance baseline, not a runtime, build-time, or release dependency. Cedar
may selectively import upstream security and correctness fixes, but each
import is reviewed and committed to the Cedar repository.

## Source ownership and names

`src/engine/rocksdb` preserves the imported RocksDB directory structure,
`rocksdb::` namespace, generated configuration conventions, and existing
internal target names during the migration. This minimises semantic and ABI
risk in a codebase with macro-generated code, static registration, and
cross-library internal interfaces.

All new Cedar-specific engine code belongs below `src/engine/cedar`. It may
include narrowly scoped bridges into `src/engine/rocksdb`, but it must not be
installed or exposed through a public CMake target. Existing Cedar changes to
the imported engine remain in place, with provenance comments at the modified
RocksDB hooks. A wholesale namespace or include-path rename is explicitly out
of scope; it is not a control boundary and would make future security imports
less reliable.

```text
Cedar/
├── include/cedar/                 # only supported consumer API
├── src/kernel/                    # transaction and snapshot semantics
├── src/runtime/                   # Cedar scheduling and policy
├── src/storage/
│   ├── facts/                     # authoritative logical columnar facts
│   └── rocks/                     # private request/result translation
└── src/engine/
    ├── cedar/                     # Cedar-owned engine extensions
    └── rocksdb/                   # embedded RocksDB-derived implementation
```

## Preserved authority model

Embedding the source does not change the Kernel design. The engine owns the
single WAL, WAL recovery, MemTables, sequence allocation, VersionSet,
MANIFEST, and native flush/compaction execution. Cedar facts remain the
authoritative logical columnar representation; Cedar Parquet files are not a
second WAL or recovery authority.

Cedar owns admission, runtime sampling policy, maintenance scheduling and
grants, and public transaction/snapshot semantics. `src/storage/rocks` stays
the sole Cedar-side layer that directly names engine APIs. No public Cedar
header may include an engine header, name `rocksdb::`, or expose engine
maintenance controls.

## Build and packaging

`cmake/CedarRocksDB.cmake` builds the engine from
`src/engine/rocksdb` into the existing private static implementation target.
The cache key is based on a Cedar engine source digest and an explicit
`CEDAR_ENGINE_BASELINE` identifier, rather than `git -C` inside a nested
repository. Pinned Cedar codec inputs, compiler flags, sanitizer profiles,
and static linkage remain unchanged.

The package exports only `Cedar::cedar` and Cedar public headers. Engine
archives may remain under `lib/cedar/internal` solely to resolve a static
link, but engine headers, CMake targets, submodule metadata, and external
engine URLs are not installed or required by consumers.

## Provenance, licensing, and updates

The embedded engine root contains `PROVENANCE.md` recording the upstream
baseline commit `3b446089141659fad25328c5ea3e7ed283df46e4`, the imported Cedar
Kernel revision `7ddbe68ba322b235b1d78591487ffed842ba9567`, the retained
copyright and Apache-2.0 licence notices, and the Cedar change inventory.
The repository retains all required upstream licence and notice files.

An engine maintenance document defines a repeatable update procedure: import
an upstream commit into a temporary branch, preserve Cedar patches, rebuild
Debug and sanitizer profiles, run recovery and performance gates, and commit
the resulting source and updated provenance atomically. No automatic upstream
merge or external fork publication is allowed.

## Migration and deletion rules

The migration copies the exact checked-out engine tree into
`src/engine/rocksdb`, including Cedar kernel additions and provenance
documentation, then removes only the RocksDB entry from `.gitmodules` and the
`third_party/rocksdb` gitlink. Other third-party submodules are unchanged.
The temporary nested `.git` directory is never committed.

After the copy, source contracts reject a `third_party/rocksdb` gitlink,
RocksDB submodule declaration, build reference, or release requirement. They
require the embedded root, provenance, license notices, and a baseline marker.
The migration is complete only when a fresh clone without submodule initialisation
can configure, build, install, and run a Cedar public consumer against the
embedded engine.

## Validation

Validation retains the existing public-header, storage, recovery, install-tree,
and bounded N+1 performance tests. It adds checks that the engine source is
tracked by Cedar, no nested Git metadata is present, `git submodule update`
is unnecessary for RocksDB, and the CMake engine cache invalidates when a
tracked embedded engine source changes.

The final acceptance sequence is a fresh Debug build and full CTest suite,
focused Release correctness/performance tests, ASAN, UBSAN, TSAN where the
platform supports it, an install-tree consumer build, and a fresh-clone
configure/build without initialising any submodule. Release performance is
reported separately from Debug correctness diagnostics.
