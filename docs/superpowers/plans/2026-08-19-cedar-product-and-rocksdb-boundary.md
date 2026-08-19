# Cedar Product and RocksDB Boundary Implementation Plan

> **Superseded:** The external `cedar-rocksdb` fork publication route is
> replaced by the approved embedded-engine design. Continue with
> `2026-08-19-cedar-embedded-engine.md`; completed product-boundary work in
> this document remains valid.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize Cedar's source, build, and release boundaries so Cedar is the only public product while a Cedar-controlled RocksDB fork remains an internal storage adapter, without changing the single-WAL architecture, storage format, transaction semantics, or performance policy.

**Architecture:** Keep Cedar's public interface in `include/cedar`, move the current fact-store and RocksDB-facing contracts behind an internal storage seam, and link the Cedar-owned RocksDB adapter privately. The fork remains a separately versioned repository derived from RocksDB, with Cedar-specific hooks and the Cedar Parquet table implementation maintained there; Cedar calls those hooks through a narrow internal adapter rather than exposing RocksDB types to applications.

**Tech Stack:** C++20, CMake, GoogleTest, RocksDB 11.1.2-derived Cedar fork, Cedar Parquet format, Git submodule, static library packaging, Debug/ASAN/UBSAN/TSAN/Release validation.

## Global Constraints

- Cedar keeps one RocksDB WAL; RocksDB owns WAL, recovery, MemTable, VersionSet, MANIFEST, sequence allocation, and native flush/compaction execution.
- Cedar facts remain the authoritative logical columnar facts; Parquet files are produced and consumed by the Cedar RocksDB table implementation and are not a second WAL or recovery authority.
- Cedar owns admission, runtime sampling policy, maintenance scheduling, flush/compaction grants, and user-facing transaction/snapshot semantics.
- This is an ownership and packaging reorganization, not a behavioral rewrite. Existing clean-break format, commit ordering, crash recovery, read visibility, and benchmark gates must remain unchanged.
- No legacy compatibility path is added. The archived pre-Kernel implementation is removed from production source and build inputs.
- Public Cedar headers must not include RocksDB headers, expose RocksDB types, expose Cedar-to-RocksDB maintenance grants, or require callers to link a RocksDB target directly.
- The Cedar RocksDB fork must be reachable from a Cedar-controlled remote before the top-level repository is published; a local-only submodule commit is not releaseable.
- Every structural change must retain a focused test and an independently runnable build or contract check.

## Target Repository Layout

The implementation must converge on this layout without changing the logical design:

```text
Cedar/
├── include/cedar/                  # installed public interface only
├── src/kernel/                     # transaction, snapshot, temporal semantics
├── src/storage/
│   ├── facts/                      # Cedar fact model, codecs, metadata, scans
│   └── rocks/                      # private RocksDB adapter and translation
├── src/runtime/                    # Cedar admission, sampling, maintenance policy
├── tests/{public,kernel,storage,recovery,performance}/
├── benchmarks/
├── cmake/
└── third_party/rocksdb             # pinned Cedar-controlled fork submodule
```

The current implementation can be moved incrementally. During the transition, compatibility forwarding headers may exist only under `src/` and must not be installed.

### Task 1: Freeze the product contract and record the boundary

**Files:**
- Create: `docs/superpowers/specs/2026-08-19-cedar-product-and-rocksdb-boundary-design.md`
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `cmake/VerifyRocksKernelSourceContract.cmake`
- Test: `tests/test_public_header_contract.cmake`

**Interfaces:**
- Produces the authoritative list of installed Cedar headers, public link targets, forbidden public symbols, and the exact RocksDB fork revision contract used by later tasks.
- Consumes the existing clean-break Kernel design and the current `main` revision `54e9f24` as the baseline.

- [ ] **Step 1: Write the failing contract test.** Create a CMake script that configures a tiny consumer with only `#include <cedar/database.h>`, links `Cedar::cedar`, and fails if preprocessing finds `rocksdb/`, `RocksDb`, `CedarMaintenance`, `FactStoreMaintenance`, or `WalDurableCallback` in the installed public include tree.

- [ ] **Step 2: Run the contract test against the baseline.** Run:

  ```bash
  cmake -S . -B build-boundary-baseline -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
  ctest --test-dir build-boundary-baseline -R PublicHeaderContract --output-on-failure
  ```

  Expected result: the test reports the current leakage from `include/cedar/fact/fact_store.h` and the `RocksDbRuntimeMetrics` field in `include/cedar/database.h`.

- [ ] **Step 3: Write the design record.** State, in concrete terms, that `libcedar` and `Cedar::cedar` are the product surface; RocksDB is a private implementation dependency; `cedar::Database`, `cedar::Snapshot`, `cedar::Transaction`, fact value types, and public status types remain stable; and the binary/storage format is unchanged.

- [ ] **Step 4: Add source-contract assertions.** Extend `VerifyRocksKernelSourceContract.cmake` to require a pinned fork revision, Cedar fork provenance metadata, absence of the archived production source path from CMake inputs, and no installed `rocksdb/*.h` directory in the Cedar install tree.

- [ ] **Step 5: Commit the contract-only change.**

  ```bash
  git add docs/superpowers/specs/2026-08-19-cedar-product-and-rocksdb-boundary-design.md README.md CONTEXT.md cmake/VerifyRocksKernelSourceContract.cmake tests/test_public_header_contract.cmake
  git commit -m "docs: define Cedar product and storage ownership boundary"
  ```

### Task 2: Provision and pin the Cedar-controlled RocksDB fork

**Files:**
- Create in the Cedar-owned RocksDB repository: `PROVENANCE.md`
- Create in the Cedar-owned RocksDB repository: `docs/cedar-fork-maintenance.md`
- Modify: `.gitmodules`
- Modify: `cmake/CedarRocksDB.cmake`
- Modify: `cmake/VerifyRocksKernelSourceContract.cmake`
- Test: `tests/test_rocksdb_dependency.cc`

**Interfaces:**
- Produces a stable submodule URL and a reachable Cedar fork tag such as `cedar-v11.1.2-kernel.1` pointing at `7ddbe68ba322b235b1d78591487ffed842ba9567`.
- Consumes the existing RocksDB fork branch `codex/cedar-kernel-runtime` and its upstream base `3b4460891`.

- [ ] **Step 1: Create the remote before changing the superproject URL.** Provision a repository named `cedar-rocksdb` under the Cedar organization, make its default remote Cedar-controlled, and push the existing fork history and tag:

  ```bash
  : "${CEDAR_ROCKSDB_REMOTE_URL:?set this to the provisioned Cedar-controlled cedar-rocksdb URL}"
  git -C third_party/rocksdb remote rename origin upstream
  git -C third_party/rocksdb remote add origin "$CEDAR_ROCKSDB_REMOTE_URL"
  git -C third_party/rocksdb push origin codex/cedar-kernel-runtime
  git -C third_party/rocksdb tag cedar-v11.1.2-kernel.1 7ddbe68ba322b235b1d78591487ffed842ba9567
  git -C third_party/rocksdb push origin cedar-v11.1.2-kernel.1
  git -C third_party/rocksdb ls-remote origin refs/tags/cedar-v11.1.2-kernel.1
  ```

  The remote URL is an external provisioning input and must be recorded in the execution log before proceeding; no top-level commit may reference a commit that only exists in a local object database.

- [ ] **Step 2: Add fork provenance.** Record the RocksDB upstream commit, Cedar commit range, license obligations, supported upstream merge procedure, and the rule that Cedar-specific changes live under Cedar-named files or narrowly annotated core hooks.

- [ ] **Step 3: Update the submodule URL and pin.** Change `.gitmodules` to the Cedar fork URL, run `git submodule sync --recursive`, and leave the gitlink at the fork tag commit. Do not change the gitlink to an upstream-only commit.

- [ ] **Step 4: Make dependency verification remote-aware.** `test_rocksdb_dependency` and `VerifyRocksKernelSourceContract.cmake` must verify the exact commit, the Cedar fork marker, and that `git ls-remote` or an equivalent repository metadata check can resolve the pinned commit in release CI.

- [ ] **Step 5: Commit and verify a clean clone.** From a fresh temporary clone, run `git submodule update --init --recursive`, configure Cedar, and verify that `third_party/rocksdb` resolves to the pinned Cedar commit without local alternates or worktree paths.

  ```bash
  : "${CEDAR_SUPERPROJECT_URL:?set this to the Cedar superproject clone URL}"
  git clone "$CEDAR_SUPERPROJECT_URL" cedar-release-clone
  git -C cedar-release-clone submodule update --init --recursive
  git -C cedar-release-clone/third_party/rocksdb rev-parse HEAD
  ```

### Task 3: Split public Cedar interface from internal storage contracts

**Files:**
- Create: `include/cedar/runtime/runtime_metrics.h`
- Create: `include/cedar/storage/fact_scan.h`
- Create: `src/storage/facts/fact_store.h`
- Create: `src/storage/facts/fact_store_internal.h`
- Modify: `include/cedar/database.h`
- Modify: `include/cedar/snapshot.h`
- Modify: `include/cedar/transaction.h`
- Modify: `include/cedar/fact/fact_store.h`
- Test: `tests/test_public_header_contract.cmake`
- Test: `tests/test_kernel_interface.cc`

**Interfaces:**
- Public `cedar::Database` exposes Cedar metrics and semantic operations, not `RocksDbRuntimeMetrics`.
- Internal `cedar::internal::FactStore` retains the current storage operations used by `src/kernel`, but its declaration is reachable only through private include paths.
- `cedar::Snapshot` and `cedar::Transaction` continue to expose only Cedar fact, value, status, and visitor types.

- [ ] **Step 1: Define the public replacements before moving implementation.** Move the public part of `RocksDbRuntimeMetrics` into a Cedar-neutral `RuntimeMetrics` type with fields expressed as Cedar terms (`wal_retained_bytes`, `active_fact_bytes`, `pending_fact_compaction_bytes`, `block_cache_hits`, and `foreground_read_operations`). Keep column-family IDs, RocksDB ticker names, native yields, and grant IDs internal.

- [ ] **Step 2: Add a failing compile consumer.** Compile a source file that includes every installed Cedar header and asserts that no RocksDB header or native storage symbol is needed. Also compile a negative fixture that attempts to name `rocksdb::DB` after including Cedar headers; the fixture must fail only because the type is not provided by Cedar's public include path.

- [ ] **Step 3: Move storage-only declarations.** Relocate `FactStoreOptions`, `StoreCommitBatch`, `StoreCommitResult`, `StoreSnapshot`, maintenance request/result types, WAL callbacks, validation cache metrics, and storage-level column vectors from the installed `include/cedar/fact/fact_store.h` into `src/storage/facts/fact_store.h` or narrower internal headers. Keep only Cedar semantic fact types and scan specifications public.

- [ ] **Step 4: Replace database metrics exposure.** Change `Database::GetCommitPipelineMetrics()` to return a Cedar-neutral metrics structure and change `Database::SampleRuntimeMetrics()` to return `RuntimeMetrics`. Perform the translation in `src/kernel/database.cc`; no public header may include the internal fact-store header.

- [ ] **Step 5: Run the interface tests.** Run:

  ```bash
  cmake -S . -B build-public-interface -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
  cmake --build build-public-interface -j2
  ctest --test-dir build-public-interface -R 'PublicHeaderContract|KernelInterface' --output-on-failure
  ```

### Task 4: Introduce the private Cedar storage adapter seam

**Files:**
- Create: `src/storage/rocks/rocks_adapter.h`
- Create: `src/storage/rocks/rocks_adapter.cc`
- Create: `src/storage/rocks/rocks_types.h`
- Create: `src/storage/facts/fact_store.cc`
- Modify: `src/fact/fact_store.cc`
- Modify: `src/fact/rocksdb_config.cc`
- Modify: `src/kernel/database.cc`
- Modify: `src/kernel/database_impl.h`
- Tests: `tests/test_fact_store.cc`, `tests/test_fact_store_commit.cc`, `tests/test_rocksdb_lifecycle.cc`, `tests/test_columnar_fact_scan.cc`

**Interfaces:**
- `src/storage/rocks/rocks_adapter.h` is the only Cedar source seam that includes `rocksdb/*.h`.
- The adapter owns translation between Cedar storage requests and fork interfaces such as `WriteCedarEpoch`, `PollCedarMaintenance`, `RunCedarMaintenance`, and `ScanCedarParquetFacts`.
- `src/kernel` consumes Cedar-neutral storage operations and never constructs RocksDB types directly.

- [ ] **Step 1: Add a private adapter contract test.** Create an adapter fake used by a focused kernel test. The fake must record epoch writes, snapshots, maintenance grants, runtime samples, and close order, allowing kernel scheduling tests to run without a RocksDB object.

- [ ] **Step 2: Move RocksDB construction.** Move `rocksdb::Options`, column-family descriptors, cache, rate limiter, WAL path validation, and Cedar Parquet factory creation from `src/fact/fact_store.cc` into `src/storage/rocks/rocks_adapter.cc`.

- [ ] **Step 3: Move native translation.** Implement exact conversions between Cedar-neutral request/result types and fork-native types. Preserve all existing yield mappings, generation checks, input/output budgets, WAL-critical checks, and shutdown behavior.

- [ ] **Step 4: Make `FactStore` a Cedar storage module.** Keep fact encoding, metadata, validation, sequence visibility, snapshots, and commit decisions in `src/storage/facts`; inject the adapter through a private constructor or a private `RocksStorage` member. Do not add a second storage backend or public plugin interface.

- [ ] **Step 5: Delete direct RocksDB use from kernel files.** After the adapter builds, `rg -n 'rocksdb::|#include "rocksdb/' src/kernel src/storage/facts` must return no matches except explicitly documented private adapter forward declarations.

- [ ] **Step 6: Run the focused storage and recovery tests.**

  ```bash
  cmake --build build-public-interface -j2
  ctest --test-dir build-public-interface -R 'FactStore|ColumnarFactScan|RocksDbLifecycle|Recovery' --output-on-failure
  ```

### Task 5: Remove the old source tree and make the build Cedar-owned

**Files:**
- Delete: `archive/pre-rocksdb-kernel-2026-08-01/`
- Create: `src/runtime/` files moved from the current runtime implementation
- Modify: `CMakeLists.txt`
- Modify: `cmake/CedarRocksDB.cmake`
- Modify: `tests/CMakeLists.txt`
- Modify: `benchmarks/cedar_kernel_bench*.cc`
- Modify: `README.md`

**Interfaces:**
- Produces `Cedar::cedar` as the only public consumer target.
- `cedar_rocksdb` and the fork include directory remain private implementation targets.
- Release builds no longer read codecs or source files from `archive/pre-rocksdb-kernel-2026-08-01`.

- [ ] **Step 1: Move pinned codec inputs into a declared Cedar vendor location.** Place the exact LZ4 1.10.0 and Zstd 1.5.7 source snapshots under `third_party/cedar_codecs/`, add provenance and license files, and change `cmake/CedarRocksDB.cmake` to hash only that path. The codec bytes and compiler flags must remain unchanged from the baseline.

- [ ] **Step 2: Rename and move Cedar sources by ownership.** Move source files into `src/storage/facts`, `src/storage/rocks`, and `src/runtime` using `git mv`; preserve namespaces and function behavior in this step. Do not combine movement with algorithm changes.

- [ ] **Step 3: Make the target graph private.** Change `target_link_libraries(cedar_core PUBLIC cedar_rocksdb)` to a private link, add an exported `Cedar::cedar` alias around `cedar_core`, and install only Cedar headers, the Cedar target export, and Cedar package metadata.

- [ ] **Step 4: Add install-tree checks.** Install into a temporary prefix and assert that the prefix contains no `rocksdb/`, `table/cedar_parquet/`, `CedarEpochOptions`, or `CedarMaintenanceGrant` headers. The static library may contain internal symbols, but no public include or link target may require them.

- [ ] **Step 5: Remove the archive.** Delete the archived pre-Kernel source only after all CMake inputs, tests, benchmarks, and documentation point to the current tree. Verify:

  ```bash
  rg -n 'archive/pre-rocksdb-kernel|src/(db|tcypher|storage|transaction)|cedar_rocksdb' CMakeLists.txt cmake tests benchmarks README.md
  ```

  The only remaining `cedar_rocksdb` references must be private build implementation references and fork provenance checks.

- [ ] **Step 6: Run a clean configure.** Use a new build directory and verify no stale cache can hide archive references:

  ```bash
  cmake -S . -B build-clean-boundary -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Debug
  cmake --build build-clean-boundary -j2
  ```

### Task 6: Reorganize tests and benchmarks by public/internal ownership

**Files:**
- Move: `tests/test_kernel_interface.cc` to `tests/public/test_kernel_interface.cc`
- Move: fact and columnar tests to `tests/storage/`
- Move: recovery tests to `tests/recovery/`
- Move: bounded benchmark tests to `tests/performance/`
- Create: `tests/public/test_install_consumer.cc`
- Create: `tests/storage/test_rocks_adapter_contract.cc`
- Modify: `tests/CMakeLists.txt`
- Modify: `benchmarks/cedar_kernel_bench_options.cc`
- Modify: `benchmarks/cedar_kernel_bench_workload.cc`

**Interfaces:**
- Public tests include only installed Cedar headers and link `Cedar::cedar`.
- Storage tests may include private headers and use the adapter seam.
- Fork tests remain in the Cedar RocksDB repository and are run by the fork CI plus Cedar's pinned dependency contract.

- [ ] **Step 1: Add a public consumer test.** Build and run a separate executable from a temporary install prefix. Exercise `Database::Open`, transaction commit, snapshot read, columnar projection, close, and reopen using only `include/cedar`.

- [ ] **Step 2: Move internal tests without changing assertions.** Use `git mv` and update only include paths and CMake target names. Preserve the existing 376-test behavior as the first migration gate.

- [ ] **Step 3: Add adapter contract assertions.** Verify one Cedar epoch maps to one fork WAL write, a maintenance grant maps to exactly one native job, RocksDB errors are converted to Cedar status, and Cedar shutdown closes the adapter before releasing Cedar-owned scheduler state.

- [ ] **Step 4: Keep benchmark semantics stable.** Keep the current Lean/Kernel comparison inputs, 1 GiB production append configuration, N+1 counters, WAL metrics, and reopen/read verification. Rename report labels to `CedarLeanProfile` and `CedarKernelProfile` only if the corresponding modes remain behaviorally identical.

- [ ] **Step 5: Run the full Debug suite.**

  ```bash
  ctest --test-dir build-clean-boundary --output-on-failure
  ```

  Expected result: all existing tests plus the public consumer and adapter contract tests pass.

### Task 7: Add release packaging and fork maintenance contracts

**Files:**
- Create: `cmake/CedarConfig.cmake.in`
- Create: `cmake/CedarInstallContract.cmake`
- Create: `docs/release/cedar-source-and-binary-layout.md`
- Create: `docs/release/cedar-rocksdb-fork-policy.md`
- Modify: `CMakeLists.txt`
- Modify: `README.md`
- Modify: `.github/workflows/` or the repository's CI configuration
- Test: `tests/test_release_source_contract.cmake`

**Interfaces:**
- Consumers use `find_package(Cedar CONFIG REQUIRED)` and link `Cedar::cedar`.
- Release CI verifies the Cedar superproject and Cedar RocksDB fork from remotes, not from local worktrees or cache directories.

- [ ] **Step 1: Export the Cedar target.** Install `CedarTargets.cmake`, `CedarConfig.cmake`, version metadata, Cedar public headers, and license/notice files. Do not install RocksDB headers or a `cedar_rocksdb` target.

- [ ] **Step 2: Add a clean consumer project.** Configure a small external CMake project with:

  ```cmake
  find_package(Cedar CONFIG REQUIRED)
  add_executable(consumer main.cc)
  target_link_libraries(consumer PRIVATE Cedar::cedar)
  ```

  The consumer must compile without adding `third_party/rocksdb` or `src` to its include paths.

- [ ] **Step 3: Document fork policy.** Define the upstream merge cadence, Cedar patch review requirement, fork tag naming, security update procedure, license preservation, ABI expectations, and the rule that Cedar fork symbols are not a supported application interface.

- [ ] **Step 4: Add CI matrix gates.** Run clean clone/submodule verification, public install consumer, Debug CTest, ASAN/UBSAN/TSAN focused suites, Release CTest, and the bounded performance smoke test. Include a check that the fork's pinned commit resolves from the Cedar remote.

### Task 8: Sanitizer, crash-recovery, and performance acceptance

**Files:**
- Modify: `tests/recovery/test_crash_matrix.cc`
- Modify: `tests/test_kernel_bounded_benchmark.cc`
- Modify: `benchmarks/cedar_production_campaign.cc` if retained in the current benchmark set
- Create: `docs/release/cedar-kernel-acceptance.md`

**Interfaces:**
- Produces the release evidence required to claim the boundary reorganization preserved behavior and performance.
- Consumes the same workload and thresholds used by the current Cedar Kernel campaign.

- [ ] **Step 1: Run Debug correctness and recovery.** Require all public, storage, kernel, recovery, source-contract, and install-contract tests to pass.

- [ ] **Step 2: Run sanitizer suites.** Build and run ASAN, UBSAN, and TSAN configurations; include bounded writer regression, N+1 pipeline, close/reopen, crash matrix, and adapter contract tests. Any sanitizer failure blocks release.

- [ ] **Step 3: Run Release performance smoke.** Use the pinned 1 GiB production append profile, report commits, operations/sec, WAL sync latency, flush/compaction debt, N+1 eligible/promoted/discarded counts, writer failures, background errors, and real close/reopen/read verification.

- [ ] **Step 4: Run the sustained campaign.** Retain the existing 1800-second campaign for final release evidence; use a shorter bounded smoke run for pull-request gating. Compare Cedar Lean and Cedar Kernel only with identical workload, storage profile, and WAL settings.

- [ ] **Step 5: Publish the acceptance record.** Record compiler, OS, fork commit/tag, Cedar commit, codec hashes, CMake flags, test counts, sanitizer outcomes, benchmark results, and any residual limitations in `docs/release/cedar-kernel-acceptance.md`.

### Task 9: Final source and branch cleanup

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`
- Modify: `docs/superpowers/plans/2026-08-19-cedar-product-and-rocksdb-boundary.md`

- [ ] **Step 1: Remove obsolete local branches only after the new main and fork tag are verified.** Keep `main` in the superproject and the Cedar fork's release tag/maintenance branch in the fork repository. Delete local branches containing superseded CAC, temporal, pre-Kernel, and old benchmark implementations only after their commits are represented in the archive history or the new source tree.

- [ ] **Step 2: Remove stale worktree registrations.** Run `git worktree prune` and verify that only the main worktree and explicitly externally managed worktrees remain.

- [ ] **Step 3: Run final repository checks.**

  ```bash
  git diff --check
  git status --short --branch
  git submodule status --recursive
  git branch --format='%(refname:short)'
  ctest --test-dir build-release --output-on-failure
  ```

- [ ] **Step 4: Mark the plan complete only with evidence.** The completion record must state the Cedar fork remote/tag, public install result, test totals, sanitizer result, and Release benchmark result. A local-only fork commit or a public header that exposes RocksDB is an incomplete implementation.

## Verification Matrix

| Gate | Command/profile | Required result |
|---|---|---|
| Public headers | `PublicHeaderContract`, external install consumer | No RocksDB dependency in public includes or target usage |
| Fork reachability | clean clone + `git submodule update --init --recursive` | Pinned Cedar fork commit resolves from Cedar remote |
| Debug correctness | `ctest --test-dir build-debug --output-on-failure` | Existing and new tests pass |
| Crash recovery | recovery test target and crash matrix | WAL/recovery/visibility invariants preserved |
| ASAN/UBSAN/TSAN | sanitizer CMake profiles | No sanitizer findings |
| Release | `CMAKE_BUILD_TYPE=Release` | Install and tests pass |
| Performance | bounded smoke plus sustained campaign | Existing throughput and correctness gates remain satisfied |
| Source cleanup | source/install contract scripts | No archived production source or exposed RocksDB headers |

## Explicit Non-Goals

- Do not replace RocksDB WAL, recovery, MemTable, VersionSet, MANIFEST, or sequence allocation.
- Do not introduce a second WAL, a second authoritative commit log, or a public storage plugin ABI.
- Do not rename Cedar's semantic public types merely to mirror RocksDB terminology.
- Do not optimize algorithms while moving files; performance changes require a separate measured change.
- Do not publish or force-push any remote until the Cedar organization/repository owner has provisioned the fork and reviewed its license/provenance metadata.
