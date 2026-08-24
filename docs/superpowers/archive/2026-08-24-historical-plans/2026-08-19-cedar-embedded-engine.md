# Cedar Embedded Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Cedar Kernel engine source a Cedar-owned `src/engine` tree,
removing the RocksDB submodule and all external fork publication requirements
without changing Cedar's storage semantics or runtime ownership model.

**Architecture:** Vendor the exact Cedar Kernel engine revision under
`src/engine/rocksdb`, retain its internal `rocksdb::` naming, and record its
upstream/Cedar provenance in-tree. Build the private engine target from that
directory using a deterministic source digest; Cedar public and storage
boundaries remain exactly as established by the product-boundary work.

**Tech Stack:** C++20, CMake, GoogleTest, embedded RocksDB 11.1.2-derived
Cedar Kernel source, Git, static library packaging, Debug/ASAN/UBSAN/TSAN/
Release validation.

## Global Constraints

- Cedar retains one engine WAL; the engine owns WAL/recovery/MemTable/VersionSet/MANIFEST/sequence allocation/native flush and compaction execution.
- Cedar facts remain authoritative logical columnar facts; Cedar Parquet files are not a second WAL or recovery authority.
- Cedar owns admission, runtime sampling policy, maintenance scheduling and grants, and public transaction/snapshot semantics.
- `src/engine/rocksdb` remains private: no installed Cedar header, package target, documentation contract, or application source may name its headers or `rocksdb::` types.
- Preserve the imported `rocksdb::` namespace, directory layout, licenses, notices, and source attribution; all net-new Cedar engine files belong under `src/engine/cedar`.
- Do not create, contact, or depend on a `cedar-rocksdb` remote. Other existing third-party submodules remain unchanged.
- Existing Kernel commit ordering, crash recovery, fact format, visibility, and benchmark gates must be behaviorally unchanged.

---

### Task 1: Define and test the embedded-engine source contract

**Files:**
- Create: `tests/test_embedded_engine_contract.cmake`
- Modify: `tests/CMakeLists.txt`
- Modify: `cmake/VerifyRocksKernelSourceContract.cmake`
- Modify: `README.md`
- Modify: `CONTEXT.md`

**Interfaces:**
- Consumes: the private `cedar_rocksdb` target and the public-header contract.
- Produces: `EmbeddedEngineContract`, a CTest that rejects a RocksDB
  submodule/gitlink, missing provenance/licences, nested Git metadata, or a
  public reference to an engine include path.

- [ ] **Step 1: Write the failing embedded-tree contract.** Add
  `tests/test_embedded_engine_contract.cmake` with checks equivalent to:

  ```cmake
  set(engine_root "${PROJECT_SOURCE_DIR}/src/engine/rocksdb")
  if(NOT EXISTS "${engine_root}/CMakeLists.txt")
    message(FATAL_ERROR "Embedded Cedar engine source is missing")
  endif()
  if(EXISTS "${engine_root}/.git")
    message(FATAL_ERROR "Embedded engine must not contain nested Git metadata")
  endif()
  if(EXISTS "${PROJECT_SOURCE_DIR}/third_party/rocksdb")
    message(FATAL_ERROR "RocksDB must not remain a Cedar submodule")
  endif()
  ```

- [ ] **Step 2: Run it against the pre-migration tree.**

  ```bash
  cmake -S . -B build-embedded-contract-baseline -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
  ctest --test-dir build-embedded-contract-baseline -R EmbeddedEngineContract --output-on-failure
  ```

  Expected: FAIL because `src/engine/rocksdb` does not yet exist and
  `third_party/rocksdb` is still present.

- [ ] **Step 3: Register the contract and add source ownership checks.** Add
  a CTest registration in `tests/CMakeLists.txt`; extend
  `VerifyRocksKernelSourceContract.cmake` to require
  `src/engine/rocksdb/PROVENANCE.md`, `src/engine/rocksdb/LICENSE.Apache`, and
  `src/engine/cedar/README.md`, and to reject both `.gitmodules` RocksDB
  declarations and `third_party/rocksdb` source references.

- [ ] **Step 4: Replace publication wording.** Change `README.md` and
  `CONTEXT.md` from “pinned Cedar fork submodule” to “embedded Cedar engine”;
  retain the factual upstream baseline and license attribution.

- [ ] **Step 5: Run the contract after migration work is staged.**

  ```bash
  ctest --test-dir build-embedded-contract -R 'EmbeddedEngineContract|RocksKernelSourceContract' --output-on-failure
  ```

  Expected: PASS without a network request or `git submodule update`.

- [ ] **Step 6: Commit the contract.**

  ```bash
  git add tests/test_embedded_engine_contract.cmake tests/CMakeLists.txt cmake/VerifyRocksKernelSourceContract.cmake README.md CONTEXT.md
  git commit -m "test: require embedded Cedar engine source"
  ```

### Task 2: Import the immutable Cedar engine source

**Files:**
- Create: `src/engine/rocksdb/**` from Git object `274c7789ff17c062a64a8b43be2d52093619cbcc`
- Create: `src/engine/cedar/README.md`
- Delete: Gitlink `third_party/rocksdb`
- Modify: `.gitmodules`

**Interfaces:**
- Consumes: the checked-out local Cedar Kernel engine commit that contains the
  existing Cedar durable-WAL, maintenance and Parquet implementation.
- Produces: source files tracked directly by the Cedar superproject, with no
  nested repository or external fetch requirement.

- [ ] **Step 1: Verify the source identity and clean engine content.**

  ```bash
  git -C third_party/rocksdb rev-parse HEAD
  git -C third_party/rocksdb diff --exit-code
  git -C third_party/rocksdb status --porcelain
  ```

  Expected: `274c7789ff17c062a64a8b43be2d52093619cbcc` and no uncommitted
  engine changes. If this does not hold, stop before copying and record the
  exact discrepancy.

- [ ] **Step 2: Materialise only tracked source at the fixed commit.**

  ```bash
  rm -rf src/engine/rocksdb
  mkdir -p src/engine/rocksdb
  git -C third_party/rocksdb archive 274c7789ff17c062a64a8b43be2d52093619cbcc | tar -x -C src/engine/rocksdb
  test ! -e src/engine/rocksdb/.git
  ```

  The archive operation prevents submodule metadata, build outputs, and local
  untracked files from entering Cedar history.

- [ ] **Step 3: Add Cedar extension ownership documentation.** Create
  `src/engine/cedar/README.md` containing:

  ```markdown
  # Cedar engine extensions

  This directory contains Cedar-owned extensions to the embedded engine.
  Do not include these headers from `include/cedar`. Existing Cedar changes
  inside `../rocksdb` retain their narrow provenance annotations until they
  are deliberately moved here in a separately tested change.
  ```

- [ ] **Step 4: Remove only the RocksDB submodule declaration and gitlink.**

  ```bash
  git rm -f third_party/rocksdb
  git config -f .gitmodules --remove-section submodule.third_party/rocksdb
  git add .gitmodules src/engine
  ```

  Do not edit entries for Arrow, Boost, RapidJSON, Thrift, or xsimd.

- [ ] **Step 5: Prove the source has exactly the expected content.**

  ```bash
  test ! -e src/engine/rocksdb/.git
  test -f src/engine/rocksdb/PROVENANCE.md
  test -f src/engine/rocksdb/LICENSE.Apache
  git ls-files src/engine/rocksdb | wc -l
  ```

  Expected: no nested Git repository, retained provenance/licence files, and
  approximately 2,187 tracked engine paths.

- [ ] **Step 6: Commit the imported engine atomically.**

  ```bash
  git add -A src/engine .gitmodules third_party/rocksdb
  git commit -m "feat: embed Cedar storage engine source"
  ```

### Task 3: Build the engine solely from Cedar-owned sources

**Files:**
- Modify: `cmake/CedarRocksDB.cmake`
- Modify: `CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Modify: `cmake/VerifyRocksKernelSourceContract.cmake`

**Interfaces:**
- Consumes: `src/engine/rocksdb/CMakeLists.txt` and every tracked source below
  that directory.
- Produces: the existing private `cedar_rocksdb_static` and `cedar_rocksdb`
  targets with a cache manifest carrying `engine_baseline` and
  `engine_source_digest` rather than a nested Git revision/patch digest.

- [ ] **Step 1: Replace the source-root declaration.** Change the top of
  `cmake/CedarRocksDB.cmake` to:

  ```cmake
  set(CEDAR_ENGINE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/engine/rocksdb")
  set(CEDAR_ENGINE_BASELINE "rocksdb-11.1.2+cedar-kernel-7ddbe68")
  if(NOT EXISTS "${CEDAR_ENGINE_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Cedar requires embedded engine source")
  endif()
  ```

- [ ] **Step 2: Replace nested-Git cache input with a deterministic digest.**
  Enumerate all non-directory files below `CEDAR_ENGINE_SOURCE_DIR`, sort the
  list, hash each file, append its source-relative path and hash to
  `CEDAR_ENGINE_SOURCE_DIGEST_INPUT`, and compute:

  ```cmake
  string(SHA256 CEDAR_ENGINE_SOURCE_DIGEST "${CEDAR_ENGINE_SOURCE_DIGEST_INPUT}")
  ```

  Remove all `git -C ${CEDAR_ROCKSDB_SOURCE_DIR}` calls and the
  untracked-file/working-tree patch digest logic.

- [ ] **Step 3: Preserve target and build options while changing paths.**
  Use `CEDAR_ENGINE_SOURCE_DIR` in the nested configure command, CMake
  include directories, test SyncPoint sources, and Cedar Parquet test sources.
  Keep static linkage, codecs, disabled unused services, sanitizer flags, and
  the private `cedar_rocksdb` target unchanged.

- [ ] **Step 4: Update the manifest and cache root identity.** Write:

  ```cmake
  file(WRITE "${CEDAR_ROCKSDB_MANIFEST}"
       "engine_baseline=${CEDAR_ENGINE_BASELINE}\nengine_source_digest=${CEDAR_ENGINE_SOURCE_DIGEST}\ncodec_sources=${CEDAR_ROCKSDB_CODEC_SOURCES}\ncodec_flags=${CEDAR_ROCKSDB_CODEC_FLAGS}\ncodec_linkage=lz4:${CEDAR_ROCKSDB_LZ4_LIBRARY}|zstd:${CEDAR_ROCKSDB_ZSTD_LIBRARY}\nbuild_type=${CEDAR_ROCKSDB_BUILD_TYPE}\nfingerprint=${CEDAR_ROCKSDB_FINGERPRINT}\nlinkage=static\n")
  ```

  Prefix cache directories with the baseline and a fingerprint including the
  source digest. A change to any tracked engine source must select a distinct
  cache directory.

- [ ] **Step 5: Verify configure/build and cache invalidation.**

  ```bash
  cmake -S . -B build-embedded-contract -DBUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug
  cmake --build build-embedded-contract -j2
  ctest --test-dir build-embedded-contract -R 'EmbeddedEngineContract|RocksKernelSourceContract' --output-on-failure
  ```

  Expected: all tests pass with the nested build source path under
  `src/engine/rocksdb`; its manifest identifies the embedded baseline and
  source digest.

- [ ] **Step 6: Commit the build transition.**

  ```bash
  git add cmake/CedarRocksDB.cmake CMakeLists.txt tests/CMakeLists.txt cmake/VerifyRocksKernelSourceContract.cmake
  git commit -m "build: compile Cedar embedded engine"
  ```

### Task 4: Verify packages and source isolation

**Files:**
- Modify: `tests/test_public_header_contract.cmake`
- Modify: `tests/test_install_consumer.cmake`
- Modify: `cmake/VerifyRocksKernelSourceContract.cmake`
- Modify: `README.md`

**Interfaces:**
- Consumes: the private engine target and installed `Cedar::cedar` export.
- Produces: evidence that an installed consumer needs neither engine headers
  nor submodule setup, while a source consumer's direct engine include fails.

- [ ] **Step 1: Add an install-tree negative scan.** Extend the package test
  to fail if the install prefix contains `rocksdb/`, `engine/rocksdb/`,
  `table/cedar_parquet/`, or an exported `cedar_rocksdb` CMake target.

- [ ] **Step 2: Add a fresh source-clone validation script.** The script must
  clone the Cedar repository, check out the tested commit, run
  `git submodule status`, assert that no line names `third_party/rocksdb`, and
  configure/build a public consumer without executing `git submodule update`.

- [ ] **Step 3: Run the package and clone checks.**

  ```bash
  ctest --test-dir build-embedded-contract -R 'PublicHeaderContract|InstallConsumer|EmbeddedEngineContract' --output-on-failure
  git clone --no-local . "$(mktemp -d)/cedar-embedded-clone"
  ```

  Expected: the clone contains the engine source directly and has no RocksDB
  submodule entry.

- [ ] **Step 4: Commit the release checks.**

  ```bash
  git add tests/test_public_header_contract.cmake tests/test_install_consumer.cmake cmake/VerifyRocksKernelSourceContract.cmake README.md
  git commit -m "test: verify Cedar embedded engine release boundary"
  ```

### Task 5: Run correctness and performance acceptance

**Files:**
- Modify only if a failing test demonstrates a migration defect.
- Test: `tests/{public,kernel,storage,recovery,performance}/**`
- Test: `benchmarks/cedar_kernel_bench*`

**Interfaces:**
- Consumes: the fully embedded engine and unmodified Cedar Kernel behavior.
- Produces: Debug, sanitizer, Release and bounded benchmark evidence for the
  migration commit.

- [ ] **Step 1: Run a fresh Debug build and full suite.**

  ```bash
  cmake -S . -B build-debug-embedded -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Debug
  cmake --build build-debug-embedded -j2
  ctest --test-dir build-debug-embedded --output-on-failure
  ```

  Expected: all registered tests pass.

- [ ] **Step 2: Run focused sanitizer gates.**

  ```bash
  cmake -S . -B build-asan-embedded -DBUILD_TESTS=ON -DCEDAR_ENABLE_ASAN=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo
  cmake --build build-asan-embedded -j2
  ctest --test-dir build-asan-embedded -R 'FactStore|Recovery|RocksDbLifecycle|ColumnarFactScan|KernelInterface' --output-on-failure
  ```

  Repeat with `CEDAR_ENABLE_UBSAN=ON` and `CEDAR_ENABLE_TSAN=ON`; record
  platform-specific skips separately from failures.

- [ ] **Step 3: Run the Release correctness gate.**

  ```bash
  cmake -S . -B build-release-embedded -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
  cmake --build build-release-embedded -j2
  ctest --test-dir build-release-embedded -R 'FactStore|Recovery|RocksDbLifecycle|ColumnarFactScan|KernelInterface|PublicHeaderContract|InstallConsumer' --output-on-failure
  ```

- [ ] **Step 4: Run the existing bounded Release performance workload.**

  ```bash
  ctest --test-dir build-release-embedded -R 'Bounded.*Benchmark|NPlusOne' --output-on-failure
  ```

  Report throughput, N+1 counters, WAL metrics, reopen/read verification, and
  whether the run meets its existing sustained-run classification. Do not
  compare it to Debug results or alter the workload to obtain a better number.

- [ ] **Step 5: Commit only migration fixes and audit history.**

  ```bash
  git diff --check
  git status --short
  git log --oneline --max-count=6
  ```

  Commit any verified migration-specific corrections with a focused message;
  do not fold unrelated cleanup into this change.
