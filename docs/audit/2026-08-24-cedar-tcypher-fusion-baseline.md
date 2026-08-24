# Cedar T-Cypher Fusion Baseline

Date: 2026-08-24

Branch: `codex/cedar-tcypher-fusion`

Base commit: `1b627b4` (`main`)

Host: Apple arm64, Darwin 25.5.0, 16 GiB physical memory.

## Build

```bash
export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake -S . -B build-tcypher -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=OFF
cmake --build build-tcypher -j1
```

Result: configure succeeded; build succeeded at 100%. No parallel build workers were used.

## Focused baseline suite

```bash
ctest --test-dir build-tcypher --output-on-failure \
  -R 'Query|Temporal|Fact|Kernel|RocksDBProfile|Recovery'
```

Result: `476/476` passed, `0` failed. Total test time: `94.44 s`.

The baseline includes Cedar query planning/runtime, temporal models and paths, canonical and
columnar fact scans, RocksDB lifecycle/profile, kernel commit/recovery, and source/install
contracts. It predates all new T-Cypher fusion implementation code.

## Final fusion verification

- Build: `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1 CMAKE_BUILD_PARALLEL_LEVEL=1 cmake --build build-tcypher -j1`
- Full test command: `ctest --test-dir build-tcypher --output-on-failure`
- Result: `741/741` passed, `549.75s` total CTest time.
- Benchmark peak RSS: `28,868,608` bytes for the 2-second vertex-scan smoke run.

The final run includes the added T-Cypher, server, Bolt envelope, and benchmark contract tests.
It is not a sustained production capacity claim; detailed workload limits are recorded in the
fusion evidence report.
