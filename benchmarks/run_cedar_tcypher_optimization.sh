#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD=${CEDAR_BUILD_DIR:-"$ROOT/build-tcypher"}
SECONDS_RUN=${1:-10}
SESSIONS=${2:-1}
export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake --build "$BUILD" -j1 --target cedar_tcypher_optimization_bench

echo "# typed selectivity sweep"
for selected in 1 10 100; do
  "$BUILD/cedar_tcypher_optimization_bench" --workload typed \
    --seconds "$SECONDS_RUN" --sessions "$SESSIONS" \
    --total-edges 100 --selected-edges "$selected"
done

echo "# point concurrency"
"$BUILD/cedar_tcypher_optimization_bench" --workload point \
  --seconds "$SECONDS_RUN" --sessions 1 --total-edges 1 --selected-edges 0
"$BUILD/cedar_tcypher_optimization_bench" --workload point \
  --seconds "$SECONDS_RUN" --sessions 8 --total-edges 1 --selected-edges 0
"$BUILD/cedar_tcypher_optimization_bench" --workload point \
  --seconds "$SECONDS_RUN" --sessions 32 --total-edges 1 --selected-edges 0
