#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD=${CEDAR_BUILD_DIR:-"$ROOT/build-tcypher"}
SECONDS_RUN=${1:-5}
export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake --build "$BUILD" -j1 --target cedar_tcypher_fusion_bench
exec "$BUILD/cedar_tcypher_fusion_bench" --seconds "$SECONDS_RUN"
