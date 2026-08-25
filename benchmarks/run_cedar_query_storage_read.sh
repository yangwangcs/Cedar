#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${ROOT}/build-tcypher"
export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake --build "${BUILD}" --target cedar_query_storage_read_bench -j1
exec "${BUILD}/cedar_query_storage_read_bench" "${1:-1}" "${2:-1000}"
