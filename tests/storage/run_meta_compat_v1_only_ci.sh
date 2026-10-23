#!/bin/bash
# Copyright 2026 The Cedar Authors
# Licensed under the Apache License, Version 2.0.
#
# CI fixture for the meta sequence one-way gate (F17).
#
# Builds the CEDAR_META_SEQUENCE_V1_ONLY compatibility profile in its own build
# directory and runs test_meta_compat there. That configuration compiles the
# real read gate to "on": a format-2 database must fail Open with an actionable
# Corruption, while a v1 database still opens and accepts v1 commits. A normal
# build exercises the same gate through the internal test hook, so this script
# is the end-to-end proof that an old (pre-v2) binary rejects the upgrade.
#
# Usage:
#   tests/storage/run_meta_compat_v1_only_ci.sh [build-dir] [extra cmake args...]
#
# Environment:
#   CEDAR_ROCKSDB_CACHE_ROOT  optional reusable RocksDB cache (as for any build)
#   CEDAR_CC / CEDAR_CXX      optional C/C++ compilers (for example the Cedar
#                             wrapper compilers used by the development image)

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source_dir=$(cd "${script_dir}/../.." && pwd)
build_dir=${1:-"${source_dir}/build-meta-v1-only"}
if [[ $# -gt 0 ]]; then shift; fi

cmake_args=(
  -S "${source_dir}"
  -B "${build_dir}"
  -G Ninja
  -DCMAKE_BUILD_TYPE=Debug
  -DBUILD_TESTS=ON
  -DCEDAR_META_SEQUENCE_V1_ONLY=ON
)

if [[ -n "${CEDAR_CC:-}" ]]; then
  cmake_args+=("-DCMAKE_C_COMPILER=${CEDAR_CC}")
fi
if [[ -n "${CEDAR_CXX:-}" ]]; then
  cmake_args+=("-DCMAKE_CXX_COMPILER=${CEDAR_CXX}")
fi
if [[ -n "${CEDAR_ROCKSDB_CACHE_ROOT:-}" ]]; then
  cmake_args+=("-DCEDAR_ROCKSDB_CACHE_ROOT=${CEDAR_ROCKSDB_CACHE_ROOT}")
fi

cmake "${cmake_args[@]}" "$@"
cmake --build "${build_dir}" --target test_meta_compat
ctest --test-dir "${build_dir}" -R '^MetaCompat' --output-on-failure
