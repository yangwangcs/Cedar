#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 ABSOLUTE_OUTPUT_DIRECTORY" >&2
  exit 2
fi

output_dir="$1"
if [[ "$output_dir" != /* ]]; then
  echo "output directory must be absolute" >&2
  exit 2
fi
mkdir -p "$output_dir"

bench="${CEDAR_BENCH:-./build-release-kernel/cedar_kernel_bench}"
if [[ ! -x "$bench" ]]; then
  echo "benchmark executable is not executable: $bench" >&2
  exit 2
fi

{
  printf 'commit='; git rev-parse HEAD
  printf 'rocksdb_revision='; git -C src/engine/rocksdb rev-parse HEAD 2>/dev/null || printf 'embedded-tree\n'
  printf 'host='; uname -a
  printf 'compiler='; c++ --version | head -n 1
  printf 'benchmark=%s\n' "$bench"
  printf 'seed=20260820\n'
  printf 'workload=mixed-90-write-10-point-read\n'
} > "$output_dir/metadata.txt"

common=(
  --seed-db "$output_dir/seed-db"
  --seed 20260820
  --operations 2048
  --read-operations 2048
  --verify-reopen true
)

run_case() {
  local profile="$1"
  local campaign="$2"
  local duration="$3"
  local name="$4"
  local prepare="$5"
  local workload="$6"
  local writer_clients="$7"
  local database_path="$output_dir/${name}-db"
  "$bench" \
    --path "$database_path" \
    --database-path "$database_path" \
    "${common[@]}" \
    --workload "$workload" \
    --writer-clients "$writer_clients" \
    --prepare-seed "$prepare" \
    --profile "$profile" \
    --campaign "$campaign" \
    --duration-seconds "$duration" \
    > "$output_dir/${name}.csv"
}

run_case lean warm 30 lean-30s true mixed-90-write-10-point-read 1
run_case kernel warm 30 kernel-30s false mixed-90-write-10-point-read 1
run_case lean preflight 60 lean-60s false mixed-90-write-10-point-read 1
run_case kernel preflight 60 kernel-60s false mixed-90-write-10-point-read 1
run_case lean preflight 300 lean-300s false mixed-90-write-10-point-read 1
run_case kernel preflight 300 kernel-300s false mixed-90-write-10-point-read 1
run_case lean sustained 1800 lean-1800s false property-put 2
run_case kernel sustained 1800 kernel-1800s false property-put 2
