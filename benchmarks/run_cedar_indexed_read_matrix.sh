#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${ROOT}/build-indexed-release"
OUT="/tmp/cedar-indexed-read-matrix.csv"
SECONDS_PER_CASE=1
ENTITIES=(1 10 100 1000)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir) BUILD="$2"; shift 2 ;;
    --seconds) SECONDS_PER_CASE="$2"; shift 2 ;;
    --output) OUT="$2"; shift 2 ;;
    --entities) IFS=, read -r -a ENTITIES <<< "$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake --build "$BUILD" --target cedar_read_side_complexity_bench -j1

tmp="$(mktemp -d /tmp/cedar-indexed-read-matrix.XXXXXX)"
trap 'rm -rf "$tmp"' EXIT
first=1
for entities in "${ENTITIES[@]}"; do
  csv="${tmp}/${entities}.csv"
  "$BUILD/cedar_read_side_complexity_bench" \
    --entities "$entities" --seconds "$SECONDS_PER_CASE" > "$csv"
  if [[ "$first" -eq 1 ]]; then
    cp "$csv" "$OUT"
    first=0
  else
    tail -n +2 "$csv" >> "$OUT"
  fi
done
printf 'wrote %s\n' "$OUT"
