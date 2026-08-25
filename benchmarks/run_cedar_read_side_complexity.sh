#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${ROOT}/build-tcypher"
SECONDS_PER_CASE=1
OUT=/tmp/cedar-read-side-complexity.csv
SESSIONS=1
ENTITIES=(1 10 100 1000 10000)
LONG_READER=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --seconds) SECONDS_PER_CASE="$2"; shift 2 ;;
    --sessions) SESSIONS="$2"; shift 2 ;;
    --entities) IFS=, read -r -a ENTITIES <<< "$2"; shift 2 ;;
    --long-reader) LONG_READER=1; shift ;;
    --output) OUT="$2"; shift 2 ;;
    *) SECONDS_PER_CASE="$1"; shift
       if [[ $# -gt 0 && "$1" != --* ]]; then OUT="$1"; shift; fi ;;
  esac
done
if [[ "${SESSIONS}" != 1 && "${SESSIONS}" != 8 && "${SESSIONS}" != 32 ]]; then
  echo "sessions must be 1, 8, or 32" >&2
  exit 2
fi
export CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1
export CMAKE_BUILD_PARALLEL_LEVEL=1
cmake --build "${BUILD}" --target cedar_read_side_complexity_bench -j1
tmp="$(mktemp -d /tmp/cedar-read-side-complexity.XXXXXX)"
trap 'rm -rf "${tmp}"' EXIT
first=1
for entities in "${ENTITIES[@]}"; do
  for session in $(seq 1 "${SESSIONS}"); do
    csv="${tmp}/${entities}.${session}.csv"
    extra=()
    if [[ "${LONG_READER}" -eq 1 ]]; then extra+=(--long-reader); fi
    "${BUILD}/cedar_read_side_complexity_bench" --entities "${entities}" \
      --seconds "${SECONDS_PER_CASE}" "${extra[@]}" >"${csv}" &
    pids[${session}]=$!
  done
  for session in $(seq 1 "${SESSIONS}"); do wait "${pids[${session}]}"; done
  for session in $(seq 1 "${SESSIONS}"); do
    csv="${tmp}/${entities}.${session}.csv"
    if [[ "${first}" -eq 1 ]]; then
      cp "${csv}" "${OUT}"
      first=0
    else
      tail -n +2 "${csv}" >>"${OUT}"
    fi
  done
done
echo "wrote ${OUT}"
