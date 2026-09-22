#!/usr/bin/env bash
set -euo pipefail

nibble=""; byte=""; candidate=""; output=""
while (($#)); do
  case "$1" in
    --nibble) nibble="$2"; shift 2 ;;
    --byte) byte="$2"; shift 2 ;;
    --candidate) candidate="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [[ "$nibble" != /* || "$byte" != /* || "$output" != /* ||
      ! -x "$nibble" || ! -x "$byte" ]]; then
  echo "--nibble, --byte, and --output must be absolute; binaries must be executable" >&2
  exit 2
fi
if [[ -n "$candidate" && ( "$candidate" != /* || ! -x "$candidate" ) ]]; then
  echo "--candidate must be an executable absolute path" >&2; exit 2
fi
if [[ -e "$output" && -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $output" >&2; exit 2
fi
mkdir -p "$output/matrix-raw"

entries=131072; seeds=(20260920 20260921 20260922); repeats=(1 2 3 4 5)
if [[ "${CEDAR_RETENTION_TEST_MODE:-0}" == 1 ]]; then
  entries=64; seeds=(20260920); repeats=(1)
fi
header="revision,workload,seed,entries,allocated_handles,writers,repeat,seconds,ops,p50_ns,p95_ns,p99_ns,peak_rss_bytes,arena_bytes,bytes_per_handle,result_hash,errors,active_scan_ns,frozen_first_scan_ns,frozen_ready_scan_ns,phase,elapsed_ns,index_insert_ns,allocate_and_insert_ns,active_scan_ns_phase,freeze_prepare_ns,first_frozen_scan_ns,ready_frozen_scan_ns"
printf 'variant,%s\n' "$header" > "$output/matrix-raw/matrix.csv"

run_one() {
  local variant="$1" binary="$2" implementation="$3" seed="$4"
  local writers="$5" phase="$6" repeat="$7"
  local raw="$output/matrix-raw/${variant}-${seed}-${writers}-${phase}-${repeat}.csv"
  "$binary" --implementation "$implementation" --entries "$entries" \
    --workload random --writers "$writers" --seed "$seed" \
    --revision "$variant" --phase "$phase" > "$raw"
  [[ "$(awk -F, 'NR == 2 { print NF }' "$raw")" == 28 ]] || { echo "field-count contract failed: $raw" >&2; exit 1; }
  [[ "$(awk -F, 'NR == 2 { print $9 }' "$raw")" == "$entries" ]] || { echo "operation-count contract failed: $raw" >&2; exit 1; }
  [[ "$(awk -F, 'NR == 2 { print $17 }' "$raw")" == 0 ]] || { echo "error contract failed: $raw" >&2; exit 1; }
  [[ "$(awk -F, 'NR == 2 { print $21 }' "$raw")" == "$phase" ]] || { echo "phase contract failed: $raw" >&2; exit 1; }
  [[ "$(awk -F, 'NR == 2 { print $6 }' "$raw")" == "$writers" ]] || { echo "writer contract failed: $raw" >&2; exit 1; }
  last_hash="$(awk -F, 'NR == 2 { print $16 }' "$raw")"
  printf '%s,%s\n' "$variant" "$(sed -n '2p' "$raw")" >> "$output/matrix-raw/matrix.csv"
}

run_group() {
  local seed="$1" writers="$2" phase="$3" repeat="$4"
  local order=(nibble byte)
  if [[ "$writers" == 1 ]]; then
    order+=(vector)
  elif [[ "$phase" == memtable ]]; then
    order+=(skiplist)
  fi
  if [[ -n "$candidate" ]]; then
    order+=(candidate)
  fi
  if (( (seed + writers + repeat) % 2 == 1 )); then
    local reversed=() index
    for ((index = ${#order[@]} - 1; index >= 0; --index)); do
      reversed+=("${order[index]}")
    done
    order=("${reversed[@]}")
  fi
  local expected_hash=""
  for variant in "${order[@]}"; do
    case "$variant" in
      nibble) run_one nibble "$nibble" radix "$seed" "$writers" "$phase" "$repeat" ;;
      byte) run_one byte "$byte" radix "$seed" "$writers" "$phase" "$repeat" ;;
      vector) run_one vector "$byte" vector "$seed" "$writers" "$phase" "$repeat" ;;
      skiplist) run_one skiplist "$byte" skiplist "$seed" "$writers" "$phase" "$repeat" ;;
      candidate) run_one candidate "$candidate" radix "$seed" "$writers" "$phase" "$repeat" ;;
    esac
    if [[ -n "$expected_hash" && "$last_hash" != "$expected_hash" ]]; then
      echo "result hash mismatch for seed=$seed writers=$writers phase=$phase repeat=$repeat" >&2
      exit 1
    fi
    expected_hash="$last_hash"
  done
}

for seed in "${seeds[@]}"; do
  for repeat in "${repeats[@]}"; do
    run_group "$seed" 1 index "$repeat"
    run_group "$seed" 1 memtable "$repeat"
    run_group "$seed" 4 memtable "$repeat"
    run_group "$seed" 8 memtable "$repeat"
  done
done
