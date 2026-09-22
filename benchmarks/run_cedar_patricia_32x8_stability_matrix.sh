#!/usr/bin/env bash
set -euo pipefail

nibble=""
byte=""
baseline=""
candidate=""
output=""
while (($#)); do
  case "$1" in
    --nibble) nibble="$2"; shift 2 ;;
    --byte) byte="$2"; shift 2 ;;
    --baseline) baseline="$2"; shift 2 ;;
    --candidate) candidate="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

for argument in nibble byte baseline candidate; do
  binary="${!argument}"
  if [[ "$binary" != /* || ! -x "$binary" ]]; then
    echo "--$argument must be an executable absolute path" >&2
    exit 2
  fi
done
if [[ "$output" != /* ]]; then
  echo "--output must be an absolute path" >&2
  exit 2
fi
if [[ -e "$output" ]]; then
  if [[ ! -d "$output" || -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "output directory must be empty: $output" >&2
    exit 2
  fi
fi
mkdir -p "$output/matrix-raw"

entries=131072
seeds=(20260920 20260921 20260922 20260923 20260924)
short_repeats=(1 2 3 4 5)
eight_repeats=(1 2 3 4 5 6 7 8 9 10)
if [[ "${CEDAR_32X8_STABILITY_TEST_MODE:-0}" == 1 ]]; then
  entries=64
  seeds=(20260920)
  short_repeats=(1)
  eight_repeats=(1 2)
fi

header="revision,workload,seed,entries,allocated_handles,writers,repeat,seconds,ops,p50_ns,p95_ns,p99_ns,peak_rss_bytes,arena_bytes,bytes_per_handle,result_hash,errors,active_scan_ns,frozen_first_scan_ns,frozen_ready_scan_ns,phase,elapsed_ns,index_insert_ns,allocate_and_insert_ns,active_scan_ns_phase,freeze_prepare_ns,first_frozen_scan_ns,ready_frozen_scan_ns"
matrix="$output/matrix-raw/matrix.csv"
printf 'variant,%s\n' "$header" > "$matrix"

run_one() {
  local variant="$1" binary="$2" implementation="$3" seed="$4"
  local writers="$5" repeat="$6"
  local raw="$output/matrix-raw/${variant}-${seed}-${writers}-memtable-${repeat}.csv"
  "$binary" --implementation "$implementation" --entries "$entries" \
    --workload random --writers "$writers" --seed "$seed" \
    --revision "$variant" --phase memtable > "$raw"

  if [[ "$(wc -l < "$raw" | tr -d ' ')" != 2 || "$(sed -n '1p' "$raw")" != "$header" ]]; then
    echo "raw CSV shape/header contract failed: $raw" >&2
    exit 1
  fi
  if ! awk -F, -v revision="$variant" -v seed="$seed" -v entries="$entries" \
      -v writers="$writers" -v repeat="$repeat" '
        NR == 2 {
          if (NF != 28 || $1 != revision || $2 != "random" || $3 != seed ||
              $4 != entries || $6 != writers || $7 != 0 ||
              $9 != entries || $16 == "" || $17 != 0 || $21 != "memtable") {
            exit 1
          }
        }
      ' "$raw"; then
    echo "raw CSV value contract failed: $raw" >&2
    exit 1
  fi
  last_hash="$(awk -F, 'NR == 2 { print $16 }' "$raw")"
  awk -F, -v OFS=, -v variant="$variant" -v repeat="$repeat" \
    'NR == 2 { $7 = repeat; print variant, $0 }' "$raw" >> "$matrix"
}

run_group() {
  local seed="$1" writers="$2" repeat="$3"
  local order
  if [[ "$writers" == 1 ]]; then
    order=(nibble byte baseline candidate)
  else
    order=(nibble skiplist baseline candidate)
  fi
  if (( (seed + writers + repeat) % 2 == 1 )); then
    local reversed=()
    local index
    for ((index = ${#order[@]} - 1; index >= 0; --index)); do
      reversed+=("${order[index]}")
    done
    order=("${reversed[@]}")
  fi

  local expected_hash=""
  local variant
  for variant in "${order[@]}"; do
    case "$variant" in
      nibble) run_one nibble "$nibble" radix "$seed" "$writers" "$repeat" ;;
      byte) run_one byte "$byte" radix "$seed" "$writers" "$repeat" ;;
      skiplist) run_one skiplist "$byte" skiplist "$seed" "$writers" "$repeat" ;;
      baseline) run_one baseline "$baseline" radix "$seed" "$writers" "$repeat" ;;
      candidate) run_one candidate "$candidate" radix "$seed" "$writers" "$repeat" ;;
    esac
    if [[ -n "$expected_hash" && "$last_hash" != "$expected_hash" ]]; then
      echo "result hash mismatch for seed=$seed writers=$writers repeat=$repeat" >&2
      exit 1
    fi
    expected_hash="$last_hash"
  done
}

for seed in "${seeds[@]}"; do
  for repeat in "${short_repeats[@]}"; do
    run_group "$seed" 1 "$repeat"
    run_group "$seed" 4 "$repeat"
  done
  for repeat in "${eight_repeats[@]}"; do
    run_group "$seed" 8 "$repeat"
  done
done
