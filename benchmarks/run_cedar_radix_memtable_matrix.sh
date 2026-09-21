#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 ABSOLUTE_BASELINE_BENCH ABSOLUTE_CANDIDATE_BENCH ABSOLUTE_EMPTY_OUTPUT_DIRECTORY" >&2
  exit 2
fi
if [[ -z "${CEDAR_RADIX_B0_REVISION:-}" || -z "${CEDAR_RADIX_B1_REVISION:-}" ||
      -z "${CEDAR_RADIX_B2_REVISION:-}" ]]; then
  echo "CEDAR_RADIX_B0_REVISION, CEDAR_RADIX_B1_REVISION, and CEDAR_RADIX_B2_REVISION are required" >&2
  exit 2
fi

baseline="$1"
candidate="$2"
output_dir="$3"
if [[ "$baseline" != /* || "$candidate" != /* || "$output_dir" != /* ||
      ! -x "$baseline" || ! -x "$candidate" ]]; then
  echo "benchmark paths and output directory must be absolute; benchmarks must be executable" >&2
  exit 2
fi
if [[ -e "$output_dir" && -n "$(find "$output_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $output_dir" >&2
  exit 2
fi
mkdir -p "$output_dir"

printf 'implementation,revision,workload,seed,entries,allocated_handles,writers,repeat,seconds,ops,p50_ns,p95_ns,p99_ns,peak_rss_bytes,arena_bytes,bytes_per_handle,result_hash,errors,active_scan_ns,frozen_first_scan_ns,frozen_ready_scan_ns,phase,elapsed_ns,index_insert_ns,allocate_and_insert_ns,active_scan_ns_phase,freeze_prepare_ns,first_frozen_scan_ns,ready_frozen_scan_ns\n' > "$output_dir/matrix.csv"
for seed in 20260920 20260921 20260922; do
for entries in 1024 16384 131072; do
  for workload in ascending random versions; do
      for writers in 1 4 8; do
        expected_hash=""
        for repeat in 1 2 3 4 5; do
        if (( (entries + writers + repeat) % 2 == 0 )); then
          implementations=(vector skiplist radix)
        else
          implementations=(radix vector skiplist)
        fi
        for implementation in "${implementations[@]}"; do
          binary="$baseline"
          revision="$CEDAR_RADIX_B0_REVISION"
          [[ "$implementation" == skiplist ]] && revision="$CEDAR_RADIX_B1_REVISION"
          [[ "$implementation" == radix ]] && revision="$CEDAR_RADIX_B2_REVISION"
          [[ "$implementation" != vector ]] && binary="$candidate"
          raw="$output_dir/${implementation}-${seed}-${entries}-${workload}-${writers}-${repeat}.csv"
          "$binary" --implementation "$implementation" --entries "$entries" --workload "$workload" --writers "$writers" \
              --seed "$seed" --revision "$revision" --phase all > "$raw"
          row="$(sed -n '2p' "$raw")"
          [[ "$(awk -F, 'NR == 2 { print NF }' "$raw")" == 28 ]] || {
            echo "benchmark field-count contract failed in $raw" >&2
            exit 1
          }
          errors="$(awk -F, 'NR == 2 { print $17 }' "$raw")"
          ops="$(awk -F, 'NR == 2 { print $9 }' "$raw")"
          result_hash="$(awk -F, 'NR == 2 { print $16 }' "$raw")"
          [[ "$errors" == 0 && "$ops" == "$entries" ]] || {
            echo "benchmark count/error contract failed in $raw" >&2
            exit 1
          }
          if [[ -z "$expected_hash" ]]; then
            expected_hash="$result_hash"
          elif [[ "$result_hash" != "$expected_hash" ]]; then
            echo "benchmark result hash mismatch in $raw" >&2
            exit 1
          fi
          awk -F, 'NR == 2 {
            if ($10 != "NA" || $11 != "NA" || $12 != "NA" || $21 != "all" || $22 !~ /^[0-9]+$/ || NF != 28) exit 1
          }' "$raw" || {
            echo "benchmark phase/percentile contract failed in $raw" >&2
            exit 1
          }
          printf '%s,%s\n' "$implementation" "$row" >> "$output_dir/matrix.csv"
  done
done
done
done
  done
done
