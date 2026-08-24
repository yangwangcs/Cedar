#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 ABSOLUTE_BENCHMARK ABSOLUTE_EMPTY_OUTPUT_DIRECTORY" >&2
  exit 2
fi

bench="$1"
output_dir="$2"
if [[ "$bench" != /* || "$output_dir" != /* || ! -x "$bench" ]]; then
  echo "benchmark and output directory must be absolute; benchmark must be executable" >&2
  exit 2
fi
if [[ -e "$output_dir" && -n "$(find "$output_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "output directory must be empty: $output_dir" >&2
  exit 2
fi
if pgrep -f -x -- "${bench}.*" >/dev/null; then
  echo "benchmark is already running from this worktree: $bench" >&2
  exit 2
fi
mkdir -p "$output_dir"

{
  printf 'commit='; git rev-parse HEAD
  printf 'benchmark=%s\n' "$bench"
  printf 'host='; uname -a
  printf 'duration_seconds=30\n'
  printf 'workload=property-put\n'
  printf 'verify_reopen=false\n'
} > "$output_dir/metadata.txt"

printf 'writer_clients,operations_per_second,epoch_transactions,wal_sync_count,transactions_per_sync,group_fill_p50,group_fill_p95,group_fill_max,qualification\n' > "$output_dir/matrix.csv"

column() {
  awk -F, -v name="$1" -v header="$2" -v row="$3" \
    'BEGIN { split(header,h,","); split(row,v,","); for (i in h) if (h[i] == name) { print v[i]; exit } }'
}

require_zero() {
  local field="$1"
  local value="$2"
  if [[ "$value" != "0" ]]; then
    echo "benchmark row has non-zero ${field}: ${value}" >&2
    exit 1
  fi
}

for clients in 2 4 8 16 32 64 128; do
  database_path="$output_dir/db-${clients}"
  raw="$output_dir/${clients}.raw.csv"
  "$bench" --path "$database_path" --database-path "$database_path" \
      --workload property-put --writer-clients "$clients" \
      --duration-seconds 30 --campaign warm --verify-reopen false > "$raw"
  header="$(sed -n '1p' "$raw")"
  row="$(sed -n '2p' "$raw")"
  require_zero writer_failures "$(column writer_failures "$header" "$row")"
  require_zero background_errors "$(column background_errors "$header" "$row")"
  require_zero maintenance_errors "$(column maintenance_errors "$header" "$row")"
  require_zero write_stopped "$(column write_stopped "$header" "$row")"
  require_zero unexplained_autonomous_jobs "$(column unexplained_autonomous_jobs "$header" "$row")"
  txns="$(column epoch_transactions "$header" "$row")"
  syncs="$(column wal_sync_count "$header" "$row")"
  printf '%s,%s,%s,%s,' "$clients" \
      "$(column operations_per_second "$header" "$row")" "$txns" "$syncs" \
      >> "$output_dir/matrix.csv"
  awk -v txns="$txns" -v syncs="$syncs" 'BEGIN { printf "%.3f", syncs ? txns / syncs : 0 }' >> "$output_dir/matrix.csv"
  printf ',%s,%s,%s,%s\n' \
      "$(column group_fill_p50 "$header" "$row")" \
      "$(column group_fill_p95 "$header" "$row")" \
      "$(column group_fill_max "$header" "$row")" \
      "$(column qualification "$header" "$row")" >> "$output_dir/matrix.csv"
done
