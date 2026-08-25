#!/usr/bin/env bash
set -euo pipefail

[[ $# -eq 1 && -s "$1" ]] || {
  echo "usage: check_cedar_temporal_range_profile.sh PROFILE.csv" >&2
  exit 2
}

header=$(head -n 1 "$1")
expected='build_id,case,source,operations,rows,ops_per_second,rows_per_second,physical_bytes,decoded_bytes,pages_read,pages_skipped,p50_us,p95_us,p99_us,peak_rss_bytes,errors'
[[ "$header" == "$expected" ]] || {
  echo "profile header mismatch" >&2
  exit 1
}

awk -F, '
  NR == 1 { next }
  NF != 16 || $2 == "" || $3 == "" || $4 == 0 || $5 == "" ||
    $12 == "" || $13 == "" || $14 == "" || $15 == "" || $16 != 0 { bad++ }
  cases[$2] = 1
  END {
    required["point_state"] = 1
    required["system_time_as_of"] = 1
    required["system_time_between"] = 1
    required["two_segment_path"] = 1
    required["transaction_read_your_writes"] = 1
    for (name in required) if (!(name in cases)) bad++
    if (NR <= 1 || bad != 0) exit 1
  }
' "$1"

[[ -s "$1.manifest" ]] || {
  echo "profile manifest is missing" >&2
  exit 1
}
for field in build_id entities edges properties commits sessions duration_seconds projection_generation query_budget; do
  grep -q "^${field}=" "$1.manifest" || {
    echo "profile manifest field is missing: ${field}" >&2
    exit 1
  }
done
echo "temporal range profile acceptance gates passed"
