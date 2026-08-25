#!/usr/bin/env bash
set -euo pipefail
csv="${1:?usage: $0 CSV}"
awk -F, '
  NR == 1 { next }
  NF < 15 || ($2 == 0 && $1 !~ /typed_graph_expansion/) || $4 == "" || $14 == 0 || $15 != 0 { bad++ }
  { rows++; seen[$1] = 1;
    if ($4 == "projection" && $8 > 0) projection_pruned++;
    if ($4 == "projection") projection_rows++;
    if ($4 == "spill" && $3 > 0) spill_rows++;
    if ($1 ~ /exact_multiget_property_bindings_1$/ && $15 == 0) exact1++;
    if ($1 ~ /exact_multiget_property_bindings_4$/ && $15 == 0) exact4++;
    if ($1 ~ /exact_multiget_property_bindings_16$/ && $15 == 0) exact16++;
    if ($1 ~ /typed_graph_expansion/ && $15 == 0) typed++;
    if ($1 ~ /temporal_history_valid_time_10pct/ && $15 == 0) selective++;
    if ($1 ~ /relational_spill_hash_join/ && $15 == 0) spill_ok++;
  }
  END {
    required = (seen["point_state_exact"] && seen["typed_graph_expansion"] &&
                seen["temporal_history"] && seen["temporal_history_valid_time_10pct"] &&
                seen["projection_page_hit"] && seen["projection_page_partial"] &&
                seen["projection_page_base"] && seen["relational_spill_hash_join"] &&
                exact1 && exact4 && exact16 && typed && selective && spill_ok)
    if (rows < 10 || bad != 0 || !required || projection_rows == 0 ||
        projection_pruned == 0 || spill_rows == 0) exit 1
  }
' "${csv}"
echo "read-side complexity acceptance gates passed"
