#!/usr/bin/env bash
set -euo pipefail

build_dir=
output=
requested_phase=all
duration=10
facts_values=1,16,64,256
writers_values=1,8
readers_values=8
degrees_values=10
selectivities_values=1
projection_states=canonical
input=
facts_auto=false
cases_run=0

require_value() {
  local option="$1"
  if (($# < 2)) || [[ -z "${2:-}" ]] || [[ "${2:-}" == --* ]]; then
    echo "$option requires a value" >&2
    exit 2
  fi
}

csv_values() {
  local csv="$1" label="$2"
  local rest="$csv" value last=false
  # Split explicitly so Bash does not discard a trailing empty field.
  while :; do
    if [[ "$rest" == *,* ]]; then
      value="${rest%%,*}"
      rest="${rest#*,}"
    else
      value="$rest"
      last=true
    fi
    [[ -n "$value" && "$value" != *[[:space:]]* ]] || {
      echo "$label contains an empty/whitespace component" >&2
      exit 2
    }
    printf '%s\n' "$value"
    [[ "$last" == true ]] && break
  done
}

validate_values() {
  local csv="$1" label="$2" pattern="$3" value
  local rest="$csv" last=false
  # Keep leading, middle, and trailing empty components visible to validation.
  while :; do
    if [[ "$rest" == *,* ]]; then
      value="${rest%%,*}"
      rest="${rest#*,}"
    else
      value="$rest"
      last=true
    fi
    [[ -n "$value" && "$value" != *[[:space:]]* ]] || { echo "$label contains an empty/whitespace component" >&2; return 2; }
    [[ "$value" =~ $pattern ]] || { echo "unsupported $label value: $value" >&2; return 2; }
    [[ "$last" == true ]] && break
  done
}

first_csv_value() {
  local first
  first=$(csv_values "$1" "$2" | sed -n '1p')
  [[ -n "$first" ]] || { echo "$2 must not be empty" >&2; exit 2; }
  printf '%s' "$first"
}

while (($#)); do
  case "$1" in
    --build-dir) require_value "$@"; build_dir="$2"; shift 2 ;;
    --output) require_value "$@"; output="$2"; shift 2 ;;
    --phase) require_value "$@"; requested_phase="$2"; shift 2 ;;
    --duration-seconds) require_value "$@"; duration="$2"; shift 2 ;;
    --facts-per-txn) require_value "$@"; facts_values="$2"; shift 2 ;;
    --writers) require_value "$@"; writers_values="$2"; shift 2 ;;
    --readers) require_value "$@"; readers_values="$2"; shift 2 ;;
    --degrees) require_value "$@"; degrees_values="$2"; shift 2 ;;
    --selectivities) require_value "$@"; selectivities_values="$2"; shift 2 ;;
    --projection-states) require_value "$@"; projection_states="$2"; shift 2 ;;
    --input) require_value "$@"; input="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

[[ -x "$build_dir/cedar_query_bench" && -n "$output" ]] || {
  echo "--build-dir must contain cedar_query_bench and --output is required" >&2
  exit 2
}
[[ "$duration" =~ ^[1-9][0-9]*$ ]] || {
  echo "--duration-seconds must be positive" >&2
  exit 2
}

build_dir=$(cd "$build_dir" && pwd)
mkdir -p "$output"
output=$(cd "$output" && pwd)
if [[ -n "$input" ]]; then
  [[ -d "$input" ]] || { echo "--input must name an existing directory" >&2; exit 2; }
  input=$(cd "$input" && pwd)
fi
if [[ "$facts_values" == auto-turning-point ]]; then
  facts_auto=true
fi
for csv in "$facts_values" "$writers_values" "$readers_values" "$degrees_values" "$selectivities_values" "$projection_states"; do
  [[ "$csv" != *[[:space:]]* && -n "$csv" ]] || { echo "invalid empty/whitespace CSV option" >&2; exit 2; }
done
if [[ "$facts_auto" == false ]]; then validate_values "$facts_values" facts-per-txn '^(1|4|8|16|32|64|128|256|512|1024|2048)$' || exit $?; fi
validate_values "$writers_values" writers '^(1|2|8|32|64)$' || exit $?
validate_values "$readers_values" readers '^(1|8|32)$' || exit $?
validate_values "$degrees_values" degrees '^(1|10|100|1000|10000)$' || exit $?
validate_values "$selectivities_values" selectivities '^(0\.1|1|10|100)$' || exit $?
validate_values "$projection_states" projection-states '^(canonical|base|short-delta|long-delta|partial)$' || exit $?

all_phases=(release-calibration write-idle-five-repeats write-active-projection-five-repeats read-cold read-warm mixed-30-minute reopen-verification space-audit)
case "$requested_phase" in
  all) phases=("${all_phases[@]}") ;;
  release-calibration|write-idle-five-repeats|write-active-projection-five-repeats|read-cold|read-warm|mixed-30-minute|reopen-verification|space-audit) phases=("$requested_phase") ;;
  *) echo "unsupported phase: $requested_phase" >&2; exit 2 ;;
esac

mkdir -p "$output"
manifest="$output/commands.manifest"
summary="$output/summary.csv"
summary_json="$output/summary.jsonl"
printf 'phase,case,command\n' > "$manifest"
printf 'phase,case,exit_code,hard_gate_pass,terminal_status,facts_per_second,end_to_end_p99_us,wal_sync_p99_us\n' > "$summary"
: > "$summary_json"
overall=0

projection_value() {
  case "$1" in
    canonical) printf 'canonical-only' ;;
    partial) printf 'partial-coverage' ;;
    *) printf '%s' "$1" ;;
  esac
}

run_case() {
  local phase="$1" case_name="$2" cache="$3" projection_work="$4" operation="${5:-state-at}" facts_per_txn="${6:-16}" writers="${7:-1}" readers="${8:-8}" degree="${9:-10}" selectivity="${10:-1}" projection="${11:-canonical-only}" case_duration="${12:-$duration}"
  local case_dir="$output/$phase/$case_name"
  cases_run=$((cases_run + 1))
  mkdir -p "$case_dir"
  local db="$case_dir/database" csv="$case_dir/run.csv" json="$case_dir/run.json"
  local -a cmd=("$build_dir/cedar_query_bench" "--path=$db" "--operation=$operation" "--projection-state=$projection" "--degree=$degree" "--selectivity-percent=$selectivity" "--readers=$readers" "--cache-state=$cache" "--projection-work=$projection_work" "--writers=$writers" "--facts-per-txn=$facts_per_txn" "--seed=1" "--duration-seconds=$case_duration" "--reopen-verify=true")
  printf '%s,%s,' "$phase" "$case_name" >> "$manifest"
  printf '%q ' "${cmd[@]}" >> "$manifest"
  printf '\n' >> "$manifest"
  set +e
  "${cmd[@]}" > "$csv" 2> "$json"
  local rc=$?
  set -e
  local gate=unknown terminal=unknown facts_rate=0 end_p99=0 wal_sync_p99=0 schema_reason=""
  if [[ -s "$csv" ]]; then
    gate=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="hard_gate_pass") gate=i; next } NR==2 && gate { print $gate }' "$csv")
    terminal=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="terminal_status") status=i; next } NR==2 && status { print $status }' "$csv")
    facts_rate=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="facts_per_second") rate=i; next } NR==2 && rate { print $rate }' "$csv")
    end_p99=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="end_to_end_p99_us") p=i; next } NR==2 && p { print $p }' "$csv")
    wal_sync_p99=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="wal_sync_p99_us") p=i; next } NR==2 && p { print $p }' "$csv")
    if ! awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="wal_sync_p99_us") found=1; exit found ? 0 : 1 }' "$csv"; then
      schema_reason="missing_wal_sync_p99_us"
      gate=false
      wal_sync_p99="-"
    fi
  else
    schema_reason="missing_run_csv"
    gate=false
    wal_sync_p99="-"
  fi
  [[ -z "$schema_reason" ]] || terminal="$schema_reason"
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$phase" "$case_name" "$rc" "$gate" "$terminal" "$facts_rate" "$end_p99" "$wal_sync_p99" >> "$summary"
  printf '{"phase":"%s","case":"%s","exit_code":%s,"hard_gate_pass":"%s","terminal_status":"%s","facts_per_second":%s,"end_to_end_p99_us":%s,"wal_sync_p99_us":%s}\n' "$phase" "$case_name" "$rc" "$gate" "$terminal" "$facts_rate" "$end_p99" "${wal_sync_p99/-/null}" >> "$summary_json"
  if [[ "$rc" -ne 0 || "$gate" != true ]]; then overall=1; fi
}

turning_point_artifact="$output/turning-point.json"
resolve_turning_point() {
  local artifact=""
  if [[ -n "$input" && -f "$input/turning-point.json" ]]; then
    artifact="$input/turning-point.json"
  elif [[ -f "$turning_point_artifact" ]]; then
    artifact="$turning_point_artifact"
  fi
  if [[ -z "$artifact" ]]; then
    echo "auto-turning-point requires a valid turning-point.json artifact via --input or --output" >&2
    exit 2
  fi
  local value
  # Parse the complete artifact shape.  Do not normalize malformed tokens:
  # a negative value, trailing text, duplicate key, or invalid JSON must fail.
  value=$(awk '
    BEGIN {
      object = "^[[:space:]]*\\{[[:space:]]*\"facts_per_txn\"[[:space:]]*:[[:space:]]*(1|4|8|16|32|64|128|256|512|1024|2048)[[:space:]]*(,[[:space:]]*\"source_summary\"[[:space:]]*:[[:space:]]*\"([^\"\\\\[:cntrl:]]|\\\\([\"\\\\/bfnrt]|u[0-9A-Fa-f]{4}))*\"[[:space:]]*)?\\}[[:space:]]*$"
      prefix = "^[[:space:]]*\\{[[:space:]]*\"facts_per_txn\"[[:space:]]*:[[:space:]]*"
    }
    {
      if (text != "") text = text "\n"
      text = text $0
    }
    END {
      if (text !~ object) exit 1
      value = text
      sub(prefix, "", value)
      if (match(value, /^[0-9]+/)) print substr(value, RSTART, RLENGTH)
      else exit 1
    }
  ' "$artifact")
  [[ "$value" =~ ^(1|4|8|16|32|64|128|256|512|1024|2048)$ ]] || {
    echo "invalid turning-point artifact: $artifact" >&2
    exit 2
  }
  printf '%s' "$value"
}

write_turning_point_artifact() {
  local source="$output/summary.csv" run_csv="$output/release-calibration"
  local value="" candidate rate best_rate=0 run_file
  while IFS= read -r run_file; do
    candidate=$(awk -F, 'FNR==1 {for(i=1;i<=NF;i++) {if($i=="facts_per_txn") f=i; if($i=="facts_per_second") r=i; if($i=="hard_gate_pass") g=i}} FNR==2 && $g=="true" {print $f ";" $r; exit}' "$run_file")
    if [[ "$candidate" == *\;* ]]; then
      value=${candidate%%;*}
      rate=${candidate#*;}
      if awk -v rate="$rate" -v best="$best_rate" 'BEGIN {exit !(rate+0 > best+0)}'; then
        best_rate="$rate"
        turning_value="$value"
      fi
    fi
  done < <(find "$run_csv" -type f -name run.csv -print)
  value="${turning_value:-}"
  [[ "$value" =~ ^[0-9]+$ ]] || return 1
  printf '{"facts_per_txn":%s,"source_summary":"%s"}\n' "$value" "$source" > "$turning_point_artifact"
}

write_idle_overhead_artifact() {
  local source="$output/summary.csv"
  local target="$output/../write-idle-overhead.csv"
  local expected_points expected_samples
  expected_points=$(csv_values "$facts_values" facts-per-txn | wc -l | tr -d ' ')
  expected_points=$((expected_points * $(csv_values "$writers_values" writers | wc -l | tr -d ' ')))
  expected_samples=$((expected_points * 5))
  if ! awk -F, -v expected="$expected_samples" '
    $1=="write-idle-five-repeats" {
      total++
      if ($4=="true" && $8 ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ && $8+0>0) {
        rate+=$6; p99+=$7; wal+=$8; good++
        if (match($2, /^repeat-[1-5]-f[0-9]+-w[0-9]+$/)) {
          key=$2; sub(/^repeat-[1-5]-/, "", key); counts[key]++
        } else { bad=1 }
      } else { bad=1 }
    }
    END {
      for (key in counts) if (counts[key] != 5) bad=1
      if (good != expected || total != expected || bad) exit 1
      printf "samples,%d\navg_facts_per_second,%s\navg_end_to_end_p99_us,%s\navg_wal_sync_p99_us,%s\n", good, rate/good, p99/good, wal/good
    }
  ' "$source" > "$target.tmp"; then
    rm -f "$target.tmp"
    : > "$target"
    overall=1
    printf '{"gate":"idle_query_overhead","pass":false,"reason":"invalid_repeat_cardinality","expected_samples":%d}\n' "$expected_samples" >> "$summary_json"
    return 1
  fi
  {
    cat "$target.tmp"
    printf 'facts_per_txn,%s\nwriters,%s\nseed,1\ncache_state,cold\nprojection_work,paused\n' "$facts_values" "$writers_values"
  } > "$target"
  rm -f "$target.tmp"
}

compare_idle_query_overhead() {
  local baseline=""
  if [[ -n "$input" && -f "$input/write-idle-baseline.csv" ]]; then
    baseline="$input/write-idle-baseline.csv"
  elif [[ -f "$output/../write-idle-baseline.csv" ]]; then
    baseline="$output/../write-idle-baseline.csv"
  fi
  local idle_rate idle_wal baseline_rate baseline_wal baseline_samples expected_points expected_samples baseline_facts baseline_writers baseline_seed baseline_cache baseline_projection reason=""
  idle_rate=$(awk -F, '$1=="write-idle-five-repeats" && $4=="true" {sum+=$6; n++} END {print n ? sum/n : ""}' "$summary")
  idle_wal=$(awk -F, '$1=="write-idle-five-repeats" && $4=="true" {sum+=$8; n++} END {print n ? sum/n : ""}' "$summary")
  if [[ ! -f "$baseline" ]]; then
    reason="missing_baseline"
  else
    baseline_rate=$(awk -F, '$1=="avg_facts_per_second" {print $2}' "$baseline")
    baseline_wal=$(awk -F, '$1=="avg_wal_sync_p99_us" {print $2}' "$baseline")
    baseline_samples=$(awk -F, '$1=="samples" {print $2}' "$baseline")
    for metric in "$baseline_rate" "$baseline_wal" "$idle_rate" "$idle_wal" "$baseline_samples"; do
      if ! awk -v value="$metric" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ && value+0 > 0 && value+0 < 1e308) }'; then
        reason="invalid_baseline_metric"
        break
      fi
    done
    expected_points=$(csv_values "$facts_values" facts-per-txn | wc -l | tr -d ' ')
    expected_points=$((expected_points * $(csv_values "$writers_values" writers | wc -l | tr -d ' ')))
    expected_samples=$((expected_points * 5))
    [[ -n "$reason" || "$baseline_samples" -eq "$expected_samples" ]] || reason="invalid_baseline_sample_count"
    baseline_facts=$(awk -F, '$1=="facts_per_txn" {print $2}' "$baseline")
    baseline_writers=$(awk -F, '$1=="writers" {print $2}' "$baseline")
    baseline_seed=$(awk -F, '$1=="seed" {print $2}' "$baseline")
    baseline_cache=$(awk -F, '$1=="cache_state" {print $2}' "$baseline")
    baseline_projection=$(awk -F, '$1=="projection_work" {print $2}' "$baseline")
    [[ -n "$reason" || "$baseline_facts" == "$facts_values" ]] || reason="incomparable_facts_per_txn"
    [[ -n "$reason" || "$baseline_writers" == "$writers_values" ]] || reason="incomparable_writers"
    [[ -n "$reason" || "$baseline_seed" == 1 ]] || reason="incomparable_seed"
    [[ -n "$reason" || "$baseline_cache" == cold ]] || reason="incomparable_cache_state"
    [[ -n "$reason" || "$baseline_projection" == paused ]] || reason="incomparable_projection_work"
  fi
  if [[ -z "$reason" ]] && awk -v base="$baseline_rate" -v idle="$idle_rate" 'BEGIN { exit !(idle >= base * 0.97) }' &&
     awk -v base="$baseline_wal" -v idle="$idle_wal" 'BEGIN { exit !(idle <= base * 1.05) }'; then
    return 0
  fi
  overall=1
  printf '{"gate":"idle_query_overhead","pass":false,"reason":"%s","baseline":"%s","baseline_facts_per_second":%s,"idle_facts_per_second":%s,"baseline_wal_sync_p99_us":%s,"idle_wal_sync_p99_us":%s}\n' \
    "${reason:-threshold_failure}" "${baseline:-missing}" "${baseline_rate:-null}" "${idle_rate:-null}" "${baseline_wal:-null}" "${idle_wal:-null}" >> "$summary_json"
  return 1
}

compare_active_overhead() {
  local baseline="$output/../write-idle-overhead.csv"
  if [[ ! -f "$baseline" ]]; then
    baseline=""
    if [[ -n "$input" && -f "$input/write-idle-overhead.csv" ]]; then
      baseline="$input/write-idle-overhead.csv"
    fi
  fi
  local idle_rate="" idle_p99="" baseline_reason=""
  if [[ -z "$baseline" ]]; then
    baseline_reason="missing_baseline"
  else
    idle_rate=$(awk -F, '$1=="avg_facts_per_second" {print $2}' "$baseline")
    idle_p99=$(awk -F, '$1=="avg_end_to_end_p99_us" {print $2}' "$baseline")
    if ! awk -v value="$idle_rate" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ && value+0 > 0 && value+0 < 1e308) }'; then
      baseline_reason="invalid_avg_facts_per_second"
    elif ! awk -v value="$idle_p99" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ && value+0 > 0 && value+0 < 1e308) }'; then
      baseline_reason="invalid_avg_end_to_end_p99_us"
    fi
  fi
  local active_rate active_p99 active_reason=""
  if ! active_reason=$(awk -F, '
    BEGIN { number = "^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$" }
    NR == 1 {
      for (i = 1; i <= NF; i++) {
        if ($i == "phase") phase = i
        if ($i == "hard_gate_pass") gate = i
        if ($i == "facts_per_second") rate = i
        if ($i == "end_to_end_p99_us") p99 = i
      }
      next
    }
    $phase == "write-active-projection-five-repeats" && $gate == "true" {
      samples++
      if (!rate || !p99 || $rate !~ number || ($rate + 0) <= 0 || !($rate + 0 < 1e308)) {
        reason = "invalid_active_facts_per_second"
      } else if ($p99 !~ number || ($p99 + 0) <= 0 || !($p99 + 0 < 1e308)) {
        reason = "invalid_active_end_to_end_p99_us"
      }
    }
    END {
      if (!samples) { print "no_active_samples"; exit 1 }
      if (reason != "") { print reason; exit 1 }
    }
  ' "$summary"); then
    active_reason="${active_reason:-invalid_active_sample}"
  fi
  active_rate=$(awk -F, '$1=="write-active-projection-five-repeats" && $4=="true" {sum+=$6; n++} END {print n ? sum/n : 0}' "$summary")
  active_p99=$(awk -F, '$1=="write-active-projection-five-repeats" && $4=="true" {sum+=$7; n++} END {print n ? sum/n : 0}' "$summary")
  if [[ -z "$baseline_reason" && -z "$active_reason" ]] &&
     awk -v value="$active_rate" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ && value+0 > 0 && value+0 < 1e308) }' &&
     awk -v value="$active_p99" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ && value+0 > 0 && value+0 < 1e308) }' &&
     awk -v i="$idle_rate" -v a="$active_rate" 'BEGIN {exit !(a >= i*0.90)}' &&
     awk -v i="$idle_p99" -v a="$active_p99" 'BEGIN {exit !(a <= i*1.15)}'; then
    return 0
  fi
  overall=1
  printf '{"gate":"active_projection_overhead","pass":false,"reason":"%s","baseline":"%s","idle_facts_per_second":%s,"active_facts_per_second":%s,"idle_p99_us":%s,"active_p99_us":%s}\n' \
    "${baseline_reason:-${active_reason:-threshold_or_active_metric_failure}}" "${baseline:-missing}" "${idle_rate:-null}" "${active_rate:-null}" "${idle_p99:-null}" "${active_p99:-null}" >> "$summary_json"
  return 1
}

# Convert RFC 4180-style records to tab-separated fields.  The benchmark CSV
# writer quotes paths and status strings that contain commas, so field indexing
# must happen after honoring quoted commas and doubled quotes.
csv_rows_as_tsv() {
  awk '
    function parse_record(line, fields,    i,c,next_c,quoted,field,count) {
      for (i in fields) delete fields[i]
      quoted = 0
      field = ""
      count = 0
      for (i = 1; i <= length(line); i++) {
        c = substr(line, i, 1)
        next_c = substr(line, i + 1, 1)
        if (quoted) {
          if (c == "\"" && next_c == "\"") {
            field = field "\""
            i++
          } else if (c == "\"") {
            quoted = 0
          } else {
            field = field c
          }
        } else if (c == ",") {
          fields[++count] = field
          field = ""
        } else if (c == "\"" && field == "") {
          quoted = 1
        } else {
          field = field c
        }
      }
      fields[++count] = field
      return count
    }
    {
      count = parse_record($0, fields)
      for (i = 1; i <= count; i++) {
        printf "%s%s", fields[i], (i == count ? "\n" : "\t")
      }
    }
  ' "$@"
}

audit_run_csv() {
  local phase="$1" file="$2" records="$3"
  csv_rows_as_tsv "$file" | awk -F '\t' -v mode="$phase" '
    function fail(message) {
      if (reason == "") reason = message
      invalid = 1
    }
    function number(value) {
      return value ~ /^[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$/ &&
             value + 0 >= 0 && value + 0 < 1e308
    }
    function integer(value) {
      return value ~ /^[0-9]+$/
    }
    function unquote(value) {
      if (value ~ /^".*"$/) {
        sub(/^"/, "", value)
        sub(/"$/, "", value)
        gsub(/""/, "\"", value)
      }
      return value
    }
    function emit(status, message, exit_value, gate_value, terminal_value,
                  reopen_value, auth_value, derived_value, stats_value,
                  scratch_value, amplification_value) {
      if (message == "") message = "-"
      if (exit_value == "") exit_value = "-"
      if (gate_value == "") gate_value = "-"
      if (terminal_value == "") terminal_value = "-"
      if (reopen_value == "") reopen_value = "-"
      if (auth_value == "") auth_value = "-"
      if (derived_value == "") derived_value = "-"
      if (stats_value == "") stats_value = "-"
      if (scratch_value == "") scratch_value = "-"
      if (amplification_value == "") amplification_value = "-"
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
        status, message, exit_value, gate_value, terminal_value, reopen_value,
        auth_value, derived_value, stats_value, scratch_value, amplification_value
    }
    NR == 1 {
      header_fields = NF
      for (i = 1; i <= NF; i++) {
        if ($i in column) fail("duplicate header field: " $i)
        column[$i] = i
      }
      required[1] = "dataset_checksum"
      required[2] = "authoritative_bytes"
      required[3] = "derived_bytes"
      required[4] = "statistics_bytes"
      required[5] = "scratch_bytes"
      required[6] = "space_amplification"
      required[7] = "reopen_verified"
      required[8] = "hard_gate_pass"
      required[9] = "terminal_status"
      for (i = 1; i <= 9; i++) {
        if (!(required[i] in column)) fail("missing header field: " required[i])
      }
      header_invalid = invalid
      header_reason = reason
      next
    }
    {
      rows++
      # Preserve header violations across all rows so malformed schemas cannot
      # be reset to PASS by a later row-level validation.
      invalid = header_invalid
      reason = header_reason
      # Do not dereference required columns when schema validation already
      # failed.  Missing headers would otherwise expand to an illegal awk
      # field expression `$()` and abort the artifact audit.
      if (header_invalid) {
        emit("FAIL", reason, "", "", "", "", "", "", "", "", "")
        next
      }
      if (NF != header_fields) {
        fail("row field count does not match header")
      }
      dataset = $(column["dataset_checksum"])
      auth = $(column["authoritative_bytes"])
      derived = $(column["derived_bytes"])
      stats = $(column["statistics_bytes"])
      scratch = $(column["scratch_bytes"])
      amplification = $(column["space_amplification"])
      reopen = $(column["reopen_verified"])
      gate = $(column["hard_gate_pass"])
      terminal = unquote($(column["terminal_status"]))
      exit_value = "0"
      if ("exit_code" in column) exit_value = $(column["exit_code"])
      if ("exit_code" in column && exit_value != "0") fail("exit_code is not zero")
      if (!integer(dataset)) fail("dataset_checksum is not an unsigned integer")
      if (!integer(auth) || !integer(derived) || !integer(stats) || !integer(scratch)) {
        fail("byte accounting field is not an unsigned integer")
      }
      if (!number(amplification)) fail("space_amplification is not finite")
      if (reopen != "true") fail("reopen_verified is not true")
      if (gate != "true") fail("hard_gate_pass is not true")
      if (terminal != "OK") fail("terminal_status is not OK")
      if (mode == "space-audit") {
        if (scratch != "0") fail("scratch_bytes is nonzero")
        if ((auth + 0) == 0) {
          if ((derived + 0) != 0) fail("derived_bytes exceed authoritative bytes")
        } else if ((derived + 0) > (auth + 0) * 1.5) {
          fail("derived projection bytes exceed 1.5x authoritative bytes")
        }
        if ((derived + 0) == 0) {
          if ((stats + 0) != 0) fail("statistics_bytes exceeds 2% of derived bytes")
        } else if ((stats + 0) > (derived + 0) * 0.02) {
          fail("statistics_bytes exceeds 2% of derived bytes")
        }
      }
      emit(invalid ? "FAIL" : "PASS", reason, exit_value, gate, terminal,
           reopen, auth, derived, stats, scratch, amplification)
    }
    END {
      if (NR == 0 || rows == 0) {
        emit("FAIL", "missing data row", "", "", "", "", "", "", "", "", "")
      } else if (header_fields == 0) {
        emit("FAIL", "missing header", "", "", "", "", "", "", "", "")
      }
    }
  ' >> "$records"
}

verify_artifact_database() {
  local phase="$1" file="$2" records="$3" verification_dir="$4"
  local raw_path expected_facts expected_checksum
  IFS=$'\t' read -r raw_path expected_facts expected_checksum < <(
    csv_rows_as_tsv "$file" | awk -F '\t' '
      NR == 1 {
        for (i = 1; i <= NF; i++) column[$i] = i
        have_path = ("raw_sample_path" in column) && ("facts" in column) && ("dataset_checksum" in column)
        next
      }
      NR == 2 {
        if (have_path) print $(column["raw_sample_path"]) "\t" $(column["facts"]) "\t" $(column["dataset_checksum"])
        exit
      }
    '
  ) || true
  # Legacy synthetic artifacts have no database path.  They remain useful for
  # schema-only audit tests; real campaign artifacts always carry this field.
  [[ -n "$raw_path" ]] || return 0
  raw_path="${raw_path#\"}"
  raw_path="${raw_path%\"}"
  raw_path="${raw_path//\"\"/\"}"
  if [[ ! "$raw_path" = /* || ! -f "$raw_path/CURRENT" ]]; then
    printf 'FAIL\traw_sample_path is not an existing database\t-\tfalse\tartifact verification failed\tfalse\t-\t-\t-\t-\t-\n' >> "$records"
    return 0
  fi
  if [[ ! "$expected_facts" =~ ^[0-9]+$ || ! "$expected_checksum" =~ ^[0-9]+$ ]]; then
    printf 'FAIL\tfacts/dataset_checksum are not unsigned integers\t-\tfalse\tartifact verification failed\tfalse\t-\t-\t-\t-\t-\n' >> "$records"
    return 0
  fi
  local verify_case="verify-existing-$(basename "$(dirname "$file")")"
  local -a verify_cmd=("$build_dir/cedar_query_bench" "--path=$raw_path"
    --operation=state-at --duration-seconds=1 --verify-existing=true
    "--expected-facts=$expected_facts" "--expected-checksum=$expected_checksum"
    --reopen-verify=true)
  printf '%s,%s,' "$phase" "$verify_case" >> "$manifest"
  printf '%q ' "${verify_cmd[@]}" >> "$manifest"
  printf '\n' >> "$manifest"
  local csv="$verification_dir/verify.csv" json="$verification_dir/verify.json" rc
  mkdir -p "$verification_dir"
  set +e
  "${verify_cmd[@]}" > "$csv" 2> "$json"
  rc=$?
  set -e
  local gate reopen terminal auth derived stats scratch amplification
  if [[ -s "$csv" ]]; then
    IFS=$'\t' read -r gate reopen terminal auth derived stats scratch amplification < <(
      csv_rows_as_tsv "$csv" | awk -F '\t' '
        NR == 1 { for (i = 1; i <= NF; i++) c[$i] = i; next }
        NR == 2 {
          print $(c["hard_gate_pass"]) "\t" $(c["reopen_verified"]) "\t" \
                $(c["terminal_status"]) "\t" $(c["authoritative_bytes"]) "\t" \
                $(c["derived_bytes"]) "\t" $(c["statistics_bytes"]) "\t" \
                $(c["scratch_bytes"]) "\t" $(c["space_amplification"])
          exit
        }
      '
    ) || true
  fi
  if [[ "$rc" -ne 0 || "$gate" != true || "$reopen" != true || "$terminal" != OK ]]; then
    printf 'FAIL\tverify-existing command failed\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$rc" "${gate:-false}" "${terminal:-artifact verification failed}" "${reopen:-false}" \
      "${auth:--}" "${derived:--}" "${stats:--}" "${scratch:--}" "${amplification:--}" >> "$records"
    return 0
  fi
  if [[ "$phase" == space-audit ]]; then
    if [[ "$scratch" != 0 ]] || ! awk -v auth="$auth" -v derived="$derived" 'BEGIN {
      valid = auth ~ /^[0-9]+$/ && derived ~ /^[0-9]+$/
      bound = (auth + 0 == 0) ? (derived + 0 == 0) : (derived + 0 <= (auth + 0) * 1.5)
      exit !(valid && bound)
    }'; then
      printf 'FAIL\tactual storage space bound failed\t0\tfalse\tspace audit failed\ttrue\t%s\t%s\t%s\t%s\t%s\n' \
        "$auth" "$derived" "$stats" "$scratch" "$amplification" >> "$records"
    fi
  fi
}

run_artifact_audit() {
  local phase="$1"
  [[ -n "$input" && -d "$input" ]] || {
    echo "--input directory is required for $phase" >&2
    exit 2
  }
  local records="$output/.${phase}-audit-records"
  local audit_csv="$output/audit-summary.csv"
  local audit_json="$output/audit-summary.json"
  : > "$records"
  local input_run_count=0 run_file
  while IFS= read -r run_file; do
    input_run_count=$((input_run_count + 1))
    audit_run_csv "$phase" "$run_file" "$records"
    verify_artifact_database "$phase" "$run_file" "$records" "$output/.verify-${input_run_count}"
  done < <(find "$input" -type f -name run.csv -print)
  if ((input_run_count == 0)); then
    echo "$phase input contains no run.csv artifacts" >&2
    rm -f "$records"
    exit 2
  fi

  printf 'phase,file,status,reason,exit_code,hard_gate_pass,terminal_status,reopen_verified,authoritative_bytes,derived_bytes,statistics_bytes,scratch_bytes,space_amplification\n' > "$audit_csv"
  local total_rows=0 failed_rows=0 status reason exit_value gate terminal reopen auth derived stats scratch amplification
  while IFS=$'\t' read -r status reason exit_value gate terminal reopen auth derived stats scratch amplification; do
    [[ -n "$status" ]] || continue
    total_rows=$((total_rows + 1))
    [[ "$status" == PASS ]] || failed_rows=$((failed_rows + 1))
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$phase" "input-artifacts" "$status" "$reason" "$exit_value" "$gate" \
      "$terminal" "$reopen" "$auth" "$derived" "$stats" "$scratch" "$amplification" >> "$audit_csv"
  done < "$records"
  rm -f "$records"

  local audit_pass=true audit_exit=0 audit_gate=true audit_terminal=OK audit_reopen=true
  if ((failed_rows != 0)); then
    audit_pass=false
    audit_exit=1
    audit_gate=false
    audit_terminal="artifact audit failed"
    audit_reopen=false
    overall=1
  fi
  printf '{"phase":"%s","input":"%s","run_files":%d,"rows":%d,"pass":%s,"failed_rows":%d}\n' \
    "$phase" "$input" "$input_run_count" "$total_rows" "$audit_pass" "$failed_rows" > "$audit_json"
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$phase" "input-artifacts" "$audit_exit" \
    "$audit_gate" "$audit_terminal" 0 0 0 >> "$summary"
  printf '{"phase":"%s","case":"input-artifacts","input":"%s","status":"%s","exit_code":%d,"hard_gate_pass":"%s","terminal_status":"%s","facts_per_second":0,"end_to_end_p99_us":0,"wal_sync_p99_us":null,"run_files":%d,"rows":%d,"failed_rows":%d}\n' \
    "$phase" "$input" "$audit_pass" "$audit_exit" "$audit_gate" "$audit_terminal" "$input_run_count" "$total_rows" "$failed_rows" >> "$summary_json"
}

for phase in "${phases[@]}"; do
  case "$phase" in
    release-calibration) [[ "$facts_auto" == false ]] || { echo "release-calibration cannot consume auto-turning-point without a prior artifact" >&2; exit 2; }; while IFS= read -r facts; do run_case "$phase" "calibration-f${facts}" cold paused state-at "$facts" 1 8 10 1 canonical-only; done < <(csv_values "$facts_values" facts-per-txn); write_turning_point_artifact || { echo "release calibration produced no valid turning-point artifact" >&2; overall=1; } ;;
    write-idle-five-repeats) while IFS= read -r facts; do for repeat in 1 2 3 4 5; do while IFS= read -r writers; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold paused state-at "$facts" "$writers" 8 10 1 canonical-only; done < <(csv_values "$writers_values" writers); done; done < <(csv_values "$facts_values" facts-per-txn); if write_idle_overhead_artifact; then baseline="$output/../write-idle-baseline.csv"; if [[ ! -f "$baseline" && ( -z "$input" || ! -f "$input/write-idle-baseline.csv" ) ]]; then cp "$output/../write-idle-overhead.csv" "$baseline"; fi; fi; compare_idle_query_overhead || true ;;
    write-active-projection-five-repeats) while IFS= read -r facts; do for repeat in 1 2 3 4 5; do while IFS= read -r writers; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold active state-at "$facts" "$writers" 8 10 1 canonical-only; done < <(csv_values "$writers_values" writers); done; done < <(csv_values "$facts_values" facts-per-txn); compare_active_overhead || true ;;
    read-cold) for operation in state-at history events changes expand-out expand-in expand-both property-filter temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration; do while IFS= read -r readers; do while IFS= read -r degree; do while IFS= read -r selectivity; do while IFS= read -r projection; do run_case "$phase" "cold-${operation}-r${readers}-d${degree}-s${selectivity}-p${projection}" cold paused "$operation" 16 1 "$readers" "$degree" "$selectivity" "$(projection_value "$projection")"; done < <(csv_values "$projection_states" projection-states); done < <(csv_values "$selectivities_values" selectivities); done < <(csv_values "$degrees_values" degrees); done < <(csv_values "$readers_values" readers); done ;;
    read-warm) for operation in state-at history events changes expand-out expand-in expand-both property-filter temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration; do while IFS= read -r readers; do while IFS= read -r degree; do while IFS= read -r selectivity; do while IFS= read -r projection; do run_case "$phase" "warm-${operation}-r${readers}-d${degree}-s${selectivity}-p${projection}" warm paused "$operation" 16 1 "$readers" "$degree" "$selectivity" "$(projection_value "$projection")"; done < <(csv_values "$projection_states" projection-states); done < <(csv_values "$selectivities_values" selectivities); done < <(csv_values "$degrees_values" degrees); done < <(csv_values "$readers_values" readers); done ;;
    mixed-30-minute) [[ "$facts_auto" == false ]] || facts_values="$(resolve_turning_point)"; mixed_ops=(state-at events expand-out temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration); mixed_case_duration=$(( (duration + ${#mixed_ops[@]} - 1) / ${#mixed_ops[@]} )); for operation in "${mixed_ops[@]}"; do run_case "$phase" "mixed-${operation}" cold active "$operation" "$(first_csv_value "$facts_values" facts-per-txn)" "$(first_csv_value "$writers_values" writers)" "$(first_csv_value "$readers_values" readers)" "$(first_csv_value "$degrees_values" degrees)" "$(first_csv_value "$selectivities_values" selectivities)" canonical-only "$mixed_case_duration"; done ;;
    reopen-verification|space-audit) run_artifact_audit "$phase" ;;
  esac
done

# Compare active projection cases against the idle baseline when both phases
# are present. The Cedar campaign gate allows at most 10% throughput loss and
# 15% end-to-end p99 growth for active derived maintenance.
if [[ "$requested_phase" == all ]]; then
  idle_rate=$(awk -F, '$1=="write-idle-five-repeats" && $4=="true" {sum+=$6; n++} END {print n ? sum/n : 0}' "$summary")
  active_rate=$(awk -F, '$1=="write-active-projection-five-repeats" && $4=="true" {sum+=$6; n++} END {print n ? sum/n : 0}' "$summary")
  idle_p99=$(awk -F, '$1=="write-idle-five-repeats" && $4=="true" {sum+=$7; n++} END {print n ? sum/n : 0}' "$summary")
  active_p99=$(awk -F, '$1=="write-active-projection-five-repeats" && $4=="true" {sum+=$7; n++} END {print n ? sum/n : 0}' "$summary")
  if [[ "$idle_rate" != 0 && "$active_rate" != 0 ]] &&
     awk -v i="$idle_rate" -v a="$active_rate" 'BEGIN {exit !(a >= i*0.90)}' &&
     awk -v i="$idle_p99" -v a="$active_p99" 'BEGIN {exit !(i == 0 || a <= i*1.15)}'; then
    :
  else
    overall=1
    printf '{"gate":"active_projection_overhead","pass":false,"idle_facts_per_second":%s,"active_facts_per_second":%s,"idle_p99_us":%s,"active_p99_us":%s}\n' "$idle_rate" "$active_rate" "$idle_p99" "$active_p99" >> "$summary_json"
  fi
fi

if ((cases_run == 0)) && [[ "$requested_phase" != reopen-verification && "$requested_phase" != space-audit ]]; then
  echo "campaign produced zero cases" >&2
  overall=1
fi

cat "$summary"
exit "$overall"
