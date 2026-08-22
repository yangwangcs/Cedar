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
printf 'phase,case,exit_code,hard_gate_pass,terminal_status,facts_per_second,end_to_end_p99_us\n' > "$summary"
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
  local gate=unknown terminal=unknown facts_rate=0 end_p99=0
  if [[ -s "$csv" ]]; then
    gate=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="hard_gate_pass") gate=i; next } NR==2 && gate { print $gate }' "$csv")
    terminal=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="terminal_status") status=i; next } NR==2 && status { print $status }' "$csv")
    facts_rate=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="facts_per_second") rate=i; next } NR==2 && rate { print $rate }' "$csv")
    end_p99=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="end_to_end_p99_us") p=i; next } NR==2 && p { print $p }' "$csv")
  fi
  printf '%s,%s,%s,%s,%s,%s,%s\n' "$phase" "$case_name" "$rc" "$gate" "$terminal" "$facts_rate" "$end_p99" >> "$summary"
  printf '{"phase":"%s","case":"%s","exit_code":%s,"hard_gate_pass":"%s","terminal_status":"%s","facts_per_second":%s,"end_to_end_p99_us":%s}\n' "$phase" "$case_name" "$rc" "$gate" "$terminal" "$facts_rate" "$end_p99" >> "$summary_json"
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
  awk -F, '$1=="write-idle-five-repeats" && $4=="true" {rate+=$6; p99+=$7; n++} END {if (n>0) printf "samples,%d\navg_facts_per_second,%s\navg_end_to_end_p99_us,%s\n", n, rate/n, p99/n}' "$source" > "$target"
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

for phase in "${phases[@]}"; do
  case "$phase" in
    release-calibration) [[ "$facts_auto" == false ]] || { echo "release-calibration cannot consume auto-turning-point without a prior artifact" >&2; exit 2; }; while IFS= read -r facts; do run_case "$phase" "calibration-f${facts}" cold paused state-at "$facts" 1 8 10 1 canonical-only; done < <(csv_values "$facts_values" facts-per-txn); write_turning_point_artifact || { echo "release calibration produced no valid turning-point artifact" >&2; overall=1; } ;;
    write-idle-five-repeats) while IFS= read -r facts; do for repeat in 1 2 3 4 5; do while IFS= read -r writers; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold paused state-at "$facts" "$writers" 8 10 1 canonical-only; done < <(csv_values "$writers_values" writers); done; done < <(csv_values "$facts_values" facts-per-txn); write_idle_overhead_artifact ;;
    write-active-projection-five-repeats) while IFS= read -r facts; do for repeat in 1 2 3 4 5; do while IFS= read -r writers; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold active state-at "$facts" "$writers" 8 10 1 canonical-only; done < <(csv_values "$writers_values" writers); done; done < <(csv_values "$facts_values" facts-per-txn); compare_active_overhead || true ;;
    read-cold) for operation in state-at history events changes expand-out expand-in expand-both property-filter temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration; do while IFS= read -r readers; do while IFS= read -r degree; do while IFS= read -r selectivity; do while IFS= read -r projection; do run_case "$phase" "cold-${operation}-r${readers}-d${degree}-s${selectivity}-p${projection}" cold paused "$operation" 16 1 "$readers" "$degree" "$selectivity" "$(projection_value "$projection")"; done < <(csv_values "$projection_states" projection-states); done < <(csv_values "$selectivities_values" selectivities); done < <(csv_values "$degrees_values" degrees); done < <(csv_values "$readers_values" readers); done ;;
    read-warm) for operation in state-at history events changes expand-out expand-in expand-both property-filter temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration; do while IFS= read -r readers; do while IFS= read -r degree; do while IFS= read -r selectivity; do while IFS= read -r projection; do run_case "$phase" "warm-${operation}-r${readers}-d${degree}-s${selectivity}-p${projection}" warm paused "$operation" 16 1 "$readers" "$degree" "$selectivity" "$(projection_value "$projection")"; done < <(csv_values "$projection_states" projection-states); done < <(csv_values "$selectivities_values" selectivities); done < <(csv_values "$degrees_values" degrees); done < <(csv_values "$readers_values" readers); done ;;
    mixed-30-minute) [[ "$facts_auto" == false ]] || facts_values="$(resolve_turning_point)"; mixed_ops=(state-at events expand-out temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration); mixed_case_duration=$(( (duration + ${#mixed_ops[@]} - 1) / ${#mixed_ops[@]} )); for operation in "${mixed_ops[@]}"; do run_case "$phase" "mixed-${operation}" cold active "$operation" "$(first_csv_value "$facts_values" facts-per-txn)" "$(first_csv_value "$writers_values" writers)" "$(first_csv_value "$readers_values" readers)" "$(first_csv_value "$degrees_values" degrees)" "$(first_csv_value "$selectivities_values" selectivities)" canonical-only "$mixed_case_duration"; done ;;
    reopen-verification|space-audit) [[ -n "$input" && -d "$input" ]] || { echo "--input directory is required for $phase" >&2; exit 2; }; input_run_count=0; while IFS= read -r _input_run; do input_run_count=$((input_run_count + 1)); done < <(find "$input" -type f -name run.csv -print); ((input_run_count > 0)) || { echo "$phase input contains no run.csv artifacts" >&2; exit 2; }; overall=1; printf '%s,%s,1,false,unsupported,0,0\n' "$phase" "input-artifacts" >> "$summary"; printf '{"phase":"%s","input":"%s","status":"unsupported","reason":"cross-artifact verification is not supported by cedar_query_bench"}\n' "$phase" "$input" >> "$summary_json" ;;
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
