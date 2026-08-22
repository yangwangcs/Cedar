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

while (($#)); do
  case "$1" in
    --build-dir) build_dir="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --phase) requested_phase="$2"; shift 2 ;;
    --duration-seconds) duration="$2"; shift 2 ;;
    --facts-per-txn) facts_values="$2"; shift 2 ;;
    --writers) writers_values="$2"; shift 2 ;;
    --readers) readers_values="$2"; shift 2 ;;
    --degrees) degrees_values="$2"; shift 2 ;;
    --selectivities) selectivities_values="$2"; shift 2 ;;
    --projection-states) projection_states="$2"; shift 2 ;;
    --input) input="$2"; shift 2 ;;
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
  input=$(cd "$input" && pwd)
fi
if [[ "$facts_values" == auto-turning-point ]]; then
  facts_auto=true
  facts_values=64
fi
for csv in "$facts_values" "$writers_values" "$readers_values" "$degrees_values" "$selectivities_values" "$projection_states"; do
  [[ "$csv" != *[[:space:]]* && -n "$csv" ]] || { echo "invalid empty/whitespace CSV option" >&2; exit 2; }
done
for value in ${facts_values//,/ }; do
  [[ "$value" =~ ^(1|4|8|16|32|64|128|256|512|1024|2048)$ ]] || { echo "unsupported facts-per-txn value: $value" >&2; exit 2; }
done
for value in ${writers_values//,/ }; do
  [[ "$value" =~ ^(1|2|8|32|64)$ ]] || { echo "unsupported writers value: $value" >&2; exit 2; }
done
for value in ${readers_values//,/ }; do
  [[ "$value" =~ ^(1|8|32)$ ]] || { echo "unsupported readers value: $value" >&2; exit 2; }
done
for value in ${degrees_values//,/ }; do
  [[ "$value" =~ ^(1|10|100|1000|10000)$ ]] || { echo "unsupported degree value: $value" >&2; exit 2; }
done
for value in ${selectivities_values//,/ }; do
  [[ "$value" =~ ^(0\.1|1|10|100)$ ]] || { echo "unsupported selectivity value: $value" >&2; exit 2; }
done
for value in ${projection_states//,/ }; do
  [[ "$value" =~ ^(canonical|base|short-delta|long-delta|partial)$ ]] || { echo "unsupported projection-state value: $value" >&2; exit 2; }
done

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

for phase in "${phases[@]}"; do
  case "$phase" in
    release-calibration) run_case "$phase" calibration cold paused state-at "$facts_values" 1 8 10 1 canonical-only ;;
    write-idle-five-repeats) for repeat in 1 2 3 4 5; do for facts in ${facts_values//,/ }; do for writers in ${writers_values//,/ }; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold paused state-at "$facts" "$writers" 8 10 1 canonical-only; done; done; done ;;
    write-active-projection-five-repeats) for repeat in 1 2 3 4 5; do for facts in ${facts_values//,/ }; do for writers in ${writers_values//,/ }; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold active state-at "$facts" "$writers" 8 10 1 canonical-only; done; done; done ;;
    read-cold) for operation in state-at history events changes expand-out expand-in expand-both temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration; do for readers in ${readers_values//,/ }; do for degree in ${degrees_values//,/ }; do for selectivity in ${selectivities_values//,/ }; do for projection in ${projection_states//,/ }; do run_case "$phase" "cold-${operation}-r${readers}-d${degree}-s${selectivity}-p${projection}" cold paused "$operation" 16 1 "$readers" "$degree" "$selectivity" "$(projection_value "$projection")"; done; done; done; done; done ;;
    read-warm) for operation in state-at history events changes expand-out expand-in expand-both temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration; do for readers in ${readers_values//,/ }; do for degree in ${degrees_values//,/ }; do for selectivity in ${selectivities_values//,/ }; do for projection in ${projection_states//,/ }; do run_case "$phase" "warm-${operation}-r${readers}-d${degree}-s${selectivity}-p${projection}" warm paused "$operation" 16 1 "$readers" "$degree" "$selectivity" "$(projection_value "$projection")"; done; done; done; done; done ;;
    mixed-30-minute) mixed_ops=(state-at events expand-out temporal-aggregate interval-join k-hop coexisting-shortest-path earliest-arrival latest-departure fastest-duration); mixed_case_duration=$(( (duration + ${#mixed_ops[@]} - 1) / ${#mixed_ops[@]} )); for operation in "${mixed_ops[@]}"; do run_case "$phase" "mixed-${operation}" cold active "$operation" "$facts_values" "${writers_values%%,*}" "${readers_values%%,*}" "${degrees_values%%,*}" "${selectivities_values%%,*}" canonical-only "$mixed_case_duration"; done ;;
    reopen-verification) run_case "$phase" reopen cold paused state-at 64 2 8 10 1 canonical-only ;;
    space-audit) for facts in 16 64 256; do run_case "$phase" "audit-f${facts}" cold paused state-at "$facts" 1 8 10 1 canonical-only ; done ;;
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

cat "$summary"
exit "$overall"
