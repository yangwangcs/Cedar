#!/usr/bin/env bash
set -euo pipefail

build_dir=
output=
requested_phase=all
duration=10

while (($#)); do
  case "$1" in
    --build-dir) build_dir="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --phase) requested_phase="$2"; shift 2 ;;
    --duration-seconds) duration="$2"; shift 2 ;;
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
printf 'phase,case,exit_code,hard_gate_pass,terminal_status\n' > "$summary"
: > "$summary_json"
overall=0

run_case() {
  local phase="$1" case_name="$2" cache="$3" projection_work="$4" operation="${5:-state-at}" facts_per_txn="${6:-16}" writers="${7:-1}"
  local case_dir="$output/$phase/$case_name"
  mkdir -p "$case_dir"
  local db="$case_dir/database" csv="$case_dir/run.csv" json="$case_dir/run.json"
  local -a cmd=("$build_dir/cedar_query_bench" "--path=$db" "--operation=$operation" "--projection-state=canonical-only" "--degree=10" "--selectivity-percent=1" "--readers=8" "--cache-state=$cache" "--projection-work=$projection_work" "--writers=$writers" "--facts-per-txn=$facts_per_txn" "--seed=1" "--duration-seconds=$duration" "--reopen-verify=true")
  printf '%s,%s,' "$phase" "$case_name" >> "$manifest"
  printf '%q ' "${cmd[@]}" >> "$manifest"
  printf '\n' >> "$manifest"
  set +e
  "${cmd[@]}" > "$csv" 2> "$json"
  local rc=$?
  set -e
  local gate=unknown terminal=unknown
  if [[ -s "$csv" ]]; then
    gate=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="hard_gate_pass") gate=i; next } NR==2 && gate { print $gate }' "$csv")
    terminal=$(awk -F, 'NR==1 { for (i=1;i<=NF;i++) if ($i=="terminal_status") status=i; next } NR==2 && status { print $status }' "$csv")
  fi
  printf '%s,%s,%s,%s,%s\n' "$phase" "$case_name" "$rc" "$gate" "$terminal" >> "$summary"
  printf '{"phase":"%s","case":"%s","exit_code":%s,"hard_gate_pass":"%s","terminal_status":"%s"}\n' "$phase" "$case_name" "$rc" "$gate" "$terminal" >> "$summary_json"
  if [[ "$rc" -ne 0 || "$gate" != true ]]; then overall=1; fi
}

for phase in "${phases[@]}"; do
  case "$phase" in
    release-calibration) run_case "$phase" calibration cold paused state-at 16 1 ;;
    write-idle-five-repeats) for repeat in 1 2 3 4 5; do for facts in 1 16 64 256; do for writers in 1 8; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold paused state-at "$facts" "$writers"; done; done; done ;;
    write-active-projection-five-repeats) for repeat in 1 2 3 4 5; do for facts in 16 64; do for writers in 1 8; do run_case "$phase" "repeat-$repeat-f${facts}-w${writers}" cold active state-at "$facts" "$writers"; done; done; done ;;
    read-cold) for operation in state-at events changes; do run_case "$phase" "cold-${operation}" cold paused "$operation" 16 1; done ;;
    read-warm) for operation in state-at events changes; do run_case "$phase" "warm-${operation}" warm paused "$operation" 16 1; done ;;
    mixed-30-minute) for operation in state-at events; do run_case "$phase" "mixed-${operation}" cold active "$operation" 64 8; done ;;
    reopen-verification) run_case "$phase" reopen cold paused state-at 64 2 ;;
    space-audit) for facts in 16 64 256; do run_case "$phase" "audit-f${facts}" cold paused state-at "$facts" 1; done ;;
  esac
done

cat "$summary"
exit "$overall"
