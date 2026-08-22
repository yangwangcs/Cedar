#!/usr/bin/env bash
set -euo pipefail
build_dir= output= phase=release-calibration duration=10
while (($#)); do case "$1" in --build-dir) build_dir="$2";shift 2;; --output) output="$2";shift 2;; --phase) phase="$2";shift 2;; --duration-seconds) duration="$2";shift 2;; *) echo "unknown option: $1" >&2; exit 2;; esac; done
[[ -x "$build_dir/cedar_query_bench" && -n "$output" ]] || exit 2
mkdir -p "$output"; db="$output/database"; printf 'phase=%s\nduration_seconds=%s\n' "$phase" "$duration" > "$output/commands.manifest"
"$build_dir/cedar_query_bench" --path="$db" --operation=state-at --degree=10 --selectivity-percent=1 --readers=1 --cache-state=cold --duration-seconds="$duration" --reopen-verify=true > "$output/run.csv" 2> "$output/run.json"
cat "$output/run.csv"
