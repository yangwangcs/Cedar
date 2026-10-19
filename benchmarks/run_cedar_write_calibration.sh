#!/usr/bin/env bash
# Repeatable Cedar write calibration matrix.
#
# Runs cedar_query_bench for every (writers, facts-per-txn) pair, repeats each
# case, and writes the raw rows plus a median summary. The query phase is not
# the subject of this script; write metrics remain valid when it hits its
# budget, so a non-OK terminal_status is reported as a warning.
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: run_cedar_write_calibration.sh --build-dir DIR --output FILE [options]
  --writers LIST        comma separated writer counts (default: 1,8)
  --facts LIST          comma separated facts-per-txn values (default: 1,16,64,256,1024)
  --seconds N           write window per run (default: 3)
  --repetitions N       runs per case (default: 3)
USAGE
}

build_dir=""
output=""
writers="1,8"
facts="1,16,64,256,1024"
seconds=3
repetitions=3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-dir) build_dir="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --writers) writers="$2"; shift 2 ;;
    --facts) facts="$2"; shift 2 ;;
    --seconds) seconds="$2"; shift 2 ;;
    --repetitions) repetitions="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$build_dir" && -n "$output" ]] || { usage >&2; exit 2; }
bench="${build_dir%/}/cedar_query_bench"
[[ -x "$bench" ]] || { echo "missing executable: $bench" >&2; exit 2; }

work_root="${TMPDIR:-/tmp}"
raw="$(mktemp "${work_root%/}/cedar-write-raw.XXXXXX")"
cleanup() { rm -f "$raw"; }
trap cleanup EXIT

python3 - "$raw" <<'PY'
import csv, sys
with open(sys.argv[1], "w", newline="") as out:
    csv.writer(out).writerow([
        "writers", "facts_per_txn", "rep", "facts_per_second",
        "transactions_per_second", "write_p50_us", "write_p99_us",
        "wal_sync_p99_us", "end_to_end_p99_us", "group_fill_p50",
        "terminal_status"])
PY

IFS=',' read -r -a writer_values <<< "$writers"
IFS=',' read -r -a fact_values <<< "$facts"

for w in "${writer_values[@]}"; do
  for f in "${fact_values[@]}"; do
    for ((rep = 1; rep <= repetitions; ++rep)); do
      run_dir="$(mktemp -d "${work_root%/}/cedar-wb-${w}-${f}-${rep}.XXXXXX")"
      # The benchmark owns and recreates --path, so its output must live
      # outside that directory or the redirect target is unlinked.
      run_out="$(mktemp "${work_root%/}/cedar-wb-out.XXXXXX")"
      run_err="$(mktemp "${work_root%/}/cedar-wb-err.XXXXXX")"
      "$bench" \
        --path="$run_dir" \
        --operation=state-at \
        --projection-work=paused \
        --writers="$w" \
        --facts-per-txn="$f" \
        --duration-seconds="$seconds" \
        --reopen-verify=false \
        > "$run_out" 2> "$run_err" || true
      python3 - "$w" "$f" "$rep" "$run_out" "$raw" <<'PY'
import csv, sys
w, f, rep, path, raw = sys.argv[1:6]
rows = []
try:
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
except OSError:
    rows = []
fields = ["facts_per_second", "transactions_per_second", "write_p50_us",
          "write_p99_us", "wal_sync_p99_us", "end_to_end_p99_us",
          "group_fill_p50", "terminal_status"]
row = rows[0] if rows else {}
with open(raw, "a", newline="") as out:
    csv.writer(out).writerow([w, f, rep] + [row.get(name, "") for name in fields])
PY
      rm -rf "$run_dir" "$run_out" "$run_err"
      echo "case writers=$w facts=$f rep=$rep" >&2
    done
  done
done

cp "$raw" "$output"

python3 - "$output" "$output.summary.csv" <<'PY'
import csv, statistics, sys
raw_path, summary_path = sys.argv[1], sys.argv[2]
numeric = ["facts_per_second", "transactions_per_second", "write_p50_us",
           "write_p99_us", "wal_sync_p99_us", "end_to_end_p99_us",
           "group_fill_p50"]
groups = {}
order = []
bad = 0
with open(raw_path, newline="") as handle:
    for row in csv.DictReader(handle):
        key = (row["writers"], row["facts_per_txn"])
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(row)
        try:
            if float(row["facts_per_second"]) <= 0:
                bad += 1
        except (TypeError, ValueError):
            bad += 1
        if row["terminal_status"] != "OK":
            print(f"warning: writers={key[0]} facts={key[1]} "
                  f"rep={row['rep']} terminal_status={row['terminal_status']}",
                  file=sys.stderr)
with open(summary_path, "w", newline="") as out:
    writer = csv.writer(out)
    writer.writerow(["writers", "facts_per_txn", "reps"] + numeric + ["terminal_status"])
    for key in order:
        rows = groups[key]
        medians = []
        for name in numeric:
            values = [float(row[name]) for row in rows if row[name] not in ("", None)]
            medians.append(f"{statistics.median(values):.6g}" if values else "")
        writer.writerow([key[0], key[1], len(rows)] + medians + [rows[0]["terminal_status"]])
if bad:
    print(f"error: {bad} run(s) produced no positive facts_per_second", file=sys.stderr)
    sys.exit(2)
PY

echo "raw: $output" >&2
echo "summary: $output.summary.csv" >&2
