#!/usr/bin/env bash
set -euo pipefail

campaign_summary=""
storage_csv=""
projection_pages_skipped=""
while (($#)); do
  case "$1" in
    --campaign-summary) campaign_summary="$2"; shift 2 ;;
    --storage-csv) storage_csv="$2"; shift 2 ;;
    --projection-pages-skipped) projection_pages_skipped="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

[[ -s "$campaign_summary" && -s "$storage_csv" ]] || {
  echo "campaign and storage CSV paths are required" >&2
  exit 2
}

awk -F, '
  NR == 1 { next }
  $3 != 0 || $4 != "true" { failed++ }
  END { if (NR <= 1 || failed != 0) exit 1 }
' "$campaign_summary"

awk -F, '
  NR == 1 { next }
  $NF != 0 { failed++ }
  END { if (NR <= 1 || failed != 0) exit 1 }
' "$storage_csv"

if [[ -n "$projection_pages_skipped" ]]; then
  [[ "$projection_pages_skipped" =~ ^[1-9][0-9]*$ ]] || {
    echo "projection page skip evidence is missing" >&2
    exit 1
  }
fi

echo "query/storage read acceptance gates passed"
