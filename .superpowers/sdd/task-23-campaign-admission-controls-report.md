# Task 23: Campaign Admission Controls

## Status

Implemented explicit campaign admission controls with validated positive values:

- `--commit-deadline-us` default `5000000`
- `--group-queue-requests` default `2048`
- `--group-queue-bytes` default `33554432`

Every generated benchmark command, including existing-artifact verification,
passes all three controls. The campaign manifest, summary CSV/JSONL, and raw
benchmark CSV/JSON expose the selected values. Cedar public defaults and hard
gates were unchanged.

The short explicit campaign used absolute paths under
`/tmp/cedar-task23-case.vqJyiB`: exit `0`, terminal `OK`, hard gate `true`, and
raw metadata `5000000,2048,33554432` in both CSV and JSON.

## Commits

- Implementation commit: `2f0d7ca` (`test: strengthen task 22 setup admission regression`).
- This report is intentionally kept as a separate ignored-file commit/artifact.

## Tests

- `bash -n benchmarks/run_cedar_query_campaign.sh`
- `git diff --check`
- `ctest --test-dir build/query-release -R QueryCampaignOptions --output-on-failure`
  - Passed: `1/1`, 208.41s.
- Explicit one-case `release-calibration` campaign with duration 1s and
  absolute build/output paths: exit `0`, terminal `OK`, hard gate `true`.
- Six malformed admission values (`0`, `-1` for each control): all rejected
  with exit `2` and a positive-value error.

## Concerns

The explicit run is a short validation case only; it provides no claim about
sustained campaign capability.
