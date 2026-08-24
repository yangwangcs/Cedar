# Task 19 Final Contract Hardening Report

Date: 2026-08-22
Base: `0d4dd7d`

## Changes

- Hardened `resolve_turning_point()` to accept only the runner's valid JSON
  artifact shape, with an exact positive `facts_per_txn` token and optional
  escaped `source_summary`. Negative values, trailing characters, duplicate
  keys, trailing commas, and malformed JSON fail closed without digit
  normalization. The parser remains Bash 3.2 compatible and uses `awk` only.
- Hardened active projection overhead validation so every selected
  `hard_gate_pass=true` row must contain positive finite
  `facts_per_second` and `end_to_end_p99_us` values before aggregation.
- Extended the campaign contract test with a valid turning-point artifact and
  a mixed-phase manifest assertion for the selected peak candidate (`64`),
  malformed artifact rejection cases, and independent non-zero summary
  coverage for both `reopen-verification` and `space-audit` unsupported paths.
- Step 7 cross-database reopen/checksum/space verification remains explicitly
  unsupported because `cedar_query_bench` exposes no interface for it; no
  implementation is fabricated.

## Verification

- `bash -n benchmarks/run_cedar_query_campaign.sh`
- `git diff --check`
- Targeted `QueryCampaignOptionsContract` CTest (when the query benchmark
  target is available) or equivalent manual runner checks.
