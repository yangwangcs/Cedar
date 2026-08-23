# Task 19 Malformed Artifact Audit Fix

Status: implemented

The artifact auditor now stops row validation after a malformed CSV header has
been detected. This prevents missing required columns from being dereferenced
as awk's illegal `$()` field expression. The existing query campaign contract
therefore receives a normal failed audit record and summary row:

```text
reopen-verification,input-artifacts,1,false,artifact audit failed,0,0
```

Verification:

```text
bash -n benchmarks/run_cedar_query_campaign.sh
```

Manual malformed-artifact checks for both `reopen-verification` and
`space-audit` returned exit code 1, emitted no awk error, and produced the
expected `input-artifacts,1,false,artifact audit failed` summary row.
