# Cedar T-Cypher Expand and Change Physical Runtime Evidence

Date: 2026-07-22

Status: Functional checkpoint complete; unified sanitizer matrix remains deferred

## Scope

This checkpoint closes the physical fixed/variable Expand blocking-sink path,
relationship valid/system-time change execution, structured relationship
grouping, and typed DISTINCT output ownership. It preserves the pinned
`QuerySnapshot`, complete `LogicalKey` edge identity, immutable
`TemporalEvent` semantics, and zero legacy history materialization for the
covered query shapes.

## Correctness Fixes

- `CHANGES FOR SYSTEM_TIME` resolves `VALID_TIME AS OF` against each complete
  logical edge timeline before applying the half-open system-time window.
- System-axis changes use bounded external ordering by
  `(commit_seq, logical_key)` rather than valid-time ordering.
- AS OF resolution uses constant per-timeline state in the naturally
  LogicalKey-ordered merge; it does not retain an unbounded set of keys.
- Grouped aggregate keys retain scalar, relationship struct, and list kinds
  through in-memory grouping, spill encoding, replay, and typed output.
- Grouped `COLLECT` applies the same typed-key contract, including structured
  relationship keys through partitioned spill and replay.
- Grouped aggregate output transfers its existing memory reservation to the
  returned batch.
- DISTINCT reserves typed output payloads before copying and retains the
  reservation until the returned batch is released.

## Regression Evidence

Focused relationship, grouping, and memory tests:

```text
18/18 passed
```

Expanded T-Cypher change/expand/physical/result-stream set:

```text
121/121 passed
```

Normal full test matrix:

```text
667/667 passed
```

The fixed-seed relationship change oracle uses:

```text
seed = 0xCE4A20260722
cases = 24
```

Hygiene:

```text
git diff --check: passed
explicit trailing-whitespace scan: no matches
```

## Deferred Unified Gates

ASAN, UBSAN, TSAN, the final cross-module fault/oracle matrix, and paper-level
benchmark regeneration remain part of the later unified release verification.
This checkpoint does not claim completion of the six-design Goal.
