# Cedar T-Cypher Production Support Matrix

Date: 2026-07-23

Status: Active release-closure evidence. “Supported” requires parser, binder,
typed physical plan, typed/vectorized runtime, projection, and regression
evidence. A logical/scalar fallback does not qualify.

| Shape | Parser/binder | Physical plan | Runtime/projection | Current evidence | Release state |
|---|---|---|---|---|---|
| Root valid-time point | Supported | `TemporalPointScan` | Typed streaming | root physical and oracle tests | Supported |
| Root valid-time range | Supported | range/derive/align/coalesce | Typed streaming | range, property alignment, EXPLAIN ANALYZE | Supported |
| Root valid-time changes | Supported | `ChangeScan` | Raw immutable events | change/oracle/property tests | Supported |
| Root system-time changes | Supported | `ChangeScan` + CommitTimeline | Raw immutable events | durable timeline and EXPLAIN tests | Supported |
| One fixed relationship | Supported | one Expand step | relationship struct/properties | point/range/change tests | Supported |
| One bounded variable relationship | Supported | one bounded Expand step | path list, `TRAIL`, spill | point/range/frontier tests | Supported |
| All-fixed multi-hop | Supported | ordered Expand steps | node/relationship/provenance/property projection | point/range/property/EXPLAIN tests | Supported |
| Variable then fixed, point | Supported; finite per-segment binder checks | Two ordered steps with independent bounds/path slot | Segmented typed frontier, global `TRAIL`, path list + fixed struct projection | `PhysicalMixedVariableThenFixedPointProjectsBothBindings`, `PhysicalMixedPointFrontierSpillsAndCleansQueryFiles`, `PhysicalMixedPointCancellationStopsBeforeFixedSegment`; planner bounds/fingerprint test | Implemented; full release/sanitizer/oracle gates pending |
| Fixed then variable, point | Supported; finite per-segment binder checks | Two ordered steps with independent bounds/path slot | Segmented typed frontier, global `TRAIL`, path list + fixed struct projection | `PhysicalMixedFixedThenVariablePointProjectsBothBindings`; planner bounds/fingerprint test; focused mixed 11/11 | Implemented; full release/sanitizer/oracle gates pending |
| Mixed fixed/variable, valid-time range | Supported; finite per-segment binder checks | Ordered steps with per-segment bounds/path slots | Segmented range frontier; complete-match interval intersection; path temporal and fixed projections; checksummed spill | `PhysicalMixedVariableThenFixedRangeIntersectsAllFacts`, `PhysicalMixedRangeAlignsIntermediateAndFixedRelationshipProperties`, `PhysicalMixedRangeFrontierSpillsAndCleansQueryFiles` | Implemented; full release/sanitizer/oracle gates pending |
| Mixed fixed/variable, changes | Parsed | No approved physical semantics | No runtime | exact negative regression required | Approved `NotSupported`: mixed variable/fixed paths are not supported in CHANGES mode |
| Fixed relationship property | Supported | typed property slot | exact edge-property gather | fixed and multi-hop property tests | Supported |
| Variable relationship property (`p.weight`) | Parsed | Ill-typed list dereference | No runtime | exact negative regression required | Approved `NotSupported`: property access on a variable relationship path is not supported |
| `EXPLAIN ANALYZE` physical point/range/change/Expand/join/spill | Supported for current physical candidates | Same plan as execution | Executes and serializes measured counters; missing dispatcher is Corruption | `ExplainAnalyzeSupportsMixedExpandOrdersAndRange`, focused Explain matrix plus per-hop Expand tests; generic range/change rejection removed | Supported for current candidates; full release/sanitizer artifact gate pending |

## Closure Rules

- The generic message `EXPLAIN ANALYZE for range/change physical runtime is not
  available yet` is not an approved support boundary. Every physical candidate
  must have a dispatcher or fail as an internal invariant.
- Variable bounds are finite and `TRAIL` applies to the complete relationship
  pattern.
- A variable relationship binding is a path list; a fixed relationship binding
  is a relationship struct.
- Focused tests do not replace final normal, ASAN, UBSAN, TSAN, fault/oracle,
  scheduler, benchmark, or paper-artifact gates.
