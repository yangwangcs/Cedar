# Cedar T-Cypher Release Support Matrix

This is the release boundary for the current typed physical runtime. A shape
listed as unsupported must fail deterministically during binding/planning or
with a terminal `NotSupported` status; it must never invoke a legacy executor.

| Query shape | Current release status | Evidence / next action |
|---|---|---|
| Root point state, fixed Expand, joins, aggregates, DISTINCT, ORDER BY, COLLECT | Supported | Physical runtime and ExplainAnalyze regressions in `tests/test_correctness_kernel.cc` |
| Root valid-time state/range/change | Supported for declared physical candidates | Range/change/interval and blocking-sink regressions; complete ExplainAnalyze coverage still required for every candidate branch |
| System-time state and `CHANGES BETWEEN` | Supported for declared point/range/change candidates | CommitTimeline and change-order tests; add release artifact fields |
| One bounded variable-length relationship pattern | Supported | Frontier, trail, path-value, temporal-bound and spill regressions |
| Variable-length relationship mixed with additional fixed relationship in one pattern | Not supported in this release | `src/tcypher/executor.cc` returns deterministic `NotSupported`; retain explicit regression and document as future extension |
| Multi-hop node/relationship property projection forms rejected by the physical planner | Not supported in this release | Keep typed planner failure; do not fall back to materialization; add each accepted syntax to a negative-test table |
| Physical range/change `EXPLAIN ANALYZE` outside `physical_runtime_candidate` | Not supported until closed | Remove the broad gap by either routing every supported physical candidate through the profile runtime or adding an explicit negative support entry and release wording |

This matrix is subordinate to the six-design completion matrix. Any change to
the support boundary must add a parser/binder/planner/runtime regression and a
corresponding ExplainAnalyze or deterministic-failure artifact.
