# Cedar T-Cypher Fusion Evidence

## Verified

- Branch: `codex/cedar-tcypher-fusion`.
- Build: Debug, `cmake --build build-tcypher -j1`, completed successfully.
- Full CTest: `747/747` passed; total test time `540.73s` after the system-time,
  metadata, and stateful Bolt changes.
- Canonical event contract: sequence metadata stores fact keys only; duplicate keys and
  mismatched commit sequences are rejected before publication.
- Parser/binder/compiler/API/write/server focused tests all passed.
- `cedar-server` and `cedar-admin` link against one `cedar_core` and own one `Database`.
- Bolt codec has bounded chunk framing and v5.4 handshake negotiation tests.
- `FOR SYSTEM_TIME AS OF` is applied as a snapshot ceiling in `PreparedCypher::Execute`; the
  valid-time scope remains independent and is never rewritten as system time.
- `valid_from(x)` and `commit_seq(x)` are typed metadata columns produced by the same
  QueryRuntime and resolved from canonical `FactEvent` winners, including graph endpoints.
- Mixed comma-separated patterns are rejected by the compiler with a typed
  `NotSupported` status; the compiler never silently drops all but the first pattern.
- The single-process server answers bounded Bolt request envelopes for HELLO/RUN/PULL/
  BEGIN/COMMIT/ROLLBACK/RESET/GOODBYE with bounded SUCCESS/IGNORED frames, typed parameter
  decoding, and cursor RECORD streaming. Explicit Bolt writes stage in one Cedar
  `Transaction`; only COMMIT publishes canonical facts.

## Measured Short Runs

These are smoke measurements, not sustained throughput claims:

| Workload | Result |
| --- | ---: |
| 4 parser tests | about `0.00s` process wall time |
| 4 binder tests | `0.06s` CTest wall time |
| 2 write tests including WAL/reopen | `1.26s` process wall time |
| full 741-test suite | `549.75s` CTest time |
| 5-second T-Cypher vertex scan | `52,171` operations, `10,434.2` ops/s, p50 `90us`, p95 `104us`, p99 `125us`, peak RSS `30,015,488` bytes, errors `0` |

The write smoke test performs two database opens, one transactional edge create, WAL
publication, close, reopen, and canonical fact scan. It is intentionally not used to claim
TPS. A sustained benchmark must run a continuous fixed-duration load and report p50/p95/p99,
WAL sync latency, columnar bytes, interval fragments, peak memory, and error counts.

## Complexity

- Lexing and parsing are `O(n)` in source bytes, with source/token/nesting caps.
- Schema property binding uses sorted catalog lookup, `O(log P)` per property name.
- Canonical event ordering sorts the bounded commit range by `(commit_seq, fact key)`.
- Bounded K-hop expansion remains bounded by the configured frontier and hop limits; `TRAIL`
  adds at most `max_hops` edge references per frontier label.

## Explicit Gaps

- System-time range scopes (`BETWEEN`) remain rejected because only bounded `AS OF` has a
  snapshot ceiling contract.
- Bolt `BEGIN`/`COMMIT`/`ROLLBACK` now owns one Cedar transaction per session. Reads while
  staged writes are pending are rejected until commit because the public QueryRuntime does
  not yet expose a transaction-local read view; this avoids claiming false read-your-writes.
- No sustained T-Cypher TPS number is claimed by this evidence file.
- The short benchmark above is a fixed-duration smoke run over one vertex scan and is
  not representative of mixed path, WAL, or columnar analytical throughput.
