# Cedar T-Cypher Fusion Evidence

## Verified

- Branch: `codex/cedar-tcypher-fusion`.
- Build: Debug, `cmake --build build-tcypher -j1`, completed successfully.
- Full CTest: `733/733` passed; total test time `144.92s`.
- Canonical event contract: sequence metadata stores fact keys only; duplicate keys and
  mismatched commit sequences are rejected before publication.
- Parser/binder/compiler/API/write/server focused tests all passed.
- `cedar-server` and `cedar-admin` link against one `cedar_core` and own one `Database`.
- Bolt codec has bounded chunk framing and v5.4 handshake negotiation tests.

## Measured Short Runs

These are smoke measurements, not sustained throughput claims:

| Workload | Result |
| --- | ---: |
| 4 parser tests | about `0.00s` process wall time |
| 4 binder tests | `0.06s` CTest wall time |
| 2 write tests including WAL/reopen | `1.26s` process wall time |
| full 733-test suite | `144.92s` CTest time |

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

- System-time scopes are parsed and bound but currently return `NotSupported` during lowering;
  they are never silently treated as valid-time.
- Metadata projections such as `valid_from(e)` are parsed but require a Cedar row-schema
  metadata projection node before execution.
- The server currently exposes a bounded text `RUN` endpoint plus Bolt handshake/chunk codec;
  full Bolt `HELLO/RUN/PULL/BEGIN/COMMIT/ROLLBACK` message handling remains the next server
  increment.
- No sustained T-Cypher TPS number is claimed by this evidence file.
