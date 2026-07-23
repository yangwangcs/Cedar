# LDBC-derived adapter evidence

The benchmark layer now includes a deterministic `AdaptLdbcCsv` adapter for
the bounded paper-run interchange subset:

- nodes: `id,valid_from,commit_seq,name,operation`;
- edges: `source_id,target_id,edge_id,edge_type,valid_from,commit_seq,operation`;
- `PUT` and `DELETE` are case-insensitive;
- input row order is irrelevant; output events are canonically sorted;
- the resulting Cedar-TG event stream receives the same BLAKE3 canonical hash
  as native Cedar-TG generation;
- source name, license, and transform policy are retained in
  `LdbcTemporalDataset` and can be copied into the benchmark manifest.

The current manifest schema carries `source_dataset_kind`,
`source_dataset_license`, and `source_transform_policy` as required fields.
There is no implicit reader compatibility path for manifests from the prior
design; an older artifact must be regenerated with the current schema before
it can enter a paper or release comparison.

Verification:

```text
build-v2/tests/test_correctness_kernel --gtest_filter='BenchmarkLdbcAdapterTest.*:BenchmarkArtifactTest.*:DurableLogTest.BenchmarkWorkloadDriverRunsVerifiedPublicApiFamilies'
```

Result: 8/8 passed. The adapter is intentionally a derived temporal adapter;
it does not claim official LDBC temporal benchmark status. External parity
comparisons must retain the upstream dataset/version/license and this exact
transform policy in the manifest.

The standalone benchmark runner now accepts the same interchange subset:

```text
cedar_bench --ldbc <nodes.csv> <edges.csv> <results-root> \
  [workload] [source-license] [transform-policy]
```

This path uses the adapter's canonical dataset hash and copies its source
name, license, and transform policy into the strict current manifest schema.
The original numeric Cedar-TG invocation remains available for synthetic
runs; the two modes produce distinct `dataset_id` values and provenance.
