# Cross-partition canonical query fix

## Root cause

Canonical temporal reads used `PartId{0}` as though it were a wildcard. It is
an actual Cedar home partition, so `TemporalSource` and `CanonicalSource`
silently omitted facts in every other partition. Property binding then grouped
states by bare entity ID, which could also associate a property from one
partition with an entity having the same ID in another partition.

## Change

- Use the existing `Snapshot::ScanFamily` storage contract for family-wide
  canonical reads. It enumerates all home partitions while preserving the
  storage-owned fact decoding and snapshot boundary.
- Filter temporal property chains by the requested property ID.
- Key temporal chains and property binding state by `(part_id, entity_id)` and
  propagate the original partition into returned `FactRef`/`VertexRef` values.
- Add a regression covering canonical vertices, events, point state,
  history, and public vertex-property binding for two partitions sharing an
  entity ID.

## Verification

- `test_query_canonical`: 21/21
- `test_temporal_expand`: 10/10
- `test_query_differential`: 16/16
- New regression: `QueryCanonicalTest.ReadsCanonicalAndPropertiesAcrossHomePartitions`

The fix does not change public scan APIs or treat partition zero as a
wildcard; it reuses the existing explicit family-wide scan boundary.
