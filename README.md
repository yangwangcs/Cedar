# Cedar

Cedar is a clean-break C++17 temporal graph HTAP storage kernel.

The current implementation uses the canonical Cedar architecture:

- typed `Value` and persistent `SchemaRegistry` epochs;
- immutable bitemporal events with `valid_from` and 64-bit `commit_seq`;
- shard prepare logs, global commit decisions, recovery, and a continuous visible prefix;
- Zone-Columnar SST pages, granule blocks, file checksums, and VersionSet snapshots;
- typed `CedarDatabase` public API.

Legacy Descriptor, Frond SST, LsmEngine, legacy OCC/WAL, graph facade, and Cypher APIs were removed. Existing databases using legacy formats are not supported.

## Build

LZ4 1.10.0 and Zstd 1.5.7 are pinned under `third_party/` and compiled
statically by default. Building and starting Cedar does not install packages or
require host codec libraries.

```bash
cmake -S . -B build -DBUILD_TESTS=ON
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

## API Shape

```cpp
cedar::CedarDatabase db("/data/cedar", 8, 0xC0FFEEULL);
db.Open();

cedar::ColumnSchema schema{/* entity type, column, epoch, logical type,
                               physical type, blob threshold, policies */};
cedar::ColumnSchema registered;
db.RegisterColumn(schema, &registered);

const auto key = cedar::LogicalKey::VertexProperty(42, 7);
db.Put(key, 1000, registered.schema_epoch, cedar::Value::Int64(99));
auto value = db.Get(key, 1000);
```

All writes are schema-checked durable transactions. `Flush()` publishes canonical SST files through VersionSet snapshots.
