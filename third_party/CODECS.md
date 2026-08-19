# Storage Dependency

The active Cedar kernel has one canonical storage engine: the embedded,
RocksDB v11.1.2-derived source at `src/engine/rocksdb`.

The pre-RocksDB compression libraries are preserved only under
`third_party/cedar_codecs` and are not built or
distributed by Cedar.
