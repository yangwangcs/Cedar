# Storage Dependency

The active Cedar kernel has one canonical storage dependency: the pinned
RocksDB v11.1.2 source at `third_party/rocksdb`.

The pre-RocksDB compression libraries are preserved only under
`third_party/cedar_codecs` and are not built or
distributed by Cedar.
