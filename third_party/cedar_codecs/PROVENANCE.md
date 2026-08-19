# Cedar codec provenance

The `lz4` and `zstd` directories are source snapshots retained by Cedar solely
to build the pinned Cedar RocksDB implementation with reproducible codec
inputs. Their licenses remain in the respective source directories.

The build hashes every source file in these directories. Changes require an
explicit Cedar RocksDB cache rebuild and release verification.
