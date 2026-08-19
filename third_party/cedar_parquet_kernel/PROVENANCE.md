# Cedar Parquet Kernel Provenance

This directory documents the Cedar-maintained minimal Parquet-format kernel
implemented in `src/engine/rocksdb/table/cedar_parquet`.

The initial files are a clean-room, limited implementation based on the
public Parquet file-format and Thrift compact-protocol specifications. They
were checked against Apache Arrow C++ 25.0.0 source layout at:

- `cpp/src/parquet/thrift_internal.h`
- `cpp/src/generated/parquet_types.{h,tcc}`

No upstream source text has been copied into the Cedar kernel in this change;
therefore no per-file upstream patch digest applies yet. Any future copied or
adapted source must retain its upstream Apache-2.0 header and add the exact
upstream revision, source path, and local patch SHA-256 here.
