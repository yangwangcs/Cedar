# Cedar Embedded Engine Provenance

This source tree is Cedar's internal, RocksDB-derived storage engine. It is
embedded in the Cedar repository under `src/engine/rocksdb`; it is not a Git
submodule or a separately published Cedar repository. Cedar applications
consume the Cedar product library and must not link or include this tree
directly.

## Baseline

- Upstream project: Facebook RocksDB
- Upstream release: `v11.1.2`
- Upstream baseline commit: `3b446089141659fad25328c5ea3e7ed283df46e4`
- Cedar kernel import commit: `7ddbe68ba322b235b1d78591487ffed842ba9567`

The Cedar import starts at the upstream baseline and contains the Cedar Kernel
runtime seams and Cedar Parquet facts table implementation. Cedar tracks this
source in its own history; it does not track an upstream branch or external
fork remote.

## License and Notices

RocksDB is distributed under Apache License 2.0. The existing upstream
`LICENSE.Apache`, `COPYING`, `NOTICE`, and third-party notices remain part of
every Cedar distribution. Cedar changes retain the same applicable
notices and must preserve upstream copyright headers.

## Ownership

RocksDB continues to own WAL append/recovery, MemTable, VersionSet, MANIFEST,
sequence allocation, and native flush/compaction execution. Cedar owns the
product API and uses explicit Cedar hooks to admit commits and grant native
maintenance work. Cedar Parquet facts files are table files, not an additional
WAL or recovery authority.
