# Cedar RocksDB Fork Provenance

This repository is Cedar's internal RocksDB storage-engine fork. Cedar
applications consume the Cedar product library and must not link or include
this repository directly.

## Baseline

- Upstream project: Facebook RocksDB
- Upstream release: `v11.1.2`
- Upstream baseline commit: `3b446089141659fad25328c5ea3e7ed283df46e4`
- Cedar kernel fork commit: `7ddbe68ba322b235b1d78591487ffed842ba9567`
- Intended Cedar fork tag: `cedar-v11.1.2-kernel.1`

The Cedar commit range starts at the upstream baseline and contains the Cedar
kernel runtime seams and Cedar Parquet facts table implementation. The Cedar
superproject pins an exact Cedar fork commit; it does not track an upstream
branch.

## License and Notices

RocksDB is distributed under Apache License 2.0. The existing upstream
`LICENSE.Apache`, `COPYING`, `NOTICE`, and third-party notices remain part of
every Cedar fork distribution. Cedar changes retain the same applicable
notices and must preserve upstream copyright headers.

## Ownership

RocksDB continues to own WAL append/recovery, MemTable, VersionSet, MANIFEST,
sequence allocation, and native flush/compaction execution. Cedar owns the
product API and uses explicit Cedar hooks to admit commits and grant native
maintenance work. Cedar Parquet facts files are table files, not an additional
WAL or recovery authority.
