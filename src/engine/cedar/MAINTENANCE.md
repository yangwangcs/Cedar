# Embedded Engine Maintenance

The engine source is maintained in the Cedar repository. An upstream RocksDB
change is imported only through a dedicated Cedar branch and is never fetched
as a build or release dependency.

1. Record the reviewed upstream commit in `../rocksdb/PROVENANCE.md`.
2. Apply the upstream change while preserving Cedar-specific engine hooks and
   upstream copyright notices.
3. Build Debug and sanitizer profiles, then run Cedar storage, recovery,
   public-header, install-consumer, and bounded Release performance gates.
4. Update provenance and commit the source, verification evidence, and Cedar
   changes atomically.

Cedar-specific source belongs under this directory whenever a new extension
does not need to modify an existing engine algorithm. A narrow modification to
an existing RocksDB-derived file must retain its provenance annotation. No
change may introduce a second WAL or recovery authority, replace engine
MemTables/VersionSet/MANIFEST/sequence allocation, or bypass native
flush/compaction execution.
