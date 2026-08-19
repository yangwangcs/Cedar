# Pre-RocksDB Kernel Archive

This directory preserves the pre-clean-break Cedar implementation removed from
the production build on 2026-08-01. It is source-only reference material:
neither CMake nor release-source verification traverses it.

The active product is the explicit-transaction embedded kernel built from
`include/cedar/{core,fact,types}`, the root public headers, and
`src/{core,fact,kernel,types}`. RocksDB is its sole canonical persistent
store. The archived source must not be reintroduced as a compatibility path or
linked into `cedar_core`.
