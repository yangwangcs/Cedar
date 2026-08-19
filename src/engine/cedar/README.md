# Cedar Engine Extensions

This directory contains Cedar-owned extensions to the embedded engine.

Do not include files from this directory or `../rocksdb` through
`include/cedar`. Existing Cedar changes inside `../rocksdb` retain their
narrow provenance annotations until a separately tested change moves them
here.
