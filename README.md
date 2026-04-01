# Cedar

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]() [![C++17](https://img.shields.io/badge/C++-17-blue.svg)](https://isocpp.org/) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Cedar** is a columnar LSM-engine for temporal property graphs. It unifies vertices and edges as an append-only stream of 32-byte fixed-length events, providing high-throughput ingestion (14 μs write latency) and efficient historical analytics via version-chain indexing and zone-columnar storage.

## Features

* **Unified Event Encoding**: 32-byte fixed-length keys homogenize vertex/edge/property updates. Hardware-aligned to 64-byte cache lines.
* **Version Chain SkipList (VCSL)**: Lock-free skip list with vertical version chains. O(1) latest-state lookup, O(log N + V′) time-travel queries.
* **Zone-Columnar Layout**: Disk-resident SSTs partitioned into 5 semantic zones (Topology / Temporal / Metadata / Property / Blob). ZLM (Zone-Level Merge) compaction reduces write amplification.
* **Temporal Consistency**: Native support for `AS OF` point queries and `BETWEEN` range scans with snapshot isolation.

## Performance

Comparison against Aion (Neo4j-based temporal graph DB) on 674 M temporal records (RE-Europe dataset):

| Metric | Cedar | Aion |
|--------|-------|------|
| Storage | 6.98 GB | 54.97 GB |
| Write Latency | 14 μs | 200 μs |
| AS OF Query | 0.8 ms | 6 ms |
| BETWEEN Query | 56 ms | 65 ms |
| Temporal BFS (Depth 3) | 270 ms | 890 ms |

## Building

### Requirements

* C++17 compiler (GCC 7+, Clang 5+, MSVC 2017+)
* CMake 3.14+
* LZ4, Protocol Buffers
* gRPC (optional, for RPC server)

### macOS (Homebrew)

```bash
brew install cmake pkg-config lz4 grpc googletest
```

### Linux (Ubuntu / Debian)

```bash
sudo apt-get update
sudo apt-get install -y cmake pkg-config liblz4-dev libprotobuf-dev protobuf-compiler libgrpc++-dev libgtest-dev
```

### Compile

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Build with Examples & Tests

```bash
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=ON -DBUILD_TESTS=ON
make -j$(nproc)
ctest --output-on-failure
```

## Example

```cpp
#include "cedar/storage/cedar_graph_storage.h"
#include <iostream>

int main() {
    cedar::CedarOptions options;
    options.create_if_missing = true;

    cedar::CedarGraphStorage* storage = nullptr;
    cedar::Status s = cedar::CedarGraphStorage::Open(
        options, "/data/cedar", &storage);

    // Store temporal event: vertex 123, timestamp 1700000000000000, value 42
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, 42);
    s = storage->Put(123ULL, 1700000000000000ULL, desc, cedar::Timestamp(1));

    // AS OF query: read state at specific timestamp
    auto result = storage->Get(123ULL, 1700000000000000ULL);
    if (result.has_value()) {
        if (auto val = result->AsInlineInt()) {
            std::cout << "Value: " << *val << std::endl;
        }
    }

    delete storage;
    return 0;
}
```

## Repository Structure

```
├── include/cedar/          # Public headers (CedarGraphStorage, Descriptor, Status)
├── src/                    # Implementation (VCSL, Zone-Columnar SST, ZLM compaction)
├── proto/                  # Protocol buffer definitions
├── examples/               # Example programs
├── tests/                  # Unit tests (GoogleTest)
├── cmake/                  # CMake helper modules
└── CMakeLists.txt          # Build configuration
```

## Architecture

* **Memory**: VCSL — Strictly partitioned into Entity Index Layer (lock-free skip list) and Version Data Layer (lightweight version nodes with older pointers).
* **Disk**: Zone-Columnar SST — Zones 0 & 2 (Topology), Zone 1 (Temporal Vector), Zone 3 (Metadata), Zone 4 (Property Values with inline/Blob threshold).
* **Compaction**: ZLM — Differential processing across zones. Metadata zones use K-way merge; large Blobs use reference forwarding (zero-copy) during L0→L1.

```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
