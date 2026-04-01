#pragma once

#include <memory>
#include <string>
#include <cstdint>

namespace cedar {

// 临时占位实现 - ShardedWAL
class ShardedWAL {
public:
    ShardedWAL() = default;
    ~ShardedWAL() = default;
    
    struct Stats {
        uint64_t total_writes = 0;
        uint64_t total_bytes = 0;
        double avg_latency_us = 0.0;
    };
    
    Stats GetStats() const { return {}; }
    
    bool Append(const std::string& data) { return true; }
    bool Sync() { return true; }
};

} // namespace cedar
