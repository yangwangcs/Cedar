// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 实体生命周期追踪（Entity Lifecycle Tracking）

#ifndef FERN_ENTITY_LIFECYCLE_H_
#define FERN_ENTITY_LIFECYCLE_H_

#include <cstdint>
#include <vector>
#include <optional>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/status.h"

namespace cedar {

// 系统保留的 column_id（用户属性使用 0-4094，系统使用 4095）
// 注意：Descriptor 只支持 12 位 column_id (0-4095)
constexpr uint16_t kLifecycleColumnId = 0xFFF;  // 4095

// 生命周期事件类型
enum class LifecycleEvent : uint32_t {
    Unknown = 0,
    Created = 1,      // 实体被创建
    Deleted = 2,      // 实体被删除（软删除）
    Recreated = 3,    // 删除后重新创建
    Purged = 4,       // 实体被永久清除（硬删除）
};

// 实体状态
enum class EntityState {
    NeverExisted,     // 从未创建过（无生命周期记录）
    Active,           // 存在
    Deleted,          // 已删除
    Purged            // 已永久清除
};

// 生命周期历史记录
struct LifecycleEntry {
    Timestamp timestamp;
    LifecycleEvent event;
    
    LifecycleEntry() = default;
    LifecycleEntry(Timestamp ts, LifecycleEvent e) : timestamp(ts), event(e) {}
};

// 存活时间段
struct AlivePeriod {
    Timestamp start;
    Timestamp end;  // Timestamp::Max() 表示至今仍然存活
    
    AlivePeriod() = default;
    AlivePeriod(Timestamp s, Timestamp e) : start(s), end(e) {}
    
    // 是否包含某时间点
    bool Contains(Timestamp ts) const {
        return ts >= start && ts <= end;
    }
    
    // 持续时间（微秒）
    // 如果 end 是 Timestamp::Max()，表示至今存活，返回 0（需要外部计算当前时间）
    uint64_t DurationMicros() const {
        if (end.value() == std::numeric_limits<uint64_t>::max()) {
            return 0;  // 至今存活，无法计算固定时长
        }
        return end.value() - start.value();
    }
    
    // 计算到指定时间点的持续时间（用于处理至今存活的情况）
    uint64_t DurationMicrosUntil(Timestamp until) const {
        if (until < start) return 0;
        if (until > end && end.value() != std::numeric_limits<uint64_t>::max()) {
            return end.value() - start.value();
        }
        return until.value() - start.value();
    }
};

// 生命周期工具类
class LifecycleDescriptor {
 public:
    // 从 Descriptor 解析生命周期事件
    static std::optional<LifecycleEvent> Parse(const Descriptor& desc);
    
    // 创建生命周期事件的 Descriptor
    static Descriptor Create(LifecycleEvent event);
    
    // 检查是否为生命周期描述符
    static bool IsLifecycleDescriptor(const Descriptor& desc);
};

// 生命周期事件转字符串
const char* LifecycleEventToString(LifecycleEvent event);
const char* EntityStateToString(EntityState state);

}  // namespace cedar

#endif  // FERN_ENTITY_LIFECYCLE_H_
