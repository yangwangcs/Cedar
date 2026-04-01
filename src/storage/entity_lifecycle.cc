// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

std::optional<LifecycleEvent> LifecycleDescriptor::Parse(const Descriptor& desc) {
    // 检查是否为生命周期描述符（column_id = 0xFFFF）
    if (desc.GetColumnId() != kLifecycleColumnId) {
        return std::nullopt;
    }
    
    // 检查类型是否为 InlineInt
    if (desc.GetKind() != EntryKind::InlineInt) {
        return std::nullopt;
    }
    
    uint32_t payload = desc.GetPayload();
    if (payload > static_cast<uint32_t>(LifecycleEvent::Purged)) {
        return std::nullopt;
    }
    
    return static_cast<LifecycleEvent>(payload);
}

Descriptor LifecycleDescriptor::Create(LifecycleEvent event) {
    // 使用 InlineInt 类型，column_id = 0xFFFF，payload = event
    return Descriptor::InlineInt(kLifecycleColumnId, static_cast<int32_t>(event));
}

bool LifecycleDescriptor::IsLifecycleDescriptor(const Descriptor& desc) {
    return desc.GetColumnId() == kLifecycleColumnId && 
           desc.GetKind() == EntryKind::InlineInt;
}

const char* LifecycleEventToString(LifecycleEvent event) {
    switch (event) {
        case LifecycleEvent::Unknown: return "Unknown";
        case LifecycleEvent::Created: return "Created";
        case LifecycleEvent::Deleted: return "Deleted";
        case LifecycleEvent::Recreated: return "Recreated";
        case LifecycleEvent::Purged: return "Purged";
        default: return "Invalid";
    }
}

const char* EntityStateToString(EntityState state) {
    switch (state) {
        case EntityState::NeverExisted: return "NeverExisted";
        case EntityState::Active: return "Active";
        case EntityState::Deleted: return "Deleted";
        case EntityState::Purged: return "Purged";
        default: return "Invalid";
    }
}

}  // namespace cedar
