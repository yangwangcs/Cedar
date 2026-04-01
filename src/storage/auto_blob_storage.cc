// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 自动Blob存储管理器实现

#include "cedar/storage/auto_blob_storage.h"

#include <cstring>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/slice.h"

namespace cedar {

AutoBlobStorage::AutoBlobStorage(LsmEngine* engine,
                                  BlobFileManager* blob_mgr,
                                  const AutoBlobConfig& config)
    : engine_(engine),
      blob_mgr_(blob_mgr),
      config_(config),
      stats_{} {}

AutoBlobStorage::~AutoBlobStorage() = default;

Status AutoBlobStorage::PutString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
    if (!engine_) {
        return Status::InvalidArgument("Storage not initialized");
    }
    
    // 决策：小字符串内联，大字符串Blob
    bool use_blob = config_.enable_auto_blob && 
                    blob_mgr_ &&
                    value.size() > config_.inline_string_max_len &&
                    value.size() >= config_.min_blob_size;
    
    if (use_blob) {
        return PutBlobString(entity_id, col_id, value);
    } else {
        return PutInlineString(entity_id, col_id, value);
    }
}

Status AutoBlobStorage::PutInlineString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
    // 将字符串编码到整数（最多4字节）
    uint32_t encoded = 0;
    size_t len = std::min(value.size(), size_t(4));
    memcpy(&encoded, value.data(), len);
    
    CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
    Descriptor desc = Descriptor::InlineInt(col_id, static_cast<int32_t>(encoded));
    
    Status s = engine_->Put(key, desc, Timestamp(0));
    if (s.ok()) {
        stats_.inline_stores++;
    }
    return s;
}

Status AutoBlobStorage::PutBlobString(uint64_t entity_id, uint16_t col_id, const std::string& value) {
    if (!blob_mgr_) {
        return Status::NotSupported("Blob manager not available");
    }
    
    // 写入Blob
    uint32_t file_id, offset, size;
    Status s = blob_mgr_->WriteBlob(Slice(value), &file_id, &offset, &size);
    if (!s.ok()) {
        // Blob失败，回退到内联
        return PutInlineString(entity_id, col_id, value);
    }
    
    // 存储Blob位置到辅助列
    uint16_t blob_hi_col = GetBlobHiCol(col_id);
    uint16_t blob_lo_col = GetBlobLoCol(col_id);
    
    CedarKey key_hi = CedarKey::Vertex(entity_id, blob_hi_col, Timestamp(0));
    Descriptor desc_hi = Descriptor::InlineInt(blob_hi_col, static_cast<int32_t>(file_id));
    engine_->Put(key_hi, desc_hi, Timestamp(0));
    
    CedarKey key_lo = CedarKey::Vertex(entity_id, blob_lo_col, Timestamp(0));
    Descriptor desc_lo = Descriptor::InlineInt(blob_lo_col, static_cast<int32_t>(offset));
    engine_->Put(key_lo, desc_lo, Timestamp(0));
    
    // 在主列存储原始大小（用于读取）
    CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
    Descriptor desc = Descriptor::InlineInt(col_id, static_cast<int32_t>(size));
    engine_->Put(key, desc, Timestamp(0));
    
    stats_.blob_stores++;
    return Status::OK();
}

std::optional<std::string> AutoBlobStorage::GetString(uint64_t entity_id, uint16_t col_id) {
    if (!engine_) {
        return std::nullopt;
    }
    
    // 先尝试读取Blob
    auto blob_result = GetBlobString(entity_id, col_id);
    if (blob_result.has_value()) {
        return blob_result;
    }
    
    // 回退到内联读取
    return GetInlineString(entity_id, col_id);
}

std::optional<std::string> AutoBlobStorage::GetInlineString(uint64_t entity_id, uint16_t col_id) {
    auto versions = engine_->GetAll(entity_id, EntityType::Vertex, col_id);
    if (versions.empty()) {
        return std::nullopt;
    }
    
    auto val = versions[0].descriptor.AsInlineInt();
    if (!val) {
        return std::nullopt;
    }
    
    // 解码为字符串
    char buf[5] = {};
    uint32_t payload = static_cast<uint32_t>(*val);
    memcpy(buf, &payload, 4);
    
    stats_.inline_reads++;
    return std::string(buf);
}

std::optional<std::string> AutoBlobStorage::GetBlobString(uint64_t entity_id, uint16_t col_id) {
    if (!blob_mgr_) {
        return std::nullopt;
    }
    
    uint16_t blob_hi_col = GetBlobHiCol(col_id);
    uint16_t blob_lo_col = GetBlobLoCol(col_id);
    
    // 读取Blob位置
    auto hi_versions = engine_->GetAll(entity_id, EntityType::Vertex, blob_hi_col);
    auto lo_versions = engine_->GetAll(entity_id, EntityType::Vertex, blob_lo_col);
    
    if (hi_versions.empty() || lo_versions.empty()) {
        return std::nullopt;  // 不是Blob存储
    }
    
    auto hi_val = hi_versions[0].descriptor.AsInlineInt();
    auto lo_val = lo_versions[0].descriptor.AsInlineInt();
    
    if (!hi_val || !lo_val) {
        // 尝试直接作为内联读取
        return std::nullopt;
    }
    
    uint32_t file_id = static_cast<uint32_t>(*hi_val);
    uint32_t offset = static_cast<uint32_t>(*lo_val);
    
    // 读取原始列获取size
    auto orig_versions = engine_->GetAll(entity_id, EntityType::Vertex, col_id);
    if (orig_versions.empty()) {
        return std::nullopt;
    }
    
    auto size_val = orig_versions[0].descriptor.AsInlineInt();
    if (!size_val || *size_val <= 0) {
        return std::nullopt;
    }
    
    // 从Blob读取
    std::string data;
    Status s = blob_mgr_->ReadBlob(file_id, offset, *size_val, &data);
    if (!s.ok()) {
        return std::nullopt;
    }
    
    stats_.blob_reads++;
    return data;
}

Status AutoBlobStorage::PutBinary(uint64_t entity_id, uint16_t col_id, const void* data, size_t size) {
    std::string str(static_cast<const char*>(data), size);
    return PutString(entity_id, col_id, str);
}

std::vector<uint8_t> AutoBlobStorage::GetBinary(uint64_t entity_id, uint16_t col_id) {
    auto str_opt = GetString(entity_id, col_id);
    if (!str_opt) {
        return {};
    }
    
    std::vector<uint8_t> result(str_opt->begin(), str_opt->end());
    return result;
}

Status AutoBlobStorage::SyncBlob() {
    if (blob_mgr_) {
        return blob_mgr_->Sync();
    }
    return Status::OK();
}

}  // namespace cedar
