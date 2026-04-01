// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 自动Blob存储管理器 - 透明处理大小数据

#ifndef FERN_AUTO_BLOB_STORAGE_H_
#define FERN_AUTO_BLOB_STORAGE_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/descriptor.h"
#include "cedar/frond/blob_file_manager.h"

namespace cedar {

class LsmEngine;
class CedarGraphStorage;

/**
 * @brief 自动存储策略配置
 */
struct AutoBlobConfig {
    // 内联字符串最大长度（超过此长度存Blob）
    size_t inline_string_max_len = 4;
    
    // 最小Blob大小（小于此长度强制内联）
    size_t min_blob_size = 1;
    
    // 是否启用自动Blob
    bool enable_auto_blob = true;
};

/**
 * @brief Blob引用信息（存储在辅助列中）
 */
struct BlobRef {
    uint32_t file_id;
    uint32_t offset;
    uint32_t size;
};

/**
 * @brief 自动Blob存储管理器
 * 
 * 提供透明的大对象存储，自动决策内联或Blob存储
 * 
 * 使用方式:
 *   AutoBlobStorage auto_storage(engine, blob_mgr);
 *   auto_storage.PutString(entity_id, col_id, "long string data...");
 *   auto value = auto_storage.GetString(entity_id, col_id);
 */
class AutoBlobStorage {
 public:
    /**
     * @brief 创建自动存储管理器
     * @param engine LSM引擎指针
     * @param blob_mgr Blob文件管理器（可选，如果不提供则所有数据内联）
     * @param config 配置
     */
    AutoBlobStorage(LsmEngine* engine,
                    BlobFileManager* blob_mgr,
                    const AutoBlobConfig& config = AutoBlobConfig{});
    
    ~AutoBlobStorage();
    
    // 禁止拷贝
    AutoBlobStorage(const AutoBlobStorage&) = delete;
    AutoBlobStorage& operator=(const AutoBlobStorage&) = delete;
    
    /**
     * @brief 存储字符串（自动选择内联或Blob）
     * @param entity_id 实体ID
     * @param col_id 主列ID
     * @param value 字符串值
     * @return 是否成功
     */
    Status PutString(uint64_t entity_id, uint16_t col_id, const std::string& value);
    
    /**
     * @brief 读取字符串（自动识别存储方式）
     * @param entity_id 实体ID
     * @param col_id 主列ID
     * @return 字符串值，不存在返回nullopt
     */
    std::optional<std::string> GetString(uint64_t entity_id, uint16_t col_id);
    
    /**
     * @brief 存储二进制数据（自动选择内联或Blob）
     * @param entity_id 实体ID
     * @param col_id 主列ID
     * @param data 二进制数据
     * @param size 数据大小
     * @return 是否成功
     */
    Status PutBinary(uint64_t entity_id, uint16_t col_id, const void* data, size_t size);
    
    /**
     * @brief 读取二进制数据
     * @param entity_id 实体ID
     * @param col_id 主列ID
     * @return 二进制数据，不存在返回空vector
     */
    std::vector<uint8_t> GetBinary(uint64_t entity_id, uint16_t col_id);
    
    /**
     * @brief 同步Blob文件
     */
    Status SyncBlob();
    
    /**
     * @brief 获取统计信息
     */
    struct Stats {
        size_t inline_stores = 0;   // 内联存储次数
        size_t blob_stores = 0;     // Blob存储次数
        size_t inline_reads = 0;    // 内联读取次数
        size_t blob_reads = 0;      // Blob读取次数
    };
    Stats GetStats() const { return stats_; }
    void ResetStats() { stats_ = Stats{}; }

 private:
    // 计算Blob引用列ID
    uint16_t GetBlobHiCol(uint16_t col_id) const { return col_id + 1000; }
    uint16_t GetBlobLoCol(uint16_t col_id) const { return col_id + 2000; }
    
    // 内联存储字符串
    Status PutInlineString(uint64_t entity_id, uint16_t col_id, const std::string& value);
    
    // Blob存储字符串
    Status PutBlobString(uint64_t entity_id, uint16_t col_id, const std::string& value);
    
    // 尝试读取内联字符串
    std::optional<std::string> GetInlineString(uint64_t entity_id, uint16_t col_id);
    
    // 尝试读取Blob字符串
    std::optional<std::string> GetBlobString(uint64_t entity_id, uint16_t col_id);
    
    LsmEngine* engine_;
    BlobFileManager* blob_mgr_;
    AutoBlobConfig config_;
    Stats stats_;
};

}  // namespace cedar

#endif  // FERN_AUTO_BLOB_STORAGE_H_
