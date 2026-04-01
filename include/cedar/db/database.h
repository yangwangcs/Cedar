// Copyright (c) 2025 The CedarGraph Authors. All rights reserved.
// Database lifecycle management for CedarGraph

#ifndef FERN_DB_DATABASE_H_
#define FERN_DB_DATABASE_H_

#include <thread>
#include <memory>
#include <chrono>
#include <string>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>

#include "cedar/core/status.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {

// ============================================================================
// 类型定义
// ============================================================================

/**
 * 数据库生命周期状态
 */
enum class DatabaseState {
    kCreated = 0,    // 已创建（刚new出来）
    kOpening = 1,    // 正在打开/恢复
    kReady = 2,      // 就绪（可正常操作）
    kClosing = 3,    // 正在关闭
    kClosed = 4,     // 已关闭
    kError = 5       // 错误状态
};

/**
 * 数据库统计信息
 */
struct DatabaseStats {
    uint64_t total_nodes = 0;
    uint64_t total_edges = 0;
    uint64_t storage_size_bytes = 0;
    uint64_t memtable_size_bytes = 0;
    uint64_t sst_file_count = 0;
    uint64_t blob_file_count = 0;
    double cache_hit_rate = 0.0;
    std::string uptime;
    uint64_t total_reads = 0;
    uint64_t total_writes = 0;
};

/**
 * 数据库配置
 */
struct DatabaseConfig {
    // 路径
    std::string db_path;
    
    // 启动选项
    bool create_if_missing = true;
    bool error_if_exists = false;
    bool enable_wal = true;
    bool enable_recovery = true;       // 崩溃恢复
    bool enable_fast_recovery = false; // 快速恢复（跳过完整WAL重放）
    bool paranoid_checks = false;      // 严格检查
    
    // 关闭选项
    bool sync_on_close = true;              // 关闭时同步
    bool wait_for_compaction = true;        // 等待压缩完成
    uint32_t close_timeout_ms = 30000;      // 关闭超时
    
    // 后台任务
    uint32_t background_threads = 4;
    bool enable_auto_compaction = true;
    bool enable_auto_flush = true;
    
    // 存储选项（透传到 CedarOptions）
    size_t memtable_threshold = 4 * 1024 * 1024;
    size_t write_buffer_size = 4 * 1024 * 1024;
    size_t file_cache_size = 256;
    bool enable_bloom_filter = true;
    int bloom_bits_per_key = 10;
    
    // 转换为 CedarOptions
    CedarOptions ToCedarOptions() const {
        CedarOptions opts;
        opts.create_if_missing = create_if_missing;
        opts.error_if_exists = error_if_exists;
        opts.enable_wal = enable_wal;
        opts.memtable_threshold = memtable_threshold;
        opts.write_buffer_size = write_buffer_size;
        opts.file_cache_size = file_cache_size;
        opts.enable_bloom_filter = enable_bloom_filter;
        opts.bloom_bits_per_key = bloom_bits_per_key;
        return opts;
    }
};

/**
 * 数据库选项（别名）
 */
using DBOptions = DatabaseConfig;

/**
 * 回调函数类型
 */
using BackgroundTask = std::function<void()>;
using CompactionCallback = std::function<void(Status)>;

// ============================================================================
// Database 类声明
// ============================================================================

/**
 * Database - 数据库生命周期管理类
 * 
 * 提供:
 * - 优雅启动（自动恢复）
 * - 优雅关闭（数据同步）
 * - 状态管理
 * - 资源管理
 */
class Database {
public:
    // ==================== 禁止直接构造 ====================
    
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    
    // ==================== 静态工厂方法 ====================
    
    /**
     * 打开数据库（带自动恢复）
     * 
     * 流程:
     * 1. 检查并创建必要目录
     * 2. 加载 Manifest/元数据
     * 3. 恢复 MemTable（从 WAL 或 SSTD）
     * 4. 重建索引
     * 5. 启动后台线程
     * 6. 标记为 Ready
     */
    static Status Open(const DatabaseConfig& config, 
                      std::unique_ptr<Database>* db_ptr);
    
    /**
     * 打开数据库（使用默认配置）
     */
    static Status Open(const std::string& db_path,
                      std::unique_ptr<Database>* db_ptr);
    
    /**
     * 关闭数据库（优雅关闭）
     */
    Status Close();
    
    // ==================== 状态查询 ====================
    
    /**
     * 获取当前状态
     */
    DatabaseState GetState() const;
    
    /**
     * 检查是否就绪
     */
    bool IsReady() const;
    
    /**
     * 检查是否正在关闭
     */
    bool IsClosing() const;
    
    /**
     * 检查是否已关闭
     */
    bool IsClosed() const;
    
    /**
     * 检查是否有错误
     */
    bool HasError() const;
    
    /**
     * 获取状态字符串
     */
    std::string GetStateString() const;
    
    // ==================== 存储访问 ====================
    
    /**
     * 获取底层存储（线程安全）
     * 如果数据库未就绪，返回 nullptr
     */
    CedarGraphStorage* GetStorageOrNull();
    
    /**
     * 获取底层存储（如果未就绪会 panic）
     * 仅在确认数据库就绪时使用
     */
    CedarGraphStorage* GetStorage();
    
    /**
     * 获取底层存储（带状态检查）
     * 返回存储和状态
     */
    std::pair<CedarGraphStorage*, Status> TryGetStorage();
    
    // ==================== 管理操作 ====================
    
    /**
     * 刷新内存表到磁盘
     */
    Status Flush();
    
    /**
     * 触发压缩
     */
    Status Compact();
    
    /**
     * 创建检查点
     */
    Status CreateCheckpoint(const std::string& checkpoint_dir);
    
    /**
     * 从检查点恢复
     */
    static Status RestoreFromCheckpoint(const std::string& checkpoint_dir,
                                       const std::string& db_path,
                                       const DatabaseConfig& config = DatabaseConfig{});
    
    // ==================== 统计 ====================
    
    /**
     * 获取统计信息
     */
    DatabaseStats GetStats() const;
    
    /**
     * 获取配置信息
     */
    const DatabaseConfig& GetConfig() const;
    
    // ==================== 高级操作 ====================
    
    /**
     * 执行后台任务
     * 如果数据库未就绪，返回错误
     */
    Status Execute(BackgroundTask task);
    
    /**
     * 等待后台任务完成
     */
    void Wait();
    
    // ==================== 析构 ====================
    
    /**
     * 析构函数 - 确保已关闭
     */
    ~Database();
    
private:
    // ==================== 私有构造函数 ====================
    
    explicit Database(const DatabaseConfig& config);
    
    // ==================== 内部方法 ====================
    
    Status OpenImpl();
    Status CloseImpl();
    Status Recover();
    Status StartBackgroundThreads();
    Status StopBackgroundThreads();
    Status SyncAndSaveCheckpoint();
    Status VerifyDataIntegrity();
    
    // 状态管理
    void SetState(DatabaseState state);
    bool TrySetState(DatabaseState expected, DatabaseState desired);
    
    // 目录操作
    Status EnsureDirectoriesExist();
    bool IsFirstOpen() const;
    
    // 检查点操作
    static Status CopyDirectoryRecursive(const std::string& src, const std::string& dst);
    static Status SaveManifestCheckpoint(const std::string& checkpoint_dir, const DatabaseConfig& config);
    static Status LoadManifestCheckpoint(const std::string& checkpoint_dir, const DatabaseConfig& config);
    
    // ==================== 成员变量 ====================
    
    // 配置
    DatabaseConfig config_;
    
    // 状态
    std::atomic<DatabaseState> state_;
    std::string error_message_;
    
    // 底层存储
    std::unique_ptr<CedarGraphStorage> storage_;
    
    // 线程管理
    std::vector<std::thread> background_threads_;
    std::atomic<bool> stop_background_;
    
    // 关闭同步
    mutable std::mutex close_mutex_;
    std::condition_variable close_cv_;
    std::atomic<uint32_t> active_operations_;
    
    // 启动时间
    std::chrono::steady_clock::time_point start_time_;
    
    // 统计
    std::atomic<uint64_t> total_reads_;
    std::atomic<uint64_t> total_writes_;
    
    // 友元声明
    friend class DatabaseTest;
};

// ============================================================================
// 便捷函数
// ============================================================================

/**
 * 销毁数据库
 */
Status DestroyDatabase(const std::string& db_path,
                      const DatabaseConfig& config = DatabaseConfig{});

/**
 * 检查数据库是否存在
 */
bool DatabaseExists(const std::string& db_path);

/**
 * 获取数据库文件大小
 */
uint64_t GetDatabaseSize(const std::string& db_path);

}  // namespace cedar

#endif  // FERN_DB_DATABASE_H_
