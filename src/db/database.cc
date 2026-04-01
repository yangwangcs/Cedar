// Copyright (c) 2025 The CedarGraph Authors. All rights reserved.
// Database lifecycle management implementation

#include "cedar/db/database.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

#include "cedar/core/env.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace fs = std::filesystem;

namespace cedar {

// ============================================================================
// 便捷函数实现
// ============================================================================

bool DatabaseExists(const std::string& db_path) {
    return fs::exists(db_path) && fs::is_directory(db_path);
}

uint64_t GetDatabaseSize(const std::string& db_path) {
    if (!DatabaseExists(db_path)) {
        return 0;
    }
    
    uint64_t total_size = 0;
    for (const auto& entry : fs::recursive_directory_iterator(db_path)) {
        if (entry.is_regular_file()) {
            total_size += entry.file_size();
        }
    }
    return total_size;
}

Status DestroyDatabase(const std::string& db_path, const DatabaseConfig& config) {
    // 先尝试优雅关闭
    std::unique_ptr<Database> db;
    Status s = Database::Open(config, &db);
    if (s.ok()) {
        s = db->Close();
    }
    
    // 删除目录
    if (fs::exists(db_path)) {
        try {
            fs::remove_all(db_path);
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Failed to remove database: ") + e.what());
        }
    }
    
    return Status::OK();
}

// ============================================================================
// Database 构造函数
// ============================================================================

Database::Database(const DatabaseConfig& config)
    : config_(config),
      state_(DatabaseState::kCreated),
      stop_background_(false),
      active_operations_(0),
      start_time_(std::chrono::steady_clock::now()),
      total_reads_(0),
      total_writes_(0) {
}

// ============================================================================
// 静态工厂方法
// ============================================================================

Status Database::Open(const DatabaseConfig& config, std::unique_ptr<Database>* db_ptr) {
    if (db_ptr == nullptr) {
        return Status::InvalidArgument("db_ptr cannot be nullptr");
    }
    
    if (config.db_path.empty()) {
        return Status::InvalidArgument("db_path cannot be empty");
    }
    
    // 创建数据库实例
    auto db = std::unique_ptr<Database>(new Database(config));

    // 打开数据库
    Status status = db->OpenImpl();
    
    if (!status.ok()) {
        db->SetState(DatabaseState::kError);
        db->error_message_ = status.ToString();
        return status;
    }
    
    *db_ptr = std::move(db);
    return Status::OK();
}

Status Database::Open(const std::string& db_path, std::unique_ptr<Database>* db_ptr) {
    DatabaseConfig config;
    config.db_path = db_path;
    config.create_if_missing = true;
    return Open(config, db_ptr);
}

// ============================================================================
// 状态管理
// ============================================================================

DatabaseState Database::GetState() const {
    return state_.load();
}

bool Database::IsReady() const {
    return state_.load() == DatabaseState::kReady;
}

bool Database::IsClosing() const {
    return state_.load() == DatabaseState::kClosing;
}

bool Database::IsClosed() const {
    return state_.load() == DatabaseState::kClosed;
}

bool Database::HasError() const {
    return state_.load() == DatabaseState::kError;
}

std::string Database::GetStateString() const {
    switch (state_.load()) {
        case DatabaseState::kCreated:  return "Created";
        case DatabaseState::kOpening:  return "Opening";
        case DatabaseState::kReady:    return "Ready";
        case DatabaseState::kClosing:  return "Closing";
        case DatabaseState::kClosed:   return "Closed";
        case DatabaseState::kError:    return "Error: " + error_message_;
        default:                       return "Unknown";
    }
}

void Database::SetState(DatabaseState state) {
    state_.store(state);
}

bool Database::TrySetState(DatabaseState expected, DatabaseState desired) {
    return state_.compare_exchange_strong(expected, desired);
}

// ============================================================================
// 存储访问
// ============================================================================

CedarGraphStorage* Database::GetStorageOrNull() {
    if (IsReady()) {
        return storage_.get();
    }
    return nullptr;
}

CedarGraphStorage* Database::GetStorage() {
    auto* storage = GetStorageOrNull();
    if (storage == nullptr) {
        // 这里我们不panic，而是返回 nullptr 让调用者处理
        // 如果确定数据库就绪，可以使用 GetStorageOrNull
    }
    return storage;
}

std::pair<CedarGraphStorage*, Status> Database::TryGetStorage() {
    if (!IsReady()) {
        return {nullptr, Status::IOError("Database not ready: " + GetStateString())};
    }
    return {storage_.get(), Status::OK()};
}

// ============================================================================
// 管理操作
// ============================================================================

Status Database::Flush() {
    auto [storage, status] = TryGetStorage();
    if (!status.ok()) {
        return status;
    }
    return storage->ForceFlush();
}

Status Database::Compact() {
    auto [storage, status] = TryGetStorage();
    if (!status.ok()) {
        return status;
    }
    return storage->Compact();
}

Status Database::CreateCheckpoint(const std::string& checkpoint_dir) {
    auto [storage, status] = TryGetStorage();
    if (!status.ok()) {
        return status;
    }
    
    // 检查目标目录
    if (fs::exists(checkpoint_dir)) {
        return Status::IOError("Checkpoint directory already exists: " + checkpoint_dir);
    }
    
    // 创建检查点目录
    try {
        fs::create_directories(checkpoint_dir);
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to create checkpoint directory: ") + e.what());
    }
    
    // 1. 刷新内存数据
    status = storage->ForceFlush();
    if (!status.ok()) {
        return Status::IOError("Failed to flush before checkpoint: " + status.ToString());
    }
    
    // 2. 复制数据库文件
    status = CopyDirectoryRecursive(config_.db_path, checkpoint_dir);
    if (!status.ok()) {
        return status;
    }
    
    // 3. 保存清单信息
    status = SaveManifestCheckpoint(checkpoint_dir, config_);
    if (!status.ok()) {
        return status;
    }
    
    return Status::OK();
}

Status Database::RestoreFromCheckpoint(const std::string& checkpoint_dir,
                                       const std::string& db_path,
                                       const DatabaseConfig& config) {
    // 检查检查点目录是否存在
    if (!fs::exists(checkpoint_dir)) {
        return Status::IOError("Checkpoint directory not found: " + checkpoint_dir);
    }
    
    // 验证检查点目录是否有效（包含必要的文件）
    if (!fs::exists(checkpoint_dir + "/MANIFEST")) {
        return Status::IOError("Invalid checkpoint: MANIFEST not found");
    }
    
    // 如果目标数据库已存在，先删除
    if (fs::exists(db_path)) {
        try {
            fs::remove_all(db_path);
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Failed to remove existing database: ") + e.what());
        }
    }
    
    // 从检查点恢复
    Status status = CopyDirectoryRecursive(checkpoint_dir, db_path);
    if (!status.ok()) {
        return status;
    }
    
    // 验证恢复的数据
    status = LoadManifestCheckpoint(db_path, config);
    if (!status.ok()) {
        return status;
    }
    
    return Status::OK();
}

// ============================================================================
// 统计
// ============================================================================

DatabaseStats Database::GetStats() const {
    DatabaseStats stats;
    
    // 基本统计
    stats.total_reads = total_reads_.load();
    stats.total_writes = total_writes_.load();
    
    // 运行时间
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
    auto hours = duration / 3600;
    auto minutes = (duration % 3600) / 60;
    auto seconds = duration % 60;
    
    std::ostringstream oss;
    oss << hours << "h " << minutes << "m " << seconds << "s";
    stats.uptime = oss.str();
    
    // 存储统计
    if (storage_) {
        auto storage_stats = storage_->GetStats();
        stats.memtable_size_bytes = storage_stats.memtable_size;
        stats.sst_file_count = storage_stats.frond_count;
    }
    
    // 文件大小
    stats.storage_size_bytes = GetDatabaseSize(config_.db_path);
    
    return stats;
}

const DatabaseConfig& Database::GetConfig() const {
    return config_;
}

// ============================================================================
// 高级操作
// ============================================================================

Status Database::Execute(BackgroundTask task) {
    if (!IsReady()) {
        return Status::IOError("Database not ready");
    }
    
    active_operations_.fetch_add(1);
    try {
        task();
    } catch (const std::exception& e) {
        active_operations_.fetch_sub(1);
        return Status::IOError(std::string("Task failed: ") + e.what());
    }
    active_operations_.fetch_sub(1);
    return Status::OK();
}

void Database::Wait() {
    std::unique_lock<std::mutex> lock(close_mutex_);
    close_cv_.wait(lock, [this] {
        return active_operations_.load() == 0;
    });
}

// ============================================================================
// 析构函数
// ============================================================================

Database::~Database() {
    if (!IsClosed()) {
        // 自动关闭
        Close();
    }
}

// ============================================================================
// 内部实现方法
// ============================================================================

Status Database::OpenImpl() {
    // 1. 设置状态为 Opening
    if (!TrySetState(DatabaseState::kCreated, DatabaseState::kOpening)) {
        return Status::IOError("Database already opened or in error state");
    }
    
    // 2. 确保目录存在
    Status status = EnsureDirectoriesExist();
    if (!status.ok()) {
        SetState(DatabaseState::kError);
        error_message_ = status.ToString();
        return status;
    }
    
    // 3. 恢复数据库（如需要）
    if (config_.enable_recovery) {
        status = Recover();
        if (!status.ok()) {
            SetState(DatabaseState::kError);
            error_message_ = status.ToString();
            return status;
        }
    }
    
    // 4. 启动后台线程
    status = StartBackgroundThreads();
    if (!status.ok()) {
        SetState(DatabaseState::kError);
        error_message_ = status.ToString();
        return status;
    }
    
    // 5. 设置为 Ready
    SetState(DatabaseState::kReady);
    
    return Status::OK();
}

Status Database::Close() {
    // 注意：不再获取锁，因为 CloseImpl 内部的状态检查已经是原子操作
    return CloseImpl();
}

Status Database::CloseImpl() {
    // 1. 检查当前状态
    DatabaseState current_state = state_.load();
    
    if (current_state == DatabaseState::kClosed) {
        return Status::OK();
    }
    
    if (current_state == DatabaseState::kClosing) {
        return Status::OK();
    }
    
    if (current_state != DatabaseState::kReady) {
        return Status::IOError("Cannot close database in state: " + GetStateString());
    }
    
    // 2. 设置为 Closing
    if (!TrySetState(DatabaseState::kReady, DatabaseState::kClosing)) {
        return Status::IOError("Failed to set closing state");
    }
    
    // 3. 停止后台线程
    StopBackgroundThreads();
    
    // 4. 刷新 MemTable
    if (storage_) {
        storage_->ForceFlush();
    }
    
    // 5. 清理存储
    storage_.reset();
    
    // 6. 设置为 Closed
    SetState(DatabaseState::kClosed);
    
    return Status::OK();
}

Status Database::Recover() {
    // 检查是否是首次打开
    bool first_open = IsFirstOpen();
    
    if (first_open) {
        // 首次打开，创建新数据库
        CedarOptions opts = config_.ToCedarOptions();
        
        CedarGraphStorage* raw_storage = nullptr;
        Status status = CedarGraphStorage::Open(opts, config_.db_path, &raw_storage);
        if (!status.ok()) {
            return Status::IOError("Failed to create database: " + status.ToString());
        }
        storage_.reset(raw_storage);
        
        return Status::OK();
    }
    
    // 非首次打开，恢复现有数据
    CedarOptions opts = config_.ToCedarOptions();
    opts.create_if_missing = false;  // 不创建新的
    
    CedarGraphStorage* raw_storage = nullptr;
    Status status = CedarGraphStorage::Open(opts, config_.db_path, &raw_storage);
    if (!status.ok()) {
        return Status::IOError("Failed to open database: " + status.ToString());
    }
    storage_.reset(raw_storage);
    
    // 如果启用了快速恢复，跳过完整恢复
    if (config_.enable_fast_recovery) {
        // 快速恢复模式：
        // 1. 只加载最新的 SSTD 文件
        // 2. 跳过 WAL 重放
        // 3. 跳过完整的索引重建
        // 这种模式适用于崩溃后快速启动，但可能丢失最近的一些写入
        
        // 记录快速恢复日志
        std::cerr << "Fast recovery mode enabled - skipping full WAL replay" << std::endl;
    }
    
    return Status::OK();
}

Status Database::StartBackgroundThreads() {
    stop_background_.store(false);
    
    // 启动后台线程
    for (uint32_t i = 0; i < config_.background_threads; ++i) {
        background_threads_.emplace_back([this, i] {
            // 后台线程主循环
            while (!stop_background_.load()) {
                // 休眠一段时间
                std::this_thread::sleep_for(std::chrono::seconds(1));
                
                // 检查是否停止
                if (stop_background_.load()) {
                    break;
                }
                
                // 执行后台任务
                if (!IsReady()) {
                    continue;
                }
                
                // 自动刷新检查
                if (config_.enable_auto_flush && storage_) {
                    auto stats = storage_->GetStats();
                    if (stats.memtable_size > config_.memtable_threshold * 0.8) {
                        // 内存表接近阈值，触发刷新
                        storage_->ForceFlush();
                    }
                }
                
                // 自动压缩检查
                if (config_.enable_auto_compaction && storage_) {
                    // 简单的压缩触发条件检查
                    // 实际实现中应该基于更复杂的条件
                    auto stats = storage_->GetStats();
                    if (stats.frond_count > 10) {
                        // 文件数量较多时触发压缩
                        storage_->Compact();
                    }
                }
            }
        });
    }
    
    return Status::OK();
}

Status Database::StopBackgroundThreads() {
    stop_background_.store(true);
    
    for (auto& thread : background_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    
    background_threads_.clear();
    return Status::OK();
}

Status Database::SyncAndSaveCheckpoint() {
    // 1. 刷新所有内存数据
    if (storage_) {
        Status flush_status = storage_->ForceFlush();
        if (!flush_status.ok()) {
            return Status::IOError("Failed to flush: " + flush_status.ToString());
        }
    }
    
    // 2. 触发最后的压缩
    if (storage_) {
        Status compact_status = storage_->Compact();
        if (!compact_status.ok()) {
            return Status::IOError("Failed to compact: " + compact_status.ToString());
        }
    }
    
    // 3. 保存检查点到临时目录，然后原子重命名
    std::string checkpoint_dir = config_.db_path + "/.checkpoint_tmp";
    std::string checkpoint_final = config_.db_path + "/checkpoint";
    
    // 删除旧的临时检查点
    if (fs::exists(checkpoint_dir)) {
        try {
            fs::remove_all(checkpoint_dir);
        } catch (...) {}
    }
    
    // 创建新检查点
    Status status = CreateCheckpoint(checkpoint_dir);
    if (!status.ok()) {
        return Status::IOError("Failed to create checkpoint: " + status.ToString());
    }
    
    // 删除旧的正式检查点
    if (fs::exists(checkpoint_final)) {
        try {
            fs::remove_all(checkpoint_final);
        } catch (...) {}
    }
    
    // 原子重命名
    try {
        fs::rename(checkpoint_dir, checkpoint_final);
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to finalize checkpoint: ") + e.what());
    }
    
    return Status::OK();
}

Status Database::VerifyDataIntegrity() {
    if (!storage_) {
        return Status::IOError("Storage not initialized");
    }
    
    // 1. 验证存储统计数据
    auto stats = storage_->GetStats();
    
    // 2. 检查文件是否存在
    if (!fs::exists(config_.db_path)) {
        return Status::IOError("Database directory does not exist");
    }
    
    // 3. 检查 SST 文件是否完整
    // 遍历 levels_ 中的所有文件，检查文件是否存在
    // 这里简化实现，只检查目录存在
    
    // 4. 验证内存表状态
    if (stats.memtable_size > config_.memtable_threshold * 2) {
        return Status::IOError("Memtable size exceeds threshold - possible data loss");
    }
    
    return Status::OK();
}

Status Database::EnsureDirectoriesExist() {
    // 创建主目录
    if (!fs::exists(config_.db_path)) {
        if (!config_.create_if_missing) {
            return Status::IOError("Database path does not exist: " + config_.db_path);
        }
        
        try {
            fs::create_directories(config_.db_path);
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Failed to create database directory: ") + e.what());
        }
    }
    
    // 创建子目录（如需要）
    // - sst/: SST 文件
    // - wal/: WAL 文件
    // - blob/: BLOB 文件
    // - meta/: 元数据
    
    return Status::OK();
}

bool Database::IsFirstOpen() const {
    // 检查数据库目录是否为空
    if (!fs::exists(config_.db_path)) {
        return true;
    }
    
    // 检查是否有数据文件
    for (const auto& entry : fs::directory_iterator(config_.db_path)) {
        // 忽略隐藏文件和临时文件
        if (entry.path().filename().string()[0] != '.') {
            return false;
        }
    }
    
    return true;
}

// ============================================================================
// 检查点相关方法
// ============================================================================

Status Database::CopyDirectoryRecursive(const std::string& src, const std::string& dst) {
    if (!fs::exists(src)) {
        return Status::IOError("Source directory does not exist: " + src);
    }
    
    if (!fs::is_directory(src)) {
        return Status::IOError("Source is not a directory: " + src);
    }
    
    try {
        // 创建目标目录
        fs::create_directories(dst);
        
        // 递归复制所有文件和子目录
        for (const auto& entry : fs::recursive_directory_iterator(src)) {
            const fs::path relative_path = fs::relative(entry.path(), src);
            const fs::path dest_path = dst / relative_path;
            
            if (entry.is_directory()) {
                fs::create_directories(dest_path);
            } else if (entry.is_regular_file()) {
                // 复制文件
                fs::copy_file(entry.path(), dest_path, fs::copy_options::overwrite_existing);
            }
        }
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to copy directory: ") + e.what());
    }
    
    return Status::OK();
}

Status Database::SaveManifestCheckpoint(const std::string& checkpoint_dir, const DatabaseConfig& config) {
    // 保存检查点元数据
    std::string manifest_path = checkpoint_dir + "/MANIFEST-CHECKPOINT";
    
    try {
        std::ofstream manifest(manifest_path, std::ios::out | std::ios::binary);
        if (!manifest.is_open()) {
            return Status::IOError("Failed to create manifest checkpoint file");
        }
        
        // 写入检查点信息
        uint64_t version = 1;
        manifest.write(reinterpret_cast<const char*>(&version), sizeof(version));
        
        // 写入创建时间
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        manifest.write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
        
        // 写入数据库路径
        uint32_t path_len = static_cast<uint32_t>(config.db_path.size());
        manifest.write(reinterpret_cast<const char*>(&path_len), sizeof(path_len));
        manifest.write(config.db_path.data(), path_len);
        
        manifest.close();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to save manifest checkpoint: ") + e.what());
    }
    
    return Status::OK();
}

Status Database::LoadManifestCheckpoint(const std::string& checkpoint_dir, const DatabaseConfig& config) {
    std::string manifest_path = checkpoint_dir + "/MANIFEST-CHECKPOINT";
    
    if (!fs::exists(manifest_path)) {
        return Status::IOError("Manifest checkpoint not found");
    }
    
    try {
        std::ifstream manifest(manifest_path, std::ios::in | std::ios::binary);
        if (!manifest.is_open()) {
            return Status::IOError("Failed to open manifest checkpoint file");
        }
        
        // 读取版本
        uint64_t version;
        manifest.read(reinterpret_cast<char*>(&version), sizeof(version));
        
        // 读取时间戳
        uint64_t timestamp;
        manifest.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
        
        // 读取数据库路径
        uint32_t path_len;
        manifest.read(reinterpret_cast<char*>(&path_len), sizeof(path_len));
        
        std::string db_path(path_len, '\0');
        manifest.read(db_path.data(), path_len);
        
        manifest.close();
        
        // 验证路径（如果提供了config）
        // 这里只验证manifest格式正确即可
        // 注意：这里简化处理，不验证路径匹配
        
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to load manifest checkpoint: ") + e.what());
    }
    
    return Status::OK();
}

}  // namespace cedar
