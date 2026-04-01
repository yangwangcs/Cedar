// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef FERN_FERN_GRAPH_STORAGE_H_
#define FERN_FERN_GRAPH_STORAGE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <unordered_map>

#include "cedar/storage/cedar_options.h"
#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/types/cedar_types.h"
#include "cedar/types/descriptor.h"

#include "cedar/core/env.h"
#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

// Forward declarations
class LsmEngine;
class OCCTransaction;
struct TransactionOptions;

/**
 * @brief CedarGraphStorage - Unified graph storage interface
 * 
 * A persistent key-value store optimized for time-series graph data.
 * Keys are composed of (entity_id, tx_time) pairs.
 *
 * Example usage:
 *   CedarOptions options;
 *   options.create_if_missing = true;
 *   CedarGraphStorage* storage;
 *   Status s = CedarGraphStorage::Open(options, "/path/to/db", &storage);
 *   if (s.ok()) {
 *     Descriptor desc = Descriptor::InlineInt(0, 42);
 *     s = storage->Put(123, 1000000, desc);
 *     
 *     Descriptor result;
 *     s = storage->Get(123, 1000000, &result);
 *     
 *     delete storage;
 *   }
 */
class CedarGraphStorage {
 public:
  // Open a database
  static Status Open(const CedarOptions& options, 
                     const std::string& name,
                     CedarGraphStorage** dbptr);
  
  // Destroy the contents of the specified database
  static Status DestroyDB(const std::string& name, 
                          const CedarOptions& options);

  CedarGraphStorage(const CedarGraphStorage&) = delete;
  CedarGraphStorage& operator=(const CedarGraphStorage&) = delete;

  virtual ~CedarGraphStorage();

  // Store the descriptor for the specified key (entity_id, tx_time)
  // Returns OK on success, non-OK on error.
  Status Put(uint64_t entity_id, uint64_t tx_time, const Descriptor& descriptor, Timestamp txn_version);
  
  // Store with write options
  Status Put(const WriteOptions& options,
             uint64_t entity_id, 
             uint64_t tx_time, 
             const Descriptor& descriptor,
             Timestamp txn_version);

  // Remove the database entry (if any) for the specified key
  Status Delete(uint64_t entity_id, uint64_t tx_time, Timestamp txn_version);
  
  // Delete with write options
  Status Delete(const WriteOptions& options,
                uint64_t entity_id, 
                uint64_t tx_time,
                Timestamp txn_version);

  // If the database contains an entry for the key, store the
  // corresponding descriptor in *descriptor and return OK.
  // If there is no entry for the key, return Status::NotFound()
  std::optional<Descriptor> Get(uint64_t entity_id, uint64_t tx_time);
  
  // Get at specific timestamp with entity type and column
  std::optional<Descriptor> Get(uint64_t entity_id, 
                                EntityType entity_type,
                                uint16_t column_id,
                                Timestamp timestamp);

  // ========== 边数据专属 API ==========
  
  /// 写入出边数据 (src -> dst)
  /// @param src_id 源节点ID
  /// @param dst_id 目标节点ID  
  /// @param edge_type 边类型
  /// @param timestamp 时间戳
  /// @param descriptor 边属性数据
  /// @param txn_version 事务版本
  Status PutEdge(uint64_t src_id, 
                 uint64_t dst_id,
                 uint16_t edge_type,
                 Timestamp timestamp,
                 const Descriptor& descriptor,
                 Timestamp txn_version);
  
  /// 写入出边（带选项）
  Status PutEdge(const WriteOptions& options,
                 uint64_t src_id, 
                 uint64_t dst_id,
                 uint16_t edge_type,
                 Timestamp timestamp,
                 const Descriptor& descriptor,
                 Timestamp txn_version);
  
  /// 查询出边数据
  /// @param src_id 源节点ID
  /// @param dst_id 目标节点ID
  /// @param edge_type 边类型
  /// @param timestamp 时间戳
  /// @return 边属性数据
  std::optional<Descriptor> GetEdge(uint64_t src_id,
                                    uint64_t dst_id,
                                    uint16_t edge_type,
                                    Timestamp timestamp);
  
  /// 扫描节点的所有出边
  /// @param src_id 源节点ID
  /// @param edge_type 边类型（0表示所有类型）
  /// @param start_time 开始时间
  /// @param end_time 结束时间
  /// @return (目标节点ID, 时间戳, 属性) 列表
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor>> ScanEdges(
      uint64_t src_id,
      uint16_t edge_type,
      Timestamp start_time,
      Timestamp end_time);

  // Force memtable flush to disk
  Status ForceFlush();
  
  // Trigger compaction manually
  Status Compact();

  // Get database statistics
  struct Stats {
    size_t memtable_size = 0;
    size_t imm_memtable_size = 0;
    size_t frond_count = 0;
    size_t frond_size = 0;
    int num_levels = 0;
  };
  Stats GetStats() const;

  // Scan all versions for an entity in a time range
  // Returns all (timestamp, descriptor) pairs where start_time <= timestamp <= end_time
  std::vector<std::pair<Timestamp, Descriptor>> Scan(
      uint64_t entity_id, 
      Timestamp start_time, 
      Timestamp end_time);

  // Scan with limit - stops after collecting max_results entries
  std::vector<std::pair<Timestamp, Descriptor>> ScanLimit(
      uint64_t entity_id, 
      Timestamp start_time, 
      Timestamp end_time,
      size_t max_results);
  
  // ========== 批量查询接口 (Batch Query API) ==========
  // Batch query items for efficient multi-entity lookup
  struct BatchQueryItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp timestamp;
    std::optional<Descriptor> result;
    
    BatchQueryItem(uint64_t eid, EntityType type, uint16_t col, Timestamp ts)
        : entity_id(eid), entity_type(type), column_id(col), timestamp(ts) {}
  };
  
  // Batch Get - query multiple entities with single lock acquisition
  // This is significantly more efficient than calling Get() multiple times
  void BatchGet(std::vector<BatchQueryItem>& items);
  
  // Batch scan items
  struct BatchScanItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp start_time;
    Timestamp end_time;
    size_t max_results;
    std::vector<std::pair<Timestamp, Descriptor>> results;
    
    BatchScanItem(uint64_t eid, EntityType type, uint16_t col, 
                  Timestamp start, Timestamp end, size_t limit = SIZE_MAX)
        : entity_id(eid), entity_type(type), column_id(col),
          start_time(start), end_time(end), max_results(limit) {}
  };
  
  // Batch Scan - query multiple entities with single lock acquisition
  void BatchScan(std::vector<BatchScanItem>& items);

  // ========== 批量写入接口 (Batch Write API) - 性能优化 ==========
  // 使用批量事务减少事务创建/销毁开销，性能提升 2-3x
  
  /// 批量写入条目
  struct BatchWriteItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Descriptor descriptor;
    Timestamp timestamp;  // 业务时间戳
    uint64_t target_id;   // 用于 Edge 存储对端节点 ID (dst for EdgeOut, src for EdgeIn)
    
    BatchWriteItem(uint64_t eid, EntityType type, uint16_t col,
                   const Descriptor& desc, Timestamp ts = Timestamp::Static(),
                   uint64_t tgt = 0)
        : entity_id(eid), entity_type(type), column_id(col),
          descriptor(desc), timestamp(ts), target_id(tgt) {}
  };
  
  /// 批量写入 - 将多个写入操作打包在一个事务中提交
  /// 相比单独调用 PutStaticVertex 等，性能提升 2-3x
  /// @param items 写入条目列表
  /// @param batch_size 分批大小，0 表示全部在一个事务中
  /// @return 成功返回 OK，失败返回错误状态
  Status BatchWrite(const std::vector<BatchWriteItem>& items, 
                    size_t batch_size = 1000);
  
  /// 批量写入静态节点属性 - 简化版
  Status BatchPutStaticVertex(const std::vector<std::pair<uint64_t, Descriptor>>& items,
                              uint16_t property_id = 1,
                              size_t batch_size = 1000);
  
  /// 批量写入动态节点属性 - 简化版  
  Status BatchPutDynamicVertex(const std::vector<std::tuple<uint64_t, Timestamp, Descriptor>>& items,
                               uint16_t property_id = 1,
                               size_t batch_size = 1000);

  // Scan with entity type filter
  std::vector<std::pair<Timestamp, Descriptor>> Scan(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp start_time,
      Timestamp end_time);

  // Scan memtable only - very fast, only returns data from memory
  std::vector<std::pair<Timestamp, Descriptor>> ScanMemTableOnly(
      uint64_t entity_id, 
      Timestamp start_time, 
      Timestamp end_time,
      size_t max_results);

  // ========== 静态属性 + 动态属性 API ==========
  
  /// 节点静态属性 - 写入（不随时间变化）
  Status PutStaticVertex(uint64_t vertex_id, 
                         uint16_t property_id,
                         const Descriptor& descriptor);
  
  /// 节点静态属性 - 读取
  std::optional<Descriptor> GetStaticVertex(uint64_t vertex_id,
                                            uint16_t property_id);
  
  /// 节点动态属性 - 写入（带时间戳）
  Status PutDynamicVertex(uint64_t vertex_id,
                          uint16_t property_id,
                          Timestamp timestamp,
                          const Descriptor& descriptor,
                          Timestamp txn_version);
  
  /// 节点动态属性 - 读取（指定时间）
  std::optional<Descriptor> GetDynamicVertex(uint64_t vertex_id,
                                             uint16_t property_id,
                                             Timestamp timestamp);
  
  /// 节点动态属性 - 扫描时间范围
  std::vector<std::pair<Timestamp, Descriptor>> ScanDynamicVertex(
      uint64_t vertex_id,
      uint16_t property_id,
      Timestamp start_time,
      Timestamp end_time);
  
  /// 边静态属性 - 写入
  Status PutStaticEdge(uint64_t src_id,
                       uint64_t dst_id,
                       uint16_t edge_type,
                       uint16_t property_id,
                       const Descriptor& descriptor);
  
  /// 边静态属性 - 读取
  std::optional<Descriptor> GetStaticEdge(uint64_t src_id,
                                          uint64_t dst_id,
                                          uint16_t edge_type,
                                          uint16_t property_id);
  
  /// 边动态属性 - 写入
  Status PutDynamicEdge(uint64_t src_id,
                        uint64_t dst_id,
                        uint16_t edge_type,
                        uint16_t property_id,
                        Timestamp timestamp,
                        const Descriptor& descriptor,
                        Timestamp txn_version);
  
  /// 边动态属性 - 读取（指定时间）
  std::optional<Descriptor> GetDynamicEdge(uint64_t src_id,
                                           uint64_t dst_id,
                                           uint16_t edge_type,
                                           uint16_t property_id,
                                           Timestamp timestamp);
  
  /// 边动态属性 - 扫描时间范围
  std::vector<std::pair<Timestamp, Descriptor>> ScanDynamicEdge(
      uint64_t src_id,
      uint64_t dst_id,
      uint16_t edge_type,
      uint16_t property_id,
      Timestamp start_time,
      Timestamp end_time);

  // ========== 混合查询结构 ==========
  
  /// 节点快照（静态 + 动态属性）
  struct VertexSnapshot {
    uint64_t vertex_id;
    std::unordered_map<uint16_t, Descriptor> static_props;
    std::unordered_map<uint16_t, Descriptor> dynamic_props;
    Timestamp query_time;
  };
  
  /// 获取节点快照（指定时间的静态+动态属性）
  VertexSnapshot GetVertexSnapshot(uint64_t vertex_id,
                                   const std::vector<uint16_t>& static_prop_ids,
                                   const std::vector<uint16_t>& dynamic_prop_ids,
                                   Timestamp timestamp);
  
  /// 批量获取节点快照
  std::vector<VertexSnapshot> BatchGetVertexSnapshots(
      const std::vector<uint64_t>& vertex_ids,
      const std::vector<uint16_t>& static_prop_ids,
      const std::vector<uint16_t>& dynamic_prop_ids,
      Timestamp timestamp);

  // ========== 实体生命周期追踪 API ==========
  
  /// 标记实体创建
  Status MarkEntityCreated(uint64_t entity_id, EntityType type, Timestamp timestamp);
  
  /// 标记实体删除
  Status MarkEntityDeleted(uint64_t entity_id, EntityType type, Timestamp timestamp);
  
  /// 标记实体重建（删除后重新创建）
  Status MarkEntityRecreated(uint64_t entity_id, EntityType type, Timestamp timestamp);
  
  /// 检查实体在某时间点是否存在
  bool EntityExistsAt(uint64_t entity_id, EntityType type, Timestamp timestamp);
  
  /// 获取实体的当前状态
  EntityState GetEntityState(uint64_t entity_id, EntityType type);
  
  /// 获取实体完整生命周期历史
  std::vector<LifecycleEntry> GetEntityLifecycleHistory(
      uint64_t entity_id, EntityType type, 
      Timestamp start_time, Timestamp end_time);
  
  /// 获取实体存活时间段列表
  std::vector<AlivePeriod> GetEntityAlivePeriods(uint64_t entity_id, EntityType type);
  
  /// 获取某时间点所有存活的实体
  std::vector<uint64_t> GetActiveEntities(
      EntityType type, Timestamp timestamp);

  // Internal accessor for LsmEngine (used by GraphSemanticLayer)
  LsmEngine* GetLsmEngine() const;

  // ========== 自动 Blob 存储 API (Auto Blob Storage API) ==========
  // 透明处理大对象存储，自动决策内联或Blob存储
  
  /**
   * @brief 存储字符串（自动选择内联或Blob）
   * 
   * 根据字符串长度自动选择存储方式：
   * - ≤4字节：内联存储（编码到Descriptor）
   * - >4字节：Blob外部存储
   * 
   * 使用示例:
   *   storage->PutString(123, 1, "Hello World!");
   *   storage->PutString(123, 2, "Short");  // 自动内联
   *
   * @param entity_id 实体ID
   * @param col_id 列ID
   * @param value 字符串值
   * @param txn_version 事务版本（默认为0）
   * @return 成功返回 OK，失败返回错误状态
   */
  Status PutString(uint64_t entity_id, uint16_t col_id, 
                   const std::string& value,
                   Timestamp txn_version = Timestamp(0));
  
  /**
   * @brief 读取字符串（自动识别存储方式）
   * 
   * 自动识别数据是内联存储还是Blob存储，并正确读取。
   * 
   * 使用示例:
   *   auto result = storage->GetString(123, 1);
   *   if (result) {
   *     std::cout << "Value: " << *result << std::endl;
   *   }
   *
   * @param entity_id 实体ID
   * @param col_id 列ID
   * @return 字符串值，不存在返回 std::nullopt
   */
  std::optional<std::string> GetString(uint64_t entity_id, uint16_t col_id);
  
  /**
   * @brief 存储二进制数据（自动选择内联或Blob）
   * 
   * 与PutString类似，但处理二进制数据。
   * 
   * @param entity_id 实体ID
   * @param col_id 列ID
   * @param data 数据指针
   * @param size 数据大小
   * @param txn_version 事务版本（默认为0）
   * @return 成功返回 OK，失败返回错误状态
   */
  Status PutBinary(uint64_t entity_id, uint16_t col_id,
                   const void* data, size_t size,
                   Timestamp txn_version = Timestamp(0));
  
  /**
   * @brief 读取二进制数据
   * 
   * @param entity_id 实体ID
   * @param col_id 列ID
   * @return 二进制数据，不存在返回空vector
   */
  std::vector<uint8_t> GetBinary(uint64_t entity_id, uint16_t col_id);

  // ========== 事务 API (Transaction API) ==========
  
  /// 开启事务 (beginTX - 简化的中文别名)
  /// 
  /// 创建一个新的事务上下文，支持乐观并发控制 (OCC)。
  /// 事务开启后会获得一个读时间戳，用于保证快照隔离。
  ///
  /// 使用示例:
  ///   OCCTransaction* txn = storage->BeginTransaction();
  ///   txn->Put(vertex_id, EntityType::Vertex, column_id, descriptor);
  ///   Status s = txn->Commit();  // 成功后 delete txn
  ///   // 或 txn->Abort(); delete txn;
  ///
  /// @param options 事务选项（隔离级别、超时等），nullptr 表示使用默认选项
  /// @return 事务对象指针，失败返回 nullptr。调用者负责 delete。
  OCCTransaction* BeginTransaction(const TransactionOptions* options = nullptr);
  
  /// 简化的 beginTX 别名，更简洁的调用方式
  /// @see BeginTransaction
  OCCTransaction* beginTX() { return BeginTransaction(nullptr); }
  
  // ========== 并行批量处理 API (Parallel Batch Processing API) ==========
  // 使用多线程并行处理大批量数据，适用于大规模图数据导入
  
  /// 并行批量写入配置
  struct ParallelBatchOptions {
    size_t num_threads = 0;        // 线程数，0 表示使用所有 CPU 核心
    size_t chunk_size = 1000;      // 每个线程处理的数据块大小
    bool parallel_vertices = true; // 是否并行处理顶点
    bool parallel_edges = false;   // 是否并行处理边（注意边可能有更多冲突）
  };
  
  /// 并行批量写入 - 使用多线程加速大批量导入
  /// 
  /// 相比 BatchWrite，此方法使用多线程并行处理不同的数据分区，
  /// 无冲突的数据块并行执行，最大化 CPU 利用率。
  /// 
  /// 适用于：大批量数据导入、初始化加载、离线处理
  /// 
  /// @param items 写入条目列表
  /// @param options 并行处理选项
  /// @return 成功返回 OK，失败返回错误状态
  Status ParallelBatchWrite(const std::vector<BatchWriteItem>& items,
                            const ParallelBatchOptions& options);
  
  /// 并行批量写入 - 使用默认选项的简化版本
  Status ParallelBatchWrite(const std::vector<BatchWriteItem>& items);

 private:
  CedarGraphStorage(const std::string& db_path, 
                   const CedarOptions& options,
                   cedar::Env* env);
  
  Status Open();

  struct Rep;
  std::unique_ptr<Rep> rep_;
};

}  // namespace cedar

#endif  // FERN_FERN_GRAPH_STORAGE_H_
