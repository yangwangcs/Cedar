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

#ifndef FERN_API_QUERY_H_
#define FERN_API_QUERY_H_

#include <cstdint>
#include <vector>
#include <optional>
#include <functional>
#include <memory>

#include "cedar/api/schema.h"
#include "cedar/api/typed_value.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/graph/cedar_graph.h"

namespace cedar {
namespace api {

// ============================================================================
// 查询条件
// ============================================================================

template<typename T>
struct Condition {
  enum class Op {
    Eq,      // ==
    Ne,      // !=
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=
    In,      // IN
    Between, // BETWEEN
  };
  
  Op op;
  T value;
  std::optional<T> value2;  // 用于 Between
  std::vector<T> values;    // 用于 In
  
  static Condition Eq(T v) { return {Op::Eq, v, std::nullopt, {}}; }
  static Condition Ne(T v) { return {Op::Ne, v, std::nullopt, {}}; }
  static Condition Lt(T v) { return {Op::Lt, v, std::nullopt, {}}; }
  static Condition Le(T v) { return {Op::Le, v, std::nullopt, {}}; }
  static Condition Gt(T v) { return {Op::Gt, v, std::nullopt, {}}; }
  static Condition Ge(T v) { return {Op::Ge, v, std::nullopt, {}}; }
  static Condition Between(T v1, T v2) { 
    return {Op::Between, v1, v2, {}}; 
  }
  static Condition In(std::vector<T> vs) { 
    return {Op::In, T{}, std::nullopt, std::move(vs)}; 
  }
};

// 便捷函数
template<typename T>
Condition<T> EQ(T v) { return Condition<T>::Eq(v); }

template<typename T>
Condition<T> NE(T v) { return Condition<T>::Ne(v); }

template<typename T>
Condition<T> LT(T v) { return Condition<T>::Lt(v); }

template<typename T>
Condition<T> LE(T v) { return Condition<T>::Le(v); }

template<typename T>
Condition<T> GT(T v) { return Condition<T>::Gt(v); }

template<typename T>
Condition<T> GE(T v) { return Condition<T>::Ge(v); }

template<typename T>
Condition<T> BETWEEN(T v1, T v2) { return Condition<T>::Between(v1, v2); }

// ============================================================================
// 查询构建器 - 节点查询
// ============================================================================

template<typename EntitySchema>
class EntityQuery {
 public:
  using Schema = EntitySchema;
  
  explicit EntityQuery(LsmEngine* engine, uint64_t entity_id)
      : engine_(engine), entity_id_(entity_id) {}
  
  // 选择字段
  template<typename Field>
  EntityQuery& select() {
    selected_fields_.push_back(Field::kId);
    return *this;
  }
  
  // 选择多个字段
  template<typename... Fields>
  EntityQuery& select_many() {
    (selected_fields_.push_back(Fields::kId), ...);
    return *this;
  }
  
  // 时间范围
  EntityQuery& between(Timestamp start, Timestamp end) {
    start_time_ = start;
    end_time_ = end;
    return *this;
  }
  
  // 特定时间点
  EntityQuery& at(Timestamp timestamp) {
    at_time_ = timestamp;
    return *this;
  }
  
  // 排序
  EntityQuery& order_by(bool desc = true) {
    order_desc_ = desc;
    return *this;
  }
  
  // 限制数量
  EntityQuery& limit(size_t n) {
    limit_ = n;
    return *this;
  }
  
  // 执行查询 - 返回单个字段的历史值
  template<typename Field>
  std::vector<std::pair<Timestamp, typename Field::ValueType>> execute() {
    std::vector<std::pair<Timestamp, typename Field::ValueType>> result;
    
    auto entries = engine_->GetRange(
        entity_id_,
        EntitySchema::kIsEdge ? EntityType::Edge : EntityType::Vertex,
        Field::kId,
        start_time_,
        end_time_
    );
    
    for (const auto& entry : entries) {
      auto opt = TypedValue<typename Field::ValueType>::FromDescriptor(entry.descriptor);
      if (opt.has_value()) {
        result.push_back({entry.timestamp, opt.value().value()});
      }
      
      if (limit_ > 0 && result.size() >= limit_) {
        break;
      }
    }
    
    return result;
  }
  
  // 执行点查
  template<typename Field>
  std::optional<typename Field::ValueType> execute_single() {
    auto desc = engine_->GetAtTime(
        entity_id_,
        EntitySchema::kIsEdge ? EntityType::Edge : EntityType::Vertex,
        Field::kId,
        at_time_.value_or(Timestamp::Max())
    );
    
    if (desc.has_value()) {
      auto opt = TypedValue<typename Field::ValueType>::FromDescriptor(desc.value());
      if (opt.has_value()) {
        return opt.value().value();
      }
    }
    
    return std::nullopt;
  }
  
 private:
  LsmEngine* engine_;
  uint64_t entity_id_;
  std::vector<uint16_t> selected_fields_;
  Timestamp start_time_ = Timestamp(0);
  Timestamp end_time_ = Timestamp::Max();
  std::optional<Timestamp> at_time_;
  bool order_desc_ = true;
  size_t limit_ = 0;
};

// ============================================================================
// 图遍历查询
// ============================================================================

template<typename EdgeSchema>
class GraphTraversal {
 public:
  using Schema = EdgeSchema;
  
  GraphTraversal(CedarGraph* graph, uint64_t start_vertex)
      : graph_(graph), start_vertex_(start_vertex) {}
  
  // 出边遍历
  GraphTraversal& out() {
    direction_ = Direction::Out;
    return *this;
  }
  
  // 入边遍历
  GraphTraversal& in() {
    direction_ = Direction::In;
    return *this;
  }
  
  // 双向遍历
  GraphTraversal& both() {
    direction_ = Direction::Both;
    return *this;
  }
  
  // 边属性过滤
  template<typename Field>
  GraphTraversal& where(const Condition<typename Field::ValueType>& cond) {
    filters_[Field::kId] = [cond](const Descriptor& desc) {
      auto opt = TypedValue<typename Field::ValueType>::FromDescriptor(desc);
      if (!opt.has_value()) return false;
      
      using ValueType = typename Field::ValueType;
      ValueType val = opt.value().value();
      switch (cond.op) {
        case Condition<ValueType>::Op::Eq: return val == cond.value;
        case Condition<ValueType>::Op::Ne: return val != cond.value;
        case Condition<ValueType>::Op::Lt: return val < cond.value;
        case Condition<ValueType>::Op::Le: return val <= cond.value;
        case Condition<ValueType>::Op::Gt: return val > cond.value;
        case Condition<ValueType>::Op::Ge: return val >= cond.value;
        default: return false;
      }
    };
    return *this;
  }
  
  // 跳数
  GraphTraversal& hop(size_t n) {
    max_hops_ = n;
    return *this;
  }
  
  // 时间范围
  GraphTraversal& between(Timestamp start, Timestamp end) {
    start_time_ = start;
    end_time_ = end;
    return *this;
  }
  
  // 限制数量
  GraphTraversal& limit(size_t n) {
    limit_ = n;
    return *this;
  }
  
  // 收集邻居 ID
  std::vector<uint64_t> collect_ids() {
    std::vector<uint64_t> result;
    
    if (direction_ == Direction::Out || direction_ == Direction::Both) {
      auto neighbors = graph_->GetOutNeighbors(
          start_vertex_, Schema::kEdgeTypeId, start_time_, end_time_);
      
      for (const auto& n : neighbors) {
        if (limit_ > 0 && result.size() >= limit_) break;
        result.push_back(n.id);
      }
    }
    
    return result;
  }
  
  // 收集特定属性
  template<typename Field>
  std::vector<typename Field::ValueType> collect() {
    std::vector<typename Field::ValueType> result;
    
    auto neighbors = graph_->GetOutNeighbors(
        start_vertex_, Schema::kEdgeTypeId, start_time_, end_time_);
    
    for (const auto& n : neighbors) {
      if (limit_ > 0 && result.size() >= limit_) break;
      
      if (n.value.has_value()) {
        result.push_back(static_cast<typename Field::ValueType>(n.value.value()));
      }
    }
    
    return result;
  }
  
  // 获取 BFS 分层结果
  std::vector<std::vector<uint64_t>> bfs() {
    return graph_->Bfs(start_vertex_, Schema::kEdgeTypeId, max_hops_, start_time_, end_time_);
  }
  
 private:
  enum class Direction { Out, In, Both };
  
  CedarGraph* graph_;
  uint64_t start_vertex_;
  Direction direction_ = Direction::Out;
  size_t max_hops_ = 1;
  Timestamp start_time_ = Timestamp(0);
  Timestamp end_time_ = Timestamp::Max();
  size_t limit_ = 0;
  
  std::map<uint16_t, std::function<bool(const Descriptor&)>> filters_;
};

// ============================================================================
// 查询引擎包装
// ============================================================================

class QueryEngine {
 public:
  explicit QueryEngine(LsmEngine* engine) : engine_(engine), graph_(nullptr) {}
  explicit QueryEngine(CedarGraphStorage* storage) 
      : engine_(nullptr), graph_(std::make_unique<CedarGraph>(storage)) {}
  
  // 节点查询
  template<typename EntitySchema>
  EntityQuery<EntitySchema> query(uint64_t entity_id) {
    return EntityQuery<EntitySchema>(engine_, entity_id);
  }
  
  // 图遍历
  template<typename EdgeSchema>
  GraphTraversal<EdgeSchema> traverse(uint64_t start_vertex) {
    if (!graph_) {
      throw std::runtime_error("Graph not initialized");
    }
    return GraphTraversal<EdgeSchema>(graph_.get(), start_vertex);
  }
  
  // 批量获取
  template<typename EntitySchema, typename Field>
  std::vector<std::optional<typename Field::ValueType>> batch_get(
      const std::vector<uint64_t>& entity_ids,
      Timestamp timestamp = Timestamp::Max()) {
    
    std::vector<std::optional<typename Field::ValueType>> result;
    result.reserve(entity_ids.size());
    
    for (uint64_t id : entity_ids) {
      auto desc = engine_->GetAtTime(
          id,
          EntitySchema::kIsEdge ? EntityType::Edge : EntityType::Vertex,
          Field::kId,
          timestamp
      );
      
      if (desc.has_value()) {
        auto opt = TypedValue<typename Field::ValueType>::FromDescriptor(desc.value());
        if (opt.has_value()) {
          result.push_back(opt.value().value());
          continue;
        }
      }
      result.push_back(std::nullopt);
    }
    
    return result;
  }
  
 private:
  LsmEngine* engine_;
  std::unique_ptr<CedarGraph> graph_;
};

} // namespace api
} // namespace cedar

#endif // FERN_API_QUERY_H_
