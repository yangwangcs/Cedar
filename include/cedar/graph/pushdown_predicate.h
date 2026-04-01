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

#ifndef FERN_PUSHDOWN_PREDICATE_H_
#define FERN_PUSHDOWN_PREDICATE_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include <functional>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// 属性过滤操作符
enum class FilterOp {
  EQ,    // =
  NE,    // !=
  LT,    // <
  LE,    // <=
  GT,    // >
  GE,    // >=
  IN,    // IN
  LIKE   // LIKE (字符串匹配)
};

// 属性过滤条件
struct PropertyFilter {
  std::string property_name;  // 属性名
  FilterOp op;                // 操作符
  Descriptor value;           // 比较值
  
  PropertyFilter() = default;
  PropertyFilter(const std::string& name, FilterOp oper, const Descriptor& val)
      : property_name(name), op(oper), value(val) {}
  
  // 评估是否满足条件
  bool Evaluate(const Descriptor& desc) const;
};

// 聚合操作类型
enum class AggregateOp {
  COUNT,  // 计数
  SUM,    // 求和
  AVG,    // 平均值
  MIN,    // 最小值
  MAX     // 最大值
};

// 聚合下推
struct AggregatePushdown {
  AggregateOp op;
  std::optional<std::string> property_name;  // 可选，对特定属性聚合
  
  AggregatePushdown(AggregateOp operation, const std::optional<std::string>& prop = std::nullopt)
      : op(operation), property_name(prop) {}
};

// 下推谓词 - 核心结构
// 用于将计算下推到存储层
struct PushdownPredicate {
  // ========== Filter 下推 ==========
  // 属性过滤条件列表 (AND 关系)
  std::vector<PropertyFilter> property_filters;
  
  // 时间范围过滤 (Cedar 特色)
  std::optional<Timestamp> time_start;
  std::optional<Timestamp> time_end;
  
  // ========== Project 下推 ==========
  // 只返回指定列 (空表示返回所有)
  std::vector<uint16_t> projected_columns;
  
  // ========== Aggregate 下推 ==========
  // 简单聚合下推
  std::optional<AggregatePushdown> aggregate;
  
  // ========== Limit 下推 ==========
  // 存储层提前截断
  std::optional<size_t> limit;
  
  // ========== 辅助方法 ==========
  
  // 是否为空谓词
  bool IsEmpty() const {
    return property_filters.empty() && 
           !time_start.has_value() && 
           !time_end.has_value() &&
           projected_columns.empty() &&
           !aggregate.has_value() &&
           !limit.has_value();
  }
  
  // 是否只有时间范围过滤
  bool IsTimeOnly() const {
    return !time_start.has_value() && 
           !time_end.has_value() &&
           property_filters.empty() &&
           projected_columns.empty() &&
           !aggregate.has_value() &&
           !limit.has_value();
  }
  
  // 检查时间戳是否在范围内
  bool CheckTimeRange(Timestamp ts) const {
    if (time_start.has_value() && ts < time_start.value()) {
      return false;
    }
    if (time_end.has_value() && ts > time_end.value()) {
      return false;
    }
    return true;
  }
  
  // 评估描述符是否满足所有过滤条件
  bool Evaluate(const Descriptor& desc) const;
  
  // 检查是否达到 limit
  bool CheckLimit(size_t current_count) const {
    return limit.has_value() && current_count >= limit.value();
  }
};

// Builder 模式创建谓词
class PredicateBuilder {
 public:
  PredicateBuilder& AddPropertyFilter(const std::string& name, FilterOp op, const Descriptor& value) {
    predicate_.property_filters.emplace_back(name, op, value);
    return *this;
  }
  
  PredicateBuilder& SetTimeRange(Timestamp start, Timestamp end) {
    predicate_.time_start = start;
    predicate_.time_end = end;
    return *this;
  }
  
  PredicateBuilder& SetProjectedColumns(const std::vector<uint16_t>& columns) {
    predicate_.projected_columns = columns;
    return *this;
  }
  
  PredicateBuilder& SetAggregate(AggregateOp op, const std::optional<std::string>& prop = std::nullopt) {
    predicate_.aggregate = AggregatePushdown(op, prop);
    return *this;
  }
  
  PredicateBuilder& SetLimit(size_t limit) {
    predicate_.limit = limit;
    return *this;
  }
  
  PushdownPredicate Build() const {
    return predicate_;
  }
  
 private:
  PushdownPredicate predicate_;
};

}  // namespace cedar

#endif  // FERN_PUSHDOWN_PREDICATE_H_
