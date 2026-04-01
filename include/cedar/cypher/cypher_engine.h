// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Query Engine - Main API

#ifndef FERN_CYPHER_ENGINE_H_
#define FERN_CYPHER_ENGINE_H_

#include <string>
#include <memory>
#include <map>

#include "cedar/cypher/value.h"
#include "cedar/cypher/execution_plan.h"
#include "cedar/core/status.h"

namespace cedar {

class CedarGraphStorage;

namespace cypher {

// 查询计划缓存项
struct CachedPlan {
  std::unique_ptr<ExecutionPlan> plan;
  size_t use_count = 0;
};

// Cypher 查询引擎
class CypherEngine {
 public:
  explicit CypherEngine(CedarGraphStorage* storage);
  ~CypherEngine();

  // 禁止拷贝
  CypherEngine(const CypherEngine&) = delete;
  CypherEngine& operator=(const CypherEngine&) = delete;

  // 执行 Cypher 查询
  ResultSet Execute(const std::string& query);
  
  // 执行带参数的查询
  ResultSet Execute(const std::string& query, 
                    const std::map<std::string, Value>& parameters);
  
  // 获取查询执行计划（用于调试和分析）
  std::string Explain(const std::string& query);
  
  // 验证查询语法
  bool IsValid(const std::string& query);
  
  // 获取上次错误信息
  std::string GetLastError() const { return last_error_; }
  
  // 清空查询计划缓存
  void ClearCache();
  
  // 获取缓存统计
  size_t GetCacheSize() const;

 private:
  CedarGraphStorage* storage_;
  std::string last_error_;
  
  // 查询计划缓存
  std::map<std::string, std::unique_ptr<ExecutionPlan>> plan_cache_;
  
  // 解析和生成执行计划
  std::unique_ptr<ExecutionPlan> ParseAndPlan(const std::string& query);
  
  // 从缓存获取计划
  ExecutionPlan* GetCachedPlan(const std::string& query);
  
  // 缓存计划
  void CachePlan(const std::string& query, std::unique_ptr<ExecutionPlan> plan);
};

}  // namespace cypher
}  // namespace cedar

#endif  // FERN_CYPHER_ENGINE_H_
