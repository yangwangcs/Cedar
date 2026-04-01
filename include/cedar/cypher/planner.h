// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher Query Planner - Converts AST to Execution Plan

#ifndef FERN_CYPHER_PLANNER_H_
#define FERN_CYPHER_PLANNER_H_

#include <memory>
#include <vector>
#include <string>
#include <map>

#include "cedar/cypher/ast.h"
#include "cedar/cypher/execution_plan.h"

namespace cedar::cypher {

// 查询计划器 - 将 AST 转换为物理执行计划
class QueryPlanner {
 public:
  // 为查询语句生成执行计划
  std::unique_ptr<ExecutionPlan> Plan(const QueryStatement& query);
  
  // 获取上次错误
  std::string GetLastError() const { return last_error_; }

 private:
  std::string last_error_;
  
  // 规划子句
  std::unique_ptr<PhysicalOperator> PlanClause(
      const QueryClause& clause,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划 MATCH
  std::unique_ptr<PhysicalOperator> PlanMatch(
      const MatchClause& match,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划 WHERE
  std::unique_ptr<PhysicalOperator> PlanWhere(
      const WhereClause& where,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划 RETURN
  std::unique_ptr<PhysicalOperator> PlanReturn(
      const ReturnClause& ret,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划 ORDER BY
  std::unique_ptr<PhysicalOperator> PlanOrderBy(
      const OrderByClause& order_by,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划 LIMIT
  std::unique_ptr<PhysicalOperator> PlanLimit(
      const LimitClause& limit,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划 SKIP
  std::unique_ptr<PhysicalOperator> PlanSkip(
      const SkipClause& skip,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划路径模式
  std::unique_ptr<PhysicalOperator> PlanPathPattern(
      const PathPattern& pattern,
      std::unique_ptr<PhysicalOperator> input);
  
  // 规划节点扫描
  std::unique_ptr<PhysicalOperator> PlanNodeScan(
      const NodePattern& node);
  
  // 创建过滤谓词
  FilterOperator::Predicate CreateFilterPredicate(
      const Expression& expr);
  
  // 评估表达式为 Value
  Value EvaluateExpression(
      const Expression& expr,
      const Record& record);
  
  // 表达式转字符串（用于调试）
  std::string ExpressionToString(const Expression& expr);
};

}  // namespace cedar::cypher

#endif  // FERN_CYPHER_PLANNER_H_
