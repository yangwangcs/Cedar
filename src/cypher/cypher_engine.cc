// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/parser.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <chrono>

namespace cedar {
namespace cypher {

CypherEngine::CypherEngine(CedarGraphStorage* storage) : storage_(storage) {}

CypherEngine::~CypherEngine() = default;

ResultSet CypherEngine::Execute(const std::string& query) {
  // Check cache first
  if (auto cached = GetCachedPlan(query)) {
    ExecutionContext ctx;
    // ctx.storage = storage_;  // TODO: Add storage to context
    return cached->Execute(&ctx);
  }
  
  // Parse and create new plan
  auto plan = ParseAndPlan(query);
  if (!plan) {
    ResultSet result;
    result.SetError(last_error_);
    return result;
  }
  
  // Cache the plan
  CachePlan(query, std::move(plan));
  
  // Execute
  ExecutionContext ctx;
  // ctx.storage = storage_;  // TODO: Add storage to context
  return GetCachedPlan(query)->Execute(&ctx);
}

ResultSet CypherEngine::Execute(const std::string& query,
                                 const std::map<std::string, Value>& parameters) {
  // TODO: Handle parameters
  return Execute(query);
}

bool CypherEngine::IsValid(const std::string& query) {
  CypherParser parser(query);
  return parser.IsValid();
}

std::string CypherEngine::Explain(const std::string& query) {
  auto plan = ParseAndPlan(query);
  if (!plan) {
    return "Error: " + last_error_;
  }
  return plan->Explain();
}

void CypherEngine::ClearCache() {
  plan_cache_.clear();
}

size_t CypherEngine::GetCacheSize() const {
  return plan_cache_.size();
}

std::unique_ptr<ExecutionPlan> CypherEngine::ParseAndPlan(const std::string& query) {
  CypherParser parser(query);
  auto stmt = parser.ParseStatement();
  
  if (!stmt) {
    last_error_ = parser.GetError();
    return nullptr;
  }
  
  auto temporal_clause = parser.GetTemporalClause();
  auto root = ExecutionPlanBuilder::Build(stmt, temporal_clause);
  
  if (!root) {
    last_error_ = "Failed to build execution plan";
    return nullptr;
  }
  
  return std::make_unique<ExecutionPlan>(root);
}

ExecutionPlan* CypherEngine::GetCachedPlan(const std::string& query) {
  auto it = plan_cache_.find(query);
  if (it != plan_cache_.end()) {
    return it->second.get();
  }
  return nullptr;
}

void CypherEngine::CachePlan(const std::string& query, 
                             std::unique_ptr<ExecutionPlan> plan) {
  plan_cache_[query] = std::move(plan);
}

}  // namespace cypher
}  // namespace cedar
