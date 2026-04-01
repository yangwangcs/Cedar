// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/types/cedar_types.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/temporal_dialect.h"
#include "cedar/cypher/value.h"

namespace cedar {

class CedarGraph;
class CedarGraphDB;

namespace cypher {

// Forward declarations
class PhysicalOperator;

/**
 * @brief Execution context for query execution
 */
struct ExecutionContext {
  CedarGraph* graph = nullptr;
  CedarGraphDB* graph_db = nullptr;  // Alternative to graph for legacy graph DB-backed queries
  std::unordered_map<std::string, Value> variables;
  std::unordered_map<std::string, std::shared_ptr<Record>> variable_records;
  
  // Temporal context
  std::shared_ptr<TemporalClause> temporal_clause;
  Timestamp query_timestamp = 0;
  std::pair<Timestamp, Timestamp> time_range{0, Timestamp::Max()};
  
  void SetVariable(const std::string& name, const Value& val);
  std::optional<Value> GetVariable(const std::string& name) const;
};

/**
 * @brief Base class for physical operators
 */
class PhysicalOperator {
 public:
  virtual ~PhysicalOperator() = default;
  
  /**
   * @brief Initialize the operator
   * @return true on success
   */
  virtual bool Init(ExecutionContext* ctx) = 0;
  
  /**
   * @brief Get next record from this operator
   * @return Record or nullptr if exhausted
   */
  virtual std::shared_ptr<Record> Next() = 0;
  
  /**
   * @brief Reset operator state
   */
  virtual void Reset() {}
  
  /**
   * @brief Get operator name for EXPLAIN
   */
  virtual std::string GetName() const = 0;
  
  /**
   * @brief Get operator details for EXPLAIN
    */
  virtual std::string GetDetails() const { return ""; }
  
  /**
   * @brief Format for EXPLAIN output
   */
  virtual std::string Explain(int indent = 0) const;
  
  /**
   * @brief Add child operator
   */
  void AddChild(std::shared_ptr<PhysicalOperator> child) {
    children_.push_back(child);
  }
  
 protected:
  ExecutionContext* context_ = nullptr;
  std::vector<std::shared_ptr<PhysicalOperator>> children_;
};

// ============================================================================
// Standard Operators
// ============================================================================

/**
 * @brief Node scan operator
 */
class NodeScan : public PhysicalOperator {
 public:
  NodeScan(std::string variable, std::optional<std::string> label = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "NodeScan"; }
  std::string GetDetails() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  size_t current_index_ = 0;
  std::vector<uint64_t> node_ids_;
};

/**
 * @brief Relationship expand operator
 */
class Expand : public PhysicalOperator {
 public:
  Expand(std::string from_variable,
         std::string rel_variable,
         std::string to_variable,
         Direction direction,
         std::optional<std::string> rel_type = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  void Reset() override;
  std::string GetName() const override { return "Expand"; }
  std::string GetDetails() const override;
  
 private:
  std::string from_variable_;
  std::string rel_variable_;
  std::string to_variable_;
  Direction direction_;
  std::optional<std::string> rel_type_;
  
  std::shared_ptr<Record> current_record_;
  size_t neighbor_index_ = 0;
  std::vector<std::pair<uint64_t, uint64_t>> neighbors_;  // (rel_id, target_id)
};

/**
 * @brief Filter operator
 */
class Filter : public PhysicalOperator {
 public:
  explicit Filter(std::shared_ptr<Expression> predicate);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Filter"; }
  std::string GetDetails() const override;
  
 private:
  std::shared_ptr<Expression> predicate_;
  
  bool EvaluatePredicate(const Record& record);
};

/**
 * @brief Project operator
 */
class Project : public PhysicalOperator {
 public:
  explicit Project(std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Project"; }
  std::string GetDetails() const override;
  
 private:
  std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections_;
};

/**
 * @brief Produce results operator (root)
 */
class ProduceResults : public PhysicalOperator {
 public:
  explicit ProduceResults(std::vector<std::string> columns);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "ProduceResults"; }
  
  ResultSet GetResultSet();
  
 private:
  std::vector<std::string> columns_;
  ResultSet result_set_;
};

/**
 * @brief Sort operator
 */
class Sort : public PhysicalOperator {
 public:
  Sort(std::vector<std::pair<std::string, bool>> sort_items);  // (key, ascending)
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Sort"; }
  
 private:
  std::vector<std::pair<std::string, bool>> sort_items_;
  std::vector<std::shared_ptr<Record>> buffered_records_;
  size_t current_index_ = 0;
  bool sorted_ = false;
  
  void DoSort();
};

/**
 * @brief Limit operator
 */
class Limit : public PhysicalOperator {
 public:
  explicit Limit(size_t limit);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Limit"; }
  std::string GetDetails() const override;
  
 private:
  size_t limit_;
  size_t count_ = 0;
};

/**
 * @brief Skip operator
 */
class Skip : public PhysicalOperator {
 public:
  explicit Skip(size_t skip);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Skip"; }
  std::string GetDetails() const override;
  
 private:
  size_t skip_;
  size_t skipped_ = 0;
  bool initialized_ = false;
};

/**
 * @brief Distinct operator
 */
class Distinct : public PhysicalOperator {
 public:
  explicit Distinct(std::vector<std::string> keys);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Distinct"; }
  
 private:
  std::vector<std::string> keys_;
  std::unordered_map<std::string, bool> seen_;
  
  std::string ComputeKey(const Record& record);
};

/**
 * @brief Aggregation operator
 */
class Aggregate : public PhysicalOperator {
 public:
  enum class AggregationFunc {
    kCount, kSum, kAvg, kMin, kMax, kCollect
  };
  
  struct AggregationItem {
    std::string output_name;
    AggregationFunc func;
    std::shared_ptr<Expression> expression;
    std::optional<std::string> group_by_key;
  };
  
  explicit Aggregate(std::vector<AggregationItem> items);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "Aggregate"; }
  
 private:
  std::vector<AggregationItem> items_;
  std::vector<std::shared_ptr<Record>> buffered_records_;
  size_t current_index_ = 0;
  bool aggregated_ = false;
  
  void DoAggregate();
};

// ============================================================================
// Temporal Operators
// ============================================================================

/**
 * @brief Temporal node scan operator
 * 
 * Scans nodes with temporal filtering based on the query's temporal clause.
 * Uses Cedar's MVCC version chain for efficient historical queries.
 */
class TemporalNodeScan : public PhysicalOperator {
 public:
  TemporalNodeScan(std::string variable,
                   std::optional<std::string> label,
                   TemporalModifierType modifier,
                   std::optional<TimestampExpression> start_time = std::nullopt,
                   std::optional<TimestampExpression> end_time = std::nullopt,
                   std::optional<uint64_t> version_number = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "TemporalNodeScan"; }
  std::string GetDetails() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  TemporalModifierType modifier_;
  std::optional<TimestampExpression> start_time_;
  std::optional<TimestampExpression> end_time_;
  std::optional<uint64_t> version_number_;
  
  // Execution state
  size_t current_index_ = 0;
  std::vector<uint64_t> node_ids_;
  Timestamp query_start_ = 0;
  Timestamp query_end_ = Timestamp::Max();
  
  // Temporal filtering methods
  bool MatchesTemporalConstraint(const Node& node) const;
  bool MatchesAsOf(const Node& node) const;
  bool MatchesBetween(const Node& node) const;
  bool MatchesContainedIn(const Node& node) const;
  bool MatchesVersion(const Node& node) const;
};

/**
 * @brief Temporal expand operator
 * 
 * Expands relationships with temporal constraints.
 * Supports different temporal path semantics (snapshot, continuous, pairwise).
 */
class TemporalExpand : public PhysicalOperator {
 public:
  TemporalExpand(std::string from_variable,
                 std::string rel_variable,
                 std::string to_variable,
                 Direction direction,
                 TemporalModifierType modifier,
                 std::optional<std::string> rel_type = std::nullopt,
                 TemporalPathSemantics path_semantics = TemporalPathSemantics::kSnapshot);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  void Reset() override;
  std::string GetName() const override { return "TemporalExpand"; }
  std::string GetDetails() const override;
  
 private:
  std::string from_variable_;
  std::string rel_variable_;
  std::string to_variable_;
  Direction direction_;
  TemporalModifierType modifier_;
  std::optional<std::string> rel_type_;
  TemporalPathSemantics path_semantics_;
  
  // Execution state
  std::shared_ptr<Record> current_record_;
  size_t neighbor_index_ = 0;
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> neighbors_;  // (rel_id, target_id, source_id)
  
  Timestamp query_start_ = 0;
  Timestamp query_end_ = Timestamp::Max();
  
  // Temporal filtering
  bool MatchesTemporalConstraint(const Relationship& rel) const;
  bool IsPathContinuous(const std::vector<Relationship>& path) const;
};

/**
 * @brief Snapshot scan operator
 * 
 * Optimized operator for AS OF queries - reads snapshot at specific version.
 */
class SnapshotScan : public PhysicalOperator {
 public:
  SnapshotScan(std::string variable,
               std::optional<std::string> label,
               Timestamp snapshot_time);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "SnapshotScan"; }
  std::string GetDetails() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  Timestamp snapshot_time_;
  
  size_t current_index_ = 0;
  std::vector<uint64_t> node_ids_;
};

/**
 * @brief Version scan operator
 * 
 * Scans all versions of nodes/relationships for ALL VERSIONS queries.
 */
class VersionScan : public PhysicalOperator {
 public:
  VersionScan(std::string variable,
              std::optional<std::string> label,
              std::optional<uint64_t> specific_version = std::nullopt);
  
  bool Init(ExecutionContext* ctx) override;
  std::shared_ptr<Record> Next() override;
  std::string GetName() const override { return "VersionScan"; }
  std::string GetDetails() const override;
  
 private:
  std::string variable_;
  std::optional<std::string> label_;
  std::optional<uint64_t> specific_version_;
  
  // Version chain iteration
  size_t current_node_index_ = 0;
  size_t current_version_index_ = 0;
  std::vector<uint64_t> node_ids_;
  std::vector<Node> current_versions_;
};

// ============================================================================
// Execution Plan Builder
// ============================================================================

/**
 * @brief Builds execution plan from AST
 */
class ExecutionPlanBuilder {
 public:
  /**
   * @brief Build physical execution plan from AST
   */
  static std::shared_ptr<PhysicalOperator> Build(
      std::shared_ptr<QueryStatement> query,
      std::shared_ptr<TemporalClause> temporal_clause = nullptr);
  
  /**
   * @brief Build plan with temporal optimization
   */
  static std::shared_ptr<PhysicalOperator> BuildTemporalPlan(
      std::shared_ptr<QueryStatement> query,
      std::shared_ptr<TemporalClause> temporal_clause);
  
 private:
  static std::shared_ptr<PhysicalOperator> BuildMatchPlan(
      std::shared_ptr<MatchClause> match,
      std::shared_ptr<TemporalClause> temporal_clause);
  
  static std::shared_ptr<PhysicalOperator> BuildCreatePlan(
      std::shared_ptr<CreateClause> create);
  
  static std::shared_ptr<PhysicalOperator> BuildScanForPattern(
      PathPattern pattern,
      std::shared_ptr<TemporalClause> temporal_clause);
  
  static std::shared_ptr<PhysicalOperator> CreateTemporalScan(
      std::string variable,
      std::optional<std::string> label,
      std::shared_ptr<TemporalClause> temporal_clause);
  
  static std::shared_ptr<PhysicalOperator> CreateTemporalExpand(
      std::string from_var,
      std::string rel_var,
      std::string to_var,
      Direction dir,
      std::optional<std::string> rel_type,
      std::shared_ptr<TemporalClause> temporal_clause);
};

/**
 * @brief Execution plan for a query
 */
class ExecutionPlan {
 public:
  explicit ExecutionPlan(std::shared_ptr<PhysicalOperator> root);
  
  /**
   * @brief Execute the plan
   */
  ResultSet Execute(ExecutionContext* ctx);
  
  /**
   * @brief Get EXPLAIN output
   */
  std::string Explain() const;
  
  /**
   * @brief Get root operator
   */
  std::shared_ptr<PhysicalOperator> GetRoot() const { return root_; }
  
 private:
  std::shared_ptr<PhysicalOperator> root_;
};

}  // namespace cypher
}  // namespace cedar
