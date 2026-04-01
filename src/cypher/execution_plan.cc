// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Execution Plan with Storage Layer Integration

#include "cedar/cypher/execution_plan.h"
#include "cedar/graph/cedar_graph.h"
#include "cedar/graph/cedar_graph_db.h"
#include "cedar/storage/cedar_graph_storage.h"
#include <sstream>

namespace cedar {
namespace cypher {

// ============================================================================
// Record Implementation
// ============================================================================

std::optional<Value> Record::Get(const std::string& key) const {
  auto it = values.find(key);
  if (it != values.end()) {
    return it->second;
  }
  return std::nullopt;
}

// ============================================================================
// Helper: Convert Cedar Neighbor to Cypher Relationship
// ============================================================================

static Relationship NeighborToRelationship(const Neighbor& neighbor, 
                                           uint64_t src_id,
                                           uint16_t edge_type) {
  Relationship rel;
  rel.id = neighbor.id;  // Use neighbor id as relationship id
  rel.start_id = src_id;
  rel.end_id = neighbor.id;
  rel.type = std::to_string(edge_type);  // Convert edge_type to string
  
  // Add properties
  if (neighbor.value) {
    rel.properties["weight"] = Value(static_cast<int64_t>(*neighbor.value));
  }
  rel.properties["timestamp"] = Value(neighbor.timestamp);
  
  return rel;
}

// ============================================================================
// Helper: Convert Cedar Neighbor to Cypher Node
// ============================================================================

static Node NeighborToNode(const Neighbor& neighbor) {
  Node node;
  node.id = neighbor.id;
  node.labels.push_back("Node");  // Default label
  
  // Add properties
  if (neighbor.value) {
    node.properties["value"] = Value(static_cast<int64_t>(*neighbor.value));
  }
  node.properties["edge_type"] = Value(static_cast<int64_t>(neighbor.edge_type));
  
  return node;
}

// ============================================================================
// PhysicalOperator base
// ============================================================================

std::string PhysicalOperator::Explain(int indent) const {
  std::ostringstream oss;
  oss << std::string(indent * 2, ' ') << GetName();
  if (!GetDetails().empty()) {
    oss << " [" << GetDetails() << "]";
  }
  oss << "\n";
  for (const auto& child : children_) {
    oss << child->Explain(indent + 1);
  }
  return oss.str();
}

// ============================================================================
// ExecutionContext
// ============================================================================

void ExecutionContext::SetVariable(const std::string& name, const Value& val) {
  variables[name] = val;
}

std::optional<Value> ExecutionContext::GetVariable(const std::string& name) const {
  auto it = variables.find(name);
  if (it != variables.end()) {
    return it->second;
  }
  return std::nullopt;
}

// ============================================================================
// ExecutionPlan
// ============================================================================

ExecutionPlan::ExecutionPlan(std::shared_ptr<PhysicalOperator> root) 
    : root_(root) {}

ResultSet ExecutionPlan::Execute(ExecutionContext* ctx) {
  if (!ctx || !ctx->graph) {
    ResultSet result;
    result.SetError("Invalid execution context");
    return result;
  }
  
  if (!root_->Init(ctx)) {
    ResultSet result;
    result.SetError("Failed to initialize execution plan");
    return result;
  }
  
  ResultSet result;
  auto produce_results = std::dynamic_pointer_cast<ProduceResults>(root_);
  
  while (auto record = root_->Next()) {
    if (produce_results) {
      // Root is ProduceResults, it handles result collection
    } else {
      // Collect records directly
      result.records.push_back(*record);
    }
  }
  
  if (produce_results) {
    result = produce_results->GetResultSet();
  }
  
  return result;
}

std::string ExecutionPlan::Explain() const {
  return root_->Explain(0);
}

// ============================================================================
// ProduceResults
// ============================================================================

ProduceResults::ProduceResults(std::vector<std::string> columns)
    : columns_(columns) {
  result_set_.columns = columns;
}

bool ProduceResults::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> ProduceResults::Next() {
  if (children_.empty()) {
    return nullptr;
  }
  
  auto record = children_[0]->Next();
  if (record) {
    result_set_.records.push_back(*record);
  }
  return record;
}

ResultSet ProduceResults::GetResultSet() {
  return result_set_;
}

// ============================================================================
// NodeScan with Storage Integration
// ============================================================================

NodeScan::NodeScan(std::string variable, std::optional<std::string> label)
    : variable_(variable), label_(label), current_index_(0) {}

bool NodeScan::Init(ExecutionContext* ctx) {
  context_ = ctx;
  node_ids_.clear();
  
  // Generic node scan - iterate over a configurable entity range
  // TODO: Make range configurable via query hints or schema metadata
  // For now, use a reasonable default range (1-1000)
  // Applications with specific entity encoding can customize this
  
  uint64_t min_entity_id = 1;
  uint64_t max_entity_id = 1000;
  
  // Check if graph context provides entity enumeration
  if (ctx->graph_db) {
    node_ids_ = ctx->graph_db->GetAllEntities(min_entity_id, max_entity_id, 1);
  } else {
    // Fallback: simple sequential range
    node_ids_.reserve(max_entity_id - min_entity_id + 1);
    for (uint64_t i = min_entity_id; i <= max_entity_id; ++i) {
      node_ids_.push_back(i);
    }
  }
  
  current_index_ = 0;
  return true;
}

std::shared_ptr<Record> NodeScan::Next() {
  if (current_index_ >= node_ids_.size()) {
    return nullptr;
  }
  
  uint64_t node_id = node_ids_[current_index_++];
  
  // Create a node value
  Node node;
  node.id = node_id;
  if (label_) {
    node.labels.push_back(*label_);
  } else {
    node.labels.push_back("Node");
  }
  node.properties["id"] = Value(static_cast<int64_t>(node_id));
  
  // Create record
  auto record = std::make_shared<Record>();
  record->Set(variable_, Value(node));
  
  return record;
}

std::string NodeScan::GetDetails() const {
  std::string details = variable_;
  if (label_) {
    details += ":" + *label_;
  }
  details += " (" + std::to_string(node_ids_.size()) + " nodes)";
  return details;
}

// ============================================================================
// Expand with Storage Integration
// ============================================================================

Expand::Expand(std::string from_variable,
               std::string rel_variable,
               std::string to_variable,
               Direction direction,
               std::optional<std::string> rel_type)
    : from_variable_(from_variable),
      rel_variable_(rel_variable),
      to_variable_(to_variable),
      direction_(direction),
      rel_type_(rel_type),
      neighbor_index_(0) {}

bool Expand::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (children_.empty()) {
    return false;
  }
  
  if (!children_[0]->Init(ctx)) {
    return false;
  }
  
  // Get first record from child
  current_record_ = children_[0]->Next();
  neighbor_index_ = 0;
  
  return true;
}

std::shared_ptr<Record> Expand::Next() {
  while (current_record_) {
    // Get the source node from current record
    auto from_val = current_record_->Get(from_variable_);
    if (!from_val || !from_val->IsNode()) {
      // Move to next input record
      current_record_ = children_[0]->Next();
      neighbor_index_ = 0;
      continue;
    }
    
    uint64_t from_id = from_val->GetNode().id;
    
    // Get neighbors from storage
    if (neighbors_.empty() || neighbor_index_ >= neighbors_.size()) {
      // Fetch neighbors from storage
      neighbors_.clear();
      
      uint16_t edge_type = 0;  // Default edge type
      if (rel_type_) {
        edge_type = static_cast<uint16_t>(std::stoi(*rel_type_));
      }
      
      if (context_->graph) {
        auto neighbor_list = context_->graph->GetOutNeighbors(
            from_id, edge_type, 0, Timestamp::Max());
        for (const auto& n : neighbor_list) {
          neighbors_.emplace_back(n.id, n.timestamp);
        }
      }
      
      neighbor_index_ = 0;
      
      if (neighbors_.empty()) {
        // No neighbors, move to next input record
        current_record_ = children_[0]->Next();
        continue;
      }
    }
    
    // Create result record
    auto record = std::make_shared<Record>(*current_record_);
    
    uint64_t target_id = neighbors_[neighbor_index_].first;
    Timestamp ts = neighbors_[neighbor_index_].second;
    
    // Add relationship
    Relationship rel;
    rel.id = target_id ^ ts.value();  // Simple hash for relationship id
    rel.start_id = from_id;
    rel.end_id = target_id;
    rel.type = rel_type_.value_or("CONNECTED_TO");
    rel.properties["timestamp"] = Value(ts);
    record->Set(rel_variable_, Value(rel));
    
    // Add target node
    Node to_node;
    to_node.id = target_id;
    to_node.labels.push_back("Node");
    to_node.properties["id"] = Value(static_cast<int64_t>(target_id));
    record->Set(to_variable_, Value(to_node));
    
    neighbor_index_++;
    return record;
  }
  
  return nullptr;
}

void Expand::Reset() {
  neighbor_index_ = 0;
  neighbors_.clear();
  current_record_.reset();
}

std::string Expand::GetDetails() const {
  std::string details = "(" + from_variable_ + ")";
  details += (direction_ == Direction::INCOMING) ? "<-" : "-";
  details += "[" + rel_variable_;
  if (rel_type_) {
    details += ":" + *rel_type_;
  }
  details += "]";
  details += (direction_ == Direction::INCOMING) ? "-" : "->";
  details += "(" + to_variable_ + ")";
  return details;
}

// ============================================================================
// Filter Implementation
// ============================================================================

Filter::Filter(std::shared_ptr<Expression> predicate)
    : predicate_(predicate) {}

bool Filter::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Filter::Next() {
  while (true) {
    auto record = children_[0]->Next();
    if (!record) {
      return nullptr;
    }
    
    if (EvaluatePredicate(*record)) {
      return record;
    }
  }
}

bool Filter::EvaluatePredicate(const Record& record) {
  // Simplified predicate evaluation
  // In a full implementation, this would evaluate the expression tree
  return true;
}

std::string Filter::GetDetails() const {
  return "predicate";
}

// ============================================================================
// Project Implementation
// ============================================================================

Project::Project(std::vector<std::pair<std::string, std::shared_ptr<Expression>>> projections)
    : projections_(projections) {}

bool Project::Init(ExecutionContext* ctx) {
  context_ = ctx;
  if (!children_.empty()) {
    return children_[0]->Init(ctx);
  }
  return true;
}

std::shared_ptr<Record> Project::Next() {
  auto record = children_[0]->Next();
  if (!record) {
    return nullptr;
  }
  
  // Create new record with projected values
  auto result = std::make_shared<Record>();
  
  for (const auto& [name, expr] : projections_) {
    // Simplified: just copy the variable value
    auto val = record->Get(name);
    if (val) {
      result->Set(name, *val);
    }
  }
  
  return result;
}

std::string Project::GetDetails() const {
  return std::to_string(projections_.size()) + " projections";
}

// ============================================================================
// ExecutionPlanBuilder
// ============================================================================

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::Build(
    std::shared_ptr<QueryStatement> stmt,
    std::shared_ptr<TemporalClause> temporal_clause) {
  
  if (stmt->clauses.empty()) {
    return nullptr;
  }
  
  // Build from clauses
  std::shared_ptr<PhysicalOperator> root = nullptr;
  
  for (const auto& clause : stmt->clauses) {
    switch (clause->clause_type) {
      case ClauseType::MATCH:
        root = BuildMatchPlan(
            std::static_pointer_cast<MatchClause>(clause), 
            temporal_clause);
        break;
      case ClauseType::RETURN:
        // Add ProduceResults on top
        {
          auto return_clause = std::static_pointer_cast<ReturnClause>(clause);
          std::vector<std::string> columns;
          for (const auto& item : return_clause->items) {
            columns.push_back(item.alias.value_or("column"));
          }
          auto produce = std::make_shared<ProduceResults>(columns);
          if (root) {
            produce->AddChild(root);
          }
          root = produce;
        }
        break;
      case ClauseType::WHERE:
        // Add Filter operator
        {
          auto where_clause = std::static_pointer_cast<WhereClause>(clause);
          if (where_clause->condition && root) {
            auto filter = std::make_shared<Filter>(where_clause->condition);
            filter->AddChild(root);
            root = filter;
          }
        }
        break;
      default:
        break;
    }
  }
  
  return root;
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildMatchPlan(
    std::shared_ptr<MatchClause> match,
    std::shared_ptr<TemporalClause> temporal_clause) {
  
  if (match->patterns.empty()) {
    return nullptr;
  }
  
  // For now, just build scan for first pattern
  return BuildScanForPattern(match->patterns[0], temporal_clause);
}

std::shared_ptr<PhysicalOperator> ExecutionPlanBuilder::BuildScanForPattern(
    PathPattern pattern,
    std::shared_ptr<TemporalClause> temporal_clause) {
  
  if (pattern.elements.empty()) {
    return nullptr;
  }
  
  // Get first node pattern
  if (std::holds_alternative<NodePattern>(pattern.elements[0])) {
    const auto& node = std::get<NodePattern>(pattern.elements[0]);
    
    std::shared_ptr<PhysicalOperator> scan;
    
    if (temporal_clause && temporal_clause->HasTemporalConstraint()) {
      // Use temporal scan
      scan = std::make_shared<TemporalNodeScan>(
          node.variable,
          node.labels.empty() ? std::nullopt : std::optional(node.labels[0]),
          temporal_clause->modifier,
          temporal_clause->start_time,
          temporal_clause->end_time,
          temporal_clause->version_number);
    } else {
      // Regular scan
      scan = std::make_shared<NodeScan>(
          node.variable,
          node.labels.empty() ? std::nullopt : std::optional(node.labels[0]));
    }
    
    // Build expand chain for relationships
    std::shared_ptr<PhysicalOperator> current = scan;
    size_t i = 1;
    
    while (i < pattern.elements.size()) {
      if (std::holds_alternative<RelationshipPattern>(pattern.elements[i])) {
        const auto& rel = std::get<RelationshipPattern>(pattern.elements[i]);
        
        // Get the next node pattern
        if (i + 1 < pattern.elements.size() && 
            std::holds_alternative<NodePattern>(pattern.elements[i + 1])) {
          const auto& next_node = std::get<NodePattern>(pattern.elements[i + 1]);
          
          // Create expand operator
          auto expand = std::make_shared<Expand>(
              node.variable,  // From the first node
              rel.variable,
              next_node.variable,
              rel.direction,
              rel.types.empty() ? std::nullopt : std::optional(rel.types[0]));
          
          expand->AddChild(current);
          current = expand;
          
          i += 2;
        } else {
          break;
        }
      } else {
        break;
      }
    }
    
    return current;
  }
  
  return nullptr;
}

}  // namespace cypher
}  // namespace cedar
