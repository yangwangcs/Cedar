// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Cypher AST (Abstract Syntax Tree) Definitions

#ifndef FERN_CYPHER_AST_H_
#define FERN_CYPHER_AST_H_

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <optional>

#include "cedar/cypher/value.h"

namespace cedar::cypher {

// 前向声明
struct AstNode;
struct Expression;
struct QueryClause;

// 关系方向（在ast.h中定义以避免循环依赖）
enum class Direction {
  OUTGOING,
  INCOMING,
  BOTH
};

// AST 节点基类
struct AstNode {
  virtual ~AstNode() = default;
};

// 表达式类型
enum class ExprType {
  LITERAL,
  VARIABLE,
  PROPERTY,
  COMPARISON,
  AND,
  OR,
  NOT,
  ARITHMETIC,
  FUNCTION_CALL,
  LIST_LITERAL,
  MAP_LITERAL,
  PARAMETER  // $param
};

// 表达式基类
struct Expression : AstNode {
  ExprType expr_type;
  explicit Expression(ExprType type) : expr_type(type) {}
};

// 字面量表达式
struct LiteralExpr : Expression {
  Value value;
  explicit LiteralExpr(Value v) : Expression(ExprType::LITERAL), value(std::move(v)) {}
};

// 变量表达式
struct VariableExpr : Expression {
  std::string name;
  explicit VariableExpr(std::string n) : Expression(ExprType::VARIABLE), name(std::move(n)) {}
};

// 属性访问表达式 (n.name)
struct PropertyExpr : Expression {
  std::string variable;
  std::string property;
  PropertyExpr(std::string var, std::string prop)
      : Expression(ExprType::PROPERTY), variable(std::move(var)), property(std::move(prop)) {}
};

// 比较表达式
struct ComparisonExpr : Expression {
  enum Op { EQ, NE, LT, GT, LE, GE };
  Op op;
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  ComparisonExpr(Op oper, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExprType::COMPARISON), op(oper), left(std::move(l)), right(std::move(r)) {}
};

// 逻辑表达式
struct AndExpr : Expression {
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  AndExpr(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExprType::AND), left(std::move(l)), right(std::move(r)) {}
};

struct OrExpr : Expression {
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  OrExpr(std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExprType::OR), left(std::move(l)), right(std::move(r)) {}
};

struct NotExpr : Expression {
  std::shared_ptr<Expression> operand;
  explicit NotExpr(std::shared_ptr<Expression> op)
      : Expression(ExprType::NOT), operand(std::move(op)) {}
};

// 算术表达式
struct ArithmeticExpr : Expression {
  enum Op { ADD, SUB, MUL, DIV, MOD };
  Op op;
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  ArithmeticExpr(Op oper, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
      : Expression(ExprType::ARITHMETIC), op(oper), left(std::move(l)), right(std::move(r)) {}
};

// 函数调用表达式
struct FunctionCallExpr : Expression {
  std::string name;
  std::vector<std::shared_ptr<Expression>> arguments;
  bool distinct = false;
  FunctionCallExpr(std::string n, std::vector<std::shared_ptr<Expression>> args, bool dist = false)
      : Expression(ExprType::FUNCTION_CALL), name(std::move(n)), 
        arguments(std::move(args)), distinct(dist) {}
};

// 参数表达式 ($name)
struct ParameterExpr : Expression {
  std::string name;
  explicit ParameterExpr(std::string n) : Expression(ExprType::PARAMETER), name(std::move(n)) {}
};

// 列表字面量
struct ListLiteralExpr : Expression {
  std::vector<std::shared_ptr<Expression>> elements;
  explicit ListLiteralExpr(std::vector<std::shared_ptr<Expression>> elems)
      : Expression(ExprType::LIST_LITERAL), elements(std::move(elems)) {}
};

// 映射字面量
struct MapLiteralExpr : Expression {
  std::map<std::string, std::shared_ptr<Expression>> entries;
  explicit MapLiteralExpr(std::map<std::string, std::shared_ptr<Expression>> ent)
      : Expression(ExprType::MAP_LITERAL), entries(std::move(ent)) {}
};

// 节点模式
struct NodePattern {
  std::string variable;
  std::vector<std::string> labels;
  std::map<std::string, std::shared_ptr<Expression>> properties;
};

// 关系模式
struct RelationshipPattern {
  std::string variable;
  Direction direction = Direction::OUTGOING;
  std::vector<std::string> types;
  std::map<std::string, std::shared_ptr<Expression>> properties;
  std::optional<uint64_t> min_hops;
  std::optional<uint64_t> max_hops;
};

// 路径模式
struct PathPattern {
  std::vector<std::variant<NodePattern, RelationshipPattern>> elements;
};

// 排序项
struct SortItem {
  std::shared_ptr<Expression> expression;
  bool ascending = true;
};

// 返回项
struct ReturnItem {
  std::shared_ptr<Expression> expression;
  std::optional<std::string> alias;
};

// 查询子句类型
enum class ClauseType {
  MATCH,
  WHERE,
  RETURN,
  ORDER_BY,
  LIMIT,
  SKIP,
  CREATE,
  SET,
  DELETE
};

// 查询子句基类
struct QueryClause : AstNode {
  ClauseType clause_type;
  explicit QueryClause(ClauseType type) : clause_type(type) {}
};

// MATCH 子句
struct MatchClause : QueryClause {
  bool optional = false;
  std::vector<PathPattern> patterns;
  MatchClause() : QueryClause(ClauseType::MATCH) {}
};

// WHERE 子句
struct WhereClause : QueryClause {
  std::shared_ptr<Expression> condition;
  WhereClause() : QueryClause(ClauseType::WHERE) {}
};

// RETURN 子句
struct ReturnClause : QueryClause {
  bool distinct = false;
  std::vector<ReturnItem> items;
  bool all = false;  // RETURN *
  ReturnClause() : QueryClause(ClauseType::RETURN) {}
};

// ORDER BY 子句
struct OrderByClause : QueryClause {
  std::vector<SortItem> items;
  OrderByClause() : QueryClause(ClauseType::ORDER_BY) {}
};

// LIMIT 子句
struct LimitClause : QueryClause {
  std::shared_ptr<Expression> expression;
  explicit LimitClause(std::shared_ptr<Expression> expr)
      : QueryClause(ClauseType::LIMIT), expression(std::move(expr)) {}
};

// SKIP 子句
struct SkipClause : QueryClause {
  std::shared_ptr<Expression> expression;
  explicit SkipClause(std::shared_ptr<Expression> expr)
      : QueryClause(ClauseType::SKIP), expression(std::move(expr)) {}
};

// CREATE 子句
struct CreateClause : QueryClause {
  std::vector<PathPattern> patterns;
  CreateClause() : QueryClause(ClauseType::CREATE) {}
};

// SET 子句
struct SetClause : QueryClause {
  struct SetItem {
    std::shared_ptr<Expression> target;  // 属性表达式或变量
    std::shared_ptr<Expression> value;
  };
  std::vector<SetItem> items;
  SetClause() : QueryClause(ClauseType::SET) {}
};

// DELETE 子句
struct DeleteClause : QueryClause {
  bool detach = false;
  std::vector<std::shared_ptr<Expression>> expressions;
  DeleteClause() : QueryClause(ClauseType::DELETE) {}
};

// 时态修饰符类型
enum class TemporalModifierType {
  NONE,
  AS_OF,           // AS OF timestamp
  AT_TIME,         // AT TIME timestamp
  BETWEEN,         // BETWEEN start AND end
  FROM_TO,         // FROM start TO end
  CONTAINED_IN,    // CONTAINED IN period
  DURING,          // DURING period
  OVERLAPS,        // OVERLAPS period
  FIRST,           // FIRST version
  LAST,            // LAST version (default)
  PREV,            // PREV version
  NEXT,            // NEXT version
  ALL_VERSIONS,    // ALL VERSIONS
  VERSION_K        // VERSION k
};

// 时态修饰符
struct TemporalModifier {
  TemporalModifierType type = TemporalModifierType::NONE;
  
  // 时间点（AS_OF, AT_TIME）
  std::shared_ptr<Expression> timestamp;
  
  // 时间范围（BETWEEN）
  std::shared_ptr<Expression> range_start;
  std::shared_ptr<Expression> range_end;
  
  // 版本号（VERSION_K）
  int64_t version_number = 0;
  
  bool IsTemporal() const { return type != TemporalModifierType::NONE; }
};

// 完整查询语句
struct QueryStatement : AstNode {
  std::vector<std::shared_ptr<QueryClause>> clauses;
  TemporalModifier temporal_modifier;  // 全局时态修饰符
};

}  // namespace cedar::cypher

#endif  // FERN_CYPHER_AST_H_
