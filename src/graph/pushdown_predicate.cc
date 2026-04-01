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

#include "cedar/graph/pushdown_predicate.h"

namespace cedar {

// 属性过滤条件评估
bool PropertyFilter::Evaluate(const Descriptor& desc) const {
  // 根据描述符类型进行比较
  // 注意：这里简化实现，实际需要根据具体类型处理
  
  if (desc.IsTombstone()) {
    return false;  // Tombstone 不满足任何条件
  }
  
  // 尝试作为内联整数比较
  auto int_val = desc.AsInlineInt();
  auto filter_int = value.AsInlineInt();
  
  if (int_val.has_value() && filter_int.has_value()) {
    int32_t v = int_val.value();
    int32_t fv = filter_int.value();
    
    switch (op) {
      case FilterOp::EQ: return v == fv;
      case FilterOp::NE: return v != fv;
      case FilterOp::LT: return v < fv;
      case FilterOp::LE: return v <= fv;
      case FilterOp::GT: return v > fv;
      case FilterOp::GE: return v >= fv;
      default: return true;  // 不支持的运算符默认通过
    }
  }
  
  // 尝试作为内联字符串比较
  if (desc.GetKind() == EntryKind::InlineShortStr && 
      value.GetKind() == EntryKind::InlineShortStr) {
    std::string str_val = desc.AsInlineShortStr();
    std::string filter_str = value.AsInlineShortStr();
    
    // 空字符串表示不是有效的内联短字符串
    if (!str_val.empty() && !filter_str.empty()) {
      const std::string& v = str_val;
      const std::string& fv = filter_str;
      
      switch (op) {
        case FilterOp::EQ: return v == fv;
        case FilterOp::NE: return v != fv;
        case FilterOp::LT: return v < fv;
        case FilterOp::LE: return v <= fv;
        case FilterOp::GT: return v > fv;
        case FilterOp::GE: return v >= fv;
        case FilterOp::LIKE: return v.find(fv) != std::string::npos;
        default: return true;
      }
    }
  }
  
  // 类型不匹配或其他情况，默认通过
  return true;
}

// 谓词评估
bool PushdownPredicate::Evaluate(const Descriptor& desc) const {
  // 检查所有属性过滤条件
  for (const auto& filter : property_filters) {
    if (!filter.Evaluate(desc)) {
      return false;
    }
  }
  return true;
}

}  // namespace cedar
