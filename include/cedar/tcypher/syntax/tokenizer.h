// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_SYNTAX_TOKENIZER_H_
#define CEDAR_TCYPHER_SYNTAX_TOKENIZER_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {

struct TcypherSourceLocation {
  uint32_t offset = 0;
  uint32_t line = 1;
  uint32_t column = 1;
};

enum class TcypherTokenKind : uint8_t {
  kIdentifier,
  kKeyword,
  kParameter,
  kInteger,
  kString,
  kLParen,
  kRParen,
  kLBracket,
  kRBracket,
  kLBrace,
  kRBrace,
  kColon,
  kComma,
  kDot,
  kSemicolon,
  kPlus,
  kMinus,
  kStar,
  kSlash,
  kEquals,
  kLess,
  kGreater,
  kLessEqual,
  kGreaterEqual,
  kNotEqual,
  kEnd,
};

struct TcypherToken {
  TcypherTokenKind kind;
  std::string text;
  TcypherSourceLocation location;
};

StatusOr<std::vector<TcypherToken>> TokenizeTcypher(const std::string& query);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_SYNTAX_TOKENIZER_H_
