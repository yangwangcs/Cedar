#ifndef CEDAR_CYPHER_LEXER_H_
#define CEDAR_CYPHER_LEXER_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/cypher/ast.h"

namespace cedar::cypher {

enum class TokenKind : uint8_t {
  kIdentifier,
  kParameter,
  kInteger,
  kString,
  kSymbol,
  kEnd,
};

struct Token {
  TokenKind kind = TokenKind::kEnd;
  std::string text;
  SourceSpan span;
};

struct LexerOptions {
  uint32_t max_source_bytes = 1U * 1024U * 1024U;
  uint32_t max_tokens = 65536;
  uint32_t max_nesting = 256;
};

StatusOr<std::vector<Token>> Lex(const std::string& source,
                                 LexerOptions options = {});

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_LEXER_H_
