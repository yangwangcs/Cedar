#include "cedar/cypher/lexer.h"

#include <cctype>
#include <limits>

namespace cedar::cypher {
namespace {

bool IsIdentStart(unsigned char c) { return std::isalpha(c) || c == '_'; }
bool IsIdentPart(unsigned char c) { return std::isalnum(c) || c == '_'; }

Status LexError(const char* message, uint32_t offset) {
  return Status::ParseError("cypher lexer", message);
}

}  // namespace

StatusOr<std::vector<Token>> Lex(const std::string& source,
                                 LexerOptions options) {
  if (source.size() > options.max_source_bytes) {
    return LexError("source exceeds limit", 0);
  }
  std::vector<Token> tokens;
  tokens.reserve(std::min<size_t>(source.size() / 2 + 1, options.max_tokens));
  uint32_t nesting = 0;
  size_t cursor = 0;
  while (cursor < source.size()) {
    const unsigned char c = static_cast<unsigned char>(source[cursor]);
    if (std::isspace(c)) {
      ++cursor;
      continue;
    }
    const size_t start = cursor;
    if (IsIdentStart(c)) {
      ++cursor;
      while (cursor < source.size() &&
             IsIdentPart(static_cast<unsigned char>(source[cursor]))) {
        ++cursor;
      }
      tokens.push_back({TokenKind::kIdentifier, source.substr(start, cursor - start),
                        {static_cast<uint32_t>(start), static_cast<uint32_t>(cursor - start)}});
    } else if (std::isdigit(c)) {
      ++cursor;
      while (cursor < source.size() &&
             std::isdigit(static_cast<unsigned char>(source[cursor]))) ++cursor;
      tokens.push_back({TokenKind::kInteger, source.substr(start, cursor - start),
                        {static_cast<uint32_t>(start), static_cast<uint32_t>(cursor - start)}});
    } else if (c == '$') {
      ++cursor;
      if (cursor == source.size() ||
          !IsIdentStart(static_cast<unsigned char>(source[cursor]))) {
        return LexError("invalid parameter", static_cast<uint32_t>(start));
      }
      ++cursor;
      while (cursor < source.size() &&
             IsIdentPart(static_cast<unsigned char>(source[cursor]))) ++cursor;
      tokens.push_back({TokenKind::kParameter, source.substr(start + 1, cursor - start - 1),
                        {static_cast<uint32_t>(start), static_cast<uint32_t>(cursor - start)}});
    } else if (c == '\'' || c == '"') {
      const char quote = static_cast<char>(c);
      ++cursor;
      std::string value;
      while (cursor < source.size() && source[cursor] != quote) {
        if (source[cursor] == '\\' && cursor + 1 < source.size()) ++cursor;
        value.push_back(source[cursor++]);
      }
      if (cursor == source.size()) return LexError("unterminated string", static_cast<uint32_t>(start));
      ++cursor;
      tokens.push_back({TokenKind::kString, std::move(value),
                        {static_cast<uint32_t>(start), static_cast<uint32_t>(cursor - start)}});
    } else {
      const char symbol = static_cast<char>(c);
      ++cursor;
      if ((symbol == '<' || symbol == '>') && cursor < source.size() &&
          source[cursor] == '=') {
        ++cursor;
        tokens.push_back({TokenKind::kSymbol,
                          std::string{symbol, '='},
                          {static_cast<uint32_t>(start), 2}});
      } else if (symbol == '.' && cursor < source.size() && source[cursor] == '.') {
        ++cursor;
        tokens.push_back({TokenKind::kSymbol, "..", {static_cast<uint32_t>(start), 2}});
      } else {
        tokens.push_back({TokenKind::kSymbol, std::string(1, symbol),
                          {static_cast<uint32_t>(start), 1}});
      }
      if (symbol == '(' || symbol == '[' || symbol == '{') {
        if (++nesting > options.max_nesting) return LexError("nesting exceeds limit", static_cast<uint32_t>(start));
      } else if (symbol == ')' || symbol == ']' || symbol == '}') {
        if (nesting == 0) return LexError("unbalanced closing delimiter", static_cast<uint32_t>(start));
        --nesting;
      }
    }
    if (tokens.size() >= options.max_tokens) return LexError("token count exceeds limit", static_cast<uint32_t>(start));
  }
  if (nesting != 0) return LexError("unclosed delimiter", static_cast<uint32_t>(source.size()));
  tokens.push_back({TokenKind::kEnd, {}, {static_cast<uint32_t>(source.size()), 0}});
  return tokens;
}

}  // namespace cedar::cypher
