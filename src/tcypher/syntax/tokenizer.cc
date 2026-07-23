// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/syntax/tokenizer.h"

#include <cctype>
#include <set>

namespace cedar {
namespace {

bool IsIdentifierStart(char character) {
  return std::isalpha(static_cast<unsigned char>(character)) || character == '_';
}

bool IsIdentifierPart(char character) {
  return std::isalnum(static_cast<unsigned char>(character)) || character == '_';
}

std::string Uppercase(std::string text) {
  for (char& character : text) {
    character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
  }
  return text;
}

bool IsKeyword(const std::string& text) {
  static const std::set<std::string> keywords = {
      "ANALYZE", "AND", "AS", "ASC", "AVG", "BEGIN", "BETWEEN", "BY", "CHANGES", "COLLECT",
      "COMMIT", "COUNT", "CREATE", "DELETE", "DESC", "DISTINCT", "EXPLAIN",
      "FOR", "FROM", "IN", "LIMIT", "MATCH", "MAX", "MIN", "OF", "ORDER", "RETURN",
      "ROLLBACK", "SET", "SKIP", "SNAPSHOT", "STRICT", "SUM", "SYSTEM_TIME",
      "STARTS", "TIMESTAMP", "TO", "TRAIL", "VALID", "VALID_TIME", "WHERE", "WITH"};
  return keywords.count(text) != 0;
}

}  // namespace

StatusOr<std::vector<TcypherToken>> TokenizeTcypher(const std::string& query) {
  std::vector<TcypherToken> tokens;
  size_t offset = 0;
  uint32_t line = 1;
  uint32_t column = 1;
  auto location = [&]() { return TcypherSourceLocation{static_cast<uint32_t>(offset), line, column}; };
  auto advance = [&]() {
    if (query[offset++] == '\n') {
      ++line;
      column = 1;
    } else {
      ++column;
    }
  };
  auto add = [&](TcypherTokenKind kind, std::string text, TcypherSourceLocation start) {
    tokens.push_back(TcypherToken{kind, std::move(text), start});
  };

  while (offset < query.size()) {
    if (std::isspace(static_cast<unsigned char>(query[offset]))) {
      advance();
      continue;
    }
    const TcypherSourceLocation start = location();
    if (IsIdentifierStart(query[offset])) {
      const size_t begin = offset;
      while (offset < query.size() && IsIdentifierPart(query[offset])) advance();
      std::string text = query.substr(begin, offset - begin);
      const std::string canonical = Uppercase(text);
      add(IsKeyword(canonical) ? TcypherTokenKind::kKeyword : TcypherTokenKind::kIdentifier,
          IsKeyword(canonical) ? canonical : text, start);
      continue;
    }
    if (query[offset] == '$') {
      advance();
      const size_t begin = offset;
      if (offset == query.size() || !IsIdentifierStart(query[offset])) {
        return Status::ParseError("T-Cypher", "parameter name expected");
      }
      while (offset < query.size() && IsIdentifierPart(query[offset])) advance();
      add(TcypherTokenKind::kParameter, query.substr(begin, offset - begin), start);
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(query[offset]))) {
      const size_t begin = offset;
      while (offset < query.size() && std::isdigit(static_cast<unsigned char>(query[offset]))) advance();
      add(TcypherTokenKind::kInteger, query.substr(begin, offset - begin), start);
      continue;
    }
    if (query[offset] == '\'') {
      advance();
      std::string text;
      bool closed = false;
      while (offset < query.size()) {
        const char character = query[offset];
        advance();
        if (character == '\'') {
          closed = true;
          break;
        }
        if (character == '\\') {
          if (offset == query.size()) return Status::ParseError("T-Cypher", "unterminated escape");
          const char escaped = query[offset];
          advance();
          switch (escaped) {
            case 'n': text.push_back('\n'); break;
            case 'r': text.push_back('\r'); break;
            case 't': text.push_back('\t'); break;
            case '\\': text.push_back('\\'); break;
            case '\'': text.push_back('\''); break;
            default: return Status::ParseError("T-Cypher", "unsupported string escape");
          }
        } else {
          text.push_back(character);
        }
      }
      if (!closed) return Status::ParseError("T-Cypher", "unterminated string");
      add(TcypherTokenKind::kString, std::move(text), start);
      continue;
    }
    const char character = query[offset];
    advance();
    switch (character) {
      case '(': add(TcypherTokenKind::kLParen, "(", start); break;
      case ')': add(TcypherTokenKind::kRParen, ")", start); break;
      case '[': add(TcypherTokenKind::kLBracket, "[", start); break;
      case ']': add(TcypherTokenKind::kRBracket, "]", start); break;
      case '{': add(TcypherTokenKind::kLBrace, "{", start); break;
      case '}': add(TcypherTokenKind::kRBrace, "}", start); break;
      case ':': add(TcypherTokenKind::kColon, ":", start); break;
      case ',': add(TcypherTokenKind::kComma, ",", start); break;
      case '.': add(TcypherTokenKind::kDot, ".", start); break;
      case ';': add(TcypherTokenKind::kSemicolon, ";", start); break;
      case '+': add(TcypherTokenKind::kPlus, "+", start); break;
      case '-': add(TcypherTokenKind::kMinus, "-", start); break;
      case '*': add(TcypherTokenKind::kStar, "*", start); break;
      case '/': add(TcypherTokenKind::kSlash, "/", start); break;
      case '=': add(TcypherTokenKind::kEquals, "=", start); break;
      case '<':
        if (offset < query.size() && query[offset] == '=') {
          advance(); add(TcypherTokenKind::kLessEqual, "<=", start);
        } else if (offset < query.size() && query[offset] == '>') {
          advance(); add(TcypherTokenKind::kNotEqual, "<>", start);
        } else {
          add(TcypherTokenKind::kLess, "<", start);
        }
        break;
      case '>':
        if (offset < query.size() && query[offset] == '=') {
          advance(); add(TcypherTokenKind::kGreaterEqual, ">=", start);
        } else {
          add(TcypherTokenKind::kGreater, ">", start);
        }
        break;
      case '!':
        if (offset < query.size() && query[offset] == '=') {
          advance(); add(TcypherTokenKind::kNotEqual, "!=", start);
        } else {
          return Status::ParseError("T-Cypher", "expected '=' after '!'");
        }
        break;
      default:
        return Status::ParseError("T-Cypher", "unexpected character");
    }
  }
  add(TcypherTokenKind::kEnd, "", location());
  return tokens;
}

}  // namespace cedar
