#ifndef CEDAR_CYPHER_PARSER_H_
#define CEDAR_CYPHER_PARSER_H_

#include <string>

#include "cedar/core/status.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/lexer.h"

namespace cedar::cypher {

StatusOr<Statement> Parse(const std::string& source,
                          LexerOptions options = {});

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_PARSER_H_
