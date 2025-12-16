/**
 * @file parser.cpp
 * @brief Parser stub
 */

#include "parser/parser.hpp"

namespace entropy {

Status Parser::parse([[maybe_unused]] std::string_view sql,
                      [[maybe_unused]] std::unique_ptr<ASTNode>* ast) {
    // TODO: Integrate SQL parser library
    return Status::NotSupported("SQL parsing not yet implemented");
}

}  // namespace entropy
