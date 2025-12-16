#pragma once

/**
 * @file parser.hpp
 * @brief SQL Parser wrapper
 */

#include <memory>
#include <string_view>

#include "entropy/status.hpp"
#include "parser/ast.hpp"

namespace entropy {

class Parser {
public:
    Parser() = default;

    [[nodiscard]] Status parse(std::string_view sql,
                                std::unique_ptr<ASTNode>* ast);
};

}  // namespace entropy
