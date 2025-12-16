#pragma once

/**
 * @file ast.hpp
 * @brief Abstract Syntax Tree nodes
 */

#include <memory>
#include <string>
#include <vector>

namespace entropy {

enum class ASTNodeType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE_TABLE,
    DROP_TABLE,
    EXPRESSION,
};

class ASTNode {
public:
    explicit ASTNode(ASTNodeType type) : type_(type) {}
    virtual ~ASTNode() = default;

    [[nodiscard]] ASTNodeType type() const noexcept { return type_; }

private:
    ASTNodeType type_;
};

}  // namespace entropy
