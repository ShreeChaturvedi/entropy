#pragma once

/**
 * @file binder.hpp
 * @brief Name resolution and type checking
 */

#include <memory>

#include "entropy/status.hpp"
#include "parser/ast.hpp"

namespace entropy {

class Catalog;

class Binder {
public:
    explicit Binder(std::shared_ptr<Catalog> catalog);

    [[nodiscard]] Status bind(ASTNode* ast);

private:
    std::shared_ptr<Catalog> catalog_;
};

}  // namespace entropy
