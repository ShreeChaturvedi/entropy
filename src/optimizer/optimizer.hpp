#pragma once

/**
 * @file optimizer.hpp
 * @brief Query optimizer
 */

#include <memory>

#include "entropy/status.hpp"
#include "optimizer/plan_node.hpp"
#include "parser/ast.hpp"

namespace entropy {

class Catalog;

class Optimizer {
public:
    explicit Optimizer(std::shared_ptr<Catalog> catalog);

    [[nodiscard]] Status optimize(const ASTNode* ast,
                                   std::unique_ptr<PlanNode>* plan);

private:
    std::shared_ptr<Catalog> catalog_;
};

}  // namespace entropy
