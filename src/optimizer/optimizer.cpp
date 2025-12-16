/**
 * @file optimizer.cpp
 * @brief Optimizer stub
 */

#include "optimizer/optimizer.hpp"

namespace entropy {

Optimizer::Optimizer(std::shared_ptr<Catalog> catalog)
    : catalog_(std::move(catalog)) {}

Status Optimizer::optimize([[maybe_unused]] const ASTNode* ast,
                            [[maybe_unused]] std::unique_ptr<PlanNode>* plan) {
    return Status::NotSupported("Optimizer not yet implemented");
}

}  // namespace entropy
