#pragma once

/**
 * @file cost_model.hpp
 * @brief Cost estimation for query optimization
 */

#include "optimizer/plan_node.hpp"

namespace entropy {

class CostModel {
public:
    CostModel() = default;

    [[nodiscard]] double estimate_cost(const PlanNode* plan);
};

}  // namespace entropy
