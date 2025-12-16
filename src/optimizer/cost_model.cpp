/**
 * @file cost_model.cpp
 * @brief Cost model stub
 */

#include "optimizer/cost_model.hpp"

namespace entropy {

double CostModel::estimate_cost([[maybe_unused]] const PlanNode* plan) {
    return 1.0;  // Stub
}

}  // namespace entropy
