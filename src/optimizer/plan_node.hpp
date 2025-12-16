#pragma once

/**
 * @file plan_node.hpp
 * @brief Query plan nodes
 */

#include <memory>
#include <vector>

namespace entropy {

enum class PlanNodeType {
    SEQ_SCAN,
    INDEX_SCAN,
    INSERT,
    UPDATE,
    DELETE,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    AGGREGATION,
    PROJECTION,
    FILTER,
    SORT,
    LIMIT,
};

class PlanNode {
public:
    explicit PlanNode(PlanNodeType type) : type_(type) {}
    virtual ~PlanNode() = default;

    [[nodiscard]] PlanNodeType type() const noexcept { return type_; }
    [[nodiscard]] const std::vector<std::unique_ptr<PlanNode>>& children() const {
        return children_;
    }

    void add_child(std::unique_ptr<PlanNode> child) {
        children_.push_back(std::move(child));
    }

private:
    PlanNodeType type_;
    std::vector<std::unique_ptr<PlanNode>> children_;
};

}  // namespace entropy
