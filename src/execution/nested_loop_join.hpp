#pragma once

#include "execution/executor.hpp"

namespace entropy {

class NestedLoopJoinExecutor : public Executor {
public:
    explicit NestedLoopJoinExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
