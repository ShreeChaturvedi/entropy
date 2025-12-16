#pragma once

#include "execution/executor.hpp"

namespace entropy {

class HashJoinExecutor : public Executor {
public:
    explicit HashJoinExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
