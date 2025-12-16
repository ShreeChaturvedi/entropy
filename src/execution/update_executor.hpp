#pragma once

#include "execution/executor.hpp"

namespace entropy {

class UpdateExecutor : public Executor {
public:
    explicit UpdateExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
