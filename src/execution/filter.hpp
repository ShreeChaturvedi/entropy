#pragma once

#include "execution/executor.hpp"

namespace entropy {

class FilterExecutor : public Executor {
public:
    explicit FilterExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
