#pragma once

#include "execution/executor.hpp"

namespace entropy {

class AggregationExecutor : public Executor {
public:
    explicit AggregationExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
