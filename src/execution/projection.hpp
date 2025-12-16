#pragma once

#include "execution/executor.hpp"

namespace entropy {

class ProjectionExecutor : public Executor {
public:
    explicit ProjectionExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
