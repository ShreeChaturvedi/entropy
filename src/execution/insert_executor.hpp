#pragma once

#include "execution/executor.hpp"

namespace entropy {

class InsertExecutor : public Executor {
public:
    explicit InsertExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
