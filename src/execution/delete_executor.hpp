#pragma once

#include "execution/executor.hpp"

namespace entropy {

class DeleteExecutor : public Executor {
public:
    explicit DeleteExecutor(ExecutorContext* ctx) : Executor(ctx) {}
    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
