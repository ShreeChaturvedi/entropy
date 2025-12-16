#pragma once

#include "execution/executor.hpp"

namespace entropy {

class SeqScanExecutor : public Executor {
public:
    explicit SeqScanExecutor(ExecutorContext* ctx) : Executor(ctx) {}

    void init() override {}
    std::optional<Tuple> next() override { return std::nullopt; }
};

}  // namespace entropy
