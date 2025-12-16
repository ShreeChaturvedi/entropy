#pragma once

/**
 * @file transaction.hpp
 * @brief Transaction object
 */

#include "common/types.hpp"

namespace entropy {

enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };

/**
 * @brief Represents a database transaction
 */
class Transaction {
public:
    explicit Transaction(txn_id_t txn_id);

    [[nodiscard]] txn_id_t txn_id() const noexcept { return txn_id_; }
    [[nodiscard]] TransactionState state() const noexcept { return state_; }

    void set_state(TransactionState state) noexcept { state_ = state; }

private:
    txn_id_t txn_id_;
    TransactionState state_ = TransactionState::GROWING;
};

}  // namespace entropy
