/**
 * @file transaction.cpp
 * @brief Transaction implementation
 */

#include "transaction/transaction.hpp"

namespace entropy {

Transaction::Transaction(txn_id_t txn_id) : txn_id_(txn_id) {}

}  // namespace entropy
