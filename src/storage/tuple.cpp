/**
 * @file tuple.cpp
 * @brief Tuple implementation
 */

#include "storage/tuple.hpp"

namespace entropy {

Tuple::Tuple(std::vector<char> data, RID rid)
    : data_(std::move(data)), rid_(rid) {}

}  // namespace entropy
