/**
 * @file binder.cpp
 * @brief Binder stub
 */

#include "parser/binder.hpp"

namespace entropy {

Binder::Binder(std::shared_ptr<Catalog> catalog)
    : catalog_(std::move(catalog)) {}

Status Binder::bind([[maybe_unused]] ASTNode* ast) {
    return Status::Ok();  // Stub
}

}  // namespace entropy
