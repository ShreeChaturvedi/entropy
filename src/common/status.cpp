/**
 * @file status.cpp
 * @brief Status class implementation
 */

#include "entropy/status.hpp"

namespace entropy {

std::string Status::to_string() const {
    std::string result;

    switch (code_) {
        case StatusCode::kOk:              result = "OK"; break;
        case StatusCode::kError:           result = "Error"; break;
        case StatusCode::kNotFound:        result = "NotFound"; break;
        case StatusCode::kAlreadyExists:   result = "AlreadyExists"; break;
        case StatusCode::kInvalidArgument: result = "InvalidArgument"; break;
        case StatusCode::kIOError:         result = "IOError"; break;
        case StatusCode::kCorruption:      result = "Corruption"; break;
        case StatusCode::kNotSupported:    result = "NotSupported"; break;
        case StatusCode::kOutOfMemory:     result = "OutOfMemory"; break;
        case StatusCode::kBusy:            result = "Busy"; break;
        case StatusCode::kTimeout:         result = "Timeout"; break;
        case StatusCode::kAborted:         result = "Aborted"; break;
        case StatusCode::kInternal:        result = "Internal"; break;
    }

    if (!message_.empty()) {
        result += ": ";
        result += message_;
    }

    return result;
}

}  // namespace entropy
