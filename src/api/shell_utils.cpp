/**
 * @file shell_utils.cpp
 * @brief Interactive shell helpers (statement termination detection)
 */

#include "api/shell_utils.hpp"

namespace entropy {

bool sql_has_complete_statement(std::string_view sql) {
  bool in_string = false;
  for (size_t i = 0; i < sql.size(); ++i) {
    char c = sql[i];
    if (in_string) {
      if (c == '\'') {
        // SQL escaped quote: ''
        if (i + 1 < sql.size() && sql[i + 1] == '\'') {
          ++i;
          continue;
        }
        in_string = false;
      }
      continue;
    }
    if (c == '\'') {
      in_string = true;
      continue;
    }
    if (c == ';') {
      return true;
    }
  }
  return false;
}

} // namespace entropy
