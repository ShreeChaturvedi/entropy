#pragma once

/**
 * @file shell_utils.hpp
 * @brief Helpers shared by the interactive SQL shell
 */

#include <string_view>

namespace entropy {

/**
 * @brief True when @p sql contains a statement-terminating semicolon that is
 * not inside a single-quoted string literal.
 *
 * Used by the shell to decide when buffered multi-line input is ready to
 * execute, without treating ';' inside 'a;b' as a terminator.
 */
[[nodiscard]] bool sql_has_complete_statement(std::string_view sql);

} // namespace entropy
