#pragma once

/**
 * @file app_shell.hpp
 * @brief The application shell: boot menu, screen routing, and frame capture.
 *
 * The shell owns the top-level view state and routes the boot menu to the
 * screen factories. It also provides a deterministic one-frame capture path for
 * still generation.
 */

#include <string>

namespace entropy::tui {

/// Run the interactive terminal app (blocking). Returns a process exit code.
[[nodiscard]] int RunApp();

/// Render exactly one frame of @p which ("boot" | "dashboard" | "console") to
/// stdout as an ANSI string and return, using fixed demo data so the output is
/// deterministic. @p cols and @p rows set the frame size. Returns 0 on success,
/// 2 if @p which is unknown.
[[nodiscard]] int CaptureFrame(const std::string &which, int cols, int rows);

}  // namespace entropy::tui
