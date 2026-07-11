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
/// deterministic. @p cols and @p rows set the frame size. @p galaxy_phase, when
/// >= 0, drives the boot galaxy's animated sheen sweep at that phase in [0,1]
/// (for assembling a looping animated capture); the default -1 renders the
/// static mark. @p demo_step, when >= 0, renders that frame of the dashboard or
/// console replay demo instead of the static screen. Returns 0 on success, 2 if
/// @p which is unknown.
[[nodiscard]] int CaptureFrame(const std::string &which, int cols, int rows,
                               double galaxy_phase = -1.0, int demo_step = -1);

/// Number of frames in the named replay demo ("dashboard" | "console"), or 0 if
/// @p which has no demo. Printed by `--demo-frames` so a capture script can
/// drive `--step 0 .. N-1` without hardcoding the count.
[[nodiscard]] int DemoFrameCount(const std::string &which);

}  // namespace entropy::tui
