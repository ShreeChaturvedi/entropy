/**
 * @file main.cpp
 * @brief entropy-tui entry point.
 *
 * Usage:
 *   entropy-tui                          launch the interactive UI
 *   entropy-tui --capture-frame <name>   render one frame of a screen to ANSI
 *                                        on stdout and exit (name is
 *                                        boot | dashboard | console)
 *   entropy-tui --size <cols>x<rows>     frame size for --capture-frame
 *                                        (default 120x40)
 *   entropy-tui --theme <dark|light>     palette mode (default dark)
 */

#include <iostream>
#include <string>

#include "app_shell.hpp"
#include "theme.hpp"

namespace {

/// Parse "<cols>x<rows>" into cols/rows. Returns false on a malformed value.
bool ParseSize(const std::string &spec, int &cols, int &rows) {
  const auto x = spec.find('x');
  if (x == std::string::npos) {
    return false;
  }
  try {
    cols = std::stoi(spec.substr(0, x));
    rows = std::stoi(spec.substr(x + 1));
  } catch (...) {
    return false;
  }
  return cols > 0 && rows > 0;
}

}  // namespace

int main(int argc, char **argv) {
  std::string capture;
  std::string demo_frames;  // print the named demo's frame count and exit
  int cols = 120;
  int rows = 40;
  double phase = -1.0;  // >= 0 drives the boot galaxy's animated sweep
  int step = -1;        // >= 0 selects a dashboard/console replay demo frame

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--capture-frame" && i + 1 < argc) {
      capture = argv[++i];
    } else if (arg == "--demo-frames" && i + 1 < argc) {
      demo_frames = argv[++i];
    } else if (arg == "--size" && i + 1 < argc) {
      if (!ParseSize(argv[++i], cols, rows)) {
        std::cerr << "entropy-tui: invalid --size (expected <cols>x<rows>)\n";
        return 2;
      }
    } else if (arg == "--phase" && i + 1 < argc) {
      try {
        phase = std::stod(argv[++i]);
      } catch (...) {
        std::cerr << "entropy-tui: invalid --phase (expected a number)\n";
        return 2;
      }
    } else if (arg == "--step" && i + 1 < argc) {
      try {
        step = std::stoi(argv[++i]);
      } catch (...) {
        std::cerr << "entropy-tui: invalid --step (expected an integer)\n";
        return 2;
      }
    } else if (arg == "--theme" && i + 1 < argc) {
      const std::string t = argv[++i];
      if (t == "dark") {
        entropy::tui::theme::set_mode(entropy::tui::theme::Mode::kDark);
      } else if (t == "light") {
        entropy::tui::theme::set_mode(entropy::tui::theme::Mode::kLight);
      } else {
        std::cerr << "entropy-tui: invalid --theme (expected dark|light)\n";
        return 2;
      }
    } else if (arg == "-h" || arg == "--help") {
      std::cout
          << "Usage: entropy-tui [--capture-frame <boot|dashboard|console>]"
             " [--size <cols>x<rows>] [--phase <0..1>] [--step <n>]"
             " [--theme <dark|light>]\n"
             "       entropy-tui --demo-frames <dashboard|console>\n";
      return 0;
    } else {
      std::cerr << "entropy-tui: unknown argument '" << arg << "'\n";
      return 2;
    }
  }

  if (!demo_frames.empty()) {
    std::cout << entropy::tui::DemoFrameCount(demo_frames) << "\n";
    return 0;
  }
  if (!capture.empty()) {
    return entropy::tui::CaptureFrame(capture, cols, rows, phase, step);
  }
  return entropy::tui::RunApp();
}
