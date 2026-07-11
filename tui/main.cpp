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
 */

#include <iostream>
#include <string>

#include "app_shell.hpp"

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
  int cols = 120;
  int rows = 40;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--capture-frame" && i + 1 < argc) {
      capture = argv[++i];
    } else if (arg == "--size" && i + 1 < argc) {
      if (!ParseSize(argv[++i], cols, rows)) {
        std::cerr << "entropy-tui: invalid --size (expected <cols>x<rows>)\n";
        return 2;
      }
    } else if (arg == "-h" || arg == "--help") {
      std::cout
          << "Usage: entropy-tui [--capture-frame <boot|dashboard|console>]"
             " [--size <cols>x<rows>]\n";
      return 0;
    } else {
      std::cerr << "entropy-tui: unknown argument '" << arg << "'\n";
      return 2;
    }
  }

  if (!capture.empty()) {
    return entropy::tui::CaptureFrame(capture, cols, rows);
  }
  return entropy::tui::RunApp();
}
