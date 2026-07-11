#pragma once

/**
 * @file boot_screen.hpp
 * @brief The entropy boot/menu screen (the Grok Build layout).
 *
 * Large galaxy mark on the left; the ENTROPY wordmark, version, and a keyboard
 * menu on the right; a one-line stats footer beneath. The menu selection is the
 * only amber element on the screen.
 */

#include <string>
#include <vector>

#include <ftxui/dom/elements.hpp>

#include "data_loader.hpp"

namespace entropy::tui {

/// One keyboard-menu entry: a label and the key that triggers it.
struct BootMenuItem {
  std::string label;
  std::string key;
};

/// The fixed boot menu, in display order.
[[nodiscard]] const std::vector<BootMenuItem> &BootMenu();

/// Render the boot screen with @p selected_index highlighted (amber). @p data
/// drives the stats footer; @p version fills the wordmark line. @p galaxy_phase
/// is forwarded to the galaxy mark: the default -1 renders the static mark
/// (baked luster only), while a value in [0,1] rides an animated sheen sweep
/// across the disc for a moving capture.
[[nodiscard]] ftxui::Element BootScreen(const DataSet &data, int selected_index,
                                        const std::string &version,
                                        double galaxy_phase = -1.0);

}  // namespace entropy::tui
