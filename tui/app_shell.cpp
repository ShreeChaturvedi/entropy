#include "app_shell.hpp"

#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <string>
#include <thread>

#include <ftxui/component/component.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/node.hpp>
#include <ftxui/screen/screen.hpp>
#include <ftxui/screen/terminal.hpp>

#include <entropy/entropy.hpp>

#include "boot_screen.hpp"
#include "dashboard_screen.hpp"
#include "data_loader.hpp"
#include "query_console_screen.hpp"

namespace entropy::tui {

using namespace ftxui;

namespace {

/// Top-level views, matching the Container::Tab child order.
enum View : int { kBoot = 0, kDashboard = 1, kConsole = 2 };

/// Build the fixed-demo-data element for a named screen. Returns nullptr for an
/// unknown name. @p galaxy_phase is forwarded to the boot galaxy's sweep.
[[nodiscard]] Element BuildScreenElement(const std::string &which,
                                         const DataSet &data,
                                         const std::string &version,
                                         double galaxy_phase) {
  if (which == "boot") {
    return BootScreen(data, /*selected_index=*/0, version, galaxy_phase);
  }
  if (which == "dashboard") {
    return MakeDashboardScreen(data)->Render();
  }
  if (which == "console") {
    return MakeQueryConsoleScreen()->Render();
  }
  return nullptr;
}

}  // namespace

int RunApp() {
  const DataSet data = LoadDemoData();
  const std::string version = entropy::version();

  int view = kBoot;
  int menu_sel = 0;
  const int menu_count = static_cast<int>(BootMenu().size());

  auto screen = ScreenInteractive::Fullscreen();

  // Clock-driven galaxy sweep: the boot mark's animated sheen rides a phase that
  // cycles once every kSweepPeriod seconds. Reading a wall clock (rather than
  // counting frames) keeps the sweep speed independent of the redraw rate.
  const auto start = std::chrono::steady_clock::now();
  constexpr double kSweepPeriod = 3.6;  // seconds per full diagonal sweep
  const auto boot_phase = [&] {
    const double t = std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - start)
                         .count();
    return std::fmod(t / kSweepPeriod, 1.0);
  };

  Component boot =
      Renderer([&] { return BootScreen(data, menu_sel, version, boot_phase()); });
  Component dashboard = MakeDashboardScreen(data);
  Component console = MakeQueryConsoleScreen();

  Component tab = Container::Tab({boot, dashboard, console}, &view);

  // Map the current menu row to an action.
  const auto activate = [&] {
    switch (menu_sel) {
      case 0:  // Crash Simulator
        view = kDashboard;
        break;
      case 1:  // Query Console
        view = kConsole;
        break;
      case 2:  // Recovery Report (routes to the dashboard for now)
        view = kDashboard;
        break;
      default:  // Quit
        screen.Exit();
        break;
    }
  };

  Component root = CatchEvent(tab, [&](const Event &e) {
    if (view == kBoot) {
      if (e == Event::ArrowUp || e == Event::Character('k')) {
        menu_sel = (menu_sel + menu_count - 1) % menu_count;
        return true;
      }
      if (e == Event::ArrowDown || e == Event::Character('j')) {
        menu_sel = (menu_sel + 1) % menu_count;
        return true;
      }
      if (e == Event::Return) {
        activate();
        return true;
      }
      if (e == Event::Character('1')) {
        view = kDashboard;
        return true;
      }
      if (e == Event::Character('2')) {
        view = kConsole;
        return true;
      }
      if (e == Event::Character('3')) {
        view = kDashboard;
        return true;
      }
      if (e == Event::Character('q')) {
        screen.Exit();
        return true;
      }
      return false;
    }

    // Inside a screen.
    if (e == Event::Escape || e == Event::Character('b')) {
      view = kBoot;
      return true;
    }
    if (e == Event::Tab) {
      view = (view == kDashboard) ? kConsole : kDashboard;
      return true;
    }
    if (e == Event::Character('q')) {
      screen.Exit();
      return true;
    }
    return false;
  });

  // Animation pump: post a redraw ~17x/second so the boot galaxy's sheen sweep
  // advances smoothly. Only the boot screen animates, so skip the post on the
  // static dashboard/console to avoid needless re-renders there. Reading `view`
  // across threads is benign here: it is a lone int used only to decide whether
  // to request a frame, so a stale value costs at most one extra or missed
  // frame, never a torn read.
  std::atomic<bool> running{true};
  std::thread ticker([&] {
    while (running.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(58));
      if (view == kBoot) {
        screen.PostEvent(Event::Custom);
      }
    }
  });

  screen.Loop(root);

  running.store(false, std::memory_order_relaxed);
  ticker.join();
  return 0;
}

int CaptureFrame(const std::string &which, int cols, int rows,
                 double galaxy_phase) {
  // Pin TrueColor so the RGB/HSV ramps emit 24-bit ANSI regardless of the
  // capturing terminal's advertised support.
  Terminal::SetColorSupport(Terminal::TrueColor);

  const DataSet data = LoadDemoData();
  const std::string version = entropy::version();

  Element doc = BuildScreenElement(which, data, version, galaxy_phase);
  if (!doc) {
    std::cerr << "entropy-tui: unknown screen '" << which
              << "' (expected boot | dashboard | console)\n";
    return 2;
  }

  Screen screen =
      Screen::Create(Dimension::Fixed(cols), Dimension::Fixed(rows));
  Render(screen, doc);
  std::cout << screen.ToString() << std::endl;
  return 0;
}

}  // namespace entropy::tui
