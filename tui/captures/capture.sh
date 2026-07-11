#!/usr/bin/env bash
# Render still frames of each entropy-tui screen to SVG + PNG.
#
# Pipeline:
#   entropy-tui --capture-frame <screen>   ->  ANSI frame (.ans)
#   freeze <ans>                           ->  vector still (.svg)
#   headless Chrome rasterizes the SVG     ->  crisp 2x still (.png)
#
# Requirements: a built entropy-tui, `freeze` (github.com/charmbracelet/freeze),
# and a Chromium/Chrome binary. Override any of them via the env vars below.
#
# Usage: tui/captures/capture.sh [<cols>x<rows>]   (default 120x40)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
size="${1:-120x40}"

BIN="${ENTROPY_TUI_BIN:-$here/../../build-integ/tui/entropy-tui}"
FREEZE="${FREEZE_BIN:-$(command -v freeze || echo "$HOME/go/bin/freeze")}"
CHROME="${CHROME_BIN:-$(command -v google-chrome || command -v chromium || true)}"
BG="#0e0e10"

[ -x "$BIN" ] || { echo "capture: entropy-tui not found at $BIN" >&2; exit 1; }
[ -x "$FREEZE" ] || { echo "capture: freeze not found (set FREEZE_BIN)" >&2; exit 1; }

for screen in boot dashboard console; do
  "$BIN" --capture-frame "$screen" --size "$size" > "$here/$screen.ans"
  "$FREEZE" "$here/$screen.ans" -o "$here/$screen.svg" \
    --font.size 13 --line-height 1.1 --padding 20 --margin 0 --background "$BG"

  if [ -n "$CHROME" ]; then
    w=$(grep -oE 'width="[0-9.]+"' "$here/$screen.svg" | head -1 | grep -oE '[0-9.]+')
    h=$(grep -oE 'height="[0-9.]+"' "$here/$screen.svg" | head -1 | grep -oE '[0-9.]+')
    wi=$(printf '%.0f' "$w"); hi=$(printf '%.0f' "$h")
    wrap="$here/.$screen.html"
    printf '<!doctype html><meta charset="utf-8"><style>html,body{margin:0;padding:0;background:%s;overflow:hidden}img{display:block;width:%spx;height:%spx}</style><img src="%s.svg">' \
      "$BG" "$wi" "$hi" "$screen" > "$wrap"
    "$CHROME" --headless=new --no-sandbox --hide-scrollbars --disable-gpu \
      --force-device-scale-factor=2 --window-size="$wi,$hi" \
      --default-background-color=0e0e10ff \
      --screenshot="$here/$screen.png" "file://$wrap" >/dev/null 2>&1
    rm -f "$wrap"
  fi
  echo "captured $screen"
done
