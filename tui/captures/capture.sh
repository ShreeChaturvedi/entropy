#!/usr/bin/env bash
# Render a still frame of each entropy-tui screen. The animated GIFs (boot,
# dashboard, console) are produced by the companion capture-anim.sh.
#
# Still pipeline:
#   entropy-tui --capture-frame <screen>   ->  ANSI frame (.ans)
#   freeze <ans>                           ->  vector still (.svg)
#   headless Chrome rasterizes the SVG     ->  crisp 2x still (.png)
#
# The galaxy mark is drawn in braille (U+28xx). JetBrains Mono (freeze's font)
# has no braille glyphs, so the browser would substitute a proportional fallback
# and the dots would drift off the panel border. We add a monospace braille
# fallback (FreeMono, from gnu-free-fonts) to the generated SVG's font stack so
# every cell keeps its advance and the layout stays grid-aligned.
#
# Requirements: a built entropy-tui, `freeze` (github.com/charmbracelet/freeze),
# a Chromium/Chrome binary, and a monospace braille font (FreeMono). Override any
# of them via the env vars below.
#
# Usage: tui/captures/capture.sh [<cols>x<rows>]   (default 120x40)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
size="${1:-120x40}"

BIN="${ENTROPY_TUI_BIN:-$here/../../build/tui/entropy-tui}"
if [ ! -x "$BIN" ]; then
  BIN="${ENTROPY_TUI_BIN:-$here/../../build-integ/tui/entropy-tui}"
fi
FREEZE="${FREEZE_BIN:-$(command -v freeze || echo "$HOME/go/bin/freeze")}"
CHROME="${CHROME_BIN:-$(command -v google-chrome || command -v chromium || true)}"
# Monospace fallback for braille cells (see header note).
BRAILLE_FALLBACK="${BRAILLE_FALLBACK:-FreeMono, monospace}"
THEME="${THEME:-dark}"
case "$THEME" in
  dark)  BG="#0e0e10"; BG_ARGB="0e0e10ff" ;;
  light) BG="#faf9f6"; BG_ARGB="faf9f6ff" ;;
  *) echo "capture: THEME must be dark or light" >&2; exit 2 ;;
esac

[ -x "$BIN" ] || { echo "capture: entropy-tui not found at $BIN" >&2; exit 1; }
[ -x "$FREEZE" ] || { echo "capture: freeze not found (set FREEZE_BIN)" >&2; exit 1; }

# freeze <ans> -> <svg>, patched with the braille-safe font stack.
render_svg() {
  local ans="$1" svg="$2"
  "$FREEZE" "$ans" -o "$svg" \
    --font.size 13 --line-height 1.1 --padding 20 --margin 0 --background "$BG"
  sed -i "s/<g font-family=\"JetBrains Mono\"/<g font-family=\"JetBrains Mono, ${BRAILLE_FALLBACK}\"/" "$svg"
  # Fail loudly if freeze changed its output shape and the patch did not apply,
  # rather than silently shipping a still with drifted braille.
  grep -qF "$BRAILLE_FALLBACK" "$svg" ||
    { echo "capture: braille font-fallback patch did not apply to $svg" >&2; exit 1; }
}

# <svg> -> <png> at 2x via headless Chrome.
render_png() {
  local svg="$1" png="$2" scale="${3:-2}"
  [ -n "$CHROME" ] || return 0
  local w h wi hi wrap
  w=$(grep -oE 'width="[0-9.]+"' "$svg" | head -1 | grep -oE '[0-9.]+')
  h=$(grep -oE 'height="[0-9.]+"' "$svg" | head -1 | grep -oE '[0-9.]+')
  wi=$(printf '%.0f' "$w"); hi=$(printf '%.0f' "$h")
  wrap="${svg%.svg}.html"
  printf '<!doctype html><meta charset="utf-8"><style>html,body{margin:0;padding:0;background:%s;overflow:hidden}img{display:block;width:%spx;height:%spx}</style><img src="file://%s">' \
    "$BG" "$wi" "$hi" "$svg" > "$wrap"
  "$CHROME" --headless=new --no-sandbox --hide-scrollbars --disable-gpu \
    --force-device-scale-factor="$scale" --window-size="$wi,$hi" \
    --default-background-color="$BG_ARGB" \
    --screenshot="$png" "file://$wrap" >/dev/null 2>&1
  rm -f "$wrap"
}

# ── Still frames ──────────────────────────────────────────────────────────────
suffix=""; [ "$THEME" = light ] && suffix="-light"
for screen in boot dashboard console; do
  out="${screen}${suffix}"
  "$BIN" --capture-frame "$screen" --size "$size" --theme "$THEME" \
    > "$here/$out.ans"
  render_svg "$here/$out.ans" "$here/$out.svg"
  render_png "$here/$out.svg" "$here/$out.png"
  echo "captured $out (theme=$THEME)"
done
