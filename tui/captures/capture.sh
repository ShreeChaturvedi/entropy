#!/usr/bin/env bash
# Render still frames (and the boot sheen-sweep GIF) of each entropy-tui screen.
#
# Still pipeline:
#   entropy-tui --capture-frame <screen>   ->  ANSI frame (.ans)
#   freeze <ans>                           ->  vector still (.svg)
#   headless Chrome rasterizes the SVG     ->  crisp 2x still (.png)
#
# GIF pipeline (boot only):
#   entropy-tui --capture-frame boot --phase p   for p across one sweep cycle
#   each frame rasterized as above, then assembled by ffmpeg into boot.gif.
#
# The galaxy mark is drawn in braille (U+28xx). JetBrains Mono (freeze's font)
# has no braille glyphs, so the browser would substitute a proportional fallback
# and the dots would drift off the panel border. We add a monospace braille
# fallback (FreeMono, from gnu-free-fonts) to the generated SVG's font stack so
# every cell keeps its advance and the layout stays grid-aligned.
#
# Requirements: a built entropy-tui, `freeze` (github.com/charmbracelet/freeze),
# a Chromium/Chrome binary, ffmpeg, and a monospace braille font (FreeMono).
# Override any of them via the env vars below.
#
# Usage: tui/captures/capture.sh [<cols>x<rows>]   (default 120x40)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
size="${1:-120x40}"

BIN="${ENTROPY_TUI_BIN:-$here/../../build-integ/tui/entropy-tui}"
FREEZE="${FREEZE_BIN:-$(command -v freeze || echo "$HOME/go/bin/freeze")}"
CHROME="${CHROME_BIN:-$(command -v google-chrome || command -v chromium || true)}"
FFMPEG="${FFMPEG_BIN:-$(command -v ffmpeg || true)}"
# Monospace fallback for braille cells (see header note).
BRAILLE_FALLBACK="${BRAILLE_FALLBACK:-FreeMono, monospace}"
# Number of frames in one boot sheen-sweep cycle, and the GIF frame rate.
GIF_FRAMES="${GIF_FRAMES:-36}"
GIF_FPS="${GIF_FPS:-20}"
BG="#0e0e10"

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
    --default-background-color=0e0e10ff \
    --screenshot="$png" "file://$wrap" >/dev/null 2>&1
  rm -f "$wrap"
}

# ── Still frames ──────────────────────────────────────────────────────────────
for screen in boot dashboard console; do
  "$BIN" --capture-frame "$screen" --size "$size" > "$here/$screen.ans"
  render_svg "$here/$screen.ans" "$here/$screen.svg"
  render_png "$here/$screen.svg" "$here/$screen.png"
  echo "captured $screen"
done

# ── Boot sheen-sweep GIF ──────────────────────────────────────────────────────
# Deterministic: GIF_FRAMES phases evenly spanning one cycle [0, 1). The sweep
# sits fully off-disc at phase 0 and ~1, so the loop is seamless.
if [ -n "$CHROME" ] && [ -n "$FFMPEG" ]; then
  frames="$(mktemp -d)"
  for i in $(seq 0 $((GIF_FRAMES - 1))); do
    phase=$(awk "BEGIN{printf \"%.5f\", $i/$GIF_FRAMES}")
    ans="$frames/f.ans"; svg="$frames/f.svg"
    png=$(printf '%s/f_%03d.png' "$frames" "$i")
    "$BIN" --capture-frame boot --size "$size" --phase "$phase" > "$ans"
    render_svg "$ans" "$svg"
    render_png "$svg" "$png" 1
  done
  "$FFMPEG" -y -framerate "$GIF_FPS" -i "$frames/f_%03d.png" \
    -vf "palettegen=stats_mode=full" "$frames/pal.png" >/dev/null 2>&1
  "$FFMPEG" -y -framerate "$GIF_FPS" -i "$frames/f_%03d.png" -i "$frames/pal.png" \
    -lavfi "paletteuse=dither=bayer:bayer_scale=3" -loop 0 "$here/boot.gif" >/dev/null 2>&1
  rm -rf "$frames"
  echo "captured boot.gif (${GIF_FRAMES} frames @ ${GIF_FPS}fps)"
else
  echo "capture: skipping boot.gif (need Chrome + ffmpeg)" >&2
fi
