#!/usr/bin/env bash
# Render the three animated entropy-tui GIFs (boot, dashboard, console).
#
# Each frame is produced deterministically by the real binary and rasterized
# through the same freeze -> headless-Chrome path as the stills, so the GIFs
# match the approved amber-on-charcoal (or amber-on-paper) look exactly:
#
#   boot       entropy-tui --capture-frame boot      --phase p   (sheen sweep)
#   dashboard  entropy-tui --capture-frame dashboard --step  i   (replay demo)
#   console    entropy-tui --capture-frame console   --step  i   (replay demo)
#
# Theme: THEME=dark (default) or THEME=light. Light writes *-light.gif so both
# palettes can live side by side. Must match theme.hpp canvas hex.
#
# The dashboard/console frame counts come from `--demo-frames`, so the demo
# length lives in one place (the C++), never hardcoded here. Frames are
# rasterized in parallel; ffmpeg assembles each looping GIF.
#
# The galaxy mark is braille (U+28xx); JetBrains Mono (freeze's font) has no
# braille glyphs, so a monospace braille fallback (FreeMono) is spliced into the
# SVG font stack to keep every cell grid-aligned (same trick as capture.sh).
#
# Requirements: a built entropy-tui, freeze, a Chrome/Chromium, ffmpeg, FreeMono.
# Usage: tui/captures/capture-anim.sh [<cols>x<rows>]   (default 120x40)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
size="${1:-120x40}"

BIN="${ENTROPY_TUI_BIN:-$here/../../build/tui/entropy-tui}"
# Prefer build/, then build-integ/ for older docs.
if [ ! -x "$BIN" ]; then
  BIN="${ENTROPY_TUI_BIN:-$here/../../build-integ/tui/entropy-tui}"
fi
FREEZE="${FREEZE_BIN:-$(command -v freeze || echo "$HOME/go/bin/freeze")}"
CHROME="${CHROME_BIN:-$(command -v google-chrome || command -v chromium || true)}"
FFMPEG="${FFMPEG_BIN:-$(command -v ffmpeg || true)}"
GIFSICLE="${GIFSICLE_BIN:-$(command -v gifsicle || true)}"
BRAILLE_FALLBACK="${BRAILLE_FALLBACK:-FreeMono, monospace}"
JOBS="${JOBS:-$(nproc)}"

# Theme: dark charcoal or light warm paper (must match theme.hpp).
THEME="${THEME:-dark}"
case "$THEME" in
  dark)
    BG="#0e0e10"
    BG_ARGB="0e0e10ff"
    OUT_SUFFIX=""
    ;;
  light)
    BG="#faf9f6"
    BG_ARGB="faf9f6ff"
    OUT_SUFFIX="-light"
    ;;
  *)
    echo "capture-anim: THEME must be dark or light (got '$THEME')" >&2
    exit 2
    ;;
esac

# Playback rate and boot sweep length.
FPS="${FPS:-20}"
BOOT_FRAMES="${BOOT_FRAMES:-36}"
# Optional final width; frames render native (~976px) and scale to this.
OUT_WIDTH="${OUT_WIDTH:-900}"
MAX_COLORS="${MAX_COLORS:-128}"
# gifsicle pass: -O3 losslessly de-duplicates the hold frames; --lossy trades a
# little quality for size. The restrained palette keeps artifacts invisible.
OPT_LOSSY="${OPT_LOSSY:-40}"
OPT_COLORS="${OPT_COLORS:-128}"

[ -x "$BIN" ] || { echo "capture-anim: entropy-tui not found at $BIN" >&2; exit 1; }
[ -x "$FREEZE" ] || { echo "capture-anim: freeze not found (set FREEZE_BIN)" >&2; exit 1; }
[ -n "$CHROME" ] || { echo "capture-anim: no Chrome/Chromium found" >&2; exit 1; }
[ -n "$FFMPEG" ] || { echo "capture-anim: ffmpeg not found" >&2; exit 1; }

# Render one ANSI frame file -> PNG (scale 1) via freeze + headless Chrome.
render_png() {
  local ans="$1" png="$2"
  local svg="${png%.png}.svg" wrap="${png%.png}.html"
  "$FREEZE" "$ans" -o "$svg" \
    --font.size 13 --line-height 1.1 --padding 20 --margin 0 --background "$BG"
  sed -i "s/<g font-family=\"JetBrains Mono\"/<g font-family=\"JetBrains Mono, ${BRAILLE_FALLBACK}\"/" "$svg"
  grep -qF "$BRAILLE_FALLBACK" "$svg" ||
    { echo "capture-anim: braille font-fallback patch did not apply" >&2; exit 1; }
  local w h wi hi
  w=$(grep -oE 'width="[0-9.]+"' "$svg" | head -1 | grep -oE '[0-9.]+')
  h=$(grep -oE 'height="[0-9.]+"' "$svg" | head -1 | grep -oE '[0-9.]+')
  wi=$(printf '%.0f' "$w"); hi=$(printf '%.0f' "$h")
  printf '<!doctype html><meta charset="utf-8"><style>html,body{margin:0;padding:0;background:%s;overflow:hidden}img{display:block;width:%spx;height:%spx}</style><img src="file://%s">' \
    "$BG" "$wi" "$hi" "$svg" > "$wrap"
  "$CHROME" --headless=new --no-sandbox --hide-scrollbars --disable-gpu \
    --force-device-scale-factor=1 --window-size="$wi,$hi" \
    --default-background-color="$BG_ARGB" --screenshot="$png" "file://$wrap" >/dev/null 2>&1
  rm -f "$wrap" "$svg"
}
export -f render_png
export FREEZE CHROME BRAILLE_FALLBACK BG BG_ARGB

# Emit ANSI frames for a screen into $dir as f_%03d.ans, returning the count.
emit_frames() {
  local screen="$1" dir="$2" i n phase
  case "$screen" in
    boot)
      n="$BOOT_FRAMES"
      for ((i=0; i<n; i++)); do
        phase=$(awk "BEGIN{printf \"%.5f\", $i/$n}")
        "$BIN" --capture-frame boot --size "$size" --phase "$phase" \
          --theme "$THEME" \
          > "$(printf '%s/f_%03d.ans' "$dir" "$i")"
      done ;;
    dashboard|console)
      n="$("$BIN" --demo-frames "$screen")"
      for ((i=0; i<n; i++)); do
        "$BIN" --capture-frame "$screen" --size "$size" --step "$i" \
          --theme "$THEME" \
          > "$(printf '%s/f_%03d.ans' "$dir" "$i")"
      done ;;
  esac
  echo "$n"
}

build_gif() {
  local screen="$1"
  local out_name="${screen}${OUT_SUFFIX}.gif"
  local dir; dir="$(mktemp -d)"
  local n; n="$(emit_frames "$screen" "$dir")"
  # Rasterize every frame in parallel.
  ls "$dir"/f_*.ans | xargs -P "$JOBS" -I{} bash -c \
    'render_png "$1" "${1%.ans}.png"' _ {}
  local pal="$dir/pal.png"
  "$FFMPEG" -y -framerate "$FPS" -i "$dir/f_%03d.png" \
    -vf "scale=${OUT_WIDTH}:-1:flags=lanczos,palettegen=max_colors=${MAX_COLORS}:stats_mode=full" \
    "$pal" >/dev/null 2>&1
  "$FFMPEG" -y -framerate "$FPS" -i "$dir/f_%03d.png" -i "$pal" \
    -lavfi "scale=${OUT_WIDTH}:-1:flags=lanczos[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle" \
    -loop 0 "$here/$out_name" >/dev/null 2>&1
  rm -rf "$dir"
  if [ -n "$GIFSICLE" ]; then
    "$GIFSICLE" -O3 --lossy="$OPT_LOSSY" --colors "$OPT_COLORS" \
      "$here/$out_name" -o "$here/$out_name" 2>/dev/null
  fi
  local bytes; bytes=$(stat -c%s "$here/$out_name")
  echo "captured $out_name ($n frames @ ${FPS}fps, $((bytes/1024)) KB, theme=$THEME)"
}

# Render all three by default, or only the screens named in $SCREENS.
for screen in ${SCREENS:-boot dashboard console}; do
  build_gif "$screen"
done
