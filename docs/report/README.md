# Entropy technical report

Typeset PDF report on the engine and its deterministic crash simulator.
Compiled output: [`dist/entropy-report.pdf`](dist/entropy-report.pdf).

Rebuild:

```bash
cd docs/report && npm install && npm run pdf
```

The pipeline composes fixed 8.5x11in pages in React (Vite), then prints them
through headless Chromium via puppeteer-core. It uses the system browser; set
`CHROME_PATH` if neither `chromium` nor `google-chrome` is on the default
paths. Fonts (Instrument Serif, Schibsted Grotesk, IBM Plex Mono) are
self-hosted through Fontsource, and every diagram is hand-authored SVG in
`src/figures/`. Terminal stills in `public/tui/` are trimmed copies of the
deterministic captures in `tui/captures/`.
