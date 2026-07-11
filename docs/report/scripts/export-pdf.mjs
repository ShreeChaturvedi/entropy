// Prints the built report site to dist/entropy-report.pdf using the system
// Chromium/Chrome via puppeteer-core (no browser download).
import { preview } from "vite";
import puppeteer from "puppeteer-core";
import { existsSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const outDir = path.join(root, "dist");
const outFile = process.env.REPORT_OUT ?? path.join(outDir, "entropy-report.pdf");

function findBrowser() {
  const candidates = [
    process.env.CHROME_PATH,
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  throw new Error("No Chromium/Chrome binary found. Set CHROME_PATH.");
}

const server = await preview({
  root,
  preview: { port: 5191, strictPort: true },
  build: { outDir: "build-site" },
});
const url = server.resolvedUrls.local[0];
console.log(`serving ${url}`);

const browser = await puppeteer.launch({
  executablePath: findBrowser(),
  headless: true,
  args: ["--no-sandbox", "--font-render-hinting=none", "--force-color-profile=srgb"],
});

try {
  const page = await browser.newPage();
  await page.setViewport({ width: 816, height: 1056, deviceScaleFactor: 2 });
  await page.goto(url, { waitUntil: "networkidle0", timeout: 60_000 });
  await page.evaluate(() => document.fonts.ready);
  await new Promise((resolve) => setTimeout(resolve, 700));

  const pageCount = await page.evaluate(() => document.querySelectorAll(".page").length);
  console.log(`rendering ${pageCount} pages`);

  const overflows = await page.evaluate(() => {
    const bad = [];
    document.querySelectorAll(".page").forEach((el, i) => {
      if (el.scrollHeight > el.clientHeight + 1 || el.scrollWidth > el.clientWidth + 1) {
        bad.push(`page ${i + 1}: ${el.scrollWidth}x${el.scrollHeight} in ${el.clientWidth}x${el.clientHeight}`);
      }
    });
    return bad;
  });
  if (overflows.length > 0) {
    console.warn("content overflow detected:");
    for (const line of overflows) console.warn(`  ${line}`);
  }

  mkdirSync(outDir, { recursive: true });
  await page.pdf({
    path: outFile,
    width: "8.5in",
    height: "11in",
    printBackground: true,
    preferCSSPageSize: true,
    margin: { top: 0, right: 0, bottom: 0, left: 0 },
  });
  console.log(`wrote ${outFile}`);
} finally {
  await browser.close();
  await server.close();
}
