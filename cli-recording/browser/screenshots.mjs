// Capture still screenshots of the Rocky browser UI, for the docs.
//
//   node screenshots.mjs --url <printed-url> --out <dir> <name>=<ui-path> [...]
//
// `<printed-url>` is the address `rocky serve --ui` printed, token fragment
// and all. The page is opened once with it, so the SPA moves the token into
// sessionStorage; every later shot navigates inside the same tab by address.
//
// Each shot waits until the page text stops changing and no "loading" text is
// left. A console error or an uncaught exception fails the run, as in
// record.mjs: a screenshot of a broken page is not documentation.

import * as fs from "node:fs";
import * as path from "node:path";
import { chromium } from "playwright";

const argv = process.argv.slice(2);
function flag(name) {
  const i = argv.indexOf(`--${name}`);
  if (i === -1) return undefined;
  const value = argv[i + 1];
  argv.splice(i, 2);
  return value;
}
const url = flag("url");
const outDir = flag("out");
if (!url || !outDir || argv.length === 0) {
  console.error("usage: node screenshots.mjs --url <printed-url> --out <dir> <name>=<ui-path> [...]");
  process.exit(2);
}
fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  colorScheme: "light",
});
const page = await context.newPage();
const errors = [];
page.on("console", (msg) => msg.type() === "error" && errors.push(msg.text()));
page.on("pageerror", (err) => errors.push(String(err)));

await page.goto(url);
await page.waitForFunction(() => !location.hash.includes("token"), null, { timeout: 10_000 });
const origin = new URL(url).origin;

// The lanes' in-flight lines all end in an ellipsis: "Loading the digest…",
// "reading the plan…", "compiling both sides…", "running the query…".
const LOADING = /(loading|reaching the engine|compiling both sides|reading the [a-z ]+|running the query|tracing)[^\n]*…/i;

for (const pair of argv) {
  const [name, uiPath] = pair.split("=");
  await page.goto(`${origin}${uiPath}`);
  // Settle: the same body text for 1.5 s, with no loading text left.
  let last = "";
  let stable = 0;
  for (let i = 0; i < 150 && stable < 15; i++) {
    const text = await page.evaluate(() => document.body.innerText);
    stable = text === last && !LOADING.test(text) ? stable + 1 : 0;
    last = text;
    await page.waitForTimeout(100);
  }
  if (stable < 15) throw new Error(`${name}: ${uiPath} never settled`);
  const file = path.join(outDir, `${name}.png`);
  await page.screenshot({ path: file });
  console.log(`✓ ${name}  ${uiPath}  ->  ${file}`);
}

await browser.close();
if (errors.length > 0) {
  console.error("page errors:", errors);
  process.exit(1);
}
