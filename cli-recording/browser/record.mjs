// Record the Rocky browser UI against a live `rocky serve --ui`.
//
//   node record.mjs <scene> --url <printed-url> [--out <dir>]
//
// `<printed-url>` is the address `rocky serve --ui` prints at start, token
// fragment and all: http://127.0.0.1:<port>/ui/#token=<secret>. The SPA reads
// the fragment once, moves it to sessionStorage and rewrites the address
// without it, so by the first frame worth keeping the location bar carries no
// secret (engine/ui/src/token.ts).
//
// Output lands in <out>/<scene>.webm. Convert and intercut with the terminal
// tapes in ../record-screencast.sh.
//
// This records a browser, not VS Code — but it is the same Playwright shape as
// editors/vscode/recording/, including the one gotcha that cost an iteration
// there: take the video handle BEFORE close(), because path() only resolves
// after the context is closed.

import * as fs from "node:fs";
import * as path from "node:path";
import { chromium } from "playwright";
import { SCENES } from "./scenes.mjs";

const HERE = import.meta.dirname;

function usage(msg) {
  if (msg) console.error(`record.mjs: ${msg}`);
  console.error("usage: node record.mjs <scene> --url <printed-url> [--out <dir>]");
  console.error("scenes:", Object.keys(SCENES).join(", "));
  process.exit(2);
}

const argv = process.argv.slice(2);
const sceneName = argv[0];
if (!sceneName || sceneName.startsWith("-")) usage("no scene given");
const scene = SCENES[sceneName];
if (!scene) usage(`no such scene: ${sceneName}`);

function flag(name) {
  const i = argv.indexOf(`--${name}`);
  return i === -1 ? undefined : argv[i + 1];
}

const url = flag("url") ?? process.env.ROCKY_UI_URL;
if (!url) usage("--url is required (the address `rocky serve --ui` printed)");
const outDir = path.resolve(flag("out") ?? path.join(HERE, "out"));

// The canvas matches the terminal tapes (1200x700) so the two halves intercut
// without a letterbox. Playwright records the viewport, not the window.
const size = scene.size ?? { width: 1200, height: 700 };

fs.mkdirSync(outDir, { recursive: true });

console.log(`▶ recording browser scene "${sceneName}" — ${scene.description}`);

const browser = await chromium.launch({ headless: scene.headless ?? true });
const context = await browser.newContext({
  viewport: size,
  recordVideo: { dir: outDir, size },
  deviceScaleFactor: 2,
  // The engine serves one theme per the viewer's preference; pin it so two
  // recordings on two machines look the same.
  colorScheme: scene.colorScheme ?? "light",
});
const page = await context.newPage();

// Surface page errors instead of recording a blank frame and calling it done.
const pageErrors = [];
page.on("pageerror", (e) => pageErrors.push(String(e)));
page.on("console", (m) => {
  if (m.type() === "error") pageErrors.push(m.text());
});

let failure = null;
try {
  await page.goto(url, { waitUntil: "domcontentloaded" });
  await scene.run(page);
} catch (e) {
  failure = e;
}

// The gotcha: grab the handle first. `video.path()` resolves only after the
// context is closed, and the handle is gone once the page is.
const video = page.video();
await context.close();
await browser.close();

if (failure) {
  console.error(`✗ scene "${sceneName}" failed: ${failure.message}`);
}
for (const e of pageErrors) console.error(`  page error: ${e}`);

const recorded = video ? await video.path() : null;
if (!recorded) {
  console.error("no video was produced");
  process.exit(1);
}
const finalPath = path.join(outDir, `${sceneName}.webm`);
fs.renameSync(recorded, finalPath);
console.log(`✓ ${finalPath}`);

// A page error means the frames are not trustworthy even if a file exists.
if (failure || pageErrors.length > 0) process.exit(1);
