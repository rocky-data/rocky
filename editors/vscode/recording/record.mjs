// Record a demo GIF of the Rocky VS Code extension.
//
//   node record.mjs <scenario>        # e.g. quickstart
//
// Output lands in out/<scenario>.gif. Per-run scratch (webm, extension dir)
// stays in .run/ for debugging; the throwaway VS Code profile is cleaned up.

import * as path from "node:path";
import * as fs from "node:fs";
import { spawnSync } from "node:child_process";
import { launchVSCode } from "./lib/vscode.mjs";
import { makeDriver } from "./lib/driver.mjs";
import { toGif } from "./lib/convert.mjs";

const HERE = import.meta.dirname;
const VSCODE_DIR = path.resolve(HERE, ".."); // editors/vscode
const REPO = path.resolve(HERE, "../../.."); // rocky-data

// VS Code loads the extension's built `main` (dist/extension.js) from
// --extensionDevelopmentPath; it does NOT compile from src/. So rebuild the
// bundle first, or we'd record stale code. `npm run bundle` (esbuild) builds
// both the extension host and the React webview apps under dist/webviews/.
function buildExtension() {
  console.log("⚙ building extension bundle…");
  const r = spawnSync("npm", ["run", "bundle"], { cwd: VSCODE_DIR, stdio: "inherit" });
  if (r.status !== 0) {
    console.error("extension build failed at `npm run bundle`");
    process.exit(1);
  }
}

const name = process.argv[2];
if (!name) {
  console.error("usage: node record.mjs <scenario>");
  console.error("scenarios:", fs.readdirSync(path.join(HERE, "scenarios")).map((f) => f.replace(/\.mjs$/, "")).join(", "));
  process.exit(1);
}

const scenarioPath = path.join(HERE, "scenarios", `${name}.mjs`);
if (!fs.existsSync(scenarioPath)) {
  console.error(`no such scenario: ${scenarioPath}`);
  process.exit(1);
}
const scenario = (await import(scenarioPath)).default;

const size = scenario.size ?? { width: 1280, height: 800 };
const workspace = path.resolve(REPO, scenario.workspace ?? "examples/playground");
const runDir = path.join(HERE, ".run", `${name}-${Date.now()}`);

console.log(`▶ recording "${name}" — ${scenario.description ?? ""}`);
buildExtension();

// Optional per-scenario setup, run before VS Code launches. Used to
// materialize a workspace (seed + `rocky run`) so data-backed Inspector tabs
// (Preview/Profile/Tests) show real rows instead of an empty/loading state.
if (scenario.setup) {
  console.log("⚙ scenario setup (materializing the workspace)…");
  await scenario.setup(workspace, REPO);
}

const { app, win, windowReadyAt, cleanup } = await launchVSCode({
  vscodeDir: VSCODE_DIR,
  workspace,
  size,
  recordDir: runDir,
  settings: scenario.settings,
});
const d = makeDriver(win);

// Preamble: let the workbench + LSP settle, then close the secondary side bar
// (the chat panel auto-opens in a fresh profile) for a clean canvas.
await d.pause(scenario.settle ?? 4000);
if (scenario.closeAuxBar !== false) {
  await d.key("Meta+Alt+B");
  await d.pause(600);
}

const scenarioStart = Date.now();
let failed;
try {
  await scenario.run(d);
} catch (e) {
  failed = e;
} finally {
  const video = win.video();
  await app.close();
  const webm = await video.path();

  // Auto-trim the boot + preamble: everything before the scenario started.
  const autoTrim = Math.max(0, (scenarioStart - windowReadyAt) / 1000 - 0.3);
  const out = path.join(HERE, "out", `${name}.gif`);
  toGif({
    webm,
    out,
    fps: scenario.fps ?? 15,
    width: scenario.gifWidth ?? 1000,
    quality: scenario.quality ?? 65,
    trimStart: scenario.trimStart ?? autoTrim,
    duration: scenario.duration,
  });
  cleanup?.();
  console.log(`✔ GIF: ${out} (${(fs.statSync(out).size / 1024).toFixed(0)} KB)`);
  console.log(`  webm kept at: ${webm}`);
}

if (failed) {
  console.error("scenario threw (GIF still written up to the failure):", failed);
  process.exit(1);
}
