// E2E: the lineage webview. This is the kind of coverage the in-process
// @vscode/test-electron suite can't provide — it reaches into the webview's
// out-of-process iframe DOM to assert the graph actually rendered, and that it
// re-fits when the viewport widens (the ResizeObserver fix in
// src/commands/lineage.ts). DOM-based, not pixel-based: correct and immune to
// the recording-time compositor quirk.

import { test as base, expect } from "@playwright/test";
import * as path from "node:path";
import { launchVSCode } from "../lib/vscode.mjs";
import { makeDriver } from "../lib/driver.mjs";

const VSCODE_DIR = path.resolve(import.meta.dirname, "..", "..");
const REPO = path.resolve(VSCODE_DIR, "..", "..");
const WORKSPACE = path.join(REPO, "examples/playground/pocs/06-developer-experience/01-lineage-column-level");

const test = base.extend({
  // Launch a real VS Code with the extension loaded; tear down after each test.
  vscode: async ({}, use, testInfo) => {
    const { app, win, cleanup } = await launchVSCode({
      vscodeDir: VSCODE_DIR,
      workspace: WORKSPACE,
      size: { width: 1280, height: 800 },
      recordDir: path.join(testInfo.outputDir, "vscode"),
      video: false,
    });
    await use({ app, win });
    await app.close();
    cleanup?.();
  },
});

// The lineage graph lives in an out-of-process webview iframe; find it by content.
async function lineageFrame(win) {
  for (const f of win.frames()) {
    if (await f.locator("#graph-svg").count().catch(() => 0)) return f;
  }
  return null;
}

function scaleOf(transform) {
  const m = /scale\(([0-9.]+)\)/.exec(transform ?? "");
  return m ? parseFloat(m[1]) : null;
}

test("lineage webview renders nodes and re-fits when the viewport widens", async ({ vscode }) => {
  const { win } = vscode;
  const d = makeDriver(win);

  // Settle, close the auto-opened chat panel, open a model, show its lineage
  // (which now renders in the bottom panel, full width).
  await d.pause(4000);
  await d.key("Meta+Alt+B");
  await d.pause(500);
  await d.openFile("fct_revenue.rocky");
  await d.pause(1500);
  await d.command("Rocky: Show Model Lineage");
  await d.pause(4000);

  const frame = await lineageFrame(win);
  expect(frame, "lineage webview iframe should be present").toBeTruthy();

  const graphGroup = frame.locator("#graph-svg > g");
  const nodeCount = await frame.locator(".node").count();
  expect(nodeCount, "graph should render at least one node").toBeGreaterThan(0);

  const initialScale = scaleOf(await graphGroup.getAttribute("transform"));
  expect(initialScale, "graph should have a fit transform").toBeGreaterThan(0);

  // Hide the primary side bar to widen the panel — the ResizeObserver should
  // re-fit the graph to a larger scale.
  await d.key("Meta+B");
  await d.pause(2500);

  expect(await frame.locator(".node").count(), "graph must not be lost on resize").toBe(nodeCount);
  const widerScale = scaleOf(await graphGroup.getAttribute("transform"));
  expect(widerScale, "graph should re-fit to a larger scale in the wider panel").toBeGreaterThan(initialScale);
});
