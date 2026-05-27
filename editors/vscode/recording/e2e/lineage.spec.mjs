// E2E: the lineage canvas webview. This is the kind of coverage the in-process
// @vscode/test-electron suite can't provide — it reaches into the webview's
// out-of-process iframe DOM to assert the ReactFlow graph actually rendered and
// survives a viewport resize. DOM-based, not pixel-based: correct and immune to
// the recording-time compositor quirk.
//
// (The panel is now a ReactFlow canvas — `.react-flow__node` — replacing the
// old dagre/d3 `#graph-svg` renderer. ReactFlow keeps the graph on container
// resize rather than re-fitting the transform, so we assert survival, not a
// scale change.)

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

// The canvas lives in an out-of-process webview iframe; find it by content.
async function canvasFrame(win) {
  for (const f of win.frames()) {
    if (await f.locator(".react-flow__node").count().catch(() => 0)) return f;
  }
  return null;
}

test("lineage canvas renders nodes and survives a viewport resize", async ({ vscode }) => {
  const { win } = vscode;
  const d = makeDriver(win);

  // Settle, close the auto-opened chat panel, open a model, show its lineage
  // (rendered in the bottom panel, full width).
  await d.pause(4000);
  await d.key("Meta+Alt+B");
  await d.pause(500);
  await d.openFile("fct_revenue.rocky");
  await d.pause(1500);
  await d.command("Rocky: Show Model Lineage");
  await d.pause(5000); // catalog + compile fan-in + dagre layout + fitView

  const frame = await canvasFrame(win);
  expect(frame, "lineage canvas webview iframe should be present").toBeTruthy();

  const nodeCount = await frame.locator(".react-flow__node").count();
  expect(nodeCount, "canvas should render at least one node").toBeGreaterThan(0);

  // Hide the primary side bar to widen the panel — ReactFlow must keep the
  // graph intact (the old renderer re-fit the transform; ReactFlow preserves it).
  await d.key("Meta+B");
  await d.pause(2500);

  expect(
    await frame.locator(".react-flow__node").count(),
    "graph must survive the resize",
  ).toBe(nodeCount);
});
