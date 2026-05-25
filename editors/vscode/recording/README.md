# Demo GIF recording

Automated demo-GIF recording for the Rocky VS Code extension. Playwright drives
a **real VS Code Electron instance** with the extension loaded from source, runs
a scripted scenario, records video, and converts it to a GIF.

This records the *extension UI* (editor, LSP, command palette, webviews). The
CLI/terminal demos in the repo-root `docs/` are a separate thing.

## Usage

```bash
cd editors/vscode/recording
npm install
npm run record quickstart        # -> out/quickstart.gif
# or from the repo root:
just record-demo quickstart
just publish-demo quickstart      # copy to editors/vscode/media/demo-quickstart.gif
```

The recorder **rebuilds the extension bundle first** (`npm run bundle`): VS Code
loads the built `dist/extension.js` from `--extensionDevelopmentPath`, not `src/`,
so without a rebuild it would record stale code. `rocky` must be on `$PATH` (the
launched VS Code inherits it, so the LSP connects automatically).

## E2E tests

The same launch infrastructure backs Playwright **end-to-end tests** — for the
things the in-process `@vscode/test-electron` suite can't reach, chiefly webview
DOM. These reach into the out-of-process webview iframe and assert real
behavior (DOM, not pixels).

```bash
npm run test:e2e        # editors/vscode/recording/, runs e2e/*.spec.mjs
```

`e2e/lineage.spec.mjs` opens the lineage webview, asserts the graph rendered,
and verifies it re-fits when the viewport widens. Tests build the extension
bundle first (global-setup) and launch without video. Keep this layer thin —
reserve it for webview/UI behavior; unit (vitest) and in-process integration
(`@vscode/test-electron`) cover the rest more cheaply.

## Requirements

- `ffmpeg` and `gifski` (`brew install ffmpeg gifski`) — gifski is preferred.
  Without gifski it falls back to an ffmpeg palette pipeline, plus a
  `gifsicle --lossy` pass if `gifsicle` is installed.
- VS Code is downloaded on first run via `@vscode/test-electron` (reuses the
  `../.vscode-test` cache the test suite already populates).

## Adding a scenario

Drop a `scenarios/<name>.mjs` exporting a default object:

```js
export default {
  name: "myscenario",
  description: "One line shown while recording.",
  workspace: "examples/playground",   // relative to repo root
  size: { width: 1280, height: 800 }, // capture (and forced window) size
  fps: 15,
  gifWidth: 1000,                      // output width; quality is the size lever
  quality: 65,                         // gifski 1-100 — the main size/quality knob
  async run(d) {
    await d.openFile("customer_orders.rocky");
    await d.pause(3000);
    await d.palette("Rocky: ");        // open palette filtered; add {run:true} to execute
    await d.pause(3000);
    await d.escape();
  },
};
```

The driver (`lib/driver.mjs`) is deliberately **keyboard-only** — Playwright's
Electron video has no visible mouse cursor, so click flows look broken. Available
actions: `openFile`, `palette`, `command`, `key`, `type`, `escape`, `pause`.

Boot + preamble are auto-trimmed (recording starts at launch; everything before
`run()` is cut). Per-run scratch (the `.webm`) is kept in `.run/` for debugging.

## Gotchas (learned the hard way)

- **macOS unix-socket cap.** VS Code's single-instance IPC socket lives inside
  `--user-data-dir`, and macOS caps socket paths at ~104 chars. A long
  user-data-dir → instant death with `connect ENOTSOCK .../<v>-main.sock`. The
  harness puts it under `/tmp/rkv-<id>`.
- **gifski halves resolution by default** (assumes 2× HiDPI input). The
  converter passes `--width` explicitly so output matches the captured frames.
- **gifsicle doesn't help gifski output.** gifski already emits a tightly
  optimized GIF; `gifski --quality` is the real size lever. gifsicle is only
  used on the ffmpeg fallback path.
- **Fresh profile auto-opens the chat panel.** The runner closes the secondary
  side bar (⌘⌥B) in the preamble; injected settings suppress the git-repo
  notification, telemetry, and minimap.
