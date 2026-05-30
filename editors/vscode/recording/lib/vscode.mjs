// Launch a real VS Code Electron instance with the Rocky extension loaded
// from source, driven by Playwright and recording to a .webm.
//
// See ../README.md for the why behind each arg. The one non-obvious thing:
// --user-data-dir MUST be short (macOS caps unix-socket paths at ~104 chars
// and VS Code's single-instance IPC socket lives inside it).

import { downloadAndUnzipVSCode } from "@vscode/test-electron";
import { _electron as electron } from "playwright";
import * as path from "node:path";
import * as fs from "node:fs";
import * as os from "node:os";

// Keep in sync with src/test/runTest.ts — the extension is built against this.
export const VSCODE_VERSION = process.env.VSCODE_TEST_VERSION ?? "1.120.0";

// Settings injected into the throwaway profile so demos look clean and
// deterministic (no telemetry prompts, no git/notification chrome, no minimap).
const DEFAULT_SETTINGS = {
  "workbench.startupEditor": "none",
  "window.commandCenter": false,
  "workbench.layoutControl.enabled": false,
  "editor.minimap.enabled": false,
  "breadcrumbs.enabled": false,
  "git.openRepositoryInParentFolders": "never",
  "telemetry.telemetryLevel": "off",
  "update.mode": "none",
  "workbench.tips.enabled": false,
  "editor.fontSize": 14,
  "workbench.colorTheme": "Default Dark Modern",
};

function shortUserDataDir() {
  const id = Math.random().toString(36).slice(2, 8);
  const base = process.platform === "darwin" ? "/tmp" : os.tmpdir();
  return path.join(base, `rkv-${id}`);
}

/**
 * @param {object} o
 * @param {string} o.vscodeDir   absolute path to editors/vscode
 * @param {string} o.workspace   absolute path to the folder to open
 * @param {{width:number,height:number}} o.size  recording + window size
 * @param {string} o.recordDir   dir to drop the .webm + per-run scratch in
 * @param {object} [o.settings]  per-scenario settings overrides
 * @param {boolean} [o.video]    record a .webm (true for recording; false for E2E tests)
 */
export async function launchVSCode({ vscodeDir, workspace, size, recordDir, settings = {}, video = true }) {
  const executablePath = await downloadAndUnzipVSCode({
    version: VSCODE_VERSION,
    cachePath: path.join(vscodeDir, ".vscode-test"),
  });

  const userDataDir = shortUserDataDir();
  const userDir = path.join(userDataDir, "User");
  fs.mkdirSync(userDir, { recursive: true });
  fs.writeFileSync(
    path.join(userDir, "settings.json"),
    JSON.stringify({ ...DEFAULT_SETTINGS, ...settings }, null, 2),
  );

  const extDir = path.join(recordDir, "extensions");
  fs.mkdirSync(extDir, { recursive: true });

  const app = await electron.launch({
    executablePath,
    args: [
      `--extensionDevelopmentPath=${vscodeDir}`,
      `--user-data-dir=${userDataDir}`,
      `--extensions-dir=${extDir}`,
      "--disable-workspace-trust",
      "--skip-welcome",
      "--skip-release-notes",
      "--disable-updates",
      "--disable-telemetry",
      "--disable-gpu-sandbox",
      workspace,
    ],
    ...(video ? { recordVideo: { dir: recordDir, size } } : {}),
    timeout: 60_000,
  });

  const win = await app.firstWindow({ timeout: 60_000 });
  await win.waitForLoadState("domcontentloaded");
  const windowReadyAt = Date.now();

  // Force the real OS window to exactly the recording size so the captured
  // frame is 1:1 instead of scaled/letterboxed.
  try {
    const bw = await app.browserWindow(win);
    await bw.evaluate((w, s) => {
      w.setBounds({ x: 0, y: 0, width: s.width, height: s.height });
      if (typeof w.setMenuBarVisibility === "function") w.setMenuBarVisibility(false);
    }, size);
  } catch (e) {
    console.warn("could not resize window:", e.message);
  }

  return {
    app,
    win,
    executablePath,
    userDataDir,
    windowReadyAt,
    cleanup: () => fs.rmSync(userDataDir, { recursive: true, force: true }),
  };
}
