import { spawnSync } from "node:child_process";
import * as path from "node:path";

// Build the extension bundle once before the suite runs. VS Code loads the
// built dist/extension.js from --extensionDevelopmentPath, not src/, so without
// this the tests would run against stale code (same trap as the recorder).
export default function globalSetup() {
  const vscodeDir = path.resolve(import.meta.dirname, "../..");
  for (const script of ["bundle:webview", "bundle"]) {
    const r = spawnSync("npm", ["run", script], { cwd: vscodeDir, stdio: "inherit" });
    if (r.status !== 0) throw new Error(`extension build failed at \`npm run ${script}\``);
  }
}
