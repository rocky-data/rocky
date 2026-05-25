import { defineConfig } from "@playwright/test";

// E2E tests drive a real VS Code Electron instance (reusing the launch helper
// in ../lib/vscode.mjs) and assert against the live extension — including
// webview DOM, which the in-process @vscode/test-electron suite can't reach.
// Serial + single worker: each test launches its own VS Code, and the shared
// /tmp user-data dirs + extension build don't parallelize cleanly.
export default defineConfig({
  testDir: ".",
  testMatch: "**/*.spec.mjs",
  globalSetup: "./global-setup.mjs",
  timeout: 120_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  workers: 1,
  reporter: "list",
});
