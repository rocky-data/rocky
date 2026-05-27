import { defineConfig } from "vitest/config";

// Two projects: the extension host runs in Node (the existing suites); the
// React webview code runs in jsdom with the automatic JSX transform. `vitest
// run` executes both.
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "host",
          include: ["src/__tests__/**/*.test.ts"],
          environment: "node",
        },
      },
      {
        test: {
          name: "webview",
          include: ["webview-ui/**/*.test.{ts,tsx}"],
          environment: "jsdom",
          setupFiles: ["webview-ui/test/setup.ts"],
        },
      },
    ],
  },
});
