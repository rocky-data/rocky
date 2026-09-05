/// <reference types="vitest/config" />
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The generated TypeScript types (`just codegen`) live in one tree, under the
// VS Code extension. The SPA imports them through this alias instead of
// carrying a copy; the imports are type-only, so nothing of theirs is bundled.
const generatedTypes = fileURLToPath(
  new URL("../../editors/vscode/src/types/generated", import.meta.url),
);
// The live-binary captures the dagster tests keep (`just regen-fixtures`), read
// by this package's render tests so "the playground renders" is a test.
const capturedFixtures = fileURLToPath(
  new URL("../../integrations/dagster/tests/fixtures_generated", import.meta.url),
);

// The SPA is served by `rocky serve --ui` under `/ui/`, from files embedded
// in the binary at build time (`engine/crates/rocky-cli/src/ui.rs`). Every
// asset URL is therefore relative to `/ui/`, and nothing may load from
// another host: the server's CSP forbids it, and this build must not try.
export default defineConfig({
  base: "/ui/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@rocky-types": generatedTypes, "@rocky-fixtures": capturedFixtures },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
    // One entry, hashed chunks. `dist/index.html` is the SPA fallback the
    // server answers for every client route.
    rollupOptions: { output: { manualChunks: undefined } },
  },
  server: {
    // Local development against a running `rocky serve`: the API keeps its
    // origin, so the same-origin token flow works unchanged.
    proxy: { "/api": "http://127.0.0.1:8080" },
  },
  test: {
    // `globals` lets testing-library register its per-test cleanup.
    globals: true,
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}", "scripts/**/*.test.mjs"],
  },
});
