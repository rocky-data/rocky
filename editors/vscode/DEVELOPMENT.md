# Development Guide

How to build, run and test the extension from source. Coding rules and architecture notes: [`AGENTS.md`](./AGENTS.md).

## Prerequisites

Node.js 22 and npm (CI uses Node 22), VS Code 1.120.0+, and the [Rocky CLI](https://github.com/rocky-data/rocky) on `$PATH`.

## Getting started

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/editors/vscode
npm install
npm run compile
npm run bundle
```

Then press **F5** in VS Code to open an Extension Development Host with the extension loaded.

## How the build fits together

```
  src/**/*.ts ──► tsc (npm run compile) ──► out/       tests run from here
       │
       └──────► esbuild (npm run bundle) ──► dist/extension.js   "main" entry

  webview-ui/**/*.tsx ──► esbuild + Tailwind ──► webview bundles (Inspector)
```

`package.json` points `main` at `./dist/extension.js`, so the dev host runs the esbuild bundle. After a change, run `npm run bundle` (or `npm run bundle:watch`) and reload the dev host. `vscode:prepublish` runs both steps.

## Project structure

```
editors/vscode/
├── src/
│   ├── extension.ts       # activate/deactivate: LSP client + command registry
│   ├── lspClient.ts       # LSP client lifecycle (start/stop/restart)
│   ├── rockyCli.ts        # subprocess helpers (execFile + progress)
│   ├── mcpServer.ts       # registers `rocky mcp` per workspace folder
│   ├── chatParticipant.ts # the @rocky chat participant
│   ├── commands/          # one file per concern; index.ts registers them
│   ├── views/             # activity-bar tree views
│   ├── webviews/          # webview panel hosts (Inspector, review, doctor, …)
│   ├── types/generated/   # generated from the engine schemas (do not edit)
│   ├── __tests__/         # Vitest unit tests
│   └── test/suite/        # Electron integration tests (Mocha)
├── webview-ui/            # React source for the webviews
├── syntaxes/              # TextMate grammar for .rocky files
├── snippets/rocky.json    # DSL and sidecar snippets
├── schemas/               # JSON Schemas for rocky.toml and *.rocky.toml
├── themes/                # semantic token colours
├── fileicons/, icons/     # file icon theme and extension icons
├── recording/             # demo-GIF recorder (see recording/README.md)
├── esbuild.mjs            # bundler config
└── package.json           # extension manifest
```

`package.json` (`contributes.commands`) is the source of truth for the 61 commands. The [README](./README.md#commands) lists the common ones.

## Architecture

### LSP client (`lspClient.ts`)

On activation the extension spawns `rocky lsp [extraArgs...]` and talks JSON-RPC over **stdio**. The path and extra arguments come from `rocky.server.path` and `rocky.server.extraArgs`. When a `rocky-lsp` binary sits next to `rocky`, the extension runs that, and falls back to `rocky lsp` otherwise.

- **Document selector**: `.rocky` files, and `.sql` files under `**/models/**`.
- **File watchers**: `**/*.rocky`, `**/*.toml`, `**/models/**/*.sql`.

The server's feature list is in the [engine `AGENTS.md`](../../engine/AGENTS.md#lsp-server).

### Commands

Command handlers live in `src/commands/`, one file per concern, registered in
`src/commands/index.ts`. They use `cp.execFile()`, never `exec()`, and
`vscode.window.withProgress()` for long operations.

### Inspector and lineage canvas

`rocky.showLineage` opens the Rocky Inspector, a bottom-panel React webview from `webview-ui/`, on its Lineage tab. The canvas is built from `rocky catalog` and `rocky compile` JSON, rendered with `@xyflow/react` and laid out with `@dagrejs/dagre`. Overlays show cost, freshness, drift, governance, breaking changes and the last run. Right-click a node for scoped AI actions.

## Common commands

```bash
npm run compile        # tsc, one shot
npm run watch          # tsc in watch mode
npm run bundle         # esbuild bundle into dist/
npm run test:unit      # Vitest unit tests
npm test               # Electron integration tests
npm run lint           # ESLint over src/ and webview-ui/
npm run package        # build rocky-<version>.vsix
npm run install:local  # package as rocky.vsix and install it

# Record a demo GIF (from the monorepo root; see recording/README.md)
just record-demo quickstart
```

## Development workflow

### 1. Extension Development Host (code changes)

Press **F5** (config in `.vscode/launch.json`). After a TypeScript change, run `npm run bundle` and reload the dev host (Ctrl+Shift+P > "Developer: Reload Window").

### 2. Install a packaged .vsix (packaging checks)

```bash
npm run install:local
```

This packages `rocky.vsix` and installs it with `--force`, to check nothing is missing from the bundle. `npm run uninstall:local` removes it.

### 3. Symlink (grammar, snippet and schema changes)

```bash
ln -s "$(pwd)" ~/.vscode/extensions/rocky-dev.rocky
```

JSON files (grammar, snippets, schemas, language config) take effect on reload
without a rebuild. Remove the symlink when you are done.

## Testing

| Suite | Where | Runner | Command |
|---|---|---|---|
| Unit | `src/__tests__/`, `webview-ui/**/*.test.{ts,tsx}` | Vitest (jsdom for webviews) | `npm run test:unit` |
| Integration | `src/test/suite/` | Mocha (TDD UI) in a real VS Code via `@vscode/test-electron` | `npm test` |

`npm test` compiles and bundles first (`pretest`) and downloads a VS Code build on the first run. The integration suite checks that the extension activates, that its commands and the `rocky` language are registered, and that the task type exists. Mocha allows 10 seconds per test and 20 seconds for activation. To add one, create a `.test.ts` file in `src/test/suite/`.

## Extension manifest

### Activation, settings, language registration

- The extension activates on `onLanguage:rocky`, on a workspace containing `**/*.rocky` or `rocky.toml`, or when the Get Started, Extension Info or Help view opens.
- `contributes.configuration` declares the settings. The [README](./README.md#settings) lists each with its default.
- Language ID `rocky`, extension `.rocky`. Grammar: `syntaxes/rocky.tmLanguage.json`. Snippets: `snippets/rocky.json`. JSON validation: `rocky.toml` against `schemas/rocky-project.schema.json`, and `*.rocky.toml` against `schemas/rocky-config.schema.json`.

## Modifying the grammar

The TextMate grammar `syntaxes/rocky.tmLanguage.json` highlights comments (`--`), pipeline steps (`from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `take`, `distinct`, `replicate`), keywords (`as`, `on`, `keep`, `asc`, `desc`, join types), literals (strings, numbers, dates `@YYYY-MM-DD`, booleans, null), operators (logical, comparison, arithmetic, arrow `=>`), functions (aggregate, string, numeric, datetime, window) and match expressions.

A DSL syntax change touches the engine parser, the compiler, this grammar, and
the snippets together. See the root [`AGENTS.md`](../../AGENTS.md).

When the server runs, its **semantic tokens** overlay the TextMate grammar.
`themes/rocky-semantic.json` holds their colours.

## Adding snippets

Snippets are in `snippets/rocky.json`: a prefix (trigger), a body and a description, in VS Code snippet syntax (`$1`, `$2` tab stops, `${1|choice1,choice2|}` choices).

## Packaging

```bash
npm run package
```

This creates `rocky-<version>.vsix`. `.vscodeignore` keeps sources, source
maps, `node_modules`, and config files out of the package.

## Main dependencies

`package.json` holds the exact ranges. `vscode-languageclient` is the one runtime dependency. Build: `typescript`, `esbuild`. Webviews: `react`, `react-dom`, `@xyflow/react`, `@dagrejs/dagre`, `tailwindcss`, `@tailwindplus/elements`. Tests: `vitest`, `mocha`, `@vscode/test-electron`. Packaging: `@vscode/vsce`.

## Related projects

- **[Rocky](https://github.com/rocky-data/rocky)**: the engine. It provides the
  CLI and the LSP server.
- **[dagster-rocky](https://github.com/rocky-data/rocky/tree/main/integrations/dagster)**:
  the Dagster integration.
