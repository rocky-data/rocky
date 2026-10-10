# Development Guide

This page covers how to build, run, and test the extension from source. For
coding rules and the architecture notes agents follow, read
[`AGENTS.md`](./AGENTS.md).

## Prerequisites

- **Node.js 22** and **npm**. CI uses Node 22.
- **VS Code 1.120.0+**
- **[Rocky CLI](https://github.com/rocky-data/rocky)** on `$PATH`. The extension
  spawns it as the language server.

## Getting started

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/editors/vscode
npm install
npm run compile
npm run bundle
```

Then press **F5** in VS Code. It opens an Extension Development Host with the
extension loaded.

## How the build fits together

```
  src/**/*.ts ──► tsc (npm run compile) ──► out/       tests run from here
       │
       └──────► esbuild (npm run bundle) ──► dist/extension.js   "main" entry

  webview-ui/**/*.tsx ──► esbuild + Tailwind ──► webview bundles (Inspector)
```

`package.json` points `main` at `./dist/extension.js`, so the dev host runs the
esbuild bundle. Run `npm run bundle` (or `npm run bundle:watch`) after a
change, then reload the dev host. `vscode:prepublish` runs both steps.

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

`package.json` (`contributes.commands`) is the source of truth for commands.
The extension contributes 61 of them. The [README](./README.md#commands) lists
the common ones.

## Architecture

### LSP client (`lspClient.ts`)

On activation the extension spawns the language server:

```
rocky lsp [extraArgs...]
```

It talks JSON-RPC over **stdio**. The binary path and extra arguments come
from `rocky.server.path` and `rocky.server.extraArgs`.

- **Document selector**: `.rocky` files, and `.sql` files under `**/models/**`.
- **File watchers**: `**/*.rocky`, `**/*.toml`, `**/models/**/*.sql`.

The server provides diagnostics, hover, go-to-definition, find references,
rename, completion, signature help, document symbols, code actions, inlay
hints, and semantic tokens.

### Commands

Command handlers live in `src/commands/`, one file per concern, registered in
`src/commands/index.ts`. They use `cp.execFile()`, never `exec()`, and
`vscode.window.withProgress()` for long operations.

### Inspector and lineage canvas

`rocky.showLineage` opens the Rocky Inspector, a bottom-panel React webview
from `webview-ui/`, on its Lineage tab. The canvas is built from
`rocky catalog` and `rocky compile` JSON. It renders with `@xyflow/react` and
lays out with `@dagrejs/dagre`. Overlays show cost, freshness, drift,
governance, breaking changes, and the last run. Right-click a node for scoped
AI actions.

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

Press **F5** (Run > Start Debugging). The configuration is in
`.vscode/launch.json`. After a TypeScript change, rebuild with
`npm run bundle` and reload the dev host (Ctrl+Shift+P > "Developer: Reload
Window").

### 2. Install a packaged .vsix (packaging checks)

```bash
npm run install:local
```

This packages `rocky.vsix` and installs it with `--force`. Use it to check that
nothing is missing from the bundle. `npm run uninstall:local` removes it.

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

`npm test` compiles and bundles first (`pretest`). It downloads a VS Code build
on the first run. The integration suite checks that the extension activates,
that its commands and the `rocky` language are registered, and that the task
type exists. Mocha uses a 10-second timeout per test. Suite setup allows 20
seconds for activation.

To add an integration test, create a `.test.ts` file in `src/test/suite/`. The
loader picks up every compiled `**.test.js` file.

## Extension manifest

### Activation

The extension activates when:

- a `.rocky` file opens (`onLanguage:rocky`);
- the workspace contains `**/*.rocky` or `rocky.toml`;
- the Get Started, Extension Info, or Help view opens.

### Settings

`contributes.configuration` declares the settings. The
[README](./README.md#settings) lists each one with its default.

### Language registration

- Language ID: `rocky`. File extension: `.rocky`.
- Grammar: `syntaxes/rocky.tmLanguage.json`.
- Snippets: `snippets/rocky.json`.
- JSON validation: `rocky.toml` against `schemas/rocky-project.schema.json`, and
  `*.rocky.toml` against `schemas/rocky-config.schema.json`.

## Modifying the grammar

The TextMate grammar is `syntaxes/rocky.tmLanguage.json`. It highlights:

- comments (`--`);
- pipeline steps (`from`, `where`, `group`, `derive`, `select`, `join`, `sort`,
  `take`, `distinct`, `replicate`);
- keywords (`as`, `on`, `keep`, `asc`, `desc`, join types);
- literals (strings, numbers, dates `@YYYY-MM-DD`, booleans, null);
- operators (logical, comparison, arithmetic, arrow `=>`);
- functions (aggregate, string, numeric, datetime, window);
- match expressions.

A DSL syntax change touches the engine parser, the compiler, this grammar, and
the snippets together. See the root [`AGENTS.md`](../../AGENTS.md).

When the server runs, its **semantic tokens** overlay the TextMate grammar.
`themes/rocky-semantic.json` holds their colours.

## Adding snippets

Snippets are in `snippets/rocky.json`. Each one has a prefix (the trigger
text), a body (a template with tab stops), and a description. Use VS Code
snippet syntax: `$1`, `$2` for tab stops, `${1|choice1,choice2|}` for choices.

## Packaging

```bash
npm run package
```

This creates `rocky-<version>.vsix`. `.vscodeignore` keeps sources, source
maps, `node_modules`, and config files out of the package.

## Main dependencies

`package.json` holds the exact ranges.

| Package | Purpose |
|---|---|
| `vscode-languageclient` | LSP client library (the one runtime dependency) |
| `typescript` | compiler |
| `esbuild` | bundler for the extension and the webviews |
| `react`, `react-dom`, `@xyflow/react`, `@dagrejs/dagre` | Inspector webviews and the lineage canvas |
| `tailwindcss`, `@tailwindplus/elements` | webview styling and headless components |
| `vitest`, `mocha`, `@vscode/test-electron` | test runners |
| `@vscode/vsce` | packaging |

## Related projects

- **[Rocky](https://github.com/rocky-data/rocky)**: the engine. It provides the
  CLI and the LSP server.
- **[dagster-rocky](https://github.com/rocky-data/rocky/tree/main/integrations/dagster)**:
  the Dagster integration.
