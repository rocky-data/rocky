---
title: IDE Setup
description: Install the Rocky VS Code extension, point it at the Rocky binary, and use hover types, lineage, and the AI commands.
sidebar:
  order: 4
---

The Rocky VS Code extension gives you compile errors, column types, lineage, and the AI commands inside the editor. It does none of that work itself. It starts a **language server** (a background Rocky process that speaks the Language Server Protocol) and asks it every question:

```
  VS Code                    stdio (LSP)        Rocky engine
  ┌─────────────────┐                       ┌──────────────────────┐
  │ Rocky extension │ ─── you type ───────► │ rocky-lsp            │
  │ editor + panels │                       │ (or `rocky lsp`)     │
  │                 │ ◄── diagnostics, ──── │ recompiles the       │
  └─────────────────┘     types, lineage    │ project incrementally│
                                            └──────────────────────┘
```

The extension needs a Rocky binary it can reach. Without an editor, use the [browser UI](/guides/browser-ui/) (`rocky serve --ui`).

![A Rocky DSL model on the left and its compiled SQL on the right, updating live as you type](/demo-vscode-compiled-sql.gif)

The source is at [`editors/vscode/`](https://github.com/rocky-data/rocky/tree/main/editors/vscode).

## 1. Install the Extension

Method A suits most users. Methods B, C and D are for people who work on the extension.

### Method A: Install from the VS Code Marketplace

The extension is published on the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky). Search for **Rocky** in **Extensions** (**Cmd+Shift+X** / **Ctrl+Shift+X**) and click **Install**. Or run:

```bash
code --install-extension rocky-data.rocky
```

VS Code updates the extension for you.

### Methods B-D: build it yourself

Clone the monorepo and build the extension:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/editors/vscode
npm install
npm run compile
```

- **B, F5 host:** open `editors/vscode` in VS Code and press **F5** for a second window with the extension loaded.
- **C, VSIX:** run `npx vsce package`, then `code --install-extension rocky-<version>.vsix` (or **Extensions** > **...** > **Install from VSIX**).
- **D, symlink:** run `ln -s "$(pwd)" ~/.vscode/extensions/rocky-data.rocky-<version>` (macOS and Linux), then restart VS Code. After a TypeScript edit, run `npm run compile` and reload the window (**Developer: Reload Window**).

## 2. Configure the Rocky Binary Path

The extension starts the language server itself. It looks for a standalone `rocky-lsp` binary first (the install scripts place it next to `rocky`). If it finds none, it runs `rocky lsp`. It resolves both from your `PATH`. If Rocky is installed elsewhere, set `rocky.server.path` in Settings (**Cmd+,** / **Ctrl+,**) or `settings.json`:

```json
{
  "rocky.server.path": "${workspaceFolder}/target/release/rocky"
}
```

### Extra arguments

`rocky.server.extraArgs` adds arguments after `rocky lsp`. The standalone `rocky-lsp` binary ignores its arguments, so this setting has effect only when the extension falls back to `rocky lsp`.

### All extension settings

| Setting | Default | Description |
|---|---|---|
| `rocky.server.path` | `"rocky"` | Path to the Rocky binary |
| `rocky.server.extraArgs` | `[]` | Extra arguments passed to `rocky lsp` |
| `rocky.inlayHints.enabled` | `true` | Show inferred column types inline |
| `rocky.diagnostics.enabled` | `true` | Show inline compile errors and warnings (set `false` to silence all Rocky diagnostics without uninstalling) |
| `rocky.costAnnotations.enabled` | `true` | Show inline per-model cost annotations above model files (fetched from `rocky optimize`) |
| `rocky.statusBar.segments` | `[]` | Extra status-bar segments after the server state. Any of `warehouse`, `lastRunAge`, `driftCount`, `branchState` |
| `rocky.preview.rowLimit` | `100` | Maximum rows returned by **Rocky: Preview Model Rows** |
| `rocky.preview.allowWarehouse` | `false` | Allow row previews to run against a non-DuckDB warehouse (may incur query cost; each run is confirmed) |

## 3. Verify the Connection

Open a Rocky project (a directory containing `rocky.toml` or `models/`), then open any `.rocky` or `.sql` file in `models/`. The status bar at the bottom left shows **Rocky: Ready**. If it shows **Rocky: Failed**, open **View** > **Output** > **Rocky Language Server** for the error.

## 4. Tour of Features

| Feature | How | What it does |
|---|---|---|
| Hover | Hover a column, a table in FROM/JOIN, or a model in a DSL `from` | Inferred type (`Int64`, `String`, `Decimal`), upstream model and column, and the model's `intent` when set. |
| Autocompletion | **Ctrl+Space** | Column names (SELECT, WHERE, GROUP BY), model names (FROM, `depends_on`), SQL function parameter hints, SQL and DSL keywords. |
| Go to Definition | **Cmd+Click** or **F12** | On a model name, opens the model. On a column, traces lineage to the upstream model where it originates. |
| Find All References | **Shift+F12** on a model name | Models that depend on it and SQL files that reference it. |
| Rename Symbol | **F2** on a model name | Renames the TOML `name`, every `depends_on` and SQL reference. |
| Diagnostics | Automatic, after a 300ms pause | Type errors, unresolved references and warnings, grouped by file in the Problems panel. |
| Document Symbols | **View** > **Outline** | Model name, intent, columns with types, CTEs. |
| Signature Help | Type a function name and `(` | Parameter hints such as `SUBSTRING(string, start, length)`. |

Rename touches files inside the Rocky project only: update Dagster assets, CI configs and other systems yourself.

## 5. Inlay Hints

An inlay hint is a type annotation the editor draws inline, so you do not have to hover. Toggle hints with `rocky.inlayHints.enabled`. They update as you edit:

```sql
SELECT
    order_id,           -- : Int64
    customer_name,      -- : String
    total_amount,       -- : Decimal
    order_date          -- : Date
FROM stg_orders
```

## 6. The Rocky Inspector

The **Rocky Inspector** is a bottom-panel view of everything Rocky knows about the model in the active editor. Open it with **Cmd+Shift+P** > **Rocky: Open in Inspector**. It follows the active editor, and a click on a lineage node retargets it.

![The Rocky Inspector's Overview as a model trust dashboard, its Governance card flagging two classified columns with one left unmasked](/demo-vscode-inspector.gif)

Each tab runs one Rocky CLI command. A tab with no data yet says so:

- **Overview** -- cost, blast radius, drift, governance, and freshness for the model in one place
- **Columns** -- the model's columns with inferred types, tracing each one's upstream lineage
- **Lineage** -- the interactive lineage canvas (below)
- **Tests** -- declarative `[[tests]]` assertions plus the model-execution check (`rocky test`)
- **Preview** -- a sample of the model's output rows (`rocky preview rows`); DuckDB runs locally, other warehouses require `rocky.preview.allowWarehouse`
- **Profile** -- per-column profiling of the materialized table (`rocky profile`)

### Lineage canvas

The **Lineage** tab draws the project's column-level graph (which columns feed which, through every transformation; see the [glossary](/reference/glossary/)). Open it directly with **Cmd+Shift+P** > **Rocky: Show Model Lineage**, framed on the current model.

- Opens on the current model's neighbourhood, and expands out to the whole project
- Built from `rocky catalog` (assets and dependencies) and `rocky compile` (per-model materialization)
- Draws overlays on the graph itself: cost, freshness, drift, governance, breaking changes against the base ref, and the last run
- Right-click a node for actions scoped to that model: open its file, refocus the graph, or run an AI action -- explain (generate intent), generate tests, draft a data-grounded contract, or build a downstream model

## 7. AI Commands

The extension runs Rocky's AI commands from the Command Palette. Each one needs `ANTHROPIC_API_KEY` set in your environment.

### Generate Model from Intent

**Cmd+Shift+P** > **Rocky: Generate Model from Intent** opens an input box. Describe the model, for example `monthly revenue per customer from the orders table, filtered to 2024`. Rocky generates the model, compiles it (retrying with the errors up to 3 attempts) and opens it in a new tab. It runs `rocky ai "<intent>"`, which also works in a terminal. The other AI commands act on existing models and write the result back to the project. Right-click a node in the Inspector's lineage canvas to reach them.

<span id="all-commands"></span>

### Main commands

Type `Rocky:` in the Command Palette to see every command.

| Command | Description |
|---|---|
| **Rocky: Generate Model from Intent** | Generate a model from a natural language description (`rocky ai`) |
| **Rocky: Sync Models (AI Schema Change Detection)** | Detect upstream schema changes and propose updates (`rocky ai-sync`) |
| **Rocky: Explain Model (Generate Intent)** | Generate an intent description from a model's code (`rocky ai-explain`) |
| **Rocky: Generate Tests from Intent** | Generate test assertions from a model's intent (`rocky ai-test`) |
| **Rocky: Open in Inspector** | Open the active model in the Rocky Inspector |
| **Rocky: Show Model Lineage** | Open the Inspector's lineage canvas, framed on the current model |
| **Rocky: Restart Language Server** | Restart the language server (fixes stale state) |

## 8. File Watchers

The language server asks the editor to watch these file patterns:

| Pattern | Effect |
|---|---|
| `**/*.rocky` | Recompile on save |
| `**/*.toml` | Recompile on save (picks up config and dependency changes) |
| `**/models/**/*.sql` | Recompile on save |

A change to a watched file recompiles the project incrementally.

The server registers these patterns at start for any LSP client that supports `workspace.didChangeWatchedFiles.dynamicRegistration`, as VS Code does. Without it, an edit to a file that is not open (such as `_defaults.toml` or `groups/*.toml`) shows only at the next recompile. To test another editor, edit a `_defaults.toml` and see whether the diagnostics update.

## 9. Troubleshooting

For "Rocky: Failed", no completions, or stale diagnostics, see [Troubleshooting](/advanced/troubleshooting/#lsp-and-ide-problems). Quick checks:

1. Run `rocky --version` and `rocky lsp` in a terminal to confirm the binary starts at the configured path.
2. Read the **Rocky Language Server** output panel.
3. Confirm the workspace root has a `models/` directory or `.rocky` files, and that `rocky compile` passes.
4. Save the file to trigger a recompile, or run **Rocky: Restart Language Server**.

### Extension not activating

The extension starts when any one of these is true:

- A file with the `.rocky` extension is open
- The workspace contains `**/*.rocky` files
- The workspace contains a `rocky.toml` file

Otherwise it stays inactive.

### Performance with large projects

The first compile of a project with hundreds of models takes a few seconds. Later recompiles are incremental. If the editor feels slow:

1. Time `rocky compile` in a terminal. Over 5 seconds is a sign the project should be split into sub-projects
2. Watch fewer files: set `files.watcherExclude` in VS Code settings
