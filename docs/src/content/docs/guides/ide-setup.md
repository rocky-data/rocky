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

So the extension needs a Rocky binary it can reach. Section 1 installs the extension; section 2 points it at that binary.

![A Rocky DSL model on the left and its compiled SQL on the right, updating live as you type](/demo-vscode-compiled-sql.gif)

The extension source is in the monorepo at [`editors/vscode/`](https://github.com/rocky-data/rocky/tree/main/editors/vscode).

## 1. Install the Extension

Pick one of four methods. Method A suits most users. Methods B, C, and D are for people who work on the extension itself.

### Method A: Install from the VS Code Marketplace

The extension is published on the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky). Install it from VS Code:

1. Open VS Code
2. Go to **Extensions** (**Cmd+Shift+X** / **Ctrl+Shift+X**)
3. Search for **Rocky**
4. Click **Install**

Or install from the command line:

```bash
code --install-extension rocky-data.rocky
```

VS Code updates the extension for you when a new version is published.

### Method B: F5 Development Host (for contributors)

Clone the monorepo and launch the extension in VS Code's Extension Development Host:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/editors/vscode
npm install
npm run compile
```

Open the `editors/vscode` folder in VS Code, then press **F5**. VS Code launches a second window with the extension loaded. It picks up TypeScript changes on the next F5 launch.

### Method C: Install from VSIX

Build a `.vsix` package and install it directly:

```bash
cd rocky/editors/vscode
npm install
npm run compile
npx vsce package
```

This writes a file like `rocky-<version>.vsix`. Install it in VS Code:

```bash
code --install-extension rocky-<version>.vsix
```

Or open VS Code, go to **Extensions** > **...** (three dots menu) > **Install from VSIX** and select the file.

### Method D: Symlink for Local Development

If you change the extension often, symlink the compiled output into VS Code's extensions directory:

```bash
cd rocky/editors/vscode
npm install
npm run compile

# macOS / Linux
ln -s "$(pwd)" ~/.vscode/extensions/rocky-data.rocky-<version>

# Restart VS Code
```

You then skip the VSIX rebuild on every change. Run `npm run compile` after you edit a TypeScript file, then reload the VS Code window (**Cmd+Shift+P** > **Developer: Reload Window**).

## 2. Configure the Rocky Binary Path

The extension starts the language server itself. It looks for a standalone `rocky-lsp` binary first, because the install scripts place `rocky-lsp` next to `rocky`. If it finds none, it runs `rocky lsp` instead. It resolves both names from your `PATH`. If Rocky is installed somewhere else, set the path yourself:

1. Open VS Code Settings (**Cmd+,** / **Ctrl+,**)
2. Search for `rocky.server.path`
3. Set it to the full path of your Rocky binary

```json
{
  "rocky.server.path": "/usr/local/bin/rocky"
}
```

Or in `settings.json`:

```json
{
  "rocky.server.path": "${workspaceFolder}/target/release/rocky"
}
```

### Extra arguments

Pass additional flags to the language server:

```json
{
  "rocky.server.extraArgs": ["--verbose"]
}
```

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

Install the extension and set the binary path, then check that the server starts:

1. Open a Rocky project in VS Code (a directory containing `rocky.toml` or `models/`)
2. Open any `.rocky` or `.sql` file in the `models/` directory
3. Check the status bar at the bottom left -- you should see **Rocky: Ready**

If the status bar shows **Rocky: Failed**, open the Output panel (**View** > **Output** > select **Rocky Language Server** from the dropdown). It carries the error.

## 4. Tour of Features

### Hover Information

Hover over any column name to see its inferred type and source lineage:

- **Column type**: The type the compiler's type checker resolved (for example `Int64`, `String`, `Decimal`)
- **Source lineage**: The upstream model and column this value comes from
- **Intent**: The model's plain-English description, when its TOML config has an `intent` field

Hover works on:
- Column references in SELECT clauses
- Table references in FROM/JOIN clauses
- Model names in Rocky DSL `from` expressions

### Autocompletion

The language server completes what you type, using the compiled project:

- **Column names**: In a SELECT, WHERE, or GROUP BY clause, it suggests columns from the referenced tables
- **Model names**: In a FROM clause, or in `depends_on` in a TOML file, it suggests models in the project
- **SQL functions**: After a function name and `(`, it shows parameter hints
- **Keywords**: It suggests SQL and Rocky DSL keywords that fit the cursor position

Completions appear as you type. Press **Ctrl+Space** to ask for them.

### Go to Definition

**Cmd+Click** (or **F12**) on a model reference to jump to its definition:

- Clicking a model name in a FROM clause opens the model's SQL file
- Clicking a model name in `depends_on` in a TOML file opens that model
- Clicking a column name traces lineage to the upstream model where it originates

### Find All References

**Shift+F12** on a model name shows all places it is referenced:

- Other models that depend on it (via `depends_on`)
- SQL files that reference it in FROM/JOIN clauses

### Rename Symbol

**F2** on a model name renames it across the project. The rename:

- Updates the TOML `name` field
- Updates every `depends_on` reference in other models
- Updates SQL references

:::caution
Rename touches files inside the Rocky project only. Update references from other systems (Dagster assets, CI configs) yourself.
:::

### Diagnostics

Type errors, unresolved references, and warnings appear as you type, after a 300ms pause. The Problems panel (**View** > **Problems**) groups every diagnostic by file.

### Document Symbols

Open the Outline panel (**View** > **Outline**) to see the model structure: model name, intent, columns with types, and CTEs.

### Signature Help

Type a function name followed by `(` to see parameter hints:

```
SUBSTRING(string, start, length)
          ^^^^^^
          active parameter
```

## 5. Inlay Hints

An inlay hint is a type annotation the editor draws inline. Hints show each column's inferred type in your SQL and Rocky DSL files, so you do not have to hover.

Turn hints on or off:

```json
{
  "rocky.inlayHints.enabled": true
}
```

With hints on, the types appear beside the columns:

```sql
SELECT
    order_id,           -- : Int64
    customer_name,      -- : String
    total_amount,       -- : Decimal
    order_date          -- : Date
FROM stg_orders
```

Hints update as you edit.

## 6. The Rocky Inspector

The **Rocky Inspector** is a bottom-panel view. It shows everything Rocky knows about the model in the active editor. Open it from **Cmd+Shift+P** > **Rocky: Open in Inspector**. While the panel is visible it follows the active editor, so switching model files switches the Inspector. Clicking a node in the lineage canvas also retargets it.

![The Rocky Inspector's Overview as a model trust dashboard, its Governance card flagging two classified columns with one left unmasked](/demo-vscode-inspector.gif)

Each tab covers one concern and runs one Rocky CLI command. A tab whose data is not available yet says so rather than failing:

- **Overview** -- cost, blast radius, drift, governance, and freshness for the model in one place
- **Columns** -- the model's columns with inferred types, tracing each one's upstream lineage
- **Lineage** -- the interactive lineage canvas (below)
- **Tests** -- declarative `[[tests]]` assertions plus the model-execution check (`rocky test`)
- **Preview** -- a sample of the model's output rows (`rocky preview rows`); DuckDB runs locally, other warehouses require `rocky.preview.allowWarehouse`
- **Profile** -- per-column profiling of the materialized table (`rocky profile`, DuckDB-only)

### Lineage canvas

The **Lineage** tab draws the project's column-level graph as an interactive canvas. Lineage is the map of which columns feed which, traced through every transformation (see the [glossary](/reference/glossary/)). Open the canvas directly with **Cmd+Shift+P** > **Rocky: Show Model Lineage**, framed on the current model.

- Opens on the current model's neighbourhood, and expands out to the whole project
- Built from `rocky catalog` (assets and dependencies) and `rocky compile` (per-model materialization)
- Draws overlays on the graph itself: cost, freshness, drift, governance, breaking changes against the base ref, and the last run
- Right-click a node for actions scoped to that model: open its file, refocus the graph, or run an AI action -- explain (generate intent), generate tests, draft a data-grounded contract, or build a downstream model

## 7. AI Commands

The extension runs Rocky's AI commands from the Command Palette. Each one needs `ANTHROPIC_API_KEY` set in your environment.

### Generate Model from Intent

**Cmd+Shift+P** > **Rocky: Generate Model from Intent**

An input box opens. Describe the model you want:

```
monthly revenue per customer from the orders table, filtered to 2024
```

Rocky generates the model code, compiles it, and opens it in a new editor tab. If the compile fails, Rocky sends the errors back to the model and retries, up to 3 attempts in total.

### AI via the command line

The extension runs `rocky ai "<intent>"` for you. The same command works in a terminal:

```bash
rocky ai "top 10 customers by lifetime value from customer_orders"
```

### Other AI commands

Three more AI commands work on models you already have. Each writes its result back into the project:

- **Rocky: Sync Models (AI Schema Change Detection)** -- reconcile a model against upstream schema changes, guided by its stored intent (`rocky ai-sync`)
- **Rocky: Explain Model (Generate Intent)** -- write a plain-English `intent` for a model from its code (`rocky ai-explain`)
- **Rocky: Generate Tests from Intent** -- derive `[[tests]]` assertions from a model's intent (`rocky ai-test`)

You can also reach these actions by right-clicking a node in the Inspector's lineage canvas.

### All commands

| Command | Description |
|---|---|
| **Rocky: Generate Model from Intent** | Generate a model from a natural language description (`rocky ai`) |
| **Rocky: Sync Models (AI Schema Change Detection)** | Detect upstream schema changes and propose updates (`rocky ai-sync`) |
| **Rocky: Explain Model (Generate Intent)** | Generate an intent description from a model's code (`rocky ai-explain`) |
| **Rocky: Generate Tests from Intent** | Generate test assertions from a model's intent (`rocky ai-test`) |
| **Rocky: Open in Inspector** | Open the active model in the Rocky Inspector |
| **Rocky: Show Model Lineage** | Open the Inspector's lineage canvas, framed on the current model |
| **Rocky: Restart Language Server** | Restart the language server (fixes stale state) |

Access via the Command Palette (**Cmd+Shift+P** / **Ctrl+Shift+P**).

## 8. File Watchers

The extension watches these file patterns:

| Pattern | Effect |
|---|---|
| `**/*.rocky` | Recompile on save |
| `**/*.toml` | Recompile on save (picks up config and dependency changes) |
| `**/models/**/*.sql` | Recompile on save |

When a watched file changes, the language server recompiles the project incrementally. The Problems panel updates as it goes.

## 9. Troubleshooting

### "Rocky: Failed" in status bar

1. Check that the `rocky` binary exists at the configured path
2. Run `rocky --version` in a terminal to confirm the binary works
3. Open the Output panel and select **Rocky Language Server** to read the error
4. Run `rocky lsp` in a terminal and see whether it starts

### No completions or hover

1. Confirm the workspace root has a `models/` directory or `.rocky` files
2. Run `rocky compile` in a terminal and confirm the project compiles
3. Restart the language server: **Cmd+Shift+P** > **Rocky: Restart Language Server**

### Diagnostics not updating

1. Look at the status bar. An error count there means the server is running
2. Save the file. A save triggers a recompile
3. Restart the language server if the diagnostics stay stale

### Extension not activating

The extension starts when any one of these is true:

- A file with the `.rocky` extension is open
- The workspace contains `**/*.rocky` files
- The workspace contains a `rocky.toml` file

Otherwise it stays inactive.

### Performance with large projects

The first compile of a project with hundreds of models takes a few seconds. Later recompiles are incremental and faster. If the editor feels slow:

1. Time `rocky compile` in a terminal. Over 5 seconds is a sign the project should be split into sub-projects
2. Watch fewer files: set `files.watcherExclude` in VS Code settings
