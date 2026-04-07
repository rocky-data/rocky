---
title: IDE Setup
description: Install and configure the Rocky VS Code extension for type-aware editing, lineage visualization, and AI generation
sidebar:
  order: 3
---

The Rocky VS Code extension connects to the Rocky language server to provide real-time compilation, type-aware hover, go-to-definition, column-level lineage, and AI model generation directly in your editor.

The extension source is at [github.com/rocky-data/rocky/tree/main/editors/vscode](https://github.com/rocky-data/rocky/tree/main/editors/vscode), inside the Rocky monorepo.

## 1. Install the Extension

There are three ways to install the extension, depending on your workflow.

### Method A: F5 Development Host (for contributors)

Clone the Rocky monorepo and launch the extension in VS Code's Extension Development Host:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/editors/vscode
npm install
npm run compile
```

Open the `editors/vscode/` folder in VS Code, then press **F5**. This launches a new VS Code window with the extension loaded. Changes to the TypeScript source are picked up on the next F5 launch.

### Method B: Install from VSIX

Build a `.vsix` package and install it directly:

```bash
cd rocky/editors/vscode
npm install
npm run compile
npx vsce package
```

This produces a file like `rocky-0.1.0.vsix`. Install it in VS Code:

```bash
code --install-extension rocky-0.1.0.vsix
```

Or open VS Code, go to **Extensions** > **...** (three dots menu) > **Install from VSIX** and select the file.

### Method C: Symlink for Local Development

If you are iterating on the extension frequently, symlink the compiled output into VS Code's extensions directory:

```bash
cd rocky/editors/vscode
npm install
npm run compile

# macOS / Linux
ln -s "$(pwd)" ~/.vscode/extensions/rocky-dev.rocky-0.1.0

# Restart VS Code
```

This avoids rebuilding a VSIX on every change. Run `npm run compile` after editing TypeScript files, then reload the VS Code window (**Cmd+Shift+P** > **Developer: Reload Window**).

## 2. Configure the Rocky Binary Path

The extension launches the Rocky language server by running `rocky lsp`. By default, it looks for `rocky` on your `PATH`. If Rocky is installed elsewhere, configure the path:

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

## 3. Verify the Connection

After installing the extension and configuring the binary path:

1. Open a Rocky project in VS Code (a directory containing `rocky.toml` or `models/`)
2. Open any `.rocky` or `.sql` file in the `models/` directory
3. Check the status bar at the bottom left -- you should see **Rocky: Ready**

If the status bar shows **Rocky: Failed**, check the Output panel (**View** > **Output** > select **Rocky Language Server** from the dropdown) for error details.

## 4. Tour of Features

### Hover Information

Hover over any column name to see its inferred type and source lineage:

- **Column type**: The resolved type from the compiler's type checker (e.g., `Int64`, `String`, `Decimal`)
- **Source lineage**: Which upstream model and column this value comes from
- **Intent**: If the model has an `intent` field in its TOML config, the hover shows the plain-English description

Hover works on:
- Column references in SELECT clauses
- Table references in FROM/JOIN clauses
- Model names in Rocky DSL `from` expressions

### Autocompletion

The language server provides context-aware completions:

- **Column names**: When typing in a SELECT, WHERE, or GROUP BY clause, the server suggests columns from the referenced tables
- **Model names**: When typing a FROM clause or `depends_on` in TOML, the server suggests available models in the project
- **SQL functions**: After typing a function name and `(`, the server shows parameter hints
- **Keywords**: SQL and Rocky DSL keywords are suggested based on cursor position

Completions are triggered automatically as you type. Press **Ctrl+Space** to trigger them manually.

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

**F2** on a model name renames it across the project:

- Updates the TOML `name` field
- Updates all `depends_on` references in other models
- Updates SQL references

:::caution
Rename only updates files within the Rocky project. If the model is referenced by external systems (Dagster assets, CI configs), update those manually.
:::

### Diagnostics

Type errors, unresolved references, and warnings appear as you type with a 300ms debounce. The Problems panel (**View** > **Problems**) shows all diagnostics grouped by file.

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

Inlay hints display inline type annotations in your SQL and Rocky DSL files. They show the inferred type of each column without hovering.

Enable or disable inlay hints:

```json
{
  "rocky.inlayHints.enabled": true
}
```

When enabled, you see type annotations inline:

```sql
SELECT
    order_id,           -- : Int64
    customer_name,      -- : String
    total_amount,       -- : Decimal
    order_date          -- : Date
FROM stg_orders
```

Inlay hints update in real time as you edit.

## 6. Lineage View

The extension includes a lineage visualization panel. Open it via:

- **Cmd+Shift+P** > **Rocky: Show Model Lineage**

This opens a side panel with an interactive DAG rendered from the compiler's column-level lineage graph. The visualization:

- Shows upstream and downstream models
- Highlights column-level data flow with edges
- Uses the Graphviz DOT format rendered via viz.js
- Updates when you switch between model files

The lineage view runs `rocky lineage <model> --format dot` under the hood and renders the result as an SVG in a webview panel.

## 7. AI Commands

The extension exposes AI features directly in the editor. These require `ANTHROPIC_API_KEY` to be set in your environment.

### Generate Model from Intent

**Cmd+Shift+P** > **Rocky: Generate Model from Intent**

This opens an input box where you describe the model you want:

```
monthly revenue per customer from the orders table, filtered to 2024
```

Rocky generates the model code, compiles it to verify correctness, and opens it in a new editor tab. If compilation fails, it retries with the error context (up to 3 attempts).

### AI via the command line

The extension runs `rocky ai "<intent>"` under the hood. You can also use this directly from the terminal:

```bash
rocky ai "top 10 customers by lifetime value from customer_orders"
```

### All commands

| Command | Description |
|---|---|
| **Rocky: Restart Language Server** | Restart the LSP (fixes stale state) |
| **Rocky: Show Model Lineage** | Open lineage DAG visualization |
| **Rocky: Generate Model from Intent** | AI model generation from natural language |

Access via the Command Palette (**Cmd+Shift+P** / **Ctrl+Shift+P**).

## 8. File Watchers

The extension watches for changes to these file types:

| Pattern | Effect |
|---|---|
| `**/*.rocky` | Recompile on save |
| `**/*.toml` | Recompile on save (picks up config and dependency changes) |
| `**/models/**/*.sql` | Recompile on save |

The language server recompiles the project incrementally when any watched file changes. Diagnostics (errors and warnings) update in real time in the Problems panel.

## 9. Troubleshooting

### "Rocky: Failed" in status bar

1. Check that the `rocky` binary is installed and accessible at the configured path
2. Run `rocky --version` in the terminal to verify the binary works
3. Open the Output panel and select **Rocky Language Server** to see the error
4. Try running `rocky lsp` manually in a terminal to see if it starts

### No completions or hover

1. Make sure you have a `models/` directory (or `.rocky` files) in the workspace root
2. Check that the project compiles: run `rocky compile` in the terminal
3. Restart the language server: **Cmd+Shift+P** > **Rocky: Restart Language Server**

### Diagnostics not updating

1. Check the status bar -- if it shows an error count, the server is running
2. Try saving the file (auto-compile triggers on save)
3. If diagnostics are stale, restart the language server

### Extension not activating

The extension activates when it detects:
- A file with the `.rocky` extension is open
- A workspace contains `**/*.rocky` files
- A workspace contains a `rocky.toml` file

If none of these conditions are met, the extension remains inactive.

### Performance with large projects

For projects with hundreds of models, the initial compilation may take a few seconds. Subsequent recompilations are incremental and fast. If the editor feels sluggish:

1. Check `rocky compile` time in the terminal -- if it takes more than 5 seconds, the project may benefit from splitting into sub-projects
2. Reduce the number of watched files by configuring `files.watcherExclude` in VS Code settings
