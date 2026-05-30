<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rocky-data/rocky/main/editors/vscode/media/rocky-readme-dark.png" />
    <img src="https://raw.githubusercontent.com/rocky-data/rocky/main/editors/vscode/media/rocky-readme-light.png" alt="Rocky" />
  </picture>
</p>

# Rocky VS Code Extension

Editor support for [Rocky](https://github.com/rocky-data/rocky), the typed graph between your code and your warehouse: a real LSP backed by the Rocky compiler, interactive column-level lineage, inline compile-time diagnostics, and AI model generation gated through the compiler.

## In action

**See the SQL behind the DSL.** Open any model and reveal its compiled SQL side by side, including the transformations Rocky applies for you (here `!=` becomes a null-safe `IS DISTINCT FROM`, and the post-aggregate filter becomes `HAVING`).

<p align="center"><img src="https://raw.githubusercontent.com/rocky-data/rocky/main/editors/vscode/media/demo-compiledSql.gif" alt="A Rocky DSL model on the left and its compiled SQL on the right, updating live" width="820" /></p>

**Inspect any model.** The Inspector is a model trust dashboard in the bottom panel: an Overview (cost, blast radius, drift, governance, freshness), Columns, an interactive Lineage canvas, Tests, Preview, and per-column Profile. Here a PII-classified model flags a column left unmasked, the Columns tab traces each column's upstream lineage, and the Lineage canvas shows the model's neighborhood with a cost overlay.

<p align="center"><img src="https://raw.githubusercontent.com/rocky-data/rocky/main/editors/vscode/media/demo-inspector.gif" alt="The Rocky Inspector touring a PII-classified model: an Overview trust dashboard with a red Governance card, Columns, the lineage canvas with a cost overlay, Tests, and per-column Profile" width="820" /></p>

**Drive it from the keyboard.** Every Rocky command is one palette away.

<p align="center"><img src="https://raw.githubusercontent.com/rocky-data/rocky/main/editors/vscode/media/demo-quickstart.gif" alt="Opening the command palette filtered to the Rocky commands" width="820" /></p>

## Features

**Editor intelligence**: diagnostics, hover, go-to-definition, find references, rename, code actions, signature help, document symbols, and inlay hints for inferred column types.

**Syntax**: `.rocky` TextMate grammar + semantic tokens, plus code snippets for every DSL construct (`from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `match`, `window`).

**Activity bar sidebar**: Get Started, Extension Info, Models, Runs, Sources, and Help panels. Workspaces without a `rocky.toml` show orientation and one-click actions for Initialize Project, Try Playground, and Open Documentation instead of CLI errors.

**Lineage view**: `Rocky: Show Model Lineage` renders the column-level DAG as an interactive graph.

**AI generate**: `Rocky: Generate Model from Intent` creates a model from a natural language description using the Rocky AI intent layer.

**Status bar**: LSP server status and live error count.

## Requirements

- **[Rocky CLI](https://github.com/rocky-data/rocky/releases?q=engine)** on your `PATH` (or set `rocky.server.path`)
- **VS Code** 1.120.0+

## Install

From the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky):

1. Open the Extensions view (`Ctrl+Shift+X` / `Cmd+Shift+X`).
2. Search for **Rocky**.
3. Install and reload.

The extension spawns `rocky lsp` on startup and attaches it as the language server for `.rocky` files.

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `rocky.server.path` | `"rocky"` | Path to the Rocky binary |
| `rocky.server.extraArgs` | `[]` | Extra arguments passed to `rocky lsp` |
| `rocky.inlayHints.enabled` | `true` | Show inferred types inline |

## Commands

A subset of the most common commands; see `Rocky: Open Command Palette` for the full list (58 commands across compile, run, AI, lineage, branch, preview, and ops).

| Command | Description |
|---------|-------------|
| `Rocky: Initialize Rocky Project` | Scaffold a new Rocky project in the current workspace |
| `Rocky: Try the Playground` | Create the self-contained DuckDB playground |
| `Rocky: Restart Language Server` | Restart the LSP server |
| `Rocky: Show Model Lineage` | Open lineage graph for the current model |
| `Rocky: Generate Model from Intent` | Create a model from a natural language description |
| `Rocky: Doctor` | Run health checks; results render in a webview |

## Contributing

Local development setup, architecture notes, and testing commands live in [`DEVELOPMENT.md`](./DEVELOPMENT.md).

## License

[Apache 2.0](../../LICENSE)
