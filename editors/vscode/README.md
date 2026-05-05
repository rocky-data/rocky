<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="media/rocky-readme-dark.svg" />
    <img src="media/rocky-readme-light.svg" alt="Rocky" />
  </picture>
</p>

# Rocky VS Code Extension

Language support for the [Rocky](https://github.com/rocky-data/rocky) SQL transformation engine — full LSP, lineage visualization, and AI model generation.

## Features

**Editor intelligence** — diagnostics, hover, go-to-definition, find references, rename, code actions, signature help, document symbols, and inlay hints for inferred column types.

**Syntax** — `.rocky` TextMate grammar + semantic tokens, plus code snippets for every DSL construct (`from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `match`, `window`).

**Lineage view** — `Rocky: Show Model Lineage` renders the column-level DAG as an interactive graph.

**AI generate** — `Rocky: Generate Model from Intent` creates a model from a natural language description using the Rocky AI intent layer.

**Status bar** — LSP server status and live error count.

## Requirements

- **[Rocky CLI](https://github.com/rocky-data/rocky/releases?q=engine)** on your `PATH` (or set `rocky.server.path`)
- **VS Code** 1.116.0+

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

| Command | Description |
|---------|-------------|
| `Rocky: Restart Language Server` | Restart the LSP server |
| `Rocky: Show Model Lineage` | Open lineage graph for the current model |
| `Rocky: Generate Model from Intent` | Create a model from a natural language description |

## Contributing

Local development setup, architecture notes, and testing commands live in [`DEVELOPMENT.md`](./DEVELOPMENT.md).

## License

[Apache 2.0](../../LICENSE)
