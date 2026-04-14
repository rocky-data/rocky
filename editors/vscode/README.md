# Rocky VS Code Extension

Language support for the [Rocky](https://github.com/rocky-data/rocky) SQL transformation engine.

## Features

- **Syntax highlighting** for `.rocky` files (TextMate grammar + semantic tokens from LSP)
- **Diagnostics** — compile errors and warnings as you type
- **Hover** — column types, lineage trace, model schemas
- **Go to Definition** — jump to model files or upstream column definitions
- **Find References** — locate all usages of a model or column across the project
- **Rename** — rename models or columns across all files
- **Completion** — model names, column names, SQL functions
- **Signature Help** — function parameter hints with active parameter tracking
- **Document Symbols** — outline view with model, columns, and CTEs
- **Code Actions** — quick fixes for diagnostics (fuzzy suggestions, CAST wrapping)
- **Inlay Hints** — inferred types shown inline after expressions
- **Semantic Tokens** — rich highlighting for model references, columns, functions
- **Code Snippets** — `from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `match`, `window`, `model`
- **Lineage View** — `Rocky: Show Model Lineage` renders the DAG as an interactive graph
- **AI Generate** — `Rocky: Generate Model from Intent` creates a model from natural language
- **Status Bar** — shows server status and error count

## Prerequisites

- **Rocky CLI** installed and on your `PATH` (or configure `rocky.server.path`)
- **VS Code** 1.85.0 or later

## Local Development (without publishing)

There are three ways to use this extension locally:

### Option 1: Run in Extension Development Host (recommended for development)

This launches a separate VS Code window with the extension loaded.

```bash
cd editors/vscode
npm install
npm run compile
```

Then press **F5** in VS Code (or **Run > Start Debugging**). This opens a new VS Code window — the "Extension Development Host" — with the Rocky extension active. Any `.rocky` files opened in that window will have full language support.

To set this up, create `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Run Extension",
      "type": "extensionHost",
      "request": "launch",
      "args": ["--extensionDevelopmentPath=${workspaceFolder}"]
    }
  ]
}
```

### Option 2: Install from VSIX (recommended for daily use)

Package the extension as a `.vsix` file and install it directly:

```bash
cd editors/vscode
npm install
npm run compile
npx @vscode/vsce package
```

This creates `rocky-0.1.0.vsix`. Install it:

```bash
code --install-extension rocky-0.1.0.vsix
```

Or in VS Code: **Extensions** > **...** menu > **Install from VSIX...** > select the `.vsix` file.

To update after making changes, rebuild and reinstall:

```bash
npm run compile && npx @vscode/vsce package && code --install-extension rocky-0.1.0.vsix
```

### Option 3: Symlink into VS Code extensions directory

Create a symlink from the VS Code extensions directory to your local clone:

```bash
ln -s "$(pwd)/editors/vscode" ~/.vscode/extensions/rocky-dev.rocky-0.1.0
```

Then restart VS Code. This is the least friction for iteration — changes to JSON files (grammar, snippets, language config) take effect on reload. TypeScript changes still require `npm run compile`.

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

## Project Structure

```
editors/vscode/
├── package.json                    # Extension manifest
├── tsconfig.json                   # TypeScript config
├── language-configuration.json     # Brackets, comments, folding
├── src/
│   └── extension.ts                # LSP client, commands, status bar
├── syntaxes/
│   └── rocky.tmLanguage.json       # TextMate grammar
├── snippets/
│   └── rocky.json                  # Code snippets
├── schemas/
│   └── rocky-config.schema.json    # JSON Schema for .rocky.toml
├── themes/
│   └── rocky-semantic.json         # Semantic token colors
└── icons/
    └── rocky-icon.svg              # File icon
```

## License

Apache-2.0
