# Rocky Playground

In-browser compiler explorer for Rocky's WASM bindings.

This page is not the `rocky playground` command. That command writes a sample
DuckDB project to disk. This page runs the compiler in the browser and touches
no warehouse.

## Prerequisites

- Rust toolchain with `wasm32-unknown-unknown` target
- `wasm-pack` (install via `cargo install wasm-pack`)

## Build and serve

```bash
# 1. Build the WASM package
cd engine && bash ../scripts/build_wasm.sh

# 2. Copy the pkg/ output into the playground directory
cp -r crates/rocky-wasm/pkg playground/pkg

# 3. Serve locally (any static HTTP server works)
cd playground && python3 -m http.server 8080
```

Open <http://localhost:8080> in your browser.

## Features

The page has four mode tabs: **SQL**, **Rocky DSL**, **Transpile** and
**Identifier**. Each tab enables its own action buttons and disables the
others. The button in bold in the table is the tab's primary action.

| Tab | Button | WASM function | Output |
|-----|--------|---------------|--------|
| SQL | **Compile SQL** | `compile_sql` | Column-level lineage (JSON) |
| Rocky DSL | Parse Rocky | `parse_rocky` | AST (JSON) |
| Rocky DSL | Lower to SQL | `lower_rocky_to_sql` | Generated SQL |
| Rocky DSL | Check Syntax | `get_parse_errors` | The first parse error, or none |
| Rocky DSL | Format | `format_rocky` | Formats the editor source in place |
| Rocky DSL | **Compile Model** | `compile_rocky_model` | Parse, lower and lineage in one call |
| Transpile | **Transpile** | `transpile_sql` | SQL in the target dialect, with warnings |
| Identifier | **Validate** | `validate_identifier` | Valid / invalid |

The Transpile tab shows a source and a target dialect picker: Snowflake,
Databricks, BigQuery and DuckDB. **Clear Output** empties the output panel. On
the Rocky DSL tab, the page also checks the source for parse errors 500 ms after
you stop typing.

### Keyboard shortcuts

- **Ctrl/Cmd + Enter** -- run the primary action of the current tab
- **Tab** -- insert two spaces (no focus jump)

### Snippet picker

The dropdown lists sample snippets for the current tab. Picking one switches to
its tab and loads it into the editor.

## Architecture

Single self-contained `index.html` -- no build step, no npm, no bundler.
Loads WASM via ES module import from `./pkg/rocky_wasm.js` (the output of
`wasm-pack build --target web`). If the module is not found, the UI
gracefully displays a "WASM not built yet" message with build instructions.

## Troubleshooting

**"WASM not built yet"** -- You need to run `build_wasm.sh` first and copy
the `pkg/` directory into `playground/`. See the build steps above.

**CORS errors when opening `index.html` directly** -- ES module imports
require an HTTP server; `file://` protocol will not work. Use any static
server (`python3 -m http.server`, `npx serve`, etc.).
