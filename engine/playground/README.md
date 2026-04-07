# Rocky Playground

In-browser compiler explorer for Rocky's WASM bindings.

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

| Button | WASM function | Input | Output |
|--------|--------------|-------|--------|
| **Compile SQL** | `compile_sql` | SQL SELECT statement | Column-level lineage (JSON) |
| **Parse Rocky** | `parse_rocky` | Rocky DSL source | AST (JSON) |
| **Lower to SQL** | `lower_rocky_to_sql` | Rocky DSL source | Generated SQL |
| **Validate** | `validate_identifier` | Any string | Valid / invalid |

### Keyboard shortcuts

- **Ctrl/Cmd + Enter** -- run Compile SQL
- **Tab** -- insert two spaces (no focus jump)

### Snippet picker

The dropdown loads pre-built examples for each action so you can try things
immediately without typing.

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
