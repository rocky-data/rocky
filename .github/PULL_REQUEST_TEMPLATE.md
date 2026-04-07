## Summary

<!-- Brief description of what this PR does -->

## Subproject(s) touched

<!-- Tick all that apply -->

- [ ] `engine/` (Rust CLI / crates)
- [ ] `integrations/dagster/` (Python package)
- [ ] `editors/vscode/` (TypeScript extension)
- [ ] `examples/playground/` (sample project / benchmarks)
- [ ] `schemas/` (canonical CLI JSON schema)
- [ ] Cross-project (CI, scripts, root docs)

## Changes

<!-- List the key changes -->

-

## Test Plan

<!-- How were these changes tested? -->

-

## Checklist

- [ ] Commit messages follow [conventional commits](https://www.conventionalcommits.org/), scoped by subproject when relevant (e.g. `feat(engine/rocky-databricks): ...`, `fix(dagster): ...`)
- [ ] No `Co-Authored-By` trailers
- [ ] If this is a CLI JSON schema or DSL syntax change, all consumers (`integrations/dagster/`, `editors/vscode/`) are updated in this same PR

### Engine (`engine/`)
- [ ] `cargo test --all-targets` passes
- [ ] `cargo clippy --all-targets -- -D warnings` is clean
- [ ] `cargo fmt --check` passes

### Dagster (`integrations/dagster/`)
- [ ] `uv run pytest` passes
- [ ] `uv run ruff check` is clean
- [ ] `uv run ruff format --check` passes

### VS Code (`editors/vscode/`)
- [ ] `npm run compile` succeeds
- [ ] `npm run test:unit` passes (electron tests run in CI)
- [ ] `npm run lint` is clean
