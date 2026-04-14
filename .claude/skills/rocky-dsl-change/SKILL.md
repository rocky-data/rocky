---
name: rocky-dsl-change
description: Rocky DSL (`.rocky` file) cross-subproject cascade. Use when adding/changing a Rocky DSL keyword, operator, or pipeline step — changes must land in the engine parser, compiler, VS Code TextMate grammar, and snippets together in one PR.
---

# Rocky DSL syntax cascade

Rocky has its own transformation language as an alternative to SQL: `.rocky` files with a `.toml` sidecar. The language has four pieces that live in different subprojects and MUST be changed in lockstep — otherwise the IDE experience and the engine diverge.

## When to use this skill

- Adding, removing, or renaming a pipeline step keyword (`from`, `where`, `group`, `derive`, `select`, `join`, `sort`, `take`, `distinct`, `replicate`, …)
- Changing operator semantics (e.g. the `!= → IS DISTINCT FROM` rule)
- Adding a new literal kind, type, or syntactic construct
- Changing how sidecar `.toml` metadata is parsed

## The four files that must move together

| # | Subproject | File | What it owns |
|---|---|---|---|
| 1 | `engine/crates/rocky-lang/` | `src/token.rs`, `src/parser.rs`, `src/lower.rs` | Lexer (logos), parser, and lowering to the IR |
| 2 | `engine/crates/rocky-compiler/` | `src/` (type checking, semantic graph) | Type checking, contract validation, diagnostics |
| 3 | `editors/vscode/syntaxes/` | `rocky.tmLanguage.json` | TextMate grammar — syntax highlighting in VS Code |
| 4 | `editors/vscode/snippets/` | `rocky.json` | Snippet triggers users type to insert keywords |

All four live under `engine/` and `editors/vscode/` in the same monorepo, so they ship in one PR with one CI run — which is the whole point of the monorepo.

## Canonical workflow

1. **Define the syntax** in the engine lexer and parser.
   - Add a token in `engine/crates/rocky-lang/src/token.rs` (logos-derived enum).
   - Extend `engine/crates/rocky-lang/src/parser.rs` to accept it.
   - Update `engine/crates/rocky-lang/src/lower.rs` to lower it to the IR (`rocky-core::ir`).
2. **Type-check it** in `engine/crates/rocky-compiler/src/`.
   - Add or extend the semantic-graph node.
   - Add a diagnostic code (E0xx / W0xx) if there's a new failure mode.
3. **Highlight it** in `editors/vscode/syntaxes/rocky.tmLanguage.json`.
   - Add the keyword to the appropriate `match` pattern. TextMate grammars are pure regex with scope names — the scope name should match an existing family (`keyword.control.rocky`, `keyword.operator.rocky`, etc.) so existing themes color it.
4. **Offer a snippet** in `editors/vscode/snippets/rocky.json` when the keyword is something users would want to autocomplete with a body.
5. **Update the spec doc**: `docs/src/content/docs/rocky-lang-spec.md` is the canonical Rocky DSL spec. Keep it in sync — it's published via the Astro docs site.
6. **Add a test**:
   - Engine side: a unit test in `rocky-lang` or an integration test in `rocky-compiler` that parses and type-checks a snippet using the new syntax.
   - VS Code side: for non-trivial grammar changes, add a case to `editors/vscode/src/` vitest tests if one exists, or smoke-test in the Extension Development Host (F5).
7. **Run all checks**:
   ```bash
   just test     # cargo test + pytest + vitest
   just lint     # cargo clippy/fmt + ruff + eslint
   ```

## Commit style

Commits scoped by subproject or crate — but land them together in one PR:

```
feat(engine/rocky-lang): add `pivot` pipeline step
feat(engine/rocky-compiler): type-check pivot step
feat(vscode): highlight + snippet for pivot step
docs(engine): document pivot in rocky-lang-spec
```

Or, for a focused change, one combined commit is fine:

```
feat(rocky-dsl): add `pivot` pipeline step across engine + vscode
```

## The NULL-safe-equality example

The canonical reminder case in `engine/CLAUDE.md`:

> **Key difference from SQL:** `!=` compiles to `IS DISTINCT FROM` (NULL-safe).

If you changed that rule, you'd touch:
- `engine/crates/rocky-lang/src/parser.rs` to recognize the operator.
- `engine/crates/rocky-compiler/` to fold into the IR.
- `engine/crates/rocky-sql/src/transpile.rs` to emit dialect-specific SQL.
- `editors/vscode/syntaxes/rocky.tmLanguage.json` so the operator highlights as an operator, not an error.
- `docs/src/content/docs/rocky-lang-spec.md` to document the semantics.

## Common pitfalls

- **Grammar-only changes without a parser change** — VS Code will highlight the keyword but `rocky compile` will reject it. Users get a confusing experience.
- **Parser-only changes without a grammar change** — the file runs fine but users see it un-highlighted, which makes it feel broken.
- **Forgetting `.toml` sidecar parsing** — model metadata lives in a sibling `.toml`; if your new syntax requires a new metadata field, update `engine/crates/rocky-core/src/models.rs`.
- **Forgetting the spec doc** — it's the only public reference for users; keep it accurate.

## Reference files

- `CLAUDE.md` (monorepo root) — "When modifying Rocky DSL syntax" cascade rules.
- `engine/CLAUDE.md` — "Rocky DSL (.rocky files)" section for the current pipeline step list.
- `docs/src/content/docs/rocky-lang-spec.md` — the published language spec.
