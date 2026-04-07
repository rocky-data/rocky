---
name: rust-style
description: Rocky's Rust coding style — let-else early returns, shadowing, newtypes, enums over bools, no wildcard matches, no noise comments. Use when writing or reviewing Rust code in any engine crate.
---

# Rocky Rust style

Source: partially vendored from [davidbarsky/8fae6dc45c294297db582378284bd1f2](https://gist.github.com/davidbarsky/8fae6dc45c294297db582378284bd1f2) @ `191b2ee46088920de97d682561e2abd1edd64a42` (SKILL-2.md).

## What this skill is and isn't

This is a **cherry-pick** of Barsky's `rust-style` gist, not a verbatim vendor. Barsky himself flags that his gist is personal preference. The rules below are the ones that fit Rocky's existing code; the rules that **conflict with idiomatic Rocky** are deliberately omitted.

### Rules deliberately dropped from the upstream gist

| Upstream rule | Why Rocky doesn't enforce it |
|---|---|
| "Use `for` loops, not iterator chains" | Rocky uses `iter().filter().map().collect()` freely throughout the codebase — it's idiomatic Rust and the codebase is already consistent with it. Swapping to mutable-accumulator loops would be a large and contentious rewrite. |
| "Avoid the `matches!` macro" | `matches!` is the standard Rust idiom for boolean variant checks. Rocky uses it; removing it would produce more verbose code, not clearer code. |
| "Always use explicit destructuring for struct field access" | Too strict for a 20-crate workspace. Dot-access is fine for ad-hoc reads; reserve destructuring for match arms and when all fields are consumed. |

The rules below are the ones that **are** enforced.

## Early returns: use `let ... else`

Use `let ... else` to extract values and exit early on failure. This keeps the happy path unindented.

```rust
// DO
let Some(user) = get_user(id) else {
    return Err(Error::NotFound);
};
let Ok(session) = user.active_session() else {
    return Err(Error::NoSession);
};
// continue with user and session

// DON'T
if let Some(user) = get_user(id) {
    if let Ok(session) = user.active_session() {
        // deeply nested code
    } else {
        return Err(Error::NoSession);
    }
} else {
    return Err(Error::NotFound);
}
```

```rust
// DO
let Some(value) = maybe_value else { continue };
let Ok(parsed) = input.parse::<i32>() else { continue };
```

## Pattern matching: use `if let` only for short, no-else cases

```rust
// ACCEPTABLE: short action, no else
if let Some(callback) = self.on_change {
    callback();
}

// DO: use let-else when you need the value to continue
let Some(config) = load_config() else {
    return default_config();
};

// DO: use match for multiple branches
match result {
    Ok(value) => process(value),
    Err(Error::NotFound) => use_default(),
    Err(e) => return Err(e),
}
```

## Variable naming: shadow, don't rename

Shadow variables through transformations. Avoid prefixes like `raw_`, `parsed_`, `trimmed_`.

```rust
// DO
let input = get_raw_input();
let input = input.trim();
let input = input.to_lowercase();
let input = parse(input)?;

// DON'T
let raw_input = get_raw_input();
let trimmed_input = raw_input.trim();
let lowercase_input = trimmed_input.to_lowercase();
let parsed_input = parse(lowercase_input)?;
```

## Comments: minimise noise

This matches Rocky's global rule from the top-level `CLAUDE.md`: *"Only add comments where the logic isn't self-evident."*

- No inline comments explaining what obvious code does.
- No section headers or dividers (`// --- Section ---`).
- No TODO comments — use the plans backlog.
- No commented-out code — use `git log`.
- **Exception:** doc comments (`///`) on public items are required and follow the `rust-doc` skill. `SAFETY:` comments on `unsafe` blocks are required and follow the `rust-unsafe` skill.

```rust
// DON'T
// Check if user is valid
if user.is_valid() {
    // Update the timestamp
    user.touch();
}

// --- Helper functions ---

// TODO: refactor this later
fn helper() { }

// Old implementation:
// fn old_way() { }

// DO
if user.is_valid() {
    user.touch();
}

fn helper() { }
```

## Type safety: newtypes over raw strings

Wrap strings in newtypes to add semantic meaning and prevent mixing. Rocky already uses this pattern for SQL identifiers and catalog/schema/table names — see `rocky-sql/src/validation.rs`.

```rust
// DO
struct TenantId(String);
struct CatalogName(String);

fn create_catalog(tenant: TenantId, catalog: CatalogName) { }

// DON'T
fn create_catalog(tenant: String, catalog: String) { }
```

When to **not** create a newtype: transient locals that never escape a function, or values that are immediately passed through unchanged. Don't cargo-cult this rule.

## Type safety: enums over bools

Use enums with meaningful variant names instead of `bool` parameters.

```rust
// DO
enum Visibility {
    Public,
    Private,
}
fn create_repo(name: &str, visibility: Visibility) { }

// DON'T
fn create_repo(name: &str, is_public: bool) { }
```

```rust
// DO — already used in Rocky for materialization strategies
enum RefreshMode {
    FullRefresh,
    Incremental,
}

// DON'T
fn run_pipeline(full: bool) { }
```

## Pattern matching: never wildcard-match an enum you own

Always match all variants explicitly so the compiler errors when a variant is added. This is load-bearing for Rocky because `MaterializationStrategy`, `PipelineType`, and the various `*Output` enums all grow over time and we want the compiler to find every site that needs updating.

```rust
// DO
match strategy {
    MaterializationStrategy::FullRefresh => handle_full(),
    MaterializationStrategy::Incremental => handle_incremental(),
    MaterializationStrategy::Merge => handle_merge(),
    MaterializationStrategy::MaterializedView => handle_mv(),
    MaterializationStrategy::DynamicTable => handle_dt(),
    MaterializationStrategy::TimeInterval => handle_ti(),
}

// DON'T
match strategy {
    MaterializationStrategy::FullRefresh => handle_full(),
    _ => handle_other(),
}
```

**Exception:** wildcard-matching an enum you **don't** own (e.g. `std::io::ErrorKind`, which is `#[non_exhaustive]`) is fine and often required. The rule is about enums defined inside the Rocky workspace.

If a wildcard seems unavoidable for a Rocky-owned enum, **stop and reconsider** — it usually means a variant-specific case was overlooked.

## Code navigation: prefer rust-analyzer LSP

When searching or navigating Rust code in the 20-crate workspace, prefer LSP operations over raw text search — they respect type resolution and paths:

- `goToDefinition` — find where a symbol is defined
- `findReferences` — find all references (respects re-exports)
- `hover` — type info and documentation
- `documentSymbol` — all symbols in a file
- `goToImplementation` — find trait implementations (especially useful for the `Adapter` trait family)

For structural refactors, see the `rust-analyzer-ssr` skill.

## Related skills

- **`rust-doc`** — RFC 1574 doc comment conventions for public items
- **`rust-error-handling`** — `thiserror` (lib) vs `anyhow` (bin) decision tree
- **`rust-async-tokio`** — Tokio, `#[async_trait]`, AIMD concurrency
- **`rust-unsafe`** — `SAFETY:` comment conventions for the DuckDB and crypto FFI surfaces
- **`rust-clippy-triage`** — playbook when `cargo clippy -D warnings` fires
