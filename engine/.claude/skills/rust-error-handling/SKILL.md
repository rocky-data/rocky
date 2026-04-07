---
name: rust-error-handling
description: Decision tree for thiserror (library crates) vs anyhow (CLI/binary) in the Rocky engine workspace. Use when adding a new error type, adding a new `From` impl, deciding where to attach context, or triaging how an error surfaces to Dagster/JSON output.
---

# Rust error handling in Rocky

Rocky uses a **two-tier** error handling strategy. The rule, from `engine/CLAUDE.md` § Coding Standards:

> Use `thiserror` for library errors, `anyhow` for binary/CLI errors.

This skill is the decision tree: when to reach for which, what `From` impls to derive, how to attach context, and how errors flow out of the binary into Dagster JSON.

## Which crates use which

| Layer | Crate | Error crate | Why |
|---|---|---|---|
| Library | `rocky-core`, `rocky-sql`, `rocky-compiler`, `rocky-lang`, `rocky-adapter-sdk`, `rocky-databricks`, `rocky-snowflake`, `rocky-fivetran`, `rocky-duckdb`, `rocky-cache`, `rocky-engine`, `rocky-ai`, `rocky-observe`, `rocky-server` | `thiserror` | Library errors need to be matchable by callers, namable, and stable. |
| Binary / CLI | `rocky-cli`, `rocky` | `anyhow` | The CLI is the top of the call stack — we don't care about matching errors, we care about printing them with context. |

If a crate imports both `thiserror` and `anyhow`, that's usually a smell — check whether the library's error enum is leaking into the binary uselessly, or whether an `anyhow::Error` is being stashed into a `#[error(transparent)]` variant.

## Library crates: `thiserror` pattern

Every library crate defines its errors as a named enum with `#[derive(Debug, thiserror::Error)]`. The `#[error("…")]` attribute provides the `Display` impl; `#[from]` derives `From` for automatic `?` conversion.

**Concrete examples already in the codebase:**

- `crates/rocky-core/src/hooks/mod.rs` — `HookError` with variants for command-failed / timeout / aborted / I/O / serialization / webhook (uses `#[from]` to chain `std::io::Error`, `serde_json::Error`, and `WebhookError`)
- `crates/rocky-core/src/hooks/webhook.rs` — `WebhookError` with variants for request-failed / timeout / HTTP-status / template / HTTP-client
- `crates/rocky-core/src/circuit_breaker.rs` — `CircuitBreakerError` with a single variant carrying `consecutive_failures`, `threshold`, and `last_error`
- `crates/rocky-databricks/src/{catalog,permissions,workspace}.rs` — each module owns a small error enum scoped to its concerns

**Template:**

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FooError {
    /// Context-bearing message — include the values that make this specific.
    #[error("foo failed for {key}: {reason}")]
    Failed { key: String, reason: String },

    /// Transparent wrap of a child error — use when Foo is a thin shim and
    /// the caller should see the child error's Display directly.
    #[error(transparent)]
    Inner(#[from] BarError),

    /// External crate errors go behind `#[from]` so `?` works.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, FooError>;
```

**Rules:**

1. Error enums are **crate-local**. Don't define a "one error to rule them all" at workspace level — the enums are supposed to be small and scoped. `rocky-core` has separate errors per subsystem (`HookError`, `WebhookError`, `CircuitBreakerError`, `ModelError`, etc.).
2. Every `#[error("...")]` message starts **lowercase**, does **not** end with a period, and includes the values that make this specific instance distinguishable from other instances of the same variant (see the `hook command failed: {command} (exit code {exit_code})` style).
3. Use `#[from]` for automatic `?` conversion when there's **only one reasonable way** the child error could arise in this variant. If the same child type could come from two different code paths, split them into two variants and use `.map_err(...)` at the call site instead.
4. Use `#[error(transparent)]` only when the wrapper adds zero information — i.e. when it's literally a newtype pass-through.
5. Derive `Debug` always. Do **not** derive `Clone` on errors unless a test harness or a background-task broadcast specifically needs it; cloning errors usually means they're being held past their useful lifetime.
6. Don't add an `#[error("other: {0}")] Other(String)` catch-all. That's an `anyhow` smell leaking into a library — if the library can't name the error, it probably shouldn't be producing it.

## Binary / CLI: `anyhow` pattern

`rocky-cli` and the `rocky` binary use `anyhow::Result<T>` as the return type for command handlers. Errors are built from:

- Library errors bubbling up through `?` (anyhow auto-converts anything `Debug + Display + Send + Sync + 'static`)
- `.context("...")` / `.with_context(|| format!("..."))` to attach extra information at each layer
- `anyhow::bail!("...")` / `anyhow::ensure!(cond, "...")` for inline failures

**Template:**

```rust
use anyhow::{Context, Result, bail, ensure};

pub fn run(config_path: &Path) -> Result<RunOutput> {
    let config = rocky_core::config::load(config_path)
        .with_context(|| format!("failed to load config at {}", config_path.display()))?;

    ensure!(!config.pipelines.is_empty(), "config has no pipelines");

    let Some(adapter) = config.adapters.get("default") else {
        bail!("no default adapter configured");
    };

    adapter.run()
        .context("pipeline run failed")
        // ↑ top-level context. Each library-level .context() below adds a layer.
}
```

**Rules:**

1. **Always attach context.** A bare `?` in a CLI handler throws away the "what were we trying to do" half of the story. Wrap every library call with `.context("...")` or `.with_context(|| ...)`. Use `with_context` when the message needs runtime data (it's lazy), plain `context` when it's a static string (it's cheaper).
2. `bail!` and `ensure!` are fine at command-handler level for preconditions that aren't worth their own library error type. Don't use them inside library crates.
3. Don't `.unwrap()` or `.expect()` outside tests. The CLI should always return an error up to `main.rs`, which formats it with the full chain.
4. Don't downcast anyhow errors unless you genuinely need to take a different action based on a specific root cause — it's rare and usually means the library should have exposed a typed predicate instead (e.g. `is_rate_limit()` on `rocky-databricks/src/connector.rs`).

## How errors surface to Dagster

The CLI emits JSON on stdout for every `--output json` invocation. When a handler returns `Err(anyhow::Error)`:

1. `rocky-cli/src/main.rs` prints the error chain to **stderr** with all `.context()` layers.
2. The process exits with code **1** (hard failure) or **2** (partial success — some tables failed, but a valid `RunOutput` JSON was still written to stdout).
3. The Dagster integration at `integrations/dagster/` reads the exit code and (for code 2) the partial-success JSON. It has `allow_partial=True` handling specifically for this case.

**Implication for error messages:** the Dagster integration surfaces your `.context("...")` strings to the Dagster event log. Make them operator-readable. `"failed to load config at /path/rocky.toml"` is useful; `"load failed"` is not.

**Implication for library errors:** if a library error variant needs to be distinguishable by Dagster (e.g. "transient, retry" vs "permanent, alert"), expose a **predicate method** on the error enum — don't make Dagster pattern-match on stringified messages. See `rocky-databricks/src/connector.rs::is_transient` and `is_rate_limit` for the canonical shape.

## Common mistakes

| Mistake | Fix |
|---|---|
| New library crate imports `anyhow::Result` in its public API. | Swap to a `thiserror` enum. `anyhow` belongs to the CLI. |
| `.unwrap()` in a non-test code path. | Use `?` with a proper library error or `.context()` in the CLI. |
| `#[error("Failed")]` with no context. | Include the runtime values — `#[error("failed to parse {file}: {message}")]`. |
| A `From<std::io::Error>` impl on an error that has **two** places I/O can arise. | Split into two variants; use `.map_err(...)` at each call site so the variant names where the I/O happened. |
| `Other(String)` catch-all variant in a library error. | Delete it. If you genuinely don't know the error type, the caller also doesn't — bubble up the concrete child error instead. |
| `eprintln!("error: {:?}", e)` in the CLI. | Return `Err(e)` from the handler. `main.rs` has a single error-printing path that handles the chain formatting. |

## Related skills

- **`rust-doc`** — public library errors need `# Errors` sections on the functions that return them.
- **`rust-async-tokio`** — async errors flow through the same two-tier model (library returns `Result<_, FooError>`, CLI wraps with `anyhow`).
- **`rocky-codegen`** (monorepo root) — when an error shape ends up in a `*Output` struct, the codegen cascade produces Pydantic + TypeScript bindings for it.
