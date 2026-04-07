---
name: rust-async-tokio
description: Tokio, #[async_trait], AIMD adaptive concurrency, and tracing spans in the Rocky engine. Use when adding async code to an adapter, adding backpressure to a REST client, choosing between spawn/join/select, or adding structured logging to an async hot path.
---

# Async Rust in Rocky

Rocky is **async end-to-end**. Tokio is the only runtime; there's a single `#[tokio::main]` entrypoint at `engine/rocky/src/main.rs:471`. Adapter I/O (Databricks REST, Snowflake REST, Fivetran REST, Valkey, webhooks) all runs on the same runtime.

## Runtime setup

- **Workspace dep** — `tokio = { version = "1", features = ["full"] }` (from `engine/Cargo.toml`). The `"full"` feature is deliberate: we use `macros`, `rt-multi-thread`, `time`, `fs`, `net`, `sync`, and `process` across the workspace, and pinning a narrower feature set per crate creates friction when async code moves between crates.
- **Entrypoint** — `engine/rocky/src/main.rs:471` has `#[tokio::main]` on `async fn main() -> anyhow::Result<()>`. Library crates never spawn their own runtime — they take `&self` on `async` methods and trust the binary to drive the reactor.
- **Library crates depend on the workspace** `tokio` re-export. Don't `cargo add tokio` in a sub-crate; inherit from `[workspace.dependencies]` in `engine/Cargo.toml`.

## Async trait rule

Every async trait uses `#[async_trait]` from the `async-trait` crate (`async-trait = "0.1"` in `[workspace.dependencies]`). This is a hard rule from `engine/CLAUDE.md`.

```rust
// DO — matches the pattern used throughout rocky-adapter-sdk
#[async_trait::async_trait]
pub trait WarehouseAdapter: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<StatementResult, AdapterError>;
    async fn describe_table(&self, qname: &QualifiedName) -> Result<TableSchema, AdapterError>;
}

// DON'T — native async-in-traits still has object-safety gaps in 2024 edition
pub trait WarehouseAdapter: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<StatementResult, AdapterError>;
}
```

Concrete examples: `crates/rocky-adapter-sdk/src/traits.rs` has four `#[async_trait]` trait definitions at lines 219, 333, 346, 372 — those are the shapes adapters must match.

When in doubt, read how `rocky-databricks` or `rocky-fivetran` implement the trait — both are full end-to-end examples.

## Backpressure: the AIMD pattern

Rocky uses adaptive concurrency on remote calls that can be rate-limited. The canonical implementation is `crates/rocky-databricks/src/throttle.rs::AdaptiveThrottle`:

- **Slow start** — below half of max, increase by 2 every `increase_interval` successes.
- **Congestion avoidance** — above half of max, increase by 1.
- **Multiplicative decrease** — on `on_rate_limit()` (triggered by 429 / 503 / "TEMPORARILY_UNAVAILABLE"), halve the current concurrency down to `min_concurrency`.
- **Clamped** — never goes below `min_concurrency` (≥ 1) or above `max_concurrency`.
- **Atomic shared state** — the inner counters are `AtomicUsize` + `AtomicU64` inside an `Arc<ThrottleInner>`, so `.clone()` shares state across tasks. No locks on the hot path.

**When to reach for this pattern:** any new adapter that talks to a rate-limited remote API. Don't re-invent it; either reuse `AdaptiveThrottle` directly (if it's a Databricks-family API) or copy its shape. The tests at `crates/rocky-databricks/src/throttle.rs:122` cover the invariants you'd want to preserve (starts at max, halves on rate limit, never below min, never above max, clone shares state).

**How to wire it:**

1. Create one `AdaptiveThrottle` per **warehouse/endpoint** at adapter construction.
2. Acquire a permit via a `tokio::sync::Semaphore` whose `available_permits()` is refreshed from `throttle.current()` before each batch.
3. On the result of each request:
   - 2xx → `throttle.on_success()`
   - 429 / 503 → `throttle.on_rate_limit()` and retry after a small delay
   - anything else → neither (it's a user error, not a concurrency signal)

## Structured logging: `tracing`, not `println!`

Hard rule from `engine/CLAUDE.md`: **use `tracing`, not `println!` or `eprintln!`**. The subscriber is initialized in `crates/rocky-observe/src/tracing_setup.rs` and emits structured JSON lines when `RUST_LOG` / `tracing_subscriber::EnvFilter` is set.

```rust
use tracing::{info, warn, error, debug};

// DO — structured fields, no interpolation in the message
info!(
    connector_id = %connector.id,
    table_count = tables.len(),
    "discover completed"
);

warn!(
    from = old,
    to = new,
    "adaptive throttle: rate limit detected, reducing concurrency"
);

// DON'T — stringify values into the message
info!("discover completed: connector={}, tables={}", connector.id, tables.len());
```

- `%value` → uses `Display`
- `?value` → uses `Debug`
- Bare `field = literal` → treats as JSON literal

`warn!` is the level `rocky-databricks/src/throttle.rs` uses for rate-limit detection — follow that pattern for any adaptive-concurrency event you add.

## Timeouts

Any network call needs a timeout. Use `tokio::time::timeout`:

```rust
use std::time::Duration;
use tokio::time::timeout;

let result = timeout(Duration::from_secs(30), client.execute(sql))
    .await
    .context("databricks execute timed out after 30s")??;
//                                                      ^^ one ? for Elapsed → anyhow, one ? for the inner Result
```

Rule of thumb: if a function makes an HTTP/SQL/network call and does not have an outer timeout, it's a bug waiting to happen. The `anyhow` `.context` layer is what surfaces the timeout reason into the Dagster event log — don't skip it.

## Cancellation and `select!`

`tokio::select!` is the right tool when you want to race two futures (e.g. "wait for this statement to finish **or** for the user to ctrl-C"). Guidelines:

- Every branch must be cancel-safe. Read the `tokio::select!` docs on which tokio primitives are cancel-safe and which aren't. (Hint: `AsyncRead::read_buf` is **not** cancel-safe.)
- Prefer `tokio::sync::oneshot` for "signal this future to stop" over manual flags.
- Don't hold a `MutexGuard` across an `.await` in any `select!` branch.

## Spawn vs. join

| Situation | Use |
|---|---|
| Fan out N tasks and wait for all | `futures::future::try_join_all` |
| Fan out N tasks, take first result | `futures::future::select_ok` |
| Fire-and-forget background worker | `tokio::spawn(...)` — but the spawned future must own its data (`'static`), and you must handle its `JoinHandle` if it can fail |
| Parallel CPU-bound chunks | `tokio::task::spawn_blocking` — **not** `spawn`. Blocking the reactor starves other adapters. |

`duckdb` and `sqlparser` operations are **CPU-bound** and should be wrapped in `spawn_blocking` if they're called from an async context on a hot path.

## What NOT to do

| Anti-pattern | Why |
|---|---|
| `std::thread::spawn` in async code | Bypasses the runtime; task never wakes correctly. Use `tokio::spawn` or `spawn_blocking`. |
| `std::sync::Mutex` held across `.await` | Can deadlock the reactor. Use `tokio::sync::Mutex` or (better) restructure to avoid holding the lock. |
| Per-crate `#[tokio::main]` or nested runtimes | There's exactly one runtime, driven by `engine/rocky/src/main.rs`. Libraries don't own the runtime. |
| `futures::executor::block_on` inside async code | Nested block_on will panic under `tokio::main`. |
| `async fn foo(...) -> Box<dyn Future<...>>` | Use `#[async_trait]` for trait methods or `impl Future` for free functions. |
| Spinning on `throttle.current()` in a busy loop | Drive concurrency off a `Semaphore`; the throttle is a signal, not a gate. |

## Related skills

- **`rust-error-handling`** — async errors follow the same two-tier model (library → `thiserror`, CLI → `anyhow` with `.context`).
- **`rust-clippy-triage`** — the `async`-family clippy lints (e.g. `clippy::unused_async`, `clippy::await_holding_lock`) fire in this surface area.
- **`rust-unsafe`** — `duckdb` calls are sync/FFI and need `spawn_blocking`, not direct `await`.
