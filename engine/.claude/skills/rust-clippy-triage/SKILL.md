---
name: rust-clippy-triage
description: Playbook for when `cargo clippy -- -D warnings` fires in the Rocky engine. Use when triaging a new clippy failure, deciding between fix/allow/refactor, choosing where to put `#[allow(...)]`, or thinking about adding a workspace-level lints table.
---

# Clippy triage for the Rocky engine

## The CI rule

`.github/workflows/engine-ci.yml:62` runs:

```
cargo clippy --all-targets -- -D warnings
```

That means **any** clippy warning in **any** target (lib, bin, tests, examples, benches) fails CI. There is no per-lint exception list, no `clippy.toml`, and **no `[workspace.lints]` table** in `engine/Cargo.toml` (verified). The policy is "default clippy, zero warnings."

## When CI goes red — triage order

When clippy fires, work through this in order. Don't jump to step 4.

### 1. Read the lint name and the message

Clippy output starts with the lint name, e.g. `warning: this could be a `match`` followed by `[#[warn(clippy::single_match)]]`. That name is load-bearing — it tells you which lint group, what the fix usually looks like, and what docs to read. The full docs are at `https://rust-lang.github.io/rust-clippy/master/#/<lint_name>`.

### 2. Does the fix make the code better?

Most clippy lints are genuinely useful. If the suggested fix makes the code clearer or shorter, **just take it** — that's the default path for the vast majority of triage.

Common "just fix it" lints in Rocky-shaped code:
- `clippy::redundant_clone` — remove the `.clone()`
- `clippy::needless_borrow` — remove the `&`
- `clippy::needless_collect` — drop the `.collect::<Vec<_>>()` before the iterator consumer
- `clippy::let_and_return` — return directly
- `clippy::single_match` → `if let`
- `clippy::or_fun_call` → `.unwrap_or_else(|| …)` for side-effecting defaults
- `clippy::unnecessary_wraps` — return `T` instead of `Option<T>` / `Result<T, _>`

### 3. Is the fix wrong for your case?

Sometimes clippy is wrong for a specific call site. Classic examples:

- `clippy::needless_pass_by_value` on a function that's part of an `#[async_trait]` trait impl — the ownership is load-bearing for the trait signature.
- `clippy::too_many_arguments` on a function whose arguments are all required config. The right response is usually a newtype (see `rust-style`), **not** `#[allow]`.
- `clippy::large_enum_variant` on a SQL AST enum — splitting the large variant behind `Box` would pessimise hot-path matching. Box the variant or allow, with a reason.
- `clippy::await_holding_lock` when the lock is a `tokio::sync::Mutex` (it's cancel-safe and designed for this). Clippy can't always tell `std::sync::Mutex` from `tokio::sync::Mutex` by type name — check which one is actually held.

In those cases, **allow at the tightest scope with a `reason = "…"`**:

```rust
// DO — allow at the function, with a reason clippy-watchers can evaluate
#[allow(
    clippy::too_many_arguments,
    reason = "all args are required config and a struct-wrapper hurts call-site readability"
)]
pub fn build_plan(
    catalog: &str,
    schema: &str,
    table: &str,
    strategy: MaterializationStrategy,
    watermark: Option<Instant>,
    // ...
) -> Plan { ... }
```

- Scope the `#[allow]` to the **function** or **block**, never the crate or the module, unless the lint is a categorical mismatch (see step 4).
- Always include `reason = "..."` — an unexplained `#[allow]` is a future maintenance trap.
- Never `#[allow(clippy::all)]` or `#[allow(warnings)]`.

### 4. The lint is consistently wrong for Rocky

If the same lint keeps getting `#[allow]`'d across the workspace with the same reason, that's the signal to add a **workspace-level policy** instead of per-site allows. The right mechanism is a `[workspace.lints]` table in `engine/Cargo.toml`:

```toml
[workspace.lints.clippy]
# Example — DO NOT add without Hugo review.
# large_enum_variant = "allow"  # SQL AST enums intentionally have size-asymmetric variants.
```

**Rules:**

- **Never** add `[workspace.lints]` on your own. Propose it to Hugo first — the project deliberately has no lint table today, and adding one is a policy change, not a fix.
- If you do end up adding one, each entry needs a comment explaining why, with a link to at least one example that triggered it.
- Crate-specific lint policy goes in that crate's `Cargo.toml` under `[lints]`, not in `[workspace.lints]` — workspace-level is for rules that apply to every crate.

### 5. The lint fires in generated code

`rocky-cli/src/output.rs` derives `JsonSchema`, `Serialize`, `Deserialize` on a lot of types. Clippy sometimes fires on the **expansion** of those derives. If you can't silence it at source, you have two choices:

- Put the `#[allow(...)]` on the derived type definition (clippy usually honors this).
- Adjust the struct shape so the derive doesn't emit the problem in the first place.

Never silence lints globally just because generated code trips them in one place — that hides real issues elsewhere.

## Local iteration loop

```bash
# From inside engine/:
cargo clippy --all-targets                 # see warnings without -D (fast iterate)
cargo clippy --all-targets -- -D warnings  # same command CI runs
cargo clippy -p rocky-core --all-targets   # scope to one crate while iterating
cargo clippy --fix --allow-dirty           # auto-apply safe suggestions (review the diff!)
```

`--fix` is safe for most lints but **always review the diff** before committing — it will happily change semantics for lints like `clippy::collapsible_if`. Don't run it on a dirty working tree without committing your in-progress work first.

## Formatter gotcha

`rustfmt` runs independently of clippy. CI has:

```
cargo fmt -- --check
```

…which fails CI just as reliably as clippy does. Before pushing, always run:

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
```

`cargo fmt` has no per-file config in Rocky — it uses the default `rustfmt.toml` behavior. Don't introduce a `rustfmt.toml` without Hugo review for the same reason as `[workspace.lints]`: it's a policy change.

## Related skills

- **`rust-style`** — the reason a lint fires is often that the code isn't following Rocky style; fix the style, not the lint.
- **`rust-error-handling`** — the `clippy::result_large_err` lint fires if a `Result<T, E>` has a large `E`; the fix is usually boxing the error variant, not allowing the lint.
- **`rust-dep-hygiene`** — when clippy complains about a deprecated API from a dependency, sometimes the right fix is a dep bump, not an allow.
