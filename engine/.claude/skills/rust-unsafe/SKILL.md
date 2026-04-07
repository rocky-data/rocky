---
name: rust-unsafe
description: `unsafe` conventions in the Rocky engine — SAFETY comment rules, the two legitimate unsafe sites today (mmap + test env-var mutation), and when to push back on new unsafe. Use when auditing, reviewing, or adding any `unsafe` block/function in the engine crates.
---

# `unsafe` in the Rocky engine

## Reality check

Rocky has **very little** `unsafe` on purpose. As of this skill's authoring, there are exactly three files in `engine/crates/` with `unsafe`:

| File | What's unsafe | Why it's justified |
|---|---|---|
| `rocky-core/src/mmap.rs` | `memmap2::Mmap::map(&file)` for project files ≥ 4 KB | Reading 50k+ SQL/TOML files during compile needs mmap for throughput; the race is read-only project data, worst case is a parse error. |
| `rocky-core/src/config.rs` (tests only) | `std::env::{set_var, remove_var}` | These became `unsafe` in Rust 1.82+ because POSIX env mutation is not thread-safe. Test-only, serial. |
| `rocky-cli/src/pipes.rs` (tests only) | `std::env::{set_var, remove_var}` | Same reason. |

**There is no FFI unsafe.** The `duckdb` crate, `jsonwebtoken`, and `rsa` all wrap their underlying C / crypto surfaces in safe APIs — Rocky calls those safe APIs and does not reach into `duckdb_sys` or similar. If you find yourself writing an FFI binding from scratch, **stop** and ask whether the upstream crate has a safe wrapper already.

Keep this list short. Every new `unsafe` site is a reviewer tax and a future soundness bug waiting to happen.

## The `SAFETY:` comment rule

**Every `unsafe` block, function, impl, or trait must be accompanied by a `SAFETY:` comment that states the invariant the caller/author is relying on.** This is non-negotiable and matches the convention across the Rocky codebase.

The comment format:

```rust
// SAFETY: <one-sentence invariant>.
//         <optional follow-up: why the invariant holds here, or the fallback if it doesn't>.
unsafe { ... }
```

Two real examples from the codebase (`rg "SAFETY:" engine/crates/` to see them live):

```rust
// rocky-core/src/mmap.rs:39
// SAFETY: We only read project files that are not being modified
// concurrently during compilation. Worst case: garbled read → parse error.
let mmap = unsafe { memmap2::Mmap::map(&file)? };
```

```rust
// rocky-core/src/config.rs:1088 (test code)
// SAFETY: test-only, no concurrent reads of this variable
unsafe { std::env::set_var("ROCKY_TEST_VAR", "hello_world") };
```

**What a good `SAFETY:` comment does:**

1. States the **invariant** the `unsafe` call depends on (not the API's contract — the thing the caller is promising).
2. Justifies **why that invariant holds here** (data shape, locking, single-threaded context, read-only guarantee, etc.).
3. Mentions the **fallback** if the invariant is violated, when possible (e.g. "worst case is a parse error, not UB").

**What a bad `SAFETY:` comment looks like:**

- `// SAFETY: this is safe` — useless; delete and re-write.
- `// SAFETY: the docs say so` — where? Cite the exact contract.
- `// SAFETY: tested` — tests don't prove memory safety.
- No comment at all — **blocks review**.

## Module-level safety docs

If an entire module's purpose is to wrap unsafe primitives behind a safe interface, document that at the module level using `//! # Safety`. `rocky-core/src/mmap.rs` is the reference:

```rust
//! Memory-mapped file I/O for efficient project loading.
//!
//! # Safety
//!
//! `memmap2::Mmap` is `unsafe` because another process could modify the
//! file while it's mapped. We accept this risk for read-only project
//! loading because:
//! 1. Project files are not modified during compilation.
//! 2. The worst case is a garbled read that fails parsing (not UB).
//! 3. The performance benefit at scale (50k+ files) justifies the trade-off.
```

Follow this shape whenever you introduce a new module containing `unsafe`. The doc-level section complements the inline `SAFETY:` comments — the module-level explains **why this module needs unsafe at all**, the inline explains **why each specific site is sound**.

Public `unsafe fn` declarations also need a `# Safety` section in their doc comment — see the `rust-doc` skill.

## When to push back on new `unsafe`

Before merging a PR that adds new `unsafe`, ask:

1. **Is there a safe alternative?** Most of the time there is — `std::cell::UnsafeCell` → `RefCell`, raw pointer arithmetic → `slice::split_at`, manual bit-twiddling → `bytemuck`, hand-rolled FFI → a safe crate wrapper.
2. **Is the performance win measured?** `mmap.rs` justifies itself with "50k+ files"; speculative performance claims are not enough.
3. **Is the invariant stable?** An invariant that holds today but could be invalidated by an unrelated refactor is a soundness bomb. If you can't describe a test or type-level property that will break loudly when the invariant fails, reconsider.
4. **Who else reads this?** `rocky-core` is the library that everything else links against. A bug there is a bug everywhere — the bar for new `unsafe` in `rocky-core` is higher than in a leaf adapter crate.

If you can't answer all four, leave the safe implementation in place and open a perf issue instead.

## Linting and auditing

### grep surface

```bash
# Find every unsafe site in the engine — run from engine/
rg -n '\bunsafe\b' crates/

# Find every SAFETY: comment (should roughly match the count above)
rg -n 'SAFETY:' crates/

# Find unsafe sites without an adjacent SAFETY: comment (needs human review)
rg -nB1 '\bunsafe\s*\{' crates/ | rg -v 'SAFETY:'
```

If the last command shows a site that doesn't have a `SAFETY:` comment within the surrounding 1-2 lines, that's a bug to fix before merge.

### Lints

Rocky runs `cargo clippy --all-targets -- -D warnings`. The relevant clippy lints for unsafe are:

- `clippy::undocumented_unsafe_blocks` — fires on `unsafe { ... }` without a `SAFETY:` comment. Not in the default set, so it currently doesn't enforce the rule at CI level — but it's the right lint to consider if you want to make the rule a machine-checkable policy. (Adding it to `[workspace.lints]` is a policy change — see the `rust-clippy-triage` skill for the rule on workspace lint changes.)
- `clippy::multiple_unsafe_ops_per_block` — fires if one `unsafe { ... }` block does more than one unsafe operation. Splitting them makes each `SAFETY:` comment tighter.

## Related skills

- **`rust-doc`** — public `unsafe fn` needs a `# Safety` section in its doc comment.
- **`rust-clippy-triage`** — how to react when an unsafe-related lint fires, and why workspace-level lint changes need Hugo review.
- **`rust-style`** — the wildcard-match rule is load-bearing around unsafe too: every enum variant you forget is one you're silently assuming doesn't exist.
