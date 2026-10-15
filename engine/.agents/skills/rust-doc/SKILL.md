---
name: rust-doc
description: Rust documentation conventions (RFC 1574) for public items in the Rocky engine. Use when writing or reviewing doc comments on anything `pub` in the engine crates — summary sentences, section headings, type references, examples, Errors/Panics/Safety sections.
---

# Rust doc conventions for Rocky

Source: vendored from [davidbarsky/8fae6dc45c294297db582378284bd1f2](https://gist.github.com/davidbarsky/8fae6dc45c294297db582378284bd1f2) @ `191b2ee46088920de97d682561e2abd1edd64a42` (SKILL-1.md).
Author: David Barsky (rust-analyzer / tokio-rs `tracing` contributor). RFC 1574 is the canonical Rust API Guidelines source.

## Where this applies in Rocky

These rules apply to **every `pub` item** in the engine. In practice that's concentrated in the public-API crates:

- `rocky-core` — generic SQL transformation engine (IR, Plan, MaterializationStrategy, DAG, catalog, checks, contracts, config)
- `rocky-adapter-sdk` — public adapter traits for third-party warehouse adapters
- `rocky-sql` — parser, dialect, validation, transpile, lineage
- `rocky-lang` / `rocky-compiler` — DSL surface that's consumed by the LSP
- `rocky-cli` output types (`*Output` structs in `crates/rocky-cli/src/output.rs`) — these are part of the JSON schema contract, so their doc comments flow into the Pydantic + TypeScript generated bindings via `schemars`. Under-documented output fields mean under-documented Dagster types.

Internal-only `pub(crate)` items don't need the full treatment, but a summary sentence is always cheap and worth keeping.

## Summary Sentence

Every doc comment starts with a single-line summary sentence.

```rust
// DO: third person singular present indicative, ends with period
/// Returns the length of the string.
/// Creates a new instance with default settings.
/// Parses the input and returns the result.

// DON'T: imperative, missing period, or verbose
/// Return the length of the string
/// This function creates a new instance with default settings.
/// Use this to parse the input and get the result back.
```

## Comment Style

Use line comments, not block comments.

```rust
// DO
/// Summary sentence here.
///
/// More details if needed.

// DON'T
/**
 * Summary sentence here.
 *
 * More details if needed.
 */
```

Use `//!` only for crate-level and module-level docs at the top of the file.

## Section Headings

Use these exact headings (always plural):

```rust
/// Summary sentence.
///
/// # Examples
///
/// # Panics
///
/// # Errors
///
/// # Safety
///
/// # Aborts
///
/// # Undefined Behavior
```

```rust
// DO
/// # Examples

// DON'T
/// # Example
/// ## Examples
/// **Examples:**
```

## Type References

Use full generic forms and link with reference-style markdown.

```rust
// DO
/// Returns [`Option<T>`] if the value exists.
///
/// [`Option<T>`]: std::option::Option

// DON'T
/// Returns `Option` if the value exists.
/// Returns an optional value.
```

## Examples

Every public item should have examples showing usage.

```rust
/// Adds two numbers together.
///
/// # Examples
///
/// ```
/// let result = my_crate::add(2, 3);
/// assert_eq!(result, 5);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

For multiple patterns:

```rust
/// Parses a string into a number.
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// let n: i32 = my_crate::parse("42").unwrap();
/// assert_eq!(n, 42);
/// ```
///
/// Handling errors:
///
/// ```
/// let result = my_crate::parse::<i32>("not a number");
/// assert!(result.is_err());
/// ```
```

## Errors Section

Document what errors can be returned and when. In Rocky this mostly applies to `rocky-core`, `rocky-sql`, and `rocky-adapter-sdk` — the library crates that use `thiserror` (see the `rust-error-handling` skill).

```rust
/// Reads a file from disk.
///
/// # Errors
///
/// Returns [`io::Error`] if the file does not exist or cannot be read.
///
/// [`io::Error`]: std::io::Error
```

## Panics Section

Document conditions that cause panics.

```rust
/// Divides two numbers.
///
/// # Panics
///
/// Panics if `divisor` is zero.
pub fn divide(dividend: i32, divisor: i32) -> i32 {
    assert!(divisor != 0, "divisor must not be zero");
    dividend / divisor
}
```

## Safety Section

Required for `unsafe` functions. In Rocky, this currently applies to the DuckDB FFI surface in `rocky-duckdb` and to low-level code in `rocky-core/src/mmap.rs`. See the `rust-unsafe` skill for the full `SAFETY:` comment conventions that complement this section.

```rust
/// Dereferences a raw pointer.
///
/// # Safety
///
/// The pointer must be non-null and properly aligned.
/// The pointed-to memory must be valid for the lifetime `'a`.
pub unsafe fn deref<'a, T>(ptr: *const T) -> &'a T {
    &*ptr
}
```

## Module vs Type Docs

- Module docs (`//!`): high-level summaries, when to use this module
- Type docs (`///`): comprehensive, self-contained

Some duplication is acceptable.

## Language

Use American English spelling: "color" not "colour", "serialize" not "serialise".

## Rocky-specific: `*Output` structs flow through codegen

Any doc comment on a field of a `*Output` struct in `crates/rocky-cli/src/output.rs` (or `commands/doctor.rs`) becomes a doc comment on the generated Pydantic model field in `integrations/dagster/src/dagster_rocky/types_generated/` and on the generated TypeScript interface field in `editors/vscode/src/types/generated/`. Write those comments assuming a Dagster user will read them — not just a Rust caller. See the `rocky-codegen` skill at the monorepo root for the full cascade.
