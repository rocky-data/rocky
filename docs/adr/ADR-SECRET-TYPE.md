# ADR-SECRET-TYPE — A resolved `${VAR}` value that can only print as `${NAME}` (#1919)

**Status:** **Proposed** · 2026-09-27 · blocks the #1919 implementation until the owner signs off
**Refs:** #1919, #1878, #1897, #1934

---

## Context

`${VAR}` is expanded on the raw config text, before TOML is parsed. The
parser then sees ordinary bytes. Nothing after that point can tell a
resolved secret from any other string.

```text
rocky.toml ──▶ substitute_env_vars_inner ──▶ expanded text ──▶ toml::from_str ──▶ RockyConfig
  "${TOKEN}"         (registers value)          "abc123…"                         String
```

Today two mechanisms cover this. Neither is at the type level.

| Mechanism | Where | Renders | Covers |
|---|---|---|---|
| `RedactedString` | `rocky-core/src/redacted.rs` | `***` | Only fields declared with it: adapter credentials |
| Value registry + filter | `rocky-core/src/secret_registry.rs`, `rocky-cli/src/secret_filter.rs`, `rocky-cli/src/output_redaction.rs` | `${NAME}` | Every byte on the serve, CLI, JSON and log boundaries (#1897, #1878) |

The registry filter matches by VALUE. It cannot tell a secret from an
identifier that happens to contain the same bytes. It also cannot see a value
that a different process registered. #1919 asks for the fix at the source: a
type whose `Display`, `Debug` and `Serialize` write `${NAME}`, with plaintext
only through `.expose()`.

### Why this is not one pass

The value and its template are separated before any type exists. Four facts
make moving that boundary a real change:

1. **Substitution runs on text, at 10 production call sites.** Two are in
   `rocky-core/src/config.rs` (`parse_rocky_config_raw` and the main
   loader). Eight are in `rocky-core/src/models.rs` (sidecars,
   `_defaults.toml`, frontmatter). Each one substitutes, then parses.
2. **Substitution works in positions a string type cannot hold.** Before
   parse, `ttl_seconds = ${TTL}` and `enabled = ${FLAG}` become valid TOML.
   A quoted key `[adapter."${NAME}"]` becomes a table key.
3. **Most config fields are `String`.** A policy `scope`, `verify_after`, an
   `autonomy_budget.window`, `metadata_columns[].value`, target templates and
   model sidecar `[target]` fields are all plain `String` or `Vec<String>`.
4. **Errors copy the value out.** Many `ConfigError` variants interpolate a
   `String` field. #1934 added two more: the `{separator:?}` variant and the
   `metadata_columns` `{column}`/`{reason}` variant.

## Decision

### 1. Parse the raw text, substitute on the tree

Parse the UNexpanded text with `toml::from_str::<toml::Value>`. A quoted
`"${TOKEN}"` is a valid TOML string, so this parses. Then walk the tree and
expand only STRING leaves.

```text
rocky.toml ──▶ toml::from_str ──▶ walk string leaves ──▶ EnvString { value, template }
                                        │
                                        └─ no `${` ─▶ plain String, unchanged
```

`parse_rocky_config_raw` already parses a tree. The change adds one walker,
`expand_env_in_tree(&mut toml::Value) -> Result<EnvReport, ConfigError>`, and
keeps `substitute_env_vars_inner` as its per-string engine. The registry keeps
registering from inside that engine, so the boundary filters stay correct.

**Bare placeholders.** `ttl_seconds = ${TTL}` does not parse raw. Keep a
pre-pass: when a `${` occurs outside a quoted string, expand that occurrence
as text first, exactly as today. Those values are numbers, booleans or keys.
They stay covered by the registry filter only, and the ADR says so.

### 2. The type

Extend `rocky-core/src/redacted.rs` with a sibling, not a new module:

```rust
pub struct EnvString {
    value: String,               // resolved
    template: Option<Arc<str>>,  // "${TOKEN}_raw"; None when nothing expanded
}
impl Display / Debug   -> template when it expanded a value >= SECRET_LENGTH_FLOOR, else value
impl Serialize         -> the same, except inside with_unredacted_scope
impl EnvString { pub fn expose(&self) -> &str }
```

The floor keeps the owner's 8-byte ruling. A short value prints as today.

Deserialization cannot see paths, so the tree walk does the work. It replaces
an expanded string leaf with an internal tagged table:

```toml
{ "\u0000rocky_env" = { value = "…", template = "${TOKEN}_raw" } }
```

`EnvString`'s `Deserialize` accepts that shape or a plain string. Fields that
stay `String` get `#[serde(deserialize_with = "env::plain")]`, which accepts
both and drops the template. That makes the migration field-by-field and
non-breaking. A field left without either attribute fails to deserialize an
expanded value, which is the loud failure we want in tests.

### 3. Which fields change type (first wave)

| Field | Why first |
|---|---|
| `PolicyRule.scope.models`, `.tags`, `.layer`, `.classifications` | #1878 probe |
| `PolicyRule.verify_after` | #1878 probe |
| `AutonomyBudget.window` | `PolicyBudgetInvalidWindow` example in #1897 |
| `metadata_columns[].value`, `schema_pattern.separator` | #1934 `ConfigError` sites |
| Adapter `host`, `http_path`, `account`, `path` | Non-credential connection text; audit which outputs print it |
| Model sidecar `[target]` `catalog`, `schema`, `table` | Printed by compile, plan, run |

Credential fields keep `RedactedString`. They already print `***`.

### 4. Errors

`ConfigError` variants that carry a config value take `EnvString`, not
`String`. `thiserror`'s `{field}` then renders the template. The two #1934
variants are in the first wave.

## Consequences

- A new output field built from an `EnvString` is safe by construction. A
  field built from `.expose()` is greppable.
- Identifiers derived from a template (`${ENV}_raw`) keep printing
  `${ENV}_raw` — the same as the registry filter shows today, but now only
  where the value came from the environment, not where the bytes collide.
- **Not closed:** bare placeholders (numbers, booleans), table keys, and any
  value a child process expands. The registry filter stays as the backstop.
  It is not removed by this ADR.
- The SQL-facing paths must call `.expose()`. A missed one renders
  `${NAME}` into SQL and fails the query, which fails closed.

## Alternatives

| Option | Rejected because |
|---|---|
| Keep text substitution, thread `EnvVarSubstitution` report to every consumer | #1897 option 2. Every future error site must opt in. |
| Make every string field `EnvString` in one PR | Touches most of `rocky-core`, every adapter and the CLI. Not reviewable. |
| Refuse `${VAR}` in load-bearing fields | #1878 option 3. Breaks projects that template scopes. |

## Validation

- Round-trip test: raw config with `"${T}"` in each first-wave field;
  `Debug`, `Display`, `serde_json` and every `ConfigError` render `${T}`.
- A compile-fail test (`rocky-core-compiletest`) that `EnvString` has no
  `Deref<Target = str>` and no `AsRef<str>`.
- The #1878 binary tests (`engine/rocky/tests/cli_secret_redaction.rs`) keep
  passing with the registry filter switched off for the first-wave fields.
- An independent red team reviews before merge (#1919 comment, 2026-09-22).
