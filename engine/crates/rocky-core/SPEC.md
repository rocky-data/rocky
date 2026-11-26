# `rocky-core` typed IR — internal contract spec

**Status:** internal contract. **No semver. No public commitment.** Refactor at will; cross-PR breakages are caught by the golden tests in `engine/crates/rocky-cli/tests/ir_golden.rs`, not by a stability promise.

This document describes the typed intermediate representation (IR) that model loading builds and `rocky-core::sql_gen` consumes. The load-bearing types are [`ModelIr`] and [`ProjectIr`], in the `rocky-ir` crate ([`rocky-ir/src/ir.rs`](../rocky-ir/src/ir.rs)), with the canonical-JSON helper, the recipe-hash and the variant tag. This spec stays in `rocky-core` because `sql_gen` and the golden tests refer to it here.

Field-level semantics live in the Rust doc-comments. This spec covers boundaries, invariants, gaps and the checklist for evolving the IR. When the doc-comments and this spec disagree, the doc-comments win.

[`ModelIr`]: ../rocky-ir/src/ir.rs
[`ProjectIr`]: ../rocky-ir/src/ir.rs

---

## 1. Scope

The IR carries **everything Rocky needs to generate SQL for a single model and reason about its content-addressed identity**: the SQL itself, the typed output columns, the lineage edges that target the model, the materialization strategy, governance metadata, the resolved column-masking plan for the active environment, the declared cost ceiling, and the source / target table refs plus the variant-specific fields of a replication, transformation, or snapshot model.

### What the IR is not

The IR is **not** the AST. The Rocky DSL parser produces a `RockyFile` AST (in `rocky-lang::ast`), which is then lowered to a SQL string via `lower::lower_to_sql`. Both DSL-authored and raw-SQL models converge at the SQL-string level *before* the IR is constructed. Raw SQL stays first-class.

The IR is **not** post-typecheck either. Type info lives sidecar in `TypedColumn` (`rocky-ir/src/types.rs`) and in the IR's [`ModelIr::typed_columns`] field, populated by the compiler when typed columns are available. A `ModelIr` whose `typed_columns` is empty represents a model whose typecheck partial-failed or which uses `SELECT *` against an upstream that hasn't been typechecked yet — both legitimate states.

The IR is **not** a runtime artifact. The runtime — adapters, secrets, env vars, run IDs, branch overrides, watermark state, and resolved partition windows — flows through other channels (`AdapterConfig`, `StateStore`, the `--branch` flag) and is intentionally absent from the IR. Recipe-hash determinism depends on this absence; see §7.

### Who reads this

- **Engine contributors** changing `rocky-ir/src/ir.rs`, `sql_gen.rs` or `models.rs::to_model_ir`: §10 is the checklist for adding a field.
- **Engine maintainers**: §9 is the gap list.
- **AI agents** generating IR: §3, §4, §5 give the shape; §6 gives the wire format.

Consumers **outside** the engine depend on the typed `*Output` structs in [`rocky-cli::output`](../rocky-cli/src/output.rs), not on `ModelIr` / `ProjectIr`. The IR is internal.

---

## 2. Boundaries

Three cuts shape what belongs in the IR.

### IR vs AST

The IR is **downstream of parsing and model loading**. `Model::to_model_ir()` (`src/models.rs`) builds a `ModelIr` from a loaded `Model` (the SQL string plus the sidecar TOML config). The runtime also builds one directly for replication tables, in `rocky-cli/src/commands/` (`plan.rs`, `run.rs` and others). `lower::lower_to_sql` returns `Result<String, String>`, so DSL and raw-SQL models both hit the IR as a SQL string + sidecar config. A direct DSL-to-IR path would slot in between lowering and IR construction.

### IR vs emit

The dialect boundary is the [`SqlDialect`](src/traits.rs) trait. Every `sql_gen` entry takes `&dyn SqlDialect`, and each warehouse adapter crate ships its own impl. The same `ModelIr` compiles to each dialect through the same entry. The golden suite (§11) pins that SQL for DuckDB, Databricks, BigQuery and Snowflake, plus Trino for the snapshot fixture.

### IR vs runtime

Explicitly **out of the IR by construction**:

- Adapter handles, secrets, env vars (configured separately, supplied to dialects + connectors).
- Run IDs (`run_id` is minted by the runtime; recorded on `RunRecord` in the state store).
- Branch overrides (`--branch` rewrites `schema_prefix` at SQL-gen time; `BranchRecord` lives in `rocky-core::state`, not on `ModelIr`).
- Resolved partition windows for `MaterializationStrategy::TimeInterval` (the static plan emits `window: None`; the runtime fills `Some(...)` per partition — see §5 and §7).
- Watermark state for `MaterializationStrategy::Incremental` (the strategy carries only `timestamp_column`; `WatermarkState` lives in the embedded state store, read at execution time via a `WHERE ts > (SELECT MAX(ts) FROM target)` subquery — see §5 and §7).

Runtime concerns leaking into the IR is the most common bug this spec prevents. If you add a "current run" or "active partition" field to `ModelIr`, stop and re-read this section.

---

## 3. `ModelIr`

`ModelIr` is the per-model intermediate representation. One instance per model; flat fields rather than a nested `kind: ModelKind` enum (see "Flat-fields design" in the doc-comment on the struct).

The full field map, with semantics, lives on the struct itself in [`rocky-ir/src/ir.rs`](../rocky-ir/src/ir.rs). The summary here is purely organizational:

| Group | Fields | Notes |
|---|---|---|
| Identity | `name`, `target` | `name` is project-unique. `Model::to_model_ir()` sets it from `config.name`, not from `target.table`. |
| Program shape | `sql`, `materialization`, `governance` | `sql` is the load-bearing recipe-hash input; `materialization` carries the variant enum (§5). |
| Typed sidecar | `typed_columns`, `lineage_edges`, `column_masks` | Populated by the compiler / governance layers downstream; empty when the upstream pipeline hasn't computed them yet. |
| Variant-specific (replication) | `source`, `columns`, `metadata_columns` | `columns` is the variant discriminator: `Some(_)` ⇒ replication. |
| Variant-specific (transformation) | `sources`, `format`, `format_options` | `sources` is non-empty only for transformation. `format` and `format_options` carry the lakehouse table format. |
| Variant-specific (snapshot) | `unique_key`, `updated_at`, `invalidate_hard_deletes` | `unique_key` non-empty AND `updated_at` `Some` ⇒ snapshot. |
| Cost | `cost_ceiling` | The declared per-model `[budget]` ceiling. `None` when the sidecar declares no cost dimension. |

### Variant inference

The flat-field shape means the variant is inferred, not stored. [`ModelIr::variant()`] returns a `ModelIrVariant` and checks the discriminators in this order:

1. `unique_key` non-empty AND `updated_at` `Some` ⇒ `Snapshot`.
2. `columns` `Some` ⇒ `Replication`.
3. Otherwise ⇒ `Transformation`.

[`ModelIr::variant()`]: ../rocky-ir/src/ir.rs

This order matters. An IR with both `unique_key` and `columns` populated resolves as snapshot, not replication. So `MaterializationStrategy::Merge`'s own `unique_key` must never leak into the top-level `ModelIr.unique_key`, or a replication model would classify as a snapshot. The inline `variant_*` tests in `ir.rs` pin the order.

### Byte-stable round-trip

`serialize → deserialize → serialize` yields identical bytes for every variant. The inline `model_ir_*_roundtrip_byte_stable` and `project_ir_roundtrip_with_multiple_models` tests pin this. Canonical-JSON equality, not `PartialEq`, is the equivalence relation the tests use.

---

## 4. `ProjectIr`

`ProjectIr` is a thin wrapper that pairs the per-model IRs with the project's DAG and the cross-model lineage edges:

```rust
pub struct ProjectIr {
    pub models: Vec<ModelIr>,
    pub dag: Vec<DagNode>,
    pub lineage_edges: Vec<LineageEdge>,
}
```

The pipeline layer is **not** an IR layer. The hierarchy `RockyConfig → pipelines → models → ModelIr` skips the pipeline layer in the IR: pipelines are *policy* (which adapter, which models), not *program shape*, and they stay in `RockyConfig`. There is no `PipelineIr` between `ProjectIr` and `ModelIr`.

Project-level recipe-hash is **derived**, not stored — see §7.

Keep `ProjectIr` thin. Each candidate addition (DAG validation results, project-level cost projections, strategy summaries) is either a derived fact (compute on demand) or a runtime concern (state store).

---

## 5. `MaterializationStrategy`

Twelve variants, defined in [`rocky-ir/src/ir.rs`](../rocky-ir/src/ir.rs). The variant-by-variant semantics are documented on the enum itself; this section pulls out the two recipe-hash invariants and a per-variant intent table.

### Variant intent table

| Variant | Use | Generated SQL shape |
|---|---|---|
| `FullRefresh` | Drop and recreate the entire table. | `CREATE OR REPLACE TABLE … AS SELECT …` (or dialect equivalent). |
| `Incremental { timestamp_column }` | Append rows newer than the watermark. | `INSERT INTO target SELECT … WHERE ts > (SELECT MAX(ts) FROM target)` — watermark resolves at execution time, not from IR. |
| `Merge { unique_key, update_columns }` | Upsert based on unique key columns. | `MERGE INTO target USING source ON … WHEN MATCHED THEN UPDATE SET …`. |
| `View` | A view; nothing is stored. | `CREATE OR REPLACE VIEW … AS SELECT …` (or dialect equivalent). |
| `MaterializedView` | Databricks Materialized View — warehouse manages refresh. | `CREATE OR REPLACE MATERIALIZED VIEW`. |
| `DynamicTable { target_lag }` | Snowflake Dynamic Table — warehouse manages lag-based refresh. | `CREATE OR REPLACE DYNAMIC TABLE … TARGET_LAG = '…'`. |
| `TimeInterval { time_column, granularity, window }` | Partition-keyed materialization. | `INSERT OVERWRITE PARTITION (…)` per partition; one IR instance per partition. |
| `Ephemeral` | Never materialized. The compiler inlines its SQL as a CTE into each consumer's `sql`. | No statement of its own. `E038` refuses the cases the inliner cannot rewrite. |
| `DeleteInsert { partition_by }` | Delete matching rows by partition key, then insert fresh data. dbt-compatible. | `DELETE FROM target WHERE …; INSERT INTO target SELECT …`. |
| `Microbatch { timestamp_column, granularity }` | Alias for `TimeInterval`; `granularity` defaults to hour. dbt-compatible naming. | Same as `TimeInterval`. |
| `ContentAddressed { storage_prefix, partition_columns, … }` | Content-hash-named Parquet files in a Delta UniForm table. | None: `sql_gen` returns an error and the runner writes the files. |
| `Snapshot(SnapshotSpec)` | SCD Type 2 history (dbt `snapshot` parity). Each run closes changed versions and inserts new ones. | Close-and-insert statements. `hard_deletes` decides what happens to keys that leave the result. |

### Recipe-hash invariants

Two strategy fields **must** stay normalized at hash time. Both are runtime state.

1. **`Incremental` carries no `WatermarkState`.** The only field on `Incremental` is `timestamp_column`. The actual watermark value (`last_value`, `updated_at`) is runtime state read from [`StateStore::get_watermark`](src/state.rs); the SQL generator emits a `WHERE ts > (SELECT MAX(ts) FROM target)` subquery that resolves the bound at execution time. If `WatermarkState` were carried on the strategy, **the recipe-hash would change every successful run** — destroying any content-addressed write semantics built on top of the recipe-hash. This invariant is enforced by construction: there is no `watermark` field on `MaterializationStrategy::Incremental`.

2. **`TimeInterval.window` is `None` at hash time.** The static plan emits `None`; the runtime fills `Some(PartitionWindow { … })` per partition before SQL generation. Recipe-hash must run on the static form — otherwise every partition gets a different hash for the same model, again defeating content-addressed writes. This invariant is **not** enforced by construction (the field is `Option<PartitionWindow>`); it is enforced by convention. Plan-construction sites must leave `window: None`; the runtime mutates the IR after recipe-hash computation. The doc-comment on the variant's `window` field pins this.

These two invariants are the most common IR regression. A new variant with a runtime-supplied field follows `TimeInterval`: declare `Option<T>` and document "static plan emits `None`; runtime fills `Some(_)` post-hash" on the field.

---

## 6. Canonical-JSON convention

Recipe-hash determinism requires a single, predictable serialization shape. The rule has three legs (also documented in the module-level doc-comment in [`rocky-ir/src/ir.rs`](../rocky-ir/src/ir.rs)):

1. **Every `Option<T>` field carries `#[serde(default, skip_serializing_if = "Option::is_none")]`.** `None` values are absent from the JSON; the recipe-hash never sees an explicit `null`.
2. **Every `Vec<T>` field that is "conceptually optional" (empty == absent) carries `#[serde(default, skip_serializing_if = "Vec::is_empty")]`.** Examples: `metadata_columns`, `unique_key`, `sources`, `column_masks`. The variant-specific `Vec` fields on `ModelIr` all follow this rule, which is what keeps the per-variant JSON shape compact.
3. **Every `bool` field whose semantic default is `false` carries `#[serde(default, skip_serializing_if = "std::ops::Not::not")]`.** Example: `invalidate_hard_deletes`. The key is omitted when the value is the default.

### Why "Rule A"

`canonical_json` ([`canonical_json`](../rocky-ir/src/ir.rs)) round-trips through `serde_json::Value` and rewrites every nested map into a key-sorted `BTreeMap`. With Rule A, skipped fields are already absent at serialize time, so there is no `null` to canonicalize. Always emitting explicit `null` would also be deterministic, but it inflates the JSON and creates surprising diffs when an `Option` flips state.

### Map ordering

`canonical_json` sorts every `serde_json::Value::Object` by key via `BTreeMap`. Arrays are not reordered (preserving caller intent for ordered fields like `lineage_edges`, `dag`, `sources`); when caller-defined order would non-deterministically affect the hash, the consumer is responsible for sorting before construction (this is what `ProjectIr::recipe_hash` does for the per-model hashes — see §7).

### Adding new fields

Every new field on `ModelIr` / `ProjectIr` follows Rule A. A field with no natural empty form (a required `String`, an `enum` with no neutral variant) is mandatory in the JSON, so it must be load-bearing on every construction path. See §10.

---

## 7. Recipe-hash

The recipe-hash is `blake3(canonical_json(ir))`. It is the content-addressed identifier of a model's program shape: byte-identical IR ⇒ byte-identical hash; any field change ⇒ different hash.

### Per-model

[`ModelIr::recipe_hash()`] computes the hash directly. The determinism contract: given two byte-identical `ModelIr` values, the returned hash is byte-identical. Mutating any input field (SQL, typed columns, lineage edges, materialization, governance, resolved masks, source/target refs, snapshot key/timestamp, lakehouse format, column selection, metadata columns, cost ceiling) changes the hash.

[`ModelIr::recipe_hash()`]: ../rocky-ir/src/ir.rs

The sensitivity matrix is regression-tested by the inline `recipe_hash_changes_when_*` tests. Adding a new field to `ModelIr` requires adding a corresponding sensitivity test — if there is no observable hash change, the field is invisible to content-addressed writes.

### Project-level

[`ProjectIr::recipe_hash()`] is **derived from the per-model hashes**, not from a fresh canonical-JSON encoding of the wrapper. Each model contributes its own [`ModelIr::recipe_hash`]; the per-model hashes are sorted lexicographically by their hex representation and combined via blake3 with a length-prefixed separator. The result is independent of the order of `ProjectIr.models`.

[`ProjectIr::recipe_hash()`]: ../rocky-ir/src/ir.rs

`ProjectIr.dag` and `ProjectIr.lineage_edges` are **not** folded into the project-level hash — they are derived facts about how the per-model recipes relate, not part of any single model's recipe. Changes to the DAG or cross-model lineage that do not change a model's own recipe leave the per-model hashes (and therefore the project-level hash) untouched.

The per-model hash addresses the storage write. The project-level hash addresses the project's "version" without changing on DAG-only edits.

### What the hash sees and doesn't see

| Carried by recipe-hash | Not carried by recipe-hash |
|---|---|
| SQL text (every character, including comments) | Watermark state |
| Typed columns | Resolved partition window |
| Lineage edges (slice targeting this model) | Run IDs, branch overrides, adapter handles |
| Materialization strategy (variant + non-runtime fields) | DAG ordering, cross-model lineage (these affect `ProjectIr` shape but not per-model hash; project-level hash sees them only via per-model hash changes) |
| Governance config | `RockyConfig` lifecycle settings (hooks, cost rates, budget) |
| Source / target refs | Environment selection (Path A: resolved masks bake env into IR; the env *itself* is not a field) |
| Resolved column masks | Approval state, retention policy |
| Snapshot unique-key, updated-at column, hard-delete flag | |
| Lakehouse format + format options | |
| Replication column selection + metadata columns | |
| Declared cost ceiling (`cost_ceiling`) | |

---

## 8. Variant guard

Every public `sql_gen` entry takes `&ModelIr` and first checks `ModelIr::variant()`:

```rust
if model_ir.variant() != ModelIrVariant::Replication {
    return Err(variant_mismatch(model_ir, "Replication"));
}
```

`variant_mismatch` returns `SqlGenError::InvalidRequest` with the message `expected <X> ModelIr for `<name>`, found <actual>`. The type system cannot enforce the variant, because `ModelIr` is one flat struct. Every production call site hands the matching variant today. The guard is a drift detector: if a new call site picks the wrong entry, it fails at once instead of producing wrong SQL. The `variant_mismatch_*` tests in `sql_gen.rs` pin the message.

---

## 9. Explicit gap list

Things the IR does not yet carry but the typed-program layer needs.

### Determinism tags

Nothing models `Determinism` on `TypedColumn` or elsewhere. `NOW()`, `RANDOM()` and `UUID()` are non-deterministic and the IR cannot say so. Closing the gap needs an enum (`enum Determinism { Pure, NonDeterministic(Reason) }`), SQL-parser detection of such calls, and a sidecar field on `TypedColumn`. Why it matters: content-addressed writes need per-model determinism, because a model with `NOW()` needs different replay semantics.

### Replay-recipe primitives beyond hash — closed outside the IR

The IR carries `recipe_hash` (§7). `input_hash` and `env_hash` are not IR fields, by design: `input_hash` depends on what the upstream models produced in this run, and `env_hash` describes the environment. Both now live on each model execution record in the state store (`src/state.rs`, schema v11), computed by `src/recipe_identity.rs`. `input_hash` is present only when the run observed its inputs.

### Hooks-in-IR (deferred)

`HooksConfig` lives on `RockyConfig` as project-level policy. There is no per-model hook attachment, so nothing to lift. If per-model hooks arrive (`[hook]` blocks in sidecars), the resolved `Vec<HookRef>` should land in `ModelIr` so the recipe-hash reflects what runs for the model. Until then hooks stay out of the IR and the hash.

### Full cost-projection-in-IR

`CostSection` (rates) and `BudgetConfig` (limits) live on `RockyConfig`. The per-model `[budget]` ceiling is on `ModelIr` as `cost_ceiling`, and `rocky compile` and `rocky plan` check it against offline estimates. A computed per-model projection is not a field. If it becomes stable, the shape is `ModelIr.cost_projection: Option<CostProjection>`, since projection needs warehouse `EXPLAIN`, which not every call path has.

### Column-level lineage — `ProjectIr.lineage_edges` is the canonical store

[`ModelIr::lineage_edges`] carries only the slice of edges that *target* this model. The full cross-model graph lives on [`ProjectIr::lineage_edges`]. Today both fields are populated by `rocky_compiler::semantic::SemanticGraph` extraction; the `ProjectIr` field is the canonical store and the `ModelIr` slice is denormalized for per-model recipe-hash determinism.

[`ModelIr::lineage_edges`]: ../rocky-ir/src/ir.rs
[`ProjectIr::lineage_edges`]: ../rocky-ir/src/ir.rs

The slice is load-bearing: the per-model recipe-hash needs it. Keep both fields in sync at construction, or the recipe-hash drifts.

### Schema declarations beyond names

[`ModelIr::typed_columns`] carries `Vec<TypedColumn>` — name + `RockyType` + nullable. Replication carries column names (and metadata columns with raw type strings) on the variant-specific fields. The full typed schema (constraints, defaults, comments, foreign keys) is not represented; the warehouse owns that today.

### Multi-frontend (DSL → IR direct)

The DSL lowers to a SQL string before the IR. A typed DSL-to-IR path would preserve structural shape (typed lineage from DSL pipelines, deterministic IR JSON for AI emit).

---

## 10. Adding a new IR field — checklist

When adding a new field to `ModelIr` or `ProjectIr`:

1. **Pick the canonical-JSON attribute pair.** §6 covers the three rules; the right pair depends on the field's type:
   - `Option<T>` → `#[serde(default, skip_serializing_if = "Option::is_none")]`
   - `Vec<T>` (conceptually-optional) → `#[serde(default, skip_serializing_if = "Vec::is_empty")]`
   - `bool` (default-false) → `#[serde(default, skip_serializing_if = "std::ops::Not::not")]`
   - Required `String`, required `enum`: no skip attribute; the field is mandatory in the JSON.

2. **Update every construction site.** `Model::to_model_ir()` and the runtime builders in `rocky-cli` must populate the new field. If the field changes which discriminator `ModelIr::variant()` reads, update §3's inference order *and* the `variant_*` tests in `rocky-ir/src/ir.rs` to cover the new ambiguity case.

3. **Add a recipe-hash sensitivity test.** Inline tests in `rocky-ir/src/ir.rs` follow the `recipe_hash_changes_when_<field>_changes` pattern. If the new field doesn't observably change the hash, it is invisible to content-addressed writes — that is either a design decision (state the rationale in the field's doc-comment) or a bug.

4. **Add a canonical-JSON enforcement test if the field is skip-able.** Inline tests follow the `<field>_omitted_from_serialization` pattern (e.g. `empty_sources_omitted_from_replication_serialization`). One assertion that the JSON does not contain the field key when the value is the empty/None/default form.

5. **Update the golden fixtures.** [`engine/crates/rocky-cli/tests/ir-golden/`](../rocky-cli/tests/ir-golden/) carries one fixture per major variant. If the new field changes any fixture's serialization, run `REGEN_IR_GOLDENS=1 cargo test -p rocky-cli --test ir_golden -- --nocapture`. It rewrites each `ir.json` and per-dialect `.sql` snapshot and prints the new recipe-hashes. Paste them into the `FIXTURES` table in `ir_golden.rs`.

6. **Update this spec.** §3's group table and §6's Rule A coverage may need a bullet; §9's gap list should shrink (the field is closing a previously-named gap) or be unaffected (the field is structural).

7. **Don't add a `JsonSchema` derive.** The IR is internal contract; consumers outside the engine should depend on the typed `*Output` structs in `rocky-cli::output`, which do derive `JsonSchema`. Adding `JsonSchema` to `ModelIr` would route the IR into the public schema export pipeline — that path requires a stable, versioned IR with semver guarantees, none of which are in scope today.

---

## 11. Versioning policy

**No semver. No public commitment. Refactor at will.** The IR is internal. Cross-PR breakages (a renamed field, a moved type, a serde attribute change) are caught by:

- The inline tests in `rocky-ir/src/ir.rs` (byte-stable round-trip, recipe-hash determinism and sensitivity, canonical-JSON Rule A, variant inference) and the `variant_mismatch_*` tests in `src/sql_gen.rs`.
- The golden tests in `engine/crates/rocky-cli/tests/ir_golden.rs` (IR JSON snapshots, per-dialect SQL snapshots, pinned recipe hashes per fixture).

After an intentional shape change, regenerate the fixtures and pinned hashes. There is no migration path or shim.

Consumers outside the engine (Dagster, VS Code, third-party tools) depend on the `*Output` structs in [`rocky-cli::output`](../rocky-cli/src/output.rs), which derive `JsonSchema` and back the generated bindings. `rocky-ir` is versioned with the workspace. A public IR would need `JsonSchema` derives, a conformance suite and semver from `1.0`. None of that is in scope.

---

## 12. Future directions

Each direction closes a §9 gap or relies on the IR's guarantees.

| Direction | What it consumes / produces here |
|---|---|
| **Content-addressed writes + replay re-execution** | Consumes `ModelIr::recipe_hash` as the per-model write address. The two §5 invariants are load-bearing. |
| **Per-model cost projection** | Closes the cost-projection gap (§9). The ceiling is already on the IR. |
| **Schema-grounded AI emission** | Emits IR directly. The flat-fields design (§3) and Rule A (§6) make `ModelIr` the natural emit target. Closes the multi-frontend gap (§9) when emit reaches IR rather than DSL/SQL. |
| **Determinism tags** | Closes the determinism gap (§9). (`input_hash` and `env_hash` already ship on execution records.) |
| **Full cross-model lineage graph** | Closes the lineage and schema-declaration gaps (§9) via `ProjectIr.lineage_edges`. |

---

## Where this document lives

- This file: the synthesizing narrative.
- Field-level semantics: [`engine/crates/rocky-ir/src/ir.rs`](../rocky-ir/src/ir.rs) doc-comments, the source of truth. The inline `#[cfg(test)]` tests in that file pin every invariant above.
- Golden tests: [`ir_golden.rs`](../rocky-cli/tests/ir_golden.rs) and [`ir-golden/`](../rocky-cli/tests/ir-golden/): the strategy matrix across DuckDB, Databricks, BigQuery and Snowflake (plus Trino for the snapshot fixture), with pinned recipe hashes.

Doc-comments beat this spec. Tests beat this spec. If a golden snapshot disagrees with the spec, regenerate it when the change was intentional and update the spec. Otherwise the snapshot wins.
