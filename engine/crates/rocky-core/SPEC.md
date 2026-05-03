# `rocky-core` typed IR — internal contract spec

**Status:** internal contract. **No semver. No public commitment.** Refactor at will; cross-PR breakages are caught by the golden tests in `engine/crates/rocky-cli/tests/ir_golden.rs`, not by a stability promise.

This document describes the typed intermediate representation produced by `rocky-compiler` and consumed by `rocky-core::sql_gen`. The two load-bearing types are [`ModelIr`] and [`ProjectIr`] in [`src/ir.rs`](src/ir.rs); the canonical-JSON convention, the recipe-hash, and the variant-extraction tripwire all live alongside them.

The IR is the program-shape representation that sits between model loading and SQL generation. Field-level semantics are documented as Rust doc-comments on the structs themselves; this spec is the synthesizing narrative — boundaries, invariants, gaps, and the operational checklist for evolving the IR. When the doc-comments and this spec disagree, the doc-comments win.

[`ModelIr`]: src/ir.rs
[`ProjectIr`]: src/ir.rs

---

## 1. Scope

The IR carries **everything Rocky needs to generate SQL for a single model and reason about its content-addressed identity**: the SQL itself, the typed output columns, the lineage edges that target the model, the materialization strategy, governance metadata, the resolved column-masking plan for the active environment, and the source / target table refs plus variant-specific fields needed to losslessly represent any [`Plan`] variant.

[`Plan`]: src/ir.rs

### What the IR is not

The IR is **not** the AST. The Rocky DSL parser produces a `RockyFile` AST (in `rocky-lang::ast`), which is then lowered to a SQL string via `lower::lower_to_sql`. Both DSL-authored and raw-SQL models converge at the SQL-string level *before* the IR is constructed (consistent with the `rocky_sql_first_class` posture: raw SQL stays first-class).

The IR is **not** post-typecheck either. Type info lives sidecar in `rocky_compiler::types::TypedColumn` (and in the IR's [`ModelIr::typed_columns`] field, populated by the compiler when typed columns are available). A `ModelIr` whose `typed_columns` is empty represents a model whose typecheck partial-failed or which uses `SELECT *` against an upstream that hasn't been typechecked yet — both legitimate states.

The IR is **not** a runtime artifact. The runtime — adapters, secrets, env vars, run IDs, branch overrides, watermark state, and resolved partition windows — flows through other channels (`AdapterConfig`, `StateStore`, the `--branch` flag) and is intentionally absent from the IR. Recipe-hash determinism depends on this absence; see §7.

### Why this exists as a doc

Three audiences:

- **Rocky engine contributors** modifying `ir.rs`, `sql_gen.rs`, or `models.rs::to_model_ir` — §10 is the operational checklist for adding a new field without breaking the canonical-JSON rule or recipe-hash determinism.
- **Engine maintainers tracking the IR's evolution** — §9 is the explicit gap list, with each gap mapped to the future direction that would close it.
- **AI agents** generating IR for the planned schema-grounded AI emission path — §3, §4, §5 describe the shape; §6 describes the canonical wire format.

Consumers **outside** the engine should depend on the typed `*Output` structs in [`rocky-cli::output`](../rocky-cli/src/output.rs), not on `ModelIr` / `ProjectIr` directly. Those are the public-API surface; the IR is internal.

---

## 2. Boundaries

Three cuts shape what does and doesn't belong in the IR.

### IR vs AST

The IR is **downstream of parsing and downstream of model loading**. `Plan` (and therefore `ModelIr`) is constructed by the runtime in `rocky-cli/src/commands/{plan,run,run_local}.rs` from a `RockyConfig` and a `Model` (which carries the SQL string + sidecar TOML config). Parser-level concerns (DSL syntax, raw-SQL parsing) are not visible here.

The DSL has no typed lowering of its own at the IR level: `lower::lower_to_sql` returns `Result<String, String>` — both DSL and raw-SQL models hit the IR as a SQL string + sidecar config. If a future "DSL → typed IR direct" path is wanted, it would slot in *between* lowering and IR construction.

### IR vs emit

The dialect boundary is the [`SqlDialect`](src/traits.rs) trait. Every `sql_gen` entry takes `&dyn SqlDialect`; each warehouse adapter (`rocky-duckdb`, `rocky-databricks`, `rocky-bigquery`, `rocky-snowflake`) ships its own impl. The IR is therefore **dialect-portable by construction** — the same `ModelIr` compiles to four different SQLs through the same `sql_gen` entry, and that property is regression-tested by the golden suite (§7).

This is the cleanest boundary in the engine and the one that would survive promotion to a public IR most easily.

### IR vs runtime

Explicitly **out of the IR by construction**:

- Adapter handles, secrets, env vars (configured separately, supplied to dialects + connectors).
- Run IDs (`run_id` is minted by the runtime; recorded on `RunRecord` in the state store).
- Branch overrides (`--branch` rewrites `schema_prefix` at SQL-gen time; `BranchRecord` lives in `rocky-core::state`, not on `ModelIr`).
- Resolved partition windows for `MaterializationStrategy::TimeInterval` (the static plan emits `window: None`; the runtime fills `Some(...)` per partition — see §5 and §7).
- Watermark state for `MaterializationStrategy::Incremental` (the strategy carries only `timestamp_column`; `WatermarkState` lives in the embedded state store, read at execution time via a `WHERE ts > (SELECT MAX(ts) FROM target)` subquery — see §5 and §7).

Runtime concerns leaking into the IR is the most common shape of bug this spec is designed to prevent. Both watermark and partition-window are recurrence-prone; if you find yourself adding a "current run" or "active partition" field to `ModelIr`, stop and re-read this section.

---

## 3. `ModelIr`

`ModelIr` is the per-model intermediate representation. One instance per model; flat fields rather than a nested `kind: ModelKind` enum (see "Flat-fields design" in the doc-comment on the struct).

The full field map, with semantics, lives on the struct itself in [`src/ir.rs`](src/ir.rs). The summary here is purely organizational:

| Group | Fields | Notes |
|---|---|---|
| Identity | `name`, `target` | `name` is project-unique; `Model::to_model_ir()` overrides `From<&Plan>`'s default (which would have been `target.table`) with `config.name`. |
| Program shape | `sql`, `materialization`, `governance` | `sql` is the load-bearing recipe-hash input; `materialization` carries the variant enum (§5). |
| Typed sidecar | `typed_columns`, `lineage_edges`, `column_masks` | Populated by the compiler / governance layers downstream; empty when the upstream pipeline hasn't computed them yet. |
| Variant-specific (replication) | `source`, `columns`, `metadata_columns` | `columns` is the variant discriminator: `Some(_)` ⇒ replication. |
| Variant-specific (transformation) | `sources`, `format`, `format_options` | `sources` is non-empty only for transformation; lakehouse fields lift `TransformationPlan::format{,_options}`. |
| Variant-specific (snapshot) | `unique_key`, `updated_at`, `invalidate_hard_deletes` | `unique_key` non-empty AND `updated_at` `Some` ⇒ snapshot. |

### Variant inference

The flat-field shape means the variant is inferred at conversion time, not stored on the struct. [`ModelIr::to_plan_compatible()`] walks the discriminators in this order:

1. `unique_key` non-empty AND `updated_at` `Some` ⇒ [`Plan::Snapshot`].
2. `columns` `Some` ⇒ [`Plan::Replication`].
3. Otherwise ⇒ [`Plan::Transformation`].

[`ModelIr::to_plan_compatible()`]: src/ir.rs

This order matters. A snapshot-ish IR with both `unique_key` and `columns` populated would resolve as snapshot, not replication; the inline test [`plan_to_model_ir_replication_with_merge_strategy_roundtrip`](src/ir.rs) pins the related contract that `MaterializationStrategy::Merge`'s own `unique_key` field must NOT leak into the top-level `ModelIr.unique_key` (which would mis-classify a Replication as a Snapshot).

### Lossless round-trip

`From<&Plan> for ModelIr` and `ModelIr::to_plan_compatible()` form a lossless conversion: for any well-formed input plan, `ModelIr::from(&plan).to_plan_compatible()` is canonical-JSON-equal to `plan`. This is regression-tested per variant by the inline `plan_to_model_ir_*_roundtrip` tests.

The conversion is not equality-tested with `PartialEq` because none of the `Plan` component types derive `PartialEq` — adding the derive cascade was deemed out of scope. Canonical-JSON-equality is the equivalence relation used instead.

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

The pipeline layer is **not** an IR layer. The hierarchy `RockyConfig → pipelines → models → Plan` skips the pipeline layer in the IR: pipelines are *policy* (which adapter, which models), not *program shape*, and they stay in `RockyConfig`. There is no `PipelineIr` between `ProjectIr` and `ModelIr`.

Project-level recipe-hash is **derived**, not stored — see §7.

`ProjectIr` is a deliberately thin wrapper. The temptation to grow it into a fat project-shaped struct (storing DAG validation results, project-level cost projections, denormalized materialization-strategy summaries, …) should be resisted: each addition is either a derived fact (compute on demand) or a runtime concern (lives in the state store), and merging it into `ProjectIr` would re-create the per-pipeline-vs-project ambiguity that the per-model IR was designed to resolve.

---

## 5. `MaterializationStrategy`

Nine variants, defined in [`src/ir.rs`](src/ir.rs). The variant-by-variant semantics are documented on the enum itself; this section pulls out the two recipe-hash invariants and a per-variant intent table.

### Variant intent table

| Variant | Use | Generated SQL shape |
|---|---|---|
| `FullRefresh` | Drop and recreate the entire table. | `CREATE OR REPLACE TABLE … AS SELECT …` (or dialect equivalent). |
| `Incremental { timestamp_column }` | Append rows newer than the watermark. | `INSERT INTO target SELECT … WHERE ts > (SELECT MAX(ts) FROM target)` — watermark resolves at execution time, not from IR. |
| `Merge { unique_key, update_columns }` | Upsert based on unique key columns. | `MERGE INTO target USING source ON … WHEN MATCHED THEN UPDATE SET …`. |
| `MaterializedView` | Databricks Materialized View — warehouse manages refresh. | `CREATE OR REPLACE MATERIALIZED VIEW`. |
| `DynamicTable { target_lag }` | Snowflake Dynamic Table — warehouse manages lag-based refresh. | `CREATE OR REPLACE DYNAMIC TABLE … TARGET_LAG = '…'`. |
| `TimeInterval { time_column, granularity, window }` | Partition-keyed materialization. | `INSERT OVERWRITE PARTITION (…)` per partition; one IR instance per partition. |
| `Ephemeral` | Inlined as a CTE in downstream consumers. | No SQL emitted; consumers reference the model name. |
| `DeleteInsert { partition_by }` | Delete matching rows by partition key, then insert fresh data. dbt-compatible. | `DELETE FROM target WHERE …; INSERT INTO target SELECT …`. |
| `Microbatch { timestamp_column, granularity }` | Alias for `TimeInterval` with sensible defaults. dbt-compatible naming. | Same as `TimeInterval` after defaulting. |

### Recipe-hash invariants

Two strategy fields **must** stay normalized at hash time. Both are runtime-state fields that previously leaked into the strategy and were stripped during the typed-IR migration.

1. **`Incremental` carries no `WatermarkState`.** The only field on `Incremental` is `timestamp_column`. The actual watermark value (`last_value`, `updated_at`) is runtime state read from [`StateStore::get_watermark`](src/state.rs); the SQL generator emits a `WHERE ts > (SELECT MAX(ts) FROM target)` subquery that resolves the bound at execution time. If `WatermarkState` were carried on the strategy, **the recipe-hash would change every successful run** — destroying any content-addressed write semantics built on top of the recipe-hash. This invariant is enforced by construction: there is no `watermark` field on `MaterializationStrategy::Incremental`.

2. **`TimeInterval.window` is `None` at hash time.** The static plan emits `None`; the runtime fills `Some(PartitionWindow { … })` per partition before SQL generation. Recipe-hash must run on the static form — otherwise every partition gets a different hash for the same model, again defeating content-addressed writes. This invariant is **not** enforced by construction (the field is `Option<PartitionWindow>`); it is enforced by convention. Plan-construction sites must leave `window: None`; the runtime mutates the IR after recipe-hash computation. The doc-comment on the variant's `window` field pins this.

These two invariants are the single most common shape of regression in the IR. New variants that need a runtime-supplied field should follow `TimeInterval`'s pattern — declare the field as `Option<T>` and document the "static plan emits `None`; runtime fills `Some(_)` post-hash" contract on the field.

---

## 6. Canonical-JSON convention

Recipe-hash determinism requires a single, predictable serialization shape. The rule has three legs (also documented in the module-level doc-comment in [`src/ir.rs`](src/ir.rs)):

1. **Every `Option<T>` field carries `#[serde(default, skip_serializing_if = "Option::is_none")]`.** `None` values are absent from the JSON; the recipe-hash never sees an explicit `null`.
2. **Every `Vec<T>` field that is "conceptually optional" (empty == absent) carries `#[serde(default, skip_serializing_if = "Vec::is_empty")]`.** Examples: `metadata_columns`, `unique_key`, `sources`, `column_masks`. The variant-specific `Vec` fields on `ModelIr` all follow this rule, which is what keeps the per-variant JSON shape compact.
3. **Every `bool` field whose semantic default is `false` carries `#[serde(default, skip_serializing_if = "std::ops::Not::not")]`.** Example: `invalidate_hard_deletes`. The key is omitted when the value is the default.

### Why "Rule A" rather than "all fields always present"

`serde_json` preserves field insertion order on `Map`, but not all orderings are deterministic across different inputs of the same logical value. The canonical-JSON helper ([`canonical_json`](src/ir.rs)) round-trips through `serde_json::Value` and rewrites every nested map into a `BTreeMap` (key-sorted). With Rule A, skipped fields are *already absent* at serialize time — there is no `null` to canonicalize. Rule B (always emit explicit `null`) would also be deterministic but would inflate the JSON and create surprising diffs when an `Option` flips state.

### Map ordering

`canonical_json` sorts every `serde_json::Value::Object` by key via `BTreeMap`. Arrays are not reordered (preserving caller intent for ordered fields like `lineage_edges`, `dag`, `sources`); when caller-defined order would non-deterministically affect the hash, the consumer is responsible for sorting before construction (this is what `ProjectIr::recipe_hash` does for the per-model hashes — see §7).

### Adding new fields

Every new field on `ModelIr` / `ProjectIr` must follow Rule A. If a field has no natural empty/default form (a `String` that is always required, an `enum` with no neutral variant), it is mandatory in the JSON — that is fine, but the field semantics need to be load-bearing across every legitimate construction path. See §10 for the full checklist.

---

## 7. Recipe-hash

The recipe-hash is `blake3(canonical_json(ir))`. It is the content-addressed identifier of a model's program shape: byte-identical IR ⇒ byte-identical hash; any field change ⇒ different hash.

### Per-model

[`ModelIr::recipe_hash()`] computes the hash directly. The determinism contract: given two byte-identical `ModelIr` values, the returned hash is byte-identical. Mutating any input field (SQL, typed columns, lineage edges, materialization, governance, resolved masks, source/target refs, snapshot key/timestamp, lakehouse format, column selection, metadata columns) changes the hash.

[`ModelIr::recipe_hash()`]: src/ir.rs

The sensitivity matrix is regression-tested by the inline `recipe_hash_changes_when_*` tests. Adding a new field to `ModelIr` requires adding a corresponding sensitivity test — if there is no observable hash change, the field is invisible to content-addressed writes.

### Project-level

[`ProjectIr::recipe_hash()`] is **derived from the per-model hashes**, not from a fresh canonical-JSON encoding of the wrapper. Each model contributes its own [`ModelIr::recipe_hash`]; the per-model hashes are sorted lexicographically by their hex representation and combined via blake3 with a length-prefixed separator. The result is independent of the order of `ProjectIr.models`.

[`ProjectIr::recipe_hash()`]: src/ir.rs

`ProjectIr.dag` and `ProjectIr.lineage_edges` are **not** folded into the project-level hash — they are derived facts about how the per-model recipes relate, not part of any single model's recipe. Changes to the DAG or cross-model lineage that do not change a model's own recipe leave the per-model hashes (and therefore the project-level hash) untouched.

This shape is load-bearing for content-addressed writes: the per-model hash addresses the storage write; the project-level hash addresses the project's "version" without inflating with every DAG-only change.

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

The right column is the gap list (§9) viewed from a different angle: each item there is "Rocky owns it elsewhere on the program side, but it doesn't go into the hash, and here is why."

---

## 8. Variant-extraction tripwire

Every public `sql_gen` entry takes `&ModelIr` and opens with a private variant-extraction helper:

```rust
fn replication_from_ir(model_ir: &ModelIr) -> Result<ReplicationPlan, SqlGenError> {
    match model_ir.to_plan_compatible() {
        Plan::Replication(plan) => Ok(plan),
        _ => Err(SqlGenError::InvalidRequest(format!(
            "expected Replication ModelIr for `{}`",
            model_ir.name
        ))),
    }
}
```

(See `transformation_from_ir` and `snapshot_from_ir` for the parallel forms.)

The `Result<_, SqlGenError::InvalidRequest>` is a runtime variant-mismatch tripwire. Type-system enforcement was lost when sql_gen flipped from variant-typed `&{Replication,Transformation,Snapshot}Plan` to a single `&ModelIr` (§Phase 2b of the typed-IR backlog) — the IR doesn't tag its variant. **Today this branch is dead code**: every production callsite hands the matching variant. The branch exists as a future drift detector — if a new construction site forgets which entry to call, the error fires immediately rather than producing wrong SQL silently.

If a future refactor introduces a sealed variant tag on `ModelIr` (an `enum ModelVariant { Replication, Transformation, Snapshot }` field, or a return-to-variant-typed-`Plan` path through `sql_gen`), the tripwire becomes redundant and can be removed.

---

## 9. Explicit gap list

Things the IR doesn't yet carry that it should — or, more precisely, that the typed-program-info layer needs to carry somewhere and the IR is the natural home for. Each gap maps to an arc work item.

### Determinism tags

There is no `Determinism` modeling on `TypedColumn` or anywhere else. SQL functions like `NOW()`, `RANDOM()`, `UUID()` produce non-deterministic output; the IR has no way to express this. Closing the gap requires a typed enum (`enum Determinism { Pure, NonDeterministic(Reason) }`) flowing through the SQL parser to detect non-deterministic calls, plus a sidecar field on `TypedColumn` (or an analogous IR-level field).

**Why it matters:** content-addressed writes want determinism guarantees on a per-model basis — a model that contains `NOW()` needs different replay semantics than a model that doesn't.

### Replay-recipe primitives beyond hash

The IR carries `recipe_hash` (this spec, §7). It does not carry `input_hash` (the hash of all upstream model outputs the model consumed) or `env_hash` (the hash of the environment configuration). Both are needed for the full content-addressed-write triple `(recipe-hash, input-hash, env-hash)` the planned replay-and-content-address direction depends on.

`input_hash` is logically a runtime computation (it depends on what the upstream models actually produced this run); `env_hash` is logically a project-level value (`ProjectIr` field, derived from `RockyConfig`). Neither belongs on `ModelIr` directly; they live on `RunRecord` or a parallel `RunIr` structure.

### Hooks-in-IR — deferred

`HooksConfig` lives on `RockyConfig` as project-level lifecycle policy. There is no per-model hook attachment today, so there is nothing to lift into `ModelIr`. If a future change introduces per-model hooks (e.g. `[hook]` blocks in model sidecar TOMLs that run before/after that specific model materializes), the resolved-per-model `Vec<HookRef>` should land in `ModelIr` to keep the recipe-hash reflecting "what runs for this model."

Until then, hooks are intentionally absent from the IR and from the recipe-hash.

### Full cost-projection-in-IR

`CostSection` (rates: `$/DBU`, `$/GB-month`) and `BudgetConfig` (limits: `max_usd`, `max_duration_ms`, `max_bytes_scanned`) live on `RockyConfig`. Per-model `[budget]` blocks exist on the model sidecar today. A computed `CostProjection` per model — bytes-scanned estimate × rate — is produced by `optimize.rs` on demand; it is not a field on `ModelIr`.

If cost-projection becomes a stable per-model property (rather than an on-demand computation), the natural shape is `ModelIr.cost_projection: Option<CostProjection>` — `Option` because cost projection requires `EXPLAIN` from the warehouse, which not every callpath can provide.

### Column-level lineage — `ProjectIr.lineage_edges` is the canonical store

[`ModelIr::lineage_edges`] carries only the slice of edges that *target* this model. The full cross-model graph lives on [`ProjectIr::lineage_edges`]. Today both fields are populated by `rocky_compiler::semantic::SemanticGraph` extraction; the `ProjectIr` field is the canonical store and the `ModelIr` slice is denormalized for per-model recipe-hash determinism.

[`ModelIr::lineage_edges`]: src/ir.rs
[`ProjectIr::lineage_edges`]: src/ir.rs

The denormalization is load-bearing: per-model recipe-hash needs the targeting slice to be deterministic per model. Both fields stay in sync at construction time; if one falls out of sync with the other, the recipe-hash will drift in unexpected ways.

### Schema declarations beyond names

[`ModelIr::typed_columns`] carries `Vec<TypedColumn>` — name + `RockyType` + nullable. Replication carries column names (and metadata columns with raw type strings) on the variant-specific fields. The full typed schema (constraints, defaults, comments, foreign keys) is not represented; the warehouse owns that today.

### Multi-frontend (DSL → IR direct)

The Rocky DSL lowers to a SQL string before reaching the IR. A typed DSL→IR path would let DSL programs surface in the IR with their structural shape preserved (typed lineage from DSL pipelines, deterministic IR JSON for AI emit). This is the LLVM-bitcode-shaped move; today's status quo is the intermediate-SQL path.

---

## 10. Adding a new IR field — checklist

When adding a new field to `ModelIr` or `ProjectIr`:

1. **Pick the canonical-JSON attribute pair.** §6 covers the three rules; the right pair depends on the field's type:
   - `Option<T>` → `#[serde(default, skip_serializing_if = "Option::is_none")]`
   - `Vec<T>` (conceptually-optional) → `#[serde(default, skip_serializing_if = "Vec::is_empty")]`
   - `bool` (default-false) → `#[serde(default, skip_serializing_if = "std::ops::Not::not")]`
   - Required `String`, required `enum`: no skip attribute; the field is mandatory in the JSON.

2. **Update `From<&Plan>` and `to_plan_compatible()` if variant-specific.** Both impls live in [`src/ir.rs`](src/ir.rs). If the new field is variant-specific (only meaningful for Replication / Transformation / Snapshot), update both directions of the conversion to populate / re-extract it. If the new field changes which discriminator triggers `to_plan_compatible()`'s variant inference, update §3's variant-inference order *and* the variant-discrimination test (`plan_to_model_ir_replication_with_merge_strategy_roundtrip`) to cover the new ambiguity case.

3. **Add a recipe-hash sensitivity test.** Inline tests in `ir.rs` follow the `recipe_hash_changes_when_<field>_changes` pattern. If the new field doesn't observably change the hash, it is invisible to content-addressed writes — that is either a design decision (state the rationale in the field's doc-comment) or a bug.

4. **Add a canonical-JSON enforcement test if the field is skip-able.** Inline tests follow the `<field>_omitted_from_serialization` pattern (e.g. `empty_sources_omitted_from_replication_serialization`). One assertion that the JSON does not contain the field key when the value is the empty/None/default form.

5. **Update the golden fixtures.** [`engine/crates/rocky-cli/tests/ir-golden/`](../rocky-cli/tests/ir-golden/) carries one fixture per major variant. If the new field changes any fixture's serialization, regenerate the affected `*.ir.json` and update the pinned recipe-hash in the runner's `EXPECTED_HASHES` table. If the new field changes SQL output, regenerate the per-dialect snapshot files.

6. **Update this spec.** §3's group table and §6's Rule A coverage may need a bullet; §9's gap list should shrink (the field is closing a previously-named gap) or be unaffected (the field is structural).

7. **Don't add a `JsonSchema` derive.** The IR is internal contract; consumers outside the engine should depend on the typed `*Output` structs in `rocky-cli::output`, which do derive `JsonSchema`. Adding `JsonSchema` to `ModelIr` would route the IR into the public schema export pipeline — that path requires a stable, versioned IR with semver guarantees, none of which are in scope today.

---

## 11. Versioning policy

**No semver. No public commitment. Refactor at will.**

The IR is internal to the Rocky engine. Cross-PR breakages — a renamed field, a moved type, a serde attribute change — are caught by:

- The inline tests in `src/ir.rs` (byte-stable round-trip, recipe-hash determinism + sensitivity, canonical-JSON Rule A enforcement, variant-extraction round-trip).
- The golden tests in `engine/crates/rocky-cli/tests/ir_golden.rs` (IR JSON snapshots + per-dialect SQL snapshots + pinned recipe hashes per fixture).

When a refactor changes the IR shape intentionally, regenerate the affected fixtures and the pinned hashes; the tests re-pin to the new shape and the next refactor catches the next shift. There is no migration path or compatibility shim.

Consumers outside the engine — Dagster integration, VS Code extension, third-party tooling — depend on the typed `*Output` structs in [`rocky-cli::output`](../rocky-cli/src/output.rs) (which derive `JsonSchema` and back the autogenerated Pydantic and TypeScript bindings). The IR is not part of that surface.

If a future need for a public, versioned IR emerges, the path is: split `rocky-ir` as a separate crate, add `JsonSchema` derives, define a conformance test suite, semver the crate from `1.0`. None of that is in scope today.

---

## 12. Future directions

The IR is the structural anchor for several follow-on workstreams. Each one closes one or more gaps from §9 or relies on the IR's structural guarantees.

| Future direction | What it consumes / produces here |
|---|---|
| **Content-addressed writes + replay re-execution** | Consumes `ModelIr::recipe_hash` as the per-model write address. The two recipe-hash invariants (§5: no `WatermarkState`, `TimeInterval.window=None` at hash time) are load-bearing here. |
| **Per-model cost projection** | Closes the `cost-projection-in-IR` gap (§9). Per-model `[budget]` blocks already shipped; cost-projection-on-IR is the demand-gated next step. |
| **Schema-grounded AI emission** | Emits IR directly. The flat-fields design (§3) and Rule A canonical encoding (§6) make `ModelIr` the natural emit target for an LLM — a JSON-Schema export of `ModelIr` would make the wedge testable. The gap on multi-frontend (§9) closes when the AI emit path reaches IR rather than DSL/SQL. |
| **Determinism + extended replay-recipe primitives** | Closes the `determinism-tags` and `replay-recipe primitives beyond hash` gaps (§9). Adds `Determinism` modeling to `TypedColumn`; introduces `input_hash` + `env_hash` alongside `recipe_hash`. |
| **Full cross-model lineage graph** | Closes the `column-level lineage full graph` and `schema declarations beyond names` gaps (§9). Builds out `ProjectIr.lineage_edges` as the load-bearing source of cross-model column lineage. |

Internal planning documents that detail these directions live outside this repo.

---

## Where this document lives

- This file: `engine/crates/rocky-core/SPEC.md` — the synthesizing narrative.
- Field-level semantics: [`engine/crates/rocky-core/src/ir.rs`](src/ir.rs) doc-comments — source of truth.
- Inline regression tests: same file, `#[cfg(test)] mod tests` block — pin every invariant the spec describes.
- Golden tests: [`engine/crates/rocky-cli/tests/ir_golden.rs`](../rocky-cli/tests/ir_golden.rs) + [`engine/crates/rocky-cli/tests/ir-golden/`](../rocky-cli/tests/ir-golden/) — fixture set covering the materialization-strategy matrix × four dialects, with pinned recipe hashes.

When the doc-comments and this spec disagree, the doc-comments win. When the inline tests and the spec disagree, the tests win. When the golden snapshots and the spec disagree, regenerate the snapshots if the change was intentional and update the spec; otherwise the snapshots win.
