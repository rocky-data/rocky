# 14-config-groups — governed materialization fan-out

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** config groups (`models/groups/<name>.toml`), `schema_template` + `[args]` routing, group `[strategy]`, group `[tags]` inheritance, `enforce`, the misplacement guard

## What it shows

A **config group** (`models/groups/<name>.toml`; the file stem is the group name) defines shared routing and materialization that models opt into by name:

- a **`schema_template`** like `mart_{region}`, filled from each member's `[args]`
- a shared **`[strategy]`**
- a **`[tags]`** baseline inherited by every member (`dagster-rocky` projects these onto each derived asset's Dagster tags)

Three members opt in with `group = "daily_marts"`:

| Model | `[args] region` | Resolved schema |
|---|---|---|
| `fct_orders_emea` | `emea` | `mart_emea` |
| `fct_orders_us_west` | `us_west` | `mart_us_west` |
| `fct_orders_apac` | `apac` | `mart_apac` |

`fct_orders_us_west` also overrides one tag (`tier = "silver"`) while keeping the inherited `domain = "sales"`.

## The two load-time guards

Two deliberately broken trees show them; `run.sh` asserts each error (full text under Expected output).

- **Guard A, misplaced `[args]`** (`broken/misplaced-args/`): a member supplies `[args]` but also pins `target.schema`. The pin wins, so the `[args]` are dead and the load fails. Either drop `[args]` or drop the pinned schema.
- **Guard B, overriding an enforced group** (`broken/enforce-override/`): by default group fields are overridable. With `enforce = true`, a member that pins the target `schema` or `strategy` the group owns fails the load. Enforcement is opt-in.

Each broken tree is compiled with `rocky compile --models broken/<case>/models` (the parent `rocky.toml` auto-loads). Each has its own `models/groups/`, because groups resolve from `<models_dir>/groups/`; the model loader does not recurse, so group files are never read as models. They sit outside the main `models/` because a group guard is a *load* error that aborts the whole load.

## Run

```bash
./run.sh
```

## Expected output

```
=== Part 1: fan-out — three members of group 'daily_marts' ===
    schema_template = "mart_{region}"; each member fills {region} from [args]

Resolved member targets + inherited tags:
  fct_orders_apac      -> warehouse.mart_apac.fct_orders_apac    [domain=sales,tier=gold]
  fct_orders_emea      -> warehouse.mart_emea.fct_orders_emea    [domain=sales,tier=gold]
  fct_orders_us_west   -> warehouse.mart_us_west.fct_orders_us_west [domain=sales,tier=silver]

Distinct schemas: mart_apac mart_emea mart_us_west
OK: group [tags] inherited by all members; us_west tier override merged per-key

=== Part 2: load-time guard A — misplaced [args] (non-enforced group) ===
    broken/misplaced-args pins target.schema AND supplies [args] -> the
    pin bypasses the template, so the [args] are dead. Expect load FAILURE.

    -> failed as expected. Error:
      failed to load models: model 'fct_orders_broken' supplies [args] for config group 'daily_marts's schema_template but also pins its own target.schema; the pin overrides the template, so the [args] are dead. Remove the pinned schema to route via [args], or remove [args] if the pinned schema is intended.

=== Part 3: load-time guard B — overriding an ENFORCED group ===
    broken/enforce-override pins target.schema under an enforced group
    that OWNS the schema. Expect load FAILURE (governance guarantee).

    -> failed as expected. Error:
      failed to load models: model 'fct_ledger_broken' overrides 'target.schema', which its enforced group 'regulated' controls; remove the local override or set the group's enforce = false

POC complete:
  - one config group fanned out to 3 distinct schemas (mart_emea / mart_us_west / mart_apac)
  - every member inherited the group [tags] baseline; us_west overrode tier per-key
  - misplacement guard rejected pin-schema + [args]
  - enforcement guard rejected overriding an enforced group's schema
```

## Precedence rules

Per-model sidecar **>** group **>** `_defaults.toml`:

1. A member can pin its own `schema` / `strategy` (unless the group is enforced).
2. Otherwise the group's `schema_template` and `[strategy]` apply.
3. Otherwise `_defaults.toml`.

Tags merge per key (the sidecar wins per key). An unknown group name, an unfilled placeholder, a pinned schema alongside `[args]`, or overriding an enforced field all fail the load.

## Related

- `docs/src/content/docs/reference/model-format.md` (`#config-groups`)
- [`07-config-layering`](../07-config-layering/)
- Source: `engine/crates/rocky-core/src/models.rs` (`load_groups_from_dir`, `resolve_model_config`)
