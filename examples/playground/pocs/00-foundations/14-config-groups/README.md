# 14-config-groups — governed materialization fan-out

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** config groups (`models/groups/<name>.toml`), `schema_template` + `[args]` routing, group `[strategy]`, group `[tags]` inheritance, `enforce`, the misplacement guard

## What it shows

When many models share the same routing and materialization, you define a
**config group** once and have each model opt in by name. The group lives in
`models/groups/<name>.toml` (the file stem is the group name) and supplies:

- a **`schema_template`** like `mart_{region}` — each member fills the
  `{region}` placeholder from its own `[args]`, so one definition fans out
  across distinct target schemas;
- a shared **`[strategy]`** — one materialization for the whole group;
- a **`[tags]`** baseline — a governance attribute applied once on the group
  lands on every member.

This POC has one group, `daily_marts`, and three members that each opt in
with `group = "daily_marts"` and a different `[args] region`:

| Model | `[args] region` | Resolved schema |
|---|---|---|
| `fct_orders_emea` | `emea` | `mart_emea` |
| `fct_orders_us_west` | `us_west` | `mart_us_west` |
| `fct_orders_apac` | `apac` | `mart_apac` |

`run.sh` compiles them with `rocky compile --output json` and asserts the
members resolved to **three distinct target schemas** and **inherited the
group's `[tags]`** (`domain = "sales"`). `fct_orders_us_west` also overrides
one tag (`tier = "silver"`) to show the per-key merge: it keeps the
inherited `domain` while pinning its own `tier`.

## The two load-time guards

A config group makes routing *easy* — and routing mistakes *loud*. Two
deliberately-broken siblings under `broken/` show the guards that fail the
load instead of silently misrouting a model. `run.sh` runs each and asserts
the expected error.

### Guard A — misplaced `[args]` (`broken/misplaced-args/`)

`[args]` exist only to fill a group's `schema_template`. If a member also
pins its own `target.schema`, the pin wins (sidecar > group), the template is
bypassed, and the `[args]` become dead — usually masking a routing mistake.
Rocky fails the load:

```
model 'fct_orders_broken' supplies [args] for config group 'daily_marts's
schema_template but also pins its own target.schema; the pin overrides the
template, so the [args] are dead. ...
```

Fix it one way or the other: pin a schema (drop `[args]`), **or** route via
`[args]` (drop the pinned schema). Not both.

### Guard B — overriding an enforced group (`broken/enforce-override/`)

By default a group is an overridable **default**: a member may pin its own
schema or strategy to diverge. Set `enforce = true` to make the group's
fields **binding** — a governance guarantee that the whole fan-out shares
routing and materialization. A member that then pins a field the enforced
group owns (its target `schema` or its `strategy`) fails the load:

```
model 'fct_ledger_broken' overrides 'target.schema', which its enforced
group 'regulated' controls; remove the local override or set the group's
enforce = false
```

Enforcement is strictly opt-in. Without `enforce`, the same pin is allowed.

## Why it's distinctive

- One group definition routes a fan-out across many schemas with no codegen
  or templating step — the dbt analogue would be a macro + per-model config
  sprawl.
- Governance attributes (`[tags]`) declared once on the group land on every
  member, and `dagster-rocky` projects them onto each derived asset's Dagster
  tags, so the governed fan-out is visible to the orchestrator end-to-end.
- `enforce = true` turns the group from a convenience default into a binding
  contract: a regulated set of models *cannot* quietly route or materialize
  itself differently from the rest. That's enforcement, not just a lint.

## Layout

```
.
├── README.md
├── rocky.toml                         pipeline + adapter (DuckDB)
├── run.sh                             fan-out asserts + both broken-sibling asserts
├── models/
│   ├── groups/
│   │   └── daily_marts.toml           the group: schema_template + [strategy] + [tags]
│   ├── fct_orders_emea.sql + .toml    member, [args] region = "emea"
│   ├── fct_orders_us_west.sql + .toml member, [args] region = "us_west" (+ tier override)
│   └── fct_orders_apac.sql + .toml    member, [args] region = "apac"
└── broken/
    ├── misplaced-args/models/         Guard A: pins schema AND supplies [args]
    │   ├── groups/daily_marts.toml              (non-enforced)
    │   └── fct_orders_broken.sql + .toml
    └── enforce-override/models/       Guard B: pins schema under an enforced group
        ├── groups/regulated.toml               (enforce = true)
        └── fct_ledger_broken.sql + .toml
```

> The broken siblings are plain `models/` trees, not standalone pipelines —
> `run.sh` compiles each with `rocky compile --models broken/<case>/models`,
> and the parent `rocky.toml` auto-loads from the POC dir. Each carries its
> own copy of `models/groups/` because a group resolves relative to the
> compiled models directory (`<models_dir>/groups/`). The model loader does
> not recurse into subdirectories, so `models/groups/` is never mistaken for
> model files. They live under `broken/` (not the main `models/` tree)
> because a group guard is a *load* error that aborts the whole load — it
> can't sit beside the good fan-out the way a per-model contract diagnostic
> can.

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

1. A member can still pin its own `schema` / `strategy` to override the group
   (unless the group is enforced).
2. Otherwise the group's `schema_template` (filled from `[args]`) and
   `[strategy]` apply.
3. Otherwise directory defaults from `_defaults.toml`.

Tags merge per key (sidecar wins per key, the rest of the group's tags stay).
An unknown group name, an unfilled `schema_template` placeholder, a pinned
schema alongside `[args]`, or overriding an enforced field all fail the load
with a clear error rather than routing a model to the wrong place.

## Related

- Model format reference — Config groups:
  `docs/src/content/docs/reference/model-format.md` (`#config-groups`).
- The three-layer config + env-var story:
  `examples/playground/pocs/00-foundations/07-config-layering/`.
- The group loader + guards live at
  `engine/crates/rocky-core/src/models.rs` (`load_groups_from_dir`,
  `resolve_model_config`).
