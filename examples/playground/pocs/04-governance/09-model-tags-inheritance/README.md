# 09-model-tags-inheritance — governance tags: group baseline + per-key override

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** model `[tags]`, config-group `[tags]` baseline, per-key tag merge (sidecar > group), `models_detail[].tags` on `rocky compile --output json`, dagster-rocky asset-tag projection

## What it shows

Model-level `[tags]` are free-form governance attributes that describe a model
as a whole — `domain`, `tier`, `owner`, whatever your governance model needs.
Unlike `[classification]` (keyed by column, drives masking), tags travel with
the model itself.

When a set of models shares a governance baseline, you declare the `[tags]`
once on a **config group** and every member inherits it. A member can then
override a single key from its own `[tags]` without dropping the rest of the
baseline — the merge is **per key**, sidecar **>** group.

This POC has one group, `finance`, that carries the baseline and two members:

| Model | Own `[tags]` | Resolved tags |
|---|---|---|
| `fct_revenue` | `tier = "gold"` | `domain = "finance"`, `tier = "gold"` |
| `dim_account` | _(none)_ | `domain = "finance"`, `tier = "silver"` |

The group declares `domain = "finance"`, `tier = "silver"` once. `fct_revenue`
overrides only `tier` (to `gold`) and still inherits `domain = "finance"` —
the override does **not** replace the whole tag set. `dim_account` declares no
tags of its own, so it inherits the full baseline verbatim.

`run.sh` compiles both with `rocky compile --output json` and asserts each
model's `models_detail[].tags` matches the table above.

## The dagster asset-tag projection

Resolved tags don't just live in Rocky's view of the model — `dagster-rocky`
projects them onto each derived asset so the same governance attribute drives
both Rocky and the orchestrator.

`RockyDagsterTranslator.get_model_tags()` reads `models_detail[].tags` and
emits each one as a **first-class Dagster tag**, so the governed set is usable
in asset selection directly — for example `tag:domain=finance` selects every
model in the `finance` group, and `tag:tier=gold` narrows to just the
gold-tier members. Alongside the governance tags it synthesizes namespaced
metadata tags (`rocky/model_name`, `rocky/target_catalog`,
`rocky/target_schema`, `rocky/strategy`). The synthesized keys always contain a
`/` and the governance keys never do, so a governance tag can never clobber
Rocky's own metadata.

The net effect: a `tier` baseline set once on the `finance` group, with one
model bumped to `gold`, shows up as `tier=silver` / `tier=gold` Dagster tags on
the corresponding assets — no per-asset tagging code, no drift between the two
systems.

## Why it's distinctive

- A governance attribute applied **once** on a config group lands on the whole
  set of members, and a single member can diverge on one key without losing the
  rest of the baseline. That's inheritance with per-key precedence, not
  copy-paste-per-model.
- The same resolved tags reach the orchestrator: `dagster-rocky` turns them
  into first-class, selectable Dagster tags. Governance declared in the model
  layer is enforced and queryable in the asset graph end-to-end.

## Layout

```
.
├── README.md
├── rocky.toml                    pipeline + adapter (DuckDB)
├── run.sh                        compile + per-model tag assertions
└── models/
    ├── _defaults.toml            shared target (catalog + schema)
    ├── groups/
    │   └── finance.toml          the group: [tags] baseline + shared [strategy]
    ├── fct_revenue.sql + .toml    member, overrides tier -> gold (keeps domain)
    └── dim_account.sql + .toml    member, inherits the full baseline
```

## Run

```bash
./run.sh
```

## Expected output

```text
Resolved governance tags per model (models_detail[].tags):
  dim_account    -> {domain=finance, tier=silver}
  fct_revenue    -> {domain=finance, tier=gold}
```

## Precedence rules

For each tag key, **sidecar > group**:

1. A member model's own `[tags]` value for a key wins.
2. Otherwise the config-group `[tags]` value for that key applies.

The merge is per key — overriding one key keeps every other key inherited from
the group. A model that belongs to no group simply carries its own `[tags]`.

## Related

- Routing fan-out via a config group's `schema_template` + `[args]`, plus the
  enforced-group load guards:
  `examples/playground/pocs/00-foundations/14-config-groups/`.
- Per-column classification tags that drive masking (the column-keyed sibling
  of model `[tags]`):
  `examples/playground/pocs/04-governance/05-classification-masking-compliance/`.
- Model format reference — `[tags]` and Group tags:
  `docs/src/content/docs/reference/model-format.md` (`#tags`, `#group-tags`).
- The dagster tag projection lives at
  `integrations/dagster/src/dagster_rocky/translator.py`
  (`RockyDagsterTranslator.get_model_tags`).
- The group loader + tag merge live at
  `engine/crates/rocky-core/src/models.rs` (`resolve_model_config`).
```
