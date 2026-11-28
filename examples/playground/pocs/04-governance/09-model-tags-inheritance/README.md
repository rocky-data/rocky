# 09-model-tags-inheritance — governance tags: group baseline + per-key override

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** model `[tags]`, config-group `[tags]` baseline, per-key tag merge (sidecar > group), `models_detail[].tags` on `rocky compile --output json`, dagster-rocky asset-tag projection

## What it shows

Model `[tags]` are free-form attributes of a whole model (`domain`, `tier`,
`owner`), unlike column-keyed `[classification]`, which drives masking. A
**config group** declares a `[tags]` baseline that members inherit. A member
overrides single keys, not the whole set.

The `finance` group declares `domain = "finance"`, `tier = "silver"`:

| Model | Own `[tags]` | Resolved tags |
|---|---|---|
| `fct_revenue` | `tier = "gold"` | `domain = "finance"`, `tier = "gold"` |
| `dim_account` | _(none)_ | `domain = "finance"`, `tier = "silver"` |

## Precedence rules

Per key, **sidecar > group**: the model's own value wins, else the group's. A
model in no group carries only its own `[tags]`.

## The dagster asset-tag projection

`dagster-rocky` (`RockyDagsterTranslator.get_model_tags()`) emits each
`models_detail[].tags` entry as a Dagster tag, usable in asset selection (for
example `tag:domain=finance` or `tag:tier=gold`). It also adds `rocky/model_name`,
`rocky/target_catalog`, `rocky/target_schema` and `rocky/strategy`. Those keys
contain a `/` and governance keys never do, so they cannot clobber each other.

## Run

```bash
./run.sh
```

It compiles with `rocky compile --output json` and asserts each model's
`models_detail[].tags`.

## Expected output

```text
Resolved governance tags per model (models_detail[].tags):
  dim_account    -> {domain=finance, tier=silver}
  fct_revenue    -> {domain=finance, tier=gold}
```

## Related

- Config-group `schema_template` + `[args]` routing and load guards:
  `examples/playground/pocs/00-foundations/14-config-groups/`.
- Model format reference, `[tags]` and Group tags:
  `docs/src/content/docs/reference/model-format.md` (`#tags`, `#group-tags`).
