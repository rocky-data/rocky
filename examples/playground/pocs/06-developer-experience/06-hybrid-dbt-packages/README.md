# 06-hybrid-dbt-packages — Rocky alongside dbt packages

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** external source references, `[[sources]]` metadata, transformation pipeline, cross-tool lineage

## What it shows

Rocky models read tables produced by dbt packages (Fivetran facebook_ads and stripe) without converting or managing those packages. Seed data simulates the Fivetran raw and dbt staging tables (`fivetran_facebook_ads.*`, `dbt_fivetran.*`, `dbt_stripe.*`).

Schema-qualified names (`dbt_fivetran.stg_facebook_ads__*`, `dbt_stripe.stg_stripe__*`) are classified as external: they do **not** appear in `depends_on`, and Rocky does not build or refresh them. Bare model references create DAG edges.

## DAG

```
dbt_fivetran.stg_facebook_ads__* ─(external)─▶ facebook_campaign_performance
dbt_fivetran.stg_facebook_ads__* ─(external)─▶ facebook_daily_trends
dbt_stripe.stg_stripe__charges   ─(external)─▶ stripe_revenue_daily
                                                         │
                    facebook_daily_trends ──(depends_on)──┤
                    stripe_revenue_daily  ──(depends_on)──┘
                                                         ▼
                                              combined_marketing_revenue
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

- `validate` reports 4 transformation models, DAG valid, no errors
- `compile` type-checks all models (external source columns are `Unknown` type with no warehouse introspection in DuckDB mode)
- `lineage` traces column-level edges from `stripe_revenue_daily` and `facebook_daily_trends` into `combined_marketing_revenue`

## Real-world workflow

dbt owns the package-generated staging layer and Rocky the analytics layer. CI runs both:

```yaml
- name: dbt run (packages)
  run: dbt run --select tag:fivetran

- name: Rocky run (analytics)
  run: rocky -c rocky.toml run
```

## Related

- dbt import POC: [`06-developer-experience/03-import-dbt-validate`](../03-import-dbt-validate/)
- Fivetran facebook_ads package: https://hub.getdbt.com/fivetran/facebook_ads/latest/
- Fivetran stripe package: https://hub.getdbt.com/fivetran/stripe/latest/
