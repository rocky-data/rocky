# 06-hybrid-dbt-packages — Rocky alongside dbt packages

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** external source references, `[[sources]]` metadata, transformation pipeline, cross-tool lineage

## What it shows

Rocky models consuming tables produced by dbt packages (Fivetran facebook_ads, Fivetran stripe) without converting or managing those packages. Seed data simulates the staging tables that dbt packages create in the warehouse. Rocky references them as external sources via schema-qualified names (`dbt_fivetran.stg_facebook_ads__*`, `dbt_stripe.stg_stripe__*`), builds analytics on top, and traces lineage through the boundary.

## Why it's distinctive

- Demonstrates the **pragmatic migration path**: keep battle-tested dbt packages, write new business logic in Rocky.
- Shows that Rocky's resolver automatically classifies two-part table references as external (no DAG dependency), while bare model references create proper DAG edges.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── data/
│   └── seed.sql                           # Simulates Fivetran raw + dbt package staging tables
├── models/
│   ├── _defaults.toml
│   ├── facebook_campaign_performance.sql   # Reads dbt_fivetran.stg_facebook_ads__*
│   ├── facebook_campaign_performance.toml
│   ├── facebook_daily_trends.sql           # Daily grain from same external sources
│   ├── facebook_daily_trends.toml
│   ├── stripe_revenue_daily.sql            # Reads dbt_stripe.stg_stripe__charges
│   ├── stripe_revenue_daily.toml
│   ├── combined_marketing_revenue.sql      # Joins Rocky models (DAG edges)
│   └── combined_marketing_revenue.toml
└── expected/
```

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

External sources do **not** appear in `depends_on` — Rocky knows they exist but does not try to build or refresh them.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

- `validate` reports 4 transformation models, DAG valid, no errors
- `compile` type-checks all models — external source columns are `Unknown` type (no warehouse introspection in DuckDB mode)
- `lineage` traces column-level edges from `stripe_revenue_daily` and `facebook_daily_trends` into `combined_marketing_revenue`

## What happened

1. Seed data creates schemas simulating what Fivetran connectors and dbt packages produce (`fivetran_facebook_ads.*`, `dbt_fivetran.*`, `dbt_stripe.*`).
2. `rocky validate` confirms the transformation pipeline config is valid and the 4-model DAG has no cycles.
3. `rocky compile` type-checks all Rocky models, resolving external source references without requiring them in the DAG.
4. `rocky lineage` traces column-level lineage from `combined_marketing_revenue` back through Rocky models and into the external dbt package tables.

## Real-world workflow

In production, the CI pipeline runs both tools:

```yaml
- name: dbt run (packages)
  run: dbt run --select tag:fivetran

- name: Rocky run (analytics)
  run: rocky -c rocky.toml run
```

dbt owns the package-generated staging layer. Rocky owns the analytics layer on top. Each tool manages its own models — no conversion needed.

## Related

- dbt import POC: [`06-developer-experience/03-import-dbt-validate`](../03-import-dbt-validate/)
- Fivetran facebook_ads package: https://hub.getdbt.com/fivetran/facebook_ads/latest/
- Fivetran stripe package: https://hub.getdbt.com/fivetran/stripe/latest/
