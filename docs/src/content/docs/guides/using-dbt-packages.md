---
title: Using Rocky with dbt Packages
description: Run Rocky alongside dbt packages like Fivetran and build analytics on top of package-managed tables without converting them
sidebar:
  order: 3
---

You don't need to convert your dbt packages to use Rocky. Packages like [fivetran/facebook_ads](https://hub.getdbt.com/fivetran/facebook_ads/latest/) or [fivetran/stripe](https://hub.getdbt.com/fivetran/stripe/latest/) produce tables in your warehouse that Rocky can reference directly as external sources. This lets you keep battle-tested staging packages while writing your custom analytics in Rocky.

## How it works

Rocky's SQL resolver classifies table references by how they are qualified:

| Reference in SQL | Rocky classification | DAG behavior |
|---|---|---|
| `my_model` (bare, matches a Rocky model) | Model dependency | Added to `depends_on`, execution ordered |
| `my_model` (bare, no matching model) | External reference | No dependency, no execution |
| `dbt_fivetran.stg_facebook_ads__ad_history` (two-part) | External source | No dependency |
| `analytics.dbt_fivetran.stg_facebook_ads__ad_history` (three-part) | Fully qualified external | No dependency |

Two-part and three-part table references are always treated as external. Rocky reads from them but does not attempt to build, refresh, or manage them. Only bare names that match another Rocky model in the project become DAG edges.

This means your Rocky models can reference any table in the warehouse -- regardless of what tool created it.

## Example: Rocky on top of Fivetran dbt packages

### The setup

Your warehouse has tables produced by dbt packages:

```
dbt_fivetran.stg_facebook_ads__ad_history
dbt_fivetran.stg_facebook_ads__campaign_history
dbt_fivetran.stg_facebook_ads__ad_report_daily
dbt_stripe.stg_stripe__charges
```

These are managed by `dbt run` using the `fivetran/facebook_ads` and `fivetran/stripe` packages. Rocky does not touch them.

### Rocky models

Create Rocky models that consume the dbt package tables:

**models/facebook_campaign_performance.sql**

```sql
SELECT
    c.campaign_id,
    c.campaign_name,
    c.objective,
    SUM(r.impressions)      AS total_impressions,
    SUM(r.clicks)           AS total_clicks,
    SUM(r.spend)            AS total_spend,
    SUM(r.conversions)      AS total_conversions,
    ROUND(SUM(r.spend) / NULLIF(SUM(r.conversions), 0), 2) AS cost_per_conversion
FROM dbt_fivetran.stg_facebook_ads__campaign_history  c
JOIN dbt_fivetran.stg_facebook_ads__ad_history        a ON a.campaign_id = c.campaign_id
JOIN dbt_fivetran.stg_facebook_ads__ad_report_daily   r ON r.ad_id = a.ad_id
WHERE a.ad_status = 'ACTIVE'
GROUP BY c.campaign_id, c.campaign_name, c.objective
```

**models/facebook_campaign_performance.toml**

```toml
[strategy]
type = "full_refresh"

# External sources -- tables managed by dbt, not by Rocky.
# Listed for lineage documentation; Rocky does not create or refresh them.
[[sources]]
catalog = "analytics"
schema = "dbt_fivetran"
table = "stg_facebook_ads__campaign_history"

[[sources]]
catalog = "analytics"
schema = "dbt_fivetran"
table = "stg_facebook_ads__ad_history"

[[sources]]
catalog = "analytics"
schema = "dbt_fivetran"
table = "stg_facebook_ads__ad_report_daily"

[target]
catalog = "analytics"
schema = "marketing"
table = "facebook_campaign_performance"
```

The `[[sources]]` entries are optional metadata for lineage documentation. Rocky resolves the actual table references from the SQL -- the `dbt_fivetran.stg_*` references are recognized as external and do not create DAG dependencies.

### Combining Rocky models

Rocky models that reference other Rocky models do create DAG dependencies:

**models/combined_marketing_revenue.sql**

```sql
SELECT
    s.created_date            AS report_date,
    s.revenue_usd             AS stripe_revenue,
    SUM(f.total_spend)        AS facebook_spend,
    s.revenue_usd - COALESCE(SUM(f.total_spend), 0) AS net_after_ads
FROM stripe_revenue_daily s
LEFT JOIN facebook_daily_trends f ON f.report_date = s.created_date
GROUP BY s.created_date, s.revenue_usd
```

**models/combined_marketing_revenue.toml**

```toml
[strategy]
type = "full_refresh"

depends_on = ["stripe_revenue_daily", "facebook_daily_trends"]

[target]
catalog = "analytics"
schema = "marketing"
table = "combined_marketing_revenue"
```

Here, `stripe_revenue_daily` and `facebook_daily_trends` are other Rocky models, so they appear in `depends_on` and Rocky ensures they run first.

### The DAG

```
dbt_fivetran.stg_facebook_ads__* --(external)---> facebook_campaign_performance
dbt_fivetran.stg_facebook_ads__* --(external)---> facebook_daily_trends
dbt_stripe.stg_stripe__charges   --(external)---> stripe_revenue_daily
                                                            |
                       facebook_daily_trends --(depends_on)-+
                       stripe_revenue_daily  --(depends_on)-+
                                                            v
                                                combined_marketing_revenue
```

External sources appear in lineage but not in the execution DAG. Rocky only schedules and executes models it owns.

## Pipeline configuration

Use a `transformation` pipeline for models that read from external sources:

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"

[pipeline.analytics]
type = "transformation"

[pipeline.analytics.target]
adapter = "prod"
```

Unlike `replication` pipelines, `transformation` pipelines don't need `source.schema_pattern` or `catalog_template` -- the target catalog and schema are defined per model in each model's TOML sidecar.

## CI pipeline: running both tools

In production, your CI pipeline runs dbt first (for packages) and Rocky second (for your custom analytics):

```yaml
# GitHub Actions
steps:
  - name: Install dbt packages
    run: dbt deps

  - name: dbt run (Fivetran staging packages)
    run: dbt run --select tag:fivetran

  - name: Rocky compile (type-check analytics models)
    run: rocky compile --models ./models

  - name: Rocky run (analytics layer)
    run: rocky run --pipeline analytics
```

Each tool manages its own models independently. dbt owns the package-generated staging layer; Rocky owns the analytics layer on top.

## Lineage across the boundary

Rocky traces column-level lineage through external sources when schema information is available. Run:

```bash
rocky lineage combined_marketing_revenue --models ./models
```

The output shows edges from Rocky models back through to the external dbt package tables:

```json
{
  "model": "combined_marketing_revenue",
  "upstream": ["stripe_revenue_daily", "facebook_daily_trends"],
  "edges": [
    {
      "source": { "model": "stripe_revenue_daily", "column": "revenue_usd" },
      "target": { "model": "combined_marketing_revenue", "column": "stripe_revenue" },
      "transform": "direct"
    }
  ]
}
```

For full type checking of columns from external sources, Rocky can introspect them at compile time if connected to the warehouse. Without a connection, external columns are typed as `Unknown` and type checking is relaxed for those references.

## When to convert vs. when to keep dbt packages

| Scenario | Recommendation |
|---|---|
| Vendor-maintained staging packages (Fivetran, Airbyte) | Keep in dbt -- they're actively maintained and tested |
| Custom macros shared across models | Consider converting to Rocky CTEs or separate models |
| Simple `ref()`-based models with no Jinja | Good candidates for conversion via `rocky import-dbt` |
| Models using `{{ var() }}`, `{% for %}`, or custom macros | Keep in dbt or use the [manifest fast path](/rocky/guides/migrate-from-dbt/#manifest-fast-path) |
| New analytics models built on existing tables | Write directly in Rocky |

The pragmatic path: let dbt packages handle what they're good at (vendor-maintained staging models), and use Rocky for the transformation layer you own and maintain.

## Try it locally

A self-contained DuckDB POC demonstrates this workflow end-to-end:

```bash
cd examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages
./run.sh
```

The POC seeds tables simulating Fivetran facebook_ads and stripe dbt package output, then builds Rocky analytics models on top with full compilation and lineage.

## Related

- [Migrating from dbt](/rocky/guides/migrate-from-dbt/) -- full conversion workflow when you want to replace dbt entirely
- [DAG & Dependencies](/rocky/concepts/dag-dependencies/) -- how Rocky resolves model dependencies and external references
- [Silver Layer (Models)](/rocky/concepts/silver-layer/) -- model configuration and materialization strategies
