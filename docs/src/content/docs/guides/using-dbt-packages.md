---
title: Using Rocky with dbt Packages
description: Run Rocky alongside dbt packages such as Fivetran, and build analytics on top of package-managed tables without converting them.
sidebar:
  order: 3
---

You do not need to convert your dbt packages to use Rocky. A package such as [fivetran/facebook_ads](https://hub.getdbt.com/fivetran/facebook_ads/latest/) or [fivetran/stripe](https://hub.getdbt.com/fivetran/stripe/latest/) produces tables in your warehouse. Rocky references those tables directly, as external sources. An external source is a table Rocky reads but never builds, refreshes, or manages. So you keep vendor-maintained staging packages in dbt and write your custom analytics in Rocky.

## How Rocky classifies a table reference

Rocky's SQL resolver classifies each table reference by how it is qualified:

| Reference in SQL | Rocky classification | DAG behavior |
|---|---|---|
| `my_model` (bare, matches a Rocky model) | Model dependency | Added to `depends_on`, execution ordered |
| `my_model` (bare, no matching model) | External reference | No dependency, no execution |
| `dbt_fivetran.stg_facebook_ads__ad_history` (two-part) | External source | No dependency |
| `analytics.dbt_fivetran.stg_facebook_ads__ad_history` (three-part) | Fully qualified external | No dependency |

A two-part or three-part reference is always external. Rocky reads from it, and never tries to build, refresh, or manage it. Only a bare name that matches another Rocky model in the project becomes a DAG edge.

## Example: Rocky on top of Fivetran dbt packages

### The setup

Your warehouse has tables produced by dbt packages:

```
dbt_fivetran.stg_facebook_ads__ad_history
dbt_fivetran.stg_facebook_ads__campaign_history
dbt_fivetran.stg_facebook_ads__ad_report_daily
dbt_stripe.stg_stripe__charges
```

`dbt run` manages these tables through the `fivetran/facebook_ads` and `fivetran/stripe` packages. Rocky does not touch them.

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
name = "facebook_campaign_performance"

[strategy]
type = "full_refresh"

[target]
catalog = "analytics"
schema = "marketing"
table = "facebook_campaign_performance"

# External sources — tables managed by dbt, not by Rocky.
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
```

The `[[sources]]` entries are optional metadata for lineage documentation. Rocky resolves the real table references from the SQL. It recognizes the `dbt_fivetran.stg_*` references as external, so they create no DAG dependencies.

### Combining Rocky models

A Rocky model that references another Rocky model does create a DAG dependency:

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
name = "combined_marketing_revenue"
depends_on = ["stripe_revenue_daily", "facebook_daily_trends"]

[strategy]
type = "full_refresh"

[target]
catalog = "analytics"
schema = "marketing"
table = "combined_marketing_revenue"
```

`stripe_revenue_daily` and `facebook_daily_trends` are other Rocky models. They appear in `depends_on`, and Rocky runs them first.

### The DAG

```
dbt_fivetran.stg_facebook_ads__* --(external)---> facebook_campaign_performance
dbt_fivetran.stg_facebook_ads__* --(external)---> facebook_daily_trends
dbt_stripe.stg_stripe__charges   --(external)---> stripe_revenue_daily

                       facebook_daily_trends --(depends_on)-+
                                                            |
                       stripe_revenue_daily  --(depends_on)-+
                                                            v
                                                combined_marketing_revenue
```

External sources appear in lineage, but not in the execution DAG.

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

A `transformation` pipeline needs no `source.schema_pattern` and no `catalog_template`, unlike a `replication` pipeline. Each model's TOML sidecar defines its own target catalog and schema.

## CI pipeline: running both tools

Run dbt first, for the packages, then Rocky, for your custom analytics:

```yaml
# GitHub Actions
steps:
  - name: Install dbt packages
    run: dbt deps

  - name: dbt run (Fivetran staging packages)
    run: dbt run --select tag:fivetran

  - name: Rocky compile (type-check analytics models)
    run: rocky compile --models ./models

  - name: Rocky plan + apply (analytics layer)
    run: |
      plan_id=$(rocky plan --pipeline analytics --output json | jq -r .plan_id)
      rocky apply "$plan_id"
```

## Lineage across the boundary

Rocky traces column-level lineage through external sources when schema information is available:

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

Rocky can introspect external columns at compile time when it is connected to the warehouse, which gives you full type checking. Without a connection it types those columns as `Unknown` and relaxes type checking for those references.

## When to convert vs. when to keep dbt packages

Rocky reads a dbt package's output tables as external sources, so nothing forces a conversion. Decide per case:

| Scenario | What works |
|---|---|
| Vendor-maintained staging packages (Fivetran, Airbyte) | The package keeps running under dbt, and Rocky reads its output tables as external sources |
| Custom macros shared across models | The Rocky equivalent is a CTE or a separate model |
| Simple `ref()`-based models with no Jinja | `rocky import-dbt` converts these directly |
| Models using `{{ var() }}`, `{% for %}`, or custom macros | Keep them in dbt, or use the [manifest fast path](/guides/migrate-from-dbt/#manifest-fast-path) |
| New analytics models built on existing tables | Write them directly in Rocky, in SQL or the DSL |

## Try it locally

A self-contained DuckDB POC demonstrates this workflow end to end:

```bash
cd examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages
./run.sh
```

The POC seeds tables that simulate Fivetran facebook_ads and stripe dbt package output. It then builds Rocky analytics models on top, with full compilation and lineage.

## Related

- [Migrating from dbt](/guides/migrate-from-dbt/) — the full conversion workflow, for when you want to move a project over entirely
- [DAG & Dependencies](/concepts/dag-dependencies/) — how Rocky resolves model dependencies and external references
- [Silver Layer (Models)](/concepts/silver-layer/) — model configuration and materialization strategies
