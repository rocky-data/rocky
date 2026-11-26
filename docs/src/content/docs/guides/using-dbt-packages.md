---title: Read dbt Package Tables
description: Run Rocky alongside dbt packages such as Fivetran and build on package-managed tables, or vendor the package with rocky package.
sidebar:
  order: 9
---

There are two ways to use a dbt package such as a Fivetran connector package with Rocky:

- **Vendor it** with [`rocky package add`](/guides/dbt-packages/). dbt compiles the package once and the models become Rocky models, which Rocky type-checks, runs and tests. Use this when one tool should own the whole DAG.
- **Run it alongside**, as this page describes. dbt keeps building the package and Rocky reads its tables as **external sources**: tables Rocky reads but never builds, refreshes or manages. Use this when dbt already runs the package and you add models on top.

You do not need to convert a package such as [fivetran/facebook_ads](https://hub.getdbt.com/fivetran/facebook_ads/latest/) or [fivetran/stripe](https://hub.getdbt.com/fivetran/stripe/latest/). Keep vendor-maintained staging in dbt and write custom analytics in Rocky.

## How Rocky classifies a table reference

| Reference in SQL | Rocky classification | DAG behavior |
|---|---|---|
| `my_model` (bare, matches a Rocky model) | Model dependency | Added to `depends_on`, execution ordered |
| `my_model` (bare, no matching model) | External reference | No dependency, no execution |
| `dbt_fivetran.stg_facebook_ads__ad_history` (two-part) | External source | No dependency |
| `analytics.dbt_fivetran.stg_facebook_ads__ad_history` (three-part) | Fully qualified external | No dependency |

A two-part or three-part reference is always external. Only a bare name that another Rocky model writes as its `[target]` table becomes a DAG edge.

## Example: Rocky on top of Fivetran dbt packages

The warehouse holds tables that `dbt run` builds through the `fivetran/facebook_ads` and `fivetran/stripe` packages (`dbt_fivetran.stg_facebook_ads__ad_history`, `dbt_fivetran.stg_facebook_ads__campaign_history`, `dbt_fivetran.stg_facebook_ads__ad_report_daily`, `dbt_stripe.stg_stripe__charges`). Rocky models consume them:

**models/facebook_campaign_performance.sql**

```sql
SELECT
    c.campaign_id,
    c.campaign_name,
    SUM(r.spend)            AS total_spend,
    SUM(r.conversions)      AS total_conversions,
    ROUND(SUM(r.spend) / NULLIF(SUM(r.conversions), 0), 2) AS cost_per_conversion
FROM dbt_fivetran.stg_facebook_ads__campaign_history  c
JOIN dbt_fivetran.stg_facebook_ads__ad_history        a ON a.campaign_id = c.campaign_id
JOIN dbt_fivetran.stg_facebook_ads__ad_report_daily   r ON r.ad_id = a.ad_id
WHERE a.ad_status = 'ACTIVE'
GROUP BY c.campaign_id, c.campaign_name
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

# External sources: managed by dbt, not by Rocky. Optional lineage metadata.
[[sources]]
catalog = "analytics"
schema = "dbt_fivetran"
table = "stg_facebook_ads__campaign_history"
```

Rocky resolves the real references from the SQL, so the `[[sources]]` entries are optional. A Rocky model that references another Rocky model does create a DAG dependency: list it in `depends_on`.

```toml
# models/combined_marketing_revenue.toml (SQL joins stripe_revenue_daily and facebook_daily_trends)
name = "combined_marketing_revenue"
depends_on = ["stripe_revenue_daily", "facebook_daily_trends"]
```

```
dbt_fivetran.stg_facebook_ads__* --(external)---> facebook_campaign_performance
dbt_fivetran.stg_facebook_ads__* --(external)---> facebook_daily_trends
dbt_stripe.stg_stripe__charges   --(external)---> stripe_revenue_daily

                       facebook_daily_trends --(depends_on)-+
                       stripe_revenue_daily  --(depends_on)-+
                                                            v
                                                combined_marketing_revenue
```

External sources appear in lineage, not in the execution DAG.

## Pipeline and CI

Use a `transformation` pipeline. It needs no `source.schema_pattern` or `catalog_template`: each model's sidecar sets its own target.

```toml
[pipeline.analytics]
type = "transformation"

[pipeline.analytics.target]
adapter = "prod"
```

In CI, run dbt first for the packages, then Rocky:

```yaml
steps:
  - run: dbt deps
  - run: dbt run --select tag:fivetran
  - run: rocky compile --models ./models
  - run: |
      plan_id=$(rocky plan --pipeline analytics --output json | jq -r .plan_id)
      rocky apply "$plan_id"
```

## Lineage across the boundary

`rocky lineage combined_marketing_revenue --models ./models` traces column-level lineage from Rocky models back through external tables when schema information is available. Connected to the warehouse, Rocky introspects external columns at compile time for full type checking. Without a connection it types those columns `Unknown` and relaxes type checking for those references.

## When to convert vs. when to keep dbt packages

| Scenario | What works |
|---|---|
| Vendor-maintained staging packages (Fivetran, Airbyte) | Keep running under dbt. Rocky reads the output tables as external sources. |
| Custom macros shared across models | The Rocky equivalent is a CTE or a separate model. |
| Simple `ref()`-based models with no Jinja | `rocky import-dbt` converts these directly. |
| Models using `{{ var() }}`, `{% for %}` or custom macros | Keep them in dbt, or use the [manifest fast path](/guides/migrate-from-dbt/#manifest-fast-path). |
| New analytics models on existing tables | Write them in Rocky, in SQL or the DSL. |

A self-contained DuckDB POC runs this end to end: `cd examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages && ./run.sh`. It seeds tables that simulate the package output and builds Rocky models on top.

## Related

- [Migrating from dbt](/guides/migrate-from-dbt/): the full conversion workflow.
- [DAG & Dependencies](/concepts/dag-dependencies/): how Rocky resolves dependencies and external references.
- [Silver Layer (Models)](/concepts/silver-layer/): model configuration and materialization strategies.
