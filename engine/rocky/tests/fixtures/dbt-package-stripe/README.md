# Recorded `fivetran/stripe` compile

Backs `rocky/tests/package_vendor.rs`. CI exercises `rocky package` against
this recording with `--compiled`, so it needs no dbt and no network.

- `seed.sql` creates the Fivetran Stripe connector tables (`stripe.*`) from the
  package's own `integration_tests/seeds` CSVs. Ids are strings, as in Stripe.
- `compiled/` is a dbt project after `dbt deps`, `dbt run --empty
  --full-refresh` and `dbt compile --full-refresh`, trimmed by
  `trim_manifest.py` to the fields Rocky reads.

Recorded with dbt-core 1.12.5 + dbt-duckdb 1.11.0, `fivetran/stripe` 1.10.1,
against a DuckDB file named `dev.duckdb` (so dbt's database is `dev`).

To re-record: seed `dev.duckdb` with `seed.sql`, write the `packages.yml`,
`dbt_project.yml` and `profiles.yml` that `rocky package add
fivetran/stripe@1.10.1` writes (profile and project `rocky_package_build`,
schema `rocky_package_build`), run the three dbt commands above, then
`python trim_manifest.py <that dir> compiled` and strip the absolute DuckDB
path from `compiled/target/manifest.json`.

The compiled SQL and seed data derive from `fivetran/dbt_stripe`,
`fivetran/dbt_fivetran_utils` and `dbt-labs/dbt-utils`, all Apache-2.0.
