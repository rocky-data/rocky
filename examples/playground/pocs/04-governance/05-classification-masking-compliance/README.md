# 05-classification-masking-compliance — `[classification]` + `[mask]` + `rocky compliance`

![rocky compliance --env dev rolls up classification tags to mask strategies; --fail-on exception exits 1, gating CI on unmasked PII](../../../../../docs/public/demo-classification-masking.gif)

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `[classification]` sidecar, `[mask]` / `[mask.<env>]` policy,
> `rocky compliance`, `--env`, `--exceptions-only`, `--fail-on exception`,
> `[classifications.allow_unmasked]`

## What it shows

Model sidecars classify columns (`email = "pii"`, `ssn = "pii_high"`,
`region = "internal"`), and `rocky.toml` binds each tag to a masking strategy in
`[mask]`, with `[mask.<env>]` overrides. `rocky compliance` checks that every
classified column resolves to a strategy, using only config and sidecars (no
warehouse calls). `--fail-on exception` exits 1 as a CI gate when a column has
no strategy and is not on `[classifications.allow_unmasked]`.

## A note on DuckDB and mask enforcement

`rocky run` on DuckDB does **not** enforce masking. DuckDB has no Unity Catalog
column tags or `CREATE MASK` / `SET MASKING POLICY` DDL, and the
classification-tag and masking-policy hooks are best-effort no-ops. Locally you
see the config flow, the static report and the CI gate. For the real DDL, see
"Apply the masks for real on Databricks" below.

## Run

```bash
cd examples/playground/pocs/04-governance/05-classification-masking-compliance
./run.sh
```

## Expected output (abridged)

```text
== Step 2: rocky compliance --env dev ==
...
  accounts.audit_note (audit_only) -> dev=unresolved!
  accounts.owner_email (pii)       -> dev=hash
  users.email (pii)                -> dev=hash
  users.region (internal)          -> dev=none
  users.ssn (pii_high)             -> dev=redact

  classified=5, masked=4, exceptions=1
...
== Step 4: rocky compliance --env prod --fail-on exception ==
CI gate exit code = 1  (1 = exceptions present -> block the merge)
```

## Suppressing a known exception

`audit_note` is tagged `audit_only`, which the team has decided not to mask. To
silence the exception, add the tag to the advisory list in `rocky.toml`:

```toml
[classifications]
allow_unmasked = ["audit_only"]
```

`rocky compliance` then reports the column's `enforced = false` in `per_column`
but emits no exception, and `--fail-on exception` stops firing on it.

## Resolved strategies by env

| Tag        | default (dev, staging, ...)  | prod        |
|------------|------------------------------|-------------|
| `pii`      | `hash`                       | `redact`    |
| `pii_high` | `redact`                     | `redact`    |
| `internal` | `none` (explicit identity)   | `none`      |
| `audit_only` | **unresolved (exception)** | **unresolved (exception)** |

`none` is an explicit policy decision and counts as masked in the summary. It is
not the same as "no strategy resolved."

## Apply the masks for real on Databricks

Switch the `[adapter]` block to a `databricks` adapter and set:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/..."
```

After the DAG, a successful `rocky run` against Unity Catalog emits, **one
statement per column** (UC rejects multi-column DDL):

- `ALTER TABLE ... ALTER COLUMN <col> SET TAGS ('classification' = '<tag>')`
- `CREATE MASK rocky_mask_<tag> ...` (idempotent, per-strategy) +
  `ALTER TABLE ... ALTER COLUMN <col> SET MASKING POLICY rocky_mask_<tag>()`

Classification, masking and retention applies are best-effort: a failure emits
`warn!` and the pipeline continues, as with `apply_grants`.

`rocky compliance` stays a static config check on any backend. It answers "is
the config right?", not "is the warehouse in sync?"
