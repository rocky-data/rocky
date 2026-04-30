# 05-classification-masking-compliance — `[classification]` + `[mask]` + `rocky compliance`

![rocky compliance --env dev rolls up classification tags to mask strategies; --fail-on exception exits 1, gating CI on unmasked PII](../../../../../docs/public/demo-classification-masking.gif)

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `[classification]` sidecar, `[mask]` / `[mask.<env>]` policy,
> `rocky compliance`, `--env`, `--exceptions-only`, `--fail-on exception`,
> `[classifications.allow_unmasked]`

## What it shows

The Wave A + Wave B governance surface shipped in `engine-v1.16.0`:

1. **Model sidecars** declare per-column classification tags
   (`email = "pii"`, `ssn = "pii_high"`, `region = "internal"`).
2. **Project `rocky.toml`** binds each tag to a masking strategy in
   `[mask]` (workspace default) and tightens it in `[mask.<env>]` for
   specific environments.
3. **`rocky compliance`** is a static resolver that answers
   _"are all classified columns masked wherever policy says they
   should be?"_ — no warehouse I/O, pure resolver over the config +
   sidecars.
4. **`--fail-on exception`** turns the rollup into a CI gate: exit 1
   when any classified column has no resolved strategy and isn't on the
   `[classifications.allow_unmasked]` advisory list.

## A note on DuckDB and mask enforcement

DuckDB does not implement Unity Catalog column tags or
`CREATE MASK` / `SET MASKING POLICY` DDL, so `rocky run` against the
local DuckDB adapter does **not** enforce masking at apply time. The
governance adapter's classification-tag and masking-policy hooks are
best-effort no-ops on this backend (mirrors the `apply_grants`
semantics).

The value of running this POC locally is:

- exercising the **config flow** (sidecar parse, `[mask]` / `[mask.<env>]`
  resolution, `allow_unmasked` advisory list),
- seeing the **static compliance report** (`rocky compliance`) in both
  human and JSON shapes,
- demonstrating the **CI gate** (`--fail-on exception` returns 1).

See _"Apply the masks for real on Databricks"_ below for the path that
actually emits the UC DDL.

## Why it's distinctive

- Classification lives in the **model sidecar**, right next to the SQL
  that produced the columns — not in an external catalog. Reviewable in
  PRs like any other code.
- One `[mask]` block governs the workspace; per-env overrides tighten
  without loosening. No duplicate policy files per environment.
- `rocky compliance` is a **thin resolver with zero warehouse calls** —
  safe to run in CI on every PR, fast enough to block merges, and
  honest about what it does (static config check, not live enforcement
  probe).

## Layout

```
.
├── README.md
├── rocky.toml                    [mask] defaults + [mask.prod] override
├── run.sh                        4-step demo: run, compliance dev, prod, CI gate
├── data/seed.sql                 DuckDB fixtures (users + accounts)
└── models/
    ├── users.sql / users.toml    email=pii, ssn=pii_high, region=internal
    └── accounts.sql / accounts.toml  owner_email=pii, audit_note=audit_only (unmapped)
```

## Run

```bash
cd examples/playground/pocs/04-governance/05-classification-masking-compliance
./run.sh
```

## Expected output (abridged)

```text
== Step 1: rocky run (materialize models) ==
run (users)     status = ...

== Step 2: rocky compliance --env dev ==

Rocky Compliance
----------------

  accounts.audit_note (audit_only) -> dev=unresolved!
  accounts.owner_email (pii)       -> dev=hash
  users.email (pii)                -> dev=hash
  users.region (internal)          -> dev=none
  users.ssn (pii_high)             -> dev=redact

  classified=5, masked=4, exceptions=1

Exceptions:
  - accounts.audit_note [dev]: no masking strategy resolves for classification tag 'audit_only'

== Step 3: rocky compliance --env prod --exceptions-only ==
  accounts.audit_note (audit_only) -> prod=unresolved!
  classified=5, masked=4, exceptions=1

== Step 4: rocky compliance --env prod --fail-on exception ==
CI gate exit code = 1  (1 = exceptions present -> block the merge)

POC complete:
  - classified columns scanned : 5
  - exceptions in prod         : 1
  - CI gate exit code          : 1
```

## Suppressing a known exception

`audit_note` is tagged `audit_only` — a classification the team has
decided not to mask (lineage/discovery tag, not a confidentiality
tag). To silence the exception without pretending the column is
enforced, add the tag to the advisory list in `rocky.toml`:

```toml
[classifications]
allow_unmasked = ["audit_only"]
```

After this, `rocky compliance` reports the column's `enforced = false`
state in `per_column` but does **not** emit an exception for it and
`--fail-on exception` stops firing on it. The knob exists to unstick
merges without hiding the fact that the column is unmasked.

## Resolved strategies by env

| Tag        | default (dev, staging, ...)  | prod        |
|------------|------------------------------|-------------|
| `pii`      | `hash`                       | `redact`    |
| `pii_high` | `redact`                     | `redact`    |
| `internal` | `none` (explicit identity)   | `none`      |
| `audit_only` | **unresolved (exception)** | **unresolved (exception)** |

`none` is an **explicit policy decision** (the team has decided this
tag does not require masking) and counts as masked in the summary —
it is not the same as "no strategy resolved."

## Apply the masks for real on Databricks

Set these env vars and switch the `[adapter]` block to a
`databricks` adapter:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/..."
```

On a successful `rocky run` against Unity Catalog, Rocky iterates the
model list post-DAG and emits, **one statement per column** (UC rejects
multi-column DDL):

- `ALTER TABLE ... ALTER COLUMN <col> SET TAGS ('classification' = '<tag>')`
- `CREATE MASK rocky_mask_<tag> ...` (idempotent, per-strategy) +
  `ALTER TABLE ... ALTER COLUMN <col> SET MASKING POLICY rocky_mask_<tag>()`

All three governance applies (classification, masking, retention) run
in a single post-DAG loop, best-effort: a failure emits `warn!` and
the pipeline continues — same semantics as `apply_grants`.

`rocky compliance` still works unchanged on the Databricks adapter;
it remains a static config check regardless of backend. The rollup
answers "is the config right?" not "is the warehouse in sync?"

## Related

- Source: `engine/crates/rocky-cli/src/commands/compliance.rs`
- Config shape: `engine/crates/rocky-core/src/config.rs` (`MaskEntry`,
  `ClassificationsConfig`, `RockyConfig::resolve_mask_for_env`)
- Governance adapter trait:
  `engine/crates/rocky-core/src/traits.rs` (`GovernanceAdapter::apply_column_tags`,
  `apply_masking_policy`)
- CHANGELOG: `engine/CHANGELOG.md` — `[1.16.0]` Wave A + Wave B entries.
