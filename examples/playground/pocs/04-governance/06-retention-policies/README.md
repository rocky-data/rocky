# 06-retention-policies — declarative data-retention policies

> **Category:** 04-governance
> **Credentials:** none (DuckDB primary path; Databricks / Snowflake
> narrative-only in the "for real" section below)
> **Runtime:** < 5s
> **Rocky features:** `retention = "<N>[dy]"` sidecar key,
> `RetentionPolicy`, `rocky retention-status` (`--model`, `--drift`),
> `GovernanceAdapter::apply_retention_policy`

## What it shows

Wave C-2 of the governance waveplan (engine-v1.16.0) gave every model
sidecar a declarative `retention = "<N>[dy]"` key. The value is parsed
once at project-load time into a typed `RetentionPolicy { duration_days: u32 }`
— grammar is strict (`30d`, `1y`; `-3d`, `90`, `abc` all reject with
actionable diagnostics). Years are flat 365 days (`1y` → 365, no
leap-year semantics — confirmed in `rocky-core::retention`).

Three models:

| Model | Sidecar `retention` | `configured_days` |
|---|---|---|
| `page_views_30d` | `"30d"` | 30 |
| `ledger_archive_1y` | `"1y"` | 365 |
| `ephemeral_summary` | *(absent)* | `null` |

At `rocky run` time, after the DAG succeeds, Rocky calls
`GovernanceAdapter::apply_retention_policy` on every model with a
declared policy. `rocky retention-status` is a static reporter over the
compiled model set — no warehouse probe.

## Why it's distinctive

- **One config → two warehouses.** The same sidecar value becomes Delta
  TBLPROPERTIES on Databricks and `DATA_RETENTION_TIME_IN_DAYS` on
  Snowflake. You declare the intent once; Rocky handles the dialect.
- **Strict grammar, early errors.** `retention = "bad"` fails at
  project-load time, not at warehouse-apply time — diagnostics surface
  in `rocky validate` and in the LSP before a single row moves.
- **Static report alongside the enforcement path.** `rocky retention-status`
  gives CI a declaration-only audit without touching the warehouse.

## Primary mode: DuckDB (no credentials)

`rocky run` on DuckDB resolves the governance adapter to
`NoopGovernanceAdapter`. Its `apply_retention_policy` impl returns
`Ok(())` unconditionally (see `rocky-core/src/traits.rs:811-821`) — **no
warning, no error, silently skipped.** The POC still exercises the
full parse-and-wire path:

1. `rocky validate` — confirms all three sidecars parse (a malformed
   value would fail here).
2. `rocky run` — the retention apply is a no-op on DuckDB; the run is
   the same smoke-test shape as the sibling
   `05-orchestration/09-idempotency-key` POC (DuckDB replication exits
   non-zero on "no discovery adapter", tolerated with `|| true`).
3. `rocky retention-status -o table` — text report.
4. `rocky retention-status` — JSON report (default output).
5. `rocky retention-status --model ledger_archive_1y -o table` — scoped
   to a single model.
6. `rocky retention-status --drift -o table` — `--drift` is accepted
   for forward-compat but the warehouse probe is deferred to v2. The
   command filters to models with a declared policy and prints
   `note: --drift probe is deferred to v2; warehouse_days will be null.`
   to stderr. `warehouse_days` stays `None` in JSON (schema is stable —
   `Option<u32>` — so v2 can slot in without a wire break).

## Apply retention for real on Databricks / Snowflake

Swap the `[adapter]` block to Databricks or Snowflake and `rocky run`
will fire `apply_retention_policy` on every model with a declared
sidecar:

- **Databricks (Delta).** Rocky writes **both**
  `delta.logRetentionDuration` **and** `delta.deletedFileRetentionDuration`
  TBLPROPERTIES in a single `ALTER TABLE` — the pair governs time-travel
  history readability and physical tombstone retention together.
- **Snowflake.** Rocky emits
  `ALTER TABLE <db>.<schema>.<table> SET DATA_RETENTION_TIME_IN_DAYS = <N>`.
  Snowflake's edition-specific cap (Standard 90, Enterprise 365)
  enforces server-side — Rocky surfaces any rejection.
- **BigQuery / DuckDB.** No first-class time-travel retention knob at
  the config level — the apply is a no-op (silent on DuckDB via
  `NoopGovernanceAdapter`; the trait default would `warn!` but the
  Databricks/Snowflake impls override it).

Apply is **best-effort** — failures emit `warn!` and the pipeline
continues, matching the semantics of `apply_grants` and the rest of
the Wave C governance reconcile loop.

## Layout

```
.
|-- README.md
|-- rocky.toml                    Minimal DuckDB config, single pipeline `poc`
|-- run.sh                        Six-step demo (validate -> run -> 4x retention-status)
|-- data/seed.sql                 raw__events / raw__ledger DuckDB seed
|-- models/
|   |-- page_views_30d.sql        -- retention = "30d"
|   |-- page_views_30d.toml
|   |-- ledger_archive_1y.sql     -- retention = "1y" (flat 365 days)
|   |-- ledger_archive_1y.toml
|   |-- ephemeral_summary.sql     -- no retention sidecar key
|   `-- ephemeral_summary.toml
`-- expected/                     Captured JSON / text, regenerated each run
```

## Run

```bash
cd examples/playground/pocs/04-governance/06-retention-policies
./run.sh
```

## Expected output (abridged)

```
--- rocky retention-status -o table ---
MODEL                                    CONFIGURED       WAREHOUSE        IN SYNC
----------------------------------------------------------------------------------
ephemeral_summary                        -                -                yes
ledger_archive_1y                        365 days         -                no
page_views_30d                           30 days          -                no

--- rocky retention-status --drift -o table ---
note: --drift probe is deferred to v2; warehouse_days will be null.
MODEL                                    CONFIGURED       WAREHOUSE        IN SYNC
----------------------------------------------------------------------------------
ledger_archive_1y                        365 days         -                no
page_views_30d                           30 days          -                no
```

JSON output (default `-o json`):

```json
{
  "version": "1.16.0",
  "command": "retention-status",
  "models": [
    { "model": "ephemeral_summary", "in_sync": true },
    { "model": "ledger_archive_1y", "configured_days": 365, "in_sync": false },
    { "model": "page_views_30d", "configured_days": 30, "in_sync": false }
  ]
}
```

Note: `configured_days` and `warehouse_days` use `skip_serializing_if`,
so unset fields are omitted rather than emitted as explicit `null`.
Consumers should treat missing keys as `null`.

`in_sync` in v1 is a declaration check — `configured_days == warehouse_days`
where `warehouse_days` is always `None` (hence `true` only for
unconfigured models, where both sides are `None`). The v2 `--drift`
probe will populate `warehouse_days` from `SHOW TBLPROPERTIES`
(Databricks) / `SHOW PARAMETERS` (Snowflake) and make `in_sync` a real
drift signal.
