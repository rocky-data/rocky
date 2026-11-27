# Recording the BigQuery live demo

A seven-shot script: one compile smoke, then six live drivers. The drivers echo `==>` status lines, so most captions are already on screen.

Use any recorder (asciinema, screen recorder). The repo's [vhs](https://github.com/charmbracelet/vhs) tapes (`cli-recording/`) are local DuckDB, and vhs's fixed `Sleep` pacing can't track multi-minute live BigQuery jobs, so don't force this into a `.tape`.

## Pre-flight

1. **Build the binary** from the repo root. Drivers call bare `rocky` from `PATH`:

   ```bash
   cargo build --release -p rocky --manifest-path engine/Cargo.toml
   export PATH="$PWD/engine/target/release:$PATH"
   rocky --version          # confirm this resolves to the build you just made
   ```

2. **`bq` and `python3` on `PATH`.**

   ```bash
   bq version
   python3 --version
   ```

3. **Credentials and project.**

   ```bash
   export GCP_PROJECT_ID="<your-gcp-project-id>"
   export GOOGLE_APPLICATION_CREDENTIALS="<path-to-service-account-json>"
   export BQ_LOCATION="EU"                                # optional; default EU
   ```

   Shots 2-7 require both `GCP_PROJECT_ID` and `GOOGLE_APPLICATION_CREDENTIALS`; the `BIGQUERY_TEST_PROJECT` and `BIGQUERY_TOKEN` fallbacks work only for the top-level `./run.sh`.

4. **Terminal:** about 100 columns so `==>` output doesn't wrap.

## Shot order

Run each from the POC directory.

### 1. Compile smoke -- no credentials

```bash
rocky compile --models live/models/ --output json
```

- **Proves:** the BigQuery model frontmatter type-checks.
- **Caption:** "Type-check the BigQuery models -- no warehouse, no creds."

### 2. Full-refresh -- `CREATE TABLE AS`

```bash
./live/run.sh
```

- **Proves:** `full_refresh` materializes and `bq query` confirms `COUNT(*) = 1`.
- **Caption:** "Full-refresh CTAS -- one row materialized and verified live."

### 3. Discover -- region-qualified `INFORMATION_SCHEMA`

```bash
./live/discover/run.sh
```

- **Proves:** two prefixed datasets surface as sources.
- **Caption:** "Discover sources -- region-qualified INFORMATION_SCHEMA, two datasets found."

### 4. Merge -- `WHEN NOT MATCHED THEN INSERT ROW`

```bash
./live/merge/run.sh
```

- **Proves:** first run bootstraps the target; the next upserts (bob updated, dave inserted, alice/carol untouched) with explicit `update_columns`. Asserts billed bytes at the 10 MB floor.
- **Caption:** "MERGE upsert -- update bob, insert dave, leave the rest."

### 5. Time-interval -- DML transaction script

```bash
./live/time-interval/run.sh
```

- **Proves:** `BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION` runs as one script; two day partitions get the expected row counts.
- **Caption:** "Two partitions, one DML transaction script -- 3 orders, then 2."

### 6. Drift -- four-stage schema evolution

```bash
./live/drift/run.sh
```

- **Proves:** four stages on one source/target pair, each action asserted from `rocky run` JSON:
  1. **initial:** replicates, no drift.
  2. **`add_columns`:** source gains `region`; target altered in place, history intact.
  3. **`drop_and_recreate`:** `id` INT64 to STRING (unsafe); target rebuilt.
  4. **`alter_column_types`:** `score` INT64 to NUMERIC (safe); altered with `ALTER COLUMN ... SET DATA TYPE`.
- **Caption:** "Four drift stages -- add column, rebuild on unsafe change, widen in place."

### 7. Cost cross-check -- exact billed-bytes round-trip

```bash
./live/cost-cross-check/run.sh
```

- **Proves:** for a transformation scanning a real source, `bytes_scanned` exactly matches `totalBytesBilled` from `bq show -j` for the same job ID.
- **Caption:** "rocky's billed bytes == BigQuery's billed bytes, to the byte."

## Notes

- Each driver drops its `poc_step*` dataset(s) via a `trap`, so retakes are clean and shots run in any order.
- The full tour takes roughly 3-5 minutes; single drivers take seconds to tens of seconds.
- Committed files carry `__GCP_PROJECT__`, not a project ID; only your terminal output can reveal yours.
