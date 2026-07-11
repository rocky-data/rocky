# Recording the BigQuery live demo

A recorder-agnostic shooting script for the seven-shot BigQuery tour: a
compile smoke that runs anywhere, plus six live drivers that materialize
against a real GCP project and assert the resulting warehouse state.

The script names the command, what it proves, and a one-line caption for
each shot, in simple to payoff order. The drivers already echo `==>`
status lines as they run, so most captions are on-screen for free.

## Recorder-agnostic, on purpose

The repo's other CLI demos record with [vhs](https://github.com/charmbracelet/vhs)
tapes (`cli-recording/`), but every one of those tapes drives a local
DuckDB pipeline with no credentials. vhs paces a recording with fixed
`Sleep` directives, which can't track the variable, multi-minute latency
of live BigQuery jobs. So this tour is recorder-agnostic: capture it with
asciinema, a terminal screen recorder, or whatever you prefer. Don't try
to force it into a `.tape`.

## Pre-flight

1. **Build the binary you're demoing.** Every driver calls a bare
   `rocky` from `PATH`. Record against a freshly built binary so the demo
   matches the current code, not a stale install:

   ```bash
   cargo build --release -p rocky --manifest-path engine/Cargo.toml
   export PATH="$PWD/engine/target/release:$PATH"
   rocky --version          # confirm this resolves to the build you just made
   ```

   (Any current `rocky` release also covers every surface here -- the
   drift, cost-attribution, and discovery paths are long-shipped -- but
   building from source removes all ambiguity about which binary ran.)

2. **`bq` and `python3` on `PATH`.** The drivers seed, verify, and clean
   up with the `bq` CLI, and assert JSON output with `python3`.

   ```bash
   bq version
   python3 --version
   ```

3. **Credentials and project.**

   ```bash
   export GCP_PROJECT_ID="<your-gcp-project-id>"          # BIGQUERY_TEST_PROJECT also accepted
   export GOOGLE_APPLICATION_CREDENTIALS="<path-to-service-account-json>"   # or BIGQUERY_TOKEN
   export BQ_LOCATION="EU"                                # optional; default EU
   ```

4. **Terminal.** Size the window so a full driver's `==>` output fits
   without wrapping (~100 cols is comfortable). A dark theme reads best on
   playback.

## Shot order

Seven shots, simple to payoff. Run each from the POC directory.

### 1. Compile smoke -- no credentials

```bash
rocky compile --models live/models/ --output json
```

- **Proves:** the BigQuery model frontmatter type-checks against the
  current binary with zero credentials. This is the path the
  credential-free POC sweep runs.
- **Caption:** "Type-check the BigQuery models -- no warehouse, no creds."

### 2. Full-refresh -- `CREATE TABLE AS`

```bash
./live/run.sh
```

- **Proves:** a `full_refresh` transformation materializes end-to-end via
  `BigQueryDialect::create_table_as`, and the row lands in the warehouse
  (`bq query` confirms `COUNT(*) = 1`).
- **Caption:** "Full-refresh CTAS -- one row materialized and verified live."

### 3. Discover -- region-qualified `INFORMATION_SCHEMA`

```bash
./live/discover/run.sh
```

- **Proves:** `BigQueryDiscoveryAdapter` enumerates datasets through the
  region-scoped `INFORMATION_SCHEMA.SCHEMATA`; two prefixed datasets
  surface as sources via the schema-pattern parser.
- **Caption:** "Discover sources -- region-qualified INFORMATION_SCHEMA, two datasets found."

### 4. Merge -- `WHEN NOT MATCHED THEN INSERT ROW`

```bash
./live/merge/run.sh
```

- **Proves:** the `merge` strategy bootstraps the target on first run,
  then upserts across two runs (bob updated, dave inserted, alice/carol
  untouched) using BigQuery's `WHEN NOT MATCHED THEN INSERT ROW` shape
  with explicit `update_columns`. Asserts billed bytes at the 10 MB floor.
- **Caption:** "MERGE upsert -- update bob, insert dave, leave the rest."

### 5. Time-interval -- DML transaction script

```bash
./live/time-interval/run.sh
```

- **Proves:** the `time_interval` strategy emits
  `BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION` as one
  semicolon-joined script (BigQuery's REST API is stateless -- separate
  calls are rejected). Two day partitions materialize the expected
  aggregated row counts.
- **Caption:** "Two partitions, one DML transaction script -- 3 orders, then 2."

### 6. Drift -- four-stage schema evolution

```bash
./live/drift/run.sh
```

- **Proves:** replication-from-BigQuery with per-table drift detection
  across four stages on the same source/target pair:
  1. **initial** -- replicates the source, no drift.
  2. **`add_columns`** -- source gains `region`; the target is `ALTER TABLE
     ADD COLUMN`-ed in place (no full refresh, historical rows intact).
  3. **`drop_and_recreate`** -- `id` changes INT64 to STRING (unsafe); the
     target is rebuilt.
  4. **`alter_column_types`** -- `score` widens INT64 to NUMERIC (safe); the
     target column type is altered in place via
     `ALTER COLUMN ... SET DATA TYPE`.

  Each stage's drift action is asserted from the `rocky run` JSON, and the
  final target schema is confirmed (`region` present, `id` is STRING,
  `score` is NUMERIC).
- **Caption:** "Four drift stages -- add column, rebuild on unsafe change, widen in place."

### 7. Cost cross-check -- exact billed-bytes round-trip

```bash
./live/cost-cross-check/run.sh
```

- **Proves:** the strongest closer. A transformation scans a real source
  table, and rocky's reported `bytes_scanned` matches the
  `totalBytesBilled` BigQuery itself reports for the same job ID via
  `bq show -j` -- exact, not an approximation.
- **Caption:** "rocky's billed bytes == BigQuery's billed bytes, to the byte."

## Notes

- **Self-cleaning.** Every driver pre-creates its own `poc_step*` dataset
  and drops it on exit (success or failure) via a `trap`. Nothing persists
  in the project between takes.
- **Independent and re-runnable.** Each driver owns its `live.rocky.toml`,
  models, and dataset, so shots run in any order and every take is a clean
  re-run. If a take is fumbled, just run that one driver again.
- **Runtime.** The full seven-shot tour runs in roughly 3-5 minutes;
  individual drivers are seconds to tens of seconds each.
- **No project ID in the repo.** The committed configs and model files
  carry a `__GCP_PROJECT__` placeholder that each driver substitutes at
  runtime from `GCP_PROJECT_ID`. Nothing in the recording reveals a real
  project ID beyond what your terminal shows live.
