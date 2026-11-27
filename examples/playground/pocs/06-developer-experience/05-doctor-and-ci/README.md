# 05-doctor-and-ci — `rocky doctor` + `rocky ci` + GitHub Actions example

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky doctor`, `rocky ci`, GitHub Actions integration

## What it shows

`rocky doctor` returns one JSON report of health checks: config, local state, adapters, pipelines, state backend config (`state_sync`), live state read/write (`state_rw`) and auth (`auth` plus per-adapter `auth/<name>`). `rocky ci` runs compile and test with CI exit codes.

`.github/workflows/rocky-ci.yml` is an example GitHub Actions workflow you can drop into any project.

## Run

```bash
./run.sh
```
