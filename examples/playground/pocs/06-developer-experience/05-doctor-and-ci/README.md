# 05-doctor-and-ci — `rocky doctor` + `rocky ci` + GitHub Actions example

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky doctor`, `rocky ci`, GitHub Actions integration

## What it shows

`rocky doctor` aggregates health checks across config, state, adapters,
pipelines, and state sync — all in one JSON output. `rocky ci` is a
combined `compile + test` command with proper exit codes for CI.

This POC also ships an example `.github/workflows/rocky-ci.yml` that you
can drop into any project to wire Rocky into GitHub Actions.

## Why it's distinctive

- **One command** for environment health (`doctor`) and **one command** for
  CI (`ci`). No bash glue needed.

## Run

```bash
./run.sh
```
