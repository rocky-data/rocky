# 03-fivetran-discover — Fivetran metadata discovery

> **Credentials:** `FIVETRAN_API_KEY` + `FIVETRAN_API_SECRET` + `FIVETRAN_DESTINATION_ID` (read-only)
> **Runtime:** depends on Fivetran API

## What it shows

`rocky discover` calls Fivetran's REST API to list connectors and their enabled tables. No SQL runs and no warehouse is written; the API key only needs read access.

## Run

```bash
export FIVETRAN_API_KEY="..."
export FIVETRAN_API_SECRET="..."
export FIVETRAN_DESTINATION_ID="..."
./run.sh
```

## Expected output

`run.sh` validates the config, runs `rocky -c rocky.toml -o json discover` into `expected/discover.json` (gitignored; a `sources` array, one entry per connector with its `tables`), and prints:

```
POC complete: discovered <N> Fivetran connectors.
```

`<N>` depends on your `FIVETRAN_DESTINATION_ID`.
