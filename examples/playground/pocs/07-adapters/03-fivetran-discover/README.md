# 03-fivetran-discover — Fivetran metadata discovery

> **Category:** 07-adapters
> **Credentials:** `FIVETRAN_API_KEY` + `FIVETRAN_API_SECRET` + `FIVETRAN_DESTINATION_ID` (read-only)
> **Runtime:** depends on Fivetran API
> **Rocky features:** Fivetran source adapter, `rocky discover`

## What it shows

`rocky discover` against the Fivetran source adapter: calls Fivetran's
REST API to list connectors and their enabled tables, with no warehouse
write. The API key only needs read access.

## Why it's distinctive

- **Discovery without execution** — find what data exists before writing
  any pipeline code.
- API-call-only POC: no warehouse adapter required, no SQL executed.

## Run

```bash
export FIVETRAN_API_KEY="..."
export FIVETRAN_API_SECRET="..."
export FIVETRAN_DESTINATION_ID="..."
./run.sh
```

## Expected output

`run.sh` validates the config, then runs `rocky -c rocky.toml -o json discover`,
writing the discovered connectors to `expected/discover.json` (gitignored) and
printing a summary line:

```
POC complete: discovered <N> Fivetran connectors.
```

`<N>` is the number of connectors reported for your `FIVETRAN_DESTINATION_ID`,
so the exact count depends on your Fivetran account. `discover.json` follows the
`rocky discover` JSON schema — a `sources` array, each entry a discovered
connector table — and no warehouse is written.
