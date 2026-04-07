# 03-fivetran-discover — Fivetran metadata discovery

> **Category:** 07-adapters
> **Credentials:** `FIVETRAN_API_KEY` + `FIVETRAN_API_SECRET` + `FIVETRAN_DESTINATION_ID` (read-only)
> **Runtime:** depends on Fivetran API
> **Rocky features:** Fivetran source adapter, `rocky discover`

## What it shows

`rocky discover` against the Fivetran source adapter — calls Fivetran's
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
