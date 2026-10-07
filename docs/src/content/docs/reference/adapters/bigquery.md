---
title: BigQuery adapter
description: The BigQuery adapter's two config fields, and the order Rocky checks for credentials
sidebar:
  order: 4
---

The BigQuery warehouse adapter runs your SQL through the BigQuery REST API.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project_id` | string | No | Google Cloud project ID that owns the datasets and is billed for query execution. |
| `location` | string | No | BigQuery processing location (e.g., `"US"`, `"EU"`, `"us-central1"`). |

```toml
[adapter.bq]
type = "bigquery"
project_id = "${GCP_PROJECT_ID}"
location = "US"
```

## Authentication

Rocky reads BigQuery credentials from the environment, never from `rocky.toml`. It checks two variables in this order.

1. **`BIGQUERY_TOKEN`** — an OAuth bearer token you obtained yourself. Rocky uses it as is. Because Rocky checks it first, setting it overrides any service-account key on the same machine.
2. **`GOOGLE_APPLICATION_CREDENTIALS`** — the path to a service-account JSON key. Rocky mints a JWT from the key, exchanges it for an access token at Google's token endpoint, and refreshes the token before it expires.

If neither variable is set, the adapter fails with `no authentication method available — set GOOGLE_APPLICATION_CREDENTIALS or provide a bearer token`.

:::caution[Key file permissions]
The service-account key holds an RSA private key. On Unix, Rocky emits a warning when the file at `GOOGLE_APPLICATION_CREDENTIALS` is group- or world-readable — `chmod 600` (or `0400`) it.
:::

## Load contracts and `NUMERIC`

BigQuery reports a default-precision decimal column by its bare name. `rocky load` checks a load contract against that report. On BigQuery it reads a bare `NUMERIC` as `NUMERIC(38, 9)`, the documented default. A contract that declares `NUMERIC(38,9)` or wider passes. A narrower one, such as `NUMERIC(10,2)`, fails with `required_column_type`.

A bare `BIGNUMERIC` is still refused with `unverifiable_landed_type`. Its default range has no exact `(precision, scale)` form. Other warehouses do not get this reading.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
