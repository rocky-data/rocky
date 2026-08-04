---
title: BigQuery adapter
description: BigQuery warehouse adapter — project and location fields, and the bearer-token / service-account detection order
sidebar:
  order: 4
---

BigQuery warehouse adapter. Executes SQL through the BigQuery REST API.

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

Credentials come from the environment rather than from `rocky.toml`, and are detected in this order:

1. **`BIGQUERY_TOKEN`** — a pre-obtained OAuth bearer token, used as-is. Checked first, so setting it overrides any service-account key on the same machine.
2. **`GOOGLE_APPLICATION_CREDENTIALS`** — path to a service-account JSON key. Rocky mints a JWT from the key, exchanges it for an access token at Google's token endpoint, and refreshes it automatically before expiry.

If neither is set, the adapter fails with `no authentication method available — set GOOGLE_APPLICATION_CREDENTIALS or provide a bearer token`.

:::caution[Key file permissions]
The service-account key holds an RSA private key. On Unix, Rocky emits a warning when the file at `GOOGLE_APPLICATION_CREDENTIALS` is group- or world-readable — `chmod 600` (or `0400`) it.
:::

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
