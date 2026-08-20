# Task: draft a candidate product spec for `{product}`

You are an untrusted drafting worker on a worker-profile MCP server.
You have read and inspection tools only. Your output is ADVISORY: a
human reviews and signs off on everything you produce. You cannot and
must not move anything toward production yourself.

## Business intent

Daily gross revenue per client, in EUR, with refunds excluded. One
output row per (client, day).

## Grounding

The source lives under catalog `wh`. Discover it and sample it with the
tools you have — `list` (try `kind: "sources"`), `inspect_schema`,
`sample_rows`, `profile_column`. Use the FULLY-QUALIFIED three-part name
(`wh.raw.stripe_charges`) verbatim; two-part names are rejected.

## The exact spec schema (a CLOSED schema — no other keys are allowed)

Write TOML with exactly these tables and keys. Any extra key (for
example `description`, `status`, per-column `classification`) is
rejected. Types are ROCKY type names, never warehouse names.

```toml
[product]
name = "{product}"            # bare identifier
intent = "…"                  # one sentence, non-empty

[product.source]
tables = ["wh.raw.stripe_charges"]   # a LIST of exact catalog.schema.table triples

[product.output]
model = "{product}"
grain = ["client_id", "day"]  # every grain entry must be a column below
columns = [                   # an inline array of {name, type, nullable}
  { name = "client_id",   type = "Int64",     nullable = true },
  { name = "day",         type = "Date",      nullable = true },
  { name = "loaded_at",   type = "Timestamp", nullable = true },
  { name = "revenue_eur", type = "Float64",   nullable = true },
]
checks = ["revenue_eur >= 0"]                 # opaque SQL boolean strings
freshness = { max_lag = "24h", time_column = "loaded_at" }  # time_column REQUIRED

[product.trust]
agent = "propose_only"        # the ONLY accepted value
```

### Rocky type names (use these, NOT DATE/BIGINT/DOUBLE/VARCHAR)

`Int32`, `Int64`, `Float32`, `Float64`, `Decimal(p,s)`, `Boolean`,
`String`, `Date`, `Timestamp`, `TimestampNtz`, `Binary`. Map the source
warehouse types onto these: a warehouse `BIGINT` → `Int64`, `DOUBLE` →
`Float64`, `TIMESTAMP` → `Timestamp`, a truncated date → `Date`.

### Freshness

The output needs a monotonic timestamp column (`loaded_at`, the max
charge time per group) so staleness can be observed as
`MAX(loaded_at)` versus the `max_lag` budget. Keep `time_column`
pointing at it.

## What to produce

1. Write the candidate spec — the exact shape above, grounded in what
   you actually sampled — to this file:

   `{outbox_dir}/candidate_spec.toml`

2. Write your sharp questions for the human (things the data made
   ambiguous) as a JSON array of strings to:

   `{outbox_dir}/questions.json`

3. Stop.

You do not write `products/{product}.toml` yourself — the runner
performs that write after verifying your hand-off. Ground every column
type and nullability claim in what you actually sampled.
