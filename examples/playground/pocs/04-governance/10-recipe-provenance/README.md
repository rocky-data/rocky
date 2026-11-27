# 10-recipe-provenance — "what produced this?"

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** recipe-identity triple (`recipe_hash`, `input_hash`, `env_hash`), `rocky history --recipe`

## What it shows

Each materialization records a **recipe-identity triple**: `recipe_hash` (the
model's canonical typed IR), `input_hash` (the resolved inputs read) and
`env_hash` (engine + adapter + dialect), tagged with a versioned `hash_scheme`.
`rocky history --recipe <hash>` returns every execution of that exact program.
The POC runs one model twice (50 rows appended in between): same `recipe_hash`,
different `input_hash`. Editing the SQL changes the `recipe_hash`.

`input_hash` here is `proof_class = heuristic`: the input was proven by an
observed freshness signature (rowcount), not a content hash. A content-addressed
input would be `strong`. A run that never observed its input records two of the
three.

## Prerequisites

- `rocky` on PATH (or set `ROCKY_BIN` to a built binary)
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
=== the recipe-identity triple on orders_clean (from rocky history) ===
    model=orders_clean
    recipe_hash      = 2148e619b51421f5...07453036
    input_hash       = 75ee2a328c52b236...c21b9c6  (proof_class=heuristic)
    env_hash         = 1df698e4...89abf00c
    hash_scheme      = v1
...
    executions of this exact program: 2
...
POC complete: one recipe_hash, two executions — 'what produced this?' answered.
```

Hashes are deterministic for a given engine build; `env_hash` moves when the
engine or adapter version does.

## Related

- Concept: [Content-addressed run records](https://rocky-data.dev/concepts/content-addressed/)
- The `02-performance/14-skip-unchanged` POC, whose observed-upstream signature
  populates this `input_hash`.
