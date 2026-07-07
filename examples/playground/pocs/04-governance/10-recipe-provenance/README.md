# 10-recipe-provenance — "what produced this?"

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** recipe-identity triple (`recipe_hash`, `input_hash`, `env_hash`), `rocky history --recipe`

## What it shows

Every time Rocky materializes a model it records a **recipe-identity triple** onto
that execution: `recipe_hash` (a fingerprint of the model's canonical typed IR —
the exact program), `input_hash` (the resolved inputs it read), and `env_hash`
(the engine + adapter + dialect it ran under), all tagged with a versioned
`hash_scheme`. `rocky history --recipe <hash>` then answers the audit question
directly: *"what produced this table, and every other time this exact program
ran?"* This POC materializes one model twice and shows both executions returned
under a single `recipe_hash`.

## Why it's distinctive

- The identity is keyed to the **program**, not the table name or a run id. Two
  executions of the same SQL share one `recipe_hash` no matter when or how often
  they ran; edit the SQL and the `recipe_hash` changes. That is the substrate for
  memoization, audit, and reproducible-by-recipe time travel.
- It is **honest about strength.** The `input_hash` here carries
  `proof_class = heuristic` because the input was proven via an observed
  freshness signature (rowcount), not a byte-for-byte content hash — so it is
  never presented as a content claim. A content-addressed input would be
  `strong`; a run that never observed its input records the triple as two-of-three
  rather than fabricating one.

## Layout

```
.
├── README.md         this file
├── rocky.toml        transformation pipeline; skip-unchanged on (populates input_hash)
├── run.sh            two executions, then `history --recipe`
├── models/
│   ├── orders_clean.sql    the program the recipe_hash fingerprints
│   └── orders_clean.toml   full_refresh sidecar
└── data/
    └── seed.sql      deterministic raw source
```

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
    env_hash         = bbf2c204...991d92bd
    hash_scheme      = v1

=== what produced this? rocky history --recipe <hash> ===
    executions of this exact program: 2
      run=run-...-341  model=orders_clean  status=success
      run=run-...-809  model=orders_clean  status=success

POC complete: one recipe_hash, two executions — 'what produced this?' answered.
```

(Hashes are deterministic for a given engine build; the `env_hash` moves when the
engine or adapter version does.)

## What happened

1. Execution #1 materialized `orders_clean` and stamped the recipe-identity
   triple onto its `ModelExecution` record.
2. 50 rows were appended and execution #2 ran the *same* SQL over the new input.
   Same program ⇒ identical `recipe_hash`; different input ⇒ different `input_hash`.
3. `rocky history` surfaces the triple on every model record, with the honest
   `heuristic` input proof-class.
4. `rocky history --recipe <hash>` returns both executions — every run of that
   exact program.

## Related

- Concept: [Content-addressed run records](https://rocky-data.dev/concepts/content-addressed/)
- Companion: the `02-performance/14-skip-unchanged` POC, whose observed-upstream
  signature is what populates this triple's `input_hash`.
