# 05-schema-grounded-validation — Trust arc 5: AI output is compiler-gated

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY`
> **Runtime:** ~10s (one LLM round-trip, plus up to 3 retries on compile failure)
> **Rocky features:** `rocky ai`, `ValidationContext`, compile-verify retry loop

## What it shows

`rocky ai` never emits a model you couldn't compile. Two gates make
that real:

1. **Schema grounding.** Before the LLM sees the prompt, Rocky
   compiles the models directory and packs the result into a
   `ValidationContext` — project models, source column schemas, and
   column-level info. The LLM sees typed columns, not a blank page,
   so it can't invent `order_total` when the real column is `amount`.
2. **Compile-verify retry.** Each LLM response is fed through the
   Rocky compiler; on failure the compiler errors are appended to the
   prompt and the LLM is re-asked. Up to 3 retries. A successful
   return = at least one compile-clean output.

## Why it's distinctive

- **Hallucinated columns cannot escape.** Non-grounded AI SQL commonly
  hallucinates plausible-but-nonexistent columns. The
  `ValidationContext` makes that impossible; the compile gate catches
  whatever slips through.
- **The compiler is the spec.** There's no separate "verify that the
  AI output is valid" pass — if `rocky compile` passes, the model is
  valid. The gate and the dev-loop use the same tool.

## Layout

```
.
├── README.md                this file
├── rocky.toml               DuckDB pipeline
├── run.sh                   compile → rocky ai → tee output to generation.log
└── models/
    ├── _defaults.toml       catalog=poc, schema=demo
    ├── raw_orders.sql       seed model — typed schema passed to the LLM
    └── raw_orders.toml      full_refresh sidecar
```

## Prerequisites

- `rocky` ≥ 1.11.0 on PATH
- `ANTHROPIC_API_KEY` env var

## Run

```bash
export ANTHROPIC_API_KEY=sk-...
./run.sh
```

## What happened

1. `rocky compile --models models` extracts the typed schema of
   `raw_orders` from the project.
2. `rocky ai` calls the Anthropic API with a prompt that embeds the
   typed schema. The response is a candidate `.rocky` model.
3. The candidate is compiled. On failure, the compiler errors are
   added to the next prompt and the LLM is re-asked — up to 3 times.
4. The final clean output is written to `expected/generation.log`.

## Related

- Engine source: `engine/crates/rocky-ai/src/generate.rs`
  (`ValidationContext`), `engine/crates/rocky-cli/src/commands/ai.rs`
- Sibling POCs: [`01-model-generation/`](../01-model-generation/),
  [`02-ai-explain-bootstrap/`](../02-ai-explain-bootstrap/),
  [`04-ai-test-generation/`](../04-ai-test-generation/)
