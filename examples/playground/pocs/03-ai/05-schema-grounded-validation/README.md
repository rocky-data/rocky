# 05-schema-grounded-validation — Trust arc 5: AI output is compiler-gated

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY`
> **Runtime:** ~10s per LLM round-trip, up to 3 attempts on compile failure
> **Rocky features:** `rocky ai`, `ValidationContext`, compile-verify retry loop

## What it shows

`rocky ai` output is compiler-gated:

1. **Schema grounding.** Rocky compiles the models directory into a `ValidationContext` (project models, source column schemas, column-level info), so the LLM sees real typed columns.
2. **Compile-verify retry.** Each response is compiled; on failure the errors are added to the prompt and the LLM is re-asked, up to 3 attempts in total.

## Prerequisites

- `rocky` ≥ 1.11.0 on PATH
- `ANTHROPIC_API_KEY` env var

## Run

```bash
export ANTHROPIC_API_KEY=sk-...
./run.sh
```

## Output

`rocky ai` writes the model and its `.toml` sidecar into `models/`; console output goes to `expected/generation.log`.

`run.sh` does not delete the generated model, and it does not pass
`--overwrite`. Delete the generated files under `models/` before a second run,
or `rocky ai` refuses to overwrite them.

Source: `engine/crates/rocky-ai/src/generate.rs`, `engine/crates/rocky-cli/src/commands/ai.rs`.
