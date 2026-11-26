---
title: Troubleshooting
description: Look up the error message you got, then follow the numbered fix
sidebar:
  order: 1
---

Search this page for the error message you got, then follow the steps. For failure categories with a recovery playbook, see [Failure modes](/advanced/failure-modes/).

## Compile errors

### "unknown dependency 'X' referenced by 'Y'"

Model `Y` lists `X` in its `depends_on`, but the project has no model named `X`.

**Likely causes**

- A typo in `depends_on`. The message adds "did you mean 'Z'?" when a model name is close.
- The model file for `X` is missing or misnamed.
- The `name` field in the `.toml` sidecar does not match the file name.
- `X` is a source table, not a model.

**Fix**

1. Check the spelling against the suggestion in the message.
2. Check that the model file exists under `models/` and that its `name` field matches.
3. Remove the entry when `X` is a source table. Rocky reads a source table from the SQL; it needs no `depends_on`.

### "join key type mismatch on column 'X'"

Two upstream models in a join share a column name but have no common type (`E001`). The diagnostic names both models and types. Add an explicit `CAST()` on one side, or change the upstream model. For intentional schema evolution, run `rocky ai-sync` to propagate it downstream.

### "Contract violation"

A model's output does not satisfy its data contract (`E010`–`E014`), for example `column 'X' type mismatch: contract expects …`. Read the model's `.contract.toml`, then change the model or the contract. See [Failure modes](/advanced/failure-modes/#2-contract-violations).

## LSP and IDE problems

### The language server does not start

The VS Code extension cannot connect to `rocky lsp`.

**Likely causes**

- The Rocky binary is not installed or not on `PATH`.
- The `rocky.server.path` VS Code setting points somewhere wrong.
- The binary is built for another platform, such as a Linux binary on macOS.

**Fix**

1. Confirm the binary runs at all:

   ```bash
   rocky --version
   ```

2. Check which path VS Code is using, under Settings → Rocky → Server Path.
3. Point that setting at the binary you just ran, or put the binary on `PATH`.

### No diagnostics or hover information

The language server connects but shows no types and no errors.

**Likely causes**

- The workspace root has no `models/` directory.
- The models have syntax errors that stop compilation.

**Fix**

1. Confirm your workspace root contains a `models/` directory.
2. Open the Rocky output channel in VS Code, under View → Output → Rocky Language Server.
3. Fix the errors it reports there.

## AI command problems

### "ANTHROPIC_API_KEY not set"

The AI commands need an Anthropic API key in the environment.

**Fix**

1. Export the key:

   ```bash
   export ANTHROPIC_API_KEY=sk-ant-...
   ```

2. Add the same line to your shell profile (`~/.zshrc`, `~/.bashrc`) so it survives a new shell.

### The generated model is wrong

The compile-verify loop retries up to 3 times, and it can still land on wrong SQL.

**Fix**

1. Rewrite the intent to be more specific. Name the grain, which is what one row of the model represents.
2. Name the key columns and where they come from.
3. State the filter conditions.
4. State the aggregation logic.

### "Compilation failed after 3 attempts"

The AI could not produce valid code inside the retry budget.

**Fix**

1. Split the intent into smaller models. One model per idea.
2. Wire the pieces together with explicit upstream dependencies.

## Connection errors

Adapter auth, quota and timeout failures: see [Adapter / runtime failures](/advanced/failure-modes/#5-adapter--runtime-failures). Two quick fixes:

- **Databricks "Statement execution timeout":** raise `timeout_secs` on the adapter (`[adapter.prod]`, for example `timeout_secs = 600`). For a large full-refresh sync, switch the model to an incremental strategy.
- **Fivetran "403 Forbidden":** check `api_key` and `api_secret` in the adapter block and the environment variables they read, and that the key has access to the configured `destination_id`.

## State store problems

The [state store](/reference/glossary/#state-store) is the embedded redb database where Rocky keeps run records, watermarks, and plans.

### "State file locked" and "State file corrupted"

Recovery for both: [State store failures](/advanced/failure-modes/#6-state-store-failures). The state store is at `models/.rocky-state.redb` by default (a legacy project has `.rocky-state.redb` in the working directory). Run `rocky doctor` to confirm the path. Deleting the file resets every watermark, so the next run is a full refresh:

```bash
rm models/.rocky-state.redb
plan_id=$(rocky plan --filter client=acme --output json | jq -r .plan_id)
rocky apply "$plan_id"
```

## Build problems

### Building from source runs out of memory

DuckDB's C++ compilation needs a lot of memory to build Rocky from source.

**Fix**

1. Install a pre-built binary instead of building from source. This is the fastest fix.
2. If you must build, close other applications first.
3. If that is not enough, add a swap file for the build:

   ```bash
   # Create a 4GB swap file
   sudo fallocate -l 4G /tmp/rocky-swap
   sudo chmod 600 /tmp/rocky-swap
   sudo mkswap /tmp/rocky-swap
   sudo swapon /tmp/rocky-swap

   # Build
   cargo build --release

   # Clean up
   sudo swapoff /tmp/rocky-swap
   sudo rm /tmp/rocky-swap
   ```
