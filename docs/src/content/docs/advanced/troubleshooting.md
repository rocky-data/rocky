---
title: Troubleshooting
description: Look up the error message you got, then follow the numbered fix
sidebar:
  order: 3
---

This page is symptom-first. Search it for the error message you got, then follow the numbered steps under it.

For the opposite view, categories of failure with a recovery playbook for each, see [Failure modes](/advanced/failure-modes/).

## Compile errors

### "Model 'X' not found"

A model references another model that the project does not contain.

**Likely causes**

- The referenced model file is missing or misnamed.
- The `name` field in the `.toml` sidecar does not match the file name.
- The model sits in a subdirectory that Rocky does not scan.

**Fix**

1. Check that the referenced model exists under `models/`.
2. Check that its `name` field matches the file name.
3. Check the SQL reference itself. Rocky discovers dependencies from SQL table references, and a bare name that matches a model file name becomes a DAG edge.

### "Type mismatch on column 'X'"

A column's type differs between the upstream model and the downstream model.

**Fix**

1. Run `rocky compile` to see which two types disagree and where.
2. Add an explicit `CAST()` to convert one side, or change the upstream model to produce the expected type.
3. If the change is intentional schema evolution, run `rocky ai-sync` to propagate it downstream.

### "Join key type mismatch"

Two models in a join share a column name but not its type.

**Fix**

1. Read the diagnostic. It names both models and both types.
2. Add an explicit `CAST()` on one side of the join so the types match.

### "Contract violation"

A model's output does not satisfy its data contract.

**Fix**

1. Open the model's `.contract.toml` file and read the required columns and types.
2. Choose one side to change. Either update the model to produce the required schema, or update the contract.

## LSP and IDE problems

### The language server does not start

The VS Code extension cannot connect to `rocky lsp`.

**Likely causes**

- The Rocky binary is not installed, or it is not on `PATH`.
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

### Databricks: "401 Unauthorized"

**Likely causes**

- The Personal Access Token expired.
- The OAuth M2M credentials are wrong.
- The token has no access to the warehouse you named.

**Fix**

1. Regenerate the token in your Databricks workspace settings.
2. For OAuth M2M, check `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`.

### Databricks: "Statement execution timeout"

A query ran longer than the configured timeout.

**Fix**

1. Raise the timeout on the Databricks adapter:

   ```toml
   [adapter.prod]
   type = "databricks"
   timeout_secs = 600  # 10 minutes
   ```

2. For a large full-refresh sync, switch the model to an incremental strategy instead of raising the timeout further.

### Fivetran: "403 Forbidden"

**Fix**

1. Check `FIVETRAN_API_KEY` and `FIVETRAN_API_SECRET`.
2. Confirm the API key has access to the `destination_id` you configured.

## State store problems

The [state store](/reference/glossary/#state-store) is the embedded redb database where Rocky keeps run records, watermarks, and plans.

### "State file locked"

Another Rocky process holds the state file lock.

**Fix**

1. Look for a running Rocky process:

   ```bash
   ps aux | grep rocky
   ```

2. If one is running, wait for it or stop it. Two Rocky runs cannot hold the lock at the same time.
3. If none is running, the lock is stale. Remove it:

   ```bash
   rm -f models/.rocky-state.redb.lock
   ```

The state store lives at `models/.rocky-state.redb` by default. Run `rocky doctor` to confirm the path. A project on the legacy current-directory state file has `.rocky-state.redb.lock` in the working directory instead.

### "State file corrupted"

The embedded redb state file is damaged.

**Fix**

1. Delete the state file and re-run. This resets every watermark, so the next run is a full refresh:

   ```bash
   rm models/.rocky-state.redb   # legacy projects: rm .rocky-state.redb in the current directory
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
