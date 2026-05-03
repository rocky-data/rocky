---
title: Troubleshooting
description: Common errors and their solutions
sidebar:
  order: 3
---

This page is a **symptom-first** reference: paste an error message into your search and find the fix. For the inverse view — categories of failure with a recovery playbook per category — see [Failure modes](./failure-modes).

## Compilation Errors

### "Model 'X' not found"

A model references another model that doesn't exist in the project.

**Possible causes:**
- The referenced model file is missing or misnamed
- The `name` field in the `.toml` sidecar doesn't match the file name
- The model is in a subdirectory that Rocky isn't scanning

**Fix:** Check that the referenced model exists in `models/` and its `name` field matches. Rocky auto-discovers dependencies from SQL table references — bare names matching model file names become DAG edges.

### "Type mismatch on column 'X'"

A column's type changed between upstream and downstream models.

**Fix:** Run `rocky compile` for detailed diagnostics. Add an explicit `CAST()` to convert types, or update the upstream model. If this is expected (schema evolution), use `rocky ai-sync` to propagate changes.

### "Join key type mismatch"

Two models being joined have the same column name but different types.

**Fix:** Add explicit `CAST()` on one side of the join to match types. The diagnostic message shows which models and types are involved.

### "Contract violation"

A model's output doesn't satisfy its data contract.

**Fix:** Check the `.contract.toml` file for required columns and types. Either update the model to produce the required schema or update the contract.

## LSP / IDE Issues

### Language server not starting

The VS Code extension can't connect to `rocky lsp`.

**Possible causes:**
- Rocky binary not installed or not on `PATH`
- Wrong path in `rocky.server.path` VS Code setting
- Binary is for a different platform (e.g., Linux binary on macOS)

**Fix:**
```bash
# Verify rocky is installed
rocky --version

# Check the path VS Code is using
# In VS Code: Settings → Rocky → Server Path
```

### No diagnostics or hover info

The LSP is connected but not providing features.

**Possible causes:**
- No `models/` directory in the workspace root
- Models have syntax errors that prevent compilation

**Fix:** Ensure your workspace root contains a `models/` directory. Check the Rocky output channel in VS Code (View → Output → Rocky Language Server) for error messages.

## AI Features

### "ANTHROPIC_API_KEY not set"

AI commands require an Anthropic API key.

**Fix:**
```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

Add to your shell profile (`~/.zshrc`, `~/.bashrc`) for persistence.

### AI generation produces incorrect code

The compile-verify loop retries up to 3 times but may still fail.

**Fix:** Try a more specific intent description. Include:
- The grain (what one row represents)
- Key columns and their sources
- Filter conditions
- Aggregation logic

### "Compilation failed after 3 attempts"

The AI couldn't generate valid code within the retry budget.

**Fix:** The intent may be too complex for a single model. Break it into smaller models with explicit upstream dependencies.

## Connection Errors

### Databricks: "401 Unauthorized"

**Possible causes:**
- Personal Access Token expired
- OAuth M2M credentials incorrect
- Token doesn't have access to the specified warehouse

**Fix:** Regenerate the token in Databricks workspace settings. For OAuth M2M, verify `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`.

### Databricks: "Statement execution timeout"

Long-running queries exceeding the configured timeout.

**Fix:** Increase the timeout on the Databricks adapter:
```toml
[adapter.prod]
type = "databricks"
timeout_secs = 600  # 10 minutes
```

For large full-refresh syncs, consider switching to incremental strategy.

### Fivetran: "403 Forbidden"

**Fix:** Verify `FIVETRAN_API_KEY` and `FIVETRAN_API_SECRET`. Ensure the API key has access to the specified `destination_id`.

## State Store

### "State file locked"

Another Rocky process is holding the state file lock.

**Fix:** Check for running Rocky processes:
```bash
ps aux | grep rocky
```

If no process is running, the lock may be stale. Remove the lock file:
```bash
rm -f .rocky-state.redb.lock
```

### "State file corrupted"

The embedded redb state file is damaged.

**Fix:** Delete the state file and re-run. This resets all watermarks, so the next run will be a full refresh:
```bash
rm .rocky-state.redb
rocky run --filter client=acme
```

## Build Issues

### DuckDB compilation fails (out of memory)

Building Rocky from source requires significant memory for DuckDB's C++ compilation.

**Fix:** Close other applications during build, or use a swap file:
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

Or install from a pre-built binary instead of building from source.
