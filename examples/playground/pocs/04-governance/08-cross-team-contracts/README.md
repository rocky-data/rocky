# Cross-team contracts

## Feature

A consumer Rocky project imports a producer project's published IR snapshot
and `rocky compile` fails with **E030** when the producer drops a column the
consumer reads (and **E033** when a configured recipe-hash pin no longer
matches the vendored snapshot).

## Why it's distinctive

Most data stacks discover a producer's breaking change at runtime, when a query
errors in production or a downstream table silently goes null. Rocky lets a
producer team publish a typed snapshot of their compiled project, and a
consumer team vendor it and compile against it. The producer dropping a column
the consumer still selects becomes a **compile error in the consumer's repo**,
caught in CI before any SQL runs. The contract is the producer's actual typed
output (the same `ProjectIr` the breaking-change classifier already diffs), not
a hand-maintained schema doc that drifts from reality.

The column check is data-grounded: it uses SQL column lineage to flag only the
columns the consumer actually references. Drop a column nobody reads and the
consumer keeps compiling.

## Layout

```
08-cross-team-contracts/
  run.sh                              # 4-step end-to-end demo (single entrypoint)
  orders-producer/                    # the producer project
    rocky.toml
    data/seed.sql                     # raw__orders.orders (id, customer_id, amount, shipped_at)
    models/orders.sql                 # SELECT id, customer_id, amount, shipped_at FROM raw__orders.orders
    models/orders.toml                # target shop.core.orders
    orders.dropped.sql                # breaking variant WITHOUT shipped_at (swapped into models/ at step 3)
    vendor-out/                       # producer writes project-ir.json here
  shipments-consumer/                 # the consumer project
    rocky.toml                        # [imports.orders] path/snapshot/baseline/pin
    models/shipments.sql              # SELECT id, customer_id, shipped_at FROM shop.core.orders
    models/shipments.toml             # [[sources]] = shop.core.orders (the link key)
    vendor/orders/baseline.json       # pre-drop snapshot (written by run.sh)
    vendor/orders/current.json        # current snapshot (rewritten after the drop)
```

## Run

```bash
# Build the engine binary the POC uses (run.sh resolves the worktree's
# release binary, not a stale PATH install):
(cd engine && cargo build --release -p rocky)

# Then:
cd examples/playground/pocs/04-governance/08-cross-team-contracts
./run.sh
```

The two key commands the POC exercises:

```bash
# Producer publishes its typed snapshot (--with-seed makes columns concrete):
rocky publish-ir --models orders-producer/models \
    --out orders-producer/vendor-out/project-ir.json --with-seed

# Consumer compile is the gate — it reads the [imports.orders] block
# (--config is a global flag, so it precedes the subcommand):
rocky --config shipments-consumer/rocky.toml compile --models shipments-consumer/models
```

## Expected output

Step 2 (before the drop) compiles clean. Step 4 (after the producer drops
`shipped_at`) fails:

```
--- Step 4: consumer recompiles — expect E030 (contract gate fires)
    PASS: compile failed with E030 (exit 1):
      "message":"model 'shipments' references column 'shipped_at' which the imported producer 'orders' (shop.core.orders) no longer outputs"

Cross-team contract enforced: the producer's breaking change was
caught at the consumer's compile, before any SQL ran.
```

`run.sh` inverts the exit code: a failing step-4 compile that contains E030 is
the success condition, so the POC exits 0.

## Filtered vs unfiltered E030

This POC's consumer is the clean single-source, explicit-column case, so E030
is **filtered**: only columns the consumer's SQL actually references are
flagged. When a consumer model uses `SELECT *` (columns can't be enumerated) or
joins multiple sources (an unqualified column can't be attributed to one
producer), the check falls back to **unfiltered**: every dropped producer
column is flagged against that model. That over-reports rather than letting a
breaking change slip through silently.
