# Cross-team contracts

## Feature

A consumer Rocky project imports a producer project's published IR snapshot, and
`rocky compile` fails with **E030** when the producer drops a column the
consumer reads (and **E033** when a configured recipe-hash pin no longer matches
the vendored snapshot). The check uses SQL column lineage, so dropping a column
nobody reads does not fail the consumer.

`orders-producer/` publishes. `shipments-consumer/` configures `[imports.orders]`
and vendors `baseline.json` (pre-drop) and `current.json` under `vendor/orders/`.
The consumer model's `[[sources]]` `shop.core.orders` is the link key. At step 3,
`orders.dropped.sql` (no `shipped_at`) replaces the producer model.

## Run

```bash
cd examples/playground/pocs/04-governance/08-cross-team-contracts
./run.sh
```

`run.sh` picks the binary in this order: `$ROCKY_BIN`, then
`engine/target/release/rocky`, then `engine/target/debug/rocky`, then `rocky`
on `PATH`. Build one with `(cd engine && cargo build -p rocky)` if none
exists.

The two key commands:

```bash
# Producer publishes its typed snapshot (--with-seed makes columns concrete):
rocky publish-ir --models orders-producer/models \
    --out orders-producer/vendor-out/project-ir.json --with-seed

# Consumer compile is the gate — it reads the [imports.orders] block
# (--config is a global flag, so it precedes the subcommand):
rocky --config shipments-consumer/rocky.toml compile --models shipments-consumer/models
```

## Expected output

Step 2 (before the drop) compiles clean. Step 4 (after it) fails:

```
--- Step 4: consumer recompiles — expect E030 (contract gate fires)
    PASS: compile failed with E030 (exit 1):
      "message": "model 'shipments' references column 'shipped_at' which the imported producer 'orders' (shop.core.orders) no longer outputs"
```

A failing step-4 compile with E030 is the success condition, so the POC exits 0.

## Filtered vs unfiltered E030

Here the consumer is single-source with explicit columns, so E030 is
**filtered** to the columns its SQL references. With `SELECT *` or a multi-source
join, the check is **unfiltered**: every dropped producer column is flagged
against that model, over-reporting rather than missing a breaking change.
