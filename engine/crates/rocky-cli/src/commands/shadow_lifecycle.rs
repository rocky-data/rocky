//! Ownership and lifecycle for the objects a `--shadow` run writes (#1273).
//!
//! Before this module, a shadow target was a *name* Rocky derived and then
//! wrote to. Nothing asked whether an object already sat at that name, and
//! nothing removed the object afterwards. Three symptoms fell out of that
//! one gap, and the contract below answers all three together because
//! fixing any one of them in isolation contradicts the others.
//!
//! **The contract: a shadow target is disposable and Rocky-owned.**
//!
//! ```text
//!   before a shadow run writes   is anything already at this name?
//!                                  yes ─▶ REFUSE, naming the object
//!                                  no  ─▶ this run owns it
//!   after the run succeeds       drop what this run created (cleanup_after)
//! ```
//!
//! Why "already there" is a sound refusal, which it would NOT have been
//! before: `apply_shadow_rewrite` now refuses the incremental family, so
//! every strategy that reaches a shadow write REPLACES its target rather
//! than adding to it — and `cleanup_after` now actually drops. Together
//! those mean a clean run leaves the name empty, so an object sitting
//! there is either somebody else's or the debris of a run that failed. In
//! both cases writing over it is the thing #1273 reported.
//!
//! **What "Rocky-owned" means here, exactly.** It means *this run created
//! it*. That is the strictest reading, and it is the only one available
//! without persisting ownership: a state record would have to survive a
//! deleted state file and a different machine to be trusted, and a
//! warehouse tag is not portable across the adapters Rocky targets. The
//! cost of the strict reading is that a shadow run which failed part-way
//! leaves objects that the next run refuses — so the refusal names the
//! object and prints the statement that clears it.

use anyhow::Result;
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::TargetRef;

/// A shadow object this run intends to write, or wrote.
#[derive(Debug, Clone)]
pub(crate) struct ShadowObject {
    /// The model that owns it, for the message.
    pub(crate) model: String,
    /// The derived shadow target.
    pub(crate) target: TargetRef,
}

/// Refuse the run if anything already sits at a shadow target.
///
/// Runs once, before any model executes, so a refusal leaves the warehouse
/// exactly as it was — the same posture as the target-collision preflight
/// (#1461) and the `metadata_columns` guard (#1594).
///
/// A `describe_table` that fails is treated as "absent". That is deliberate
/// and is the same reading `execute_one_plain_model` takes: the adapters do
/// not agree on a typed not-found error, so an existence probe can only
/// distinguish "returned columns" from "did not". The consequence is a
/// missed refusal on a transient error, never a spurious one — and the
/// alternative, refusing every shadow run whose probe hiccuped, would make
/// the feature unusable on a flaky connection.
///
/// # Errors
///
/// Names the first occupied target, the model that would have written it,
/// and the statement that clears it.
pub(crate) async fn refuse_occupied_shadow_targets(
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn SqlDialect,
    objects: &[ShadowObject],
) -> Result<()> {
    for object in objects {
        let table_ref = rocky_ir::TableRef {
            catalog: object.target.catalog.clone(),
            schema: object.target.schema.clone(),
            table: object.target.table.clone(),
        };
        let occupied = warehouse
            .describe_table(&table_ref)
            .await
            .map(|columns| !columns.is_empty())
            .unwrap_or(false);
        if occupied {
            let formatted = dialect
                .format_table_ref(
                    &object.target.catalog,
                    &object.target.schema,
                    &object.target.table,
                )
                .map_err(|e| anyhow::anyhow!("formatting the shadow target ref: {e}"))?;
            anyhow::bail!(
                "shadow target {formatted} already exists, and this run did not create it — \
                 refusing to replace an object Rocky does not own. A shadow object is \
                 disposable and belongs to the run that made it; anything already at that \
                 name is either not Rocky's or debris from a run that did not finish. \
                 Model '{}' would have written it. Drop it if it is debris:\n    {}",
                object.model,
                dialect.drop_table_sql(&formatted)
            );
        }
    }
    Ok(())
}

/// Drop the shadow objects this run created.
///
/// Called only on a successful run, and only when `cleanup_after` is set —
/// which is the default for a one-off `--shadow`, and deliberately off for
/// a named `--branch`, whose objects are the point of the branch.
///
/// Best-effort by design: a failed drop is reported to the caller as a
/// warning rather than failing a run whose real work already succeeded.
/// The next run's ownership refusal is what stops the leftover being
/// silently written over, so a missed drop degrades to a refusal with a
/// remedy, never to a silent replace.
pub(crate) async fn drop_owned_shadow_objects(
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn SqlDialect,
    objects: &[ShadowObject],
) -> Vec<String> {
    let mut warnings = Vec::new();
    for object in objects {
        let formatted = match dialect.format_table_ref(
            &object.target.catalog,
            &object.target.schema,
            &object.target.table,
        ) {
            Ok(formatted) => formatted,
            Err(e) => {
                warnings.push(format!(
                    "could not format the shadow target for model '{}' to drop it: {e}",
                    object.model
                ));
                continue;
            }
        };
        if let Err(e) = warehouse
            .execute_statement(&dialect.drop_table_sql(&formatted))
            .await
        {
            warnings.push(format!(
                "could not drop shadow object {formatted} for model '{}': {e}. It stays on the \
                 warehouse; the next shadow run will refuse until it is removed",
                object.model
            ));
        }
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_duckdb::DuckDbConnector;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
    use std::sync::{Arc, Mutex};

    fn object(table: &str) -> ShadowObject {
        ShadowObject {
            model: "orders".to_string(),
            target: TargetRef {
                catalog: String::new(),
                schema: "main".into(),
                table: table.into(),
            },
        }
    }

    fn duckdb() -> DuckDbWarehouseAdapter {
        let shared = Arc::new(Mutex::new(DuckDbConnector::in_memory().expect("duckdb")));
        DuckDbWarehouseAdapter::from_shared(shared)
    }

    /// An empty name is this run's to take.
    #[tokio::test]
    async fn an_unoccupied_target_passes() {
        let wh = duckdb();
        let dialect = wh.dialect();
        refuse_occupied_shadow_targets(&wh, dialect, &[object("orders_rocky_shadow")])
            .await
            .expect("nothing is there, so the run owns the name");
    }

    /// The #1273 case: somebody else's table at the derived name. The
    /// refusal names the object, the model, and the statement that clears
    /// it — and the table is still there afterwards.
    #[tokio::test]
    async fn an_occupied_target_is_refused_and_left_untouched() {
        let wh = duckdb();
        let dialect = wh.dialect();
        wh.execute_statement(
            "CREATE TABLE main.orders_rocky_shadow AS SELECT 'do-not-touch' AS sentinel",
        )
        .await
        .expect("seed somebody else's table");

        let err = refuse_occupied_shadow_targets(&wh, dialect, &[object("orders_rocky_shadow")])
            .await
            .expect_err("an occupied shadow target must refuse");
        let msg = format!("{err:#}");
        assert!(msg.contains("orders_rocky_shadow"), "{msg}");
        assert!(msg.contains("does not own"), "{msg}");
        assert!(msg.contains("DROP TABLE"), "the remedy is printed: {msg}");
        assert!(msg.contains("'orders'"), "the model is named: {msg}");

        let columns = wh
            .describe_table(&rocky_ir::TableRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "orders_rocky_shadow".into(),
            })
            .await
            .expect("the seeded table is still there");
        assert_eq!(
            columns.into_iter().map(|c| c.name).collect::<Vec<_>>(),
            vec!["sentinel".to_string()],
            "a refusal must not have touched the object it refused over"
        );
    }

    /// The refusal is scoped to the DISPOSABLE mode, and the caller is what
    /// scopes it (`run.rs` calls this only when `cleanup_after` is set).
    ///
    /// This test pins the reason, because the scoping is a decision and not
    /// an oversight: with `cleanup_after` off, the previous run's objects
    /// are supposed to still be there and the next run is supposed to
    /// replace them, so an unconditional refusal would make a named
    /// `--branch` refuse its own workspace on every re-run. Rocky cannot
    /// tell its own leftover from a stranger's without a persisted owner
    /// record, so the persistent mode keeps no per-object check and #1273
    /// stays open for it.
    ///
    /// What this function must NOT do is decide that for itself — a future
    /// caller that forgets the gate should get a refusal, not a silent pass.
    #[tokio::test]
    async fn the_check_itself_is_unconditional_and_the_caller_scopes_it() {
        let wh = duckdb();
        let dialect = wh.dialect();
        wh.execute_statement("CREATE TABLE main.orders_rocky_shadow AS SELECT 1 AS id")
            .await
            .expect("seed");
        assert!(
            refuse_occupied_shadow_targets(&wh, dialect, &[object("orders_rocky_shadow")])
                .await
                .is_err(),
            "the primitive always refuses an occupied target; only the call site is conditional"
        );
    }

    /// Cleanup removes what the run created, and is idempotent — a second
    /// drop of an absent object is not an error, so a crash between the
    /// drop and the record cannot wedge the next run.
    #[tokio::test]
    async fn cleanup_drops_owned_objects_and_is_idempotent() {
        let wh = duckdb();
        let dialect = wh.dialect();
        wh.execute_statement("CREATE TABLE main.orders_rocky_shadow AS SELECT 1 AS id")
            .await
            .expect("create the shadow object");

        let objects = [object("orders_rocky_shadow")];
        assert!(
            drop_owned_shadow_objects(&wh, dialect, &objects)
                .await
                .is_empty(),
            "dropping an object this run created reports nothing"
        );
        let gone = wh
            .describe_table(&rocky_ir::TableRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "orders_rocky_shadow".into(),
            })
            .await
            .map(|c| c.is_empty())
            .unwrap_or(true);
        assert!(gone, "the shadow object is dropped");

        assert!(
            drop_owned_shadow_objects(&wh, dialect, &objects)
                .await
                .is_empty(),
            "a second drop is a no-op, not an error"
        );

        // And the name is free again: the next run's ownership check passes.
        refuse_occupied_shadow_targets(&wh, dialect, &objects)
            .await
            .expect("cleanup leaves the name available for the next run");
    }
}
