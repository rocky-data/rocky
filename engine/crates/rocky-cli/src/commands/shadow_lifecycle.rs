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
//!   after a passing comparison   drop what this run created (unless kept)
//! ```
//!
//! Why "already there" is a sound refusal, which it would NOT have been
//! before: `apply_shadow_rewrite` now refuses the incremental family, so
//! every strategy that reaches a shadow write REPLACES its target rather
//! than adding to it — and `cleanup_after` now actually drops. Together
//! those mean a clean default run leaves the name empty. An object sitting
//! there is either somebody else's, a retained object, or failed-run debris. In
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
use rocky_ir::{TableRef, TargetRef};

#[derive(Debug, Clone, Copy)]
pub(crate) enum ShadowKind {
    Table,
    View,
    MaterializedView,
    DynamicTable,
}

impl ShadowKind {
    fn drop_sql(self, dialect: &dyn SqlDialect, formatted: &str) -> String {
        match self {
            Self::Table => dialect.drop_table_sql(formatted),
            Self::View => format!("DROP VIEW IF EXISTS {formatted}"),
            Self::MaterializedView => format!("DROP MATERIALIZED VIEW IF EXISTS {formatted}"),
            Self::DynamicTable => format!("DROP DYNAMIC TABLE IF EXISTS {formatted}"),
        }
    }
}

/// A shadow object this run intends to write, or wrote.
#[derive(Debug, Clone)]
pub(crate) struct ShadowObject {
    /// The model that owns it, for the message.
    pub(crate) model: String,
    /// The derived shadow target.
    pub(crate) target: TargetRef,
    /// The production object paired with this shadow target.
    pub(crate) production: TargetRef,
    pub(crate) kind: ShadowKind,
}

/// A failed describe is absence only when a successful catalog read confirms it.
pub(crate) async fn target_is_absent(
    warehouse: &dyn WarehouseAdapter,
    target: &TargetRef,
) -> Result<bool> {
    let table = TableRef {
        catalog: target.catalog.clone(),
        schema: target.schema.clone(),
        table: target.table.clone(),
    };
    match warehouse.describe_table(&table).await {
        Ok(_) => Ok(false),
        Err(describe_error) => {
            let names = warehouse
                .list_tables(&target.catalog, &target.schema)
                .await
                .map_err(|list_error| anyhow::anyhow!(
                    "cannot determine whether {} exists: describe failed: {describe_error}; catalog read failed: {list_error}",
                    target.full_name()
                ))?;
            if names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&target.table))
            {
                anyhow::bail!(
                    "cannot determine whether {} exists: describe failed: {describe_error}; catalog still lists it",
                    target.full_name()
                );
            }
            Ok(true)
        }
    }
}

/// Refuse the run if anything already sits at a shadow target.
///
/// Runs once, before any model executes, so a refusal leaves the warehouse
/// exactly as it was — the same posture as the target-collision preflight
/// (#1461) and the `metadata_columns` guard (#1594).
///
/// A failed describe requires a successful catalog listing that confirms absence.
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
        let occupied = !target_is_absent(warehouse, &object.target).await?;
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
                object.kind.drop_sql(dialect, &formatted)
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
        let sql = object.kind.drop_sql(dialect, &formatted);
        if let Err(e) = warehouse.execute_statement(&sql).await {
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
    use async_trait::async_trait;
    use rocky_core::traits::{AdapterError, AdapterResult, QueryResult};
    use rocky_ir::ColumnInfo;
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
            production: TargetRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "orders".into(),
            },
            kind: ShadowKind::Table,
        }
    }

    fn duckdb() -> DuckDbWarehouseAdapter {
        let shared = Arc::new(Mutex::new(DuckDbConnector::in_memory().expect("duckdb")));
        DuckDbWarehouseAdapter::from_shared(shared)
    }

    struct UncertainWarehouse {
        inner: DuckDbWarehouseAdapter,
        catalog_error: bool,
    }

    #[async_trait]
    impl WarehouseAdapter for UncertainWarehouse {
        fn dialect(&self) -> &dyn SqlDialect { self.inner.dialect() }
        async fn execute_statement(&self, _sql: &str) -> AdapterResult<()> {
            panic!("an uncertain ownership read must never authorize a write")
        }
        async fn execute_query(&self, _sql: &str) -> AdapterResult<QueryResult> {
            panic!("an uncertain ownership read must never authorize a query")
        }
        async fn describe_table(&self, _table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
            Err(AdapterError::msg("transient metadata failure"))
        }
        async fn list_tables(&self, _catalog: &str, _schema: &str) -> AdapterResult<Vec<String>> {
            if self.catalog_error {
                Err(AdapterError::msg("catalog unavailable"))
            } else {
                Ok(vec!["orders_rocky_shadow".to_string()])
            }
        }
    }

    #[tokio::test]
    async fn uncertain_metadata_never_grants_shadow_ownership() {
        for catalog_error in [false, true] {
            let wh = UncertainWarehouse { inner: duckdb(), catalog_error };
            let err = refuse_occupied_shadow_targets(
                &wh, wh.dialect(), &[object("orders_rocky_shadow")]
            ).await.expect_err("uncertain metadata cannot prove absence");
            let message = err.to_string();
            assert!(message.contains("cannot determine whether"), "{message}");
        }
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

    /// The caller scopes this refusal to one-off shadow runs.
    ///
    /// This test pins the reason, because the scoping is a decision and not
    /// an oversight: a named `--branch` replaces its objects on every
    /// re-run. A one-off `--keep-shadow` run still refuses leftovers. Rocky
    /// cannot tell its own branch object from a stranger's without a
    /// persisted owner record, so #1273 stays open for branch objects.
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
