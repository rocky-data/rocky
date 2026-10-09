//! Parquet export of the compiled project graph (`rocky docs --format parquet`).
//!
//! Writes one Parquet file per table into the output directory so DuckDB (or
//! any Parquet reader) can query the project:
//!
//! | file                     | one row per                                   |
//! |--------------------------|-----------------------------------------------|
//! | `models.parquet`         | model                                         |
//! | `columns.parquet`        | model output column                           |
//! | `edges.parquet`          | model-level dependency (model or source)      |
//! | `column_lineage.parquet` | column-level lineage edge                     |
//! | `tests.parquet`          | declared test                                 |
//! | `contracts.parquet`      | contract constraint                           |
//! | `sources.parquet`        | external table the models read                |
//!
//! Every table is written even when it has no rows, so queries never fail on
//! a missing file.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use rocky_core::project_docs::{DocTest, ProjectDocs};

/// Table file names, in write order.
pub const TABLE_FILES: [&str; 7] = [
    "models.parquet",
    "columns.parquet",
    "edges.parquet",
    "column_lineage.parquet",
    "tests.parquet",
    "contracts.parquet",
    "sources.parquet",
];

/// A column under construction: its Arrow field and values.
enum Col {
    Str(&'static str, Vec<Option<String>>),
    Int(&'static str, Vec<Option<i64>>),
    Bool(&'static str, Vec<Option<bool>>),
}

fn s(v: impl Into<String>) -> Option<String> {
    Some(v.into())
}

fn write_table(dir: &Path, file: &str, cols: Vec<Col>) -> Result<()> {
    let mut fields = Vec::with_capacity(cols.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for col in cols {
        match col {
            Col::Str(name, v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(v)));
            }
            Col::Int(name, v) => {
                fields.push(Field::new(name, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(v)));
            }
            Col::Bool(name, v) => {
                fields.push(Field::new(name, DataType::Boolean, true));
                arrays.push(Arc::new(BooleanArray::from(v)));
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .with_context(|| format!("failed to build {file}"))?;
    let path = dir.join(file);
    let handle = std::fs::File::create(&path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = ArrowWriter::try_new(handle, schema, None)
        .with_context(|| format!("failed to start {file}"))?;
    writer
        .write(&batch)
        .with_context(|| format!("failed to write {file}"))?;
    writer
        .close()
        .with_context(|| format!("failed to finish {file}"))?;
    Ok(())
}

/// Write all tables into `dir` (created if missing). Returns the file names.
pub fn write_parquet_tables(docs: &ProjectDocs, dir: &Path) -> Result<Vec<String>> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create output directory {}", dir.display()))?;

    // models
    let mut name = vec![];
    let mut target = vec![];
    let mut strategy = vec![];
    let mut description = vec![];
    let mut file = vec![];
    let mut sql = vec![];
    let mut access = vec![];
    let mut group = vec![];
    let mut owner = vec![];
    let mut version = vec![];
    let mut lag = vec![];
    let mut time_col = vec![];
    let mut fresh_sev = vec![];
    let mut tags = vec![];
    let mut has_contract = vec![];
    let mut column_count = vec![];
    let mut test_count = vec![];
    for m in &docs.index.models {
        let detail = docs.details.get(&m.name);
        name.push(s(&m.name));
        target.push(s(&m.target));
        strategy.push(s(&m.strategy));
        description.push(m.description.clone());
        file.push(detail.map(|d| d.file.clone()));
        sql.push(detail.map(|d| d.sql.clone()));
        access.push(m.governance.as_ref().map(|g| g.access.clone()));
        group.push(m.governance.as_ref().and_then(|g| g.group.clone()));
        owner.push(m.governance.as_ref().and_then(|g| g.owner.clone()));
        version.push(m.governance.as_ref().and_then(|g| g.version.clone()));
        let fresh = detail.and_then(|d| d.freshness.as_ref());
        lag.push(fresh.and_then(|f| i64::try_from(f.max_lag_seconds).ok()));
        time_col.push(fresh.and_then(|f| f.time_column.clone()));
        fresh_sev.push(fresh.and_then(|f| f.severity.clone()));
        tags.push(detail.map(|d| serde_json::to_string(&d.tags).unwrap_or_else(|_| "{}".into())));
        has_contract.push(Some(detail.is_some_and(|d| d.contract.is_some())));
        column_count.push(i64::try_from(m.columns.len()).ok());
        test_count.push(i64::try_from(m.tests.len()).ok());
    }
    write_table(
        dir,
        "models.parquet",
        vec![
            Col::Str("name", name),
            Col::Str("target", target),
            Col::Str("strategy", strategy),
            Col::Str("description", description),
            Col::Str("file", file),
            Col::Str("sql", sql),
            Col::Str("access", access),
            Col::Str("group_name", group),
            Col::Str("owner", owner),
            Col::Str("version", version),
            Col::Int("freshness_max_lag_seconds", lag),
            Col::Str("freshness_time_column", time_col),
            Col::Str("freshness_severity", fresh_sev),
            Col::Str("tags", tags),
            Col::Bool("has_contract", has_contract),
            Col::Int("column_count", column_count),
            Col::Int("test_count", test_count),
        ],
    )?;

    // columns
    let mut model = vec![];
    let mut ordinal = vec![];
    let mut cname = vec![];
    let mut dtype = vec![];
    let mut nullable = vec![];
    let mut cdesc = vec![];
    let mut class = vec![];
    for m in &docs.index.models {
        let detail = docs.details.get(&m.name);
        for (i, c) in m.columns.iter().enumerate() {
            model.push(s(&m.name));
            ordinal.push(i64::try_from(i + 1).ok());
            cname.push(s(&c.name));
            dtype.push(s(&c.data_type));
            nullable.push(Some(c.nullable));
            cdesc.push(c.description.clone());
            class.push(detail.and_then(|d| {
                d.classification
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(&c.name))
                    .map(|(_, v)| v.clone())
            }));
        }
    }
    write_table(
        dir,
        "columns.parquet",
        vec![
            Col::Str("model", model),
            Col::Int("ordinal", ordinal),
            Col::Str("name", cname),
            Col::Str("data_type", dtype),
            Col::Bool("nullable", nullable),
            Col::Str("description", cdesc),
            Col::Str("classification", class),
        ],
    )?;

    // edges
    let source_names: std::collections::HashSet<&str> =
        docs.sources.iter().map(|x| x.name.as_str()).collect();
    let mut up = vec![];
    let mut down = vec![];
    let mut kind = vec![];
    for (u, d) in docs.model_edges() {
        kind.push(s(if source_names.contains(u.as_str()) {
            "source"
        } else {
            "model"
        }));
        up.push(s(u));
        down.push(s(d));
    }
    write_table(
        dir,
        "edges.parquet",
        vec![
            Col::Str("upstream", up),
            Col::Str("downstream", down),
            Col::Str("upstream_kind", kind),
        ],
    )?;

    // column_lineage
    let mut sm = vec![];
    let mut sc = vec![];
    let mut tm = vec![];
    let mut tc = vec![];
    let mut tr = vec![];
    for e in &docs.column_lineage {
        sm.push(s(&e.source_model));
        sc.push(s(&e.source_column));
        tm.push(s(&e.target_model));
        tc.push(s(&e.target_column));
        tr.push(s(&e.transform));
    }
    write_table(
        dir,
        "column_lineage.parquet",
        vec![
            Col::Str("source_model", sm),
            Col::Str("source_column", sc),
            Col::Str("target_model", tm),
            Col::Str("target_column", tc),
            Col::Str("transform", tr),
        ],
    )?;

    // tests
    let mut tmodel = vec![];
    let mut tkind = vec![];
    let mut tcol = vec![];
    let mut tsev = vec![];
    let mut tparams = vec![];
    let mut tfilter = vec![];
    for m in &docs.index.models {
        for t in m.tests.iter().map(DocTest::from_decl) {
            tmodel.push(s(&m.name));
            tkind.push(s(t.kind));
            tcol.push(t.column);
            tsev.push(s(t.severity));
            tparams.push(s(t.params));
            tfilter.push(t.filter);
        }
    }
    write_table(
        dir,
        "tests.parquet",
        vec![
            Col::Str("model", tmodel),
            Col::Str("kind", tkind),
            Col::Str("column_name", tcol),
            Col::Str("severity", tsev),
            Col::Str("params", tparams),
            Col::Str("filter", tfilter),
        ],
    )?;

    // contracts
    let mut cmodel = vec![];
    let mut ckind = vec![];
    let mut ccol = vec![];
    let mut ctype = vec![];
    let mut cnull = vec![];
    let mut cdescr = vec![];
    for (model_name, detail) in &docs.details {
        let Some(contract) = &detail.contract else {
            continue;
        };
        let mut push = |k: &str,
                        col: Option<String>,
                        ty: Option<String>,
                        nl: Option<bool>,
                        d: Option<String>| {
            cmodel.push(s(model_name));
            ckind.push(s(k));
            ccol.push(col);
            ctype.push(ty);
            cnull.push(nl);
            cdescr.push(d);
        };
        for c in &contract.columns {
            push(
                "column",
                s(&c.name),
                c.type_name.clone(),
                c.nullable,
                c.description.clone(),
            );
        }
        for c in &contract.required {
            push("required", s(c), None, None, None);
        }
        for c in &contract.protected {
            push("protected", s(c), None, None, None);
        }
        if contract.no_new_nullable {
            push("no_new_nullable", None, None, None, None);
        }
    }
    write_table(
        dir,
        "contracts.parquet",
        vec![
            Col::Str("model", cmodel),
            Col::Str("kind", ckind),
            Col::Str("column_name", ccol),
            Col::Str("type_name", ctype),
            Col::Bool("nullable", cnull),
            Col::Str("description", cdescr),
        ],
    )?;

    // sources
    let mut sname = vec![];
    let mut scols = vec![];
    let mut sused = vec![];
    for src in &docs.sources {
        sname.push(s(&src.name));
        scols.push(i64::try_from(src.columns.len()).ok());
        sused.push(i64::try_from(src.used_by.len()).ok());
    }
    write_table(
        dir,
        "sources.parquet",
        vec![
            Col::Str("name", sname),
            Col::Int("columns_read", scols),
            Col::Int("used_by_count", sused),
        ],
    )?;

    Ok(TABLE_FILES.iter().map(|f| (*f).to_string()).collect())
}
