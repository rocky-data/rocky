//! `rocky export-openapi <path>` — generate an OpenAPI 3.1 document for the
//! `rocky serve` HTTP API.
//!
//! The document is assembled from two sources, both already in the binary:
//!
//! - **`components/schemas`** is hoisted from the same in-process JSON Schema
//!   registry that backs `rocky export-schemas`
//!   ([`super::export_schemas::schemas`]). Each registered schema is a
//!   standalone Draft-07 document with its own `definitions` and
//!   `#/definitions/…` refs; this generator relocates every definition into one
//!   shared `components/schemas` map, deduplicates the types that appear in
//!   more than one document (`AdapterConfig` and friends), and rewrites every
//!   `#/definitions/X` pointer to `#/components/schemas/X`.
//! - **`paths`** is built from a route table that mirrors the axum router in
//!   [`crate::api`]. The paths are not derivable from the schemas, so the table
//!   is maintained here and pinned against drift by a test that compares it to
//!   [`crate::api::api_v1_routes`].
//!
//! The result is validated against the embedded OpenAPI 3.1 meta-schema (via
//! `boon`, offline, no network) and against a "every `$ref` resolves" check
//! before it is written, so a broken document can never be emitted.
//!
//! This command emits an OpenAPI document, not a `--output json` payload, so it
//! is deliberately **not** registered in [`super::export_schemas::schemas`] and
//! does not feed the Pydantic / TypeScript / fixture codegen cascade. Its own
//! drift is guarded by `codegen-drift.yml` diffing the committed artifact.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use schemars::schema_for;
use serde_json::{Map, Value, json};

use super::export_schemas::schemas;
use crate::api::JobRequest;

/// The embedded OpenAPI 3.1 meta-schema (OAS `2022-10-07` revision), used to
/// validate the generated document offline. It is a self-contained JSON Schema
/// 2020-12 document (its only external reference is the standard 2020-12
/// `$schema`, which `boon` bundles), so validation needs no network access.
const OAS_31_METASCHEMA: &str = include_str!("oas-3.1-schema.json");

/// Resource id under which the meta-schema is registered with `boon`.
const OAS_31_METASCHEMA_ID: &str = "https://spec.openapis.org/oas/3.1/schema/2022-10-07";

/// The `/api/v1` **contract** version, deliberately independent of the engine
/// release version. The document describes the shape of the `v1` surface;
/// embedders read the live engine version at runtime from `GET /api/v1/meta`.
/// Keeping this stable means a routine engine version bump does not churn the
/// committed artifact and trip the drift gate.
const API_VERSION: &str = "1.0.0";

/// Generate the OpenAPI 3.1 document, validate it, and write it to `output_path`.
///
/// # Errors
///
/// Returns an error when a component-name collision is detected (two
/// structurally different types sharing a schema name), when the assembled
/// document fails OpenAPI 3.1 meta-schema validation, when any `$ref` dangles,
/// or when the file cannot be written.
pub fn export_openapi(output_path: &Path) -> Result<()> {
    let doc = build_document()?;
    validate_document(&doc).context("validating the generated OpenAPI document")?;

    if let Some(parent) = output_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let pretty = serde_json::to_string_pretty(&doc).context("serializing OpenAPI document")?;
    std::fs::write(output_path, pretty + "\n")
        .with_context(|| format!("writing {}", output_path.display()))?;
    println!("wrote {}", output_path.display());
    Ok(())
}

/// Assemble the full OpenAPI 3.1 document (deterministically).
fn build_document() -> Result<Value> {
    let components = build_components()?;
    let paths = build_paths();

    Ok(json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Rocky Engine API",
            "version": API_VERSION,
            "description":
                "HTTP surface exposed by `rocky serve` under `/api/v1`. The \
                 canonical read routes return the same typed payloads as the \
                 corresponding `rocky <verb> --output json` command, byte for \
                 byte. Mutating work (`run`, `plan`, `apply`) is submitted as a \
                 job and polled. This document describes the `v1` contract \
                 shape; read the live engine version, schema-set hash, and \
                 available routes at runtime from `GET /api/v1/meta`.",
            "license": { "name": "Apache-2.0", "identifier": "Apache-2.0" }
        },
        "externalDocs": {
            "description": "Embedding Rocky guide",
            "url": "https://rocky-data.dev/guides/embedding/"
        },
        "servers": [
            {
                "url": "http://127.0.0.1:8080",
                "description":
                    "Default `rocky serve` bind (loopback only). A non-loopback \
                     host requires a bearer token."
            }
        ],
        "security": [ { "bearerAuth": [] } ],
        "paths": paths,
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "description":
                        "Optional shared-secret bearer token. Required on every \
                         route except `GET /api/v1/health` when the server is \
                         started with a token (mandatory for a non-loopback bind)."
                }
            },
            "schemas": Value::Object(components.into_iter().collect())
        }
    }))
}

// --- components/schemas assembly ---

/// Hoist every registered schema's root and `definitions` into one shared,
/// deduplicated `components/schemas` map, then rewrite all `#/definitions/…`
/// pointers to `#/components/schemas/…`.
///
/// Uses a [`BTreeMap`] throughout so the output is byte-stable across runs
/// (the drift gate depends on it).
fn build_components() -> Result<BTreeMap<String, Value>> {
    let mut components: BTreeMap<String, Value> = BTreeMap::new();

    // The registered `--output json` payload schemas.
    for (export_name, schema) in schemas() {
        hoist_schema(export_name, schema, &mut components)?;
    }

    // The job-submission request body. `JobRequest` derives `JsonSchema` but is
    // intentionally not in the export-schemas registry (it is a request input,
    // not a `--output json` payload), so it is added here directly.
    let job_request = serde_json::to_value(schema_for!(JobRequest))
        .context("serializing the JobRequest schema")?;
    hoist_schema("job_request", job_request, &mut components)?;

    // Draft-07 → OpenAPI 3.1 (JSON Schema 2020-12) bridge: relocate refs, strip
    // the Draft-07 `$schema` marker, and convert Draft-07 tuple validation
    // (`items: [schema, …]`) to 2020-12 `prefixItems`. Type arrays such as
    // `["string", "null"]`, `allOf`/`anyOf`/`oneOf`, `format`, and numeric
    // bounds are all already valid 2020-12, so they pass through unchanged.
    for schema in components.values_mut() {
        bridge_dialect(schema);
    }

    Ok(components)
}

/// Split one standalone schema document into its named components and merge them
/// into `components`, deduplicating on the way.
///
/// The root type becomes a component keyed by its `title` (the Rust struct
/// name); every entry under `definitions` becomes a component keyed by its
/// definition name.
fn hoist_schema(
    export_name: &str,
    mut schema: Value,
    components: &mut BTreeMap<String, Value>,
) -> Result<()> {
    let obj = schema
        .as_object_mut()
        .with_context(|| format!("schema `{export_name}` is not a JSON object"))?;

    // Pull out the nested definitions before we treat the remainder as the root.
    if let Some(Value::Object(defs)) = obj.remove("definitions") {
        for (name, def) in defs {
            insert_component(components, &name, def)
                .with_context(|| format!("hoisting definition `{name}` from `{export_name}`"))?;
        }
    }

    let title = obj
        .get("title")
        .and_then(Value::as_str)
        .with_context(|| format!("schema `{export_name}` has no `title` to key its component"))?
        .to_string();
    insert_component(components, &title, schema)
        .with_context(|| format!("hoisting root of `{export_name}`"))?;
    Ok(())
}

/// Insert one named component, deduplicating against any prior insertion.
///
/// The same Rust type appears both as a titled root (in its own schema file)
/// and as an untitled `definitions` entry (embedded in another file); to make
/// those compare equal, the top-level `title` and Draft-07 `$schema` markers
/// are stripped before storing. A genuine collision — the same name mapping to
/// two structurally different schemas — is a hard error rather than a silent
/// overwrite.
fn insert_component(
    components: &mut BTreeMap<String, Value>,
    name: &str,
    mut schema: Value,
) -> Result<()> {
    if let Some(obj) = schema.as_object_mut() {
        obj.remove("title");
        obj.remove("$schema");
    }
    match components.get(name) {
        None => {
            components.insert(name.to_string(), schema);
            Ok(())
        }
        Some(existing) if *existing == schema => Ok(()),
        Some(_) => bail!(
            "component name collision for `{name}`: two structurally different \
             schemas map to the same component name. Rename one of the Rust \
             types so their schemars short names differ."
        ),
    }
}

/// Recursively bridge a Draft-07 schema fragment to the JSON Schema 2020-12
/// dialect that OpenAPI 3.1 requires:
///
/// - rewrite `#/definitions/X` refs to `#/components/schemas/X`;
/// - drop the Draft-07 `$schema` marker;
/// - convert tuple validation, where `items` is an array of positional
///   schemas, to `prefixItems` (in 2020-12 `items` must be a single schema).
fn bridge_dialect(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.remove("$schema");

            if let Some(Value::String(reference)) = map.get("$ref")
                && let Some(rest) = reference.strip_prefix("#/definitions/")
            {
                let rewritten = format!("#/components/schemas/{rest}");
                map.insert("$ref".to_string(), Value::String(rewritten));
            }

            // Draft-07 tuple form `items: [s0, s1, …]` → 2020-12 `prefixItems`.
            // The source schemas pin tuple length with min/maxItems, so no
            // trailing `items: false` is needed to preserve semantics.
            if matches!(map.get("items"), Some(Value::Array(_)))
                && let Some(tuple) = map.remove("items")
            {
                map.insert("prefixItems".to_string(), tuple);
            }

            for child in map.values_mut() {
                bridge_dialect(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                bridge_dialect(item);
            }
        }
        _ => {}
    }
}

// --- paths assembly (mirrors crate::api::router) ---

/// Which response body a route returns.
enum Body {
    /// `$ref` to a component in `components/schemas`.
    Component(&'static str),
    /// The `202 Accepted` job-submission body: `{ "job_id": string }`.
    JobAccepted,
    /// An ad-hoc object explicitly outside the `/api/v1` value contract.
    OutOfContract,
}

/// One HTTP response for a route.
struct Resp {
    status: &'static str,
    description: &'static str,
    body: Body,
}

/// One HTTP operation, mirrored from the axum router in [`crate::api`].
struct Route {
    method: &'static str,
    path: &'static str,
    operation_id: &'static str,
    tag: &'static str,
    summary: &'static str,
    description: &'static str,
    path_params: &'static [&'static str],
    /// Advisory request headers (optional), e.g. `X-Rocky-Principal`.
    header_params: &'static [&'static str],
    /// Request-body component name, if the route accepts a body.
    request_body: Option<&'static str>,
    responses: &'static [Resp],
    /// The one auth-exempt route (`GET /api/v1/health`) sets `security: []`.
    auth_exempt: bool,
}

/// The `/api/v1` route table. Order is irrelevant to the output (paths are
/// keyed into a [`BTreeMap`]); a test pins its method+path set to
/// [`crate::api::api_v1_routes`].
fn route_table() -> Vec<Route> {
    // Shared response fragments.
    const ENGINE_BUSY_OR_NOT_READY: Resp = Resp {
        status: "503",
        description: "The engine has no compile result yet, or the state store is locked \
             by a running job (retryable).",
        body: Body::Component("ErrorEnvelope"),
    };
    const MODEL_NOT_FOUND: Resp = Resp {
        status: "404",
        description: "The named model is not in the compiled graph.",
        body: Body::Component("ErrorEnvelope"),
    };
    const JOB_ACCEPTED: Resp = Resp {
        status: "202",
        description: "Job accepted. Poll `GET /api/v1/jobs/{id}` for status.",
        body: Body::JobAccepted,
    };
    const BAD_REQUEST: Resp = Resp {
        status: "400",
        description: "The request body or a header could not be parsed.",
        body: Body::Component("ErrorEnvelope"),
    };
    const MUTATION_IN_PROGRESS: Resp = Resp {
        status: "409",
        description: "Another run/apply job already holds the single-mutating-job permit. \
             The holder's id is carried in `running_job_id`.",
        body: Body::Component("ErrorEnvelope"),
    };

    vec![
        Route {
            method: "get",
            path: "/api/v1/health",
            operation_id: "getHealth",
            tag: "meta",
            summary: "Liveness probe",
            description: "Auth-exempt liveness probe. Server-lifecycle, outside the \
                 `/api/v1` value contract.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[Resp {
                status: "200",
                description: "The server is alive.",
                body: Body::OutOfContract,
            }],
            auth_exempt: true,
        },
        Route {
            method: "get",
            path: "/api/v1/meta",
            operation_id: "getMeta",
            tag: "meta",
            summary: "Engine and config fingerprint",
            description: "Feature-detection endpoint. Every field is computed per request \
                 (engine version, state-schema version, schema-set hash, config \
                 hash, capabilities, routes).",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[Resp {
                status: "200",
                description: "The engine and config fingerprint.",
                body: Body::Component("MetaOutput"),
            }],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/models",
            operation_id: "listModels",
            tag: "models",
            summary: "List models (dashboard)",
            description: "Dashboard-only model list. Outside the `/api/v1` value contract \
                 (no canonical CLI counterpart).",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[Resp {
                status: "200",
                description: "The model list.",
                body: Body::OutOfContract,
            }],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/models/{name}",
            operation_id: "getModel",
            tag: "models",
            summary: "Model detail (dashboard)",
            description: "Dashboard-only model detail. Outside the `/api/v1` value contract.",
            path_params: &["name"],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The model detail.",
                    body: Body::OutOfContract,
                },
                MODEL_NOT_FOUND,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/models/{name}/lineage",
            operation_id: "getModelLineage",
            tag: "models",
            summary: "Column-level lineage",
            description: "Canonical lineage for a model. Byte-identical to \
                 `rocky lineage <name> --output json`.",
            path_params: &["name"],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The model's column-level lineage.",
                    body: Body::Component("LineageOutput"),
                },
                MODEL_NOT_FOUND,
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/models/{name}/lineage/{column}",
            operation_id: "traceColumnLineage",
            tag: "models",
            summary: "Trace a single column",
            description: "Canonical upstream trace for one column. Byte-identical to \
                 `rocky lineage <name> --column <column> --output json`.",
            path_params: &["name", "column"],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The column's upstream lineage trace.",
                    body: Body::Component("ColumnLineageOutput"),
                },
                MODEL_NOT_FOUND,
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/models/{name}/history",
            operation_id: "getModelHistory",
            tag: "history",
            summary: "Per-model run history",
            description: "Canonical per-model run history. Byte-identical to \
                 `rocky history --model <name> --output json`.",
            path_params: &["name"],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The model's run history.",
                    body: Body::Component("ModelHistoryOutput"),
                },
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/models/{name}/metrics",
            operation_id: "getModelMetrics",
            tag: "history",
            summary: "Per-model quality snapshots",
            description: "Canonical per-model quality snapshots. Byte-identical to \
                 `rocky metrics <name> --output json`.",
            path_params: &["name"],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The model's quality snapshots.",
                    body: Body::Component("MetricsOutput"),
                },
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/runs",
            operation_id: "listRuns",
            tag: "history",
            summary: "Project run history",
            description: "Canonical project run history. Byte-identical to \
                 `rocky history --output json`.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The project run history.",
                    body: Body::Component("HistoryOutput"),
                },
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/compile",
            operation_id: "getCompile",
            tag: "compile",
            summary: "Full compile result",
            description: "Canonical compile result. Byte-identical to \
                 `rocky compile --output json`, except the wall-clock \
                 `compile_timings`, which is inherently non-deterministic.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The full compile result.",
                    body: Body::Component("CompileOutput"),
                },
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "post",
            path: "/api/v1/compile",
            operation_id: "triggerCompile",
            tag: "compile",
            summary: "Trigger recompilation",
            description: "Server-lifecycle recompile trigger. Outside the `/api/v1` value \
                 contract.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[Resp {
                status: "200",
                description: "Recompilation was triggered.",
                body: Body::OutOfContract,
            }],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/dag",
            operation_id: "getDag",
            tag: "dag",
            summary: "Full unified DAG",
            description: "Canonical unified DAG. Byte-identical to `rocky dag --output json`.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The full unified DAG.",
                    body: Body::Component("DagOutput"),
                },
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/dag/layers",
            operation_id: "getDagLayers",
            tag: "dag",
            summary: "Execution layers (dashboard)",
            description: "Dashboard-only execution layers. Outside the `/api/v1` value \
                 contract.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[Resp {
                status: "200",
                description: "The execution layers.",
                body: Body::OutOfContract,
            }],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/dag/status",
            operation_id: "getDagStatus",
            tag: "dag",
            summary: "Latest DAG run status (server)",
            description: "Latest recorded DAG execution status. Server-only, outside the \
                 `/api/v1` value contract.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The latest DAG run status.",
                    body: Body::OutOfContract,
                },
                Resp {
                    status: "503",
                    description: "No DAG run has been recorded yet.",
                    body: Body::Component("ErrorEnvelope"),
                },
            ],
            auth_exempt: false,
        },
        Route {
            method: "post",
            path: "/api/v1/jobs/run",
            operation_id: "submitRunJob",
            tag: "jobs",
            summary: "Submit a run job",
            description: "Submit a mutating `run` job. Takes the single-mutating-job \
                 permit; a second run/apply while one is held returns 409.",
            path_params: &[],
            header_params: &["X-Rocky-Principal"],
            request_body: Some("JobRequest"),
            responses: &[JOB_ACCEPTED, BAD_REQUEST, MUTATION_IN_PROGRESS],
            auth_exempt: false,
        },
        Route {
            method: "post",
            path: "/api/v1/jobs/plan",
            operation_id: "submitPlanJob",
            tag: "jobs",
            summary: "Submit a plan job",
            description: "Submit a non-mutating `plan` job. Does not take the mutating \
                 permit, so it is never blocked by a running run/apply.",
            path_params: &[],
            header_params: &["X-Rocky-Principal"],
            request_body: Some("JobRequest"),
            responses: &[JOB_ACCEPTED, BAD_REQUEST],
            auth_exempt: false,
        },
        Route {
            method: "post",
            path: "/api/v1/jobs/apply",
            operation_id: "submitApplyJob",
            tag: "jobs",
            summary: "Submit an apply job",
            description: "Submit a mutating `apply` job. Takes the single-mutating-job \
                 permit; a second run/apply while one is held returns 409.",
            path_params: &[],
            header_params: &["X-Rocky-Principal"],
            request_body: Some("JobRequest"),
            responses: &[JOB_ACCEPTED, BAD_REQUEST, MUTATION_IN_PROGRESS],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/jobs/{id}",
            operation_id: "getJob",
            tag: "jobs",
            summary: "Job status",
            description: "Job status, with the embedded canonical result once terminal. \
                 Falls back to the durable job table after a sidecar restart.",
            path_params: &["id"],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The job status (with embedded result when done).",
                    body: Body::Component("JobStatus"),
                },
                Resp {
                    status: "404",
                    description: "No job with this id (neither in-memory nor persisted).",
                    body: Body::Component("ErrorEnvelope"),
                },
            ],
            auth_exempt: false,
        },
        Route {
            method: "get",
            path: "/api/v1/schedule",
            operation_id: "getScheduleStatus",
            tag: "schedule",
            summary: "Scheduler status",
            description: "A read-only scheduler snapshot: what is scheduled, each pipeline's \
                 cursors, throttle and in-flight claims, the projected next fire, and the \
                 tick-lock state. Reports stored state rather than evaluating demand, so it \
                 is side-effect free. A `next_fire_at` in the past means overdue, and a \
                 `tick_lock.state` of `free` is the normal state between ticks.",
            path_params: &[],
            header_params: &[],
            request_body: None,
            responses: &[
                Resp {
                    status: "200",
                    description: "The scheduler status snapshot.",
                    body: Body::Component("ScheduleStatusOutput"),
                },
                Resp {
                    status: "500",
                    description: "The bound `rocky.toml` could not be read or parsed.",
                    body: Body::Component("ErrorEnvelope"),
                },
                ENGINE_BUSY_OR_NOT_READY,
            ],
            auth_exempt: false,
        },
    ]
}

/// Build the `paths` object from the route table, grouping operations that
/// share a path (e.g. `GET`/`POST /api/v1/compile`) into one path item.
fn build_paths() -> Value {
    let mut paths: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    for route in route_table() {
        let item = paths.entry(route.path.to_string()).or_default();
        item.insert(route.method.to_string(), operation_object(&route));
    }
    let obj: Map<String, Value> = paths
        .into_iter()
        .map(|(path, item)| (path, Value::Object(item)))
        .collect();
    Value::Object(obj)
}

/// Serialize one [`Route`] into its OpenAPI operation object.
fn operation_object(route: &Route) -> Value {
    let mut op = Map::new();
    op.insert("operationId".to_string(), json!(route.operation_id));
    op.insert("tags".to_string(), json!([route.tag]));
    op.insert("summary".to_string(), json!(route.summary));
    op.insert("description".to_string(), json!(route.description));

    let mut params: Vec<Value> = Vec::new();
    for name in route.path_params {
        params.push(json!({
            "name": name,
            "in": "path",
            "required": true,
            "schema": { "type": "string" },
            "description": format!("Path parameter `{name}`.")
        }));
    }
    for name in route.header_params {
        params.push(json!({
            "name": name,
            "in": "header",
            "required": false,
            "schema": { "type": "string" },
            "description":
                "Advisory principal recorded for audit only. Spoofable under \
                 shared-secret auth; never an authorization input."
        }));
    }
    if !params.is_empty() {
        op.insert("parameters".to_string(), Value::Array(params));
    }

    if let Some(component) = route.request_body {
        op.insert(
            "requestBody".to_string(),
            json!({
                "required": false,
                "description":
                    "Optional job parameters. An empty body runs the verb with \
                     its defaults.",
                "content": {
                    "application/json": {
                        "schema": { "$ref": format!("#/components/schemas/{component}") }
                    }
                }
            }),
        );
    }

    let mut responses = Map::new();
    for resp in route.responses {
        responses.insert(resp.status.to_string(), response_object(resp));
    }
    op.insert("responses".to_string(), Value::Object(responses));

    if route.auth_exempt {
        op.insert("security".to_string(), json!([]));
    }

    Value::Object(op)
}

/// Serialize one [`Resp`] into an OpenAPI response object.
fn response_object(resp: &Resp) -> Value {
    let schema = match &resp.body {
        Body::Component(name) => json!({ "$ref": format!("#/components/schemas/{name}") }),
        Body::JobAccepted => json!({
            "type": "object",
            "properties": { "job_id": { "type": "string" } },
            "required": ["job_id"]
        }),
        Body::OutOfContract => json!({
            "type": "object",
            "additionalProperties": true,
            "description":
                "Ad-hoc body outside the `/api/v1` value contract; shape is not \
                 pinned and may change without a version bump."
        }),
    };
    json!({
        "description": resp.description,
        "content": { "application/json": { "schema": schema } }
    })
}

// --- validation ---

/// Validate the generated document against the embedded OpenAPI 3.1 meta-schema
/// (structure) and confirm every internal `$ref` resolves (wiring).
///
/// # Errors
///
/// Returns an error if the meta-schema rejects the document, or if any `$ref`
/// points at a component that is not present (including a Draft-07
/// `#/definitions/…` ref that escaped rewriting).
fn validate_document(doc: &Value) -> Result<()> {
    // 1. Structural validation against the OpenAPI 3.1 meta-schema.
    let metaschema: Value = serde_json::from_str(OAS_31_METASCHEMA)
        .context("parsing embedded OpenAPI 3.1 meta-schema")?;
    // boon's `CompileError` is not `Send + Sync + StdError`, so it cannot flow
    // through anyhow's `.context()`; stringify it at the boundary instead.
    let mut schemas = boon::Schemas::new();
    let mut compiler = boon::Compiler::new();
    compiler
        .add_resource(OAS_31_METASCHEMA_ID, metaschema)
        .map_err(|e| anyhow::anyhow!("registering the OpenAPI 3.1 meta-schema: {e}"))?;
    let sch = compiler
        .compile(OAS_31_METASCHEMA_ID, &mut schemas)
        .map_err(|e| anyhow::anyhow!("compiling the OpenAPI 3.1 meta-schema: {e}"))?;
    if let Err(err) = schemas.validate(doc, sch) {
        bail!("document failed OpenAPI 3.1 meta-schema validation:\n{err}");
    }

    let components = doc
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .context("document has no components/schemas object")?;

    // 2. Dialect: the OpenAPI 3.1 meta-schema treats each schema object as
    //    opaque (its `$dynamicRef` plug point admits any subschema), so it does
    //    NOT catch a Draft-07 construct that is invalid in JSON Schema 2020-12
    //    (e.g. tuple `items: [...]`). Validate every component against the
    //    bundled 2020-12 meta-schema so the dialect bridge is actually enforced
    //    in CI, with no node dependency.
    const METASCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
    let mut dialect_schemas = boon::Schemas::new();
    let mut dialect_compiler = boon::Compiler::new();
    let dialect_sch = dialect_compiler
        .compile(METASCHEMA_2020_12, &mut dialect_schemas)
        .map_err(|e| anyhow::anyhow!("compiling the JSON Schema 2020-12 meta-schema: {e}"))?;
    let mut invalid = Vec::new();
    for (name, component) in components {
        if dialect_schemas.validate(component, dialect_sch).is_err() {
            invalid.push(name.clone());
        }
    }
    if !invalid.is_empty() {
        invalid.sort();
        bail!(
            "{} component schema(s) are not valid JSON Schema 2020-12 (the \
             OpenAPI 3.1 dialect): {}. The Draft-07 → 2020-12 bridge is \
             incomplete.",
            invalid.len(),
            invalid.join(", ")
        );
    }

    // 3. Ref-resolution: the meta-schemas check structure, not that refs
    //    resolve. This is the real proof the hoist + rewrite wired every type.
    let mut dangling = Vec::new();
    collect_dangling_refs(doc, components, &mut dangling);
    if !dangling.is_empty() {
        dangling.sort();
        dangling.dedup();
        bail!(
            "document has {} unresolved $ref(s): {}",
            dangling.len(),
            dangling.join(", ")
        );
    }
    Ok(())
}

/// Walk the document collecting any `$ref` that does not resolve to a known
/// component (and any Draft-07 `#/definitions/…` ref that survived rewriting).
fn collect_dangling_refs(value: &Value, components: &Map<String, Value>, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get("$ref") {
                if let Some(name) = reference.strip_prefix("#/components/schemas/") {
                    if !components.contains_key(name) {
                        out.push(reference.clone());
                    }
                } else if reference.starts_with("#/definitions/") {
                    out.push(reference.clone());
                }
            }
            for child in map.values() {
                collect_dangling_refs(child, components, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_dangling_refs(item, components, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::api_v1_routes;
    use std::collections::HashSet;

    /// The generated document validates against the OpenAPI 3.1 meta-schema and
    /// has no dangling refs — this is the production code path run end to end.
    #[test]
    fn document_is_valid_openapi_31() {
        let doc = build_document().expect("build");
        validate_document(&doc).expect("generated document must be valid OpenAPI 3.1");
        assert_eq!(doc["openapi"], "3.1.0");
    }

    /// Every registered payload type is reachable as a component, and the
    /// contract types the paths reference are present.
    #[test]
    fn components_cover_registered_and_referenced_types() {
        let components = build_components().expect("components");
        // Sanity: the shared embedded type deduped to exactly one entry.
        assert!(components.contains_key("AdapterConfig"));
        // The response/request component names the route table references.
        for name in [
            "MetaOutput",
            "LineageOutput",
            "ColumnLineageOutput",
            "ModelHistoryOutput",
            "MetricsOutput",
            "HistoryOutput",
            "CompileOutput",
            "DagOutput",
            "JobStatus",
            "ErrorEnvelope",
            "JobRequest",
        ] {
            assert!(components.contains_key(name), "missing component `{name}`");
        }
        // No Draft-07 markers survived into any component.
        for (name, schema) in &components {
            assert!(
                !schema.to_string().contains("#/definitions/"),
                "component `{name}` still carries a Draft-07 #/definitions/ ref"
            );
            assert!(
                !schema.to_string().contains("\"$schema\""),
                "component `{name}` still carries a $schema marker"
            );
            assert!(
                !has_array_items(schema),
                "component `{name}` still carries a Draft-07 tuple `items: [..]` \
                 (must be bridged to `prefixItems`)"
            );
        }
        // The one known tuple (`doctor` HealthCheck.details) must have bridged.
        assert!(
            components
                .values()
                .any(|s| s.to_string().contains("prefixItems")),
            "expected at least one tuple to be bridged to `prefixItems`"
        );
    }

    /// True if any object in the tree has an array-valued `items` (the invalid
    /// Draft-07 tuple form that must have become `prefixItems`).
    fn has_array_items(value: &Value) -> bool {
        match value {
            Value::Object(map) => {
                if matches!(map.get("items"), Some(Value::Array(_))) {
                    return true;
                }
                map.values().any(has_array_items)
            }
            Value::Array(items) => items.iter().any(has_array_items),
            _ => false,
        }
    }

    /// The generated output is byte-stable across runs — the drift gate relies
    /// on this.
    #[test]
    fn generation_is_deterministic() {
        let a = serde_json::to_string_pretty(&build_document().unwrap()).unwrap();
        let b = serde_json::to_string_pretty(&build_document().unwrap()).unwrap();
        assert_eq!(a, b, "OpenAPI generation must be deterministic");
    }

    /// The `paths` table covers exactly the routes the live axum router serves.
    /// This is the anti-drift guard: adding a route to `api.rs` (and to
    /// `api_v1_routes`) fails here until it is added to the route table.
    #[test]
    fn paths_match_live_router_routes() {
        let from_table: HashSet<String> = route_table()
            .into_iter()
            .map(|r| format!("{} {}", r.method.to_uppercase(), r.path))
            .collect();
        // `api_v1_routes` uses the same `{param}` brace syntax as our table.
        let from_router: HashSet<String> = api_v1_routes().into_iter().collect();
        assert_eq!(
            from_table,
            from_router,
            "OpenAPI paths table drifted from crate::api::router.\n  \
             only in table: {:?}\n  only in router: {:?}",
            from_table.difference(&from_router).collect::<Vec<_>>(),
            from_router.difference(&from_table).collect::<Vec<_>>(),
        );
    }

    /// The auth-exempt health route carries `security: []`; a contract route
    /// inherits the global bearer requirement (no per-op override).
    #[test]
    fn health_is_auth_exempt_meta_is_not() {
        let doc = build_document().unwrap();
        let health = &doc["paths"]["/api/v1/health"]["get"];
        assert_eq!(health["security"], json!([]));
        let meta = &doc["paths"]["/api/v1/meta"]["get"];
        assert!(meta.get("security").is_none());
    }
}
