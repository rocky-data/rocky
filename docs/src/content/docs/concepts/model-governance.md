---
title: Model governance
description: Control who may read a model with access levels and ownership groups, and change a model safely with versions.
sidebar:
  order: 10
---

Model governance answers two questions about a shared model: who may read it, and how do you change its shape without breaking the models that read it? **Access levels** and **ownership groups** answer the first. **Model versions** answer the second. `rocky compile` checks both, and a broken rule fails the compile before any warehouse call. The features match dbt's [model access](https://docs.getdbt.com/docs/mesh/govern/model-access) and [model versions](https://docs.getdbt.com/docs/mesh/govern/model-versions). A project that sets none of the keys on this page compiles as before.

## Access levels

An access level says how widely a model may be read. Set it with the
top-level `access` key in the model's sidecar:

```toml
# models/fin_base.toml
access = "private"
access_group = "finance"
```

| `access` | Who may reference the model |
|---|---|
| `private` | Only models in the same ownership group. |
| `protected` | Any model in this project. This is the default. |
| `public` | Any model in this project, and other projects through `rocky publish-ir`. |

A model with no `access` key is `protected`, as every model behaved before access levels existed.

### How Rocky checks a private model

Rocky checks every edge of the model graph. When a model reads a `private` model, the two must share an ownership group, or the compile fails with `E047` on the reader. A `private` model with no group also fails with `E047`: no other model could ever read it, so the setting is almost always a mistake.

```
fin_base   (private, group finance)
   │
   ├──▶ fin_mart     (group finance)     ok
   └──▶ mkt_report   (group marketing)   E047
```

## Ownership groups and owners

An ownership group names the team that owns a model. Set it with `access_group`. When absent, Rocky uses the model's config `group`. Rocky reads the ownership group only for `private` models, which did not exist before, so the fallback is safe for existing projects. A config group still controls routing and strategy as before (see [Folder-level config](/guides/migrate-from-dbt/#folder-level-config-materialized-schema)). `access_group` inherits no config: it only says who owns the model.

A group can name an accountable owner. Add an `[owner]` table to the group
file `models/groups/<group>.toml`:

```toml
# models/groups/finance.toml
[owner]
name = "Finance Data"
email = "finance-data@example.com"
```

The file needs no other keys. `rocky docs` shows the access level, group and
owner on each model's page. `rocky catalog` writes them to the `governance`
object of each asset in `catalog.json`.

## Model versions

A model version is a separate copy of a model with a new shape. Readers move
to the new version on their own schedule. The old version keeps working until
you remove it.

### Declare a versioned model

A versioned model has three parts:

- one **version declaration**: `<name>.toml` with a `[[versions]]` array, and
  no `<name>.sql` beside it;
- one ordinary model per version, named `<name>_v<N>`, with its own `.sql`
  file and sidecar;
- one **latest alias**: a view named `<name>` that Rocky adds for you.

```
models/
├── orders.toml        version declaration (no orders.sql)
├── orders_v1.sql      version 1
├── orders_v1.toml
├── orders_v2.sql      version 2, the latest
└── orders_v2.toml
```

```toml
# models/orders.toml
latest_version = 2
access = "public"        # default for every version without its own

[[versions]]
v = 1
deprecation_date = "2026-12-31"

[[versions]]
v = 2
```

| Key | Default | Meaning |
|---|---|---|
| `versions` | required | The declared versions. Each has `v` and an optional `deprecation_date` (`YYYY-MM-DD`). |
| `latest_version` | the highest `v` | The version that `<name>` reads. |
| `latest_alias` | `true` | Add the `<name>` view over the latest version. |
| `access`, `access_group` | none | Defaults for each version that does not set its own. |
| `drop_existing_kind` | none | Lets the alias view replace an existing table named `<name>` (see below). |

Each version is an ordinary model that materializes to `<name>_v<N>`. Its own sidecar sets its strategy and target. A contract applies per version, in `<name>_v<N>.contract.toml`. Version files must be `.sql` models in the same directory as the declaration.

### Reference a version

A reference picks a version by name. Plain SQL needs no new syntax:

| You write | You read |
|---|---|
| `FROM orders` | The latest version, through the `orders` view. |
| `FROM orders_v1` | Version 1, pinned. |
| `depends_on = ["orders@v1"]` | Version 1, pinned. `@v<N>` is a sidecar shorthand for `orders_v1`. |

The `orders` view is `SELECT * FROM orders_v2`. Raising `latest_version` moves every reader of `orders` to the new version on the next run. A reader of `orders_v1` stays on version 1.

When you turn an unversioned model into a versioned one, a table called `orders` may already exist. A run refuses to replace a table with a view unless the declaration sets `drop_existing_kind = "table"`. Like the sidecar key of the same name, it works on DuckDB today. On other warehouses, drop or rename the old table yourself. Set `latest_alias = false` when you do not want the view (for example, when the latest version already materializes to `orders`): a reference to bare `orders` then fails with `E048`.

### Deprecate a version

A `deprecation_date` tells readers when a version will go away. A model that
reads that version gets the warning `W048` from 30 days before the date. The
warning continues after the date. It never fails the compile.

Rocky compares the date with today's UTC date. To pin the clock, for a
reproducible CI run or a test, set `ROCKY_GOVERNANCE_TODAY=YYYY-MM-DD`.

### Version errors

`rocky compile` reports `E048` when:

- `latest_version` names a version that is not declared;
- a declared version has no `<name>_v<N>` model file (this includes a missing
  latest version);
- a model reads `<name>_v<N>`, and `N` is not a declared version;
- a model reads `<name>` while `latest_alias = false`.

## Governance across projects

`rocky publish-ir` writes a producer's snapshot for other projects to import;
see [Cross-team contracts](/concepts/cross-team-contracts/). The snapshot
holds only `public` models.

```
producer models                 snapshot                 consumer
orders   (public)     ───────▶  orders                ◀── reads orders       ok
margins  (protected)  ─ ✗ ───▶  withheld: margins     ◀── reads margins      E047
```

The snapshot also lists each withheld model's target and access level. When a
consumer's `[[sources]]` entry names a withheld target, the consumer's compile
fails with `E047`. The snapshot carries each exported model's version, so a
consumer that reads a deprecated version gets `W048`.

A project in which no model sets `access` predates access levels. For that
project, `rocky publish-ir` still exports every model, as before, and prints a
notice. When any model sets `access`, only `public` models are exported. A
project with `access` keys but no `public` model refuses to publish.

The governance metadata lives beside the IR in the snapshot file. It is not
part of the recipe hash, so it changes no existing `pin`.

## Diagnostics

| Code | Severity | Meaning |
|---|---|---|
| `E047` | error | A model reads a `private` model outside its ownership group; a `private` model has no group; or a consumer reads a producer model that is not `public`. |
| `E048` | error | A version problem: undeclared latest version, missing version file, reference to an undeclared version, or a bare reference with `latest_alias = false`. |
| `W048` | warning | A model reads a version whose `deprecation_date` has passed or is less than 30 days away. |

## Limits

- Version files must be `.sql` models. A `.rocky` version file is not stamped
  as a version.
- Rocky checks access on the model graph inside one project and on
  `[[sources]]` entries across projects. It does not control warehouse
  permissions; use grants for that.
