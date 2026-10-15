# rocky.toml reference: more warehouse adapters

This file covers the PostgreSQL / Redshift, ClickHouse and SQL Server adapter blocks and their per-adapter rules.

## PostgreSQL / Redshift (password auth, libpq-style `sslmode`)

```toml
[adapter]
type     = "postgres"                            # or "redshift" (beta)
host     = "${PGHOST}"                           # host or host:port (default 5432 / 5439)
database = "analytics"                           # the connected database = the only valid catalog
username = "${PGUSER}"
password = "${PGPASSWORD}"
# timeout_secs = 300                             # connect timeout + statement_timeout

[adapter.extra]                                  # unknown keys are refused
sslmode = "verify-full"                          # disable | prefer (default) | require | verify-full
# sslrootcert = "/etc/ssl/rds-ca.pem"            # extra PEM roots under verify-full
# port = 6543                                    # wins over host:port
# max_connections = 8
# merge_mode = "on_conflict"                     # postgres < 15 only; needs a unique index on unique_key
# late_binding_views = true                      # redshift only: views WITH NO SCHEMA BINDING
```

Redshift models can set table attributes in the sidecar `[redshift]` block (`dist_style`, `dist_key`, `sort_key`, `sort_style`); `rocky compile` validates it (E052 / W052) and any other adapter refuses it at SQL generation. IAM auth for Redshift is not built in yet.

## ClickHouse (beta; HTTP interface, user/password, TLS)

```toml
[adapter]
type     = "clickhouse"
host     = "${CLICKHOUSE_HOST}"                  # host or host:port (default 8123, 8443 with secure)
username = "${CLICKHOUSE_USER}"                  # default "default"
password = "${CLICKHOUSE_PASSWORD}"
# database = "default"                           # session default database for unqualified names
# timeout_secs = 300                             # per statement; also max_execution_time

[adapter.extra]                                  # unknown keys are refused
secure = true                                    # HTTPS, certificate always verified
# ca_cert = "/etc/ssl/ch-ca.pem"                 # extra PEM roots (needs secure = true)
# port = 9443                                    # wins over host:port
```

A Rocky schema is a ClickHouse database: set every model's `catalog = ""` (a non-empty catalog is refused). Models can set `[clickhouse]` (`engine` = a parameterless MergeTree-family name, `order_by` = columns, `partition_by` = a column or `fn(column)`); `rocky compile` validates it (E053 / W053). `merge` and `incremental` with `unique_key` are refused (E053, no `MERGE`), as are snapshots (E049), UDFs (E051) and `materialized_view`.

## SQL Server / Azure SQL / Fabric Warehouse (beta)

```toml
[adapter]
type     = "sqlserver"
host     = "${MSSQL_HOST}"                       # host, host,port or host:port (default 1433)
database = "analytics"                           # the connected database = the only valid catalog
# exactly ONE auth method:
username = "${MSSQL_USER}"                       # SQL auth (not on Fabric)
password = "${MSSQL_PASSWORD}"
# oauth_token = "${MSSQL_ACCESS_TOKEN}"          # Entra ID access token (not refreshed)
# client_id = "${AZURE_CLIENT_ID}"               # Entra ID service principal, with extra.tenant_id
# client_secret = "${AZURE_CLIENT_SECRET}"

[adapter.extra]                                  # unknown keys are refused
# encrypt = "mandatory"                          # mandatory (default) | strict (TDS 8.0) | optional
# trust_server_certificate = true                # local/test servers only
# ca_cert = "/etc/ssl/corp-ca.pem"
# flavor = "fabric"                              # Fabric Warehouse renderings
# tenant_id = "${AZURE_TENANT_ID}"
# port = 1433
# max_connections = 8
```

Snapshots, `materialized_view`, UDFs (E051) and `regex_match` checks are refused on SQL Server.
