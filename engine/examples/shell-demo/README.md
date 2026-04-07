# Shell Demo

Demonstrates `rocky shell` -- an interactive SQL REPL that connects to the configured warehouse adapter. No models are needed; the shell connects directly to the database.

## Running

```bash
rocky --config engine/examples/shell-demo/rocky.toml shell
```

You'll see a prompt like:

```
rocky> 
```

## Example Session

```sql
rocky> SELECT 1 + 1 AS result;
+--------+
| result |
+--------+
| 2      |
+--------+

rocky> CREATE TABLE demo (id INTEGER, name VARCHAR, value DOUBLE);

rocky> INSERT INTO demo VALUES (1, 'alpha', 10.5), (2, 'beta', 20.3), (3, 'gamma', 30.1);

rocky> SELECT name, value FROM demo WHERE value > 15 ORDER BY value DESC;
+-------+-------+
| name  | value |
+-------+-------+
| gamma | 30.1  |
| beta  | 20.3  |
+-------+-------+

rocky> SELECT COUNT(*) AS total, AVG(value) AS avg_value FROM demo;
+-------+-----------+
| total | avg_value |
+-------+-----------+
| 3     | 20.3      |
+-------+-----------+
```

## Meta-Commands

The shell supports special dot-prefixed commands:

| Command | Description |
|---------|-------------|
| `.tables` | List all tables in the current database |
| `.schema <table>` | Show the column definitions for a table |
| `.quit` | Exit the shell (also: `Ctrl+D`) |

### Example

```
rocky> .tables
+--------+
| name   |
+--------+
| demo   |
+--------+

rocky> .schema demo
+---------+---------+
| column  | type    |
+---------+---------+
| id      | INTEGER |
| name    | VARCHAR |
| value   | DOUBLE  |
+---------+---------+

rocky> .quit
```

## Multi-Line Queries

The shell supports multi-line SQL. A statement is executed when it ends with a semicolon:

```sql
rocky> SELECT
   ...>   name,
   ...>   value,
   ...>   value / (SELECT SUM(value) FROM demo) AS pct_of_total
   ...> FROM demo
   ...> ORDER BY value DESC;
```

The continuation prompt (`...>`) indicates the shell is waiting for more input.

## Use Cases

- **Ad-hoc exploration** -- query source or target tables during development
- **Debugging** -- inspect table contents after a `rocky run` to verify results
- **Quick prototyping** -- test SQL snippets before adding them to a model
- **Schema inspection** -- use `.tables` and `.schema` to understand the database layout

## Adapter Compatibility

The shell works with any configured adapter. When connected to DuckDB (as in this demo), queries execute locally with no external credentials. When connected to Databricks or Snowflake, queries execute against the remote warehouse.
