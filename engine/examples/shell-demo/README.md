# Shell demo

This example shows `rocky shell`, an interactive SQL prompt. The shell sends
each statement straight to a warehouse adapter and prints the result. It does
not compile models, so this example needs no `models/` directory.

The shell runs whatever you type, including `INSERT`, `CREATE`, and `DROP`.

## Which adapter the shell connects to

Rocky picks the adapter in this order.

1. The pipeline named by `--pipeline`, using that pipeline's target adapter.
2. The only pipeline in the config, using its target adapter.
3. The only warehouse adapter in the config.

If several adapters are configured and none of the rules above resolve, Rocky
stops and asks for `--pipeline`.

## Start it

```bash
cd engine/examples/shell-demo
rocky shell
```

```
Connecting... ok

Rocky Shell (adapter: duckdb, name: local)
Type SQL to execute. Special commands:
  .tables              List tables in a schema (.tables catalog.schema)
  .schema <table>      Describe a table's columns
  .quit / .exit        Exit the shell
  Lines ending with \ continue on the next line.

rocky>
```

`rocky.toml` declares a DuckDB adapter with no `path`, so this session runs
against an in-memory database. Nothing you create survives the process.

## A session

Every transcript below is DuckDB output. Result shapes differ by warehouse.

```
rocky> SELECT 1 + 1 AS result;
 result
--------
 2
(1 rows)

rocky> CREATE TABLE demo (id INTEGER, name VARCHAR, value DOUBLE);
(0 rows)

rocky> INSERT INTO demo VALUES (1, 'alpha', 10.5), (2, 'beta', 20.3), (3, 'gamma', 30.1);
 Count
-------
 3
(1 rows)

rocky> SELECT name, value FROM demo WHERE value > 15 ORDER BY value DESC;
 name  | value
-------+-------
 gamma | 30.1
 beta  | 20.3
(2 rows)
```

Rocky strips a trailing semicolon before it sends the statement, so the
semicolon is optional.

## Dot-commands

| Command | What it runs |
|---------|--------------|
| `.tables` | `SHOW TABLES` |
| `.tables <catalog>.<schema>` | `SHOW TABLES IN <catalog>.<schema>` |
| `.schema <table>` | `DESCRIBE TABLE <table>` |
| `.quit` or `.exit` | Leaves the shell |

Each one is a shortcut for the SQL beside it, so the result columns come from
your warehouse:

```
rocky> .tables
 name
------
 demo
(1 rows)

rocky> .schema demo
 column_name | column_type | null | key  | default | extra
-------------+-------------+------+------+---------+-------
 id          | INTEGER     | YES  | NULL | NULL    | NULL
 name        | VARCHAR     | YES  | NULL | NULL    | NULL
 value       | DOUBLE      | YES  | NULL | NULL    | NULL
(3 rows)
```

Ctrl-D also exits. Both routes print `Bye.` and leave with status 0.

## Continue a statement across lines

End a line with a backslash. Only a backslash continues a statement. Leaving
off the semicolon does not.

```
rocky> SELECT name, \
   ... value \
   ... FROM demo ORDER BY value DESC;
 name  | value
-------+-------
 gamma | 30.1
 beta  | 20.3
 alpha | 10.5
(3 rows)
```

The `   ... ` prompt means Rocky is waiting for the rest of the statement.

## What the shell is useful for

- Look at a table while you are writing the model that reads it.
- Try a SQL snippet before you paste it into a `.sql` model.
- Use `.tables` and `.schema` to learn an unfamiliar database.

Point the config at a file-backed DuckDB or a remote warehouse when you want
the results of a `rocky run` to still be there.
