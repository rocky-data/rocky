-- SELECT * lint: this leaf model trips the always-on I001 (Info) lint,
-- "SELECT * used — consider explicit column list for stability", carrying a
-- span so an editor can flag the file. The stronger P002 blast-radius lint
-- (Warning) is a separate, semantic-graph-aware check: it fires only when a
-- SELECT * model has a downstream consumer that pins specific columns, so an
-- upstream schema change would silently propagate. This leaf has no such
-- consumer, so P002 correctly stays quiet — only I001 fires here.

SELECT *
FROM raw__orders.orders
