-- Initial customer master: 50 rows.
-- run.sh applies this once, then mutates the table between rocky runs to
-- exercise the MERGE upsert path (existing keys updated, new keys inserted).

CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.customers AS
SELECT
    CAST(i AS BIGINT) AS customer_id,
    'Customer ' || i AS name,
    'customer' || i || '@example.com' AS email,
    CASE WHEN i % 10 = 0 THEN 'gold' WHEN i % 5 = 0 THEN 'silver' ELSE 'bronze' END AS tier
FROM generate_series(1, 50) AS t(i);
