CREATE SCHEMA IF NOT EXISTS raw__crm;
CREATE SCHEMA IF NOT EXISTS snapshots;

CREATE OR REPLACE TABLE raw__crm.customers AS
SELECT * FROM (VALUES
    (1, 'Alice',   'alice@example.com',   'gold',   TIMESTAMP '2026-01-15 10:00:00'),
    (2, 'Bob',     'bob@example.com',     'silver', TIMESTAMP '2026-02-01 09:30:00'),
    (3, 'Charlie', 'charlie@example.com', 'bronze', TIMESTAMP '2026-03-10 14:00:00')
) AS t(customer_id, name, email, tier, updated_at);
