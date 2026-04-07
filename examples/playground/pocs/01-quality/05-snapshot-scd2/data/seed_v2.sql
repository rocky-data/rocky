-- Alice upgraded to platinum, Dave is new, Charlie removed (hard delete)
CREATE OR REPLACE TABLE raw__crm.customers AS
SELECT * FROM (VALUES
    (1, 'Alice',  'alice@example.com',    'platinum', TIMESTAMP '2026-04-01 08:00:00'),
    (2, 'Bob',    'bob@example.com',      'silver',   TIMESTAMP '2026-02-01 09:30:00'),
    (4, 'Dave',   'dave@example.com',     'bronze',   TIMESTAMP '2026-04-05 11:00:00')
) AS t(customer_id, name, email, tier, updated_at);
