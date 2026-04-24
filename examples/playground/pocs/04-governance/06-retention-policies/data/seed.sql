CREATE SCHEMA IF NOT EXISTS raw__events;
CREATE SCHEMA IF NOT EXISTS raw__ledger;

CREATE OR REPLACE TABLE raw__events.page_views AS
SELECT i AS event_id, 'user_' || i AS user_id FROM generate_series(1, 10) AS t(i);

CREATE OR REPLACE TABLE raw__ledger.entries AS
SELECT i AS entry_id, i * 100 AS amount_cents FROM generate_series(1, 10) AS t(i);
