CREATE SCHEMA IF NOT EXISTS raw__users;
CREATE SCHEMA IF NOT EXISTS raw__accounts;

CREATE OR REPLACE TABLE raw__users.users AS
  SELECT
    i                          AS id,
    'user' || i || '@test.org' AS email,
    '555-01-' || lpad(i::TEXT, 2, '0') AS ssn,
    CASE WHEN i % 2 = 0 THEN 'us_west' ELSE 'eu_central' END AS region
  FROM generate_series(1, 10) AS t(i);

CREATE OR REPLACE TABLE raw__accounts.accounts AS
  SELECT
    i                                   AS id,
    'owner' || i || '@test.org'         AS owner_email,
    'ACCT-AUDIT-' || lpad(i::TEXT, 4, '0') AS audit_note
  FROM generate_series(1, 10) AS t(i);
