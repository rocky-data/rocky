-- Seed data for the run-variables POC.
--
-- run.sh loads this into the persistent poc.duckdb before running the
-- pipeline:  duckdb poc.duckdb < data/seed.sql
--
-- Three regions across two channels so a `--var region=us` run can be shown to
-- materialize ONLY the matching rows, and a `--var channel=mobile` override
-- can be shown to change which rows land (the optional @var(channel, web)
-- defaults to `web`).

CREATE SCHEMA IF NOT EXISTS raw__sales;

CREATE OR REPLACE TABLE raw__sales.sales (
    region   VARCHAR,
    channel  VARCHAR,
    customer VARCHAR,
    amount   INTEGER
);

INSERT INTO raw__sales.sales VALUES
    ('us',   'web',    'alice',   100),
    ('us',   'web',    'bob',     250),
    ('us',   'mobile', 'carol',   180),
    ('emea', 'web',    'dieter',  300),
    ('emea', 'mobile', 'elena',   210),
    ('apac', 'web',    'fumiko',  220);
