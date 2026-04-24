-- Financial ledger mirror. Long retention so auditors can time-travel
-- back a year on Databricks/Snowflake.
SELECT
    entry_id,
    amount_cents
FROM raw__ledger.entries
