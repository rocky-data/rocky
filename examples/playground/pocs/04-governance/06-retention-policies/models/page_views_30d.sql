-- Short-lived observability table. Keep 30 days of time-travel history
-- on Databricks/Snowflake so we can diff recent runs but don't pay for
-- long-tail storage.
SELECT
    event_id,
    user_id
FROM raw__events.page_views
