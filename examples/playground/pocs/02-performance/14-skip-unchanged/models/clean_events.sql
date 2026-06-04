-- A single plain SELECT over a bare source table — no CTEs, no subqueries, no
-- set operations, and only deterministic functions (UPPER). That shape is what
-- makes the model skip-eligible: its lineage is provably complete and its
-- output is reproducible from unchanged inputs.
SELECT
    event_id,
    user_id,
    UPPER(event_type) AS event_type,
    occurred_at
FROM raw__events.events
