-- SQL equivalent of running_totals.rocky
-- Shows the same logic in standard SQL for comparison.
SELECT
    txn_id,
    account_id,
    txn_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY account_id
        ORDER BY txn_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    COUNT() OVER (
        PARTITION BY account_id
        ORDER BY txn_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_count,
    SUM(amount) OVER () AS grand_total
FROM source.raw.transactions
