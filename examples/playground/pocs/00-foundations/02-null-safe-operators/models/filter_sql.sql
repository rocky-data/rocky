-- Plain SQL `!=`: NULL comparisons return UNKNOWN, so WHERE drops NULL rows.
SELECT order_id, status, amount FROM raw_orders WHERE status != 'cancelled'
