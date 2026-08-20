SELECT
    client_id,
    CAST(charged_at AS DATE) AS day,
    SUM(amount_eur) AS revenue_eur,
    CURRENT_TIMESTAMP AS loaded_at
FROM wh.raw.stripe_charges
WHERE NOT COALESCE(is_refund, FALSE)
  AND client_id IS NOT NULL
  AND charged_at IS NOT NULL
GROUP BY client_id, CAST(charged_at AS DATE)
