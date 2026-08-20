SELECT
    client_id,
    CAST(charged_at AS DATE) AS day,
    SUM(amount_eur) AS revenue_eur,
    CURRENT_TIMESTAMP AS loaded_at
FROM wh.raw.stripe_charges
WHERE client_id IS NOT NULL
  AND charged_at IS NOT NULL
  AND amount_eur IS NOT NULL
  AND COALESCE(is_refund, FALSE) = FALSE
GROUP BY
    client_id,
    CAST(charged_at AS DATE)
