select
    client_id,
    cast(charged_at as date) as day,
    sum(amount_eur) as revenue_eur,
    current_timestamp as loaded_at
from wh.raw.stripe_charges
where coalesce(is_refund, false) = false
  and client_id is not null
  and charged_at is not null
group by
    client_id,
    cast(charged_at as date)
