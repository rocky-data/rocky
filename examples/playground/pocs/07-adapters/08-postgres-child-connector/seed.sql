-- Source table for the reference child connector POC.
-- Insert-only: the connector only appends, so it never sees an UPDATE.
--
-- Every row gets its own created_at. Never use DEFAULT now() here:
-- now() is fixed for one transaction, so every row would tie.

CREATE TABLE public.events (
    id           integer     PRIMARY KEY,
    event_type   text        NOT NULL,
    amount_cents integer     NOT NULL,
    created_at   timestamptz NOT NULL
);

-- 20 rows, one minute apart: 2026-10-01 00:01 UTC to 00:20 UTC.
INSERT INTO public.events (id, event_type, amount_cents, created_at)
SELECT g,
       (ARRAY['signup', 'purchase', 'refund'])[1 + g % 3],
       g * 100,
       timestamptz '2026-10-01 00:00:00+00' + g * interval '1 minute'
FROM generate_series(1, 20) AS g;
