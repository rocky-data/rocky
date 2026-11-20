-- Replay fixture 2 for `rocky connector test pg_events`: public.events as
-- run.sh leaves it before load 2. Same columns as seed.sql, in DuckDB types.
CREATE SCHEMA IF NOT EXISTS public;
CREATE TABLE public.events (
    id           INTEGER     PRIMARY KEY,
    event_type   VARCHAR     NOT NULL,
    amount_cents INTEGER     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL
);
-- Rows 1 to 20 come from seed.sql. Rows 21 to 25 are the five run.sh
-- inserts before load 2.
INSERT INTO public.events
SELECT g,
       CASE WHEN g > 20 THEN 'purchase' ELSE (['signup', 'purchase', 'refund'])[1 + g % 3] END,
       g * 100,
       TIMESTAMPTZ '2026-10-01 00:00:00+00' + g * INTERVAL '1 minute'
FROM range(1, 26) t(g);
