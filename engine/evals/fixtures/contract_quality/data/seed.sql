CREATE SCHEMA IF NOT EXISTS raw;
CREATE OR REPLACE TABLE raw.customers (customer_id BIGINT NOT NULL, email VARCHAR NOT NULL, country VARCHAR, signup_date DATE NOT NULL, referral_code VARCHAR);
INSERT INTO raw.customers SELECT i, 'user' || i || '@example.com', CASE WHEN i % 7 = 0 THEN NULL WHEN i % 3 = 0 THEN 'PT' ELSE 'US' END, DATE '2024-01-01' + CAST(i AS INTEGER), 'REF' || (i % 5) FROM range(1, 61) t(i);
CREATE OR REPLACE TABLE raw.orders (order_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, amount_cents INTEGER NOT NULL, status VARCHAR NOT NULL, ordered_at TIMESTAMP NOT NULL, coupon VARCHAR);
INSERT INTO raw.orders SELECT i, 1 + (i % 40), 500 + (i * 37) % 9500, CASE WHEN i % 10 = 0 THEN 'CANCELLED' WHEN i % 4 = 0 THEN 'PENDING' ELSE 'COMPLETE' END, TIMESTAMP '2024-03-01 00:00:00' + INTERVAL (i) HOUR, CASE WHEN i % 6 = 0 THEN 'SPRING' ELSE NULL END FROM range(1, 201) t(i);
CREATE OR REPLACE TABLE raw.payments (payment_id BIGINT NOT NULL, order_id BIGINT NOT NULL, method VARCHAR, paid_at TIMESTAMP);
INSERT INTO raw.payments SELECT i, i, CASE WHEN i % 2 = 0 THEN 'card' ELSE 'transfer' END, CASE WHEN i % 9 = 0 THEN NULL ELSE TIMESTAMP '2024-03-02 00:00:00' + INTERVAL (i) HOUR END FROM range(1, 151) t(i);
