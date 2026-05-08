-- Delta applied between the two rocky runs to exercise MERGE semantics:
--   * Customer 5 changes tier bronze -> gold (UPDATE)
--   * Customer 7 changes name + email (UPDATE)
--   * Customer 51 is brand new (INSERT)
-- Customer 5 had tier=silver in the seed (i % 5 = 0); we bump it to gold.

UPDATE seeds.customers
SET tier = 'gold'
WHERE customer_id = 5;

UPDATE seeds.customers
SET name = 'Customer 7 (renamed)',
    email = 'customer7+renamed@example.com'
WHERE customer_id = 7;

INSERT INTO seeds.customers VALUES
    (51, 'Customer 51', 'customer51@example.com', 'bronze');
