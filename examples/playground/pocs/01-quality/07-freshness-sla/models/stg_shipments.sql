-- Shipments staged from raw. `shipped_at` passes through from the source as a
-- TIMESTAMP, and the sidecar declares a [freshness] SLA on it, so W005 is
-- suppressed for this model.
SELECT
    shipment_id,
    order_id,
    shipped_at
FROM seeds.shipments
