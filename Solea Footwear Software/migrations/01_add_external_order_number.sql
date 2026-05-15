-- Migration: R1-01 + R1-02 + R1-03 support
-- Adds external_order_number to units, brand-return-rate view, and event dedup index.

-- 1. Add the column + index on units.
ALTER TABLE units
    ADD COLUMN IF NOT EXISTS external_order_number VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_units_external_order_number
    ON units(external_order_number);

COMMENT ON COLUMN units.external_order_number IS
    'The marketplace order number under which this unit was sold (e.g., eBay order id).';

-- 2. Dedup safety net for return_events (R1-03).
CREATE UNIQUE INDEX IF NOT EXISTS uq_return_events_msg_event
    ON return_events(email_message_id, event_type)
    WHERE email_message_id IS NOT NULL;

-- 3. Brand return rate view (R1-02).
CREATE OR REPLACE VIEW vw_brand_return_rates AS
WITH sold AS (
    SELECT p.brand,
           COUNT(*) AS sold_count
    FROM units u
    JOIN products p ON p.id = u.product_id
    WHERE u.sold_platform = 'eBay'
      AND u.sold_at IS NOT NULL
    GROUP BY p.brand
),
returned AS (
    SELECT r.brand,
           COUNT(*) AS return_count,
           COUNT(*) FILTER (WHERE r.final_outcome = 'closed_buyer_never_shipped')
               AS no_ship_count,
           COUNT(*) FILTER (WHERE r.final_outcome = 'refunded_after_return_received')
               AS refunded_after_count,
           COUNT(*) FILTER (WHERE r.final_outcome = 'refunded_without_return_received')
               AS refunded_without_count
    FROM returns r
    WHERE r.brand IS NOT NULL
      AND r.internal_order_id IS NOT NULL
    GROUP BY r.brand
)
SELECT COALESCE(s.brand, r.brand) AS brand,
       COALESCE(s.sold_count, 0)  AS sold_count,
       COALESCE(r.return_count, 0) AS return_count,
       COALESCE(r.no_ship_count, 0) AS no_ship_count,
       COALESCE(r.refunded_after_count, 0) AS refunded_after_count,
       COALESCE(r.refunded_without_count, 0) AS refunded_without_count,
       CASE WHEN COALESCE(s.sold_count, 0) > 0
            THEN ROUND(COALESCE(r.return_count, 0) * 100.0 / s.sold_count, 2)
            ELSE NULL END AS return_rate_percent
FROM sold s
FULL OUTER JOIN returned r ON r.brand = s.brand
ORDER BY sold_count DESC NULLS LAST, return_count DESC;

COMMENT ON VIEW vw_brand_return_rates IS
    'Brand return-rate report. matched_returns / sold_eBay_units, with closure counts.';
