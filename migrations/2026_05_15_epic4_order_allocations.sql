-- EPIC 4 — Order Allocation System
-- Adds: orders, order_allocations, marketplace_events (fixes latent missing-table bug)

CREATE TABLE IF NOT EXISTS orders (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    platform             text NOT NULL,
    platform_order_id    text,
    platform_listing_id  text,
    message_id           text NOT NULL,
    status               text NOT NULL DEFAULT 'pending',
    sale_price           numeric(10,2),
    buyer_name           text,
    raw_payload          jsonb,
    needs_reconciliation boolean NOT NULL DEFAULT false,
    allocated_at         timestamp,
    shipped_at           timestamp,
    created_at           timestamp DEFAULT now(),
    updated_at           timestamp DEFAULT now(),
    CONSTRAINT chk_order_status CHECK (
        status IN ('pending','allocated','shipped','completed','failed','needs_reconciliation')
    )
);

-- Deduplicate on (platform, platform_order_id) when the order ID is present
CREATE UNIQUE INDEX IF NOT EXISTS uq_order_platform_order_id
    ON orders (platform, platform_order_id)
    WHERE platform_order_id IS NOT NULL;

-- Always deduplicate on (platform, message_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_order_platform_message_id
    ON orders (platform, message_id);

-- order_allocations: which units fulfil which order
CREATE TABLE IF NOT EXISTS order_allocations (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id     uuid NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    unit_id      uuid NOT NULL REFERENCES units(id),
    listing_id   uuid REFERENCES listings(id),
    allocated_at timestamp DEFAULT now(),
    CONSTRAINT uq_order_unit UNIQUE (order_id, unit_id)
);

-- marketplace_events: raw event log
-- Fixes: email_parser_service.py writes to this table but it was never created in schema.sql
CREATE TABLE IF NOT EXISTS marketplace_events (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    platform              text NOT NULL,
    event_type            text NOT NULL,
    message_id            text NOT NULL,
    external_listing_id   text,
    external_order_id     text,
    sku                   text,
    raw_payload           jsonb,
    needs_reconciliation  boolean NOT NULL DEFAULT false,
    reconciliation_reason text,
    created_at            timestamp DEFAULT now(),
    CONSTRAINT uq_marketplace_event UNIQUE (platform, message_id)
);
