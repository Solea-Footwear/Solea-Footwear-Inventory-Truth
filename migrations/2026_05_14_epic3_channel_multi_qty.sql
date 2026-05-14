-- EPIC 3 Ticket 3.1 — Add supports_multi_quantity to channels
-- Mercari and Poshmark do not allow multi-quantity listings per their ToS.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'channels' AND column_name = 'supports_multi_quantity'
    ) THEN
        ALTER TABLE channels
            ADD COLUMN supports_multi_quantity BOOLEAN NOT NULL DEFAULT TRUE;
    END IF;
END;
$$;

-- Seed known channel constraints (idempotent — safe to run multiple times)
UPDATE channels SET supports_multi_quantity = FALSE WHERE LOWER(name) IN ('mercari', 'poshmark');
UPDATE channels SET supports_multi_quantity = TRUE  WHERE LOWER(name) = 'ebay';

-- Ensure poshmark and mercari channels exist (idempotent)
INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
VALUES ('poshmark', 'Poshmark', true, false)
ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity;

INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
VALUES ('mercari', 'Mercari', true, false)
ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity;
