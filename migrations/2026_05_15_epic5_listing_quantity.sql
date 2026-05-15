-- EPIC 5 Ticket 5.1 — Add quantity column and mode/quantity CHECK to listings
-- Safe to run multiple times (idempotent).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'listings' AND column_name = 'quantity'
    ) THEN
        ALTER TABLE listings ADD COLUMN quantity INTEGER NOT NULL DEFAULT 1;
    END IF;
END;
$$;

-- Enforce: single_quantity listings must always have quantity = 1
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_listing_quantity_mode'
          AND conrelid = 'listings'::regclass
    ) THEN
        ALTER TABLE listings
            ADD CONSTRAINT chk_listing_quantity_mode
            CHECK (mode = 'multi_quantity' OR quantity = 1);
    END IF;
END;
$$;
