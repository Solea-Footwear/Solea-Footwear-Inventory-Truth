-- EPIC 1 — Product ID System: additive columns on products table
-- Safe to run multiple times (IF NOT EXISTS / idempotent ALTER).

ALTER TABLE products
    ADD COLUMN IF NOT EXISTS product_id      VARCHAR(255),
    ADD COLUMN IF NOT EXISTS style_code      VARCHAR(100),
    ADD COLUMN IF NOT EXISTS condition_code  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS is_interchangeable BOOLEAN NOT NULL DEFAULT FALSE;

-- Full UNIQUE constraint (required for ON CONFLICT (product_id) DO NOTHING in product_registry_service)
DO $$
BEGIN
    -- Drop old partial index if it exists (from earlier versions of this migration)
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'products' AND indexname = 'ix_products_product_id'
    ) THEN
        DROP INDEX ix_products_product_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_products_product_id' AND conrelid = 'products'::regclass
    ) THEN
        ALTER TABLE products
            ADD CONSTRAINT uq_products_product_id UNIQUE (product_id);
    END IF;
END;
$$;

-- Check constraint
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'check_condition_code' AND conrelid = 'products'::regclass
    ) THEN
        ALTER TABLE products
            ADD CONSTRAINT check_condition_code
            CHECK (condition_code IN ('NEW','LIKE_NEW','EXCELLENT','GOOD','FAIR') OR condition_code IS NULL);
    END IF;
END;
$$;
