-- EPIC 1 — Product ID System: additive columns on products table
-- Safe to run multiple times (IF NOT EXISTS / idempotent ALTER).

ALTER TABLE products
    ADD COLUMN IF NOT EXISTS product_id      VARCHAR(255),
    ADD COLUMN IF NOT EXISTS style_code      VARCHAR(100),
    ADD COLUMN IF NOT EXISTS condition_code  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS is_interchangeable BOOLEAN NOT NULL DEFAULT FALSE;

-- Unique index (CREATE UNIQUE INDEX ... IF NOT EXISTS requires Postgres 9.5+)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'products' AND indexname = 'ix_products_product_id'
    ) THEN
        CREATE UNIQUE INDEX ix_products_product_id ON products (product_id)
            WHERE product_id IS NOT NULL;
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
