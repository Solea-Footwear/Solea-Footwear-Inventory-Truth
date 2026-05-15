-- Migration: Add sold_at, sold_price, sold_platform columns
-- Run this to add missing columns to existing database

-- Add columns to units table
ALTER TABLE units ADD COLUMN IF NOT EXISTS sold_at TIMESTAMP;
ALTER TABLE units ADD COLUMN IF NOT EXISTS sold_price DECIMAL(10, 2);
ALTER TABLE units ADD COLUMN IF NOT EXISTS sold_platform VARCHAR(50);

-- Add columns to listings table
ALTER TABLE listings ADD COLUMN IF NOT EXISTS sold_at TIMESTAMP;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS sold_price DECIMAL(10, 2);

-- Verify columns were added
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'units' 
AND column_name IN ('sold_at', 'sold_price', 'sold_platform')
ORDER BY column_name;

SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'listings' 
AND column_name IN ('sold_at', 'sold_price')
ORDER BY column_name;
