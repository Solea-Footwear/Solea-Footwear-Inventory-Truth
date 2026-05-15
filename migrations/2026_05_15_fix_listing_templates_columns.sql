-- Fix listing_templates: add columns referenced by template_service and app routes
-- All additions are idempotent (IF NOT EXISTS).

ALTER TABLE listing_templates
    ADD COLUMN IF NOT EXISTS photo_metadata      JSONB,
    ADD COLUMN IF NOT EXISTS pricing             JSONB,
    ADD COLUMN IF NOT EXISTS category_mappings   JSONB,
    ADD COLUMN IF NOT EXISTS seo_keywords        JSONB,
    ADD COLUMN IF NOT EXISTS is_validated        BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS validation_errors   JSONB,
    ADD COLUMN IF NOT EXISTS template_version    INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_synced_at      TIMESTAMP;
