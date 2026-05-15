-- ============================================
-- INVENTORY MANAGEMENT SYSTEM - DATABASE SCHEMA
-- Phase 1: Core Inventory & eBay Integration
-- ============================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- TABLE: categories
-- Stores shoe categories mapped to eBay category IDs
-- ============================================
CREATE TABLE categories (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    internal_name VARCHAR(100) UNIQUE NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    ebay_category_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE categories IS 'Shoe categories with eBay category mappings';
COMMENT ON COLUMN categories.internal_name IS 'Internal identifier (e.g., athletic_shoes)';
COMMENT ON COLUMN categories.ebay_category_id IS 'eBay numeric category ID';

-- ============================================
-- TABLE: condition_grades
-- Maps internal condition grades to eBay conditions
-- ============================================
CREATE TABLE condition_grades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    internal_code VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    ebay_condition_id INTEGER,
    ebay_condition_name VARCHAR(100),
    ebay_condition_note_template TEXT,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE condition_grades IS 'Condition grades mapped to eBay condition system';
COMMENT ON COLUMN condition_grades.ebay_condition_note_template IS 'Template for condition notes (e.g., "Excellent condition: {details}")';

-- ============================================
-- TABLE: locations
-- Physical warehouse locations (shelf/bin codes)
-- ============================================
CREATE TABLE locations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE locations IS 'Physical warehouse shelf/bin locations';
COMMENT ON COLUMN locations.code IS 'Barcode location (e.g., A1-01-06-03)';

-- ============================================
-- TABLE: products
-- Catalog-level shoe models (not individual pairs)
-- ============================================
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brand VARCHAR(200) NOT NULL,
    model VARCHAR(300) NOT NULL,
    colorway VARCHAR(200),
    size VARCHAR(50) NOT NULL,
    gender VARCHAR(20),
    category_id UUID REFERENCES categories(id) ON DELETE SET NULL,
    condition_grade_id UUID REFERENCES condition_grades(id) ON DELETE SET NULL,
    default_price_ebay DECIMAL(10, 2),
    sku_prefix VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE products IS 'Shoe models/catalog entries (not individual pairs)';
COMMENT ON COLUMN products.default_price_ebay IS 'Default eBay listing price for this product';
COMMENT ON COLUMN products.sku_prefix IS 'Optional prefix for generating unit codes';

-- Create index for faster product searches
CREATE INDEX idx_products_brand_model ON products(brand, model);
CREATE INDEX idx_products_size ON products(size);

-- ============================================
-- TABLE: units
-- Individual physical pairs of shoes
-- ============================================
CREATE TABLE units (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_code VARCHAR(100) UNIQUE NOT NULL,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    location_id UUID REFERENCES locations(id) ON DELETE SET NULL,
    condition_grade_id UUID REFERENCES condition_grades(id) ON DELETE SET NULL,
    status VARCHAR(50) DEFAULT 'ready_to_list',
    cost_basis DECIMAL(10, 2),
    notes TEXT,
    sold_at TIMESTAMP,
    sold_price DECIMAL(10, 2),
    sold_platform VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_status CHECK (status IN (
        'ready_to_list',
        'listed',
        'sold',
        'shipped',
        'returned',
        'damaged',
        'reserved'
    ))
);

COMMENT ON TABLE units IS 'Individual physical pairs of shoes with barcodes';
COMMENT ON COLUMN units.unit_code IS 'Unique barcode on shoe (also used as eBay SKU)';
COMMENT ON COLUMN units.status IS 'Current status: ready_to_list, listed, sold, shipped, returned, damaged, reserved';

-- Create indexes for unit lookups
CREATE INDEX idx_units_unit_code ON units(unit_code);
CREATE INDEX idx_units_product_id ON units(product_id);
CREATE INDEX idx_units_status ON units(status);
CREATE INDEX idx_units_location_id ON units(location_id);

-- ============================================
-- TABLE: channels
-- Sales channels (eBay, Poshmark, Mercari, etc.)
-- ============================================
CREATE TABLE channels (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) UNIQUE NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    api_credentials JSONB,
    settings JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE channels IS 'Sales platforms (eBay, Poshmark, etc.)';
COMMENT ON COLUMN channels.api_credentials IS 'Encrypted API keys/tokens (stored as JSON)';
COMMENT ON COLUMN channels.settings IS 'Channel-specific configuration';

-- ============================================
-- TABLE: listings
-- Actual listings on sales channels
-- ============================================
CREATE TABLE listings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    channel_listing_id VARCHAR(200),
    title TEXT,
    description TEXT,
    current_price DECIMAL(10, 2),
    listing_url TEXT,
    status VARCHAR(50) DEFAULT 'active',
    mode VARCHAR(50) DEFAULT 'single_quantity',
    quantity INTEGER NOT NULL DEFAULT 1,
    photos JSONB,
    item_specifics JSONB,
    sold_at TIMESTAMP,
    sold_price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,

    CONSTRAINT check_listing_status CHECK (status IN (
        'active',
        'sold',
        'ended',
        'draft'
    )),
    CONSTRAINT check_listing_mode CHECK (mode IN (
        'single_quantity',
        'multi_quantity'
    )),
    CONSTRAINT chk_listing_quantity_mode CHECK (mode = 'multi_quantity' OR quantity = 1)
);

--  i am little bit ocnfused by channel listing id so it means that is id of listing on specifici channel like ebay explain me please
--  i am not sure why we have titlte , description , etc here in this even the products table has those details too but i think these are details from specific listing on ebay etc
-- what is difference between single quantity and multi quantity.
-- what each stauts means , ended or draft specifically ?

COMMENT ON TABLE listings IS 'Active and historical listings on sales channels';
COMMENT ON COLUMN listings.channel_listing_id IS 'Platform listing ID (e.g., eBay ItemID)';
COMMENT ON COLUMN listings.mode IS 'single_quantity (one unit) or multi_quantity (multiple units)';
COMMENT ON COLUMN listings.photos IS 'Array of photo URLs stored as JSON';

-- Create indexes for listing lookups
CREATE INDEX idx_listings_channel_listing_id ON listings(channel_listing_id);
CREATE INDEX idx_listings_product_id ON listings(product_id);
CREATE INDEX idx_listings_channel_id ON listings(channel_id);
CREATE INDEX idx_listings_status ON listings(status);

-- ============================================
-- TABLE: listing_units
-- Maps which units are associated with which listings
-- ============================================
CREATE TABLE listing_units (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    listing_id UUID NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    unit_id UUID NOT NULL REFERENCES units(id) ON DELETE CASCADE,
    matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(listing_id, unit_id)
);

COMMENT ON TABLE listing_units IS 'Junction table linking listings to specific units via SKU';

-- Create indexes
CREATE INDEX idx_listing_units_listing_id ON listing_units(listing_id);
CREATE INDEX idx_listing_units_unit_id ON listing_units(unit_id);

-- ============================================
-- TABLE: listing_templates
-- Reusable listing templates for cross-posting
-- ============================================
CREATE TABLE listing_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    source_channel_id UUID REFERENCES channels(id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    description TEXT,
    photos JSONB,
    item_specifics JSONB,
    base_price DECIMAL(10, 2),
    photo_metadata JSONB,
    pricing JSONB,
    category_mappings JSONB,
    seo_keywords JSONB,
    is_validated BOOLEAN NOT NULL DEFAULT FALSE,
    validation_errors JSONB,
    template_version INTEGER NOT NULL DEFAULT 1,
    last_synced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE listing_templates IS 'Templates created from eBay listings for cross-posting';

-- Create index
CREATE INDEX idx_listing_templates_product_id ON listing_templates(product_id);

-- ============================================
-- TABLE: sync_logs
-- Track eBay API sync operations
-- ============================================
CREATE TABLE sync_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    channel_id UUID REFERENCES channels(id) ON DELETE CASCADE,
    sync_type VARCHAR(100),
    status VARCHAR(50),
    records_processed INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    errors JSONB,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

COMMENT ON TABLE sync_logs IS 'Logs of eBay sync operations for debugging';

-- can you please shortly desciribe with example for 1 record i wanna udnerstand what is sync type and also the records that needs to process , update, create are done on our systme by syncing the details form ebay like we have to keep our systmer updated rigt?

-- Create index
CREATE INDEX idx_sync_logs_started_at ON sync_logs(started_at DESC);

-- ============================================
-- TABLE: alerts
-- System alerts for mismatches, issues, etc.
-- ============================================
CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) DEFAULT 'info',
    title VARCHAR(300) NOT NULL,
    message TEXT,
    related_entity_type VARCHAR(50),
    related_entity_id UUID,
    is_resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_severity CHECK (severity IN ('info', 'warning', 'error', 'critical'))
);

COMMENT ON TABLE alerts IS 'System alerts for mismatches, missing SKUs, etc.';

-- Create indexes
CREATE INDEX idx_alerts_is_resolved ON alerts(is_resolved);
CREATE INDEX idx_alerts_created_at ON alerts(created_at DESC);
CREATE INDEX idx_alerts_severity ON alerts(severity);

-- ============================================
-- TRIGGERS: Update timestamps automatically
-- ============================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to all tables with updated_at
CREATE TRIGGER update_categories_updated_at BEFORE UPDATE ON categories FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_condition_grades_updated_at BEFORE UPDATE ON condition_grades FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_locations_updated_at BEFORE UPDATE ON locations FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_units_updated_at BEFORE UPDATE ON units FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_channels_updated_at BEFORE UPDATE ON channels FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_listings_updated_at BEFORE UPDATE ON listings FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_listing_templates_updated_at BEFORE UPDATE ON listing_templates FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- SEED DATA: Initial Setup
-- ============================================

-- Insert default channel (eBay)
INSERT INTO channels (name, display_name, is_active) VALUES
('ebay', 'eBay', true);

-- Insert common categories
INSERT INTO categories (internal_name, display_name, ebay_category_id) VALUES
('athletic_shoes', 'Athletic Shoes', '15709'),
('casual_shoes', 'Casual Shoes', '24087'),
('boots', 'Boots', '11498'),
('sandals', 'Sandals & Flip-Flops', '11504'),
('dress_shoes', 'Dress Shoes', '53120');

-- Insert condition grades (eBay condition IDs)
INSERT INTO condition_grades (internal_code, display_name, ebay_condition_id, ebay_condition_name, ebay_condition_note_template, sort_order) VALUES
('new_with_box', 'New with Box', 1000, 'New with box', 'Brand new, never worn. Original box and tags included.', 1),
('new_without_box', 'New without Box', 1500, 'New without box', 'Brand new, never worn. No original box.', 2),
('excellent', 'Excellent Pre-Owned', 2750, 'Pre-owned', 'Excellent pre-owned condition. Minimal wear. {details}', 3),
('very_good', 'Very Good Pre-Owned', 2500, 'Pre-owned', 'Very good pre-owned condition. Light wear. {details}', 4),
('good', 'Good Pre-Owned', 3000, 'Pre-owned', 'Good pre-owned condition. Moderate wear. {details}', 5),
('fair', 'Fair Pre-Owned', 4000, 'Pre-owned', 'Fair condition. Noticeable wear. {details}', 6);

-- Insert sample locations
INSERT INTO locations (code, description) VALUES
('A1-01-01-01', 'Aisle A1, Row 01, Column 01, Shelf 01'),
('A1-01-01-02', 'Aisle A1, Row 01, Column 01, Shelf 02'),
('A1-01-01-03', 'Aisle A1, Row 01, Column 01, Shelf 03'),
('RETURNS', 'Returns Processing Area'),
('DAMAGED', 'Damaged Items Area');

-- ============================================
-- VIEWS: Useful queries
-- ============================================

-- View: Product inventory summary
CREATE OR REPLACE VIEW vw_product_inventory AS
SELECT 
    p.id AS product_id,
    p.brand,
    p.model,
    p.colorway,
    p.size,
    p.gender,
    c.display_name AS category,
    COUNT(u.id) AS total_units,
    COUNT(CASE WHEN u.status = 'ready_to_list' THEN 1 END) AS ready_to_list,
    COUNT(CASE WHEN u.status = 'listed' THEN 1 END) AS listed,
    COUNT(CASE WHEN u.status = 'sold' THEN 1 END) AS sold,
    COUNT(CASE WHEN u.status = 'shipped' THEN 1 END) AS shipped,
    COUNT(CASE WHEN u.status = 'returned' THEN 1 END) AS returned,
    COUNT(CASE WHEN u.status = 'reserved' THEN 1 END) AS reserved,
    p.default_price_ebay
FROM products p
LEFT JOIN categories c ON p.category_id = c.id
LEFT JOIN units u ON p.id = u.product_id
GROUP BY p.id, p.brand, p.model, p.colorway, p.size, p.gender, c.display_name, p.default_price_ebay;

COMMENT ON VIEW vw_product_inventory IS 'Summary of inventory counts per product';

-- View: Units with listing information
CREATE OR REPLACE VIEW vw_units_with_listings AS
SELECT 
    u.id AS unit_id,
    u.unit_code,
    u.status AS unit_status,
    p.brand,
    p.model,
    p.size,
    p.colorway,
    l.code AS location_code,
    cg.display_name AS condition,
    lst.channel_listing_id,
    lst.title AS listing_title,
    lst.current_price,
    lst.status AS listing_status,
    ch.display_name AS channel_name,
    u.created_at,
    u.updated_at
FROM units u
JOIN products p ON u.product_id = p.id
LEFT JOIN locations l ON u.location_id = l.id
LEFT JOIN condition_grades cg ON u.condition_grade_id = cg.id
LEFT JOIN listing_units lu ON u.id = lu.unit_id
LEFT JOIN listings lst ON lu.listing_id = lst.id
LEFT JOIN channels ch ON lst.channel_id = ch.id;

COMMENT ON VIEW vw_units_with_listings IS 'Complete unit information with listing details';

-- ============================================
-- FUNCTIONS: Useful queries
-- ============================================

-- Function: Get available units for a product
CREATE OR REPLACE FUNCTION get_available_units(p_product_id UUID)
RETURNS TABLE (
    unit_id UUID,
    unit_code VARCHAR,
    location_code VARCHAR,
    condition VARCHAR,
    status VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        u.id,
        u.unit_code,
        l.code,
        cg.display_name,
        u.status
    FROM units u
    LEFT JOIN locations l ON u.location_id = l.id
    LEFT JOIN condition_grades cg ON u.condition_grade_id = cg.id
    WHERE u.product_id = p_product_id
    AND u.status IN ('ready_to_list', 'returned', 'reserved')
    ORDER BY u.created_at;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_available_units IS 'Returns all available units for a given product';

-- ============================================
-- END OF SCHEMA
-- ============================================


-- ============================================
-- RETURN TRACKING SYSTEM - DATABASE SCHEMA
-- Additional tables for eBay Returns Tracking Workflow
-- ============================================

-- ============================================
-- TABLE: returns
-- Master table for tracking return lifecycle
-- ============================================
CREATE TABLE returns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    marketplace VARCHAR(50) DEFAULT 'eBay',
    return_id VARCHAR(100),
    order_number VARCHAR(100),
    buyer_username VARCHAR(200),
    item_title TEXT,
    brand VARCHAR(200),
    sku VARCHAR(100),
    external_listing_id VARCHAR(200),
    internal_order_id UUID REFERENCES units(id),
    return_reason_ebay VARCHAR(200),
    buyer_comment TEXT,
    request_amount DECIMAL(10, 2),
    opened_at TIMESTAMP,
    buyer_ship_by_date TIMESTAMP,
    buyer_shipped_at TIMESTAMP,
    tracking_number VARCHAR(200),
    item_delivered_back_at TIMESTAMP,
    refund_issued_at TIMESTAMP,
    closed_at TIMESTAMP,
    status_current VARCHAR(50),
    final_outcome VARCHAR(50),
    internal_bucket VARCHAR(50),
    notes TEXT,
    recommended_fix TEXT,
    classifier_source VARCHAR(50),
    classifier_confidence DECIMAL(5, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE returns IS 'Master table for tracking return lifecycle';
COMMENT ON COLUMN returns.marketplace IS 'Sales platform (e.g., eBay)';
COMMENT ON COLUMN returns.return_id IS 'eBay return ID';
COMMENT ON COLUMN returns.external_listing_id IS 'Marketplace listing ID (e.g., eBay ItemID)';
COMMENT ON COLUMN returns.order_number IS 'Original order number';
COMMENT ON COLUMN returns.internal_order_id IS 'Reference to internal unit';
COMMENT ON COLUMN returns.status_current IS 'Current status of the return';
COMMENT ON COLUMN returns.final_outcome IS 'Final outcome of the return';
COMMENT ON COLUMN returns.internal_bucket IS 'Internal classification bucket';
COMMENT ON COLUMN returns.classifier_source IS 'Source of classification';
COMMENT ON COLUMN returns.classifier_confidence IS 'Confidence level of classification';

-- Create indexes for returns table
CREATE INDEX idx_returns_return_id ON returns(return_id);
CREATE INDEX idx_returns_order_number ON returns(order_number);
CREATE INDEX idx_returns_sku ON returns(sku);
CREATE INDEX idx_returns_external_listing_id ON returns(external_listing_id);
CREATE INDEX idx_returns_internal_order_id ON returns(internal_order_id);
CREATE INDEX idx_returns_status_current ON returns(status_current);
CREATE INDEX idx_returns_final_outcome ON returns(final_outcome);
CREATE INDEX idx_returns_created_at ON returns(created_at);

-- ============================================
-- TABLE: return_events
-- Audit trail for return-related events
-- ============================================
CREATE TABLE return_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    return_id UUID REFERENCES returns(id) ON DELETE CASCADE,
    event_type VARCHAR(100),
    event_timestamp TIMESTAMP,
    source_type VARCHAR(50) DEFAULT 'email', -- email, page_scrape, manual
    email_message_id VARCHAR(200),
    email_subject TEXT,
    raw_payload TEXT,
    parsed_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE return_events IS 'Audit trail for return-related events';
COMMENT ON COLUMN return_events.source_type IS 'Source of the event: email, page_scrape, manual';
COMMENT ON COLUMN return_events.email_message_id IS 'Gmail message ID for tracking';
COMMENT ON COLUMN return_events.raw_payload IS 'Raw email payload for reference';
COMMENT ON COLUMN return_events.parsed_data IS 'Parsed data from email';

-- Create indexes for return_events table
CREATE INDEX idx_return_events_return_id ON return_events(return_id);
CREATE INDEX idx_return_events_email_message_id ON return_events(email_message_id);
CREATE INDEX idx_return_events_event_type ON return_events(event_type);
CREATE INDEX idx_return_events_created_at ON return_events(created_at);

-- ============================================
-- TABLE: email_processing_log
-- Track processed emails to avoid duplicate processing
-- ============================================
CREATE TABLE email_processing_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_message_id VARCHAR(200) UNIQUE NOT NULL, -- Gmail's unique message ID
    email_subject TEXT,
    email_sender VARCHAR(200),
    received_date TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_status VARCHAR(50) DEFAULT 'success', -- success, failed, skipped
    processing_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE email_processing_log IS 'Track processed emails to avoid duplicate processing';
COMMENT ON COLUMN email_processing_log.email_message_id IS 'Gmail unique message ID';
COMMENT ON COLUMN email_processing_log.processing_status IS 'Result of email processing';
COMMENT ON COLUMN email_processing_log.processing_notes IS 'Additional notes about processing';

-- Create indexes for email_processing_log table
CREATE INDEX idx_email_processing_log_message_id ON email_processing_log(email_message_id);
CREATE INDEX idx_email_processing_log_processed_at ON email_processing_log(processed_at);
CREATE INDEX idx_email_processing_log_status ON email_processing_log(processing_status);

-- ============================================
-- TRIGGER: Update timestamps for returns table
-- ============================================
CREATE TRIGGER update_returns_updated_at BEFORE UPDATE ON returns FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- VIEWS: Return-related queries
-- ============================================

-- View: Return summary with key metrics
CREATE OR REPLACE VIEW vw_returns_summary AS
SELECT 
    r.id AS return_id,
    r.return_id AS ebay_return_id,
    r.order_number,
    r.buyer_username,
    r.item_title,
    r.brand,
    r.sku,
    r.return_reason_ebay,
    r.status_current,
    r.final_outcome,
    r.internal_bucket,
    r.recommended_fix,
    r.opened_at,
    r.closed_at,
    EXTRACT(EPOCH FROM (r.closed_at - r.opened_at))/86400 AS days_to_close
FROM returns r;

COMMENT ON VIEW vw_returns_summary IS 'Summary view of return information with key metrics';

-- View: Return statistics by brand
CREATE OR REPLACE VIEW vw_returns_by_brand AS
SELECT 
    r.brand,
    COUNT(*) AS total_returns,
    COUNT(CASE WHEN r.final_outcome = 'refunded_after_return_received' THEN 1 END) AS refunded_after_return_received,
    COUNT(CASE WHEN r.final_outcome = 'refunded_without_return_received' THEN 1 END) AS refunded_without_return_received,
    COUNT(CASE WHEN r.final_outcome = 'closed_buyer_never_shipped' THEN 1 END) AS closed_buyer_never_shipped,
    COUNT(CASE WHEN r.final_outcome = 'closed_other' THEN 1 END) AS closed_other,
    ROUND(COUNT(CASE WHEN r.final_outcome = 'closed_buyer_never_shipped' THEN 1 END) * 100.0 / COUNT(*), 2) AS percent_closed_buyer_never_shipped
FROM returns r
WHERE r.brand IS NOT NULL
GROUP BY r.brand
ORDER BY total_returns DESC;

COMMENT ON VIEW vw_returns_by_brand IS 'Return statistics grouped by brand';

-- View: Return classification summary
CREATE OR REPLACE VIEW vw_returns_classification AS
SELECT 
    r.internal_bucket,
    COUNT(*) AS count,
    COUNT(CASE WHEN r.final_outcome = 'closed_buyer_never_shipped' THEN 1 END) AS buyer_never_shipped_count,
    ROUND(COUNT(CASE WHEN r.final_outcome = 'closed_buyer_never_shipped' THEN 1 END) * 100.0 / COUNT(*), 2) AS buyer_never_shipped_percent
FROM returns r
WHERE r.internal_bucket IS NOT NULL
GROUP BY r.internal_bucket
ORDER BY count DESC;

COMMENT ON VIEW vw_returns_classification IS 'Return classification summary with buyer never shipped stats';

-- ============================================
-- END OF RETURN TRACKING SCHEMA
-- ============================================