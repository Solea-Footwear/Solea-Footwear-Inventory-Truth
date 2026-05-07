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
    photos JSONB,
    item_specifics JSONB,
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
    ))
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
