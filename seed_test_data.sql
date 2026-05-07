-- ============================================
-- SEED DATA FOR TESTING RETURN TRACKING
-- eBay Item: 306850784377 (Ecco ST.1 Hybrid Shoes)
-- ============================================

-- Get the eBay channel ID and dress_shoes category ID
DO $$
DECLARE
    v_ebay_channel_id UUID;
    v_dress_shoes_category_id UUID;
    v_new_without_box_condition_id UUID;
    v_location_id UUID;
    v_product_id UUID;
    v_unit_id UUID;
    v_listing_id UUID;
BEGIN
    -- Get channel ID for eBay
    SELECT id INTO v_ebay_channel_id FROM channels WHERE name = 'ebay';
    
    -- Get category ID for dress shoes
    SELECT id INTO v_dress_shoes_category_id FROM categories WHERE internal_name = 'dress_shoes';
    
    -- Get condition grade ID for new without box
    SELECT id INTO v_new_without_box_condition_id FROM condition_grades WHERE internal_code = 'new_without_box';
    
    -- Get location ID
    SELECT id INTO v_location_id FROM locations WHERE code = 'A1-01-01-01';
    
    -- Insert Product (Ecco ST.1 Hybrid)
    INSERT INTO products (
        brand,
        model,
        colorway,
        size,
        gender,
        category_id,
        condition_grade_id,
        default_price_ebay,
        sku_prefix,
        notes
    ) VALUES (
        'Ecco',
        'ST.1 Hybrid',
        'Black',
        '10',
        'Men',
        v_dress_shoes_category_id,
        v_new_without_box_condition_id,
        99.99,
        'ECCO',
        'Ecco ST.1 Hybrid Mens Size 10-10.5 Black Classic Plain Toe Oxford Derby Shoes'
    ) RETURNING id INTO v_product_id;
    
    RAISE NOTICE 'Created product with ID: %', v_product_id;
    
    -- Insert Unit (Individual pair with SKU 00101733)
    INSERT INTO units (
        unit_code,
        product_id,
        location_id,
        condition_grade_id,
        status,
        cost_basis,
        notes,
        sold_at,
        sold_price,
        sold_platform
    ) VALUES (
        '00101733',  -- This is the SKU from eBay
        v_product_id,
        v_location_id,
        v_new_without_box_condition_id,
        'sold',  -- Item was sold
        50.00,  -- Example cost
        'Sold to bfuchs226 on eBay',
        '2026-03-30 05:18:50',  -- Sale date from eBay data
        99.99,  -- Sold price
        'eBay'
    ) RETURNING id INTO v_unit_id;
    
    RAISE NOTICE 'Created unit with ID: % and SKU: 00101733', v_unit_id;
    
    -- Insert Listing (eBay listing 306850784377)
    INSERT INTO listings (
        product_id,
        channel_id,
        channel_listing_id,
        title,
        description,
        current_price,
        listing_url,
        status,
        mode,
        photos,
        item_specifics,
        sold_at,
        sold_price,
        ended_at
    ) VALUES (
        v_product_id,
        v_ebay_channel_id,
        '306850784377',  -- eBay ItemID
        'Ecco ST.1 Hybrid Mens Size 10-10.5 Black Classic Plain Toe Oxford Derby Shoes',
        'Ecco ST.1 Hybrid mens dress shoes in black. Size 10-10.5. Classic plain toe oxford derby style. New without box.',
        99.99,
        'https://www.ebay.com/itm/306850784377',
        'sold',
        'single_quantity',
        '["https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/YKIAAeSwlBhpyMCv/$_57.JPG"]'::jsonb,
        '{
            "Brand": "Ecco",
            "Model": "ECCO ST. 1 Hybrid",
            "US Shoe Size": "10",
            "Color": "Black",
            "Style": "Oxford",
            "Type": "Dress",
            "Upper Material": "Leather",
            "Closure": "Lace Up"
        }'::jsonb,
        '2026-03-30 05:18:50',
        99.99,
        '2026-03-30 05:18:50'
    ) RETURNING id INTO v_listing_id;
    
    RAISE NOTICE 'Created listing with ID: % and eBay ItemID: 306850784377', v_listing_id;
    
    -- Link Unit to Listing
    INSERT INTO listing_units (
        listing_id,
        unit_id
    ) VALUES (
        v_listing_id,
        v_unit_id
    );
    
    RAISE NOTICE 'Linked unit to listing';
    
    -- Summary
    RAISE NOTICE '========================================';
    RAISE NOTICE 'SEED DATA CREATED SUCCESSFULLY';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Product ID: %', v_product_id;
    RAISE NOTICE 'Unit ID: %', v_unit_id;
    RAISE NOTICE 'Unit SKU: 00101733';
    RAISE NOTICE 'Listing ID: %', v_listing_id;
    RAISE NOTICE 'eBay ItemID: 306850784377';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'You can now test return tracking with:';
    RAISE NOTICE '- SKU: 00101733';
    RAISE NOTICE '- eBay ItemID: 306850784377';
    RAISE NOTICE '- Buyer: bfuchs226';
    RAISE NOTICE '========================================';
    
END $$;

-- Verify the data was created
SELECT 
    p.brand,
    p.model,
    p.size,
    u.unit_code AS sku,
    u.status AS unit_status,
    l.channel_listing_id AS ebay_item_id,
    l.status AS listing_status,
    l.sold_price
FROM products p
JOIN units u ON p.id = u.product_id
JOIN listing_units lu ON u.id = lu.unit_id
JOIN listings l ON lu.listing_id = l.id
WHERE u.unit_code = '00101733';
