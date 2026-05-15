"""
Unit tests for src.services.order_allocation_service (EPIC 4).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.  Requires postgres running with the EPIC 4 migration
applied (orders, order_allocations, marketplace_events tables).
"""
import pytest
import psycopg2.extras

from src.services.order_allocation_service import allocate_order


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product(db, condition_code='NEW', is_interchangeable=True):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        pid = f"NIKE-AJ1-TEST-MEN-10-{condition_code}"
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES (gen_random_uuid(), %s, 'Nike', 'Air Jordan 1', 'TEST', 'Men', '10', %s, %s)
            RETURNING *
            """,
            [pid, condition_code, is_interchangeable],
        )
        return dict(cur.fetchone())


def _make_unit(db, product_id, unit_code, status='ready_to_list'):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING *",
            [unit_code, product_id, status],
        )
        return dict(cur.fetchone())


def _make_channel(db, name='ebay', supports_multi=True):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES (%s, %s, TRUE, %s)
            ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity
            RETURNING *
            """,
            [name, name.capitalize(), supports_multi],
        )
        return dict(cur.fetchone())


def _make_listing(db, product_id, channel_id, channel_listing_id=None):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, current_price, status, mode)
            VALUES
                (gen_random_uuid(), %s, %s, 'Test Listing', 100.0, 'active', 'single_quantity')
            RETURNING *
            """,
            [product_id, channel_id],
        )
        listing = dict(cur.fetchone())
        if channel_listing_id:
            cur.execute(
                "UPDATE listings SET channel_listing_id = %s WHERE id = %s RETURNING *",
                [channel_listing_id, listing['id']],
            )
            listing = dict(cur.fetchone())
        return listing


def _make_multi_listing(db, product_id, channel_id, channel_listing_id=None):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, current_price, status, mode, quantity)
            VALUES
                (gen_random_uuid(), %s, %s, 'Multi Test', 100.0, 'active', 'multi_quantity', 2)
            RETURNING *
            """,
            [product_id, channel_id],
        )
        listing = dict(cur.fetchone())
        if channel_listing_id:
            cur.execute(
                "UPDATE listings SET channel_listing_id = %s WHERE id = %s RETURNING *",
                [channel_listing_id, listing['id']],
            )
            listing = dict(cur.fetchone())
        return listing


def _attach_unit(db, listing_id, unit_id):
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) "
            "VALUES (gen_random_uuid(), %s, %s)",
            [listing_id, unit_id],
        )


# ---------------------------------------------------------------------------
# Tests 1–3: happy paths per platform
# ---------------------------------------------------------------------------

def test_ebay_sale_sku_match(db):
    """eBay sale with platform_order_id + matching unit SKU → allocated."""
    product = _make_product(db)
    unit = _make_unit(db, product['id'], 'AJ1-EBAY-001')

    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-ebay-001',
        'order_id': 'ORD-001',
        'sku': 'AJ1-EBAY-001',
        'price': 130.0,
    })

    assert created is True
    assert order['status'] == 'allocated'
    assert order['platform'] == 'ebay'
    assert len(allocs) == 1

    with db.cursor() as cur:
        cur.execute("SELECT status, sold_platform FROM units WHERE id = %s", [unit['id']])
        row = cur.fetchone()
    assert row[0] == 'sold'
    assert row[1] == 'ebay'


def test_poshmark_sale_message_id_only(db):
    """Poshmark sale with message_id (no order_id) + SKU → allocated."""
    product = _make_product(db)
    unit = _make_unit(db, product['id'], 'AJ1-POSH-001')

    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'poshmark',
        'message_id': 'msg-posh-001',
        'sku': 'AJ1-POSH-001',
        'price': 115.0,
    })

    assert created is True
    assert order['status'] == 'allocated'
    assert order['platform_order_id'] is None
    assert len(allocs) == 1

    with db.cursor() as cur:
        cur.execute("SELECT sold_platform FROM units WHERE id = %s", [unit['id']])
    # verify no error


def test_mercari_sale_no_order_id(db):
    """Mercari sale — no order_id, SKU match → order created without platform_order_id."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-MERC-001')

    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'mercari',
        'message_id': 'msg-merc-001',
        'sku': 'AJ1-MERC-001',
        'price': 105.0,
    })

    assert created is True
    assert order['platform_order_id'] is None
    assert order['status'] == 'allocated'


# ---------------------------------------------------------------------------
# Tests 4–5: idempotency
# ---------------------------------------------------------------------------

def test_idempotent_same_platform_order_id(db):
    """Second call with same platform_order_id returns existing order, created=False."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-IDEM-001')

    order1, _, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay', 'message_id': 'msg-idem-001',
        'order_id': 'ORD-IDEM', 'sku': 'AJ1-IDEM-001', 'price': 100.0,
    })
    order2, allocs2, created2 = allocate_order(db, parsed_sale={
        'platform': 'ebay', 'message_id': 'msg-idem-002',
        'order_id': 'ORD-IDEM', 'sku': 'AJ1-IDEM-001', 'price': 100.0,
    })

    assert created2 is False
    assert str(order2['id']) == str(order1['id'])


def test_idempotent_same_message_id_no_order_id(db):
    """Second call with same message_id (no order_id) returns existing order."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-IDEM-002')

    order1, _, _ = allocate_order(db, parsed_sale={
        'platform': 'poshmark', 'message_id': 'msg-idem-posh',
        'sku': 'AJ1-IDEM-002', 'price': 90.0,
    })
    order2, _, created2 = allocate_order(db, parsed_sale={
        'platform': 'poshmark', 'message_id': 'msg-idem-posh',
        'sku': 'AJ1-IDEM-002', 'price': 90.0,
    })

    assert created2 is False
    assert str(order2['id']) == str(order1['id'])


# ---------------------------------------------------------------------------
# Tests 6–8: reconciliation paths
# ---------------------------------------------------------------------------

def test_sku_not_found_needs_reconciliation(db):
    """SKU not found → order created with needs_reconciliation=True, empty allocs."""
    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-recon-001',
        'sku': 'NONEXISTENT-SKU',
    })

    assert created is True
    assert order['needs_reconciliation'] is True
    assert order['status'] == 'needs_reconciliation'
    assert allocs == []


def test_listing_id_fallback_finds_unit(db):
    """No SKU, listing_id fallback resolves unit via listing_units JOIN."""
    product = _make_product(db)
    channel = _make_channel(db)
    unit = _make_unit(db, product['id'], 'AJ1-FALL-001')
    listing = _make_listing(db, product['id'], channel['id'], channel_listing_id='EXT-LIST-001')
    _attach_unit(db, listing['id'], unit['id'])

    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-fall-001',
        'listing_id': 'EXT-LIST-001',
        'price': 120.0,
    })

    assert created is True
    assert order['status'] == 'allocated'
    assert len(allocs) == 1


def test_both_sku_and_listing_id_missing_needs_reconciliation(db):
    """Neither SKU nor listing_id → needs_reconciliation."""
    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'mercari',
        'message_id': 'msg-recon-002',
        'price': 80.0,
    })

    assert created is True
    assert order['needs_reconciliation'] is True
    assert allocs == []


# ---------------------------------------------------------------------------
# Tests 9–11: unit and order field values
# ---------------------------------------------------------------------------

def test_unit_sold_fields_set_correctly(db):
    """sold_at, sold_price, sold_platform are set on the unit after allocation."""
    product = _make_product(db)
    unit = _make_unit(db, product['id'], 'AJ1-SOLD-001')

    allocate_order(db, parsed_sale={
        'platform': 'poshmark',
        'message_id': 'msg-sold-001',
        'sku': 'AJ1-SOLD-001',
        'price': 95.50,
    })

    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT status, sold_price, sold_platform, sold_at FROM units WHERE id = %s", [unit['id']])
        row = dict(cur.fetchone())

    assert row['status'] == 'sold'
    assert float(row['sold_price']) == 95.50
    assert row['sold_platform'] == 'poshmark'
    assert row['sold_at'] is not None


def test_order_allocation_row_fields(db):
    """order_allocations row has correct order_id and unit_id."""
    product = _make_product(db)
    unit = _make_unit(db, product['id'], 'AJ1-ALLOC-001')

    order, allocs, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-alloc-001',
        'sku': 'AJ1-ALLOC-001',
    })

    assert len(allocs) == 1
    assert str(allocs[0]['order_id']) == str(order['id'])
    assert str(allocs[0]['unit_id']) == str(unit['id'])


def test_sale_price_stored_in_order(db):
    """sale_price is stored in orders.sale_price."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-PRICE-001')

    order, _, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-price-001',
        'sku': 'AJ1-PRICE-001',
        'price': 142.99,
    })

    assert order['sale_price'] is not None
    assert float(order['sale_price']) == 142.99


# ---------------------------------------------------------------------------
# Test 12: return type
# ---------------------------------------------------------------------------

def test_return_type(db):
    """allocate_order returns (dict, list, bool)."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-TYPE-001')

    result = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-type-001',
        'sku': 'AJ1-TYPE-001',
    })

    order, allocs, created = result
    assert isinstance(order, dict)
    assert isinstance(allocs, list)
    assert isinstance(created, bool)


# ---------------------------------------------------------------------------
# Tests 13–14: validation errors
# ---------------------------------------------------------------------------

def test_missing_platform_raises(db):
    with pytest.raises(ValueError, match="platform"):
        allocate_order(db, parsed_sale={'message_id': 'msg-x'})


def test_missing_message_id_raises(db):
    with pytest.raises(ValueError, match="message_id"):
        allocate_order(db, parsed_sale={'platform': 'ebay'})


# ---------------------------------------------------------------------------
# Test 15: buyer_name stored
# ---------------------------------------------------------------------------

def test_buyer_name_stored(db):
    """buyer_name is persisted in the order row."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-BUYER-001')

    order, _, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-buyer-001',
        'sku': 'AJ1-BUYER-001',
        'buyer_name': 'Jane Doe',
    })

    assert order['buyer_name'] == 'Jane Doe'


# ---------------------------------------------------------------------------
# Test 16: already-sold unit excluded from SKU match
# ---------------------------------------------------------------------------

def test_already_sold_unit_excluded(db):
    """A unit with status='sold' is skipped; order goes to needs_reconciliation."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-ALRSLD-001', status='sold')

    order, allocs, created = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-alrsld-001',
        'sku': 'AJ1-ALRSLD-001',
    })

    assert order['needs_reconciliation'] is True
    assert allocs == []


# ---------------------------------------------------------------------------
# Test 17: bundle — two units, same order (simulate via two allocs)
# ---------------------------------------------------------------------------

def test_idempotent_call_does_not_duplicate_allocations(db):
    """Idempotent re-call returns the same allocations, not duplicates."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-DUP-001')

    _, allocs1, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay', 'message_id': 'msg-dup-001',
        'order_id': 'ORD-DUP', 'sku': 'AJ1-DUP-001',
    })
    _, allocs2, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay', 'message_id': 'msg-dup-001',
        'order_id': 'ORD-DUP', 'sku': 'AJ1-DUP-001',
    })

    # Both calls return the same single allocation
    assert len(allocs1) == 1
    assert len(allocs2) == 1
    assert str(allocs1[0]['id']) == str(allocs2[0]['id'])


# ---------------------------------------------------------------------------
# Test 18: ship endpoint integration (order status flow)
# ---------------------------------------------------------------------------

def test_order_status_transitions(db):
    """Order moves: pending → allocated after allocate_order."""
    product = _make_product(db)
    _make_unit(db, product['id'], 'AJ1-STATUS-001')

    # Before allocation the order doesn't exist yet — verify status after
    order, _, created = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-status-001',
        'sku': 'AJ1-STATUS-001',
        'price': 110.0,
    })

    assert created is True
    assert order['status'] == 'allocated'
    assert order['allocated_at'] is not None


# ---------------------------------------------------------------------------
# Tests 19-20: listing lifecycle on sale (EPIC 5 Ticket 5.2)
# ---------------------------------------------------------------------------

def test_allocate_order_closes_single_quantity_listing(db):
    """After allocation, a single_quantity listing's status becomes 'sold'."""
    product = _make_product(db)
    channel = _make_channel(db)
    unit = _make_unit(db, product['id'], 'AJ1-SQ-CLOSE-001')
    listing = _make_listing(db, product['id'], channel['id'])
    _attach_unit(db, listing['id'], unit['id'])

    allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-sq-close-001',
        'sku': 'AJ1-SQ-CLOSE-001',
        'price': 100.0,
    })

    with db.cursor() as cur:
        cur.execute("SELECT status FROM listings WHERE id = %s", [listing['id']])
        assert cur.fetchone()[0] == 'sold'


def test_allocate_order_keeps_multi_quantity_listing_active_when_units_remain(db):
    """Allocating one unit of a 2-unit multi_quantity listing leaves it 'active'."""
    product = _make_product(db)
    channel = _make_channel(db)
    u1 = _make_unit(db, product['id'], 'AJ1-MQ-LIVE-001')
    u2 = _make_unit(db, product['id'], 'AJ1-MQ-LIVE-002')
    listing = _make_multi_listing(db, product['id'], channel['id'])
    _attach_unit(db, listing['id'], u1['id'])
    _attach_unit(db, listing['id'], u2['id'])

    allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'msg-mq-live-001',
        'sku': 'AJ1-MQ-LIVE-001',
        'price': 100.0,
    })

    with db.cursor() as cur:
        cur.execute("SELECT status FROM listings WHERE id = %s", [listing['id']])
        assert cur.fetchone()[0] == 'active'
