"""
EPIC 10 Ticket 10.1 — Core inventory scenario tests.

Validates the five fundamental business rules end-to-end:
  1. Multi-SKU grouping works for NEW (interchangeable)
  2. Multi-SKU grouping works for LIKE_NEW / E1 (interchangeable)
  3. EXCELLENT / E2 does NOT group — each unit requires its own listing
  4. Sale reduces the quantity of available units correctly
  5. Last SKU sold triggers auto-delist of the listing
  6. A SKU cannot be double-sold

Uses real Postgres with per-test transaction rollback (db fixture from conftest.py).
"""
import pytest
import psycopg2.extras

from src.services.listing_service import (
    create_listing,
    assign_unit_to_listing,
    update_listing_on_unit_sold,
)
from src.services.order_allocation_service import allocate_order


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product(db, condition_code='NEW', is_interchangeable=True, suffix=''):
    """Insert a product with a unique product_id to avoid UNIQUE conflicts."""
    pid = f"NIKE-AJ1-SCEN-MEN-10-{condition_code}{suffix}"
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), %s, 'Nike', 'Air Jordan 1', 'SCEN', 'Men', '10', %s, %s)
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


def _count_listing_units(db, listing_id):
    """Return the number of units attached to a listing via listing_units."""
    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM listing_units WHERE listing_id = %s",
            [listing_id],
        )
        return cur.fetchone()[0]


def _get_listing_status(db, listing_id):
    """Return the current status string for a listing."""
    with db.cursor() as cur:
        cur.execute("SELECT status FROM listings WHERE id = %s", [listing_id])
        row = cur.fetchone()
    return row[0] if row else None


def _get_unit_status(db, unit_id):
    with db.cursor() as cur:
        cur.execute("SELECT status FROM units WHERE id = %s", [unit_id])
        return cur.fetchone()[0]


def _count_allocations_for_unit(db, unit_id):
    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM order_allocations WHERE unit_id = %s",
            [unit_id],
        )
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Scenario 1 — Multi-SKU grouping works for NEW
# ---------------------------------------------------------------------------

def test_new_three_skus_share_one_multi_quantity_listing(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='NEW', is_interchangeable=True)

    listing, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Size 10',
        price=200.0,
        quantity=3,
        status='active',
    )

    assert listing['mode'] == 'multi_quantity'

    unit1 = _make_unit(db, product['id'], 'SCEN-NEW-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-NEW-002')
    unit3 = _make_unit(db, product['id'], 'SCEN-NEW-003')

    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit1['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit2['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit3['id']))

    assert _count_listing_units(db, listing['id']) == 3
    assert _get_listing_status(db, listing['id']) == 'active'


# ---------------------------------------------------------------------------
# Scenario 2 — Multi-SKU grouping works for E1 (LIKE_NEW)
# ---------------------------------------------------------------------------

def test_like_new_skus_share_one_multi_quantity_listing(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='LIKE_NEW', is_interchangeable=True)

    listing, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Like New Size 10',
        price=180.0,
        quantity=2,
        status='active',
    )

    assert listing['mode'] == 'multi_quantity'

    unit1 = _make_unit(db, product['id'], 'SCEN-LN-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-LN-002')

    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit1['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit2['id']))

    assert _count_listing_units(db, listing['id']) == 2
    assert _get_listing_status(db, listing['id']) == 'active'


# ---------------------------------------------------------------------------
# Scenario 3 — E2 (EXCELLENT) does NOT group
# ---------------------------------------------------------------------------

def test_excellent_sku_cannot_be_added_to_existing_listing(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='EXCELLENT', is_interchangeable=False)

    listing, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Excellent Size 10',
        price=150.0,
        status='active',
    )

    assert listing['mode'] == 'single_quantity'

    unit1 = _make_unit(db, product['id'], 'SCEN-EXC-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-EXC-002')

    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit1['id']))

    with pytest.raises(ValueError):
        assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit2['id']))


def test_excellent_units_each_require_own_listing(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='EXCELLENT', is_interchangeable=False, suffix='-B')

    listing_a, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Excellent A',
        price=150.0,
        status='active',
    )
    listing_b, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Excellent B',
        price=140.0,
        status='active',
    )

    unit1 = _make_unit(db, product['id'], 'SCEN-EXCB-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-EXCB-002')

    assign_unit_to_listing(db, listing_id=str(listing_a['id']), unit_id=str(unit1['id']))
    assign_unit_to_listing(db, listing_id=str(listing_b['id']), unit_id=str(unit2['id']))

    assert _count_listing_units(db, listing_a['id']) == 1
    assert _count_listing_units(db, listing_b['id']) == 1


# ---------------------------------------------------------------------------
# Scenario 4 — Sale reduces quantity correctly
# ---------------------------------------------------------------------------

def test_sale_of_one_unit_leaves_remaining_units_listed(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='NEW', is_interchangeable=True, suffix='-Q')

    listing, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Qty Test',
        price=200.0,
        quantity=3,
        status='active',
    )

    unit1 = _make_unit(db, product['id'], 'SCEN-QTY-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-QTY-002')
    unit3 = _make_unit(db, product['id'], 'SCEN-QTY-003')

    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit1['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit2['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit3['id']))

    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [unit1['id']])
    update_listing_on_unit_sold(db, unit_id=str(unit1['id']))

    assert _get_listing_status(db, listing['id']) == 'active'
    assert _count_listing_units(db, listing['id']) == 3

    assert _get_unit_status(db, unit1['id']) == 'sold'
    assert _get_unit_status(db, unit2['id']) == 'listed'
    assert _get_unit_status(db, unit3['id']) == 'listed'


# ---------------------------------------------------------------------------
# Scenario 5 — Last SKU triggers delist
# ---------------------------------------------------------------------------

def test_selling_last_sku_ends_listing_via_service(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='NEW', is_interchangeable=True, suffix='-DL')

    listing, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 Delist Test',
        price=200.0,
        quantity=2,
        status='active',
    )

    unit1 = _make_unit(db, product['id'], 'SCEN-DL-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-DL-002')

    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit1['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit2['id']))

    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [unit1['id']])
    update_listing_on_unit_sold(db, unit_id=str(unit1['id']))
    assert _get_listing_status(db, listing['id']) == 'active'

    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [unit2['id']])
    update_listing_on_unit_sold(db, unit_id=str(unit2['id']))
    assert _get_listing_status(db, listing['id']) == 'ended'


def test_selling_last_sku_ends_listing_via_allocate_order(db):
    _make_channel(db, 'ebay', supports_multi=True)
    product = _make_product(db, condition_code='NEW', is_interchangeable=True, suffix='-E2E')

    listing, _ = create_listing(
        db,
        product_id=str(product['id']),
        channel_name='ebay',
        title='AJ1 E2E Delist Test',
        price=200.0,
        quantity=2,
        status='active',
    )

    unit1 = _make_unit(db, product['id'], 'SCEN-E2E-001')
    unit2 = _make_unit(db, product['id'], 'SCEN-E2E-002')

    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit1['id']))
    assign_unit_to_listing(db, listing_id=str(listing['id']), unit_id=str(unit2['id']))

    order1, _, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-e2e-msg-001',
        'order_id': 'SCEN-E2E-ORD-001',
        'sku': 'SCEN-E2E-001',
        'price': 200.0,
    })
    assert order1['status'] == 'allocated'
    assert _get_listing_status(db, listing['id']) == 'active'

    order2, _, _ = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-e2e-msg-002',
        'order_id': 'SCEN-E2E-ORD-002',
        'sku': 'SCEN-E2E-002',
        'price': 200.0,
    })
    assert order2['status'] == 'allocated'
    assert _get_listing_status(db, listing['id']) == 'ended'


# ---------------------------------------------------------------------------
# Scenario 6 — SKU cannot be double-sold
# ---------------------------------------------------------------------------

def test_double_sell_second_order_needs_reconciliation(db):
    product = _make_product(db, suffix='-DS')
    unit = _make_unit(db, product['id'], 'SCEN-DS-001')

    order1, _, created1 = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-ds-msg-001',
        'order_id': 'SCEN-DS-ORD-001',
        'sku': 'SCEN-DS-001',
        'price': 130.0,
    })
    assert created1 is True
    assert order1['status'] == 'allocated'

    order2, _, created2 = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-ds-msg-002',
        'order_id': 'SCEN-DS-ORD-002',
        'sku': 'SCEN-DS-001',
        'price': 130.0,
    })
    assert created2 is True
    assert order2['status'] == 'needs_reconciliation'

    assert _count_allocations_for_unit(db, unit['id']) == 1


def test_double_sell_unit_status_unchanged(db):
    product = _make_product(db, suffix='-DSU')
    unit = _make_unit(db, product['id'], 'SCEN-DSU-001')

    allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-dsu-msg-001',
        'order_id': 'SCEN-DSU-ORD-001',
        'sku': 'SCEN-DSU-001',
        'price': 130.0,
    })
    allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-dsu-msg-002',
        'order_id': 'SCEN-DSU-ORD-002',
        'sku': 'SCEN-DSU-001',
        'price': 130.0,
    })

    assert _get_unit_status(db, unit['id']) == 'sold'


def test_double_sell_idempotent_for_same_order_id(db):
    product = _make_product(db, suffix='-IDM')
    _make_unit(db, product['id'], 'SCEN-IDM-001')

    order1, _, created1 = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-idm-msg-001',
        'order_id': 'SCEN-IDM-ORD-001',
        'sku': 'SCEN-IDM-001',
        'price': 130.0,
    })
    assert created1 is True
    assert order1['status'] == 'allocated'

    order2, _, created2 = allocate_order(db, parsed_sale={
        'platform': 'ebay',
        'message_id': 'scen-idm-msg-001',
        'order_id': 'SCEN-IDM-ORD-001',
        'sku': 'SCEN-IDM-001',
        'price': 130.0,
    })
    assert created2 is False
    assert order2['id'] == order1['id']
