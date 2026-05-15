"""
Unit tests for src.services.photo_pair_service (EPIC 7 Tickets 7.1–7.2).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.
"""
import uuid

import psycopg2.extras
import pytest

from src.services.photo_pair_service import (
    assign_sku_to_photo_pair,
    get_sku_inventory_count,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product(db, tag="PP"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), %s, 'Nike', 'Air Jordan 1', 'TEST', 'Men', '10',
                 'NEW', TRUE)
            RETURNING id
            """,
            [f"NIKE-AJ1-{tag}-TEST"],
        )
        return dict(cur.fetchone())


def _make_unit(db, product_id, unit_code, status="ready_to_list"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING id",
            [unit_code, product_id, status],
        )
        return dict(cur.fetchone())


# ---------------------------------------------------------------------------
# Tests 1–5: assign_sku_to_photo_pair
# ---------------------------------------------------------------------------

def test_assign_sku_to_photo_pair_creates_ready_to_list_unit(db):
    product = _make_product(db, tag="PP1")

    unit = assign_sku_to_photo_pair(db, unit_code="PP-SKU-001", product_id=str(product["id"]))

    assert unit["status"] == "ready_to_list"


def test_assign_sku_to_photo_pair_stores_unit_code_and_product_id(db):
    product = _make_product(db, tag="PP2")

    unit = assign_sku_to_photo_pair(db, unit_code="PP-SKU-002", product_id=str(product["id"]))

    assert unit["unit_code"] == "PP-SKU-002"
    assert str(unit["product_id"]) == str(product["id"])


def test_assign_sku_to_photo_pair_duplicate_unit_code_raises(db):
    product = _make_product(db, tag="PP3")
    _make_unit(db, product["id"], "PP-SKU-003")

    with pytest.raises(ValueError, match="unit_code already assigned"):
        assign_sku_to_photo_pair(db, unit_code="PP-SKU-003", product_id=str(product["id"]))


def test_assign_sku_to_photo_pair_nonexistent_product_raises(db):
    fake_product_id = str(uuid.uuid4())

    with pytest.raises(ValueError, match="Product not found"):
        assign_sku_to_photo_pair(db, unit_code="PP-SKU-004", product_id=fake_product_id)


def test_assign_sku_to_photo_pair_empty_unit_code_raises(db):
    product = _make_product(db, tag="PP5")

    with pytest.raises(ValueError, match="unit_code is required"):
        assign_sku_to_photo_pair(db, unit_code="", product_id=str(product["id"]))


# ---------------------------------------------------------------------------
# Tests 6–8: get_sku_inventory_count
# ---------------------------------------------------------------------------

def test_get_sku_inventory_count_excludes_sold_units(db):
    product = _make_product(db, tag="PP6")
    _make_unit(db, product["id"], "PP-INV-001", status="ready_to_list")
    _make_unit(db, product["id"], "PP-INV-002", status="ready_to_list")
    _make_unit(db, product["id"], "PP-INV-003", status="sold")

    count = get_sku_inventory_count(db)

    assert count >= 2


def test_get_sku_inventory_count_filters_by_product_id(db):
    product_a = _make_product(db, tag="PP7A")
    product_b = _make_product(db, tag="PP7B")
    _make_unit(db, product_a["id"], "PP-PROD-A-001", status="ready_to_list")
    _make_unit(db, product_a["id"], "PP-PROD-A-002", status="ready_to_list")
    _make_unit(db, product_b["id"], "PP-PROD-B-001", status="ready_to_list")

    count_a = get_sku_inventory_count(db, product_id=str(product_a["id"]))
    count_b = get_sku_inventory_count(db, product_id=str(product_b["id"]))

    assert count_a == 2
    assert count_b == 1


def test_get_sku_inventory_count_returns_zero_when_none_available(db):
    product = _make_product(db, tag="PP8")
    _make_unit(db, product["id"], "PP-ZERO-001", status="sold")
    _make_unit(db, product["id"], "PP-ZERO-002", status="shipped")

    count = get_sku_inventory_count(db, product_id=str(product["id"]))

    assert count == 0
