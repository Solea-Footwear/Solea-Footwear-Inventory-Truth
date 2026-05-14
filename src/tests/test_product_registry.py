"""Unit tests for src.services.product_registry_service (EPIC 1 Ticket 1.3).

Uses a real Postgres connection with a per-test transaction rollback so
no data persists between tests.  Requires postgres to be running.
"""
import pytest
import psycopg2.extras

from src.services.product_registry_service import find_or_create_product
from src.services.product_id_service import map_solea_condition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nike(db, condition, sku=None):
    return find_or_create_product(
        db,
        brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition=condition, sku=sku,
    )


def _count_by_pid(db, product_id):
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM products WHERE product_id = %s", [product_id])
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Basic create
# ---------------------------------------------------------------------------

def test_first_call_creates_row(db):
    product, created = _nike(db, "NEW")
    assert created is True
    assert product["product_id"] == "NIKE-AIRJORDAN1-555088-MEN-10-NEW"


def test_created_row_has_correct_columns(db):
    p, _ = _nike(db, "NEW")
    assert p["brand"] == "Nike"
    assert p["model"] == "Air Jordan 1"
    assert p["style_code"] == "555088"
    assert p["gender"] == "Men"
    assert p["size"] == "10"
    assert p["condition_code"] == "NEW"
    assert p["is_interchangeable"] is True


def test_non_interchangeable_created_correctly(db):
    p, created = _nike(db, "EXCELLENT", sku="SKU-001")
    assert created is True
    assert p["is_interchangeable"] is False
    assert p["condition_code"] == "EXCELLENT"


# ---------------------------------------------------------------------------
# Idempotency — second call returns same row
# ---------------------------------------------------------------------------

def test_second_call_returns_existing_row(db):
    p1, c1 = _nike(db, "NEW")
    p2, c2 = _nike(db, "NEW")
    assert c1 is True
    assert c2 is False
    assert str(p1["id"]) == str(p2["id"])


def test_only_one_row_in_db_after_two_calls(db):
    _nike(db, "NEW")
    _nike(db, "NEW")
    assert _count_by_pid(db, "NIKE-AIRJORDAN1-555088-MEN-10-NEW") == 1


# ---------------------------------------------------------------------------
# Interchangeable: two SKUs → one product row
# ---------------------------------------------------------------------------

def test_new_two_skus_one_row(db):
    p1, c1 = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="NEW", sku="SKU-001",
    )
    p2, c2 = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="NEW", sku="SKU-002",
    )
    assert str(p1["id"]) == str(p2["id"])
    assert c2 is False


def test_like_new_two_skus_one_row(db):
    p1, _ = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", sku="A",
    )
    p2, c2 = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", sku="B",
    )
    assert str(p1["id"]) == str(p2["id"])
    assert c2 is False


# ---------------------------------------------------------------------------
# Non-interchangeable: two SKUs → two distinct rows
# ---------------------------------------------------------------------------

def test_excellent_two_skus_two_rows(db):
    p1, c1 = _nike(db, "EXCELLENT", sku="SKU-001")
    p2, c2 = _nike(db, "EXCELLENT", sku="SKU-002")
    assert c1 is True
    assert c2 is True
    assert str(p1["id"]) != str(p2["id"])


# ---------------------------------------------------------------------------
# Race condition — ON CONFLICT DO NOTHING path
# The service uses INSERT ... ON CONFLICT DO NOTHING RETURNING *.
# When the row already exists (concurrent insert), RETURNING yields nothing
# and the service falls back to a SELECT.  We test this by pre-inserting the
# row, then calling find_or_create_product — it must return created=False and
# the pre-existing row.
# ---------------------------------------------------------------------------

def test_conflict_path_returns_existing_row(db):
    pid = "NIKE-AIRJORDAN1-555088-MEN-10-NEW"
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO products (id, product_id, brand, model, style_code, gender, size, "
            "condition_code, is_interchangeable) "
            "VALUES (gen_random_uuid(), %s, 'Nike', 'Air Jordan 1', '555088', 'Men', '10', 'NEW', TRUE) "
            "RETURNING id",
            [pid],
        )
        pre_id = str(cur.fetchone()["id"])

    p, created = _nike(db, "NEW")
    assert created is False
    assert str(p["id"]) == pre_id
    assert p["product_id"] == pid


# ---------------------------------------------------------------------------
# Solea condition mapping integration
# ---------------------------------------------------------------------------

def test_solea_very_good_maps_to_excellent(db):
    condition = map_solea_condition("very_good")
    p, created = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition=condition, sku="SKU-001",
    )
    assert p["condition_code"] == "EXCELLENT"
    assert p["is_interchangeable"] is False
