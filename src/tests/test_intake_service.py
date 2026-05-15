"""Unit tests for src.services.intake_service (EPIC 2 Tickets 2.2 and 2.3).

Uses a real Postgres connection with a per-test transaction rollback so
no data persists between tests.  Requires postgres to be running.
"""
import pytest
import psycopg2.extras

from src.services.intake_service import register_unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nike_new(db, unit_code="SHOE-001", **kwargs):
    return register_unit(
        db,
        brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="NEW",
        unit_code=unit_code, **kwargs,
    )


def _nike_excellent(db, unit_code="SHOE-001", **kwargs):
    return register_unit(
        db,
        brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="EXCELLENT",
        unit_code=unit_code, **kwargs,
    )


def _count_units(db, unit_code):
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM units WHERE unit_code = %s", [unit_code])
        return cur.fetchone()[0]


def _count_all_units(db):
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM units")
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Tests 1-3: Happy path + basic idempotency
# ---------------------------------------------------------------------------

def test_creates_unit_and_product(db):
    unit, product, unit_created, product_created = _nike_new(db)
    assert unit_created is True
    assert product_created is True
    assert unit["unit_code"] == "SHOE-001"
    assert unit["status"] == "ready_to_list"


def test_same_unit_code_returns_existing(db):
    u1, _, _, _ = _nike_new(db, unit_code="SHOE-001")
    u2, _, unit_created, product_created = _nike_new(db, unit_code="SHOE-001")
    assert unit_created is False
    assert product_created is False
    assert str(u1["id"]) == str(u2["id"])


def test_only_one_unit_row_after_duplicate(db):
    _nike_new(db, unit_code="SHOE-001")
    _nike_new(db, unit_code="SHOE-001")
    assert _count_units(db, "SHOE-001") == 1


# ---------------------------------------------------------------------------
# Tests 4-5: Interchangeable — two unit_codes share one product
# ---------------------------------------------------------------------------

def test_new_two_units_share_one_product(db):
    _, p1, _, _ = _nike_new(db, unit_code="SHOE-001")
    _, p2, _, p2_created = _nike_new(db, unit_code="SHOE-002")
    assert str(p1["id"]) == str(p2["id"])
    assert p2_created is False


def test_like_new_two_units_share_one_product(db):
    _, p1, _, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", unit_code="A",
    )
    _, p2, _, pc = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", unit_code="B",
    )
    assert str(p1["id"]) == str(p2["id"])
    assert pc is False


# ---------------------------------------------------------------------------
# Test 6: Non-interchangeable — two unit_codes get separate products
# ---------------------------------------------------------------------------

def test_excellent_two_units_get_separate_products(db):
    _, p1, _, pc1 = _nike_excellent(db, unit_code="SHOE-001")
    _, p2, _, pc2 = _nike_excellent(db, unit_code="SHOE-002")
    assert str(p1["id"]) != str(p2["id"])
    assert pc1 is True
    assert pc2 is True


# ---------------------------------------------------------------------------
# Tests 7-9: Solea legacy condition codes auto-mapped
# ---------------------------------------------------------------------------

def test_solea_new_with_box_maps_to_new(db):
    _, product, unit_created, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="new_with_box", unit_code="SHOE-001",
    )
    assert unit_created is True
    assert product["condition_code"] == "NEW"
    assert product["is_interchangeable"] is True


def test_solea_very_good_maps_to_excellent(db):
    _, product, _, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="very_good", unit_code="SHOE-001",
    )
    assert product["condition_code"] == "EXCELLENT"
    assert product["is_interchangeable"] is False


def test_solea_good_maps_to_good(db):
    _, product, _, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="good", unit_code="SHOE-001",
    )
    assert product["condition_code"] == "GOOD"


# ---------------------------------------------------------------------------
# Tests 10-12: Location resolution
# ---------------------------------------------------------------------------

def test_location_resolved_by_code(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO locations (id, code, description, is_active) "
            "VALUES (gen_random_uuid(), %s, %s, TRUE) RETURNING id",
            ["A1-01-06-03", "Shelf A1"]
        )
        loc_id = str(cur.fetchone()["id"])

    unit, _, _, _ = _nike_new(db, location_code="A1-01-06-03")
    assert str(unit["location_id"]) == loc_id


def test_missing_location_code_silently_ignored(db):
    unit, _, _, _ = _nike_new(db, location_code="NONEXISTENT")
    assert unit["location_id"] is None


def test_no_location_code_sets_null(db):
    unit, _, _, _ = _nike_new(db)
    assert unit["location_id"] is None


# ---------------------------------------------------------------------------
# Tests 13-15: Validation errors
# ---------------------------------------------------------------------------

def test_empty_unit_code_raises(db):
    with pytest.raises(ValueError, match="unit_code must not be empty"):
        register_unit(
            db, brand="Nike", model="Air Jordan 1", style_code="555088",
            gender="Men", size="10", condition="NEW", unit_code="",
        )


def test_whitespace_unit_code_raises(db):
    with pytest.raises(ValueError, match="unit_code must not be empty"):
        register_unit(
            db, brand="Nike", model="Air Jordan 1", style_code="555088",
            gender="Men", size="10", condition="NEW", unit_code="   ",
        )


def test_unknown_condition_raises(db):
    with pytest.raises(ValueError):
        register_unit(
            db, brand="Nike", model="Air Jordan 1", style_code="555088",
            gender="Men", size="10", condition="PERFECT", unit_code="SHOE-001",
        )


# ---------------------------------------------------------------------------
# Test 16: Return tuple structure
# ---------------------------------------------------------------------------

def test_return_tuple_structure(db):
    unit, product, unit_created, product_created = _nike_new(db)
    assert isinstance(unit, dict)
    assert isinstance(product, dict)
    assert unit_created is True
    assert product_created is True


# ---------------------------------------------------------------------------
# Tests 17-22: Batch intake (service-layer simulation)
# ---------------------------------------------------------------------------

def _batch(db, items):
    results = []
    units_created = units_skipped = products_created = products_reused = 0
    for item in items:
        unit, product, uc, pc = register_unit(db, **item)
        if uc:
            units_created += 1
        else:
            units_skipped += 1
        if pc:
            products_created += 1
        else:
            products_reused += 1
        results.append({"unit": unit, "product": product, "unit_created": uc, "product_created": pc})
    return {
        "total": len(items),
        "units_created": units_created,
        "units_skipped": units_skipped,
        "products_created": products_created,
        "products_reused": products_reused,
        "items": results,
    }


def test_batch_three_new_units(db):
    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="A"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="B"),
        dict(brand="Adidas", model="Yeezy", style_code="CP9654", gender="Men", size="9", condition="NEW", unit_code="C"),
    ]
    summary = _batch(db, items)
    assert summary["total"] == 3
    assert summary["units_created"] == 3
    assert summary["units_skipped"] == 0


def test_batch_two_new_units_same_style_share_product(db):
    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="A"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="B"),
    ]
    summary = _batch(db, items)
    assert summary["products_created"] == 1
    assert summary["products_reused"] == 1
    assert str(summary["items"][0]["product"]["id"]) == str(summary["items"][1]["product"]["id"])


def test_batch_with_preexisting_unit_code_skipped(db):
    _nike_new(db, unit_code="SHOE-001")
    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="SHOE-001"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="SHOE-002"),
    ]
    summary = _batch(db, items)
    assert summary["units_skipped"] == 1
    assert summary["units_created"] == 1


def test_batch_empty_list_produces_no_rows(db):
    before = _count_all_units(db)
    summary = _batch(db, [])
    assert summary["total"] == 0
    assert _count_all_units(db) == before


def test_batch_invalid_condition_raises_value_error(db):
    with pytest.raises(ValueError):
        _batch(db, [
            dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10",
                 condition="PERFECT", unit_code="SHOE-001"),
        ])


def test_batch_unit_linked_to_correct_product(db):
    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10",
             condition="EXCELLENT", unit_code="E-001"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10",
             condition="EXCELLENT", unit_code="E-002"),
    ]
    summary = _batch(db, items)
    assert summary["products_created"] == 2
    assert str(summary["items"][0]["product"]["id"]) != str(summary["items"][1]["product"]["id"])
    for item in summary["items"]:
        assert str(item["unit"]["product_id"]) == str(item["product"]["id"])
