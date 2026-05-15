"""
Unit tests for src.services.admin_service (EPIC 8 Tickets 8.1–8.2).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.
"""
import pytest
import psycopg2.extras

from src.services.admin_service import get_product_detail, get_sku_list


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product(db, condition_code="NEW", is_interchangeable=True, suffix=""):
    pid = f"NIKE-AJ1-555088-MEN-10-{condition_code}{suffix}"
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), %s, 'Nike', 'Air Jordan 1', '555088', 'Men', '10',
                 %s, %s)
            RETURNING *
            """,
            [pid, condition_code, is_interchangeable],
        )
        return dict(cur.fetchone())


def _make_unit(db, product_id, unit_code, status="ready_to_list", location_id=None):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status, location_id) "
            "VALUES (gen_random_uuid(), %s, %s, %s, %s) RETURNING *",
            [unit_code, product_id, status, location_id],
        )
        return dict(cur.fetchone())


def _make_location(db, code="A1-01-06-03"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO locations (id, code, description, is_active) "
            "VALUES (gen_random_uuid(), %s, 'Test Shelf', TRUE) RETURNING *",
            [code],
        )
        return dict(cur.fetchone())


def _make_listing(db, product_id, channel_name="test_admin_ch", status="active"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES (%s, %s, TRUE, FALSE)
            ON CONFLICT (name) DO UPDATE SET is_active = TRUE
            RETURNING id
            """,
            [channel_name, channel_name],
        )
        channel_id = cur.fetchone()["id"]

        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, current_price, status, mode)
            VALUES
                (gen_random_uuid(), %s, %s, 'Test Listing', 100.0, %s, 'single_quantity')
            RETURNING *
            """,
            [product_id, channel_id, status],
        )
        return dict(cur.fetchone())


# ---------------------------------------------------------------------------
# Tests 1–6: get_product_detail
# ---------------------------------------------------------------------------

def test_get_product_detail_returns_all_required_keys(db):
    product = _make_product(db)

    result = get_product_detail(db, product_id=str(product["id"]))

    for key in ("id", "product_id", "brand", "model", "size", "condition_code",
                "is_interchangeable", "total_quantity", "available_skus",
                "reserved_skus", "sold_skus", "active_listings"):
        assert key in result, f"Missing key: {key}"

    assert result["total_quantity"] == 0
    assert result["available_skus"] == 0
    assert result["reserved_skus"] == 0
    assert result["sold_skus"] == 0
    assert result["active_listings"] == 0


def test_get_product_detail_counts_units_by_status(db):
    product = _make_product(db)
    _make_unit(db, product["id"], "AD-RTL-001", status="ready_to_list")
    _make_unit(db, product["id"], "AD-RTL-002", status="ready_to_list")
    _make_unit(db, product["id"], "AD-RTL-003", status="ready_to_list")
    _make_unit(db, product["id"], "AD-RES-001", status="reserved")
    _make_unit(db, product["id"], "AD-SLD-001", status="sold")
    _make_unit(db, product["id"], "AD-SLD-002", status="sold")
    _make_unit(db, product["id"], "AD-LST-001", status="listed")

    result = get_product_detail(db, product_id=str(product["id"]))

    assert result["total_quantity"] == 7
    assert result["available_skus"] == 3
    assert result["reserved_skus"] == 1
    assert result["sold_skus"] == 2
    assert result["active_listings"] == 0


def test_get_product_detail_counts_only_active_listings(db):
    product = _make_product(db)
    _make_listing(db, product["id"], channel_name="admin_ch_active", status="active")
    _make_listing(db, product["id"], channel_name="admin_ch_active2", status="active")
    _make_listing(db, product["id"], channel_name="admin_ch_sold", status="sold")
    _make_listing(db, product["id"], channel_name="admin_ch_ended", status="ended")

    result = get_product_detail(db, product_id=str(product["id"]))

    assert result["active_listings"] == 2


def test_get_product_detail_not_found_raises(db):
    with pytest.raises(ValueError, match="Product not found"):
        get_product_detail(db, product_id="00000000-0000-0000-0000-000000000000")


def test_get_product_detail_returns_canonical_product_id_string(db):
    product = _make_product(db, suffix="-CANONICAL")

    result = get_product_detail(db, product_id=str(product["id"]))

    assert result["product_id"] == "NIKE-AJ1-555088-MEN-10-NEW-CANONICAL"
    assert result["id"] == str(product["id"])
    assert result["product_id"] != result["id"]


def test_get_product_detail_is_interchangeable_is_bool(db):
    product_true = _make_product(db, is_interchangeable=True, suffix="-T")
    product_false = _make_product(db, is_interchangeable=False, suffix="-F")

    result_true = get_product_detail(db, product_id=str(product_true["id"]))
    result_false = get_product_detail(db, product_id=str(product_false["id"]))

    assert result_true["is_interchangeable"] is True
    assert result_false["is_interchangeable"] is False


# ---------------------------------------------------------------------------
# Tests 7–11: get_sku_list
# ---------------------------------------------------------------------------

def test_get_sku_list_returns_all_units_with_correct_fields(db):
    product = _make_product(db)
    _make_unit(db, product["id"], "SL-ALL-001", status="ready_to_list")
    _make_unit(db, product["id"], "SL-ALL-002", status="sold")

    result = get_sku_list(db)

    unit_codes = [r["unit_code"] for r in result]
    assert "SL-ALL-001" in unit_codes
    assert "SL-ALL-002" in unit_codes

    row = next(r for r in result if r["unit_code"] == "SL-ALL-001")
    for key in ("unit_code", "product_uuid", "product_id", "status", "location_code"):
        assert key in row, f"Missing key: {key}"


def test_get_sku_list_filters_by_status(db):
    product = _make_product(db)
    _make_unit(db, product["id"], "SL-FLT-001", status="ready_to_list")
    _make_unit(db, product["id"], "SL-FLT-002", status="ready_to_list")
    _make_unit(db, product["id"], "SL-FLT-003", status="sold")

    result = get_sku_list(db, status="ready_to_list")

    returned_codes = [r["unit_code"] for r in result]
    assert "SL-FLT-001" in returned_codes
    assert "SL-FLT-002" in returned_codes
    assert "SL-FLT-003" not in returned_codes
    assert all(r["status"] == "ready_to_list" for r in result
               if r["unit_code"].startswith("SL-FLT"))


def test_get_sku_list_ordered_by_unit_code(db):
    product = _make_product(db)
    _make_unit(db, product["id"], "ZZZ-ORD-001")
    _make_unit(db, product["id"], "AAA-ORD-001")
    _make_unit(db, product["id"], "MMM-ORD-001")

    result = get_sku_list(db)

    codes = [r["unit_code"] for r in result if r["unit_code"].endswith("-ORD-001")]
    assert codes == sorted(codes)


def test_get_sku_list_location_code_is_none_when_no_location(db):
    product = _make_product(db)
    _make_unit(db, product["id"], "SL-LOC-NONE")

    result = get_sku_list(db)

    row = next(r for r in result if r["unit_code"] == "SL-LOC-NONE")
    assert row["location_code"] is None


def test_get_sku_list_location_code_and_product_uuid_populated(db):
    product = _make_product(db)
    location = _make_location(db, code="B2-03-07-01")
    _make_unit(db, product["id"], "SL-LOC-SET", location_id=location["id"])

    result = get_sku_list(db)

    row = next(r for r in result if r["unit_code"] == "SL-LOC-SET")
    assert row["location_code"] == "B2-03-07-01"
    assert row["product_uuid"] == str(product["id"])
    assert row["product_id"] == "NIKE-AJ1-555088-MEN-10-NEW"
    assert row["product_uuid"] != row["product_id"]
