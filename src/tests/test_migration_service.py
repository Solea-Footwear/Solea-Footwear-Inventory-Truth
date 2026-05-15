"""
Unit tests for src.services.migration_service (EPIC 9 Tickets 9.1–9.2).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.
"""
import pytest
import psycopg2.extras

from src.services.migration_service import backfill_product_ids, get_exception_report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product_no_pid(db, condition_code="NEW", style_code="MGT001",
                          is_interchangeable=True, size="10",
                          brand="MigTest", model="BackfillShoe"):
    """Insert a product with product_id=NULL (simulates pre-EPIC-1 data).

    Uses a test-specific brand/style_code unlikely to exist in the live DB to
    avoid UNIQUE constraint collisions on products.product_id.
    """
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, brand, model, style_code, gender, size, condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), %s, %s, %s, 'Men', %s, %s, %s)
            RETURNING *
            """,
            [brand, model, style_code, size, condition_code, is_interchangeable],
        )
        return dict(cur.fetchone())


def _make_product_with_pid(db, suffix=""):
    """Insert a product that already has a product_id set."""
    pid = f"NIKE-AIRJORDAN1-555088-MEN-10-NEW{suffix}"
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), %s, 'Nike', 'Air Jordan 1', '555088', 'Men', '10',
                 'NEW', TRUE)
            RETURNING *
            """,
            [pid],
        )
        return dict(cur.fetchone())


def _make_unit(db, product_id, unit_code, status="ready_to_list"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING *",
            [unit_code, product_id, status],
        )
        return dict(cur.fetchone())


def _fetch_product_id(db, product_uuid):
    """Re-fetch the products.product_id column for the given UUID."""
    with db.cursor() as cur:
        cur.execute("SELECT product_id FROM products WHERE id = %s", [product_uuid])
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Tests 1–7: backfill_product_ids
# ---------------------------------------------------------------------------

def test_backfill_interchangeable_product_gets_product_id(db):
    product = _make_product_no_pid(db, condition_code="NEW", is_interchangeable=True)

    result = backfill_product_ids(db)

    assert result["backfilled"] >= 1
    saved_pid = _fetch_product_id(db, product["id"])
    assert saved_pid == "MIGTEST-BACKFILLSHOE-MGT001-MEN-10-NEW"


def test_backfill_non_interchangeable_uses_unit_code(db):
    product = _make_product_no_pid(db, condition_code="EXCELLENT", is_interchangeable=False)
    _make_unit(db, product["id"], "MIG-EXC-001")

    result = backfill_product_ids(db)

    assert result["backfilled"] >= 1
    saved_pid = _fetch_product_id(db, product["id"])
    assert saved_pid == "MIGTEST-BACKFILLSHOE-MGT001-MEN-10-EXCELLENT-MIGEXC001"


def test_backfill_skips_existing_product_id(db):
    product = _make_product_with_pid(db, suffix="-SKIP")

    result = backfill_product_ids(db)

    assert result["backfilled"] == 0
    saved_pid = _fetch_product_id(db, product["id"])
    assert saved_pid == "NIKE-AIRJORDAN1-555088-MEN-10-NEW-SKIP"


def test_backfill_skips_missing_style_code(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, brand, model, style_code, gender, size, condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), 'Nike', 'Air Jordan 1', NULL, 'Men', '10', 'NEW', TRUE)
            RETURNING *
            """,
        )
        product = dict(cur.fetchone())

    result = backfill_product_ids(db)

    assert result["skipped_incomplete"] >= 1
    assert _fetch_product_id(db, product["id"]) is None


def test_backfill_skips_missing_condition_code(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, brand, model, style_code, gender, size, condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), 'Nike', 'Air Jordan 1', '555088', 'Men', '10', NULL, TRUE)
            RETURNING *
            """,
        )
        product = dict(cur.fetchone())

    result = backfill_product_ids(db)

    assert result["skipped_incomplete"] >= 1
    assert _fetch_product_id(db, product["id"]) is None


def test_backfill_skips_non_interchangeable_with_no_unit(db):
    product = _make_product_no_pid(db, condition_code="GOOD", is_interchangeable=False)

    result = backfill_product_ids(db)

    assert result["skipped_no_unit"] >= 1
    assert _fetch_product_id(db, product["id"]) is None


def test_backfill_returns_correct_counts_for_mixed_set(db):
    # 1 backfillable interchangeable product
    _make_product_no_pid(db, condition_code="NEW", is_interchangeable=True)
    # 1 incomplete (missing style_code)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO products (id, brand, model, style_code, gender, size, "
            "condition_code, is_interchangeable) "
            "VALUES (gen_random_uuid(), 'Nike', 'React', NULL, 'Men', '9', 'NEW', TRUE)"
        )
    # 1 non-interchangeable with no unit
    _make_product_no_pid(db, condition_code="FAIR", is_interchangeable=False)

    result = backfill_product_ids(db)

    assert result["backfilled"] == 1
    assert result["skipped_incomplete"] >= 1   # includes Nike React + any pre-existing DB rows
    assert result["skipped_no_unit"] >= 1      # at least the MigTest FAIR product
    assert result["skipped_conflict"] == 0


# ---------------------------------------------------------------------------
# Tests 8–12: get_exception_report
# ---------------------------------------------------------------------------

def test_exception_report_flags_missing_style_code(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO products (id, brand, model, style_code, gender, size, "
            "condition_code, is_interchangeable) "
            "VALUES (gen_random_uuid(), 'Nike', 'Air Max', NULL, 'Men', '11', 'NEW', TRUE) "
            "RETURNING *"
        )
        product = dict(cur.fetchone())
    _make_unit(db, product["id"], "EXC-SC-001")

    result = get_exception_report(db)

    codes = [r["unit_code"] for r in result]
    assert "EXC-SC-001" in codes
    row = next(r for r in result if r["unit_code"] == "EXC-SC-001")
    assert "style_code" in row["missing_fields"]


def test_exception_report_flags_missing_condition_code(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO products (id, brand, model, style_code, gender, size, "
            "condition_code, is_interchangeable) "
            "VALUES (gen_random_uuid(), 'Adidas', 'Yeezy', '700', 'Men', '10', NULL, TRUE) "
            "RETURNING *"
        )
        product = dict(cur.fetchone())
    _make_unit(db, product["id"], "EXC-CC-001")

    result = get_exception_report(db)

    codes = [r["unit_code"] for r in result]
    assert "EXC-CC-001" in codes
    row = next(r for r in result if r["unit_code"] == "EXC-CC-001")
    assert "condition_code" in row["missing_fields"]


def test_exception_report_excludes_complete_products(db):
    product = _make_product_no_pid(db, condition_code="NEW", style_code="555088",
                                    is_interchangeable=True)
    _make_unit(db, product["id"], "EXC-OK-001")

    result = get_exception_report(db)

    codes = [r["unit_code"] for r in result]
    assert "EXC-OK-001" not in codes


def test_exception_report_multiple_missing_fields(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO products (id, brand, model, style_code, gender, size, "
            "condition_code, is_interchangeable) "
            "VALUES (gen_random_uuid(), 'Puma', 'Suede', NULL, 'Men', '9', NULL, TRUE) "
            "RETURNING *"
        )
        product = dict(cur.fetchone())
    _make_unit(db, product["id"], "EXC-MF-001")

    result = get_exception_report(db)

    row = next(r for r in result if r["unit_code"] == "EXC-MF-001")
    assert "style_code" in row["missing_fields"]
    assert "condition_code" in row["missing_fields"]


def test_exception_report_returns_all_required_keys(db):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO products (id, brand, model, style_code, gender, size, "
            "condition_code, is_interchangeable) "
            "VALUES (gen_random_uuid(), 'Nike', 'Cortez', NULL, 'Women', '8', 'NEW', TRUE) "
            "RETURNING *"
        )
        product = dict(cur.fetchone())
    _make_unit(db, product["id"], "EXC-KEYS-001")

    result = get_exception_report(db)

    row = next(r for r in result if r["unit_code"] == "EXC-KEYS-001")
    for key in ("unit_code", "status", "product_uuid", "product_id",
                "brand", "model", "size", "style_code", "condition_code", "missing_fields"):
        assert key in row, f"Missing key: {key}"
    assert row["product_uuid"] == str(product["id"])
    assert isinstance(row["missing_fields"], list)
