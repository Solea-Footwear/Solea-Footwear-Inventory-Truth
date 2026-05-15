"""
Unit tests for module-level functions in src.services.sync_service (EPIC 6 Ticket 6.3).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.
"""
from datetime import datetime, timezone

import psycopg2.extras
import pytest

from src.services.sync_service import (
    apply_unit_sold_from_sync,
    create_sync_log,
    finish_sync_log,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product(db, tag="SS"):
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


def _make_unit(db, product_id, unit_code, status="listed"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING *",
            [unit_code, product_id, status],
        )
        return dict(cur.fetchone())


def _upsert_channel(db, name, supports_multi=True):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES (%s, %s, TRUE, %s)
            ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity
            RETURNING id
            """,
            [name, name.capitalize(), supports_multi],
        )
        return dict(cur.fetchone())


def _make_active_listing(db, product_id, channel_id):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, current_price, status, mode)
            VALUES
                (gen_random_uuid(), %s, %s, 'Test Listing', 100.0, 'active', 'single_quantity')
            RETURNING id
            """,
            [product_id, channel_id],
        )
        return dict(cur.fetchone())


def _attach_unit(db, listing_id, unit_id):
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) "
            "VALUES (gen_random_uuid(), %s, %s)",
            [listing_id, unit_id],
        )


# ---------------------------------------------------------------------------
# Tests 1–8
# ---------------------------------------------------------------------------

def test_apply_unit_sold_from_sync_updates_unit_fields(db):
    product = _make_product(db, tag="SY1")
    unit = _make_unit(db, product["id"], "SS-SOLD-001")
    sold_at = datetime(2026, 5, 1, 12, 0, 0)

    result = apply_unit_sold_from_sync(
        db, unit_code="SS-SOLD-001", platform="ebay", sold_price=125.0, sold_at=sold_at,
    )

    assert result is not None
    assert result["status"] == "sold"
    assert result["sold_platform"] == "ebay"
    assert float(result["sold_price"]) == 125.0


def test_apply_unit_sold_from_sync_closes_single_quantity_listing(db):
    product = _make_product(db, tag="SY2")
    unit = _make_unit(db, product["id"], "SS-SOLD-002")
    channel = _upsert_channel(db, "ebay")
    listing = _make_active_listing(db, product["id"], channel["id"])
    _attach_unit(db, listing["id"], unit["id"])
    sold_at = datetime(2026, 5, 1, 12, 0, 0)

    apply_unit_sold_from_sync(
        db, unit_code="SS-SOLD-002", platform="ebay", sold_price=120.0, sold_at=sold_at,
    )

    with db.cursor() as cur:
        cur.execute("SELECT status FROM listings WHERE id = %s", [listing["id"]])
        assert cur.fetchone()[0] == "sold"


def test_apply_unit_sold_from_sync_unknown_unit_code_returns_none(db):
    result = apply_unit_sold_from_sync(
        db, unit_code="NONEXISTENT-SKU-999", platform="ebay",
        sold_price=100.0, sold_at=datetime.utcnow(),
    )
    assert result is None


def test_apply_unit_sold_from_sync_already_sold_returns_none(db):
    product = _make_product(db, tag="SY3")
    unit = _make_unit(db, product["id"], "SS-SOLD-003", status="sold")
    with db.cursor() as cur:
        cur.execute(
            "UPDATE units SET sold_at = now() WHERE id = %s", [unit["id"]]
        )

    result = apply_unit_sold_from_sync(
        db, unit_code="SS-SOLD-003", platform="ebay",
        sold_price=100.0, sold_at=datetime.utcnow(),
    )
    assert result is None


def test_create_sync_log_returns_running_row(db):
    _upsert_channel(db, "ebay")

    log = create_sync_log(db, channel_name="ebay", sync_type="sold_items")

    assert log["status"] == "running"
    assert log["sync_type"] == "sold_items"
    assert log["started_at"] is not None
    assert log["completed_at"] is None


def test_create_sync_log_unknown_channel_raises(db):
    with pytest.raises(ValueError, match="Channel not found"):
        create_sync_log(db, channel_name="nonexistent_platform", sync_type="sold_items")


def test_finish_sync_log_sets_completed_status(db):
    _upsert_channel(db, "ebay")
    log = create_sync_log(db, channel_name="ebay", sync_type="active_listings")

    result = finish_sync_log(
        db,
        sync_log_id=str(log["id"]),
        status="completed",
        records_processed=10,
        records_updated=7,
    )

    assert result["status"] == "completed"
    assert result["completed_at"] is not None
    assert result["records_processed"] == 10
    assert result["records_updated"] == 7


def test_finish_sync_log_stores_errors(db):
    _upsert_channel(db, "ebay")
    log = create_sync_log(db, channel_name="ebay", sync_type="sold_items")

    result = finish_sync_log(
        db,
        sync_log_id=str(log["id"]),
        status="failed",
        errors=[{"item_id": "123", "error": "Not found"}],
    )

    assert result["status"] == "failed"
    assert result["errors"] is not None
