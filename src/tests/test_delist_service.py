"""
Unit tests for module-level functions in src.services.delisting.delist_service
(EPIC 6 Ticket 6.2).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.
"""
import pytest
import psycopg2.extras

from src.services.delisting.delist_service import (
    find_active_listings_for_unit,
    mark_listing_sold,
    mark_listing_ended,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_product(db, tag="DL"):
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


def _make_unit(db, product_id, unit_code):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, 'listed') RETURNING id",
            [unit_code, product_id],
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


def _make_listing(db, product_id, channel_id, status="active"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, current_price, status, mode)
            VALUES
                (gen_random_uuid(), %s, %s, 'Test Listing', 100.0, %s, 'single_quantity')
            RETURNING id
            """,
            [product_id, channel_id, status],
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
# Tests 1–7: find_active_listings_for_unit, mark_listing_sold, mark_listing_ended
# ---------------------------------------------------------------------------

def test_find_active_listings_for_unit_returns_one_listing(db):
    product = _make_product(db, tag="FAL1")
    unit = _make_unit(db, product["id"], "DL-FAL-001")
    channel = _upsert_channel(db, "ebay")
    listing = _make_listing(db, product["id"], channel["id"], status="active")
    _attach_unit(db, listing["id"], unit["id"])

    results = find_active_listings_for_unit(db, unit["id"])

    assert len(results) == 1
    assert str(results[0]["id"]) == str(listing["id"])
    assert results[0]["channel_name"] == "ebay"


def test_find_active_listings_for_unit_excludes_sold_and_ended(db):
    product = _make_product(db, tag="FAL2")
    unit = _make_unit(db, product["id"], "DL-FAL-002")
    channel = _upsert_channel(db, "ebay")

    sold_listing = _make_listing(db, product["id"], channel["id"], status="sold")
    ended_listing = _make_listing(db, product["id"], channel["id"], status="ended")
    _attach_unit(db, sold_listing["id"], unit["id"])
    _attach_unit(db, ended_listing["id"], unit["id"])

    results = find_active_listings_for_unit(db, unit["id"])
    assert results == []


def test_find_active_listings_for_unit_multi_platform(db):
    product = _make_product(db, tag="FAL3")
    unit = _make_unit(db, product["id"], "DL-FAL-003")
    ebay = _upsert_channel(db, "ebay")
    poshmark = _upsert_channel(db, "poshmark", supports_multi=False)

    l_ebay = _make_listing(db, product["id"], ebay["id"], status="active")
    l_posh = _make_listing(db, product["id"], poshmark["id"], status="active")
    _attach_unit(db, l_ebay["id"], unit["id"])
    _attach_unit(db, l_posh["id"], unit["id"])

    results = find_active_listings_for_unit(db, unit["id"])
    assert len(results) == 2
    channel_names = {r["channel_name"] for r in results}
    assert channel_names == {"ebay", "poshmark"}


def test_mark_listing_sold_sets_status_and_timestamps(db):
    product = _make_product(db, tag="MLS1")
    channel = _upsert_channel(db, "ebay")
    listing = _make_listing(db, product["id"], channel["id"])

    result = mark_listing_sold(db, listing_id=str(listing["id"]))

    assert result["status"] == "sold"
    assert result["sold_at"] is not None
    assert result["ended_at"] is not None


def test_mark_listing_sold_stores_price(db):
    product = _make_product(db, tag="MLS2")
    channel = _upsert_channel(db, "ebay")
    listing = _make_listing(db, product["id"], channel["id"])

    result = mark_listing_sold(db, listing_id=str(listing["id"]), sold_price=135.50)

    assert float(result["sold_price"]) == 135.50


def test_mark_listing_ended_sets_status_and_ended_at(db):
    product = _make_product(db, tag="MLE1")
    channel = _upsert_channel(db, "ebay")
    listing = _make_listing(db, product["id"], channel["id"])

    result = mark_listing_ended(db, listing_id=str(listing["id"]))

    assert result["status"] == "ended"
    assert result["ended_at"] is not None


def test_mark_listing_ended_nonexistent_raises(db):
    import uuid
    with pytest.raises(ValueError, match="Listing not found"):
        mark_listing_ended(db, listing_id=str(uuid.uuid4()))
