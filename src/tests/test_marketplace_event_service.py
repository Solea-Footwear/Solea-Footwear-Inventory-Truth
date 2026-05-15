"""
Unit tests for src.services.marketplace_event_service (EPIC 6 Ticket 6.1).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.
"""
import pytest
import psycopg2.extras

from src.services.marketplace_event_service import (
    record_marketplace_event,
    resolve_mercari_sku,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mercari_listing(db, unit_code="MERC-SKU-001", channel_listing_id="MERC-LIST-001"):
    """Insert product, unit, mercari channel, listing, and listing_units row."""
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
            [f"NIKE-AJ1-TEST-{unit_code}"],
        )
        product_id = cur.fetchone()["id"]

        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, 'listed') RETURNING id",
            [unit_code, product_id],
        )
        unit_id = cur.fetchone()["id"]

        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES ('mercari', 'Mercari', TRUE, FALSE)
            ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity
            RETURNING id
            """
        )
        channel_id = cur.fetchone()["id"]

        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, current_price, status,
                 mode, channel_listing_id)
            VALUES
                (gen_random_uuid(), %s, %s, 'Test Listing', 100.0, 'active',
                 'single_quantity', %s)
            RETURNING id
            """,
            [product_id, channel_id, channel_listing_id],
        )
        listing_id = cur.fetchone()["id"]

        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) "
            "VALUES (gen_random_uuid(), %s, %s)",
            [listing_id, unit_id],
        )

    return unit_code


# ---------------------------------------------------------------------------
# Tests 1–6: record_marketplace_event and resolve_mercari_sku
# ---------------------------------------------------------------------------

def test_record_marketplace_event_new_returns_dict_and_true(db):
    event, created = record_marketplace_event(
        db,
        platform="ebay",
        message_id="msg-me-001",
        event_type="sale",
        sku="AJ1-001",
        external_order_id="ORD-001",
        raw_payload={"title": "Test Shoe"},
    )
    assert created is True
    assert isinstance(event, dict)
    assert event["platform"] == "ebay"
    assert event["message_id"] == "msg-me-001"
    assert event["sku"] == "AJ1-001"


def test_record_marketplace_event_duplicate_returns_false(db):
    record_marketplace_event(
        db, platform="poshmark", message_id="msg-me-002", event_type="sale",
    )
    event2, created2 = record_marketplace_event(
        db, platform="poshmark", message_id="msg-me-002", event_type="sale",
    )
    assert created2 is False
    assert isinstance(event2, dict)
    assert event2["platform"] == "poshmark"

    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM marketplace_events "
            "WHERE platform = 'poshmark' AND message_id = 'msg-me-002'"
        )
        assert cur.fetchone()[0] == 1


def test_record_marketplace_event_two_different_message_ids(db):
    record_marketplace_event(db, platform="ebay", message_id="msg-me-003a", event_type="sale")
    record_marketplace_event(db, platform="ebay", message_id="msg-me-003b", event_type="sale")

    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM marketplace_events "
            "WHERE platform = 'ebay' AND message_id IN ('msg-me-003a', 'msg-me-003b')"
        )
        assert cur.fetchone()[0] == 2


def test_record_marketplace_event_missing_platform_raises(db):
    with pytest.raises(ValueError, match="platform is required"):
        record_marketplace_event(db, platform="", message_id="msg-x", event_type="sale")


def test_resolve_mercari_sku_with_matching_listing(db):
    expected_sku = _make_mercari_listing(db, unit_code="MERC-FIND-001", channel_listing_id="MERC-EXT-001")
    sku = resolve_mercari_sku(db, mercari_listing_id="MERC-EXT-001")
    assert sku == expected_sku


def test_resolve_mercari_sku_with_no_match_returns_none(db):
    result = resolve_mercari_sku(db, mercari_listing_id="MERC-NONEXISTENT-999")
    assert result is None
