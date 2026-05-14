"""
Unit tests for src.services.listing_service (EPIC 3 Tickets 3.1 and 3.2).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.  Requires postgres to be running with the EPIC 3
migration applied (supports_multi_quantity column on channels).
"""
import pytest
import psycopg2.extras

from src.services.listing_service import (
    create_listing,
    assign_unit_to_listing,
    end_listing,
)


# ---------------------------------------------------------------------------
# Fixtures — all inserts are inside the open transaction and auto-rolled back
# ---------------------------------------------------------------------------

@pytest.fixture
def channels(db):
    """Upsert ebay/poshmark/mercari rows with correct supports_multi_quantity."""
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES ('ebay', 'eBay', TRUE, TRUE)
            ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity
            """
        )
        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES ('poshmark', 'Poshmark', TRUE, FALSE)
            ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity
            """
        )
        cur.execute(
            """
            INSERT INTO channels (name, display_name, is_active, supports_multi_quantity)
            VALUES ('mercari', 'Mercari', TRUE, FALSE)
            ON CONFLICT (name) DO UPDATE SET supports_multi_quantity = EXCLUDED.supports_multi_quantity
            """
        )


@pytest.fixture
def new_product(db):
    """Interchangeable NEW product."""
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), 'NIKE-AJ1-555088-MEN-10-NEW',
                 'Nike', 'Air Jordan 1', '555088', 'Men', '10', 'NEW', TRUE)
            RETURNING *
            """
        )
        return dict(cur.fetchone())


@pytest.fixture
def like_new_product(db):
    """Interchangeable LIKE_NEW product."""
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), 'NIKE-AJ1-555088-MEN-10-LIKE_NEW',
                 'Nike', 'Air Jordan 1', '555088', 'Men', '10', 'LIKE_NEW', TRUE)
            RETURNING *
            """
        )
        return dict(cur.fetchone())


@pytest.fixture
def excellent_product(db):
    """Non-interchangeable EXCELLENT product."""
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products
                (id, product_id, brand, model, style_code, gender, size,
                 condition_code, is_interchangeable)
            VALUES
                (gen_random_uuid(), 'NIKE-AJ1-555088-MEN-10-EXCELLENT-E001',
                 'Nike', 'Air Jordan 1', '555088', 'Men', '10', 'EXCELLENT', FALSE)
            RETURNING *
            """
        )
        return dict(cur.fetchone())


def _make_unit(db, product_id, unit_code="AJ1-TEST-001", status="ready_to_list"):
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO units (id, unit_code, product_id, status) "
            "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING *",
            [unit_code, product_id, status],
        )
        return dict(cur.fetchone())


# ---------------------------------------------------------------------------
# Tests 1-6: create_listing — mode selection
# ---------------------------------------------------------------------------

def test_new_product_ebay_is_multi_quantity(db, channels, new_product):
    listing, created = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Nike AJ1 NEW", price=120.0,
    )
    assert listing["mode"] == "multi_quantity"
    assert created is True


def test_new_product_poshmark_is_single_quantity(db, channels, new_product):
    """Poshmark ToS forbids multi-quantity listings."""
    listing, created = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="poshmark", title="Nike AJ1 NEW", price=110.0,
    )
    assert listing["mode"] == "single_quantity"


def test_excellent_product_ebay_is_single_quantity(db, channels, excellent_product):
    """Non-interchangeable product → single_quantity even on eBay."""
    listing, _ = create_listing(
        db, product_id=str(excellent_product["id"]),
        channel_name="ebay", title="Nike AJ1 EXCELLENT", price=100.0,
    )
    assert listing["mode"] == "single_quantity"


def test_like_new_product_mercari_is_single_quantity(db, channels, like_new_product):
    """Mercari ToS forbids multi-quantity listings."""
    listing, _ = create_listing(
        db, product_id=str(like_new_product["id"]),
        channel_name="mercari", title="Nike AJ1 LIKE_NEW", price=105.0,
    )
    assert listing["mode"] == "single_quantity"


def test_unknown_channel_raises(db, channels, new_product):
    with pytest.raises(ValueError, match="Channel not found"):
        create_listing(
            db, product_id=str(new_product["id"]),
            channel_name="nonexistent_platform", title="Test", price=50.0,
        )


def test_create_listing_returns_dict_and_bool(db, channels, new_product):
    listing, created = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Nike AJ1 NEW", price=120.0,
    )
    assert isinstance(listing, dict)
    assert "mode" in listing
    assert "id" in listing
    assert created is True


# ---------------------------------------------------------------------------
# Tests 7-10: assign_unit_to_listing
# ---------------------------------------------------------------------------

def test_assign_unit_sets_status_listed_and_creates_row(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=100.0,
    )
    unit = _make_unit(db, str(new_product["id"]))

    lu = assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(unit["id"]))

    assert isinstance(lu, dict)
    assert str(lu["listing_id"]) == str(listing["id"])
    assert str(lu["unit_id"]) == str(unit["id"])

    # Verify unit status changed in DB
    with db.cursor() as cur:
        cur.execute("SELECT status FROM units WHERE id = %s", [unit["id"]])
        assert cur.fetchone()[0] == "listed"

    # Verify listing_units row exists
    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM listing_units WHERE listing_id = %s AND unit_id = %s",
            [listing["id"], unit["id"]],
        )
        assert cur.fetchone()[0] == 1


def test_assign_second_unit_to_single_quantity_raises(db, channels, excellent_product):
    """single_quantity listing already has one unit — second assign must fail."""
    listing, _ = create_listing(
        db, product_id=str(excellent_product["id"]),
        channel_name="ebay", title="Test", price=100.0,
    )
    u1 = _make_unit(db, str(excellent_product["id"]), unit_code="E-001")
    u2 = _make_unit(db, str(excellent_product["id"]), unit_code="E-002")

    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u1["id"]))

    with pytest.raises(ValueError, match="single_quantity listing already has a unit assigned"):
        assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u2["id"]))


def test_assign_second_unit_to_multi_quantity_succeeds(db, channels, new_product):
    """multi_quantity listing (eBay + interchangeable) may hold multiple units."""
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0,
    )
    u1 = _make_unit(db, str(new_product["id"]), unit_code="N-001")
    u2 = _make_unit(db, str(new_product["id"]), unit_code="N-002")

    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u1["id"]))
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u2["id"]))

    with db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM listing_units WHERE listing_id = %s",
            [listing["id"]],
        )
        assert cur.fetchone()[0] == 2


def test_assign_duplicate_unit_raises(db, channels, new_product):
    """Assigning the same unit to the same listing twice must raise."""
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0,
    )
    unit = _make_unit(db, str(new_product["id"]))

    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(unit["id"]))

    with pytest.raises(ValueError, match="Unit already assigned to this listing"):
        assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(unit["id"]))


# ---------------------------------------------------------------------------
# Tests 11-13: end_listing
# ---------------------------------------------------------------------------

def test_end_listing_sets_status_and_ended_at(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, status="active",
    )
    result = end_listing(db, listing_id=str(listing["id"]))

    assert result["status"] == "ended"
    assert result["ended_at"] is not None


def test_end_listing_reverts_non_sold_units_to_ready(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, status="active",
    )
    unit = _make_unit(db, str(new_product["id"]))
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(unit["id"]))

    # unit is now 'listed'; end_listing must revert it
    end_listing(db, listing_id=str(listing["id"]))

    with db.cursor() as cur:
        cur.execute("SELECT status FROM units WHERE id = %s", [unit["id"]])
        assert cur.fetchone()[0] == "ready_to_list"


def test_end_listing_does_not_revert_sold_or_shipped_units(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, status="active",
    )
    sold_unit = _make_unit(db, str(new_product["id"]), unit_code="SOLD-001", status="sold")
    shipped_unit = _make_unit(db, str(new_product["id"]), unit_code="SHIP-001", status="shipped")

    # Attach both units directly (bypass service to avoid status-change side-effect)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) "
            "VALUES (gen_random_uuid(), %s, %s), (gen_random_uuid(), %s, %s)",
            [listing["id"], sold_unit["id"], listing["id"], shipped_unit["id"]],
        )

    end_listing(db, listing_id=str(listing["id"]))

    with db.cursor() as cur:
        cur.execute("SELECT status FROM units WHERE id = %s", [sold_unit["id"]])
        assert cur.fetchone()[0] == "sold"

        cur.execute("SELECT status FROM units WHERE id = %s", [shipped_unit["id"]])
        assert cur.fetchone()[0] == "shipped"
