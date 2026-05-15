"""
Unit tests for src.services.listing_service (EPIC 3 Tickets 3.1-3.2, EPIC 5 Tickets 5.1-5.2).

Uses a real Postgres connection with per-test transaction rollback so no data
persists between tests.  Requires postgres to be running with the EPIC 3 and
EPIC 5 migrations applied.
"""
import psycopg2
import psycopg2.extras
import pytest

from src.services.listing_service import (
    create_listing,
    assign_unit_to_listing,
    end_listing,
    update_listing_on_unit_sold,
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


# ---------------------------------------------------------------------------
# Tests 14-17: quantity param (Ticket 5.1)
# ---------------------------------------------------------------------------

def test_create_listing_default_quantity_is_one(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Nike AJ1 NEW", price=120.0,
    )
    assert listing["quantity"] == 1


def test_multi_quantity_listing_accepts_quantity_gt_one(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Nike AJ1 NEW", price=120.0, quantity=3,
    )
    assert listing["quantity"] == 3
    assert listing["mode"] == "multi_quantity"


def test_single_quantity_listing_rejects_quantity_gt_one(db, channels, new_product):
    with pytest.raises(ValueError, match="single_quantity listings must have quantity=1"):
        create_listing(
            db, product_id=str(new_product["id"]),
            channel_name="poshmark", title="Nike AJ1 NEW", price=110.0, quantity=2,
        )


def test_db_constraint_rejects_single_quantity_with_quantity_gt_one(db, channels, new_product):
    with pytest.raises(psycopg2.IntegrityError):
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO listings
                    (id, product_id, channel_id, title, current_price, status, mode, quantity)
                VALUES
                    (gen_random_uuid(), %s,
                     (SELECT id FROM channels WHERE name = 'poshmark'),
                     'Test', 100.0, 'draft', 'single_quantity', 2)
                """,
                [new_product["id"]],
            )
    db.rollback()


# ---------------------------------------------------------------------------
# Tests 18-21: update_listing_on_unit_sold (Ticket 5.2)
# ---------------------------------------------------------------------------

def test_single_quantity_listing_becomes_sold_when_unit_sold(db, channels, excellent_product):
    listing, _ = create_listing(
        db, product_id=str(excellent_product["id"]),
        channel_name="ebay", title="Test", price=100.0, status="active",
    )
    unit = _make_unit(db, str(excellent_product["id"]))
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(unit["id"]))

    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [unit["id"]])

    update_listing_on_unit_sold(db, unit_id=str(unit["id"]))

    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT status, sold_at FROM listings WHERE id = %s", [listing["id"]])
        row = dict(cur.fetchone())

    assert row["status"] == "sold"
    assert row["sold_at"] is not None


def test_multi_quantity_listing_stays_active_when_one_unit_sold(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, quantity=2, status="active",
    )
    u1 = _make_unit(db, str(new_product["id"]), unit_code="MQ-A001")
    u2 = _make_unit(db, str(new_product["id"]), unit_code="MQ-A002")
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u1["id"]))
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u2["id"]))

    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [u1["id"]])

    update_listing_on_unit_sold(db, unit_id=str(u1["id"]))

    with db.cursor() as cur:
        cur.execute("SELECT status FROM listings WHERE id = %s", [listing["id"]])
        assert cur.fetchone()[0] == "active"


def test_multi_quantity_listing_ends_when_all_units_sold(db, channels, new_product):
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, quantity=2, status="active",
    )
    u1 = _make_unit(db, str(new_product["id"]), unit_code="MQ-B001")
    u2 = _make_unit(db, str(new_product["id"]), unit_code="MQ-B002")
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u1["id"]))
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u2["id"]))

    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id IN (%s, %s)", [u1["id"], u2["id"]])

    update_listing_on_unit_sold(db, unit_id=str(u1["id"]))
    update_listing_on_unit_sold(db, unit_id=str(u2["id"]))

    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT status, ended_at FROM listings WHERE id = %s", [listing["id"]])
        row = dict(cur.fetchone())

    assert row["status"] == "ended"
    assert row["ended_at"] is not None


def test_update_listing_on_unit_sold_no_active_listing_is_noop(db, channels, new_product):
    unit = _make_unit(db, str(new_product["id"]))
    with db.cursor() as cur:
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [unit["id"]])

    update_listing_on_unit_sold(db, unit_id=str(unit["id"]))


# ---------------------------------------------------------------------------
# Fix C3: multi-qty listing ends when remaining unit is damaged (not just sold)
# ---------------------------------------------------------------------------

def test_multi_quantity_listing_ends_when_last_unit_is_damaged(db, channels, new_product):
    """C3: listing must end when one unit is sold and the other is damaged."""
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, quantity=2, status="active",
    )
    u1 = _make_unit(db, str(new_product["id"]), unit_code="C3-SOLD-001")
    u2 = _make_unit(db, str(new_product["id"]), unit_code="C3-DMGD-001")
    assign_unit_to_listing(db, listing_id=str(listing["id"]), unit_id=str(u1["id"]))

    # Attach u2 directly so we can set it to damaged without triggering status change
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) VALUES (gen_random_uuid(), %s, %s)",
            [listing["id"], u2["id"]],
        )
        cur.execute("UPDATE units SET status = 'damaged' WHERE id = %s", [u2["id"]])
        cur.execute("UPDATE units SET status = 'sold' WHERE id = %s", [u1["id"]])

    update_listing_on_unit_sold(db, unit_id=str(u1["id"]))

    with db.cursor() as cur:
        cur.execute("SELECT status FROM listings WHERE id = %s", [listing["id"]])
        assert cur.fetchone()[0] == "ended"


# ---------------------------------------------------------------------------
# Fix C4: end_listing does not revert damaged unit to ready_to_list
# ---------------------------------------------------------------------------

def test_end_listing_does_not_revert_damaged_unit(db, channels, new_product):
    """C4: end_listing must leave damaged units as-is."""
    listing, _ = create_listing(
        db, product_id=str(new_product["id"]),
        channel_name="ebay", title="Test", price=120.0, status="active",
    )
    damaged_unit = _make_unit(db, str(new_product["id"]), unit_code="C4-DMGD-001", status="damaged")

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) VALUES (gen_random_uuid(), %s, %s)",
            [listing["id"], damaged_unit["id"]],
        )

    end_listing(db, listing_id=str(listing["id"]))

    with db.cursor() as cur:
        cur.execute("SELECT status FROM units WHERE id = %s", [damaged_unit["id"]])
        assert cur.fetchone()[0] == "damaged"
