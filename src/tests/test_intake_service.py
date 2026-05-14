"""Unit tests for src.services.intake_service (EPIC 2 Tickets 2.2 and 2.3).

Uses an in-memory SQLite database.  CheckConstraints (unit_code != '',
status IN (...)) are Postgres-specific and are not enforced by SQLite;
those are validated by Postgres integration tests.

Tables created in FK-dependency order: Location → Product → Unit.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import database.py first — bootstraps Base and all model re-exports so that
# subsequent individual model imports see fully-initialized modules (avoids
# the circular-import error that occurs when a model file is the very first
# thing imported in a fresh Python process).
from src.backend.db.database import Base  # noqa — must precede model imports
from src.backend.db.models.location import Location
from src.backend.db.models.product import Product
from src.backend.db.models.unit import Unit
from src.services.intake_service import register_unit


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    # Create tables in FK order; avoids ARRAY issues from listing_templates
    for tbl in [Location.__table__, Product.__table__, Unit.__table__]:
        tbl.create(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    for tbl in reversed([Location.__table__, Product.__table__, Unit.__table__]):
        tbl.drop(bind=engine)


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


# ---------------------------------------------------------------------------
# Test 1-3: Happy path + basic idempotency
# ---------------------------------------------------------------------------

def test_creates_unit_and_product(db):
    unit, product, unit_created, product_created = _nike_new(db)
    db.commit()
    assert unit_created is True
    assert product_created is True
    assert unit.unit_code == "SHOE-001"
    assert unit.status == "ready_to_list"


def test_same_unit_code_returns_existing(db):
    u1, _, _, _ = _nike_new(db, unit_code="SHOE-001")
    db.commit()
    u2, _, unit_created, product_created = _nike_new(db, unit_code="SHOE-001")
    db.commit()
    assert unit_created is False
    assert product_created is False
    assert str(u1.id) == str(u2.id)


def test_only_one_unit_row_after_duplicate(db):
    _nike_new(db, unit_code="SHOE-001")
    db.commit()
    _nike_new(db, unit_code="SHOE-001")
    db.commit()
    assert db.query(Unit).filter(Unit.unit_code == "SHOE-001").count() == 1


# ---------------------------------------------------------------------------
# Test 4-5: Interchangeable — two unit_codes share one product
# ---------------------------------------------------------------------------

def test_new_two_units_share_one_product(db):
    _, p1, _, _ = _nike_new(db, unit_code="SHOE-001")
    db.commit()
    _, p2, _, p2_created = _nike_new(db, unit_code="SHOE-002")
    db.commit()
    assert str(p1.id) == str(p2.id)
    assert p2_created is False


def test_like_new_two_units_share_one_product(db):
    _, p1, _, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", unit_code="A",
    )
    db.commit()
    _, p2, _, pc = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", unit_code="B",
    )
    db.commit()
    assert str(p1.id) == str(p2.id)
    assert pc is False


# ---------------------------------------------------------------------------
# Test 6: Non-interchangeable — two unit_codes get separate products
# ---------------------------------------------------------------------------

def test_excellent_two_units_get_separate_products(db):
    _, p1, _, pc1 = _nike_excellent(db, unit_code="SHOE-001")
    db.commit()
    _, p2, _, pc2 = _nike_excellent(db, unit_code="SHOE-002")
    db.commit()
    assert str(p1.id) != str(p2.id)
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
    db.commit()
    assert unit_created is True
    assert product.condition_code == "NEW"
    assert product.is_interchangeable is True


def test_solea_very_good_maps_to_excellent(db):
    _, product, _, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="very_good", unit_code="SHOE-001",
    )
    db.commit()
    assert product.condition_code == "EXCELLENT"
    assert product.is_interchangeable is False


def test_solea_good_maps_to_good(db):
    _, product, _, _ = register_unit(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="good", unit_code="SHOE-001",
    )
    db.commit()
    assert product.condition_code == "GOOD"


# ---------------------------------------------------------------------------
# Tests 10-12: Location resolution
# ---------------------------------------------------------------------------

def test_location_resolved_by_code(db):
    loc = Location(code="A1-01-06-03", description="Shelf A1")
    db.add(loc)
    db.flush()
    unit, _, _, _ = _nike_new(db, location_code="A1-01-06-03")
    db.commit()
    assert str(unit.location_id) == str(loc.id)


def test_missing_location_code_silently_ignored(db):
    unit, _, _, _ = _nike_new(db, location_code="NONEXISTENT")
    db.commit()
    assert unit.location_id is None


def test_no_location_code_sets_null(db):
    unit, _, _, _ = _nike_new(db)
    db.commit()
    assert unit.location_id is None


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
    db.commit()
    assert isinstance(unit, Unit)
    assert isinstance(product, Product)
    assert unit_created is True
    assert product_created is True


# ---------------------------------------------------------------------------
# Tests 17-22: Batch intake (route-level logic tested via service layer)
# ---------------------------------------------------------------------------

def _batch(db, items):
    """Helper: call register_unit for each item, return summary dict."""
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
        results.append({'unit': unit, 'product': product, 'unit_created': uc, 'product_created': pc})
    db.commit()
    return {
        'total': len(items),
        'units_created': units_created,
        'units_skipped': units_skipped,
        'products_created': products_created,
        'products_reused': products_reused,
        'items': results,
    }


def test_batch_three_new_units(db):
    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="A"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="B"),
        dict(brand="Adidas", model="Yeezy", style_code="CP9654", gender="Men", size="9", condition="NEW", unit_code="C"),
    ]
    summary = _batch(db, items)
    assert summary['total'] == 3
    assert summary['units_created'] == 3
    assert summary['units_skipped'] == 0


def test_batch_two_new_units_same_style_share_product(db):
    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="A"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="B"),
    ]
    summary = _batch(db, items)
    assert summary['products_created'] == 1
    assert summary['products_reused'] == 1
    assert str(summary['items'][0]['product'].id) == str(summary['items'][1]['product'].id)


def test_batch_with_preexisting_unit_code_skipped(db):
    # Pre-insert one unit
    _nike_new(db, unit_code="SHOE-001")
    db.commit()

    items = [
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="SHOE-001"),
        dict(brand="Nike", model="AJ1", style_code="555088", gender="Men", size="10", condition="NEW", unit_code="SHOE-002"),
    ]
    summary = _batch(db, items)
    assert summary['units_skipped'] == 1
    assert summary['units_created'] == 1


def test_batch_empty_list_raises_no_rows(db):
    # An empty batch call with no items should produce no DB rows
    summary = _batch(db, [])
    assert summary['total'] == 0
    assert db.query(Unit).count() == 0


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
    # EXCELLENT is non-interchangeable → each unit gets its own product
    assert summary['products_created'] == 2
    assert str(summary['items'][0]['product'].id) != str(summary['items'][1]['product'].id)
    # Each unit's product_id FK matches the product
    for item in summary['items']:
        assert str(item['unit'].product_id) == str(item['product'].id)
