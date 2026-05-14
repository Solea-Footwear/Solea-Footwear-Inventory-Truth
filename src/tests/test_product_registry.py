"""Unit tests for src.services.product_registry_service (EPIC 1 Ticket 1.3).

Uses an in-memory SQLite database so tests run without Postgres or Docker.
SQLAlchemy's CHECK constraint syntax is Postgres-specific and is not enforced
by SQLite; those constraints are validated by Postgres integration tests.
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.backend.db.database import Base
from src.backend.db.models.product import Product  # noqa — registers Product with Base.metadata
from src.services.product_registry_service import find_or_create_product
from src.services.product_id_service import map_solea_condition


# ---------------------------------------------------------------------------
# In-memory SQLite fixture — only creates the products table to avoid the
# Postgres-only ARRAY type in listing_templates.seo_keywords.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    # Only create the products table; other models use Postgres-specific types
    # (ARRAY) that SQLite cannot render.
    Product.__table__.create(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Product.__table__.drop(bind=engine)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nike(db, condition, sku=None):
    return find_or_create_product(
        db,
        brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition=condition, sku=sku,
    )


# ---------------------------------------------------------------------------
# Basic create
# ---------------------------------------------------------------------------

def test_first_call_creates_row(db):
    product, created = _nike(db, "NEW")
    db.commit()
    assert created is True
    assert product.product_id == "NIKE-AIRJORDAN1-555088-MEN-10-NEW"


def test_created_row_has_correct_columns(db):
    p, _ = _nike(db, "NEW")
    db.commit()
    assert p.brand == "Nike"
    assert p.model == "Air Jordan 1"
    assert p.style_code == "555088"
    assert p.gender == "Men"
    assert p.size == "10"
    assert p.condition_code == "NEW"
    assert p.is_interchangeable is True


def test_non_interchangeable_created_correctly(db):
    p, created = _nike(db, "EXCELLENT", sku="SKU-001")
    db.commit()
    assert created is True
    assert p.is_interchangeable is False
    assert p.condition_code == "EXCELLENT"


# ---------------------------------------------------------------------------
# Idempotency — second call returns same row
# ---------------------------------------------------------------------------

def test_second_call_returns_existing_row(db):
    p1, c1 = _nike(db, "NEW")
    db.commit()
    p2, c2 = _nike(db, "NEW")
    db.commit()
    assert c1 is True
    assert c2 is False
    assert p1.id == p2.id


def test_only_one_row_in_db_after_two_calls(db):
    from src.backend.db.models.product import Product
    _nike(db, "NEW")
    db.commit()
    _nike(db, "NEW")
    db.commit()
    count = db.query(Product).filter(Product.product_id == "NIKE-AIRJORDAN1-555088-MEN-10-NEW").count()
    assert count == 1


# ---------------------------------------------------------------------------
# Interchangeable: two SKUs → one product row
# ---------------------------------------------------------------------------

def test_new_two_skus_one_row(db):
    p1, c1 = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="NEW", sku="SKU-001",
    )
    db.commit()
    p2, c2 = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="NEW", sku="SKU-002",
    )
    db.commit()
    assert p1.id == p2.id
    assert c2 is False


def test_like_new_two_skus_one_row(db):
    p1, _ = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", sku="A",
    )
    db.commit()
    p2, c2 = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition="LIKE_NEW", sku="B",
    )
    db.commit()
    assert p1.id == p2.id
    assert c2 is False


# ---------------------------------------------------------------------------
# Non-interchangeable: two SKUs → two distinct rows
# ---------------------------------------------------------------------------

def test_excellent_two_skus_two_rows(db):
    p1, c1 = _nike(db, "EXCELLENT", sku="SKU-001")
    db.commit()
    p2, c2 = _nike(db, "EXCELLENT", sku="SKU-002")
    db.commit()
    assert c1 is True
    assert c2 is True
    assert p1.id != p2.id


# ---------------------------------------------------------------------------
# Race condition — IntegrityError retry
# ---------------------------------------------------------------------------

def test_integrity_error_retry_returns_existing_row(db):
    """Simulate a concurrent INSERT winning the race: flush raises IntegrityError,
    the service rolls back and re-reads the row the winner inserted.

    Setup:
      - The "winning" session has already committed the row.
      - We mock the INITIAL query to return None (simulating the race window
        where our session looked before the winner committed).
      - We mock flush to raise IntegrityError (simulating the duplicate-key
        violation when we try to insert the same product_id).
      - no_autoflush prevents SQLAlchemy from calling flush during our queries,
        so the mock only fires on the service's explicit db.flush() call.
    """
    from src.backend.db.models.product import Product

    # Commit the row that the "winning" concurrent session inserted
    pre = Product(
        product_id="NIKE-AIRJORDAN1-555088-MEN-10-NEW",
        brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition_code="NEW", is_interchangeable=True,
    )
    db.add(pre)
    db.commit()

    original_query = db.query
    first_call = [True]

    def mock_query(model):
        if first_call[0]:
            first_call[0] = False
            # Race window: our session looks up the product_id and finds nothing
            m = MagicMock()
            m.filter.return_value.first.return_value = None
            return m
        # Re-query after rollback: use the real query to find the committed row
        return original_query(model)

    with db.no_autoflush:
        with patch.object(db, "query", side_effect=mock_query), \
             patch.object(db, "flush", side_effect=IntegrityError("race", {}, None)):
            p, created = _nike(db, "NEW")

    assert created is False
    assert p.product_id == "NIKE-AIRJORDAN1-555088-MEN-10-NEW"


# ---------------------------------------------------------------------------
# Solea condition mapping integration
# ---------------------------------------------------------------------------

def test_solea_very_good_maps_to_excellent(db):
    condition = map_solea_condition("very_good")
    p, created = find_or_create_product(
        db, brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition=condition, sku="SKU-001",
    )
    db.commit()
    assert p.condition_code == "EXCELLENT"
    assert p.is_interchangeable is False
