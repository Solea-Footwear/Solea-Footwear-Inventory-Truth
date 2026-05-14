"""EPIC 1 Ticket 1.3 — idempotent product lookup-or-create.

find_or_create_product() is the single entry point for attaching a canonical
Product ID to an incoming SKU.  The caller controls the DB commit so that
multiple operations can be batched in one transaction.
"""
from sqlalchemy.exc import IntegrityError

from src.backend.db.models.product import Product
from src.services.product_id_service import generate_product_id, is_interchangeable


def find_or_create_product(db, *, brand: str, model: str, style_code: str,
                           gender: str, size: str, condition: str,
                           sku: str = None) -> tuple:
    """Return (Product, created: bool).

    Generates a deterministic product_id then queries for an existing row.
    If none exists, creates one.  Race-safe: a concurrent INSERT on the same
    product_id raises IntegrityError (UNIQUE constraint); the retry reads the
    row the winner inserted.

    Caller is responsible for db.commit().  Columns outside the EPIC 1 set
    (colorway, category_id, condition_grade_id, sku_prefix, default_price_ebay,
    notes) are left NULL — EPIC 9 backfill will populate them for legacy rows.
    """
    pid = generate_product_id(
        brand=brand, model=model, style_code=style_code,
        gender=gender, size=size, condition=condition, sku=sku,
    )

    existing = db.query(Product).filter(Product.product_id == pid).first()
    if existing:
        return existing, False

    product = Product(
        product_id=pid,
        brand=brand,
        model=model,
        style_code=style_code,
        gender=gender,
        size=size,
        condition_code=condition.upper(),
        is_interchangeable=is_interchangeable(condition),
    )
    db.add(product)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        row = db.query(Product).filter(Product.product_id == pid).first()
        return row, False

    return product, True
