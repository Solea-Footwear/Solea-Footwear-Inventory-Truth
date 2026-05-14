"""EPIC 2 Ticket 2.2 — intake path for physical inventory units.

register_unit() is the single entry point for adding a new shoe to the
system.  It looks up or creates the parent Product via find_or_create_product()
(EPIC 1) and then creates the Unit row linked to it.

The caller controls db.commit() so multiple units can be batched in one
transaction (used by the POST /api/intake/units batch endpoint).
"""
from typing import Optional, Tuple

from src.backend.db.models.location import Location
from src.backend.db.models.product import Product
from src.backend.db.models.unit import Unit
from src.services.product_id_service import (
    VALID_CONDITIONS,
    SOLEA_TO_EPIC_CONDITION,
    map_solea_condition,
    is_interchangeable,
)
from src.services.product_registry_service import find_or_create_product


def _normalize_condition(condition: str) -> str:
    """Return a canonical EPIC condition code from any accepted input.

    Accepts canonical EPIC codes (NEW, LIKE_NEW, EXCELLENT, GOOD, FAIR) —
    returned uppercase as-is.  Also accepts Solea legacy internal_codes
    (new_with_box, new_without_box, excellent, very_good, good, fair) which
    are translated via map_solea_condition().  Raises ValueError for anything
    else.
    """
    if not condition or not condition.strip():
        raise ValueError("condition must not be empty")
    upper = condition.strip().upper()
    if upper in VALID_CONDITIONS:
        return upper
    return map_solea_condition(condition)


def register_unit(
    db,
    *,
    brand: str,
    model: str,
    style_code: str,
    gender: str,
    size: str,
    condition: str,
    unit_code: str,
    location_code: Optional[str] = None,
    cost_basis: Optional[float] = None,
    notes: Optional[str] = None,
) -> Tuple:
    """Register a physical inventory unit and ensure its Product row exists.

    Accepts canonical EPIC condition codes (NEW, LIKE_NEW, EXCELLENT, GOOD,
    FAIR) or Solea legacy internal_codes (new_with_box, new_without_box,
    excellent, very_good, good, fair).  Legacy codes are auto-mapped before
    any further processing.

    Idempotent on unit_code: if a unit with the given unit_code already exists
    in the database, the existing unit and its product are returned unchanged.

    Location resolution: if location_code is provided the locations table is
    queried by Location.code.  A missing location is silently ignored (the
    unit is still created with location_id = None).

    The caller is responsible for calling db.commit().  This function only
    calls db.flush() so the new Unit row gets its primary key while remaining
    part of the caller's transaction.

    Args:
        db:             SQLAlchemy session.
        brand:          Shoe brand name (e.g. "Nike").
        model:          Shoe model name (e.g. "Air Jordan 1").
        style_code:     Manufacturer style code (e.g. "555088-001").
        gender:         Gender designation (e.g. "Men", "Women", "GS").
        size:           US size string (e.g. "10", "10.5").
        condition:      Condition — canonical EPIC code or Solea legacy code.
        unit_code:      Physical barcode / eBay SKU.  Must be non-empty.
        location_code:  Optional warehouse location code.  Resolved to
                        location_id; silently ignored if location not found.
        cost_basis:     Optional purchase cost in USD.
        notes:          Optional free-text notes.

    Returns:
        (unit, product, unit_created: bool, product_created: bool)

        unit_created is False when a unit with the given unit_code already
        existed — the existing unit is returned unchanged.
        product_created is False when find_or_create_product() found an
        existing product row.

    Raises:
        ValueError: if unit_code is empty, or if condition cannot be mapped
                    to a canonical EPIC code.
    """
    # --- Validate unit_code ---
    if not unit_code or not unit_code.strip():
        raise ValueError("unit_code must not be empty")
    unit_code = unit_code.strip()

    # --- Normalize condition to canonical EPIC code ---
    canonical_condition = _normalize_condition(condition)

    # --- Idempotency check ---
    existing_unit = db.query(Unit).filter(Unit.unit_code == unit_code).first()
    if existing_unit is not None:
        # Explicit re-query avoids DetachedInstanceError if the relationship
        # is accessed outside the session context by the caller.
        existing_product = (
            db.query(Product).filter(Product.id == existing_unit.product_id).first()
        )
        return existing_unit, existing_product, False, False

    # --- Find or create parent product ---
    # For non-interchangeable conditions the unit_code becomes the SKU token
    # in the product_id (e.g. NIKE-AJ1-...-EXCELLENT-SHOE001).
    # For interchangeable conditions (NEW/LIKE_NEW) the SKU is ignored so all
    # units of the same style share one Product row.
    sku_for_product = unit_code if not is_interchangeable(canonical_condition) else None
    product, product_created = find_or_create_product(
        db,
        brand=brand,
        model=model,
        style_code=style_code,
        gender=gender,
        size=size,
        condition=canonical_condition,
        sku=sku_for_product,
    )

    # --- Resolve location ---
    location_id = None
    if location_code:
        loc = db.query(Location).filter(Location.code == location_code).first()
        if loc is not None:
            location_id = loc.id

    # --- Create unit ---
    unit = Unit(
        unit_code=unit_code,
        product_id=product.id,
        location_id=location_id,
        cost_basis=cost_basis,
        notes=notes,
        status='ready_to_list',
    )
    db.add(unit)
    db.flush()

    return unit, product, True, product_created
