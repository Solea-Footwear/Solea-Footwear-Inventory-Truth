"""EPIC 2 Ticket 2.2 — intake path for physical inventory units (psycopg2).

register_unit() is the single entry point for adding a new shoe to the
system.  It looks up or creates the parent Product via find_or_create_product()
(EPIC 1) and then creates the Unit row linked to it.

The caller controls conn.commit() so multiple units can be batched in one
transaction (used by the POST /api/intake/units batch endpoint).
"""
from typing import Optional, Tuple

import psycopg2.extras

from src.services.product_id_service import (
    VALID_CONDITIONS,
    map_solea_condition,
    is_interchangeable,
)
from src.services.product_registry_service import find_or_create_product


def _normalize_condition(condition: str) -> str:
    """Return a canonical EPIC condition code from any accepted input."""
    if not condition or not condition.strip():
        raise ValueError("condition must not be empty")
    upper = condition.strip().upper()
    if upper in VALID_CONDITIONS:
        return upper
    return map_solea_condition(condition)


def register_unit(
    conn,
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

    Returns: (unit_dict, product_dict, unit_created: bool, product_created: bool)

    Idempotent on unit_code: returns the existing row unchanged if it already
    exists. Caller is responsible for conn.commit().
    """
    if not unit_code or not unit_code.strip():
        raise ValueError("unit_code must not be empty")
    unit_code = unit_code.strip()

    canonical_condition = _normalize_condition(condition)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM units WHERE unit_code = %s", [unit_code])
        existing_unit = cur.fetchone()

    if existing_unit is not None:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM products WHERE id = %s", [existing_unit["product_id"]])
            existing_product = cur.fetchone()
        return dict(existing_unit), dict(existing_product), False, False

    sku_for_product = unit_code if not is_interchangeable(canonical_condition) else None
    product, product_created = find_or_create_product(
        conn,
        brand=brand,
        model=model,
        style_code=style_code,
        gender=gender,
        size=size,
        condition=canonical_condition,
        sku=sku_for_product,
    )

    location_id = None
    if location_code:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM locations WHERE code = %s", [location_code])
            loc = cur.fetchone()
            if loc:
                location_id = loc[0]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO units (id, unit_code, product_id, location_id,
                               cost_basis, notes, status)
            VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, 'ready_to_list')
            RETURNING *
            """,
            [unit_code, product["id"], location_id, cost_basis, notes],
        )
        unit = dict(cur.fetchone())

    return unit, product, True, product_created
