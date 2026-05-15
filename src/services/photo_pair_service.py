"""
Photo pair service — post-photography SKU assignment workflow (EPIC 7 Ticket 7.1).

Units are created HERE, after photography, never at intake time.
Follows the caller-owns-commit pattern.
"""
import logging
import psycopg2.extras
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def assign_sku_to_photo_pair(
    conn,
    *,
    unit_code: str,
    product_id: str,
    location_id: str = None,
    cost_basis: float = None,
    notes: str = None,
) -> Dict:
    """
    Create a unit record after photography and SKU assignment.

    This is the single post-photography entry point. Units are never created
    as temporary placeholders — the SKU must be known before calling this.

    Returns the new unit dict with status='ready_to_list'.
    Caller owns commit.
    Raises ValueError if product not found or unit_code already assigned.
    """
    if not unit_code or not unit_code.strip():
        raise ValueError("unit_code is required")
    if not product_id:
        raise ValueError("product_id is required")

    unit_code = unit_code.strip()

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM products WHERE id = %s", [product_id])
        if not cur.fetchone():
            raise ValueError(f"Product not found: {product_id}")

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM units WHERE unit_code = %s", [unit_code])
        if cur.fetchone():
            raise ValueError(f"unit_code already assigned: {unit_code}")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO units
                (id, unit_code, product_id, location_id, cost_basis, notes, status)
            VALUES
                (gen_random_uuid(), %s, %s, %s, %s, %s, 'ready_to_list')
            RETURNING *
            """,
            [unit_code, product_id, location_id, cost_basis, notes],
        )
        unit = dict(cur.fetchone())

    logger.info("assign_sku_to_photo_pair: created unit %s for product %s", unit_code, product_id)
    return unit


def get_sku_inventory_count(
    conn,
    *,
    product_id: str = None,
) -> int:
    """
    Count units available for selling that carry a SKU.

    Counts status IN ('ready_to_list', 'listed', 'reserved').
    Excludes sold, shipped, damaged, returned.
    Because unit_code is NOT NULL in the schema, every unit in the DB
    has a SKU by definition — photo pairs without SKUs cannot exist.

    Optional product_id narrows the count to one product.
    Read-only. Caller owns commit.
    """
    if product_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM units
                WHERE unit_code IS NOT NULL
                  AND product_id = %s
                  AND status IN ('ready_to_list', 'listed', 'reserved')
                """,
                [product_id],
            )
            return cur.fetchone()[0]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM units
            WHERE unit_code IS NOT NULL
              AND status IN ('ready_to_list', 'listed', 'reserved')
            """
        )
        return cur.fetchone()[0]
