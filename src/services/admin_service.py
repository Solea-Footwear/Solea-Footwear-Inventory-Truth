"""
Admin service — read-only queries backing the Admin UI (EPIC 8).

Caller-owns-commit pattern: neither function calls conn.commit().
Both functions are SELECT-only, so no commit is ever needed.
"""
import psycopg2.extras
from typing import Dict, List, Optional


def get_product_detail(conn, *, product_id: str) -> Dict:
    """
    Return a product's full detail including aggregated unit counts and active listing count.

    product_id is the UUID string from products.id (not the canonical product_id string).

    Raises ValueError("Product not found") if no matching row exists.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                p.id,
                p.product_id,
                p.brand,
                p.model,
                p.size,
                p.condition_code,
                p.is_interchangeable,
                COUNT(u.id)                                              AS total_quantity,
                COUNT(u.id) FILTER (WHERE u.status = 'ready_to_list')   AS available_skus,
                COUNT(u.id) FILTER (WHERE u.status = 'reserved')        AS reserved_skus,
                COUNT(u.id) FILTER (WHERE u.status = 'sold')            AS sold_skus,
                (
                    SELECT COUNT(*)
                    FROM listings l
                    WHERE l.product_id = p.id
                      AND l.status = 'active'
                ) AS active_listings
            FROM products p
            LEFT JOIN units u ON u.product_id = p.id
            WHERE p.id = %s
            GROUP BY p.id, p.product_id, p.brand, p.model, p.size,
                     p.condition_code, p.is_interchangeable
            """,
            [product_id],
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError("Product not found")

    return {
        "id": str(row["id"]),
        "product_id": row["product_id"],
        "brand": row["brand"],
        "model": row["model"],
        "size": row["size"],
        "condition_code": row["condition_code"],
        "is_interchangeable": bool(row["is_interchangeable"]),
        "total_quantity": int(row["total_quantity"]),
        "available_skus": int(row["available_skus"]),
        "reserved_skus": int(row["reserved_skus"]),
        "sold_skus": int(row["sold_skus"]),
        "active_listings": int(row["active_listings"]),
    }


def get_sku_list(conn, *, status: Optional[str] = None) -> List[Dict]:
    """
    Return all units with their canonical product_id string, status, and location code.

    Optional status filter. Returns an empty list when no units match.
    Results are ordered by unit_code ascending.
    """
    sql = """
        SELECT
            u.unit_code,
            p.id         AS product_uuid,
            p.product_id,
            u.status,
            loc.code     AS location_code
        FROM units u
        JOIN products p        ON p.id  = u.product_id
        LEFT JOIN locations loc ON loc.id = u.location_id
        {where}
        ORDER BY u.unit_code
    """

    if status is not None:
        query = sql.format(where="WHERE u.status = %s")
        params = [status]
    else:
        query = sql.format(where="")
        params = []

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        {
            "unit_code": row["unit_code"],
            "product_uuid": str(row["product_uuid"]),
            "product_id": row["product_id"],
            "status": row["status"],
            "location_code": row["location_code"],
        }
        for row in rows
    ]
