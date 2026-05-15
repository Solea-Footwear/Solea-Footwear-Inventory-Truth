"""
Migration service — EPIC 9 backfill and exception reporting.

Caller-owns-commit pattern:
  - backfill_product_ids() performs UPDATEs; caller must commit (or rollback on error).
  - get_exception_report() is SELECT-only; no commit needed.
"""
import psycopg2.extras
from typing import Dict, List

from src.services.product_id_service import generate_product_id, is_interchangeable


def backfill_product_ids(conn) -> Dict:
    """
    For every product where product_id IS NULL, attempt to generate and write the
    canonical product_id string using the existing generate_product_id() logic.

    Skips a product (and increments the appropriate counter) when:
      - style_code or condition_code is missing  → skipped_incomplete
      - condition is non-interchangeable but no unit exists  → skipped_no_unit
      - the generated ID already exists on another product  → skipped_conflict

    Returns a dict with counts:
      backfilled, skipped_incomplete, skipped_no_unit, skipped_conflict
    """
    backfilled = 0
    skipped_incomplete = 0
    skipped_no_unit = 0
    skipped_conflict = 0

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as read_cur:
        read_cur.execute(
            """
            SELECT id, brand, model, style_code, gender, size, condition_code
            FROM products
            WHERE product_id IS NULL
            """
        )
        candidates = read_cur.fetchall()

    for row in candidates:
        if not row["style_code"] or not row["condition_code"]:
            skipped_incomplete += 1
            continue

        sku = None
        if not is_interchangeable(row["condition_code"]):
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as unit_cur:
                unit_cur.execute(
                    "SELECT unit_code FROM units WHERE product_id = %s LIMIT 1",
                    [row["id"]],
                )
                unit = unit_cur.fetchone()
            if unit is None:
                skipped_no_unit += 1
                continue
            sku = unit["unit_code"]

        generated_id = generate_product_id(
            brand=row["brand"],
            model=row["model"],
            style_code=row["style_code"],
            gender=row["gender"] or "",
            size=row["size"],
            condition=row["condition_code"],
            sku=sku,
        )

        with conn.cursor() as write_cur:
            write_cur.execute(
                "SELECT 1 FROM products WHERE product_id = %s AND id != %s",
                [generated_id, row["id"]],
            )
            if write_cur.fetchone():
                skipped_conflict += 1
                continue

            write_cur.execute(
                "UPDATE products SET product_id = %s WHERE id = %s AND product_id IS NULL",
                [generated_id, row["id"]],
            )
            backfilled += 1

    return {
        "backfilled": backfilled,
        "skipped_incomplete": skipped_incomplete,
        "skipped_no_unit": skipped_no_unit,
        "skipped_conflict": skipped_conflict,
    }


def get_exception_report(conn) -> List[Dict]:
    """
    Return all units whose parent product is missing style_code, condition_code,
    or size — fields required to generate a product_id.

    Each result includes a computed `missing_fields` list naming which fields are absent.
    Results are ordered by unit_code ascending.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                u.unit_code,
                u.status,
                p.id          AS product_uuid,
                p.product_id,
                p.brand,
                p.model,
                p.size,
                p.style_code,
                p.condition_code
            FROM units u
            JOIN products p ON p.id = u.product_id
            WHERE p.style_code IS NULL
               OR p.condition_code IS NULL
               OR p.size IS NULL
               OR trim(p.size) = ''
            ORDER BY u.unit_code
            """
        )
        rows = cur.fetchall()

    result = []
    for row in rows:
        missing = []
        if not row["style_code"]:
            missing.append("style_code")
        if not row["condition_code"]:
            missing.append("condition_code")
        if not row["size"] or not str(row["size"]).strip():
            missing.append("size")

        result.append({
            "unit_code": row["unit_code"],
            "status": row["status"],
            "product_uuid": str(row["product_uuid"]),
            "product_id": row["product_id"],
            "brand": row["brand"],
            "model": row["model"],
            "size": row["size"],
            "style_code": row["style_code"],
            "condition_code": row["condition_code"],
            "missing_fields": missing,
        })

    return result
