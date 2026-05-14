"""EPIC 1 Ticket 1.3 — idempotent product lookup-or-create (psycopg2).

find_or_create_product() is the single entry point for attaching a canonical
Product ID to an incoming SKU.  The caller controls the DB commit so that
multiple operations can be batched in one transaction.

Uses INSERT ... ON CONFLICT (product_id) DO NOTHING RETURNING * which is
race-safe and cleaner than the former SQLAlchemy IntegrityError retry.
"""
import psycopg2.extras

from src.services.product_id_service import generate_product_id, is_interchangeable


def find_or_create_product(conn, *, brand: str, model: str, style_code: str,
                           gender: str, size: str, condition: str,
                           sku: str = None) -> tuple:
    """Return (product_dict, created: bool).

    Generates a deterministic product_id then upserts via ON CONFLICT DO NOTHING.
    If the row already existed the insert returns nothing; a follow-up SELECT
    fetches the winner's row.  Caller is responsible for conn.commit().
    """
    pid = generate_product_id(
        brand=brand, model=model, style_code=style_code,
        gender=gender, size=size, condition=condition, sku=sku,
    )

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO products (id, product_id, brand, model, style_code, gender, size,
                                  condition_code, is_interchangeable)
            VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            RETURNING *
            """,
            [pid, brand, model, style_code, gender, size,
             condition.upper(), is_interchangeable(condition)],
        )
        row = cur.fetchone()
        if row is not None:
            return dict(row), True

        # Row already existed — fetch it
        cur.execute("SELECT * FROM products WHERE product_id = %s", [pid])
        row = cur.fetchone()
        return dict(row), False
