"""
Listing service — product-based listing management.

Enforces marketplace ToS:
  - Mercari and Poshmark: single_quantity only (supports_multi_quantity = FALSE on channel)
  - eBay: multi_quantity allowed when product is interchangeable (NEW / LIKE_NEW)

All three public functions expect caller to own the commit.
"""
import json
import logging
import psycopg2.extras
from datetime import datetime
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


def create_listing(
    conn,
    *,
    product_id: str,
    channel_name: str,
    title: str,
    price: float,
    description: str = None,
    photos=None,
    item_specifics=None,
    channel_listing_id: str = None,
    listing_url: str = None,
    status: str = "draft",
) -> Tuple[Dict, bool]:
    """
    Create a listing for a product on a channel.

    Mode is determined automatically:
      multi_quantity  — only when channel.supports_multi_quantity AND product.is_interchangeable
      single_quantity — all other cases (non-interchangeable product or ToS-restricted channel)

    Returns (listing_dict, created=True).
    Caller owns commit.
    Raises ValueError if product_id or channel_name not found.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, is_interchangeable FROM products WHERE id = %s",
            [product_id],
        )
        product = cur.fetchone()
    if not product:
        raise ValueError("Product not found")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, supports_multi_quantity FROM channels WHERE LOWER(name) = LOWER(%s)",
            [channel_name],
        )
        channel = cur.fetchone()
    if not channel:
        raise ValueError(f"Channel not found: {channel_name}")

    mode = (
        "multi_quantity"
        if channel["supports_multi_quantity"] and product["is_interchangeable"]
        else "single_quantity"
    )

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO listings
                (id, product_id, channel_id, title, description, current_price,
                 photos, item_specifics, channel_listing_id, listing_url, status, mode)
            VALUES
                (gen_random_uuid(), %s, %s, %s, %s, %s,
                 %s::jsonb, %s::jsonb, %s, %s, %s, %s)
            RETURNING *
            """,
            [
                product["id"],
                channel["id"],
                title,
                description,
                price,
                json.dumps(photos) if photos is not None else None,
                json.dumps(item_specifics) if item_specifics is not None else None,
                channel_listing_id,
                listing_url,
                status,
                mode,
            ],
        )
        listing = dict(cur.fetchone())

    return listing, True


def assign_unit_to_listing(conn, *, listing_id: str, unit_id: str) -> Dict:
    """
    Assign a unit to a listing and mark the unit as 'listed'.

    single_quantity listings may have at most one unit.
    A unit may not be assigned to the same listing twice.

    Returns the new listing_units row as a dict.
    Caller owns commit.
    Raises ValueError on invalid state.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, mode FROM listings WHERE id = %s", [listing_id])
        listing = cur.fetchone()
    if not listing:
        raise ValueError("Listing not found")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id FROM units WHERE id = %s", [unit_id])
        unit = cur.fetchone()
    if not unit:
        raise ValueError("Unit not found")

    if listing["mode"] == "single_quantity":
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM listing_units WHERE listing_id = %s",
                [listing_id],
            )
            if cur.fetchone()[0] > 0:
                raise ValueError("single_quantity listing already has a unit assigned")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM listing_units WHERE listing_id = %s AND unit_id = %s",
            [listing_id, unit_id],
        )
        if cur.fetchone():
            raise ValueError("Unit already assigned to this listing")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO listing_units (id, listing_id, unit_id) "
            "VALUES (gen_random_uuid(), %s, %s) RETURNING *",
            [listing_id, unit_id],
        )
        lu = dict(cur.fetchone())

    with conn.cursor() as cur:
        cur.execute("UPDATE units SET status = 'listed' WHERE id = %s", [unit_id])

    return lu


def end_listing(conn, *, listing_id: str) -> Dict:
    """
    End a listing.  Units that are not already sold or shipped are reverted
    to 'ready_to_list'.

    Returns the updated listing dict.
    Caller owns commit.
    Raises ValueError if listing not found.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id FROM listings WHERE id = %s", [listing_id])
        if not cur.fetchone():
            raise ValueError("Listing not found")

    now = datetime.utcnow()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "UPDATE listings SET status = 'ended', ended_at = %s "
            "WHERE id = %s RETURNING *",
            [now, listing_id],
        )
        listing = dict(cur.fetchone())

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT u.id, u.status
            FROM units u
            JOIN listing_units lu ON lu.unit_id = u.id
            WHERE lu.listing_id = %s
            """,
            [listing_id],
        )
        attached_units = cur.fetchall()

    with conn.cursor() as cur:
        for u in attached_units:
            if u["status"] not in ("sold", "shipped"):
                cur.execute(
                    "UPDATE units SET status = 'ready_to_list' WHERE id = %s",
                    [u["id"]],
                )

    return listing
