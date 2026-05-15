"""
Order allocation service — creates Order + OrderAllocation rows from a parsed sale event.

Called by delist_service after email parsing.  Caller owns commit.
Returns (order_dict, allocations_list, created: bool).
"""
import json
import logging
import psycopg2.extras
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from src.services.listing_service import update_listing_on_unit_sold

logger = logging.getLogger(__name__)


def allocate_order(conn, *, parsed_sale: dict) -> Tuple[Dict, List[Dict], bool]:
    """
    Create an Order for a parsed sale and allocate matching Units to it.

    parsed_sale keys (produced by email_parser_service outputs):
        platform          str  — 'ebay' | 'poshmark' | 'mercari'
        message_id        str  — email Message-ID (required)
        sku               str | None
        order_id          str | None  — platform order ID (absent for Mercari)
        listing_id        str | None  — channel_listing_id value
        price             float | None
        title             str | None
        buyer_name        str | None

    Returns (order_dict, allocations_list, created).
    created=False when the order already existed (idempotent re-call).
    Caller owns commit.
    Raises ValueError on missing required fields.
    """
    platform = parsed_sale.get('platform')
    message_id = parsed_sale.get('message_id')
    if not platform:
        raise ValueError("parsed_sale must include 'platform'")
    if not message_id:
        raise ValueError("parsed_sale must include 'message_id'")

    platform_order_id = parsed_sale.get('order_id') or None
    platform_listing_id = parsed_sale.get('listing_id') or None
    sku = str(parsed_sale['sku']).strip().upper() if parsed_sale.get('sku') else None
    sale_price = parsed_sale.get('price')
    buyer_name = parsed_sale.get('buyer_name')

    # ------------------------------------------------------------------
    # 1. Idempotency — return existing order if already processed
    # ------------------------------------------------------------------
    existing = _find_existing_order(conn, platform, platform_order_id, message_id)
    if existing:
        allocations = _fetch_allocations(conn, existing['id'])
        logger.info("allocate_order: returning existing order %s", existing['id'])
        return existing, allocations, False

    # ------------------------------------------------------------------
    # 2. Create the order row (status='pending')
    # ------------------------------------------------------------------
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO orders
                (id, platform, platform_order_id, platform_listing_id,
                 message_id, status, sale_price, buyer_name, raw_payload)
            VALUES
                (gen_random_uuid(), %s, %s, %s, %s, 'pending', %s, %s, %s::jsonb)
            RETURNING *
            """,
            [
                platform,
                platform_order_id,
                platform_listing_id,
                message_id,
                sale_price,
                buyer_name,
                json.dumps({k: v for k, v in parsed_sale.items() if isinstance(v, (str, int, float, bool, type(None)))}),
            ],
        )
        order = dict(cur.fetchone())

    # ------------------------------------------------------------------
    # 3. Resolve unit(s) — SKU first, listing_id fallback
    # ------------------------------------------------------------------
    unit = None

    if sku:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, unit_code, status FROM units WHERE LOWER(unit_code) = LOWER(%s) AND status != 'sold' LIMIT 1",
                [sku],
            )
            row = cur.fetchone()
            if row:
                unit = dict(row)
                logger.info("allocate_order: found unit by SKU %s", sku)

    if not unit and platform_listing_id:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.unit_code, u.status
                FROM units u
                JOIN listing_units lu ON lu.unit_id = u.id
                JOIN listings l ON l.id = lu.listing_id
                WHERE l.channel_listing_id = %s AND u.status != 'sold'
                LIMIT 1
                """,
                [platform_listing_id],
            )
            row = cur.fetchone()
            if row:
                unit = dict(row)
                logger.info("allocate_order: found unit via listing_id fallback %s", platform_listing_id)

    if not unit:
        # Mark as needing reconciliation and return early
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE orders
                SET status = 'needs_reconciliation', needs_reconciliation = true
                WHERE id = %s
                RETURNING *
                """,
                [order['id']],
            )
            order = dict(cur.fetchone())
        logger.warning(
            "allocate_order: unit not found (SKU=%s, listing_id=%s) — marked needs_reconciliation",
            sku, platform_listing_id,
        )
        return order, [], True

    # ------------------------------------------------------------------
    # 4. Create allocation row
    # ------------------------------------------------------------------
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO order_allocations (id, order_id, unit_id, listing_id)
            VALUES (gen_random_uuid(), %s, %s, %s)
            ON CONFLICT (order_id, unit_id) DO NOTHING
            RETURNING *
            """,
            [order['id'], unit['id'], None],
        )
        alloc_row = cur.fetchone()

    if alloc_row:
        allocation = dict(alloc_row)
    else:
        existing = _fetch_allocations(conn, order['id'])
        if not existing:
            raise ValueError(f"Allocation missing for order {order['id']} — possible concurrent duplicate")
        allocation = existing[0]

    # ------------------------------------------------------------------
    # 5. Mark unit sold
    # ------------------------------------------------------------------
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE units
            SET status = 'sold', sold_at = %s, sold_price = %s, sold_platform = %s
            WHERE id = %s
            """,
            [now, sale_price, platform, unit['id']],
        )

    # ------------------------------------------------------------------
    # 5a. Update listing lifecycle based on unit sold
    # ------------------------------------------------------------------
    update_listing_on_unit_sold(conn, unit_id=unit['id'])

    # ------------------------------------------------------------------
    # 6. Update order to 'allocated'
    # ------------------------------------------------------------------
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "UPDATE orders SET status = 'allocated', allocated_at = %s WHERE id = %s RETURNING *",
            [now, order['id']],
        )
        order = dict(cur.fetchone())

    logger.info("allocate_order: allocated order %s to unit %s", order['id'], unit['id'])
    return order, [allocation], True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_existing_order(conn, platform: str, platform_order_id, message_id: str):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if platform_order_id:
            cur.execute(
                "SELECT * FROM orders WHERE platform = %s AND platform_order_id = %s LIMIT 1",
                [platform, platform_order_id],
            )
            row = cur.fetchone()
            if row:
                return dict(row)
        cur.execute(
            "SELECT * FROM orders WHERE platform = %s AND message_id = %s LIMIT 1",
            [platform, message_id],
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _fetch_allocations(conn, order_id) -> List[Dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM order_allocations WHERE order_id = %s",
            [str(order_id)],
        )
        return [dict(r) for r in cur.fetchall()]
