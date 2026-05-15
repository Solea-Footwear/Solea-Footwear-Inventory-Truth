"""
Marketplace event service — records sale events from email parsing into marketplace_events.

Follows the same caller-owns-commit pattern as listing_service and order_allocation_service.
"""
import json
import logging
import psycopg2.extras
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def record_marketplace_event(
    conn,
    *,
    platform: str,
    message_id: str,
    event_type: str,
    external_listing_id: str = None,
    external_order_id: str = None,
    sku: str = None,
    raw_payload: dict = None,
) -> Tuple[Dict, bool]:
    """
    Insert a marketplace event row, deduplicating on (platform, message_id).

    Returns (event_dict, created).
    created=False means the row already existed (idempotent re-call).
    Caller owns commit.
    Raises ValueError on missing required fields.
    """
    if not platform:
        raise ValueError("platform is required")
    if not message_id:
        raise ValueError("message_id is required")
    if not event_type:
        raise ValueError("event_type is required")
    if event_type.lower() == "sale" and not sku:
        raise ValueError("sku is required for sale events")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO marketplace_events
                (platform, event_type, message_id, external_listing_id,
                 external_order_id, sku, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (platform, message_id) DO NOTHING
            RETURNING *
            """,
            [
                platform,
                event_type,
                message_id,
                external_listing_id,
                external_order_id,
                sku,
                json.dumps(raw_payload) if raw_payload is not None else None,
            ],
        )
        row = cur.fetchone()

    if row:
        return dict(row), True

    # Duplicate — fetch the existing row
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM marketplace_events WHERE platform = %s AND message_id = %s",
            [platform, message_id],
        )
        existing = cur.fetchone()
    if not existing:
        logger.warning(
            "record_marketplace_event: duplicate row vanished before SELECT (platform=%s, message_id=%s)",
            platform, message_id,
        )
        return None, False
    return dict(existing), False


def resolve_mercari_sku(conn, *, mercari_listing_id: str) -> Optional[str]:
    """
    Look up the unit_code (SKU) for a Mercari listing by its channel_listing_id.

    Returns the SKU string if found, else None.
    Read-only. Caller owns commit.
    """
    if not mercari_listing_id:
        return None

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.unit_code
            FROM listings l
            JOIN channels c ON c.id = l.channel_id
            JOIN listing_units lu ON lu.listing_id = l.id
            JOIN units u ON u.id = lu.unit_id
            WHERE LOWER(c.name) = 'mercari'
              AND l.channel_listing_id = %s
            LIMIT 1
            """,
            [mercari_listing_id],
        )
        row = cur.fetchone()
    return row[0] if row else None
