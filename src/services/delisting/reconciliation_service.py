import logging
from datetime import datetime, timedelta

import psycopg2.extras

logger = logging.getLogger(__name__)


class ReconciliationService:
    """Finds mismatches between sold units and active listings."""

    def __init__(self, conn):
        self.conn = conn

    def run_daily_reconciliation(self):
        logger.info("Starting reconciliation job...")

        sold_units = self._get_recent_sold_units()
        logger.info(f"Checking {len(sold_units)} sold units")

        for unit in sold_units:
            active_listings = self._get_active_listings_for_unit(unit['id'])
            for listing in active_listings:
                platform = self._get_listing_platform(listing)
                logger.warning(
                    f"RECON ISSUE: Unit {unit['unit_code']} sold but still active on {platform}"
                )
                self._create_reconciliation_alert(
                    unit=unit, listing=listing, platform=platform, issue='sold_unit_still_active'
                )

        logger.info("Reconciliation job complete")

    def _get_recent_sold_units(self):
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM units WHERE status = 'sold' AND updated_at >= %s",
                [seven_days_ago],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_active_listings_for_unit(self, unit_id):
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT l.* FROM listings l
                JOIN listing_units lu ON lu.listing_id = l.id
                WHERE lu.unit_id = %s AND l.status = 'active'
                """,
                [str(unit_id)],
            )
            return [dict(r) for r in cur.fetchall()]

    def _get_listing_platform(self, listing: dict) -> str:
        if listing.get('channel_id'):
            with self.conn.cursor() as cur:
                cur.execute("SELECT LOWER(name) FROM channels WHERE id = %s", [listing['channel_id']])
                row = cur.fetchone()
                if row:
                    return row[0]
        return str(listing.get('channel_id', 'unknown'))

    def _create_reconciliation_alert(self, unit, listing, platform, issue):
        logger.error(
            f"[RECON ALERT] unit={unit['unit_code']} listing={listing['id']} platform={platform} issue={issue}"
        )
