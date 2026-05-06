import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ReconciliationService:
    """
    Finds mismatches between sold units and active listings.
    """

    def __init__(self, db):
        self.db = db

    def run_daily_reconciliation(self):
        logger.info("Starting reconciliation job...")

        sold_units = self._get_recent_sold_units()

        logger.info(f"Checking {len(sold_units)} sold units")

        for unit in sold_units:
            active_listings = self._get_active_listings_for_unit(unit.id)

            for listing in active_listings:
                platform = self._get_listing_platform(listing)

                logger.warning(
                    f"❗ RECON ISSUE: Unit {unit.unit_code} sold but still active on {platform}"
                )

                self._create_reconciliation_alert(
                    unit=unit,
                    listing=listing,
                    platform=platform,
                    issue='sold_unit_still_active'
                )

        logger.info("Reconciliation job complete")

    def _get_recent_sold_units(self):
        from database import Unit

        seven_days_ago = datetime.utcnow() - timedelta(days=7)

        return self.db.query(Unit).filter(
            Unit.status == 'sold',
            Unit.updated_at >= seven_days_ago
        ).all()

    def _get_active_listings_for_unit(self, unit_id):
        from database import Listing, ListingUnit

        return self.db.query(Listing).join(ListingUnit).filter(
            ListingUnit.unit_id == unit_id,
            Listing.status == 'active'
        ).all()

    def _get_listing_platform(self, listing):
        return listing.channel_id

    def _create_reconciliation_alert(self, unit, listing, platform, issue):
        logger.error(
            f"[RECON ALERT] unit={unit.unit_code} listing={listing.id} platform={platform} issue={issue}"
        )
