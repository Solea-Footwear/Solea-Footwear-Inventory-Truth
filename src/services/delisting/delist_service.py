"""
Delist Service - Main Delisting Coordinator
Handles delisting logic when items sell on any platform
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2.extras

from src.services.delisting.gmail_service import GmailService

logger = logging.getLogger(__name__)


class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


class DelistService:
    """Main service for coordinating cross-platform delisting"""

    def __init__(self, conn):
        self.conn = conn

    def process_sale(self, parsed_email: Dict) -> Dict:
        logger.info(f"Processing sale from {parsed_email.get('platform')}")
        results = {
            'success': False,
            'unit_found': False,
            'unit_updated': False,
            'listings_found': [],
            'delisted': [],
            'errors': [],
        }

        try:
            unit = self._find_unit(parsed_email)

            if not unit:
                email_subject = parsed_email.get('title', 'Unknown subject')[:100]
                sku = parsed_email.get('sku', 'N/A')
                order_id = parsed_email.get('order_id', 'N/A')
                platform = parsed_email.get('platform', 'unknown')

                print(f"\n{Colors.RED}{Colors.BOLD}{'='*80}{Colors.END}")
                print(f"{Colors.RED}{Colors.BOLD}❌ UNIT NOT FOUND ❌{Colors.END}")
                print(f"{Colors.YELLOW}Subject:{Colors.END} {email_subject}")
                print(f"{Colors.YELLOW}SKU:{Colors.END} {sku} | {Colors.YELLOW}Order:{Colors.END} {order_id} | {Colors.YELLOW}Platform:{Colors.END} {platform}")
                print(f"{Colors.RED}{Colors.BOLD}{'='*80}{Colors.END}\n")
                logger.warning(f"Unit not found: SKU={sku}, Order={order_id}, Subject={email_subject}")

                message_id = parsed_email.get('message_id')
                if message_id:
                    try:
                        gmail = GmailService()
                        success = gmail.move_to_label(message_id, 'eBay Sales Not In System', remove_inbox=True)
                        if success:
                            logger.info("Email moved to 'eBay Sales Not In System' label and archived")
                        else:
                            logger.warning("Failed to move email to label")
                    except Exception as e:
                        logger.error(f"Error moving email to label: {e}")

                results['errors'].append('Unit not found by SKU or title')
                return results

            results['unit_found'] = True
            results['unit_code'] = unit['unit_code']

            self._update_unit_sold(unit, parsed_email)
            results['unit_updated'] = True

            listings = self._find_unit_listings(unit['id'])
            results['listings_found'] = [str(l['id']) for l in listings]
            logger.info(f"Found {len(listings)} listings for unit {unit['unit_code']}")

            sold_platform = parsed_email.get('platform')

            for listing in listings:
                try:
                    listing_platform = self._get_listing_platform(listing)

                    if listing_platform == sold_platform:
                        self._update_listing_sold(listing, parsed_email)
                        logger.info(f"Updated {listing_platform} listing as sold")
                    else:
                        delist_result = self._delist_from_platform(listing, listing_platform)

                        if delist_result['success']:
                            self._update_listing_ended(listing)
                            results['delisted'].append({
                                'platform': listing_platform,
                                'listing_id': str(listing['id']),
                                'status': 'delisted',
                            })
                            logger.info(f"Delisted from {listing_platform}")
                        else:
                            error_msg = str(delist_result.get('error', '')).lower()
                            already_ended_indicators = [
                                'already been closed', 'already closed',
                                'auction has been closed', 'listing has ended', 'no longer available',
                            ]
                            if any(indicator in error_msg for indicator in already_ended_indicators):
                                logger.warning(f"{listing_platform} listing already ended on platform, syncing database status")
                                self._update_listing_ended(listing)
                                results['delisted'].append({
                                    'platform': listing_platform,
                                    'listing_id': str(listing['id']),
                                    'status': 'sync_ended',
                                })
                            else:
                                results['errors'].append({'platform': listing_platform, 'error': delist_result.get('error')})
                                logger.error(f"Failed to delist from {listing_platform}: {delist_result.get('error')}")

                except Exception as e:
                    logger.error(f"Error processing listing {listing['id']}: {e}")
                    results['errors'].append({'listing_id': str(listing['id']), 'error': str(e)})

            self.conn.commit()
            results['success'] = True
            logger.info(f"Sale processed: Unit {unit['unit_code']}, Delisted from {len(results['delisted'])} platforms")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error processing sale: {e}")
            results['errors'].append(str(e))

        return results

    def _find_unit(self, parsed_email: Dict) -> Optional[dict]:
        """Find unit by SKU first, then channel_listing_id fallback."""
        platform = parsed_email.get('platform')
        listing_id = parsed_email.get('listing_id')
        order_id = parsed_email.get('order_id')

        raw_sku = parsed_email.get('sku')
        sku = str(raw_sku).strip().upper() if raw_sku else None

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if sku:
                cur.execute("SELECT * FROM units WHERE unit_code = %s LIMIT 1", [sku])
                unit = cur.fetchone()
                if unit:
                    logger.info(f"Found unit by SKU FIRST: {sku}")
                    return dict(unit)

            if listing_id:
                cur.execute(
                    """
                    SELECT u.* FROM units u
                    JOIN listing_units lu ON lu.unit_id = u.id
                    JOIN listings l ON l.id = lu.listing_id
                    WHERE l.channel_listing_id = %s
                    LIMIT 1
                    """,
                    [listing_id],
                )
                unit = cur.fetchone()
                if unit:
                    logger.debug(f"Found unit by listing_id: {unit['unit_code']}")
                    return dict(unit)

        logger.warning(f"Unit not found for platform={platform}, listing_id={listing_id}, order_id={order_id}, sku={sku}")
        return None

    def _update_unit_sold(self, unit: dict, parsed_email: Dict):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE units
                SET status='sold', sold_at=%s, sold_price=%s, sold_platform=%s
                WHERE id=%s
                """,
                [datetime.utcnow(), parsed_email.get('price'), parsed_email.get('platform'), unit['id']],
            )
        logger.debug(f"Unit {unit['unit_code']} marked as sold")

    def _find_unit_listings(self, unit_id) -> List[dict]:
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
        return 'unknown'

    def _update_listing_sold(self, listing: dict, parsed_email: Dict):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE listings SET status='sold', sold_at=%s, sold_price=%s, ended_at=%s WHERE id=%s",
                [datetime.utcnow(), parsed_email.get('price'), datetime.utcnow(), listing['id']],
            )
        logger.debug(f"Listing {listing['id']} marked as sold")

    def _update_listing_ended(self, listing: dict):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE listings SET status='ended', ended_at=%s WHERE id=%s",
                [datetime.utcnow(), listing['id']],
            )
        logger.debug(f"Listing {listing['id']} marked as ended")

    def _delist_from_platform(self, listing: dict, platform: str) -> Dict:
        if platform == 'ebay':
            print(f"delisting from {platform}")
            print(listing['channel_listing_id'])
            from src.integrations.ebay.ebay_delist import delist_ebay_item
            return delist_ebay_item(listing['channel_listing_id'])

        elif platform in ['poshmark', 'mercari']:
            print(f"delisting from {platform}")
            print(listing['channel_listing_id'])
            from src.integrations.selenium.selenium_delist import delist_item
            return delist_item(platform, listing['channel_listing_id'])

        else:
            return {'success': False, 'error': f'Unknown platform: {platform}'}
