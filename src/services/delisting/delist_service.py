"""
Delist Service - Main Delisting Coordinator
Handles delisting logic when items sell on any platform
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2.extras

from src.services.delisting.gmail_service import GmailService
from src.services.order_allocation_service import allocate_order

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
            order, allocations, created = allocate_order(self.conn, parsed_sale=parsed_email)

            if order['needs_reconciliation']:
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

                self.conn.commit()
                results['errors'].append('Unit not found by SKU or listing_id')
                return results

            results['unit_found'] = True
            results['unit_updated'] = True
            results['order_id'] = str(order['id'])

            # Cross-platform delisting for each allocated unit
            for alloc in allocations:
                unit_id = alloc['unit_id']
                listings = self._find_unit_listings(unit_id)
                results['listings_found'].extend([str(l['id']) for l in listings])

                with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT unit_code FROM units WHERE id = %s", [str(unit_id)])
                    row = cur.fetchone()
                    unit_code = row['unit_code'] if row else str(unit_id)
                results['unit_code'] = unit_code
                logger.info(f"Found {len(listings)} listings for unit {unit_code}")

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
            logger.info(f"Sale processed: Order {order['id']}, Delisted from {len(results['delisted'])} platforms")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error processing sale: {e}")
            results['errors'].append(str(e))

        return results

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
            from src.integrations.ebay.ebay_delist import delist_ebay_item
            return delist_ebay_item(listing['channel_listing_id'])

        return {'success': False, 'error': f'Unknown platform: {platform}'}


# ---------------------------------------------------------------------------
# Module-level pure-DB functions (caller owns commit)
# ---------------------------------------------------------------------------

def find_active_listings_for_unit(conn, unit_id: str) -> List[dict]:
    """
    Return all active listings for a unit, including channel_name in each row.

    Joins listings → listing_units → channels so callers don't need a second query.
    Returns empty list if none found.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT l.*, LOWER(c.name) AS channel_name
            FROM listings l
            JOIN listing_units lu ON lu.listing_id = l.id
            JOIN channels c ON c.id = l.channel_id
            WHERE lu.unit_id = %s AND l.status = 'active'
            """,
            [str(unit_id)],
        )
        return [dict(r) for r in cur.fetchall()]


def mark_listing_sold(conn, *, listing_id: str, sold_price: float = None) -> Dict:
    """
    Mark a listing as 'sold', setting sold_at and ended_at to now().

    Returns updated listing dict.
    Caller owns commit. Raises ValueError if listing not found.
    """
    now = datetime.utcnow()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "UPDATE listings SET status='sold', sold_at=%s, sold_price=%s, ended_at=%s "
            "WHERE id=%s RETURNING *",
            [now, sold_price, now, listing_id],
        )
        row = cur.fetchone()
    if not row:
        raise ValueError("Listing not found")
    return dict(row)


def mark_listing_ended(conn, *, listing_id: str) -> Dict:
    """
    Mark a listing as 'ended', setting ended_at to now().

    Returns updated listing dict.
    Caller owns commit. Raises ValueError if listing not found.
    """
    now = datetime.utcnow()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "UPDATE listings SET status='ended', ended_at=%s WHERE id=%s RETURNING *",
            [now, listing_id],
        )
        row = cur.fetchone()
    if not row:
        raise ValueError("Listing not found")
    return dict(row)
