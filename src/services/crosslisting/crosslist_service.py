"""
Cross-listing Service
Main coordinator for automated listing creation on multiple platforms
"""
import logging
from datetime import datetime
from typing import Dict, List

import psycopg2.extras

logger = logging.getLogger(__name__)

POSHMARK_DAILY_CAP = 300


class CrosslistService:
    """Service for managing cross-listing to multiple platforms"""

    def __init__(self, conn):
        self.conn = conn

    def check_and_crosslist(self, unit_id) -> Dict:
        """Check if unit needs cross-listing and create listings on missing platforms."""
        logger.info(f"Checking cross-listing for unit {unit_id}")
        results = {
            'unit_id': str(unit_id),
            'needs_crosslisting': False,
            'platforms_to_list': [],
            'created_listings': [],
            'errors': [],
        }

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM units WHERE id = %s", [str(unit_id)])
                unit = cur.fetchone()

            if not unit:
                results['errors'].append('Unit not found')
                return results

            if unit['status'] != 'listed':
                logger.debug(f"Unit {unit['unit_code']} not listed yet, skipping cross-listing")
                return results

            # Determine which platforms the unit is already listed on
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT LOWER(c.name) AS channel_name
                    FROM listing_units lu
                    JOIN listings l ON l.id = lu.listing_id
                    JOIN channels c ON c.id = l.channel_id
                    WHERE lu.unit_id = %s AND l.status = 'active'
                    """,
                    [str(unit_id)],
                )
                listed_platforms = [r[0] for r in cur.fetchall()]

            logger.debug(f"Unit {unit['unit_code']} currently listed on: {listed_platforms}")

            target_platforms = ['poshmark']
            platforms_to_list = [p for p in target_platforms if p not in listed_platforms]

            if not platforms_to_list:
                logger.debug(f"Unit {unit['unit_code']} already listed on all platforms")
                return results

            results['needs_crosslisting'] = True
            results['platforms_to_list'] = platforms_to_list

            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM listing_templates WHERE product_id = %s LIMIT 1",
                    [str(unit['product_id'])],
                )
                template = cur.fetchone()

            if not template:
                results['errors'].append('No listing template found')
                return results

            if not template['is_validated']:
                results['errors'].append('Template not validated')
                return results

            for platform in platforms_to_list:
                if platform == 'ebay':
                    continue
                try:
                    listing_result = self._create_listing_on_platform(unit=unit, template=template, platform=platform)
                    if listing_result['success']:
                        results['created_listings'].append({
                            'platform': platform,
                            'listing_id': listing_result['listing_id'],
                            'channel_listing_id': listing_result['channel_listing_id'],
                        })
                        logger.info(f"Successfully created {platform} listing for unit {unit['unit_code']}")
                    else:
                        results['errors'].append({'platform': platform, 'error': listing_result.get('error')})
                        logger.error(f"Failed to create {platform} listing: {listing_result.get('error')}")
                except Exception as e:
                    logger.error(f"Error creating {platform} listing: {e}")
                    results['errors'].append({'platform': platform, 'error': str(e)})

            self.conn.commit()
            logger.info(f"Cross-listing complete for unit {unit['unit_code']}: {len(results['created_listings'])} listings created")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error in cross-listing: {e}")
            results['errors'].append(str(e))

        return results

    def _create_listing_on_platform(self, unit: dict, template: dict, platform: str) -> Dict:
        from src.services.image_handler import ImageHandler

        platform_data = self._format_for_platform(template, platform)
        image_handler = ImageHandler()
        local_images = []

        try:
            local_images = image_handler.download_images(template['photos'] or [])
            if not local_images:
                return {'success': False, 'error': 'Failed to download images'}

            if platform == 'poshmark':
                from src.integrations.poshmark.poshmark_lister import PoshmarkLister
                result = PoshmarkLister().create_listing(platform_data, local_images)
            elif platform == 'mercari':
                from src.integrations.mercari.mercari_lister import MercariLister
                result = MercariLister().create_listing(platform_data, local_images)
            else:
                return {'success': False, 'error': f'Unknown platform: {platform}'}
        finally:
            if local_images:
                image_handler.cleanup(local_images)

        if not result['success']:
            return result

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM channels WHERE LOWER(name) = %s LIMIT 1", [platform])
            channel = cur.fetchone()

        if not channel:
            return {'success': False, 'error': f'Channel {platform} not found in database'}

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO listings
                    (id, channel_id, product_id, channel_listing_id, title, description,
                     current_price, status, created_at)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, 'active', now())
                RETURNING id
                """,
                [
                    channel['id'], str(unit['product_id']),
                    result['channel_listing_id'],
                    platform_data['title'], platform_data['description'],
                    platform_data['price'],
                ],
            )
            listing_id = cur.fetchone()['id']

            cur.execute(
                "INSERT INTO listing_units (id, listing_id, unit_id) VALUES (gen_random_uuid(), %s, %s)",
                [listing_id, str(unit['id'])],
            )

        logger.debug(f"Created {platform} listing in database: {listing_id}")
        return {'success': True, 'listing_id': str(listing_id), 'channel_listing_id': result['channel_listing_id']}

    def _format_for_platform(self, template: dict, platform: str) -> Dict:
        base_price = template.get('base_price') or 0
        title = (template.get('title') or '')
        description = (template.get('description') or '')

        if platform in ('poshmark', 'mercari'):
            price = base_price
            shipping = 'buyer_pays'
        else:
            price = base_price
            shipping = None

        if platform in ('poshmark', 'mercari'):
            title = title[:80]
        if platform == 'poshmark':
            description = description[:500]
        elif platform == 'mercari':
            description = description[:1000]

        category_mappings = template.get('category_mappings') or {}
        platform_specifics = category_mappings.get(f'{platform}_data', {})

        formatted_data = {
            'title': title,
            'description': description,
            'price': price,
            'shipping': shipping,
            'photos': template.get('photos') or [],
            'sku': None,
        }

        if platform_specifics:
            for key in ('category', 'condition', 'size', 'brand', 'color'):
                if key in platform_specifics:
                    formatted_data[key] = platform_specifics[key]
            logger.info(f"Using AI data for {platform}: category={formatted_data.get('category')}")
        else:
            logger.warning(f"No AI data for {platform}, using fallback")
            formatted_data['item_specifics'] = template.get('item_specifics') or {}
            formatted_data['category'] = category_mappings.get(platform, '')

        return formatted_data

    def _unit_needs_crosslist(self, unit_id) -> bool:
        target_platforms = ['poshmark']

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, unit_code, status, product_id FROM units WHERE id = %s", [str(unit_id)])
            unit = cur.fetchone()

        if not unit:
            logger.warning(f"Unit {unit_id} not found")
            return False
        if unit['status'] != 'listed':
            logger.debug(f"Unit {unit['unit_code']} status is {unit['status']}, skipping")
            return False

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT LOWER(c.name) FROM listing_units lu
                JOIN listings l ON l.id = lu.listing_id
                JOIN channels c ON c.id = l.channel_id
                WHERE lu.unit_id = %s AND l.status = 'active'
                """,
                [str(unit_id)],
            )
            listed_platforms = [r[0] for r in cur.fetchall()]

        platforms_to_list = [p for p in target_platforms if p not in listed_platforms]
        if not platforms_to_list:
            return False

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT is_validated FROM listing_templates WHERE product_id = %s LIMIT 1",
                [str(unit['product_id'])],
            )
            row = cur.fetchone()

        if not row or not row[0]:
            return False

        return True

    def bulk_crosslist(self, unit_ids: List) -> Dict:
        results = {'total': len(unit_ids), 'processed': 0, 'created': 0, 'errors': []}
        poshmark_created_today = 0

        for unit_id in unit_ids:
            try:
                if not self._unit_needs_crosslist(unit_id):
                    results['processed'] += 1
                    continue

                if poshmark_created_today >= POSHMARK_DAILY_CAP:
                    logger.info("Reached Poshmark daily cap of 300. Stopping run.")
                    break

                result = self.check_and_crosslist(unit_id)
                results['processed'] += 1
                results['created'] += len(result.get('created_listings', []))
                for listing in result.get('created_listings', []):
                    if listing.get('platform') == 'poshmark':
                        poshmark_created_today += 1

                if result.get('errors'):
                    results['errors'].extend(result['errors'])

                if result.get('created_listings'):
                    import time
                    time.sleep(60)

            except Exception as e:
                logger.error(f"Error cross-listing unit {unit_id}: {e}")
                results['errors'].append({'unit_id': str(unit_id), 'error': str(e)})

        logger.info(f"Bulk cross-listing complete: {results['created']} listings created")
        return results
