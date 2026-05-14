"""
Audit Service for Inventory Management
Performs comprehensive audits and generates reports
"""
import csv
import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict

import psycopg2.extras

logger = logging.getLogger(__name__)


class AuditService:
    """Service for auditing inventory and identifying issues"""

    def __init__(self, conn):
        self.conn = conn

    def run_full_audit(self) -> Dict:
        logger.info("Starting full inventory audit...")
        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'sku_issues': self.audit_sku_issues(),
            'inventory_mismatches': self.audit_inventory_mismatches(),
            'template_issues': self.audit_template_issues(),
            'pricing_issues': self.audit_pricing_issues(),
            'photo_issues': self.audit_photo_issues(),
            'summary': {},
        }
        total_issues = sum([
            results['sku_issues']['total'],
            results['inventory_mismatches']['total'],
            results['template_issues']['total'],
            results['pricing_issues']['total'],
            results['photo_issues']['total'],
        ])
        results['summary'] = {
            'total_issues': total_issues,
            'critical': self._count_by_severity(results, 'critical'),
            'warning': self._count_by_severity(results, 'warning'),
            'info': self._count_by_severity(results, 'info'),
        }
        logger.info(f"Audit complete: {total_issues} total issues found")
        return results

    def audit_sku_issues(self) -> Dict:
        issues = {'missing_skus': [], 'unmatched_skus': [], 'duplicate_skus': [], 'total': 0}

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Listings with no channel_listing_id
            cur.execute(
                """
                SELECT id, title FROM listings
                WHERE status = 'active'
                  AND (channel_listing_id IS NULL OR channel_listing_id = '')
                """
            )
            for row in cur.fetchall():
                issues['missing_skus'].append({
                    'listing_id': str(row['id']),
                    'title': row['title'],
                    'severity': 'critical',
                    'message': 'Listing has no SKU/Channel Listing ID',
                })

            # Units marked listed but with no active listing
            cur.execute("SELECT id, unit_code FROM units WHERE status = 'listed'")
            listed_units = cur.fetchall()

        for unit in listed_units:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM listing_units lu
                    JOIN listings l ON l.id = lu.listing_id
                    WHERE lu.unit_id = %s AND l.status = 'active'
                    LIMIT 1
                    """,
                    [unit['id']],
                )
                if cur.fetchone() is None:
                    issues['unmatched_skus'].append({
                        'unit_code': unit['unit_code'],
                        'unit_id': str(unit['id']),
                        'severity': 'warning',
                        'message': 'Unit marked as "listed" but has no active listing',
                    })

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT unit_code, COUNT(*) AS cnt
                FROM units
                GROUP BY unit_code
                HAVING COUNT(*) > 1
                """
            )
            for row in cur.fetchall():
                issues['duplicate_skus'].append({
                    'sku': row['unit_code'],
                    'count': row['cnt'],
                    'severity': 'critical',
                    'message': f"SKU used {row['cnt']} times",
                })

        issues['total'] = (
            len(issues['missing_skus']) +
            len(issues['unmatched_skus']) +
            len(issues['duplicate_skus'])
        )
        return issues

    def audit_inventory_mismatches(self) -> Dict:
        issues = {
            'units_without_listings': [],
            'listings_without_units': [],
            'status_mismatches': [],
            'location_missing': [],
            'total': 0,
        }

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, unit_code, created_at FROM units WHERE status = 'ready_to_list'")
            for unit in cur.fetchall():
                if unit['created_at']:
                    days_waiting = (datetime.utcnow() - unit['created_at']).days
                    if days_waiting > 7:
                        issues['units_without_listings'].append({
                            'unit_code': unit['unit_code'],
                            'unit_id': str(unit['id']),
                            'days_waiting': days_waiting,
                            'severity': 'warning',
                            'message': f'Ready to list for {days_waiting} days',
                        })

            cur.execute("SELECT id, title, channel_listing_id FROM listings WHERE status = 'active'")
            active_listings = cur.fetchall()

        for listing in active_listings:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM listing_units WHERE listing_id = %s",
                    [listing['id']],
                )
                count = cur.fetchone()[0]
            if count == 0:
                issues['listings_without_units'].append({
                    'listing_id': str(listing['id']),
                    'title': listing['title'],
                    'channel_listing_id': listing['channel_listing_id'],
                    'severity': 'critical',
                    'message': 'Active listing has no linked units',
                })

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, unit_code FROM units
                WHERE location_id IS NULL
                  AND status IN ('ready_to_list', 'listed', 'reserved')
                """
            )
            for unit in cur.fetchall():
                issues['location_missing'].append({
                    'unit_code': unit['unit_code'],
                    'unit_id': str(unit['id']),
                    'severity': 'info',
                    'message': 'Unit has no warehouse location assigned',
                })

        issues['total'] = (
            len(issues['units_without_listings']) +
            len(issues['listings_without_units']) +
            len(issues['location_missing'])
        )
        return issues

    def audit_template_issues(self) -> Dict:
        issues = {'invalid_templates': [], 'missing_photos': [], 'total': 0}

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, product_id, title, validation_errors FROM listing_templates WHERE is_validated = false"
            )
            for row in cur.fetchall():
                issues['invalid_templates'].append({
                    'template_id': str(row['id']),
                    'product_id': str(row['product_id']),
                    'title': row['title'],
                    'errors': row['validation_errors'],
                    'severity': 'warning',
                    'message': 'Template failed validation',
                })

            cur.execute(
                "SELECT id, product_id, title FROM listing_templates WHERE photos IS NULL OR photos = '[]'::jsonb"
            )
            for row in cur.fetchall():
                issues['missing_photos'].append({
                    'template_id': str(row['id']),
                    'product_id': str(row['product_id']),
                    'title': row['title'],
                    'severity': 'warning',
                    'message': 'Template has no photos',
                })

        issues['total'] = len(issues['invalid_templates']) + len(issues['missing_photos'])
        return issues

    def audit_pricing_issues(self) -> Dict:
        issues = {'zero_prices': [], 'cost_exceeds_price': [], 'total': 0}

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, title, current_price FROM listings
                WHERE status = 'active'
                  AND (current_price IS NULL OR current_price <= 0)
                """
            )
            for row in cur.fetchall():
                issues['zero_prices'].append({
                    'listing_id': str(row['id']),
                    'title': row['title'],
                    'price': float(row['current_price']) if row['current_price'] else 0,
                    'severity': 'critical',
                    'message': 'Listing has invalid price',
                })

            cur.execute(
                """
                SELECT id, unit_code, cost_basis, sold_price FROM units
                WHERE status = 'sold'
                  AND sold_price IS NOT NULL
                  AND cost_basis IS NOT NULL
                  AND sold_price < cost_basis
                """
            )
            for row in cur.fetchall():
                loss = float(row['cost_basis']) - float(row['sold_price'])
                issues['cost_exceeds_price'].append({
                    'unit_code': row['unit_code'],
                    'unit_id': str(row['id']),
                    'cost_basis': float(row['cost_basis']),
                    'sold_price': float(row['sold_price']),
                    'loss': loss,
                    'severity': 'info',
                    'message': f'Sold at loss: ${loss:.2f}',
                })

        issues['total'] = len(issues['zero_prices']) + len(issues['cost_exceeds_price'])
        return issues

    def audit_photo_issues(self) -> Dict:
        issues = {'insufficient_photos': [], 'total': 0}

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, product_id, title, photos FROM listing_templates")
            for row in cur.fetchall():
                photos = row['photos'] or []
                count = len(photos)
                if 0 < count < 3:
                    issues['insufficient_photos'].append({
                        'template_id': str(row['id']),
                        'product_id': str(row['product_id']),
                        'title': row['title'],
                        'photo_count': count,
                        'severity': 'info',
                        'message': f'Only {count} photo(s), recommended 3+',
                    })

        issues['total'] = len(issues['insufficient_photos'])
        return issues

    def get_audit_summary(self) -> Dict:
        with self.conn.cursor() as cur:
            def count(sql, *args):
                cur.execute(sql, list(args))
                return cur.fetchone()[0]

            return {
                'units': {
                    'total': count("SELECT COUNT(*) FROM units"),
                    'ready_to_list': count("SELECT COUNT(*) FROM units WHERE status = 'ready_to_list'"),
                    'listed': count("SELECT COUNT(*) FROM units WHERE status = 'listed'"),
                    'sold': count("SELECT COUNT(*) FROM units WHERE status = 'sold'"),
                    'no_location': count("SELECT COUNT(*) FROM units WHERE location_id IS NULL"),
                },
                'listings': {
                    'total': count("SELECT COUNT(*) FROM listings"),
                    'active': count("SELECT COUNT(*) FROM listings WHERE status = 'active'"),
                    'sold': count("SELECT COUNT(*) FROM listings WHERE status = 'sold'"),
                },
                'templates': {
                    'total': count("SELECT COUNT(*) FROM listing_templates"),
                    'validated': count("SELECT COUNT(*) FROM listing_templates WHERE is_validated = true"),
                    'invalid': count("SELECT COUNT(*) FROM listing_templates WHERE is_validated = false"),
                },
                'alerts': {
                    'total': count("SELECT COUNT(*) FROM alerts"),
                    'unresolved': count("SELECT COUNT(*) FROM alerts WHERE is_resolved = false"),
                },
            }

    def _count_by_severity(self, results: Dict, severity: str) -> int:
        count = 0
        for category in results.values():
            if isinstance(category, dict):
                for issue_list in category.values():
                    if isinstance(issue_list, list):
                        count += sum(1 for issue in issue_list if issue.get('severity') == severity)
        return count

    def export_audit_report(self, audit_results: Dict) -> str:
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Category', 'Issue Type', 'Severity', 'Details', 'Message'])
        for category, data in audit_results.items():
            if category in ['timestamp', 'summary']:
                continue
            for issue_type, issues in data.items():
                if issue_type == 'total':
                    continue
                if isinstance(issues, list):
                    for issue in issues:
                        writer.writerow([
                            category,
                            issue_type,
                            issue.get('severity', 'info'),
                            str(issue.get('unit_code') or issue.get('listing_id') or issue.get('sku', '')),
                            issue.get('message', ''),
                        ])
        return output.getvalue()
