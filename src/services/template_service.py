"""
Template Service for Enhanced Listing Templates
Handles template creation, validation, and platform-specific formatting
"""
import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


class TemplateService:
    """Service for managing enhanced listing templates"""

    def __init__(self, conn):
        self.conn = conn

    def create_enhanced_template(self, product_id, listing_data, channel_id=None, ebay_category_data=None):
        """Create or update enhanced listing template. Returns template dict."""
        photos = listing_data.get('photos', [])
        item_specifics = listing_data.get('item_specifics', {})
        base_price = listing_data.get('current_price', 0)
        title = listing_data.get('title', '')
        description = listing_data.get('description', '')

        photo_metadata = self._create_photo_metadata(photos)
        pricing = self._calculate_platform_pricing(base_price)
        category_mappings = self._map_categories(
            item_specifics,
            ebay_category_data,
            listing_data.get('poshmark_data', {}),
            listing_data.get('mercari_data', {}),
        )
        seo_keywords = self._extract_keywords(title, description)

        # Build a temporary dict for validation
        tmp = {
            'title': title,
            'description': description,
            'photos': photos,
            'base_price': base_price,
            'item_specifics': item_specifics,
        }
        validation_result = self.validate_template(tmp)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM listing_templates WHERE product_id = %s LIMIT 1",
                [str(product_id)],
            )
            row = cur.fetchone()

            if row is None:
                cur.execute(
                    """
                    INSERT INTO listing_templates
                        (id, product_id, source_channel_id, title, description, photos,
                         item_specifics, base_price, photo_metadata, pricing,
                         category_mappings, seo_keywords, template_version,
                         last_synced_at, is_validated, validation_errors, created_at)
                    VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 2, %s, %s, %s, now())
                    RETURNING *
                    """,
                    [
                        str(product_id), str(channel_id) if channel_id else None,
                        title, description,
                        json.dumps(photos), json.dumps(item_specifics), base_price,
                        json.dumps(photo_metadata), json.dumps(pricing),
                        json.dumps(category_mappings), json.dumps(seo_keywords),
                        datetime.utcnow(),
                        validation_result['valid'],
                        json.dumps(validation_result.get('errors')) if validation_result.get('errors') else None,
                    ],
                )
            else:
                cur.execute(
                    """
                    UPDATE listing_templates
                    SET source_channel_id=%s, title=%s, description=%s, photos=%s,
                        item_specifics=%s, base_price=%s, photo_metadata=%s, pricing=%s,
                        category_mappings=%s, seo_keywords=%s, template_version=2,
                        last_synced_at=%s, is_validated=%s, validation_errors=%s
                    WHERE id=%s
                    RETURNING *
                    """,
                    [
                        str(channel_id) if channel_id else None,
                        title, description,
                        json.dumps(photos), json.dumps(item_specifics), base_price,
                        json.dumps(photo_metadata), json.dumps(pricing),
                        json.dumps(category_mappings), json.dumps(seo_keywords),
                        datetime.utcnow(),
                        validation_result['valid'],
                        json.dumps(validation_result.get('errors')) if validation_result.get('errors') else None,
                        row['id'],
                    ],
                )
            template = dict(cur.fetchone())

        self.conn.commit()
        logger.info(f"Enhanced template created for product {product_id}, validated: {template['is_validated']}")
        return template

    def _create_photo_metadata(self, photos: List[str]) -> Dict:
        if not photos:
            return {'count': 0, 'primary': 0, 'validated': False}
        return {
            'count': len(photos),
            'primary': 0,
            'validated': len(photos) >= 1,
            'urls': photos[:12],
        }

    def _calculate_platform_pricing(self, base_price: float) -> Dict:
        if not base_price or base_price <= 0:
            return {}
        return {
            'ebay': round(base_price, 2),
            'poshmark': round(base_price * 0.97, 2),
            'mercari': round(base_price * 0.93, 2),
            'shopify': round(base_price * 1.00, 2),
            'suggested_range': {
                'min': round(base_price * 0.85, 2),
                'max': round(base_price * 1.15, 2),
            },
        }

    def _map_categories(self, item_specifics: Dict, ebay_category_data: Dict = None,
                        poshmark_data: Dict = None, mercari_data: Dict = None) -> Dict:
        mappings = {}
        if ebay_category_data:
            mappings['ebay'] = ebay_category_data.get('category_id', '')
            mappings['ebay_path'] = ebay_category_data.get('category_name', '')
        if poshmark_data and 'category' in poshmark_data:
            posh_cat = poshmark_data['category']
            parts = [posh_cat.get('level_1', ''), posh_cat.get('level_2', ''), posh_cat.get('level_3', '')]
            mappings['poshmark'] = ' > '.join(p for p in parts if p and p != 'None')
            mappings['poshmark_data'] = poshmark_data
        if mercari_data and 'category' in mercari_data:
            merc_cat = mercari_data['category']
            parts = [merc_cat.get('level_1', ''), merc_cat.get('level_2', ''), merc_cat.get('level_3', '')]
            mappings['mercari'] = ' > '.join(p for p in parts if p and p != 'None')
            mappings['mercari_data'] = mercari_data
        return mappings

    def _extract_keywords(self, title: str, description: str) -> List[str]:
        text = re.sub(r'[^a-z0-9\s]', ' ', f"{title} {description}".lower())
        stop_words = {
            'the', 'and', 'for', 'with', 'this', 'that', 'from', 'have',
            'been', 'will', 'your', 'their', 'what', 'when', 'where',
            'which', 'there', 'these', 'those', 'about', 'into', 'through',
        }
        keywords, seen = [], set()
        for word in text.split():
            if len(word) > 3 and word not in stop_words and word not in seen:
                keywords.append(word)
                seen.add(word)
                if len(keywords) >= 20:
                    break
        return keywords

    def validate_template(self, template: dict) -> Dict:
        """Validate a template dict. Returns {'valid': bool, 'errors': list|None}."""
        errors = []
        title = template.get('title') or ''
        description = template.get('description') or ''
        photos = template.get('photos') or []
        base_price = template.get('base_price') or 0
        item_specifics = template.get('item_specifics') or {}

        if len(title) < 10:
            errors.append("Title too short or missing (minimum 10 characters)")
        if len(description) < 50:
            errors.append("Description too short or missing (minimum 50 characters)")
        if not photos:
            errors.append("At least 1 photo required")
        if not base_price or base_price <= 0:
            errors.append("Invalid price")
        if not item_specifics:
            errors.append("Item specifics missing")
        else:
            missing = [s for s in ['Brand', 'Size'] if s not in item_specifics]
            if missing:
                errors.append(f"Missing item specifics: {', '.join(missing)}")

        return {'valid': len(errors) == 0, 'errors': errors if errors else None}

    def get_template_for_platform(self, template_id, platform: str) -> Optional[Dict]:
        """Get template dict formatted for a specific platform."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM listing_templates WHERE id = %s", [str(template_id)])
            row = cur.fetchone()

        if not row:
            return None

        template = dict(row)
        pricing = template.get('pricing') or {}
        category_mappings = template.get('category_mappings') or {}
        photos = template.get('photos') or []
        base_price = template.get('base_price') or 0

        price = pricing.get(platform, base_price)
        category = category_mappings.get(platform, '')

        formatted = {
            'title': template.get('title', ''),
            'description': template.get('description', ''),
            'price': price,
            'photos': photos[:12],
            'category': category,
            'keywords': (template.get('seo_keywords') or [])[:10],
            'item_specifics': template.get('item_specifics') or {},
        }

        if platform == 'poshmark':
            formatted['title'] = formatted['title'][:80]
            formatted['description'] = formatted['description'][:500]
        elif platform == 'mercari':
            formatted['title'] = formatted['title'][:40]
            formatted['description'] = formatted['description'][:1000]

        return formatted

    def bulk_validate_templates(self) -> Dict:
        """Validate all templates and persist results. Returns summary dict."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM listing_templates")
            templates = [dict(r) for r in cur.fetchall()]

        results = {'total': len(templates), 'valid': 0, 'invalid': 0, 'updated': []}

        for template in templates:
            validation_result = self.validate_template(template)
            is_valid = validation_result['valid']
            errors = validation_result.get('errors')

            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE listing_templates SET is_validated=%s, validation_errors=%s WHERE id=%s",
                    [is_valid, json.dumps(errors) if errors else None, template['id']],
                )

            if is_valid:
                results['valid'] += 1
            else:
                results['invalid'] += 1
            results['updated'].append({
                'product_id': str(template['product_id']),
                'is_validated': is_valid,
                'errors': errors,
            })

        self.conn.commit()
        logger.info(f"Bulk validation complete: {results['valid']} valid, {results['invalid']} invalid")
        return results
