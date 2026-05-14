"""
Return Service
Manages return lifecycle and database operations
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import psycopg2.extras

from src.services.returns.return_classifier import ReturnClassifier
from src.integrations.ebay.ebay_api import ebay_api

logger = logging.getLogger(__name__)


class ReturnService:
    """Service for managing return lifecycle"""

    def __init__(self, conn):
        self.conn = conn
        self.classifier = ReturnClassifier()

    def process_return_email(self, parsed_email: Dict) -> Dict:
        try:
            return_id = parsed_email.get('return_id')
            order_number = parsed_email.get('order_number')
            event_type = parsed_email.get('event_type')
            buyer_username = parsed_email.get('buyer_username')

            logger.info(f"[PROCESS] Processing return email")
            logger.info(f"[PROCESS] Return ID: {return_id or 'NOT FOUND'}")
            logger.info(f"[PROCESS] Order Number: {order_number or 'NOT FOUND'}")
            logger.info(f"[PROCESS] Buyer: {buyer_username or 'NOT FOUND'}")
            logger.info(f"[PROCESS] Event Type: {event_type}")

            if not return_id:
                logger.warning(f"[PROCESS] No return_id found - will attempt matching by buyer/order")

            existing_return = None

            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if return_id:
                    cur.execute("SELECT * FROM returns WHERE return_id = %s LIMIT 1", [return_id])
                    existing_return = cur.fetchone()
                    if existing_return:
                        existing_return = dict(existing_return)
                        logger.info(f"[PROCESS] Found existing return by return_id: {return_id}")

                if not existing_return and order_number and buyer_username:
                    cur.execute(
                        "SELECT * FROM returns WHERE order_number = %s AND buyer_username = %s LIMIT 1",
                        [order_number, buyer_username],
                    )
                    existing_return = cur.fetchone()
                    if existing_return:
                        existing_return = dict(existing_return)
                        logger.info(f"[PROCESS] Found existing return by order_number + buyer: {order_number}")

                if not existing_return and buyer_username:
                    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
                    cur.execute(
                        """
                        SELECT * FROM returns
                        WHERE buyer_username = %s AND created_at >= %s AND final_outcome = 'still_open'
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        [buyer_username, thirty_days_ago],
                    )
                    existing_return = cur.fetchone()
                    if existing_return:
                        existing_return = dict(existing_return)
                        logger.info(f"[PROCESS] Found existing return by buyer_username: {buyer_username}")
                        if return_id and not existing_return.get('return_id'):
                            cur.execute(
                                "UPDATE returns SET return_id = %s WHERE id = %s",
                                [return_id, existing_return['id']],
                            )
                            existing_return['return_id'] = return_id

            if not existing_return:
                logger.info(f"[PROCESS] No existing return found - will create new return")

            if existing_return:
                logger.info(f"[PROCESS] Updating existing return (ID: {existing_return['id']})")
                return_record = self._update_return(existing_return, parsed_email)
                action = 'updated'
            else:
                logger.info(f"[PROCESS] Creating new return")
                return_record = self._create_return(parsed_email)
                action = 'created'

            logger.info(f"[PROCESS] Creating return event: {event_type}")
            self._create_return_event(return_record['id'], parsed_email)

            self.conn.commit()

            logger.info(f"[PROCESS] SUCCESS - Return {action}")
            logger.info(f"[PROCESS] Return ID: {return_record.get('return_id')}")
            logger.info(f"[PROCESS] Internal ID: {return_record['id']}")
            matched = return_record.get('internal_order_id') is not None
            logger.info(f"[PROCESS] Matched to unit: {'YES' if matched else 'NO'}")

            return {
                'success': True,
                'action': action,
                'return_id': return_record.get('return_id'),
                'internal_id': str(return_record['id']),
                'event_type': event_type,
                'matched': matched,
            }

        except Exception as e:
            self.conn.rollback()
            logger.error(f"[PROCESS] CRITICAL ERROR processing return email: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    # ----------------------------------------------------------------- create

    def _create_return(self, parsed_email: Dict) -> dict:
        return_id = parsed_email.get('return_id')
        order_number = parsed_email.get('order_number')

        needs_enrichment = any(
            not parsed_email.get(f)
            for f in ['order_number', 'buyer_username', 'item_title', 'return_reason_ebay', 'external_listing_id']
        )
        if return_id and ebay_api.is_configured() and needs_enrichment:
            logger.info(f"[ENRICH] Attempting eBay API enrichment for return {return_id}")
            try:
                ebay_details = ebay_api.get_return_details(return_id)
                if ebay_details:
                    logger.info(f"[ENRICH] eBay API enrichment successful")
                    if not order_number and ebay_details.get('order_id'):
                        order_number = ebay_details['order_id']
                        parsed_email['order_number'] = order_number
                    for em_key, api_key in [
                        ('external_listing_id', 'item_id'),
                        ('buyer_username', 'buyer_username'),
                        ('return_reason_ebay', 'dispute_reason'),
                    ]:
                        if not parsed_email.get(em_key) and ebay_details.get(api_key):
                            parsed_email[em_key] = ebay_details[api_key]
                    if ebay_details.get('title'):
                        parsed_email['item_title'] = ebay_details['title']
            except Exception as e:
                logger.error(f"[ENRICH] eBay API enrichment failed: {e}")

        internal_order_id = None
        brand = None
        sku = parsed_email.get('sku')
        external_listing_id = parsed_email.get('external_listing_id')

        logger.info(f"[MATCH] Attempting to match return to internal unit")

        if sku:
            unit = self._match_by_sku(sku)
            if unit:
                internal_order_id = unit['id']
                brand = unit.get('brand')
                logger.info(f"[MATCH] MATCHED by SKU: {sku}")
            else:
                logger.warning(f"[MATCH] No match found for SKU: {sku}")

        if not internal_order_id and external_listing_id:
            unit = self._match_by_external_listing_id(external_listing_id, parsed_email.get('marketplace'))
            if unit:
                internal_order_id = unit['id']
                brand = unit.get('brand')
                sku = unit['unit_code']
                logger.info(f"[MATCH] MATCHED by listing ID: {external_listing_id}")

        if not brand:
            brand = self._extract_brand_from_title(parsed_email.get('item_title', ''))

        logger.info(f"[CLASSIFY] Classifying return")
        classification = self.classifier.classify_and_recommend(
            parsed_email.get('return_reason_ebay'),
            parsed_email.get('buyer_comment'),
        )

        status_current, final_outcome = self._map_status_and_outcome(
            parsed_email.get('event_type'), parsed_email=parsed_email
        )

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO returns (
                    id, marketplace, return_id, order_number, buyer_username, item_title,
                    brand, sku, external_listing_id, internal_order_id, return_reason_ebay,
                    buyer_comment, request_amount, opened_at, buyer_ship_by_date,
                    buyer_shipped_at, tracking_number, item_delivered_back_at, refund_issued_at,
                    status_current, final_outcome, internal_bucket, recommended_fix,
                    classifier_source, classifier_confidence, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
                ) RETURNING *
                """,
                [
                    'eBay', parsed_email.get('return_id'), order_number,
                    parsed_email.get('buyer_username'), parsed_email.get('item_title'),
                    brand, sku, external_listing_id, internal_order_id,
                    parsed_email.get('return_reason_ebay'), parsed_email.get('buyer_comment'),
                    parsed_email.get('request_amount'), parsed_email.get('opened_at'),
                    parsed_email.get('buyer_ship_by_date'), parsed_email.get('buyer_shipped_at'),
                    parsed_email.get('tracking_number'), parsed_email.get('item_delivered_back_at'),
                    parsed_email.get('refund_issued_at'),
                    status_current, final_outcome,
                    classification['internal_bucket'], classification['recommended_fix'],
                    classification['classifier_source'], classification['classifier_confidence'],
                ],
            )
            return dict(cur.fetchone())

    # ----------------------------------------------------------------- update

    def _update_return(self, return_record: dict, parsed_email: Dict) -> dict:
        event_type = parsed_email.get('event_type')
        updates: dict = {}

        # API enrichment for missing fields
        return_id = parsed_email.get('return_id') or return_record.get('return_id')
        needs_enrichment = any([
            not return_record.get('order_number') and not parsed_email.get('order_number'),
            not return_record.get('buyer_username') and not parsed_email.get('buyer_username'),
            not return_record.get('item_title') and not parsed_email.get('item_title'),
            not return_record.get('return_reason_ebay') and not parsed_email.get('return_reason_ebay'),
            not return_record.get('external_listing_id') and not parsed_email.get('external_listing_id'),
        ])
        if return_id and ebay_api.is_configured() and needs_enrichment:
            try:
                ebay_details = ebay_api.get_return_details(return_id)
                if ebay_details:
                    for em_key, api_key in [
                        ('order_number', 'order_id'),
                        ('external_listing_id', 'item_id'),
                        ('buyer_username', 'buyer_username'),
                        ('return_reason_ebay', 'dispute_reason'),
                    ]:
                        if not return_record.get(em_key) and ebay_details.get(api_key):
                            parsed_email[em_key] = ebay_details[api_key]
                    if ebay_details.get('title'):
                        parsed_email['item_title'] = ebay_details['title']
            except Exception as e:
                logger.error(f"[ENRICH] eBay API enrichment failed: {e}")

        # Conditional field updates
        for field in ('return_id', 'order_number', 'buyer_username', 'item_title', 'external_listing_id'):
            if parsed_email.get(field) and not return_record.get(field):
                updates[field] = parsed_email[field]

        if event_type == 'buyer_shipped':
            if parsed_email.get('buyer_shipped_at'):
                updates['buyer_shipped_at'] = parsed_email['buyer_shipped_at']
            if parsed_email.get('tracking_number'):
                updates['tracking_number'] = parsed_email['tracking_number']
        elif event_type == 'delivered_back' and parsed_email.get('item_delivered_back_at'):
            updates['item_delivered_back_at'] = parsed_email['item_delivered_back_at']
        elif event_type == 'refund_issued' and parsed_email.get('refund_issued_at'):
            updates['refund_issued_at'] = parsed_email['refund_issued_at']
        elif event_type == 'closed_no_ship':
            updates['closed_at'] = datetime.utcnow()

        status_current, final_outcome = self._map_status_and_outcome(
            event_type, return_record=return_record, parsed_email=parsed_email
        )
        updates['status_current'] = status_current
        updates['final_outcome'] = final_outcome

        # Try to match to a unit if still unmatched
        if not return_record.get('internal_order_id'):
            ext_id = updates.get('external_listing_id') or return_record.get('external_listing_id')
            if ext_id:
                unit = self._match_by_external_listing_id(ext_id, return_record.get('marketplace'))
                if unit:
                    updates['internal_order_id'] = unit['id']
                    updates['brand'] = unit.get('brand')
                    updates['sku'] = unit['unit_code']
            if 'internal_order_id' not in updates:
                ord_num = updates.get('order_number') or return_record.get('order_number')
                if ord_num:
                    unit = self._match_to_internal_order(ord_num)
                    if unit:
                        updates['internal_order_id'] = unit['id']
                        updates['brand'] = unit.get('brand')
                        updates['sku'] = unit['unit_code']

        # Re-classify if new info
        if parsed_email.get('return_reason_ebay') or parsed_email.get('buyer_comment'):
            cls = self.classifier.classify_and_recommend(
                parsed_email.get('return_reason_ebay') or return_record.get('return_reason_ebay'),
                parsed_email.get('buyer_comment') or return_record.get('buyer_comment'),
            )
            updates.update({
                'internal_bucket': cls['internal_bucket'],
                'recommended_fix': cls['recommended_fix'],
                'classifier_source': cls['classifier_source'],
                'classifier_confidence': cls['classifier_confidence'],
            })

        updates['updated_at'] = datetime.utcnow()

        if updates:
            set_clause = ", ".join(f"{k} = %s" for k in updates)
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"UPDATE returns SET {set_clause} WHERE id = %s RETURNING *",
                    list(updates.values()) + [return_record['id']],
                )
                return dict(cur.fetchone())

        return return_record

    # ----------------------------------------------------------------- events

    def _create_return_event(self, return_id, parsed_email: Dict):
        parsed_data_json = {
            k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in parsed_email.items()
        }
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO return_events (
                    id, return_id, event_type, event_timestamp, source_type,
                    email_message_id, email_subject, raw_payload, parsed_data, created_at
                ) VALUES (gen_random_uuid(), %s, %s, %s, 'email', %s, %s, %s, %s, now())
                """,
                [
                    str(return_id),
                    parsed_email.get('event_type'),
                    datetime.utcnow(),
                    parsed_email.get('email_message_id'),
                    parsed_email.get('email_subject'),
                    parsed_email.get('raw_body'),
                    json.dumps(parsed_data_json),
                ],
            )

    # ----------------------------------------------------------------- matching

    def _match_by_sku(self, sku: str) -> Optional[dict]:
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT u.*, p.brand FROM units u
                    LEFT JOIN products p ON p.id = u.product_id
                    WHERE u.unit_code = %s LIMIT 1
                    """,
                    [sku],
                )
                row = cur.fetchone()
                if row:
                    logger.info(f"Found unit by SKU: {sku}")
                    return dict(row)
                logger.warning(f"No unit found for SKU: {sku}")
                return None
        except Exception as e:
            logger.error(f"Error matching by SKU: {e}")
            return None

    def _match_by_external_listing_id(self, external_listing_id: str, marketplace: Optional[str]) -> Optional[dict]:
        try:
            sql = """
                SELECT u.*, p.brand FROM units u
                JOIN listing_units lu ON lu.unit_id = u.id
                JOIN listings l ON l.id = lu.listing_id
                JOIN channels c ON c.id = l.channel_id
                LEFT JOIN products p ON p.id = u.product_id
                WHERE l.channel_listing_id = %s
            """
            params = [external_listing_id]
            if marketplace:
                sql += " AND LOWER(c.name) = %s"
                params.append(marketplace.lower())
            sql += " LIMIT 1"
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if row:
                    logger.info(f"Found unit by external listing ID: {external_listing_id}")
                    return dict(row)
                return None
        except Exception as e:
            logger.error(f"Error matching by external listing ID: {e}")
            return None

    def _match_to_internal_order(self, order_number: str) -> Optional[dict]:
        return None  # Placeholder — order number not stored in units

    def _extract_brand_from_title(self, item_title: str) -> Optional[str]:
        if not item_title:
            return None
        title_lower = item_title.lower()
        brands = [
            'Nike', 'Adidas', 'Jordan', 'Puma', 'Reebok', 'New Balance',
            'Converse', 'Vans', 'Asics', 'Saucony', 'Brooks', 'Under Armour',
            'Fila', 'Skechers', 'Timberland', 'Dr. Martens', 'Clarks',
            'Salomon', 'Merrell', 'Hoka', 'On Running', 'Allbirds',
        ]
        for brand in brands:
            if brand.lower() in title_lower:
                return brand
        words = item_title.split()
        return words[0] if words else None

    def _map_status_and_outcome(self, event_type: str, return_record: Optional[dict] = None,
                                parsed_email: Dict = None) -> tuple:
        if event_type == 'return_opened':
            return 'opened', 'still_open'
        elif event_type == 'buyer_shipped':
            return 'buyer_shipped', 'still_open'
        elif event_type == 'delivered_back':
            return 'delivered_back', 'still_open'
        elif event_type == 'refund_issued':
            delivered = (
                (return_record and return_record.get('item_delivered_back_at')) or
                (parsed_email and parsed_email.get('item_delivered_back_at'))
            )
            if delivered:
                return 'refunded', 'refunded_after_return_received'
            return 'refunded', 'refunded_without_return_received'
        elif event_type == 'closed_no_ship':
            return 'closed_no_buyer_shipment', 'closed_buyer_never_shipped'
        return 'opened', 'still_open'

    # ----------------------------------------------------------------- getters

    def get_return_by_id(self, return_id: str) -> Optional[dict]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM returns WHERE return_id = %s LIMIT 1", [return_id])
            row = cur.fetchone()
            return dict(row) if row else None

    def get_return_events(self, return_internal_id: str) -> list:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM return_events WHERE return_id = %s ORDER BY created_at",
                [str(return_internal_id)],
            )
            return [dict(r) for r in cur.fetchall()]
