"""
Return Service
Manages return lifecycle and database operations.

This module implements the V1 returns workflow described in
"eBay Returns Tracking Workflow V1". Key behaviours:

  R1-01: matching to internal units uses SKU, then external listing id,
         then the marketplace order number (units.external_order_number).
  R1-03: return_events are deduplicated by (email_message_id, event_type).
  R1-04: return_event.event_timestamp is the event's real time
         (parsed body date, then email Date header, then utcnow as a
         last resort) - never the scheduler tick time.
  R1-05: returns are joined only via explicit identifiers (return_id,
         then order_number + buyer_username). A new record is created
         otherwise and flagged in `notes` (R4-24).
  R1-06: the service no longer commits internally. Callers are expected
         to commit (or rollback) once per email so the return row and
         the email_processing_log row land together.
  R1-07: status never regresses; out-of-order emails are still saved as
         events for the audit trail.
  R2-12: brand falls back to a vetted list; no "first word of title".
  R2-16: event_type=='unknown' is recorded but does not change status.
  R2-17: tracking_number is only set when not already known.
  R4-26: parameter names disambiguate Return.id (UUID) from the eBay
         return id.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from sqlalchemy.orm import Session
from database import Return, ReturnEvent, Unit
from returns.return_classifier import ReturnClassifier
from ebay_api import ebay_api

logger = logging.getLogger(__name__)


# R1-07: explicit ordering for monotonic status. Higher = later in the
# return lifecycle. Out-of-order events never demote status_current.
_STATUS_ORDER = {
    None: 0,
    'opened': 1,
    'awaiting_buyer_shipment': 1,
    'buyer_shipped': 2,
    'delivered_back': 3,
    'refunded': 4,
    'closed_no_buyer_shipment': 4,
    'closed_other': 4,
}


# R1-04: which parsed body field is the "real" timestamp for each event.
_EVENT_DATE_FIELDS = {
    'return_opened': 'opened_at',
    'buyer_shipped': 'buyer_shipped_at',
    'delivered_back': 'item_delivered_back_at',
    'refund_issued': 'refund_issued_at',
    'closed_no_ship': 'closed_at',
}


# R2-12: vetted brand list. Extend in one place rather than peppering
# parsing code. Order matters: multi-word brands first so "New Balance"
# beats "New" / "Balance".
_KNOWN_BRANDS = [
    'New Balance', 'Dr. Martens', 'Dr Martens', 'Under Armour',
    'On Running', 'Air Jordan', 'Doc Martens',
    'Nike', 'Adidas', 'Jordan', 'Puma', 'Reebok', 'Converse', 'Vans',
    'Asics', 'Saucony', 'Brooks', 'Fila', 'Skechers', 'Timberland',
    'Clarks', 'Salomon', 'Merrell', 'Hoka', 'Allbirds', 'Yeezy',
    'Birkenstock', 'UGG', 'Crocs',
]


class ReturnService:
    """Service for managing return lifecycle."""

    def __init__(self, db: Session):
        self.db = db
        self.classifier = ReturnClassifier()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def process_return_email(self, parsed_email: Dict) -> Dict:
        """Process a parsed return email.

        R1-06: this method NO LONGER commits. The caller is expected to
        commit/rollback so the return + event + email_processing_log
        write happens in a single transaction.
        """
        try:
            return_id = parsed_email.get('return_id')
            order_number = parsed_email.get('order_number')
            event_type = parsed_email.get('event_type')
            buyer_username = parsed_email.get('buyer_username')

            logger.info("[PROCESS] ===== processing return email =====")
            logger.info(f"[PROCESS] return_id={return_id or 'NONE'} "
                        f"order_number={order_number or 'NONE'} "
                        f"event_type={event_type}")
            # R3-23: buyer_username at DEBUG, not INFO.
            logger.debug(f"[PROCESS] buyer_username={buyer_username or 'NONE'}")

            # R1-05: only join via explicit identifiers. No fuzzy 30-day
            # buyer-only merge.
            existing_return = self._find_existing_return(
                return_id=return_id,
                order_number=order_number,
                buyer_username=buyer_username,
            )

            if existing_return:
                logger.info(f"[PROCESS] Found existing return {existing_return.id}")
                return_record = self._update_return(existing_return, parsed_email)
                action = 'updated'
            else:
                logger.info("[PROCESS] Creating new return record")
                return_record = self._create_return(parsed_email)
                action = 'created'

            # R1-03: dedup events by (message_id, event_type) before insert.
            event_row = self._create_return_event(return_record.id, parsed_email)

            # R1-06: flush only; the caller commits.
            self.db.flush()

            logger.info(f"[PROCESS] {action} return_id={return_record.return_id} "
                        f"matched={'YES' if return_record.internal_order_id else 'NO'}")

            return {
                'success': True,
                'action': action,
                'return_id': return_record.return_id,
                'internal_id': str(return_record.id),
                'event_type': event_type,
                'event_recorded': event_row is not None,
                'matched': return_record.internal_order_id is not None,
            }

        except Exception as e:
            # R1-06: do NOT rollback here. The caller owns the transaction
            # boundary so it can also roll back the email_processing_log row.
            logger.error(f"[PROCESS] Error processing return email: {e}",
                         exc_info=True)
            return {'success': False, 'error': str(e)}

    # ------------------------------------------------------------------
    # Matching helpers (R1-05)
    # ------------------------------------------------------------------
    def _find_existing_return(
        self,
        return_id: Optional[str],
        order_number: Optional[str],
        buyer_username: Optional[str],
    ) -> Optional[Return]:
        """Find an existing return using explicit identifiers only.

        Priority per spec section 4:
          1. return_id (the eBay-issued ID)
          2. order_number + buyer_username
        No fuzzy buyer-only fallback (R1-05).
        """
        if return_id:
            hit = self.db.query(Return).filter(Return.return_id == return_id).first()
            if hit:
                return hit

        if order_number and buyer_username:
            hit = self.db.query(Return).filter(
                Return.order_number == order_number,
                Return.buyer_username == buyer_username,
            ).first()
            if hit:
                return hit

        return None

    # ------------------------------------------------------------------
    # Create / update logic
    # ------------------------------------------------------------------
    def _create_return(self, parsed_email: Dict) -> Return:
        return_id = parsed_email.get('return_id')
        order_number = parsed_email.get('order_number')

        # eBay API enrichment for missing fields (unchanged behaviour).
        needs_enrichment = any(
            not parsed_email.get(field)
            for field in ['order_number', 'buyer_username', 'item_title',
                          'return_reason_ebay', 'external_listing_id']
        )

        if return_id and ebay_api.is_configured() and needs_enrichment:
            try:
                details = ebay_api.get_return_details(return_id)
                if details:
                    self._merge_ebay_enrichment(parsed_email, details)
            except Exception as e:
                logger.error(f"[ENRICH] eBay enrichment failed: {e}")
            order_number = parsed_email.get('order_number') or order_number

        # Match to internal unit (R1-01).
        sku = parsed_email.get('sku')
        external_listing_id = parsed_email.get('external_listing_id')
        match_note = None

        unit = self._find_matching_unit(
            sku=sku,
            external_listing_id=external_listing_id,
            marketplace=parsed_email.get('marketplace'),
            order_number=order_number,
        )

        internal_order_id = None
        brand = None
        if unit:
            internal_order_id = unit.id
            brand = unit.product.brand if unit.product else None
            sku = sku or unit.unit_code
        else:
            match_note = "Unmatched: no SKU/listing/order match"

        # R2-12: fall back to vetted brand list only (no first-word).
        if not brand:
            brand = _extract_known_brand(parsed_email.get('item_title'))

        # Classify (unchanged).
        classification = self.classifier.classify_and_recommend(
            parsed_email.get('return_reason_ebay'),
            parsed_email.get('buyer_comment'),
        )

        # Determine initial status (no prior to compare against).
        status_current, final_outcome = self._map_status_and_outcome(
            parsed_email.get('event_type'),
            parsed_email=parsed_email,
        )

        # R4-24: surface unmatched-by-id and unmatched-by-unit reasons
        # in the notes column.
        notes_lines = []
        if not return_id:
            notes_lines.append("No return_id in email - matched by other fields if any")
        if match_note:
            notes_lines.append(match_note)

        return_record = Return(
            marketplace='eBay',
            return_id=return_id,
            order_number=order_number,
            buyer_username=parsed_email.get('buyer_username'),
            item_title=parsed_email.get('item_title'),
            brand=brand,
            sku=sku,
            external_listing_id=external_listing_id,
            internal_order_id=internal_order_id,
            return_reason_ebay=parsed_email.get('return_reason_ebay'),
            buyer_comment=parsed_email.get('buyer_comment'),
            request_amount=parsed_email.get('request_amount'),
            opened_at=parsed_email.get('opened_at'),
            buyer_ship_by_date=parsed_email.get('buyer_ship_by_date'),
            buyer_shipped_at=parsed_email.get('buyer_shipped_at'),
            tracking_number=parsed_email.get('tracking_number'),
            item_delivered_back_at=parsed_email.get('item_delivered_back_at'),
            refund_issued_at=parsed_email.get('refund_issued_at'),
            status_current=status_current,
            final_outcome=final_outcome,
            internal_bucket=classification['internal_bucket'],
            recommended_fix=classification['recommended_fix'],
            classifier_source=classification['classifier_source'],
            classifier_confidence=classification['classifier_confidence'],
            notes="\n".join(notes_lines) if notes_lines else None,
        )

        self.db.add(return_record)
        self.db.flush()
        return return_record

    def _update_return(self, return_record: Return, parsed_email: Dict) -> Return:
        event_type = parsed_email.get('event_type')

        # Enrichment if we're still missing key fields.
        return_id = parsed_email.get('return_id') or return_record.return_id
        needs_enrichment = any([
            not return_record.order_number and not parsed_email.get('order_number'),
            not return_record.buyer_username and not parsed_email.get('buyer_username'),
            not return_record.item_title and not parsed_email.get('item_title'),
            not return_record.return_reason_ebay and not parsed_email.get('return_reason_ebay'),
            not return_record.external_listing_id and not parsed_email.get('external_listing_id'),
        ])
        if return_id and ebay_api.is_configured() and needs_enrichment:
            try:
                details = ebay_api.get_return_details(return_id)
                if details:
                    self._merge_ebay_enrichment(parsed_email, details)
            except Exception as e:
                logger.error(f"[ENRICH] eBay enrichment failed: {e}")

        # Section 11 of spec: only fill missing fields; don't overwrite.
        if parsed_email.get('return_id') and not return_record.return_id:
            return_record.return_id = parsed_email.get('return_id')
        if parsed_email.get('order_number') and not return_record.order_number:
            return_record.order_number = parsed_email.get('order_number')
        if parsed_email.get('buyer_username') and not return_record.buyer_username:
            return_record.buyer_username = parsed_email.get('buyer_username')
        if parsed_email.get('item_title') and not return_record.item_title:
            return_record.item_title = parsed_email.get('item_title')
        if parsed_email.get('external_listing_id') and not return_record.external_listing_id:
            return_record.external_listing_id = parsed_email.get('external_listing_id')

        # Date fields only get filled if missing, and only from real
        # parsed values (no fake utcnow).
        if event_type == 'buyer_shipped':
            if not return_record.buyer_shipped_at and parsed_email.get('buyer_shipped_at'):
                return_record.buyer_shipped_at = parsed_email.get('buyer_shipped_at')
            # R2-17: don't overwrite an existing tracking number.
            new_tracking = parsed_email.get('tracking_number')
            if new_tracking:
                if not return_record.tracking_number:
                    return_record.tracking_number = new_tracking
                elif new_tracking != return_record.tracking_number:
                    self._append_note(return_record,
                                      f"Tracking number from email differs from stored "
                                      f"({new_tracking}); kept existing.")
        elif event_type == 'delivered_back':
            if not return_record.item_delivered_back_at and parsed_email.get('item_delivered_back_at'):
                return_record.item_delivered_back_at = parsed_email.get('item_delivered_back_at')
        elif event_type == 'refund_issued':
            if not return_record.refund_issued_at and parsed_email.get('refund_issued_at'):
                return_record.refund_issued_at = parsed_email.get('refund_issued_at')
        elif event_type == 'closed_no_ship':
            if not return_record.closed_at:
                return_record.closed_at = (parsed_email.get('closed_at')
                                           or parsed_email.get('email_received_at')
                                           or datetime.utcnow())

        # R1-07: only advance status, never regress. R2-16: unknown events
        # never touch status.
        if event_type and event_type != 'unknown':
            new_status, new_outcome = self._map_status_and_outcome(
                event_type, return_record=return_record, parsed_email=parsed_email,
            )
            if _STATUS_ORDER.get(new_status, 0) >= _STATUS_ORDER.get(return_record.status_current, 0):
                return_record.status_current = new_status
                return_record.final_outcome = new_outcome
            else:
                logger.info(f"[STATUS] Ignoring out-of-order event {event_type} "
                            f"({new_status}); keeping {return_record.status_current}")
                self._append_note(return_record,
                                  f"Late {event_type} event ignored for status "
                                  f"(would demote {return_record.status_current}).")

        # Try matching if we're still unmatched and now have new identifiers.
        if not return_record.internal_order_id:
            unit = self._find_matching_unit(
                sku=parsed_email.get('sku') or return_record.sku,
                external_listing_id=(parsed_email.get('external_listing_id')
                                     or return_record.external_listing_id),
                marketplace=return_record.marketplace,
                order_number=(parsed_email.get('order_number') or return_record.order_number),
            )
            if unit:
                return_record.internal_order_id = unit.id
                return_record.brand = return_record.brand or (unit.product.brand if unit.product else None)
                return_record.sku = return_record.sku or unit.unit_code

        # Re-classify only if we got new reason or comment text.
        if parsed_email.get('return_reason_ebay') or parsed_email.get('buyer_comment'):
            classification = self.classifier.classify_and_recommend(
                parsed_email.get('return_reason_ebay') or return_record.return_reason_ebay,
                parsed_email.get('buyer_comment') or return_record.buyer_comment,
            )
            return_record.internal_bucket = classification['internal_bucket']
            return_record.recommended_fix = classification['recommended_fix']
            return_record.classifier_source = classification['classifier_source']
            return_record.classifier_confidence = classification['classifier_confidence']

        return_record.updated_at = datetime.utcnow()
        return return_record

    # ------------------------------------------------------------------
    # Events (R1-03, R1-04, R2-16)
    # ------------------------------------------------------------------
    def _create_return_event(
        self,
        return_uuid: str,           # R4-26: was misleadingly named return_id
        parsed_email: Dict,
    ) -> Optional[ReturnEvent]:
        """Insert an event row, deduplicated by (email_message_id, event_type)."""
        msg_id = parsed_email.get('email_message_id')
        event_type = parsed_email.get('event_type') or 'unknown'

        # R1-03: skip if we already have this exact event.
        if msg_id:
            existing = self.db.query(ReturnEvent).filter(
                ReturnEvent.email_message_id == msg_id,
                ReturnEvent.event_type == event_type,
            ).first()
            if existing:
                logger.info(f"[EVENT] Duplicate event skipped (msg_id={msg_id}, "
                            f"event_type={event_type})")
                return None

        # R1-04: pick a real timestamp.
        event_ts = self._pick_event_timestamp(parsed_email)

        # JSON-safe parsed copy.
        parsed_json = {
            k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in parsed_email.items()
        }

        event = ReturnEvent(
            return_id=return_uuid,
            event_type=event_type,
            event_timestamp=event_ts,
            source_type='email',
            email_message_id=msg_id,
            email_subject=parsed_email.get('email_subject'),
            raw_payload=parsed_email.get('raw_body'),
            parsed_data=parsed_json,
        )
        self.db.add(event)
        return event

    @staticmethod
    def _pick_event_timestamp(parsed_email: Dict) -> datetime:
        field = _EVENT_DATE_FIELDS.get(parsed_email.get('event_type'))
        if field and parsed_email.get(field):
            return parsed_email[field]
        if parsed_email.get('email_received_at'):
            return parsed_email['email_received_at']
        return datetime.utcnow()

    # ------------------------------------------------------------------
    # Matching helpers (R1-01)
    # ------------------------------------------------------------------
    def _find_matching_unit(
        self,
        sku: Optional[str],
        external_listing_id: Optional[str],
        marketplace: Optional[str],
        order_number: Optional[str],
    ) -> Optional[Unit]:
        """Match a return to an internal unit. Priority:
              1. SKU (most reliable, comes from internal data)
              2. external listing id (eBay item id, etc.)
              3. order number (R1-01)
        """
        if sku:
            unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
            if unit:
                logger.info(f"[MATCH] matched by sku={sku}")
                return unit

        if external_listing_id:
            unit = self._match_by_external_listing_id(external_listing_id, marketplace)
            if unit:
                logger.info(f"[MATCH] matched by listing_id={external_listing_id}")
                return unit

        if order_number:
            unit = self._match_by_order_number(order_number)
            if unit:
                logger.info(f"[MATCH] matched by order_number={order_number}")
                return unit

        logger.warning("[MATCH] no internal unit found for this return")
        return None

    def _match_by_order_number(self, order_number: str) -> Optional[Unit]:
        """R1-01: match by units.external_order_number."""
        if not order_number:
            return None
        try:
            return self.db.query(Unit).filter(
                Unit.external_order_number == order_number
            ).first()
        except Exception as e:
            logger.error(f"[MATCH] error matching by order number: {e}")
            return None

    def _match_by_external_listing_id(
        self, external_listing_id: str, marketplace: Optional[str],
    ) -> Optional[Unit]:
        try:
            from database import Listing, ListingUnit, Channel
            q = self.db.query(Unit).join(
                ListingUnit, ListingUnit.unit_id == Unit.id
            ).join(
                Listing, Listing.id == ListingUnit.listing_id
            ).join(
                Channel, Channel.id == Listing.channel_id
            ).filter(Listing.channel_listing_id == external_listing_id)
            if marketplace:
                q = q.filter(Channel.name == marketplace.lower())
            return q.first()
        except Exception as e:
            logger.error(f"[MATCH] error matching by listing id: {e}")
            return None

    # ------------------------------------------------------------------
    # Status mapping (unchanged from spec section 9, with monotonic gate)
    # ------------------------------------------------------------------
    def _map_status_and_outcome(
        self,
        event_type: str,
        return_record: Optional[Return] = None,
        parsed_email: Dict = None,
    ) -> tuple:
        parsed_email = parsed_email or {}
        if event_type == 'return_opened':
            return 'opened', 'still_open'
        if event_type == 'buyer_shipped':
            return 'buyer_shipped', 'still_open'
        if event_type == 'delivered_back':
            return 'delivered_back', 'still_open'
        if event_type == 'refund_issued':
            delivered = bool(
                (return_record and return_record.item_delivered_back_at)
                or parsed_email.get('item_delivered_back_at')
            )
            if delivered:
                return 'refunded', 'refunded_after_return_received'
            return 'refunded', 'refunded_without_return_received'
        if event_type == 'closed_no_ship':
            return 'closed_no_buyer_shipment', 'closed_buyer_never_shipped'
        if event_type == 'closed_other':
            return 'closed_other', 'closed_other'
        # Default kept for backwards compatibility, but the caller
        # filters out event_type=='unknown' first (R2-16).
        return 'opened', 'still_open'

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _merge_ebay_enrichment(parsed_email: Dict, details: Dict) -> None:
        """Copy values from eBay API into parsed_email when missing."""
        if not parsed_email.get('order_number') and details.get('order_id'):
            parsed_email['order_number'] = details.get('order_id')
        if not parsed_email.get('external_listing_id') and details.get('item_id'):
            parsed_email['external_listing_id'] = details.get('item_id')
        if not parsed_email.get('buyer_username') and details.get('buyer_username'):
            parsed_email['buyer_username'] = details.get('buyer_username')
        # Title from eBay is always preferred (email parsing unreliable).
        if details.get('title'):
            parsed_email['item_title'] = details.get('title')
        if not parsed_email.get('return_reason_ebay') and details.get('dispute_reason'):
            parsed_email['return_reason_ebay'] = details.get('dispute_reason')

    @staticmethod
    def _append_note(return_record: Return, line: str) -> None:
        """R4-24: append a dated line to returns.notes."""
        stamp = datetime.utcnow().strftime("%Y-%m-%d")
        prefix = f"[{stamp}] {line}"
        return_record.notes = f"{return_record.notes}\n{prefix}" if return_record.notes else prefix

    # ------------------------------------------------------------------
    # Read helpers used elsewhere
    # ------------------------------------------------------------------
    def get_return_by_id(self, return_id: str) -> Optional[Return]:
        return self.db.query(Return).filter(Return.return_id == return_id).first()

    def get_return_events(self, return_internal_id: str) -> list:
        return self.db.query(ReturnEvent).filter(
            ReturnEvent.return_id == return_internal_id
        ).order_by(ReturnEvent.created_at).all()


# R2-12: pure helper, exported for unit testing.
def _extract_known_brand(item_title: Optional[str]) -> Optional[str]:
    """Return a brand from the vetted list, or None. No first-word fallback."""
    if not item_title:
        return None
    title_lower = item_title.lower()
    for brand in _KNOWN_BRANDS:
        if brand.lower() in title_lower:
            return brand
    return None
