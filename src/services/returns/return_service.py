"""
Return Service
Manages return lifecycle and database operations
"""
import logging
from datetime import datetime
from typing import Dict, Optional
from sqlalchemy.orm import Session
from src.backend.db.database import Return, ReturnEvent, Unit
from src.services.returns.return_classifier import ReturnClassifier
from src.integrations.ebay.ebay_api import ebay_api

logger = logging.getLogger(__name__)


class ReturnService:
    """Service for managing return lifecycle"""
    
    def __init__(self, db: Session):
        self.db = db
        self.classifier = ReturnClassifier()
    
    def process_return_email(self, parsed_email: Dict) -> Dict:
        """
        Process a parsed return email
        
        Args:
            parsed_email: Parsed email data from EbayReturnParser
        
        Returns:
            dict: Processing result
        """
        try:
            # Extract key fields
            return_id = parsed_email.get('return_id')
            order_number = parsed_email.get('order_number')
            event_type = parsed_email.get('event_type')
            buyer_username = parsed_email.get('buyer_username')
            
            logger.info(f"[PROCESS] ═══════════════════════════════════════")
            logger.info(f"[PROCESS] Processing return email")
            logger.info(f"[PROCESS] Return ID: {return_id or 'NOT FOUND'}")
            logger.info(f"[PROCESS] Order Number: {order_number or 'NOT FOUND'}")
            logger.info(f"[PROCESS] Buyer: {buyer_username or 'NOT FOUND'}")
            logger.info(f"[PROCESS] Event Type: {event_type}")
            
            # Return ID is optional - leave as None if not found
            if not return_id:
                logger.warning(f"[PROCESS]  No return_id found - will attempt matching by buyer/order")
            
            # Check if return already exists (by return_id if available)
            existing_return = None
            
            if return_id:
                existing_return = self.db.query(Return).filter(
                    Return.return_id == return_id
                ).first()
                
                if existing_return:
                    logger.info(f"[PROCESS]  Found existing return by return_id: {return_id}")
            
            # If no return found by return_id, try to find by order_number + buyer
            if not existing_return and order_number and buyer_username:
                existing_return = self.db.query(Return).filter(
                    Return.order_number == order_number,
                    Return.buyer_username == buyer_username
                ).first()
                
                if existing_return:
                    logger.info(f"[PROCESS]  Found existing return by order_number + buyer: {order_number}")
            
            # If still no match, try to find by buyer_username (recent returns only)
            if not existing_return and buyer_username:
                # Look for recent returns from same buyer (within last 30 days)
                from datetime import datetime, timedelta
                thirty_days_ago = datetime.utcnow() - timedelta(days=30)
                
                existing_return = self.db.query(Return).filter(
                    Return.buyer_username == buyer_username,
                    Return.created_at >= thirty_days_ago,
                    Return.final_outcome == 'still_open'
                ).order_by(Return.created_at.desc()).first()
                
                if existing_return:
                    logger.info(f"[PROCESS]  Found existing return by buyer_username: {buyer_username}")
                    # Update the return_id if we now have it
                    if return_id and not existing_return.return_id:
                        existing_return.return_id = return_id
                        logger.info(f"[PROCESS]  Updated return_id on existing return: {return_id}")
            
            if not existing_return:
                logger.info(f"[PROCESS]  No existing return found - will create new return")
            
            # Create or update return
            if existing_return:
                logger.info(f"[PROCESS]  Updating existing return (ID: {existing_return.id})")
                return_record = self._update_return(existing_return, parsed_email)
                action = 'updated'
            else:
                logger.info(f"[PROCESS]  Creating new return")
                return_record = self._create_return(parsed_email)
                action = 'created'
            
            # Create return event
            logger.info(f"[PROCESS]  Creating return event: {event_type}")
            self._create_return_event(return_record.id, parsed_email)
            
            self.db.commit()
            
            logger.info(f"[PROCESS]  SUCCESS - Return {action}")
            logger.info(f"[PROCESS] Return ID: {return_record.return_id}")
            logger.info(f"[PROCESS] Internal ID: {return_record.id}")
            logger.info(f"[PROCESS] Matched to unit: {'YES' if return_record.internal_order_id else 'NO'}")
            if return_record.internal_order_id:
                logger.info(f"[PROCESS] Matched Unit ID: {return_record.internal_order_id}")
                logger.info(f"[PROCESS] SKU: {return_record.sku}")
            logger.info(f"[PROCESS] ═══════════════════════════════════════")
            
            return {
                'success': True,
                'action': action,
                'return_id': return_record.return_id,
                'internal_id': str(return_record.id),
                'event_type': event_type,
                'matched': return_record.internal_order_id is not None
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"[PROCESS]  CRITICAL ERROR processing return email: {e}", exc_info=True)
            logger.error(f"[PROCESS] Return ID: {parsed_email.get('return_id', 'N/A')}")
            logger.error(f"[PROCESS] Event Type: {parsed_email.get('event_type', 'N/A')}")
            logger.error(f"[PROCESS] Buyer: {parsed_email.get('buyer_username', 'N/A')}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _create_return(self, parsed_email: Dict) -> Return:
        """Create new return record"""
        # Try to enrich from eBay API first
        return_id = parsed_email.get('return_id')
        order_number = parsed_email.get('order_number')
        
        # Try eBay API enrichment only if key fields are missing
        needs_enrichment = any(
            not parsed_email.get(field)
            for field in ['order_number', 'buyer_username', 'item_title', 'return_reason_ebay', 'external_listing_id']
        )

        if return_id and ebay_api.is_configured() and needs_enrichment:
            logger.info(f"[ENRICH] Attempting eBay API enrichment for return {return_id}")
            try:
                ebay_details = ebay_api.get_return_details(return_id)
                
                if ebay_details:
                    logger.info(f"[ENRICH]  eBay API enrichment successful")
                    # Use eBay API data to enrich parsed_email
                    if not order_number and ebay_details.get('order_id'):
                        order_number = ebay_details.get('order_id')
                        parsed_email['order_number'] = order_number
                        logger.info(f"[ENRICH]  Added order_number: {order_number}")

                    if not parsed_email.get('external_listing_id') and ebay_details.get('item_id'):
                        parsed_email['external_listing_id'] = ebay_details.get('item_id')
                        logger.info(f"[ENRICH]  Added external_listing_id: {ebay_details.get('item_id')}")
                    
                    # Enrich other missing fields from API
                    if not parsed_email.get('buyer_username') and ebay_details.get('buyer_username'):
                        parsed_email['buyer_username'] = ebay_details.get('buyer_username')
                        logger.info(f"[ENRICH]  Added buyer_username: {ebay_details.get('buyer_username')}")
                    
                    # ALWAYS use eBay API title if available (email parsing is unreliable)
                    if ebay_details.get('title'):
                        parsed_email['item_title'] = ebay_details.get('title')
                        logger.info(f"[ENRICH]  Set item_title from eBay API: {ebay_details.get('title')[:50]}...")
                    
                    if not parsed_email.get('return_reason_ebay') and ebay_details.get('dispute_reason'):
                        parsed_email['return_reason_ebay'] = ebay_details.get('dispute_reason')
                        logger.info(f"[ENRICH]  Added return_reason_ebay")
                else:
                    logger.warning(f"[ENRICH]  eBay API returned no data for return {return_id}")
            except Exception as e:
                logger.error(f"[ENRICH]  eBay API enrichment failed: {e}")
        elif not ebay_api.is_configured():
            logger.debug(f"[ENRICH] eBay API not configured - skipping enrichment")
        elif not needs_enrichment:
            logger.debug(f"[ENRICH] All key fields present - skipping enrichment")
        
        # Try to match to internal order
        internal_order_id = None
        brand = None
        sku = parsed_email.get('sku')
        external_listing_id = parsed_email.get('external_listing_id')
        
        logger.info(f"[MATCH] Attempting to match return to internal unit")
        logger.info(f"[MATCH] Available identifiers - SKU: {sku or 'N/A'}, Listing ID: {external_listing_id or 'N/A'}, Order: {order_number or 'N/A'}")
        
        # Method 1: Match by SKU (most reliable)
        if sku:
            logger.info(f"[MATCH]  Trying match by SKU: {sku}")
            unit = self._match_by_sku(sku)
            if unit:
                internal_order_id = unit.id
                brand = unit.product.brand if unit.product else None
                logger.info(f"[MATCH]  MATCHED by SKU: {sku}  Unit ID: {unit.id}, Brand: {brand}")
            else:
                logger.warning(f"[MATCH]  No match found for SKU: {sku}")
        
        # Method 2: Match by external listing ID
        if not internal_order_id and external_listing_id:
            logger.info(f"[MATCH]  Trying match by external listing ID: {external_listing_id}")
            unit = self._match_by_external_listing_id(external_listing_id, parsed_email.get('marketplace'))
            if unit:
                internal_order_id = unit.id
                brand = unit.product.brand if unit.product else None
                sku = unit.unit_code
                logger.info(f"[MATCH]  MATCHED by listing ID: {external_listing_id}  Unit ID: {unit.id}, SKU: {sku}, Brand: {brand}")
            else:
                logger.warning(f"[MATCH]  No match found for listing ID: {external_listing_id}")

        # Method 3: Match by order number (if available)
        if not internal_order_id and order_number:
            logger.info(f"[MATCH]  Trying match by order number: {order_number}")
            unit = self._match_to_internal_order(order_number)
            if unit:
                internal_order_id = unit.id
                brand = unit.product.brand if unit.product else None
                sku = unit.unit_code
                logger.info(f"[MATCH]  MATCHED by order number: {order_number}  Unit ID: {unit.id}, SKU: {sku}, Brand: {brand}")
            else:
                logger.warning(f"[MATCH]  No match found for order number: {order_number}")
        
        if not internal_order_id:
            logger.warning(f"[MATCH]  UNMATCHED - Could not match return to any internal unit")
        
        # If no match, try to extract brand from item title
        if not brand:
            item_title = parsed_email.get('item_title', '')
            brand = self._extract_brand_from_title(item_title)
            if brand:
                logger.info(f"[MATCH] Extracted brand from title: {brand}")
        
        # Classify return
        logger.info(f"[CLASSIFY] Classifying return")
        classification = self.classifier.classify_and_recommend(
            parsed_email.get('return_reason_ebay'),
            parsed_email.get('buyer_comment')
        )
        logger.info(f"[CLASSIFY] Bucket: {classification['internal_bucket']}, Confidence: {classification['classifier_confidence']}")
        logger.info(f"[CLASSIFY] Recommended fix: {classification['recommended_fix']}")
        
        # Determine status and outcome
        status_current, final_outcome = self._map_status_and_outcome(
            parsed_email.get('event_type'),
            parsed_email=parsed_email
        )
        
        # Create return
        return_record = Return(
            marketplace='eBay',
            return_id=parsed_email.get('return_id'),
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
            classifier_confidence=classification['classifier_confidence']
        )
        
        self.db.add(return_record)
        self.db.flush()  # Get ID without committing
        
        return return_record
    
    def _match_by_sku(self, sku: str) -> Optional[Unit]:
        """
        Match return to internal unit by SKU
        
        Args:
            sku: Unit code / SKU
        
        Returns:
            Unit: Matched unit or None
        """
        try:
            unit = self.db.query(Unit).filter(
                Unit.unit_code == sku
            ).first()
            
            if unit:
                logger.info(f"Found unit by SKU: {sku}")
                return unit
            else:
                logger.warning(f"No unit found for SKU: {sku}")
                return None
                
        except Exception as e:
            logger.error(f"Error matching by SKU: {e}")
            return None
    
    def _extract_brand_from_title(self, item_title: str) -> Optional[str]:
        """
        Extract brand from item title
        Common shoe brands to look for
        """
        if not item_title:
            return None
        
        title_lower = item_title.lower()
        
        # Common shoe brands
        brands = [
            'Nike', 'Adidas', 'Jordan', 'Puma', 'Reebok', 'New Balance',
            'Converse', 'Vans', 'Asics', 'Saucony', 'Brooks', 'Under Armour',
            'Fila', 'Skechers', 'Timberland', 'Dr. Martens', 'Clarks',
            'Salomon', 'Merrell', 'Hoka', 'On Running', 'Allbirds'
        ]
        
        for brand in brands:
            if brand.lower() in title_lower:
                return brand
        
        # Try to get first word as brand (common pattern)
        words = item_title.split()
        if words:
            return words[0]
        
        return None
    
    def _update_return(self, return_record: Return, parsed_email: Dict) -> Return:
        """Update existing return record"""
        event_type = parsed_email.get('event_type')
        
        # Try API enrichment if missing key fields
        return_id = parsed_email.get('return_id') or return_record.return_id
        needs_enrichment = any([
            not return_record.order_number and not parsed_email.get('order_number'),
            not return_record.buyer_username and not parsed_email.get('buyer_username'),
            not return_record.item_title and not parsed_email.get('item_title'),
            not return_record.return_reason_ebay and not parsed_email.get('return_reason_ebay'),
            not return_record.external_listing_id and not parsed_email.get('external_listing_id')
        ])

        if return_id and ebay_api.is_configured() and needs_enrichment:
            logger.info(f"Attempting eBay API enrichment for existing return {return_id}")
            ebay_details = ebay_api.get_return_details(return_id)
            
            if ebay_details:
                logger.info(f"[ENRICH]  eBay API enrichment successful for return {return_id}")
                if not return_record.order_number and ebay_details.get('order_id'):
                    parsed_email['order_number'] = ebay_details.get('order_id')
                if not return_record.external_listing_id and ebay_details.get('item_id'):
                    parsed_email['external_listing_id'] = ebay_details.get('item_id')
                if ebay_details.get('buyer_username') and not return_record.buyer_username:
                    parsed_email['buyer_username'] = ebay_details.get('buyer_username')
                
                # ALWAYS use eBay API title if available (email parsing is unreliable)
                if ebay_details.get('title'):
                    parsed_email['item_title'] = ebay_details.get('title')
                    logger.info(f"[ENRICH]  Set item_title from eBay API: {ebay_details.get('title')[:50]}...")
                
                if ebay_details.get('dispute_reason') and not return_record.return_reason_ebay:
                    parsed_email['return_reason_ebay'] = ebay_details.get('dispute_reason')
                    
        
        # Update fields that may have changed
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
        
        # Update dates based on event type
        if event_type == 'buyer_shipped':
            # Only set if we have actual data from email
            if parsed_email.get('buyer_shipped_at'):
                return_record.buyer_shipped_at = parsed_email.get('buyer_shipped_at')
            # Otherwise leave as NULL
            
            if parsed_email.get('tracking_number'):
                return_record.tracking_number = parsed_email.get('tracking_number')
        
        elif event_type == 'delivered_back':
            # Only set if we have actual data from email
            if parsed_email.get('item_delivered_back_at'):
                return_record.item_delivered_back_at = parsed_email.get('item_delivered_back_at')
        
        elif event_type == 'refund_issued':
            # Only set if we have actual data from email
            if parsed_email.get('refund_issued_at'):
                return_record.refund_issued_at = parsed_email.get('refund_issued_at')
        
        elif event_type == 'closed_no_ship':
            # Closure time is NOW - this is the only case where we set current time
            return_record.closed_at = datetime.utcnow()
        
        # Update status and outcome
        status_current, final_outcome = self._map_status_and_outcome(
            event_type, return_record=return_record, parsed_email=parsed_email
        )
        return_record.status_current = status_current
        return_record.final_outcome = final_outcome
        
        # Try to match to internal order if not already matched
        if not return_record.internal_order_id and return_record.external_listing_id:
            unit = self._match_by_external_listing_id(
                return_record.external_listing_id,
                return_record.marketplace
            )
            if unit:
                return_record.internal_order_id = unit.id
                return_record.brand = unit.product.brand if unit.product else None
                return_record.sku = unit.unit_code

        if not return_record.internal_order_id and return_record.order_number:
            unit = self._match_to_internal_order(return_record.order_number)
            if unit:
                return_record.internal_order_id = unit.id
                return_record.brand = unit.product.brand if unit.product else None
                return_record.sku = unit.unit_code
        
        # Re-classify if we have new information
        if parsed_email.get('return_reason_ebay') or parsed_email.get('buyer_comment'):
            classification = self.classifier.classify_and_recommend(
                parsed_email.get('return_reason_ebay') or return_record.return_reason_ebay,
                parsed_email.get('buyer_comment') or return_record.buyer_comment
            )
            
            return_record.internal_bucket = classification['internal_bucket']
            return_record.recommended_fix = classification['recommended_fix']
            return_record.classifier_source = classification['classifier_source']
            return_record.classifier_confidence = classification['classifier_confidence']
        
        return_record.updated_at = datetime.utcnow()
        
        return return_record
    
    def _create_return_event(self, return_id: str, parsed_email: Dict) -> ReturnEvent:
        """Create return event record"""
        
        # Convert datetime objects to ISO format strings for JSON storage
        parsed_data_json = {}
        for key, value in parsed_email.items():
            if isinstance(value, datetime):
                parsed_data_json[key] = value.isoformat()
            else:
                parsed_data_json[key] = value
        
        event = ReturnEvent(
            return_id=return_id,
            event_type=parsed_email.get('event_type'),
            event_timestamp=datetime.utcnow(),
            source_type='email',
            email_message_id=parsed_email.get('email_message_id'),
            email_subject=parsed_email.get('email_subject'),
            raw_payload=parsed_email.get('raw_body'),
            parsed_data=parsed_data_json  # Use JSON-serializable version
        )
        
        self.db.add(event)
        
        return event
    
    def _match_to_internal_order(self, order_number: str) -> Optional[Unit]:
        """
        Match return to internal order by order number
        
        Args:
            order_number: eBay order number
        
        Returns:
            Unit: Matched unit or None
        """
        try:
            # Try to find unit by order number
            # Note: This assumes order_number is stored somewhere in the unit
            # You may need to adjust this based on your actual data model
            
            # For now, we'll try to match by SKU if it's in the order number
            # This is a placeholder - adjust based on your actual order tracking
            
            # Option 1: If you store order numbers in units table
            # unit = self.db.query(Unit).filter(
            #     Unit.order_number == order_number
            # ).first()
            
            # Option 2: If order number contains SKU
            # Extract potential SKU from order number and search
            # This is a simplified approach
            
            # For now, return None - implement based on your data model
            return None
            
        except Exception as e:
            logger.error(f"Error matching order: {e}")
            return None

    def _match_by_external_listing_id(self, external_listing_id: str, marketplace: Optional[str]) -> Optional[Unit]:
        """Match return to internal unit by marketplace listing ID"""
        try:
            from src.backend.db.database import Listing, ListingUnit, Channel

            query = self.db.query(Unit).join(
                ListingUnit, ListingUnit.unit_id == Unit.id
            ).join(
                Listing, Listing.id == ListingUnit.listing_id
            ).join(
                Channel, Channel.id == Listing.channel_id
            ).filter(
                Listing.channel_listing_id == external_listing_id
            )

            if marketplace:
                query = query.filter(Channel.name == marketplace.lower())

            unit = query.first()

            if unit:
                logger.info(f"Found unit by external listing ID: {external_listing_id}")

            return unit
        except Exception as e:
            logger.error(f"Error matching by external listing ID: {e}")
            return None
    
    def _map_status_and_outcome(self, event_type: str, return_record: Optional[Return] = None, parsed_email: Dict = None) -> tuple:
        """
        Map event type to status and outcome
        
        Args:
            event_type: Event type from email
            return_record: Optional existing return record
            parsed_email: Optional parsed email data
        
        Returns:
            tuple: (status_current, final_outcome)
        """
        if event_type == 'return_opened':
            return 'opened', 'still_open'
        elif event_type == 'buyer_shipped':
            return 'buyer_shipped', 'still_open'
        elif event_type == 'delivered_back':
            return 'delivered_back', 'still_open'
        elif event_type == 'refund_issued':
            # Check if delivered back
            delivered = False
            if return_record and return_record.item_delivered_back_at:
                delivered = True
            if parsed_email and parsed_email.get('item_delivered_back_at'):
                delivered = True
                
            if delivered:
                return 'refunded', 'refunded_after_return_received'
            else:
                return 'refunded', 'refunded_without_return_received'
        elif event_type == 'closed_no_ship':
            return 'closed_no_buyer_shipment', 'closed_buyer_never_shipped'
            
        return 'opened', 'still_open'
    
    def get_return_by_id(self, return_id: str) -> Optional[Return]:
        """Get return by eBay return ID"""
        return self.db.query(Return).filter(
            Return.return_id == return_id
        ).first()
    
    def get_return_events(self, return_internal_id: str) -> list:
        """Get all events for a return"""
        return self.db.query(ReturnEvent).filter(
            ReturnEvent.return_id == return_internal_id
        ).order_by(ReturnEvent.created_at).all()
