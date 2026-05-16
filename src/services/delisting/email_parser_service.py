"""
Email Parser Service using Claude AI
Parses sale notification emails to extract SKU, price, platform
"""
import logging
import os
import json
import re
from typing import Dict, Optional , List
import anthropic
import psycopg2.extras
from src.backend.db.database import acquire_conn, release_conn


from src.services.delisting.ebay_email_parser import EbayEmailParser




logger = logging.getLogger(__name__)

class EmailParserService:
    """Service for parsing sale emails using Claude AI"""
    
    def __init__(self):
        self.api_key = os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            logger.warning("ANTHROPIC_API_KEY not set")
        self.client = anthropic.Anthropic(api_key=self.api_key) if self.api_key else None
        
        self.ebay_parser = EbayEmailParser()
      
    
    def parse_sale_email(self, email_data: Dict) -> List[Dict]:
        """
        Parse sale email to extract key information
        Returns list of items (handles bundles)
        
        Args:
            email_data (dict): Email data from Gmail
        
        Returns:
            List[Dict]: List of parsed sale items (empty list if parsing fails)
        """
        platform = email_data.get('platform', 'unknown')
        
        if platform == 'unknown':
            logger.warning("Unknown platform, skipping")
            return []
        
        try:
            # eBay - returns single item, wrap in list
            if platform == 'ebay':
                result = self.ebay_parser.parse(email_data)
                return result if result else []

            # Try AI parsing first
            print("Using AI to fetch email parsing result...")
            if self.client:
                result = self._parse_with_ai(email_data)
                return [result] if result else []
            
            # Fallback to rule-based parsing
            logger.info(f"Falling back to rule-based parsing for {platform}")
            result = self._parse_with_rules(email_data)
            return [result] if result else []
        
        except Exception as e:
            logger.error(f"Error parsing sale email: {e}")
            return []

    def _update_marketplace_event_sku(self, platform: str, message_id: str, sku: str) -> None:
        conn = acquire_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE marketplace_events
                    SET sku = %s,
                        raw_payload = jsonb_set(raw_payload, '{sku}', to_jsonb(%s::text), true)
                    WHERE platform = %s AND message_id = %s
                    """,
                    [sku, sku, platform, message_id]
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("marketplace_event_sku_update_failed platform=%s message_id=%s error=%s",
                         platform, message_id, e)
        finally:
            release_conn(conn)

    def _mark_marketplace_event_needs_reconciliation(self, platform: str, message_id: str, reason: str) -> None:
        conn = acquire_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE marketplace_events
                    SET raw_payload = jsonb_set(
                        jsonb_set(raw_payload, '{needs_reconciliation}', 'true'::jsonb, true),
                        '{reconciliation_reason}', to_jsonb(%s::text), true
                    )
                    WHERE platform = %s AND message_id = %s
                    """,
                    [reason, platform, message_id]
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("marketplace_event_reconciliation_update_failed platform=%s message_id=%s error=%s",
                         platform, message_id, e)
        finally:
            release_conn(conn)

    def _insert_marketplace_event(self, parsed_event: Dict) -> bool:
        conn = acquire_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO marketplace_events
                        (platform, event_type, message_id, external_listing_id,
                         external_order_id, sku, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (platform, message_id) DO NOTHING
                    RETURNING id
                    """,
                    [
                        parsed_event["platform"],
                        parsed_event["event_type"],
                        parsed_event["message_id"],
                        parsed_event.get("external_listing_id"),
                        parsed_event.get("external_order_id"),
                        parsed_event.get("sku"),
                        json.dumps(parsed_event),
                    ]
                )
                row = cur.fetchone()
            conn.commit()
            return row is not None
        except Exception as e:
            conn.rollback()
            logger.error("marketplace_event_insert_failed message_id=%s error=%s",
                         parsed_event.get("message_id"), e)
            raise
        finally:
            release_conn(conn)

    def _parse_with_ai(self, email_data: Dict) -> Optional[Dict]:
        """
        Parse email using Claude AI
        
        Args:
            email_data (dict): Email data
        
        Returns:
            dict: Parsed data
        """
        try:
            subject = email_data.get('subject', '')
            body = email_data.get('body', '')
            platform = email_data.get('platform', 'unknown')
            
            # Create prompt
            prompt = f"""Parse this {platform} sale notification email and extract the following information in JSON format:

{{
  "listing_id": "platform listing ID (IMPORTANT: for Poshmark extract from image URL like /posts/2025/10/23/LISTING_ID/)",
  "sku": "product SKU or item number (look for SKU:, Item #:, or similar)",
  "title": "item title or description",
  "price": "sale price as number only (no $ symbol)",
  "buyer_name": "buyer's name if available",
  "order_id": "order or transaction ID if available",
  "sold_date": "sale date if mentioned"
}}

Email Subject: {subject}

Email Body:
{body}

Only return valid JSON. If a field is not found, use null.
"""
            
            # Call Claude API
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            # Parse response
            response_text = message.content[0].text.strip()
            
            # Extract JSON from response (handle markdown code blocks)
            if '```json' in response_text:
                json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(1)
            elif '```' in response_text:
                json_match = re.search(r'```\s*(.*?)\s*```', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(1)
            
            parsed_data = json.loads(response_text)
            
            # Add platform
            parsed_data['platform'] = platform
            parsed_data['message_id'] = email_data.get('message_id')
            
            # Convert price to float
            if parsed_data.get('price'):
                try:
                    parsed_data['price'] = float(str(parsed_data['price']).replace('$', '').replace(',', ''))
                except:
                    parsed_data['price'] = None
            
            logger.info(f"AI parsed email: SKU={parsed_data.get('sku')}, Price=${parsed_data.get('price')}")
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing with AI: {e}")
            return None
    
    def _parse_with_rules(self, email_data: Dict) -> Optional[Dict]:
        """
        Parse email using rule-based extraction
        
        Args:
            email_data (dict): Email data
        
        Returns:
            dict: Parsed data
        """
        platform = email_data.get('platform')
        subject = email_data.get('subject', '')
        body = email_data.get('body', '')
        
        if platform == 'ebay':
            return self._parse_ebay_email(subject, body, email_data.get('message_id'))

        return None
    
    def _parse_ebay_email(self, subject: str, body: str, message_id: str) -> Optional[Dict]:
        """Parse eBay sale email"""
        try:
            result = {
                'platform': 'ebay',
                'message_id': message_id,
                'sku': None,
                'title': None,
                'price': None,
                'order_id': None
            }
            
            # Extract SKU (look for "SKU:", "Item #:", etc.)
            sku_patterns = [
                r'SKU[:\s]+([A-Z0-9\-]+)',
                r'Item\s+#[:\s]+([A-Z0-9\-]+)',
                r'Custom\s+label[:\s]+([A-Z0-9\-]+)'
            ]
            
            for pattern in sku_patterns:
                match = re.search(pattern, body, re.IGNORECASE)
                if match:
                    result['sku'] = match.group(1).strip()
                    break
            
            # Extract price
            price_patterns = [
                r'\$([0-9,]+\.[0-9]{2})',
                r'([0-9,]+\.[0-9]{2})\s+USD'
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, body)
                if match:
                    result['price'] = float(match.group(1).replace(',', ''))
                    break
            
            # Extract order ID
            order_match = re.search(r'Order\s+#[:\s]+([0-9\-]+)', body, re.IGNORECASE)
            if order_match:
                result['order_id'] = order_match.group(1).strip()
            
            # Extract title from subject
            title_match = re.search(r'sold[:\s]+(.+)', subject, re.IGNORECASE)
            if title_match:
                result['title'] = title_match.group(1).strip()
            
            return result if result['sku'] or result['title'] else None
            
        except Exception as e:
            logger.error(f"Error parsing eBay email: {e}")
            return None
    
