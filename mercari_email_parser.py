"""
Mercari Email Parser - Regex-based parsing for Mercari sale emails
Optimized for speed and cost (no AI required for Mercari)
"""
import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class MercariEmailParser:
    """Fast regex-based parser for Mercari sale notification emails"""
    
    def __init__(self):
        self.mercari_senders = [
            'no-reply@alerts.us.mercari.com',
            'mercari.com'
        ]
    
    def is_mercari_email(self, from_email: str) -> bool:
        """Check if email is from Mercari"""
        from_email_lower = from_email.lower()
        return any(sender in from_email_lower for sender in self.mercari_senders)
    
    def parse(self, email_data: Dict) -> Optional[Dict]:
        """
        Parse Mercari sale email
        
        Returns:
            {
                'listing_id': str,   # From "ID: m75684672719"
                'sku': None,         # Mercari doesn't include SKU
                'title': str,        # From subject or item details section
                'price': float,      # From price table ($35.00)
                'buyer_name': str,   # From "Ship to:" section
                'order_id': None,    # Mercari doesn't include order ID in email
                'sold_date': 'Today',# Mercari always shows "Today"
                'platform': 'mercari',
                'message_id': str,
                'external_listing_id': str,
                'external_order_id': None,
                'event_type': 'sale',
                'raw_message_id': str,
            }
        """
        try:
            subject = email_data.get('subject', '')
            body = email_data.get('body', '')
            from_email = email_data.get('from', '')
            message_id = email_data.get('message_id', '')
            
            if not self.is_mercari_email(from_email):
                logger.warning(f"Not a Mercari email: {from_email}")
                return None
            
            if not self._is_sale_notification(subject):
                logger.debug(f"Not a sale notification: {subject}")
                return None
            
            listing_id = self._extract_listing_id(body)

            result = {
                'listing_id': listing_id,
                'external_listing_id': listing_id,
                'sku': None,          # Mercari doesn't include SKU in emails
                'title': self._extract_title(subject, body),
                'price': self._extract_price(body),
                'buyer_name': self._extract_buyer_name(body),
                'order_id': None,     # Mercari doesn't include order ID in emails
                'external_order_id': None,
                'sold_date': self._extract_sold_date(email_data.get('date', '')),
                'platform': 'mercari',
                'event_type': 'sale',
                'message_id': message_id,
                'raw_message_id': message_id,
            }

            if not result['listing_id']:
                logger.warning(
                    "mercari_parse_missing_listing_id",
                    extra={
                        "title": result["title"],
                        "message_id": message_id,
                        "body_snippet": body[:500]
                    }
                )
                return None

            if result['price'] is None:
                logger.warning(
                    "mercari_parse_missing_price",
                    extra={
                        "listing_id": result["listing_id"],
                        "message_id": message_id,
                        "body_snippet": body[:500]
                    }
                )            
            
            logger.info(
                "mercari_parse_success",
                extra={
                    "listing_id": result["listing_id"],
                    "price": result["price"],
                    "buyer": result["buyer_name"],
                    "message_id": message_id
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing Mercari email: {e}")
            return None
    
    def _is_sale_notification(self, subject: str) -> bool:
        """Check if subject indicates a sale"""
        sale_patterns = [
            r"you['’]?ve made a sale",
            r"you made a sale",
        ]

        return any(re.search(p, subject, re.IGNORECASE) for p in sale_patterns)
    
    def _extract_title(self, subject: str, body: str) -> Optional[str]:
        """
        Extract item title
        
        Subject format: "You've made a sale: TITLE"
        Or from HTML body item details section:
        <a style="...">Timberland Teddy Fleece...</a>
        """
        # Pattern 1: From subject "You've made a sale: TITLE"
        match = re.search(r"You've made a sale:\s*(.+)", subject, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 2: From HTML - item name link inside e2-details-item-detail-with-id-itemName
        match = re.search(
            r'class="e2-details-item-detail-with-id-itemName[^"]*"[^>]*>.*?<a[^>]*>([^<]+)</a>',
            body, re.IGNORECASE | re.DOTALL
        )
        if match:
            return match.group(1).strip()
        
        # Pattern 3: Plain text fallback - title before "ID: m..."
        match = re.search(
            r'Item details\s*\r?\n([^\r\n]+)\s*\(',
            body, re.IGNORECASE
        )
        if match:
            return match.group(1).strip()
        
        return None
    
    def _extract_price(self, body: str) -> Optional[float]:
        """
        Extract sale price
        
        Pattern in HTML payment table:
        <p class="e2-details-payment-detail-itemName">Price</p>
        ...
        <p class="e2-details-payment-detail-itemPrice">$35.00</p>
        
        Also in plain text:
        Price
        $35.00
        -$3.50  (selling fee)
        """
        # Pattern 1: Price row in payment detail table (first price = sale price)
        match = re.search(
            r'e2-details-payment-detail-itemName[^>]*>\s*Price\s*</p>.*?'
            r'e2-details-payment-detail-itemPrice[^>]*>\s*\$([0-9,]+\.?[0-9]{2})',
            body, re.IGNORECASE | re.DOTALL
        )
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                pass
        
        # Pattern 2: Plain text - "Price\n\n$XX.XX" (before selling fee)
        match = re.search(
            r'Price\s*\r?\n+\s*\$([0-9,]+\.[0-9]{2})\s*\r?\n',
            body, re.IGNORECASE
        )
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                pass
        
        return None
    
    def _extract_buyer_name(self, body: str) -> Optional[str]:
        """
        Extract buyer name from "Ship to:" section
        """
        # Pattern 1: HTML - after "Ship to:" get text before next <br>
        match = re.search(
            r'Ship to:</span><br[^>]*>\s*([^<\r\n]+?)\s*<br',
            body,
            re.IGNORECASE | re.DOTALL
        )
        if match:
            name = match.group(1).strip()
            name = re.sub(r'[^A-Za-z\s\-\.\']', '', name).strip()
            if len(name) > 2:
                return name

        # Pattern 2: Plain text - after "Ship to:"
        match = re.search(
            r'Ship to:\s*\r?\n([^\r\n]+)',
            body,
            re.IGNORECASE
        )
        if match:
            name = match.group(1).strip()
            name = re.sub(r'[^A-Za-z\s\-\.\']', '', name).strip()
            if len(name) > 2:
                return name

        return None
    
    def _extract_sold_date(self, date_str: str) -> Optional[str]:
        """
        Extract sold date from email date header
        
        Input:  "Tue, 16 Dec 2025 13:22:00 +0000 (UTC)"
        Output: "Dec 16, 2025 13:22"
        """
        if not date_str:
            return 'Today'
        
        try:
            from email.utils import parsedate
            from datetime import datetime
            
            parsed = parsedate(date_str)
            if parsed:
                dt = datetime(*parsed[:6])
                return dt.strftime('%b %d, %Y %H:%M')
        except Exception as e:
            logger.debug(f"Could not parse date: {e}")
        
        return date_str  # Return raw if parsing fails

    def _extract_listing_id(self, body: str) -> Optional[str]:
        """
        Extract Mercari listing ID
        
        Pattern in HTML and plain text:
        ID: m75684672719
        
        Also in image URL:
        src="https://u-mercari-images.mercdn.net/thumb/photos/m75684672719_1.jpg"
        """
        # Pattern 1: "ID: X..." where X is any single letter followed by digits
        match = re.search(r'ID:\s*([a-z][0-9]+)', body, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Pattern 2: From image URL thumb/photos/XXXXX_1.jpg
        match = re.search(
            r'mercdn\.net/thumb/photos/([a-z][0-9]+)_\d+\.jpg',
            body
        )
        if match:
            return match.group(1).strip()
            
        # Pattern 3: Last fallback - any Mercari-style listing ID in body
        match = re.search(r'\bm[0-9]{8,}\b', body, re.IGNORECASE)
        if match:
            return match.group(0).strip()
        
        return None
