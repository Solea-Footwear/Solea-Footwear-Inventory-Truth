"""
eBay Email Parser - Regex-based parsing for eBay sale emails
Optimized for speed and cost (no AI required for eBay)
"""
import logging
import re
from typing import Dict, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class EbayEmailParser:
    """Fast regex-based parser for eBay sale notification emails"""
    
    def __init__(self):
        # eBay email senders
        self.ebay_senders = [
            'ebay@ebay.com',
            'members@ebay.com',
            'ebay@reply.ebay.com',
            'notification@ebay.com',
            'ebay.com'
        ]
    
    def is_ebay_email(self, from_email: str) -> bool:
        """Check if email is from eBay"""
        from_email_lower = from_email.lower()
        return any(sender in from_email_lower for sender in self.ebay_senders)
    
    def parse(self, email_data: Dict) -> List[Dict]:
        """
        Parse eBay sale email
        
        Args:
            email_data: Email data from Gmail with keys:
                - subject: Email subject
                - body: Email body (HTML)
                - message_id: Gmail message ID
                - from: Sender email
        
        Returns:
            dict: Parsed sale information:
            {
                'listing_id': None,  # Will be fetched later using order_id
                'sku': None,  # Not in eBay emails
                'title': str,  # From subject
                'price': float,  # From "Sold: $XX.XX"
                'buyer_name': str,  # From shipping details
                'order_id': str,  # From "Order: XX-XXXXX-XXXXX"
                'sold_date': str,  # From "Date sold: Dec 10, 2025 09:16"
                'platform': 'ebay',
                'message_id': str
            }
        """
        try:
            subject = email_data.get('subject', '')
            body = email_data.get('body', '')
            from_email = email_data.get('from', '')
            message_id = email_data.get('message_id', '')
            
            # Verify this is an eBay email
            if not self.is_ebay_email(from_email):
                logger.warning(f"Not an eBay email: {from_email}")
                return []
            
            # Verify this is a sale notification
            if not self._is_sale_notification(subject):
                logger.debug(f"Not a sale notification: {subject}")
                return []
            
            result = {
                'listing_id': None,  # eBay doesn't include this in email
                'sku': None,  # eBay doesn't include this in email
                'title': self._extract_title(subject),
                'price': self._extract_price(body),
                'buyer_name': self._extract_buyer_name(body),
                'order_id': self._extract_order_id(body),
                'sold_date': self._extract_sold_date(body),
                'platform': 'ebay',
                'message_id': message_id
            }
            
            # Log what was found
            logger.info(f"Parsed eBay email: order={result['order_id']}, price=${result['price']}, title={result['title'][:50] if result['title'] else 'N/A'}")
            
            # Return None if critical fields are missing
            if not result['order_id'] and not result['title']:
                logger.warning("Could not extract order_id or title from eBay email")
                return []
            
            order_id = result.get('order_id')

            if not order_id:
                return [result]  # fallback

            order_items = self._get_order_items_from_ebay(order_id)

            if not order_items:
                return [result]  # fallback

            results = []

            for item in order_items:
                enriched = result.copy()
                enriched['listing_id'] = item.get('listing_id')
                enriched['sku'] = item.get('sku')
                results.append(enriched)

            return results
            
        except Exception as e:
            logger.error(f"Error parsing eBay email: {e}")
            return []
    def _get_order_items_from_ebay(self, order_id: str) -> List[Dict]:
        """
        Fetch all items (item_id + SKU) from eBay order
        """
        try:
            from src.integrations.ebay.ebay_api import ebay_api

            response = ebay_api.api.execute('GetOrders', {
                'OrderIDArray': {'OrderID': order_id}
            })

            results = []

            if response.reply.OrderArray:
                order = response.reply.OrderArray.Order[0]

                if order.TransactionArray:
                    transactions = order.TransactionArray.Transaction

                    for transaction in transactions:
                        item_id = transaction.Item.ItemID
                        sku = transaction.Item.SKU if hasattr(transaction.Item, 'SKU') else None

                        results.append({
                            'listing_id': item_id,
                            'sku': sku
                        })

            logger.info(f"Fetched {len(results)} items from eBay order {order_id}")
            return results

        except Exception as e:
            logger.error(f"Error fetching eBay order items: {e}")
            return []
        
    def _is_sale_notification(self, subject: str) -> bool:
        """Check if subject indicates a sale"""
        sale_keywords = [
            'you made the sale',
            'congratulations',
            'you sold',
            'item sold'
        ]
        subject_lower = subject.lower()
        return any(keyword in subject_lower for keyword in sale_keywords)
    
    def _extract_title(self, subject: str) -> Optional[str]:
        """
        Extract item title from subject
        
        Subject format: "You made the sale for [TITLE]"
        """
        # Pattern: "You made the sale for [TITLE]"
        patterns = [
            r'You made the sale for\s+(.+)',
            r'sold for\s+(.+)',
            r'Congratulations.*?for\s+(.+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                # Remove any trailing punctuation
                title = title.rstrip('!.,')
                return title
        
        logger.warning(f"Could not extract title from subject: {subject}")
        return None
    
    def _extract_price(self, body: str) -> Optional[float]:
        """
        Extract sale price from body
        
        Pattern in HTML:
        <h4>Sold:</h4>
        ...
        <span class="blueFix">$51.00</span>
        """
        # Pattern: "Sold:" followed by price (with HTML tags in between)
        patterns = [
            # Pattern 1: Sold: with HTML tags, then price
            r'Sold:.*?\$([0-9,]+\.?[0-9]{2})',
            # Pattern 2: Sale price with HTML
            r'Sale price:.*?\$([0-9,]+\.?[0-9]{2})',
            # Pattern 3: Just find any price near "Sold"
            r'Sold[:\s\w<>="\/\-]*\$([0-9,]+\.[0-9]{2})',
            # Pattern 4: Price in blueFix span
            r'<span class="blueFix">\$([0-9,]+\.?[0-9]{2})</span>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    price_str = match.group(1).replace(',', '')
                    price = float(price_str)
                    logger.debug(f"Extracted price: ${price}")
                    return price
                except ValueError:
                    continue
        
        logger.warning("Could not extract price from eBay email body")
        return None
    
    def _extract_buyer_name(self, body: str) -> Optional[str]:
        """
        Extract buyer name from shipping details
        
        Pattern in HTML:
        <h3>Your buyer's shipping details:</h3>
        <p>Peter Pastoret<br/>
        2241 Bluff Blvd<br/>
        """
        # Pattern: After "shipping details:" find <p> tag, get first line before <br/>
        patterns = [
            # Pattern 1: shipping details, then <p>, then name before <br/>
            r'shipping details:.*?<p[^>]*>(.*?)<br',
            # Pattern 2: shipping details, then any tags, then name on next line
            r'shipping details:.*?>\s*([A-Za-z\s\-\.]+)\s*<br',
            # Pattern 3: Ship to with similar structure
            r'Ship to:.*?<p[^>]*>(.*?)<br',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                name = match.group(1).strip()
                # Clean up any remaining HTML tags
                name = re.sub(r'<[^>]+>', '', name)
                # Validate name (should not contain numbers or weird chars)
                if re.match(r'^[A-Za-z\s\-\.\']+$', name) and len(name) > 2:
                    logger.debug(f"Extracted buyer name: {name}")
                    return name
        
        logger.debug("Could not extract buyer name from eBay email")
        return None
    
    def _extract_order_id(self, body: str) -> Optional[str]:
        """
        Extract order ID
        
        Pattern in HTML:
        <h4>Order:</h4>
        ...
        <p>27-13930-98148</p>
        """
        # Pattern: "Order:" followed by order ID (with HTML in between)
        patterns = [
            r'Order:.*?([0-9]{2}-[0-9]{5}-[0-9]{5})',  # eBay format: XX-XXXXX-XXXXX
            r'Order[:\s\w<>="\/\-]*([0-9]{2}-[0-9]{5}-[0-9]{5})',
            r'Order number:.*?([0-9\-]+)',
            r'Order #:.*?([0-9\-]+)',
            r'Transaction:.*?([0-9\-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                order_id = match.group(1).strip()
                logger.debug(f"Extracted order ID: {order_id}")
                return order_id
        
        logger.warning("Could not extract order_id from eBay email")
        return None
    
    def _extract_sold_date(self, body: str) -> Optional[str]:
        """
        Extract sold date
        
        Pattern in HTML:
        <h4>Date sold:</h4>
        ...
        <p>Dec 10, 2025 09:16</p>
        """
        # Pattern: "Date sold:" followed by date (with HTML in between)
        patterns = [
            r'Date sold:.*?([A-Za-z]+\s+\d+,\s+\d{4}\s+\d{2}:\d{2})',
            r'Sold on:.*?([A-Za-z]+\s+\d+,\s+\d{4}\s+\d{2}:\d{2})',
            r'Sale date:.*?([A-Za-z]+\s+\d+,\s+\d{4}\s+\d{2}:\d{2})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                sold_date = match.group(1).strip()
                logger.debug(f"Extracted sold date: {sold_date}")
                return sold_date
        
        logger.debug("Could not extract sold_date from eBay email")
        return None


# Testing function
def test_parser():
    """Test the eBay parser with sample data"""
    
    sample_email = {
        'subject': 'You made the sale for Kuru Atom Womens Size 11 Black White Gum Athletic Running Walking Shoes Sneakers',
        'from': 'ebay@ebay.com',
        'message_id': '19b094406267238a',
        'body': '''
Your buyer's shipping details:
Peter Pastoret
2241 Bluff Blvd
columbia, MO 65201-6101
United States

Ship by:
Dec 11, 2025

Your buyer paid and now it's time to get a shipping label.

Sold:
$51.00

Order:
27-13930-98148

Date sold:
Dec 10, 2025 09:16

Buyer:
jppast10
        '''
    }
    
    parser = EbayEmailParser()
    result = parser.parse(sample_email)
    
    print("Parsed result:")
    print(result)
    
    expected = {
        'listing_id': None,
        'sku': None,
        'title': 'Kuru Atom Womens Size 11 Black White Gum Athletic Running Walking Shoes Sneakers',
        'price': 51.0,
        'buyer_name': 'Peter Pastoret',
        'order_id': '27-13930-98148',
        'sold_date': 'Dec 10, 2025 09:16',
        'platform': 'ebay',
        'message_id': '19b094406267238a'
    }
    
    print("\nExpected:")
    print(expected)
    
    print("\nMatches:")
    parsed_item = result[0] if result else {}

    for key in expected:
        match = "✓" if parsed_item.get(key) == expected[key] else "✗"
        print(f"{match} {key}: {parsed_item.get(key)} == {expected[key]}")


if __name__ == '__main__':
    test_parser()
