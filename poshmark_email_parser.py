"""
Poshmark Email Parser - Regex-based parsing for Poshmark sale emails
Optimized for speed and cost (no AI required for Poshmark)
"""
import logging
import re
from typing import Dict, Optional , List

logger = logging.getLogger(__name__)


class PoshmarkEmailParser:
    """Fast regex-based parser for Poshmark sale notification emails"""
    
    def __init__(self):
        # Poshmark email senders
        self.poshmark_senders = [
            'poshmark.com',
            'orders@poshmark.com',
            'no-reply@poshmark.com',
            'quicksilver32309@gmail.com'
        ]
    
    def is_poshmark_email(self, from_email: str) -> bool:
        """Check if email is from Poshmark"""
        from_email_lower = from_email.lower()
        return any(sender in from_email_lower for sender in self.poshmark_senders)
    
    # def parse(self, email_data: Dict) -> Optional[Dict]:
    #     """
    #     Parse Poshmark sale email
        
    #     Args:
    #         email_data: Email data from Gmail with keys:
    #             - subject: Email subject
    #             - body: Email body (HTML)
    #             - message_id: Gmail message ID
    #             - from: Sender email
        
    #     Returns:
    #         dict: Parsed sale information:
    #         {
    #             'listing_id': str,  # From image URL in HTML
    #             'sku': str,  # From "SKU: XXXXXXXX"
    #             'title': str,  # Item title
    #             'price': float,  # From "Price: $XX.XX"
    #             'buyer_name': str,  # From "Buyer" section
    #             'order_id': str,  # From "Order ID"
    #             'sold_date': str,  # From "Order Date"
    #             'platform': 'poshmark',
    #             'message_id': str
    #         }
    #     """
    #     try:
    #         subject = email_data.get('subject', '')
    #         body = email_data.get('body', '')
    #         from_email = email_data.get('from', '')
    #         message_id = email_data.get('message_id', '')
            
    #         # Verify this is a Poshmark email
    #         if not self.is_poshmark_email(from_email):
    #             logger.warning(f"Not a Poshmark email: {from_email}")
    #             return None
            
    #         # Verify this is a sale notification
    #         if not self._is_sale_notification(subject):
    #             logger.debug(f"Not a sale notification: {subject}")
    #             return None
            
    #         result = {
    #             'listing_id': self._extract_listing_id(body),
    #             'sku': self._extract_sku(body),
    #             'title': self._extract_title(subject, body),
    #             'price': self._extract_price(body),
    #             'buyer_name': self._extract_buyer_name(body),
    #             'order_id': self._extract_order_id(body),
    #             'sold_date': self._extract_sold_date(body),
    #             'platform': 'poshmark',
    #             'message_id': message_id
    #         }
            
    #         # Log what was found
    #         logger.info(f"Parsed Poshmark email: order={result['order_id']}, sku={result['sku']}, price=${result['price']}")
            
    #         # Return None if critical fields are missing
    #         if not result['order_id'] and not result['sku']:
    #             logger.warning("Could not extract order_id or SKU from Poshmark email")
    #             return None
            
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"Error parsing Poshmark email: {e}")
    #         return None
    
    
    # def parse(self, email_data: Dict) -> Optional[Dict]:
    #     """
    #     Parse Poshmark sale email (supports bundle sales)
        
    #     Returns:
    #         dict: Parsed sale information:
    #         {
    #             'listing_id': str,
    #             'skus': list,  # Changed from 'sku' to 'skus' (list)
    #             'title': str,
    #             'price': float,
    #             'buyer_name': str,
    #             'order_id': str,
    #             'sold_date': str,
    #             'platform': 'poshmark',
    #             'message_id': str,
    #             'is_bundle': bool  # True if multiple items
    #         }
    #     """
    #     try:
    #         subject = email_data.get('subject', '')
    #         body = email_data.get('body', '')
    #         from_email = email_data.get('from', '')
    #         message_id = email_data.get('message_id', '')
            
    #         # Verify this is a Poshmark email
    #         if not self.is_poshmark_email(from_email):
    #             logger.warning(f"Not a Poshmark email: {from_email}")
    #             return None
            
    #         # Verify this is a sale notification
    #         if not self._is_sale_notification(subject):
    #             logger.debug(f"Not a sale notification: {subject}")
    #             return None
            
    #         skus = self._extract_sku(body)  # Returns list now
            
    #         result = {
    #             'listing_id': self._extract_listing_id(body),
    #             'skus': skus,  # Changed from 'sku' to 'skus'
    #             'title': self._extract_title(subject, body),
    #             'price': self._extract_price(body),
    #             'buyer_name': self._extract_buyer_name(body),
    #             'order_id': self._extract_order_id(body),
    #             'sold_date': self._extract_sold_date(body),
    #             'platform': 'poshmark',
    #             'message_id': message_id,
    #             'is_bundle': len(skus) > 1  # Flag for bundle sales
    #         }
            
    #         # Log what was found
    #         if result['is_bundle']:
    #             logger.info(f"Parsed Poshmark BUNDLE email: order={result['order_id']}, {len(skus)} items: {skus}, total=${result['price']}")
    #         else:
    #             logger.info(f"Parsed Poshmark email: order={result['order_id']}, sku={skus[0] if skus else 'none'}, price=${result['price']}")
            
    #         # Return None if critical fields are missing
    #         if not result['order_id'] and not result['skus']:
    #             logger.warning("Could not extract order_id or SKU from Poshmark email")
    #             return None
            
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"Error parsing Poshmark email: {e}")
    #         return None
        

    

    def parse(self, email_data: Dict) -> Optional[List[Dict]]:
        """
        Parse Poshmark sale email - returns LIST of items (handles bundles)
        
        Args:
            email_data: Email data from Gmail
        
        Returns:
            List[Dict]: List of sale items (1 for single sale, 2+ for bundles)
            Each dict contains: {
                'listing_id': str,
                'sku': str,
                'title': str,
                'price': float,
                'buyer_name': str,
                'order_id': str,
                'sold_date': str,
                'platform': 'poshmark',
                'message_id': str
            }
            Returns None if parsing fails
        """
        try:
            subject = email_data.get('subject', '')
            body = email_data.get('body', '')
            from_email = email_data.get('from', '')
            message_id = email_data.get('message_id', '')
            
            # Verify this is a Poshmark email
            if not self.is_poshmark_email(from_email):
                logger.warning(f"Not a Poshmark email: {from_email}")
                return None
            
            # Verify this is a sale notification
            if not self._is_sale_notification(subject):
                logger.debug(f"Not a sale notification: {subject}")
                return None
            
            # Extract shared data (same for all items in bundle)
            order_id = self._extract_order_id(body)
            buyer_name = self._extract_buyer_name(body)
            sold_date = self._extract_sold_date(body)
            
            # Extract per-item data (unique for each item)
            skus = self._extract_sku(body)  # Returns list
            listing_ids = self._extract_listing_id(body)  # Returns list
            titles = self._extract_titles(body, subject)  # Returns list
            prices = self._extract_prices(body)  # Returns list
            
            if not skus:
                logger.warning("Could not extract any SKUs from Poshmark email")
                return None
            
            # If we don't have enough listing_ids, repeat the first one
            if len(listing_ids) < len(skus):
                logger.warning(f"Only found {len(listing_ids)} listing_id(s) for {len(skus)} SKU(s), using first listing_id")
                listing_ids = listing_ids + [listing_ids[0]] * (len(skus) - len(listing_ids)) if listing_ids else [None] * len(skus)
            
            # If we don't have individual prices, divide total equally
            if len(prices) < len(skus):
                total_price = prices[0] if prices else 0.0
                prices = [total_price / len(skus)] * len(skus)
            
            # If we don't have individual titles, use subject
            if len(titles) < len(skus):
                subject_title = self._extract_title(subject, body) or subject
                titles = titles + [subject_title] * (len(skus) - len(titles))
            
            # Create individual item dicts
            items = []
            for i in range(len(skus)):
                item = {
                    'listing_id': listing_ids[i],
                    'sku': skus[i],
                    'title': titles[i],
                    'price': prices[i],
                    'order_id': order_id,
                    'buyer_name': buyer_name,
                    'sold_date': sold_date,
                    'platform': 'poshmark',
                    'message_id': message_id
                }
                items.append(item)
                logger.debug(f"Item {i+1}: SKU={item['sku']}, listing_id={item['listing_id']}, price=${item['price']:.2f}")
            
            # Log summary
            if len(items) > 1:
                logger.info(f"Parsed Poshmark BUNDLE: {len(items)} items, Order={order_id}, Total=${sum(prices):.2f}")
            else:
                logger.info(f"Parsed Poshmark sale: SKU={items[0]['sku']}, Order={order_id}, Price=${items[0]['price']:.2f}")
            
            return items
            
        except Exception as e:
            logger.error(f"Error parsing Poshmark email: {e}")
            return None


    def _is_sale_notification(self, subject: str) -> bool:
        """Check if subject indicates a sale"""
        sale_keywords = [
            'just sold',
            'you sold',
            'sale on poshmark'
        ]
        subject_lower = subject.lower()
        return any(keyword in subject_lower for keyword in sale_keywords)
    
    def _extract_title(self, subject: str, body: str) -> Optional[str]:
        """
        Extract item title from subject or body
        
        Subject format: "TITLE" just sold to @USERNAME on Poshmark!
        Or from body: you just sold "TITLE" on Poshmark
        """
        # Try subject first (more reliable)
        # Pattern: "TITLE" or 'TITLE' in quotes
        patterns = [
            r'"([^"]+)"\s*just sold',  # "TITLE" just sold
            r'you just sold\s*"([^"]+)"',  # you just sold "TITLE"
            r'Fwd:\s*"([^"]+)"',  # Fwd: "TITLE"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                logger.debug(f"Extracted title from subject: {title}")
                return title
        
        # Try body if subject fails
        body_patterns = [
            r'you just sold\s*&quot;([^&]+)&quot;',  # HTML encoded quotes
            r'you just sold\s*"([^"]+)"',
        ]
        
        for pattern in body_patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                logger.debug(f"Extracted title from body: {title}")
                return title
        
        logger.warning("Could not extract title from Poshmark email")
        return None
    

    def _extract_price(self, body: str) -> Optional[float]:
        """
        Extract sale price from body
        
        Pattern in HTML:
        Price: $50.00
        or in table: <td>$50.00</td>
        """
        # Pattern: Price: $XX.XX or just $XX.XX in price column
        patterns = [
            r'Price:\s*\$([0-9,]+\.?[0-9]{2})',  # Price: $50.00
            r'<span[^>]*>Price:\s*\$([0-9,]+\.?[0-9]{2})</span>',  # HTML span
            # In table after "Price" header
            r'Price</td>.*?\$([0-9,]+\.?[0-9]{2})',
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
        
        logger.warning("Could not extract price from Poshmark email body")
        return None
    
    
    def _extract_titles(self, body: str, subject: str) -> list:
        """
        Extract individual item titles from bundle email body
        
        In bundle emails, each item has its title before the Size/SKU info
        Pattern in HTML:
        <td>Kuru Roam Atla Womens Size 6 Brown White Casual Walking Leather Slip-On Shoes</td>
        <tr><td>Size: 6</td></tr>
        <tr><td>SKU: 00089825</td></tr>
        
        <td>Kuru Atom Womens Size 6.5D Blue Black Athletic Running Walking Shoes Sneakers</td>
        <tr><td>Size: 6.5</td></tr>
        <tr><td>SKU: 00089894</td></tr>
        
        Returns:
            list: List of title strings
        """
        titles = []
        
        # Pattern: Title appears before Size/SKU in table cells
        # Look for text in <td> that comes before "Size:" or "SKU:"
        pattern = r'<td[^>]*>([^<]+(?:Womens|Mens|Kids|Boys|Girls|Toddler|Baby)[^<]+(?:Shoes|Sneakers|Sandals|Boots|Clogs)[^<]*)</td>'
        
        matches = re.findall(pattern, body, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            # Clean up the title
            title = match.strip()
            # Remove extra whitespace
            title = re.sub(r'\s+', ' ', title)
            # Skip if it's too short (likely not a real title)
            if len(title) > 20:
                titles.append(title)
                logger.debug(f"Extracted title: {title[:50]}...")
        
        # Fallback: If no titles found, try to extract from subject
        if not titles:
            subject_title = self._extract_title(subject, body)
            if subject_title:
                titles.append(subject_title)
        
        return titles


    def _extract_prices(self, body: str) -> list:
        """
        Extract ALL individual item prices from body (supports bundles)
        
        Pattern in HTML:
        <span>Price: $49.00</span>
        <span>Price: $59.00</span>
        
        Or:
        Offer Price: $75.00  (this is the total, not individual)
        
        Returns:
            list: List of price floats
        """
        prices = []
        
        # Pattern 1: Individual item prices (Price: $XX.XX)
        patterns = [
            r'<span[^>]*>Price:\s*\$([0-9,]+\.?[0-9]{2})</span>',
            r'Price:\s*\$([0-9,]+\.?[0-9]{2})',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, body, re.IGNORECASE)
            for match in matches:
                try:
                    price = float(match.replace(',', ''))
                    prices.append(price)
                    logger.debug(f"Extracted price: ${price:.2f}")
                except ValueError:
                    continue
        
        # If no individual prices found, look for "Offer Price" (total bundle price)
        if not prices:
            offer_pattern = r'Offer Price:\s*\$([0-9,]+\.?[0-9]{2})'
            match = re.search(offer_pattern, body, re.IGNORECASE)
            if match:
                try:
                    total_price = float(match.group(1).replace(',', ''))
                    prices.append(total_price)
                    logger.debug(f"Extracted total bundle price: ${total_price:.2f}")
                except ValueError:
                    pass
        
        if not prices:
            logger.warning("Could not extract any prices from Poshmark email")
        
        return prices
    
    # def _extract_sku(self, body: str) -> Optional[str]:
    #     """
    #     Extract SKU from body
        
    #     Pattern in HTML:
    #     SKU: 00042150
    #     """
    #     # Pattern: SKU: followed by alphanumeric
    #     patterns = [
    #         r'SKU:\s*([A-Z0-9]+)',
    #         r'<td[^>]*>SKU:\s*([A-Z0-9]+)</td>',
    #     ]
        
    #     for pattern in patterns:
    #         match = re.search(pattern, body, re.IGNORECASE)
    #         if match:
    #             sku = match.group(1).strip()
    #             logger.debug(f"Extracted SKU: {sku}")
    #             return sku
        
    #     logger.debug("Could not extract SKU from Poshmark email")
    #     return None


    # def _extract_sku(self, body: str) -> list:
    #     """
    #     Extract ALL SKUs from body (supports bundle sales with multiple items)
        
    #     Pattern in HTML:
    #     SKU: 00042150
    #     SKU: 00089894
        
    #     Returns:
    #         list: List of SKU strings (empty list if none found)
    #     """
    #     # Pattern: SKU: followed by alphanumeric
    #     patterns = [
    #         r'SKU:\s*([A-Z0-9]+)',
    #         r'<td[^>]*>SKU:\s*([A-Z0-9]+)</td>',
    #     ]
        
    #     skus = []
    #     for pattern in patterns:
    #         matches = re.findall(pattern, body, re.IGNORECASE)  # Changed from search to findall
    #         if matches:
    #             skus.extend([sku.strip() for sku in matches])
        
    #     # Remove duplicates while preserving order
    #     seen = set()
    #     unique_skus = []
    #     for sku in skus:
    #         if sku not in seen:
    #             seen.add(sku)
    #             unique_skus.append(sku)
        
    #     if unique_skus:
    #         logger.debug(f"Extracted {len(unique_skus)} SKU(s): {unique_skus}")
    #     else:
    #         logger.debug("Could not extract SKU from Poshmark email")
        
    #     return unique_skus
    

    def _extract_sku(self, body: str) -> list:
        """
        Extract ALL SKUs from body (supports bundle sales with multiple items)
        
        Pattern in HTML:
        SKU: 00042150
        SKU: 00089894
        
        Returns:
            list: List of SKU strings (empty list if none found)
        """
        patterns = [
            r'SKU:\s*([A-Z0-9]+)',
            r'<td[^>]*>SKU:\s*([A-Z0-9]+)</td>',
        ]
        
        skus = []
        for pattern in patterns:
            matches = re.findall(pattern, body, re.IGNORECASE)
            if matches:
                skus.extend([sku.strip() for sku in matches])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_skus = []
        for sku in skus:
            if sku not in seen:
                seen.add(sku)
                unique_skus.append(sku)
        
        if unique_skus:
            logger.debug(f"Extracted {len(unique_skus)} SKU(s): {unique_skus}")
        else:
            logger.debug("Could not extract SKU from Poshmark email")
        
        return unique_skus
    
    def _extract_buyer_name(self, body: str) -> Optional[str]:
        """
        Extract buyer name from body
        
        Pattern in HTML:
        <td style="font-weight:bold">Buyer </td>
        <tr><td>Davina Meyer</td></tr>
        """
        # Pattern: After "Buyer" label, get next table cell content
        patterns = [
            r'<td[^>]*>Buyer\s*</td>.*?<td>([A-Za-z\s\-\.\']+)</td>',
            r'Buyer\s*</td>.*?<td>([A-Za-z\s\-\.\']+)</td>',
            r'Buyer.*?\n([A-Za-z\s\-\.\']+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                name = match.group(1).strip()
                # Clean up any remaining HTML
                name = re.sub(r'<[^>]+>', '', name)
                # Validate name (should not contain numbers or weird chars)
                if re.match(r'^[A-Za-z\s\-\.\']+$', name) and len(name) > 2:
                    logger.debug(f"Extracted buyer name: {name}")
                    return name
        
        logger.debug("Could not extract buyer name from Poshmark email")
        return None
    
    def _extract_order_id(self, body: str) -> Optional[str]:
        """
        Extract order ID
        
        Pattern in HTML:
        <td style="font-weight:bold">Order ID</td>
        <tr><td>5d8a4c3a65d17f6ff9dd87ad</td></tr>
        """
        # Pattern: After "Order ID" label
        patterns = [
            r'Order ID.*?<td>([a-f0-9]+)</td>',
            r'Order ID.*?\n([a-f0-9]{24})',  # Poshmark uses 24-char hex IDs
            r'<td[^>]*>Order ID</td>.*?<td>([a-f0-9]+)</td>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                order_id = match.group(1).strip()
                logger.debug(f"Extracted order ID: {order_id}")
                return order_id
        
        logger.warning("Could not extract order_id from Poshmark email")
        return None
    
    def _extract_sold_date(self, body: str) -> Optional[str]:
        """
        Extract sold date
        
        Pattern in HTML:
        <td style="font-weight:bold">Order Date</td>
        <tr><td>December 08, 2025</td></tr>
        """
        # Pattern: After "Order Date" label
        patterns = [
            r'Order Date.*?<td>([A-Za-z]+\s+\d+,\s+\d{4})</td>',
            r'Order Date.*?\n([A-Za-z]+\s+\d+,\s+\d{4})',
            r'<td[^>]*>Order Date</td>.*?<td>([A-Za-z]+\s+\d+,\s+\d{4})</td>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                sold_date = match.group(1).strip()
                logger.debug(f"Extracted sold date: {sold_date}")
                return sold_date
        
        logger.debug("Could not extract sold_date from Poshmark email")
        return None
    
    # def _extract_listing_id(self, body: str) -> Optional[str]:
    #     """
    #     Extract listing ID from image URL
        
    #     Pattern in HTML:
    #     <img src="https://di2ponv0v5otw.cloudfront.net/posts/2025/10/23/68fadc61ac1b049a7e8a4d06/m_68fadc62d7555df6e8c191ad.jpeg">
        
    #     Listing ID is the part after /posts/YYYY/MM/DD/LISTING_ID/
    #     """
    #     # Pattern: Image URL contains listing ID
    #     patterns = [
    #         r'/posts/\d{4}/\d{2}/\d{2}/([a-f0-9]+)/',  # cloudfront URL
    #         r'poshmark\.com/listing/([a-f0-9]+)',  # Direct listing URL
    #     ]
        
    #     for pattern in patterns:
    #         match = re.search(pattern, body)
    #         if match:
    #             listing_id = match.group(1).strip()
    #             logger.debug(f"Extracted listing ID: {listing_id}")
    #             return listing_id
        
    #     logger.debug("Could not extract listing_id from Poshmark email")
    #     return None

    
    def _extract_listing_id(self, body: str) -> list:
        """
        Extract ALL listing IDs from image URLs (supports bundle sales)
        
        Pattern in HTML:
        <img src="https://di2ponv0v5otw.cloudfront.net/posts/2025/10/23/68fadc61ac1b049a7e8a4d06/m_68fadc62d7555df6e8c191ad.jpeg">
        
        Listing ID is the part after /posts/YYYY/MM/DD/LISTING_ID/
        
        Returns:
            list: List of listing ID strings (empty list if none found)
        """
        patterns = [
            r'/posts/\d{4}/\d{2}/\d{2}/([a-f0-9]+)/',  # cloudfront URL
            r'poshmark\.com/listing/([a-f0-9]+)',  # Direct listing URL
        ]
        
        listing_ids = []
        for pattern in patterns:
            matches = re.findall(pattern, body)
            if matches:
                listing_ids.extend([lid.strip() for lid in matches])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_listing_ids = []
        for lid in listing_ids:
            if lid not in seen:
                seen.add(lid)
                unique_listing_ids.append(lid)
        
        if unique_listing_ids:
            logger.debug(f"Extracted {len(unique_listing_ids)} listing ID(s): {unique_listing_ids}")
        else:
            logger.debug("Could not extract listing_id from Poshmark email")
        
        return unique_listing_ids

    
# # Testing function
# def test_parser():
#     """Test the Poshmark parser with sample data"""
    
#     sample_email = {
#         'subject': 'Fwd: "Birkenstock Profi Birki Womens Size 7 Black Walking Professional Clogs Shoes" just sold to @bxsicdxvinx on Poshmark!',
#         'from': 'orders@poshmark.com',
#         'message_id': '19b094cfc222055d',
#         'body': '''
# <td style="font-weight:bold">Buyer </td>
# <tr><td>Davina Meyer</td></tr>
# <tr><td>@bxsicdxvinx</td></tr>

# <td style="font-weight:bold">Order Date</td>
# <tr><td>December 08, 2025</td></tr>

# <td style="font-weight:bold">Order ID</td>
# <tr><td>5d8a4c3a65d17f6ff9dd87ad</td></tr>

# <td>Birkenstock Profi Birki Womens Size 7 Black Walking Professional Clogs Shoes</td>
# <tr><td>Size: 7</td></tr>
# <tr><td><span>Price: $50.00</span></td></tr>
# <tr><td style="white-space:initial">SKU: 00042150</td></tr>

# <img height="75" src="https://di2ponv0v5otw.cloudfront.net/posts/2025/10/23/68fadc61ac1b049a7e8a4d06/m_68fadc62d7555df6e8c191ad.jpeg" width="75">
#         '''
#     }
    
#     parser = PoshmarkEmailParser()
#     result = parser.parse(sample_email)
    
#     print("Parsed result:")
#     print(result)
    
#     expected = {
#         'listing_id': '68fadc61ac1b049a7e8a4d06',
#         'sku': '00042150',
#         'title': 'Birkenstock Profi Birki Womens Size 7 Black Walking Professional Clogs Shoes',
#         'price': 50.0,
#         'buyer_name': 'Davina Meyer',
#         'order_id': '5d8a4c3a65d17f6ff9dd87ad',
#         'sold_date': 'December 08, 2025',
#         'platform': 'poshmark',
#         'message_id': '19b094cfc222055d'
#     }
    
#     print("\nExpected:")
#     print(expected)
    
#     print("\nMatches:")
#     for key in expected:
#         match = "✓" if result.get(key) == expected[key] else "✗"
#         print(f"{match} {key}: {result.get(key)} == {expected[key]}")



def test_parser():
    """Test the Poshmark parser with sample data including bundle sale"""
    
    # Test 1: Single item sale
    single_item_email = {
        'subject': 'Fwd: "Birkenstock Profi Birki Womens Size 7" just sold to @buyer on Poshmark!',
        'from': 'orders@poshmark.com',
        'message_id': '19b094cfc222055d',
        'body': '''
<td>SKU: 00042150</td>
<td>Price: $50.00</td>
<td>Order ID</td>
<tr><td>5d8a4c3a65d17f6ff9dd87ad</td></tr>
        '''
    }
    
    # Test 2: Bundle sale (2 items)
    bundle_email = {
        'subject': '"Bundle of Kuru Roam Atla Womens Size 6... and 1 more item" just sold to @cbliznik on Poshmark!',
        'from': 'orders@poshmark.com',
        'message_id': '00089894',
        'body': '''
<td>Kuru Roam Atla Womens Size 6 Brown White Casual Walking Leather Slip-On Shoes</td>
<tr><td>Size: 6</td></tr>
<tr><td>SKU: 00089925</td></tr>
<tr><td><span>Price: $49.00</span></td></tr>

<td>Kuru Atom Womens Size 6.5D Blue Black Athletic Running Walking Shoes Sneakers</td>
<tr><td>Size: 6.5</td></tr>
<tr><td>SKU: 00089894</td></tr>
<tr><td><span>Price: $59.00</span></td></tr>

<td>Order ID</td>
<tr><td>69950453a6bb765eb79e5a12</td></tr>
<tr><td>Offer Price: $75.00</td></tr>
        '''
    }
    
    parser = PoshmarkEmailParser()
    
    print("="*60)
    print("TEST 1: Single Item Sale")
    print("="*60)
    result1 = parser.parse(single_item_email)
    print(f"SKUs found: {result1['skus']}")
    print(f"Is bundle: {result1['is_bundle']}")
    print(f"Match: {'✓' if result1['skus'] == ['00042150'] else '✗'}")
    
    print("\n" + "="*60)
    print("TEST 2: Bundle Sale (2 items)")
    print("="*60)
    result2 = parser.parse(bundle_email)
    print(f"SKUs found: {result2['skus']}")
    print(f"Is bundle: {result2['is_bundle']}")
    print(f"Match: {'✓' if result2['skus'] == ['00089925', '00089894'] else '✗'}")
    print(f"Price: ${result2['price']}")


if __name__ == '__main__':
    test_parser()

if __name__ == '__main__':
    test_parser()