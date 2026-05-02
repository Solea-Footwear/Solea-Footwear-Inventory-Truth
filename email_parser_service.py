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
from dotenv import load_dotenv


# ebay email parser
from delisting.ebay_email_parser import EbayEmailParser
from delisting.poshmark_email_parser import PoshmarkEmailParser
from delisting.mercari_email_parser import MercariEmailParser




logger = logging.getLogger(__name__)

class EmailParserService:
    """Service for parsing sale emails using Claude AI"""
    
    def __init__(self):
        self.api_key = os.getenv('ANTHROPIC_API_KEY')
        if not self.api_key:
            logger.warning("ANTHROPIC_API_KEY not set")
        self.client = anthropic.Anthropic(api_key=self.api_key) if self.api_key else None
        
        # Init ebay email parser
        self.ebay_parser = EbayEmailParser()

        # Init poshmark email parser
        self.poshmark_parser = PoshmarkEmailParser()

        # init mercari email parser
        self.mercari_parser = MercariEmailParser()
    
    # def parse_sale_email(self, email_data: Dict) -> Optional[Dict]:
    #     """
    #     Parse sale email to extract key information
        
    #     Args:
    #         email_data (dict): Email data from Gmail
        
    #     Returns:
    #         dict: Parsed sale information or None
    #     """
    #     platform = email_data.get('platform', 'unknown')
        
    #     if platform == 'unknown':
    #         logger.warning("Unknown platform, skipping")
    #         return None
        
        
    #     # Use it first for eBay
    #     if platform == 'ebay':
    #         result = self.ebay_parser.parse(email_data)
    #         if result:
    #             return result


    #     # In parse_sale_email:
    #     if platform == 'poshmark':
    #         result = self.poshmark_parser.parse(email_data)
    #         if result:
    #             return result
            

    #     # In parse_sale_email:
    #     if platform == 'mercari':
    #         result = self.mercari_parser.parse(email_data)
    #         if result:
    #             return result

    #     # ! uncomment below code if you want to use ai and comment above blocks for specific platforms.
    #     # Try AI parsing first
    #     print("Using Ai to fetch email parsing result...")
    #     if self.client:
    #         result = self._parse_with_ai(email_data)
    #         if result:
    #             return result
        
    #     # Fallback to rule-based parsing
    #     logger.info(f"Falling back to rule-based parsing for {platform}")
    #     return self._parse_with_rules(email_data)
    
    
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

            # Poshmark - returns list (handles bundles)
            if platform == 'poshmark':
                result = self.poshmark_parser.parse(email_data)
                return result if result else []  # Already a list

            # Mercari - returns single item, wrap in list
            if platform == 'mercari':
                result = self.mercari_parser.parse(email_data)
                return [result] if result else []

            # ! uncomment below code if you want to use ai and comment above blocks for specific platforms.
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
        elif platform == 'poshmark':
            return self._parse_poshmark_email(subject, body, email_data.get('message_id'))
        elif platform == 'mercari':
            return self._parse_mercari_email(subject, body, email_data.get('message_id'))
        
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
    
    def _parse_poshmark_email(self, subject: str, body: str, message_id: str) -> Optional[Dict]:
        """Parse Poshmark sale email"""
        try:
            result = {
                'platform': 'poshmark',
                'message_id': message_id,
                'sku': None,
                'title': None,
                'price': None,
                'order_id': None
            }
            
            # Poshmark emails usually have title in subject
            # "Congrats! You sold [ITEM NAME]"
            title_match = re.search(r'sold\s+(.+)', subject, re.IGNORECASE)
            if title_match:
                result['title'] = title_match.group(1).strip()
            
            # Extract SKU from title or body (if seller includes it)
            sku_patterns = [
                r'SKU[:\s]+([A-Z0-9\-]+)',
                r'\(([A-Z0-9\-]+)\)',  # SKU in parentheses
            ]
            
            for pattern in sku_patterns:
                match = re.search(pattern, body, re.IGNORECASE)
                if match:
                    result['sku'] = match.group(1).strip()
                    break
            
            # Extract price
            price_match = re.search(r'\$([0-9,]+\.[0-9]{2})', body)
            if price_match:
                result['price'] = float(price_match.group(1).replace(',', ''))
            
            return result if result['sku'] or result['title'] else None
            
        except Exception as e:
            logger.error(f"Error parsing Poshmark email: {e}")
            return None
    
    def _parse_mercari_email(self, subject: str, body: str, message_id: str) -> Optional[Dict]:
        """Parse Mercari sale email"""
        try:
            result = {
                'platform': 'mercari',
                'message_id': message_id,
                'sku': None,
                'title': None,
                'price': None,
                'order_id': None
            }
            
            # Mercari subject: "You made a sale!"
            # Title usually in body
            title_match = re.search(r'Item:[:\s]+(.+)', body, re.IGNORECASE)
            if title_match:
                result['title'] = title_match.group(1).strip().split('\n')[0]
            
            # Extract SKU
            sku_patterns = [
                r'SKU[:\s]+([A-Z0-9\-]+)',
                r'\(([A-Z0-9\-]+)\)',
            ]
            
            for pattern in sku_patterns:
                match = re.search(pattern, body, re.IGNORECASE)
                if match:
                    result['sku'] = match.group(1).strip()
                    break
            
            # Extract price
            price_match = re.search(r'\$([0-9,]+\.[0-9]{2})', body)
            if price_match:
                result['price'] = float(price_match.group(1).replace(',', ''))
            
            return result if result['sku'] or result['title'] else None
            
        except Exception as e:
            logger.error(f"Error parsing Mercari email: {e}")
            return None
