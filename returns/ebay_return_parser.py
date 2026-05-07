"""
eBay Return Email Parser
Parses eBay return notification emails to extract return data
"""
import logging
import re
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class EbayReturnParser:
    """Parser for eBay return notification emails"""
    
    def __init__(self):
        self.event_patterns = {
            'return_opened': [
                'return request from',
                'buyer opened a return',
                'return has been opened'
            ],
            'buyer_shipped': [
                'buyer shipped your return',
                'buyer has shipped the item back',
                'tracking number'
            ],
            'delivered_back': [
                'item delivered back',
                'return delivered',
                'package was delivered'
            ],
            'refund_issued': [
                'refund sent',
                'refund issued',
                'you issued a refund'
            ],
            'closed_no_ship': [
                'buyer did not ship',
                'return closed automatically',
                'buyer never shipped',
                'no refund required'
            ]
        }
    
    def parse(self, email_data: Dict) -> Optional[Dict]:
        """
        Parse eBay return email
        
        Args:
            email_data: Email data from Gmail service
        
        Returns:
            dict: Parsed return data or None if parsing fails
        """
        try:
            subject = email_data.get('subject', '')
            body = email_data.get('body', '')
            from_email = email_data.get('from', '')
            message_id = email_data.get('message_id', 'unknown')
            
            logger.info(f"[PARSE] Starting email parse - Message ID: {message_id}")
            logger.debug(f"[PARSE] Subject: {subject}")
            logger.debug(f"[PARSE] From: {from_email}")
            
            # # Check if it's a forwarded email - extract original sender
            # original_from = from_email
            # if 'fwd:' in subject.lower() or 'fw:' in subject.lower():
            #     logger.info(f"[PARSE] Detected forwarded email")
            #     # Try to find original eBay sender in body
            #     import re
            #     ebay_from_match = re.search(r'From:.*?(ebay[^\n<]+)', body, re.IGNORECASE)
            #     if ebay_from_match:
            #         original_from = ebay_from_match.group(1)
            #         logger.info(f"[PARSE] Found original eBay sender in forwarded email: {original_from}")
            
            # Verify it's an eBay return email (check both from_email and body for forwarded emails)
            is_ebay = 'ebay' in from_email.lower() or 'ebay' in body.lower()
            
            if not is_ebay:
                logger.info(f"[PARSE] SKIPPED - Not an eBay email")
                logger.info(f"[PARSE] From: {from_email}")
                logger.info(f"[PARSE] Body contains 'ebay': {('ebay' in body.lower())}")
                return None
            
            # Check if it's a return-related email
            if not self._is_return_email(subject, body):
                logger.info(f"[PARSE] SKIPPED - Not a return email")
                logger.info(f"[PARSE] Subject: {subject}")
                logger.debug(f"[PARSE] Body preview: {body[:300]}")
                logger.info(f"[PARSE] Checked keywords: return request, return opened, buyer shipped, refund sent, etc.")
                return None
            
            logger.info(f"[PARSE] Confirmed eBay return email")
            
            # Detect event type
            event_type = self._detect_event_type(subject, body)
            logger.info(f"[PARSE] Event type detected: {event_type}")
            
            # Extract return data with detailed logging
            return_id = self._extract_return_id(body)
            if return_id:
                logger.info(f"[PARSE] Return ID found: {return_id}")
            else:
                logger.warning(f"[PARSE] Return ID NOT found - will attempt matching by other fields")
            
            order_number = self._extract_order_number(body)
            if order_number:
                logger.info(f"[PARSE] Order number found: {order_number}")
            else:
                logger.warning(f"[PARSE] Order number NOT found")
            
            buyer_username = self._extract_buyer_username(subject, body)
            if buyer_username:
                logger.info(f"[PARSE] Buyer username found: {buyer_username}")
            else:
                logger.warning(f"[PARSE] Buyer username NOT found")
            
            tracking_number = self._extract_tracking_number(body)
            if tracking_number:
                logger.info(f"[PARSE] Tracking number found: {tracking_number}")
            
            # Extract return data
            parsed_data = {
                'email_message_id': message_id,
                'email_subject': subject,
                'event_type': event_type,
                'return_id': return_id,
                'order_number': order_number,
                'buyer_username': buyer_username,
                'return_reason_ebay': self._extract_return_reason(body),
                'buyer_comment': self._extract_buyer_comment(body),
                'request_amount': self._extract_amount(body),
                'opened_at': self._extract_opened_date(body),
                'buyer_ship_by_date': self._extract_ship_by_date(body),
                'buyer_shipped_at': self._extract_shipped_date(body, event_type),
                'tracking_number': tracking_number,
                'item_delivered_back_at': self._extract_delivered_date(body, event_type),
                'refund_issued_at': self._extract_refund_date(body, event_type),
                'raw_body': body[:5000]  # Store first 5000 chars for reference
            }
            
            # Remove None values
            parsed_data = {k: v for k, v in parsed_data.items() if v is not None}
            
            # Log summary of extracted fields
            extracted_fields = [k for k in parsed_data.keys() if k not in ['email_message_id', 'email_subject', 'raw_body']]
            logger.info(f"[PARSE] SUCCESS - Extracted {len(extracted_fields)} fields: {', '.join(extracted_fields)}")
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"[PARSE] CRITICAL ERROR parsing email: {e}", exc_info=True)
            logger.error(f"[PARSE] Email subject: {email_data.get('subject', 'N/A')}")
            logger.error(f"[PARSE] Email from: {email_data.get('from', 'N/A')}")
            return None
    
    def _is_return_email(self, subject: str, body: str) -> bool:
        """Check if email is return-related"""
        return_keywords = [
            'return',  # Generic return keyword
            'return approved',
            'return request',
            'return opened',
            'buyer shipped your return',
            'return delivered',
            'refund sent',
            'buyer did not ship',
            'buyer is returning',
            'return has been',
            'return case'
        ]
        
        text = (subject + ' ' + body).lower()
        found = any(keyword in text for keyword in return_keywords)
        
        if not found:
            # Debug: show what we're checking
            logger.debug(f"[PARSE] Text to check (first 200 chars): {text[:200]}")
            logger.debug(f"[PARSE] Keywords checked: {', '.join(return_keywords)}")
        
        return found
    
    def _detect_event_type(self, subject: str, body: str) -> str:
        """Detect the type of return event"""
        text = (subject + ' ' + body).lower()
        
        for event_type, patterns in self.event_patterns.items():
            if any(pattern in text for pattern in patterns):
                return event_type
        
        return 'return_opened'  # Default
    
    def _extract_return_id(self, body: str) -> Optional[str]:
        """Extract eBay return ID"""
        # Pattern: Return ID: 5123456789 or similar
        # Also check for return ID in URLs like /returns/5123456789
        patterns = [
            r'return\s+id[:\s]+(\d{10,})',
            r'return\s+number[:\s]+(\d{10,})',
            r'case\s+id[:\s]+(\d{10,})',
            r'case[:\s]+(\d{10,})',
            r'/returns?/(\d{10,})',
            r'return[:\s]*#?(\d{10,})',
            r'request\s+id[:\s]+(\d{10,})',
            r'id[:\s]+(\d{10,})'  # Generic ID pattern as fallback
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                return_id = match.group(1)
                # Validate it's a reasonable length (eBay return IDs are typically 10-12 digits)
                if 10 <= len(return_id) <= 15:
                    return return_id
        
        return None
    
    def _extract_order_number(self, body: str) -> Optional[str]:
        """Extract eBay order number"""
        # Pattern: Order number: 27-13930-98148 or similar
        # NOTE: Many eBay return emails do NOT contain order numbers
        # This is optional and may return None
        patterns = [
            r'order\s+number[:\s]+([\d\-]+)',
            r'order[:\s]+([\d\-]{10,})',
            r'transaction[:\s]+([\d\-]+)',
            r'order\s+id[:\s]+([\d\-]+)',
            r'sale\s+record[:\s]+([\d\-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                order_num = match.group(1)
                # Validate it looks like an eBay order number (has dashes and is long enough)
                if len(order_num) >= 10:
                    return order_num
        
        return None
    
    def _extract_buyer_username(self, subject: str, body: str) -> Optional[str]:
        """Extract buyer username"""
        # Try subject first: "Return request from buyer123"
        match = re.search(r'from\s+([a-zA-Z0-9_\-]+)', subject, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Try body
        patterns = [
            r'buyer[:\s]+([a-zA-Z0-9_\-]+)',
            r'username[:\s]+([a-zA-Z0-9_\-]+)',
            r'member[:\s]+([a-zA-Z0-9_\-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_item_title(self, subject: str, body: str) -> Optional[str]:
        """
        Extract item title
        
        NOTE: Item titles in emails are often incomplete or incorrectly formatted.
        It's better to rely on eBay API enrichment for accurate item titles.
        This method is kept for cases where API enrichment is not available.
        """
        # Don't try to extract from email - too unreliable
        # Let eBay API enrichment handle this
        return None
    
    def _extract_return_reason(self, body: str) -> Optional[str]:
        """Extract eBay return reason"""
        patterns = [
            r'reason[:\s]+(.+?)(?:\n|$)',
            r'return\s+reason[:\s]+(.+?)(?:\n|$)',
            r'why\s+they\'re\s+returning[:\s]+(.+?)(?:\n|$)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                reason = match.group(1).strip()
                return reason[:200]  # Limit to 200 chars
        
        return None
    
    def _extract_buyer_comment(self, body: str) -> Optional[str]:
        """Extract buyer's comment"""
        patterns = [
            r'buyer\'s\s+comment[:\s]+(.+?)(?:\n\n|$)',
            r'buyer\s+said[:\s]+(.+?)(?:\n\n|$)',
            r'comment[:\s]+(.+?)(?:\n\n|$)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                comment = match.group(1).strip()
                return comment[:1000]  # Limit to 1000 chars
        
        return None
    
    def _extract_amount(self, body: str) -> Optional[float]:
        """Extract refund/request amount"""
        patterns = [
            r'amount[:\s]+\$?([\d,]+\.?\d*)',
            r'refund[:\s]+\$?([\d,]+\.?\d*)',
            r'total[:\s]+\$?([\d,]+\.?\d*)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    return float(amount_str)
                except ValueError:
                    continue
        
        return None
    
    def _extract_opened_date(self, body: str) -> Optional[datetime]:
        """Extract return opened date"""
        # Look for date patterns
        return self._extract_date(body, ['opened', 'requested', 'started'])
    
    def _extract_ship_by_date(self, body: str) -> Optional[datetime]:
        """Extract buyer ship by date"""
        return self._extract_date(body, ['ship by', 'return by', 'deadline'])
    
    def _extract_shipped_date(self, body: str, event_type: str) -> Optional[datetime]:
        """Extract buyer shipped date"""
        # Don't create fake timestamps - return only if found in email
        return self._extract_date(body, ['shipped', 'sent'])
    
    def _extract_delivered_date(self, body: str, event_type: str) -> Optional[datetime]:
        """Extract delivered back date"""
        # Don't create fake timestamps - return only if found in email
        return self._extract_date(body, ['delivered', 'received'])
    
    def _extract_refund_date(self, body: str, event_type: str) -> Optional[datetime]:
        """Extract refund issued date"""
        # Don't create fake timestamps - return only if found in email
        return self._extract_date(body, ['refund', 'issued'])
    
    def _extract_tracking_number(self, body: str) -> Optional[str]:
        """Extract tracking number"""
        patterns = [
            r'tracking\s+number[:\s]+([A-Z0-9]{10,})',
            r'tracking[:\s]+([A-Z0-9]{10,})',
            r'track\s+package[:\s]+([A-Z0-9]{10,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_date(self, body: str, keywords: list) -> Optional[datetime]:
        """Extract date near specific keywords"""
        # Common date patterns
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
            r'(\d{4}-\d{2}-\d{2})',       # YYYY-MM-DD
            r'(\w+ \d{1,2}, \d{4})'       # Month DD, YYYY
        ]
        
        for keyword in keywords:
            # Look for date near keyword
            keyword_pattern = rf'{keyword}[:\s]+.{{0,50}}?({"|".join(date_patterns)})'
            match = re.search(keyword_pattern, body, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                return self._parse_date(date_str)
        
        return None
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime"""
        date_formats = [
            '%m/%d/%Y',
            '%Y-%m-%d',
            '%B %d, %Y',
            '%b %d, %Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
