"""
eBay Return Email Parser
Parses eBay return notification emails to extract return data.

This implementation was refined against real eBay return notifications
captured from the seller mailbox. See `returns/tests/fixtures/` (when
added) for representative samples.

Recognised email types (subject -> event_type):
  "Return <id>: Return approved"         -> return_opened
  "Return <id>: Buyer shipped item"      -> buyer_shipped
  "Return <id>: Refund initiated"        -> refund_issued
  "Return <id>: Issue refund"            -> unknown (reminder, no state change)
  "Return <id>: Return closed"           -> closed_other
  "eBay Customer Support made a decision"
    + body "buyer did not return"        -> closed_no_ship
"""
import email.utils
import logging
import re
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Sanity bounds for parsed body dates (R2-15).
_MIN_DATE = datetime(2020, 1, 1)

# Username stop-words: things that pattern-match the regex shape but aren't
# real eBay usernames. eBay usernames are 6-64 chars of [a-z0-9_.-].
_USERNAME_STOPWORDS = {
    'ebay', 'a', 'an', 'the', 'buyer', 'seller', 'item', 'this', 'that',
    'you', 'your', 'we', 'us', 'me', 'evan', 'amazon',
}


class EbayReturnParser:
    """Parser for eBay return notification emails."""

    def __init__(self):
        # Event detection patterns - ordered by specificity. First hit wins.
        # Phrases come straight from observed eBay email bodies/subjects.
        self.event_patterns = {
            'closed_no_ship': [
                # Customer Support decision email
                'did not return the item to you within the required timeframe',
                'closed it without any refund to the buyer',
                # Generic phrasings (kept for forward compat)
                'buyer did not ship',
                'buyer never shipped',
                'return closed automatically',
                'no refund required',
            ],
            'refund_issued': [
                'refund initiated',
                'refund has been initiated',
                'refund for this item',
                'thank you for initiating a refund',
                'refund sent',
                'refund issued',
                'you issued a refund',
                'we issued a refund',
            ],
            'delivered_back': [
                'item delivered back',
                'return delivered',
                'package was delivered',
                'we received the return',
            ],
            'buyer_shipped': [
                'has started shipping your item back',
                'started shipping your item back',
                'buyer shipped your return',
                'buyer has shipped the item back',
                'buyer shipped the item back',
                'refund your buyer when the item is delivered',
                'shipped your return',
            ],
            'return_opened': [
                'is returning the item',
                'is returning this item',
                'return has been automatically approved',
                'return approved',
                'return request from',
                'buyer opened a return',
                'buyer is returning',
                'return has been opened',
                'return request opened',
            ],
            # New: explicit "this return is closed" with no other signal.
            # Treated as closed_other in status mapping.
            'closed_other': [
                'this return is closed',
                'the return request for the item has been closed',
                'return has been closed',
            ],
            # New: nudge emails (no state change in eBay's system; the
            # return is still open). Map to "unknown" downstream so we
            # don't accidentally advance status.
            'reminder': [
                'we noticed that you haven',
                'issue a refund by',
            ],
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def parse(self, email_data: Dict) -> Optional[Dict]:
        try:
            subject = email_data.get('subject', '') or ''
            body_raw = email_data.get('body', '') or ''
            from_email = (email_data.get('from', '') or '').lower()
            message_id = email_data.get('message_id', 'unknown')
            date_header = email_data.get('date', '')

            text_body = _html_to_text(body_raw)

            logger.info(f"[PARSE] msg_id={message_id}")
            logger.debug(f"[PARSE] subject={subject}")

            is_ebay = ('ebay' in from_email) or _looks_forwarded_ebay(text_body)
            if not is_ebay:
                logger.info("[PARSE] skipped - not an eBay email")
                return None

            if not self._is_return_email(subject, text_body):
                logger.info("[PARSE] skipped - not a return-related email")
                return None

            event_type = self._detect_event_type(subject, text_body)
            # Reminder emails are real, but don't change return state -
            # surface as 'unknown' so the service records the event for
            # audit and leaves status alone (R2-16).
            if event_type == 'reminder':
                logger.info("[PARSE] reminder email - recording as unknown event")
                event_type = 'unknown'
            logger.info(f"[PARSE] event_type={event_type}")

            return_id = self._extract_return_id(subject, text_body)
            order_number = self._extract_order_number(text_body)
            buyer_username = self._extract_buyer_username(subject, text_body)
            tracking_number = self._extract_tracking_number(text_body)
            email_received_at = _parse_email_date_header(date_header)

            parsed_data = {
                'email_message_id': message_id,
                'email_subject': subject,
                'email_received_at': email_received_at,
                'event_type': event_type,
                'return_id': return_id,
                'order_number': order_number,
                'buyer_username': buyer_username,
                'return_reason_ebay': self._extract_return_reason(text_body),
                'buyer_comment': self._extract_buyer_comment(text_body),
                'request_amount': self._extract_amount(text_body),
                'opened_at': self._extract_opened_date(text_body),
                'buyer_ship_by_date': self._extract_ship_by_date(text_body),
                'buyer_shipped_at': self._extract_shipped_date(text_body),
                'tracking_number': tracking_number,
                'item_delivered_back_at': self._extract_delivered_date(text_body),
                'refund_issued_at': self._extract_refund_date(text_body),
                'raw_body': body_raw,
            }
            return {k: v for k, v in parsed_data.items() if v is not None}

        except Exception as e:
            logger.error(f"[PARSE] critical error: {e}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Email gating
    # ------------------------------------------------------------------
    def _is_return_email(self, subject: str, text_body: str) -> bool:
        """R2-10: requires a specific return phrase, not bare 'return'."""
        text = (subject + ' ' + text_body).lower()
        specific_phrases = [
            # Subject-line "Return <id>:" matches everything from the sample set.
            'return ',  # checked below with a stricter regex
            # Body / phrase signals
            'return request',
            'return approved',
            'return opened',
            'return has been opened',
            'return has been closed',
            'return request has been closed',
            'return is closed',
            'is returning the item',
            'is returning this item',
            'buyer is returning',
            'buyer opened a return',
            'buyer shipped your return',
            'buyer has shipped the item back',
            'started shipping your item back',
            'buyer did not ship',
            'buyer did not return the item',
            'buyer never shipped',
            'return delivered',
            'return was delivered',
            'return case',
            'refund sent',
            'refund issued',
            'refund initiated',
            'refund for this item',
            'we received the return',
            'item delivered back',
            'refund your buyer',
        ]
        # The phrase "return " on its own is too loose; require the
        # eBay subject template "Return <digits>:" OR another specific phrase.
        if re.search(r'^return\s+\d{8,15}\s*:', subject, re.IGNORECASE):
            return True
        return any(phrase in text for phrase in specific_phrases if phrase != 'return ')

    def _detect_event_type(self, subject: str, text_body: str) -> str:
        """R2-16: returns 'unknown' instead of silently defaulting to opened.

        Subject-line cues are checked first because eBay's subjects are the
        most reliable signal across the sample set.
        """
        subj_lower = subject.lower()
        body_lower = text_body.lower()
        combined = subj_lower + ' ' + body_lower

        # Subject suffix mapping (after "Return <id>:")
        subject_map = [
            ('refund initiated', 'refund_issued'),
            ('refund sent',      'refund_issued'),
            ('refund issued',    'refund_issued'),
            ('return approved',  'return_opened'),
            ('return opened',    'return_opened'),
            ('return request',   'return_opened'),
            ('buyer shipped item', 'buyer_shipped'),
            ('issue refund',     'reminder'),   # nudge, see parse()
            ('return closed',    'closed_other'),
            ('case has been closed', 'closed_other'),
            ('customer support made a decision', 'closed_no_ship'),
        ]
        for phrase, event in subject_map:
            if phrase in subj_lower:
                # For "Customer Support made a decision", the actual outcome
                # depends on the body. Check body for the "did not return"
                # signal before classifying as closed_no_ship.
                if event == 'closed_no_ship' and 'did not return the item' not in body_lower:
                    continue
                return event

        # Body pattern fallback
        for event_type, patterns in self.event_patterns.items():
            if any(pattern in combined for pattern in patterns):
                return event_type
        return 'unknown'

    # ------------------------------------------------------------------
    # Field extractors
    # ------------------------------------------------------------------
    def _extract_return_id(self, subject: str, body: str) -> Optional[str]:
        """R2-08: eBay subject 'Return <id>: ...' first, then labelled body patterns."""
        # Subject pattern - the dominant format in the sample set.
        m = re.search(r'^Return\s+(\d{8,15})\s*:', subject, re.IGNORECASE)
        if m:
            return m.group(1)

        patterns = [
            r'return\s+id[:\s#]+(\d{8,15})',
            r'return\s+number[:\s#]+(\d{8,15})',
            r'case\s+id[:\s#]+(\d{8,15})',
            r'case\s+number[:\s#]+(\d{8,15})',
            r'case\s*#\s*(\d{8,15})',
            r'request\s+id[:\s#]+(\d{8,15})',
            r'/returns?/(\d{8,15})\b',
        ]
        for pattern in patterns:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

    def _extract_order_number(self, body: str) -> Optional[str]:
        """eBay order numbers like 16-14357-82449."""
        patterns = [
            r'order\s+number[:\s#]+([\d\-]{10,20})',
            r'order\s+id[:\s#]+([\d\-]{10,20})',
            r'order[:\s#]+(\d{2,4}-\d{4,6}-\d{4,6})',
            r'sale\s+record[:\s#]+([\d\-]{8,})',
            r'transaction[:\s#]+([\d\-]{10,})',
        ]
        for pattern in patterns:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                value = m.group(1)
                if any(c.isdigit() for c in value) and len(value) >= 10:
                    return value
        return None

    def _extract_buyer_username(self, subject: str, body: str) -> Optional[str]:
        """R2-09: scoped to phrasings observed in real eBay return emails."""
        # Body patterns observed in the sample set, in priority order.
        body_patterns = [
            # "The buyer justme77ellen is returning the item"
            r'the\s+buyer\s+([a-z0-9._\-]{3,64})\s+is\s+returning',
            # "A $15.99 refund for this item to braxtonbaileigh has been initiated"
            r'refund\s+for\s+this\s+item\s+to\s+([a-z0-9._\-]{3,64})\s+has\s+been',
            # "readersmith has started shipping your item back to you"
            r'\b([a-z0-9._\-]{3,64})\s+has\s+started\s+shipping',
            # "Thank you for initiating a refund to braxtonbaileigh"
            r'initiating\s+a\s+refund\s+to\s+([a-z0-9._\-]{3,64})',
            # "buyer <username> ..."
            r'\bbuyer\s+([a-z0-9._\-]{3,64})\s+(?:opened|shipped|returned|requested|is)',
            # Structured field "Buyer name: <username>"
            r'\bbuyer\s+name[:\s]+([a-z0-9._\-]{3,64})',
            r'\bmember\s+name[:\s]+([a-z0-9._\-]{3,64})',
            r'\busername[:\s]+([a-z0-9._\-]{3,64})',
        ]
        for pattern in body_patterns:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                username = m.group(1).lower()
                if username not in _USERNAME_STOPWORDS:
                    return username

        # Subject patterns are less common in the sample set, but kept.
        subject_patterns = [
            r'return\s+request\s+from\s+([a-z0-9._\-]{3,64})',
            r'request\s+from\s+([a-z0-9._\-]{3,64})',
            r'([a-z0-9._\-]{3,64})\s+opened\s+a\s+return',
            r'([a-z0-9._\-]{3,64})\s+wants\s+to\s+return',
        ]
        for pattern in subject_patterns:
            m = re.search(pattern, subject, re.IGNORECASE)
            if m:
                username = m.group(1).lower()
                if username not in _USERNAME_STOPWORDS:
                    return username

        return None

    def _extract_return_reason(self, body: str) -> Optional[str]:
        patterns = [
            r'return\s+reason[:\s]+(.+?)(?:\r?\n|\.)',
            r'reason\s+for\s+return[:\s]+(.+?)(?:\r?\n|\.)',
            r"why\s+they(?:'re|\s+are)\s+returning[:\s]+(.+?)(?:\r?\n|\.)",
            r'reason[:\s]+(.+?)(?:\r?\n|\.)',
        ]
        for pattern in patterns:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                return m.group(1).strip()[:200]
        return None

    def _extract_buyer_comment(self, body: str) -> Optional[str]:
        patterns = [
            r"buyer'?s?\s+comment[:\s]+(.+?)(?:\r?\n\r?\n|$)",
            r'buyer\s+said[:\s]+(.+?)(?:\r?\n\r?\n|$)',
            r'comment[:\s]+(.+?)(?:\r?\n\r?\n|$)',
        ]
        for pattern in patterns:
            m = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if m:
                return m.group(1).strip()[:1000]
        return None

    def _extract_amount(self, body: str) -> Optional[float]:
        """R2-14: refund/request context required.

        Real eBay emails phrase this as "A $15.99 refund for this item ..."
        with the dollar amount BEFORE the word "refund". The earlier
        version of this method only looked for "refund amount: $X".
        """
        patterns = [
            # "$15.99 refund" - the dominant pattern in real emails.
            r'\$([\d,]+\.\d{2})\s+refund',
            # Structured labels (still useful).
            r'refund\s+amount[:\s]+\$?([\d,]+\.?\d*)',
            r'request\s+amount[:\s]+\$?([\d,]+\.?\d*)',
            r'amount\s+requested[:\s]+\$?([\d,]+\.?\d*)',
            r'refund\s+total[:\s]+\$?([\d,]+\.?\d*)',
            r'refunding\s+\$?([\d,]+\.?\d*)',
        ]
        for pattern in patterns:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                try:
                    return float(m.group(1).replace(',', ''))
                except ValueError:
                    continue
        return None

    def _extract_opened_date(self, body: str) -> Optional[datetime]:
        return _extract_date_near(body, ['opened', 'requested', 'started'])

    def _extract_ship_by_date(self, body: str) -> Optional[datetime]:
        return _extract_date_near(body, ['ship by', 'return by', 'deadline'])

    def _extract_shipped_date(self, body: str) -> Optional[datetime]:
        return _extract_date_near(body, ['shipped', 'sent on'])

    def _extract_delivered_date(self, body: str) -> Optional[datetime]:
        return _extract_date_near(body, ['delivered', 'received on'])

    def _extract_refund_date(self, body: str) -> Optional[datetime]:
        return _extract_date_near(body, ['refund issued', 'refund sent', 'issued on'])

    def _extract_tracking_number(self, body: str) -> Optional[str]:
        patterns = [
            r'tracking\s+number[:\s]+([A-Z0-9]{10,30})',
            r'tracking[:\s#]+([A-Z0-9]{10,30})',
            r'track\s+package[:\s]+([A-Z0-9]{10,30})',
        ]
        for pattern in patterns:
            m = re.search(pattern, body, re.IGNORECASE)
            if m:
                return m.group(1)
        return None


# ---------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------
def _html_to_text(body: str) -> str:
    if not body:
        return ''
    if '<' not in body and '>' not in body:
        return body
    try:
        soup = BeautifulSoup(body, 'html.parser')
        for tag in soup(['script', 'style']):
            tag.decompose()
        text = soup.get_text(separator=' ', strip=True)
        return re.sub(r'\s+', ' ', text)
    except Exception as e:
        logger.warning(f"[PARSE] HTML strip failed, falling back to raw: {e}")
        return body


def _looks_forwarded_ebay(text_body: str) -> bool:
    lower = text_body.lower()
    return ('from: ' in lower and 'ebay' in lower) or 'ebay.com' in lower


def _parse_email_date_header(date_header: str) -> Optional[datetime]:
    if not date_header:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(date_header)
        if dt is None:
            return None
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (TypeError, ValueError):
        return None


def _extract_date_near(body: str, keywords) -> Optional[datetime]:
    if not body:
        return None
    raw_date_pattern = (
        r'(\d{1,2}/\d{1,2}/\d{2,4}'
        r'|\d{4}-\d{1,2}-\d{1,2}'
        r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})'
    )
    for keyword in keywords:
        pattern = rf'{re.escape(keyword)}[^A-Za-z0-9]{{0,25}}?{raw_date_pattern}'
        m = re.search(pattern, body, re.IGNORECASE)
        if m:
            parsed = _parse_date_string(m.group(1))
            if parsed and _date_is_sane(parsed):
                return parsed
    return None


def _parse_date_string(date_str: str) -> Optional[datetime]:
    for fmt in ('%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d',
                '%B %d, %Y', '%b %d, %Y', '%B %d %Y', '%b %d %Y'):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def _date_is_sane(dt: datetime) -> bool:
    if dt < _MIN_DATE:
        return False
    if (dt - datetime.utcnow()).days > 1:
        return False
    return True
