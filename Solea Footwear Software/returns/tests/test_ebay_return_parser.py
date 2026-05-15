"""Parser tests against representative eBay return emails.

These are sanitized versions of real seller emails captured from the
solea_footwear inbox. They exist to lock in the behaviour of the parser
rewrite (R2-08 through R2-17) so future eBay template changes are caught
quickly.

Run with: pytest returns/tests/
"""
from datetime import datetime
import sys
import os
import pathlib

# Allow running with `python -m pytest` from the repo root.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from returns.ebay_return_parser import EbayReturnParser  # noqa: E402


def _build(subject, body, message_id, date_header):
    return {
        "subject": subject,
        "from": "ebay@ebay.com",
        "body": body,
        "message_id": message_id,
        "date": date_header,
    }


def setup_module(module):
    module.parser = EbayReturnParser()


def test_refund_initiated():
    """Email: 'Return <id>: Refund initiated' - subject + body refund pattern."""
    out = parser.parse(_build(
        "Return 5319314244: Refund initiated",
        ("<h1>Thank you for initiating a refund to braxtonbaileigh.</h1>"
         "<p>A $15.99 refund for this item to braxtonbaileigh has been initiated</p>"),
        "msg1",
        "Tue, 11 May 2026 07:05:00 -0400",
    ))
    assert out["event_type"] == "refund_issued"
    assert out["return_id"] == "5319314244"
    assert out["buyer_username"] == "braxtonbaileigh"
    assert out["request_amount"] == 15.99


def test_return_approved():
    out = parser.parse(_build(
        "Return 5319300742: Return approved",
        ("<h1>The buyer justme77ellen is returning this item</h1>"
         "<p>The buyer justme77ellen is returning the item. Per eBay's policy, "
         "this return has been automatically approved.</p>"),
        "msg2",
        "Tue, 11 May 2026 01:44:00 -0400",
    ))
    assert out["event_type"] == "return_opened"
    assert out["return_id"] == "5319300742"
    assert out["buyer_username"] == "justme77ellen"


def test_issue_refund_reminder_is_unknown():
    """Reminder emails are not state changes - must NOT advance status."""
    out = parser.parse(_build(
        "Return 5318140997: Issue refund",
        "<h1>Issue a refund by May 12</h1>"
        "<p>We noticed that you haven't refunded the buyer yet.</p>",
        "msg3",
        "Tue, 11 May 2026 03:20:00 -0400",
    ))
    assert out["event_type"] == "unknown"
    assert out["return_id"] == "5318140997"


def test_buyer_shipped():
    out = parser.parse(_build(
        "Return 5319174693: Buyer shipped item",
        ("<h1>Refund your buyer when the item is delivered</h1>"
         "<p>readersmith has started shipping your item back to you.</p>"),
        "msg4",
        "Sat, 09 May 2026 17:53:00 -0400",
    ))
    assert out["event_type"] == "buyer_shipped"
    assert out["return_id"] == "5319174693"
    assert out["buyer_username"] == "readersmith"


def test_return_closed_generic():
    out = parser.parse(_build(
        "Return 5316164436: Return closed",
        ("<h1>This return is closed</h1>"
         "<p>The return request for the item has been closed.</p>"),
        "msg5",
        "Tue, 14 Apr 2026 10:29:00 -0400",
    ))
    assert out["event_type"] == "closed_other"
    assert out["return_id"] == "5316164436"


def test_customer_support_no_ship():
    """No return ID in subject; matches must work via order_number alone."""
    out = parser.parse(_build(
        "eBay Customer Support made a decision",
        ("<h1>eBay Customer Support made a decision</h1>"
         "<p>We reviewed this case and have closed it without any refund.</p>"
         "<p>The buyer did not return the item to you within the required timeframe.</p>"
         "<p>Order number: 16-14357-82449</p>"),
        "msg6",
        "Wed, 08 Apr 2026 16:10:00 -0400",
    ))
    assert out["event_type"] == "closed_no_ship"
    assert out["order_number"] == "16-14357-82449"
    assert out.get("return_id") is None


def test_marketing_email_rejected():
    """Bare 'return' isn't enough to enter the pipeline (R2-10)."""
    out = parser.parse(_build(
        "Free returns on all orders!",
        "<p>Lots of marketing copy. Word 'return' appears here.</p>",
        "msg-marketing",
        "Mon, 04 May 2026 12:00:00 +0000",
    ))
    assert out is None


def test_greedy_id_fallback_removed():
    """Plain 'Tracking ID: <digits>' must not be captured as return_id (R2-08)."""
    out = parser.parse(_build(
        "Return 5319300742: Return approved",
        ("<p>Tracking ID: 1234567890</p>"
         "<p>The buyer alice is returning the item.</p>"),
        "msg-tracking",
        "Mon, 04 May 2026 12:00:00 +0000",
    ))
    assert out["return_id"] == "5319300742"  # from subject, not the tracking
