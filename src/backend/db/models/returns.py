"""Return, ReturnEvent, EmailProcessingLog — returns-tracking models."""
import uuid
from datetime import datetime

from sqlalchemy import Column, String, Float, Text, TIMESTAMP, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID

from src.backend.db.database import Base


class Return(Base):
    __tablename__ = 'returns'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    marketplace = Column(String(50), default='eBay')
    return_id = Column(String(100))
    order_number = Column(String(100))
    buyer_username = Column(String(200))
    item_title = Column(Text)
    brand = Column(String(200))
    sku = Column(String(100))
    external_listing_id = Column(String(200))
    internal_order_id = Column(UUID(as_uuid=True), ForeignKey('units.id'))
    return_reason_ebay = Column(String(200))
    buyer_comment = Column(Text)
    request_amount = Column(Float)
    opened_at = Column(TIMESTAMP)
    buyer_ship_by_date = Column(TIMESTAMP)
    buyer_shipped_at = Column(TIMESTAMP)
    tracking_number = Column(String(200))
    item_delivered_back_at = Column(TIMESTAMP)
    refund_issued_at = Column(TIMESTAMP)
    closed_at = Column(TIMESTAMP)
    status_current = Column(String(50))
    final_outcome = Column(String(50))
    internal_bucket = Column(String(50))
    notes = Column(Text)
    recommended_fix = Column(Text)
    classifier_source = Column(String(50))
    classifier_confidence = Column(Float)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)


class ReturnEvent(Base):
    __tablename__ = 'return_events'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    return_id = Column(UUID(as_uuid=True), ForeignKey('returns.id', ondelete='CASCADE'), nullable=False)
    event_type = Column(String(100))
    event_timestamp = Column(TIMESTAMP)
    source_type = Column(String(50), default='email')  # email, page_scrape, manual
    email_message_id = Column(String(200))
    email_subject = Column(Text)
    raw_payload = Column(Text)
    parsed_data = Column(JSON)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)


class EmailProcessingLog(Base):
    __tablename__ = 'email_processing_log'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_message_id = Column(String(200), unique=True, nullable=False)  # Gmail's unique message ID
    email_subject = Column(Text)
    email_sender = Column(String(200))
    received_date = Column(TIMESTAMP)
    processed_at = Column(TIMESTAMP, default=datetime.utcnow)
    processing_status = Column(String(50), default='success')  # success, failed, skipped
    processing_notes = Column(Text)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    __table_args__ = (
        {'extend_existing': True},
    )
