"""Listing, ListingUnit, ListingTemplate, Channel — marketplace listing graph."""
import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Text, TIMESTAMP, ForeignKey,
    JSON, CheckConstraint, ARRAY,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.backend.db.database import Base


class Channel(Base):
    __tablename__ = 'channels'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), unique=True, nullable=False)
    display_name = Column(String(200), nullable=False)
    is_active = Column(Boolean, default=True)
    api_credentials = Column(JSON)
    settings = Column(JSON)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    listings = relationship("Listing", back_populates="channel", cascade="all, delete-orphan")
    listing_templates = relationship("ListingTemplate", back_populates="source_channel")
    sync_logs = relationship("SyncLog", back_populates="channel", cascade="all, delete-orphan")


class Listing(Base):
    __tablename__ = 'listings'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_id = Column(UUID(as_uuid=True), ForeignKey('products.id', ondelete='CASCADE'), nullable=False)
    channel_id = Column(UUID(as_uuid=True), ForeignKey('channels.id', ondelete='CASCADE'), nullable=False)
    channel_listing_id = Column(String(200))
    title = Column(Text)
    description = Column(Text)
    current_price = Column(Float)
    listing_url = Column(Text)
    status = Column(String(50), default='active')
    mode = Column(String(50), default='single_quantity')
    photos = Column(JSON)
    item_specifics = Column(JSON)

    sold_at = Column(TIMESTAMP)
    sold_price = Column(Float)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    ended_at = Column(TIMESTAMP)

    __table_args__ = (
        CheckConstraint(
            status.in_(['active', 'sold', 'ended', 'draft']),
            name='check_listing_status'
        ),
        CheckConstraint(
            mode.in_(['single_quantity', 'multi_quantity']),
            name='check_listing_mode'
        ),
    )

    product = relationship("Product", back_populates="listings")
    channel = relationship("Channel", back_populates="listings")
    listing_units = relationship("ListingUnit", back_populates="listing", cascade="all, delete-orphan")


class ListingUnit(Base):
    __tablename__ = 'listing_units'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    listing_id = Column(UUID(as_uuid=True), ForeignKey('listings.id', ondelete='CASCADE'), nullable=False)
    unit_id = Column(UUID(as_uuid=True), ForeignKey('units.id', ondelete='CASCADE'), nullable=False)
    matched_at = Column(TIMESTAMP, default=datetime.utcnow)

    listing = relationship("Listing", back_populates="listing_units")
    unit = relationship("Unit", back_populates="listing_units")


class ListingTemplate(Base):
    __tablename__ = 'listing_templates'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_id = Column(UUID(as_uuid=True), ForeignKey('products.id', ondelete='CASCADE'), nullable=False)
    source_channel_id = Column(UUID(as_uuid=True), ForeignKey('channels.id', ondelete='SET NULL'))
    title = Column(Text, nullable=False)
    description = Column(Text)
    photos = Column(JSON)
    item_specifics = Column(JSON)
    base_price = Column(Float)

    photo_metadata = Column(JSON, default={})
    pricing = Column(JSON, default={})
    category_mappings = Column(JSON, default={})
    seo_keywords = Column(ARRAY(String))
    template_version = Column(Integer, default=2)
    is_validated = Column(Boolean, default=False)
    validation_errors = Column(JSON)
    last_synced_at = Column(TIMESTAMP)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    product = relationship("Product", back_populates="listing_templates")
    source_channel = relationship("Channel", back_populates="listing_templates")
