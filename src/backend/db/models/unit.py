"""Unit — a physical pair of shoes (one SKU = one Unit row)."""
import uuid
from datetime import datetime

from sqlalchemy import Column, String, Float, Text, TIMESTAMP, ForeignKey, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.backend.db.database import Base


class Unit(Base):
    __tablename__ = 'units'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    unit_code = Column(String(100), unique=True, nullable=False)
    product_id = Column(UUID(as_uuid=True), ForeignKey('products.id', ondelete='CASCADE'), nullable=False)
    location_id = Column(UUID(as_uuid=True), ForeignKey('locations.id', ondelete='SET NULL'))
    condition_grade_id = Column(UUID(as_uuid=True), ForeignKey('condition_grades.id', ondelete='SET NULL'))
    status = Column(String(50), default='ready_to_list')
    cost_basis = Column(Float)
    notes = Column(Text)

    sold_at = Column(TIMESTAMP)
    sold_price = Column(Float)
    sold_platform = Column(String(50))

    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        CheckConstraint(
            status.in_(['ready_to_list', 'listed', 'sold', 'shipped', 'returned', 'damaged', 'reserved']),
            name='check_status',
        ),
        CheckConstraint("unit_code != ''", name='check_unit_code_nonempty'),
    )

    product = relationship("Product", back_populates="units")
    location = relationship("Location", back_populates="units")
    condition_grade = relationship("ConditionGrade", back_populates="units")
    listing_units = relationship("ListingUnit", back_populates="unit", cascade="all, delete-orphan")
