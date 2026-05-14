"""Product — the catalog entity."""
import uuid
from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, Column, Index, String, Float, Text, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.backend.db.database import Base


class Product(Base):
    __tablename__ = 'products'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(200), nullable=False)
    model = Column(String(300), nullable=False)
    colorway = Column(String(200))
    size = Column(String(50), nullable=False)
    gender = Column(String(20))
    category_id = Column(UUID(as_uuid=True), ForeignKey('categories.id', ondelete='SET NULL'))
    condition_grade_id = Column(UUID(as_uuid=True), ForeignKey('condition_grades.id', ondelete='SET NULL'))
    default_price_ebay = Column(Float)
    sku_prefix = Column(String(50))
    notes = Column(Text)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # EPIC 1 — Product ID System (additive; nullable so existing rows are unaffected)
    product_id = Column(String(255), unique=True, nullable=True, index=True)
    style_code = Column(String(100), nullable=True)
    condition_code = Column(String(20), nullable=True)
    is_interchangeable = Column(Boolean, default=False, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "condition_code IN ('NEW','LIKE_NEW','EXCELLENT','GOOD','FAIR') OR condition_code IS NULL",
            name='check_condition_code',
        ),
    )

    category = relationship("Category", back_populates="products")
    condition_grade = relationship("ConditionGrade", back_populates="products")
    units = relationship("Unit", back_populates="product", cascade="all, delete-orphan")
    listings = relationship("Listing", back_populates="product", cascade="all, delete-orphan")
    listing_templates = relationship("ListingTemplate", back_populates="product", cascade="all, delete-orphan")
