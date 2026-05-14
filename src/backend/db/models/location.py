"""Location, Category, ConditionGrade — reference tables for inventory."""
import uuid
from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.backend.db.database import Base


class Category(Base):
    __tablename__ = 'categories'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    internal_name = Column(String(100), unique=True, nullable=False)
    display_name = Column(String(200), nullable=False)
    ebay_category_id = Column(String(50))
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    products = relationship("Product", back_populates="category")


class ConditionGrade(Base):
    __tablename__ = 'condition_grades'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    internal_code = Column(String(50), unique=True, nullable=False)
    display_name = Column(String(100), nullable=False)
    ebay_condition_id = Column(Integer)
    ebay_condition_name = Column(String(100))
    ebay_condition_note_template = Column(Text)
    sort_order = Column(Integer, default=0)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    products = relationship("Product", back_populates="condition_grade")
    units = relationship("Unit", back_populates="condition_grade")


class Location(Base):
    __tablename__ = 'locations'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    units = relationship("Unit", back_populates="location")
