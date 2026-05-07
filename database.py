"""
Database connection and SQLAlchemy models
"""
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, Text, TIMESTAMP, ForeignKey, JSON, CheckConstraint,ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
from urllib.parse import quote_plus

# Database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')

# If DATABASE_URL not set, try to build it from individual components
if not DATABASE_URL:
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'postgres')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')
    
    # URL-encode password if it contains special characters
    if db_password:
        db_password_encoded = quote_plus(db_password)
        DATABASE_URL = f'postgresql://{db_user}:{db_password_encoded}@{db_host}:{db_port}/{db_name}'
    else:
        DATABASE_URL = f'postgresql://{db_user}@{db_host}:{db_port}/{db_name}'


# Create engine with connection pooling settings optimized for Supabase
engine = create_engine(
    DATABASE_URL, 
    echo=False,
    pool_pre_ping=True,  # Test connections before using them
    pool_size=5,  # Number of connections to maintain
    max_overflow=10,  # Additional connections if needed
    pool_recycle=3600  # Recycle connections after 1 hour
)



# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

# ============================================
# MODELS
# ============================================

class Category(Base):
    __tablename__ = 'categories'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    internal_name = Column(String(100), unique=True, nullable=False)
    display_name = Column(String(200), nullable=False)
    ebay_category_id = Column(String(50))
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
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
    
    # Relationships
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
    
    # Relationships
    units = relationship("Unit", back_populates="location")

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
    
    # Relationships
    category = relationship("Category", back_populates="products")
    condition_grade = relationship("ConditionGrade", back_populates="products")
    units = relationship("Unit", back_populates="product", cascade="all, delete-orphan")
    listings = relationship("Listing", back_populates="product", cascade="all, delete-orphan")
    listing_templates = relationship("ListingTemplate", back_populates="product", cascade="all, delete-orphan")

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

    # ===== ADD THESE NEW FIELDS =====
    sold_at = Column(TIMESTAMP)
    sold_price = Column(Float)
    sold_platform = Column(String(50))
    # ================================


    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        CheckConstraint(
            status.in_(['ready_to_list', 'listed', 'sold', 'shipped', 'returned', 'damaged', 'reserved']),
            name='check_status'
        ),
    )
    
    # Relationships
    product = relationship("Product", back_populates="units")
    location = relationship("Location", back_populates="units")
    condition_grade = relationship("ConditionGrade", back_populates="units")
    listing_units = relationship("ListingUnit", back_populates="unit", cascade="all, delete-orphan")

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
    
    # Relationships
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

    # ===== ADD THESE NEW FIELDS =====
    sold_at = Column(TIMESTAMP)
    sold_price = Column(Float)
    # ================================

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
    
    # Relationships
    product = relationship("Product", back_populates="listings")
    channel = relationship("Channel", back_populates="listings")
    listing_units = relationship("ListingUnit", back_populates="listing", cascade="all, delete-orphan")

class ListingUnit(Base):
    __tablename__ = 'listing_units'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    listing_id = Column(UUID(as_uuid=True), ForeignKey('listings.id', ondelete='CASCADE'), nullable=False)
    unit_id = Column(UUID(as_uuid=True), ForeignKey('units.id', ondelete='CASCADE'), nullable=False)
    matched_at = Column(TIMESTAMP, default=datetime.utcnow)
    
    # Relationships
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

    # ===== ADD THESE NEW FIELDS =====
    photo_metadata = Column(JSON, default={})
    pricing = Column(JSON, default={})
    category_mappings = Column(JSON, default={})
    seo_keywords = Column(ARRAY(String))
    template_version = Column(Integer, default=2)
    is_validated = Column(Boolean, default=False)
    validation_errors = Column(JSON)
    last_synced_at = Column(TIMESTAMP)
    # ================================


    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    product = relationship("Product", back_populates="listing_templates")
    source_channel = relationship("Channel", back_populates="listing_templates")

class SyncLog(Base):
    __tablename__ = 'sync_logs'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    channel_id = Column(UUID(as_uuid=True), ForeignKey('channels.id', ondelete='CASCADE'))
    sync_type = Column(String(100))
    status = Column(String(50))
    records_processed = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    errors = Column(JSON)
    started_at = Column(TIMESTAMP, default=datetime.utcnow)
    completed_at = Column(TIMESTAMP)
    
    # Relationships
    channel = relationship("Channel", back_populates="sync_logs")

class Alert(Base):
    __tablename__ = 'alerts'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    alert_type = Column(String(100), nullable=False)
    severity = Column(String(20), default='info')
    title = Column(String(300), nullable=False)
    message = Column(Text)
    related_entity_type = Column(String(50))
    related_entity_id = Column(UUID(as_uuid=True))
    is_resolved = Column(Boolean, default=False)
    resolved_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    
    __table_args__ = (
        CheckConstraint(
            severity.in_(['info', 'warning', 'error', 'critical']),
            name='check_severity'
        ),
    )


# ============================================
# RETURNS TRACKING MODELS
# ============================================

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

    # Index for faster lookup by message ID
    __table_args__ = (
        {'extend_existing': True}
    )


# ============================================
# DATABASE SESSION HELPERS
# ============================================

def get_db():
    """
    Get database session
    Usage: 
        db = next(get_db())
        try:
            # use db
        finally:
            db.close()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """
    Initialize database - create all tables
    """
    Base.metadata.create_all(bind=engine)
    print("Database initialized successfully!")

if __name__ == "__main__":
    # Initialize database when run directly
    init_db()