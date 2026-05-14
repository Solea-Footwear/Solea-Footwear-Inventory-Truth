"""SyncLog and Alert — operational tables."""
import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Integer, Boolean, Text, TIMESTAMP, ForeignKey, JSON, CheckConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.backend.db.database import Base


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
