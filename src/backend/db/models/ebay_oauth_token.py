"""EbayOAuthToken — single-row table holding the active eBay OAuth tokens.

The `refresh_token` column is stored Fernet-encrypted; the application code in
`src.integrations.ebay.ebay_token_store` encrypts on write and decrypts on read.
"""
import uuid
from datetime import datetime

from sqlalchemy import Column, String, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID

from src.backend.db.database import Base


class EbayOAuthToken(Base):
    __tablename__ = 'ebay_oauth_tokens'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)            # Fernet-encrypted at rest
    access_expires_at = Column(TIMESTAMP, nullable=False)
    refresh_expires_at = Column(TIMESTAMP, nullable=False)
    scope = Column(Text)
    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
