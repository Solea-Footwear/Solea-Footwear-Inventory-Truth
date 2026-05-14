"""
Database engine, session factory, and lifecycle helpers.

Models live in `src.backend.db.models.*`.  `init_db()` imports the models
package so that `Base.metadata` is fully populated before `create_all`.
"""
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ----- DATABASE_URL ----------------------------------------------------------

DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'postgres')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')

    if db_password:
        db_password_encoded = quote_plus(db_password)
        DATABASE_URL = f'postgresql://{db_user}:{db_password_encoded}@{db_host}:{db_port}/{db_name}'
    else:
        DATABASE_URL = f'postgresql://{db_user}@{db_host}:{db_port}/{db_name}'

# ----- Engine + Session ------------------------------------------------------

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


# ----- Lifecycle helpers -----------------------------------------------------

def get_db():
    """Yield a session for use with `next(get_db())` / `finally db.close()`."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create every table registered on Base.metadata."""
    from src.backend.db import models  # noqa: F401 — populates Base.metadata
    Base.metadata.create_all(bind=engine)
    print("Database initialized successfully!")


# ----- Re-exports ------------------------------------------------------------
# Existing code does `from src.backend.db.database import Product, Unit, ...`.
# Make that work by exposing every ORM class at this module's top level.
# Done at module-bottom so `Base` is defined before models import it.
from src.backend.db.models.location import Category, ConditionGrade, Location  # noqa: E402
from src.backend.db.models.product import Product  # noqa: E402
from src.backend.db.models.unit import Unit  # noqa: E402
from src.backend.db.models.listing import Channel, Listing, ListingUnit, ListingTemplate  # noqa: E402
from src.backend.db.models.sync_log import SyncLog, Alert  # noqa: E402
from src.backend.db.models.returns import Return, ReturnEvent, EmailProcessingLog  # noqa: E402
from src.backend.db.models.ebay_oauth_token import EbayOAuthToken  # noqa: E402


if __name__ == "__main__":
    init_db()
