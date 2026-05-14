"""
Persistent eBay OAuth token store with Fernet-encrypted refresh tokens
and lazy refresh-on-read of the short-lived access token.

Public surface:
  * `get_valid_access_token()`  — returns a non-expired access token, auto-refreshing if needed
  * `save_tokens(...)`          — upserts a (access, refresh, expiries, scope) tuple after the OAuth callback

Internal: the table has at most one row (the active credential).  We don't
support multi-account; if eBay reauth happens, the row is updated in place.
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

from src.backend.db.database import SessionLocal
from src.backend.db.models.ebay_oauth_token import EbayOAuthToken
from src.integrations.ebay import ebay_oauth

logger = logging.getLogger(__name__)

# Refresh the access token if it's expiring within this window
REFRESH_LEAD_TIME = timedelta(minutes=5)


def _fernet() -> Fernet:
    key = os.getenv("EBAY_TOKEN_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError(
            "EBAY_TOKEN_ENCRYPTION_KEY is not set. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    return Fernet(key.encode("ascii") if isinstance(key, str) else key)


def _encrypt(plaintext: str) -> str:
    return _fernet().encrypt(plaintext.encode("utf-8")).decode("ascii")


def _decrypt(ciphertext: str) -> str:
    try:
        return _fernet().decrypt(ciphertext.encode("ascii")).decode("utf-8")
    except InvalidToken as e:
        raise RuntimeError(
            "Failed to decrypt the stored eBay refresh token. "
            "Did the EBAY_TOKEN_ENCRYPTION_KEY change?  Re-run the OAuth consent flow."
        ) from e


# ----- Public API ------------------------------------------------------------

def save_tokens(
    *,
    access_token: str,
    refresh_token: str,
    access_expires_at: datetime,
    refresh_expires_at: datetime,
    scope: Optional[str] = None,
) -> None:
    """Upsert the single token row.  Encrypts refresh_token on the way in."""
    encrypted_refresh = _encrypt(refresh_token)
    db = SessionLocal()
    try:
        row = db.query(EbayOAuthToken).order_by(EbayOAuthToken.updated_at.desc()).first()
        if row is None:
            row = EbayOAuthToken(
                access_token=access_token,
                refresh_token=encrypted_refresh,
                access_expires_at=access_expires_at,
                refresh_expires_at=refresh_expires_at,
                scope=scope,
            )
            db.add(row)
        else:
            row.access_token = access_token
            row.refresh_token = encrypted_refresh
            row.access_expires_at = access_expires_at
            row.refresh_expires_at = refresh_expires_at
            row.scope = scope
            row.updated_at = datetime.utcnow()
        db.commit()
    finally:
        db.close()


def _get_row(db) -> Optional[EbayOAuthToken]:
    return db.query(EbayOAuthToken).order_by(EbayOAuthToken.updated_at.desc()).first()


def has_tokens() -> bool:
    db = SessionLocal()
    try:
        return _get_row(db) is not None
    finally:
        db.close()


def get_valid_access_token() -> str:
    """
    Return a non-expired access token, calling the eBay refresh endpoint if
    the stored access token is within REFRESH_LEAD_TIME of expiry.
    Raises RuntimeError if no tokens are stored yet.
    """
    db = SessionLocal()
    try:
        row = _get_row(db)
        if row is None:
            raise RuntimeError(
                "No eBay OAuth tokens stored. "
                "Visit /oauth/ebay/start to grant consent."
            )
        if row.access_expires_at - datetime.utcnow() > REFRESH_LEAD_TIME:
            return row.access_token

        # Refresh path
        logger.info("eBay access token within %s of expiry — refreshing", REFRESH_LEAD_TIME)
        refresh_plaintext = _decrypt(row.refresh_token)
        new_access, new_expires, new_scope = ebay_oauth.refresh_access_token(refresh_plaintext)
        row.access_token = new_access
        row.access_expires_at = new_expires
        if new_scope:
            row.scope = new_scope
        row.updated_at = datetime.utcnow()
        db.commit()
        return new_access
    finally:
        db.close()
