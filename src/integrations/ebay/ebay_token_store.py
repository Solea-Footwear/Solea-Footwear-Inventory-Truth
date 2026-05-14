"""
Persistent eBay OAuth token store with Fernet-encrypted refresh tokens
and lazy refresh-on-read of the short-lived access token (psycopg2).

Public surface:
  * get_valid_access_token()  — returns a non-expired access token, auto-refreshing if needed
  * save_tokens(...)          — upserts a (access, refresh, expiries, scope) tuple after OAuth callback
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras
from cryptography.fernet import Fernet, InvalidToken

from src.backend.db.database import _dsn
from src.integrations.ebay import ebay_oauth

logger = logging.getLogger(__name__)

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


def _connect():
    return psycopg2.connect(_dsn())


def save_tokens(
    *,
    access_token: str,
    refresh_token: str,
    access_expires_at: datetime,
    refresh_expires_at: datetime,
    scope: Optional[str] = None,
) -> None:
    """Upsert the single token row. Encrypts refresh_token on the way in."""
    encrypted_refresh = _encrypt(refresh_token)
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM ebay_oauth_tokens ORDER BY updated_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    INSERT INTO ebay_oauth_tokens
                        (id, access_token, refresh_token, access_expires_at,
                         refresh_expires_at, scope, created_at, updated_at)
                    VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, now(), now())
                    """,
                    [access_token, encrypted_refresh, access_expires_at,
                     refresh_expires_at, scope],
                )
            else:
                cur.execute(
                    """
                    UPDATE ebay_oauth_tokens
                    SET access_token=%s, refresh_token=%s, access_expires_at=%s,
                        refresh_expires_at=%s, scope=%s, updated_at=now()
                    WHERE id=%s
                    """,
                    [access_token, encrypted_refresh, access_expires_at,
                     refresh_expires_at, scope, row["id"]],
                )
        conn.commit()
    finally:
        conn.close()


def has_tokens() -> bool:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM ebay_oauth_tokens LIMIT 1")
            return cur.fetchone() is not None
    finally:
        conn.close()


def get_valid_access_token() -> str:
    """Return a non-expired access token, refreshing via eBay if within REFRESH_LEAD_TIME."""
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM ebay_oauth_tokens ORDER BY updated_at DESC LIMIT 1"
            )
            row = cur.fetchone()

        if row is None:
            raise RuntimeError(
                "No eBay OAuth tokens stored. Visit /oauth/ebay/start to grant consent."
            )

        if row["access_expires_at"] - datetime.utcnow() > REFRESH_LEAD_TIME:
            return row["access_token"]

        logger.info("eBay access token within %s of expiry — refreshing", REFRESH_LEAD_TIME)
        refresh_plaintext = _decrypt(row["refresh_token"])
        new_access, new_expires, new_scope = ebay_oauth.refresh_access_token(refresh_plaintext)

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ebay_oauth_tokens
                SET access_token=%s, access_expires_at=%s, scope=COALESCE(%s, scope), updated_at=now()
                WHERE id=%s
                """,
                [new_access, new_expires, new_scope, row["id"]],
            )
        conn.commit()
        return new_access
    finally:
        conn.close()
