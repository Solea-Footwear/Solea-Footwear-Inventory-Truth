"""
eBay OAuth 2.0 user-token client.

Three operations:
  * `build_consent_url(state)`            — URL to redirect the seller to for consent
  * `exchange_code_for_tokens(code)`      — authorization_code grant -> access + refresh tokens
  * `refresh_access_token(refresh_token)` — refresh_token grant -> new access token

The Trading API accepts the OAuth access token via the `iaf_token` parameter on
the ebaysdk Trading connection, so once the token store has a valid token the
rest of `ebay_api.py` is unchanged.

eBay endpoint reference:
  https://developer.ebay.com/api-docs/static/oauth-authorization-code-grant.html
"""
import base64
import logging
import os
from datetime import datetime, timedelta
from typing import Tuple
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

PRODUCTION_AUTH_URL = "https://auth.ebay.com/oauth2/authorize"
SANDBOX_AUTH_URL = "https://auth.sandbox.ebay.com/oauth2/authorize"
PRODUCTION_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SANDBOX_TOKEN_URL = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"


def _env():
    """Snapshot env at call time so changes in tests are visible."""
    return {
        "app_id": os.getenv("EBAY_APP_ID"),
        "cert_id": os.getenv("EBAY_CERT_ID"),
        "redirect_uri": os.getenv(
            "EBAY_OAUTH_REDIRECT_URI",
            "http://localhost:9500/oauth/ebay/callback",
        ),
        "scopes": os.getenv(
            "EBAY_OAUTH_SCOPES",
            "https://api.ebay.com/oauth/api_scope "
            "https://api.ebay.com/oauth/api_scope/sell.inventory "
            "https://api.ebay.com/oauth/api_scope/sell.account "
            "https://api.ebay.com/oauth/api_scope/sell.fulfillment",
        ),
        "environment": os.getenv("EBAY_ENVIRONMENT", "production"),
    }


def _basic_auth_header(app_id: str, cert_id: str) -> str:
    raw = f"{app_id}:{cert_id}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _token_url(environment: str) -> str:
    return SANDBOX_TOKEN_URL if environment == "sandbox" else PRODUCTION_TOKEN_URL


def _auth_url(environment: str) -> str:
    return SANDBOX_AUTH_URL if environment == "sandbox" else PRODUCTION_AUTH_URL


def build_consent_url(state: str) -> str:
    """
    Build the URL to send the seller to for consent.

    `state` is an opaque CSRF value that we'll verify on the callback.
    """
    env = _env()
    if not env["app_id"]:
        raise RuntimeError("EBAY_APP_ID is not set; cannot build consent URL")
    qs = urlencode({
        "client_id": env["app_id"],
        "response_type": "code",
        "redirect_uri": env["redirect_uri"],
        "scope": env["scopes"],
        "state": state,
    })
    return f"{_auth_url(env['environment'])}?{qs}"


def exchange_code_for_tokens(code: str) -> Tuple[str, str, datetime, datetime, str]:
    """
    Exchange an authorization code for (access_token, refresh_token, …).

    Returns: (access_token, refresh_token, access_expires_at, refresh_expires_at, scope)
    """
    env = _env()
    if not env["app_id"] or not env["cert_id"]:
        raise RuntimeError("EBAY_APP_ID and EBAY_CERT_ID must be set")
    resp = requests.post(
        _token_url(env["environment"]),
        headers={
            "Authorization": _basic_auth_header(env["app_id"], env["cert_id"]),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": env["redirect_uri"],
        },
        timeout=15,
    )
    if resp.status_code != 200:
        logger.error("eBay token exchange failed: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
    body = resp.json()
    now = datetime.utcnow()
    return (
        body["access_token"],
        body["refresh_token"],
        now + timedelta(seconds=int(body["expires_in"])),
        now + timedelta(seconds=int(body["refresh_token_expires_in"])),
        body.get("scope", env["scopes"]),
    )


def refresh_access_token(refresh_token: str) -> Tuple[str, datetime, str]:
    """
    Use a stored refresh_token to mint a fresh access_token.

    Returns: (access_token, access_expires_at, scope)
    """
    env = _env()
    if not env["app_id"] or not env["cert_id"]:
        raise RuntimeError("EBAY_APP_ID and EBAY_CERT_ID must be set")
    resp = requests.post(
        _token_url(env["environment"]),
        headers={
            "Authorization": _basic_auth_header(env["app_id"], env["cert_id"]),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": env["scopes"],
        },
        timeout=15,
    )
    if resp.status_code != 200:
        logger.error("eBay token refresh failed: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
    body = resp.json()
    return (
        body["access_token"],
        datetime.utcnow() + timedelta(seconds=int(body["expires_in"])),
        body.get("scope", env["scopes"]),
    )
