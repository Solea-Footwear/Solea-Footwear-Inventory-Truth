"""
eBay OAuth consent + callback.

Routes:
  GET /oauth/ebay/start    -> 302 to eBay consent URL, with a CSRF `state` in the Flask session
  GET /oauth/ebay/callback -> consume code + state, persist tokens, render a simple success page
"""
import logging
import secrets

from flask import Blueprint, redirect, request, session

from src.integrations.ebay import ebay_oauth, ebay_token_store

logger = logging.getLogger(__name__)

oauth_bp = Blueprint("oauth", __name__, url_prefix="/oauth/ebay")

_SESSION_STATE_KEY = "ebay_oauth_state"


@oauth_bp.route("/start", methods=["GET"])
def ebay_oauth_start():
    state = secrets.token_urlsafe(32)
    session[_SESSION_STATE_KEY] = state
    return redirect(ebay_oauth.build_consent_url(state))


@oauth_bp.route("/callback", methods=["GET"])
def ebay_oauth_callback():
    code = request.args.get("code")
    state = request.args.get("state")
    expected_state = session.pop(_SESSION_STATE_KEY, None)

    if not code:
        return ("Missing 'code' query parameter", 400)
    if not state or state != expected_state:
        logger.warning("eBay OAuth state mismatch (got %r expected %r)", state, expected_state)
        return ("State mismatch — possible CSRF.  Retry from /oauth/ebay/start.", 400)

    try:
        access_token, refresh_token, access_expires_at, refresh_expires_at, scope = (
            ebay_oauth.exchange_code_for_tokens(code)
        )
    except Exception as e:  # noqa: BLE001 — surface to user with status, log details
        logger.exception("eBay token exchange failed")
        return (f"eBay token exchange failed: {e}", 502)

    ebay_token_store.save_tokens(
        access_token=access_token,
        refresh_token=refresh_token,
        access_expires_at=access_expires_at,
        refresh_expires_at=refresh_expires_at,
        scope=scope,
    )

    return (
        "<h1>eBay connected</h1>"
        "<p>Tokens saved.  You can close this tab.</p>",
        200,
    )
