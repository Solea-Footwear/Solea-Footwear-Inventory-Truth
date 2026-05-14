"""
Central env loading.

Imports `dotenv.load_dotenv()` once on import.  All env-var reads should
go through this module so missing required vars fail fast instead of
silently falling back to insecure defaults.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"Add it to .env or your deployment environment."
        )
    return val


# ----- Flask -----------------------------------------------------------------

# Fail fast — no 'dev-secret-key' fallback.  Set SECRET_KEY in .env.
SECRET_KEY = _required("SECRET_KEY")

API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "9500"))


# ----- Database --------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


# ----- eBay ------------------------------------------------------------------

EBAY_APP_ID = os.getenv("EBAY_APP_ID")
EBAY_CERT_ID = os.getenv("EBAY_CERT_ID")
EBAY_DEV_ID = os.getenv("EBAY_DEV_ID")
EBAY_OAUTH_REDIRECT_URI = os.getenv("EBAY_OAUTH_REDIRECT_URI", "http://localhost:9500/oauth/ebay/callback")
EBAY_OAUTH_SCOPES = os.getenv(
    "EBAY_OAUTH_SCOPES",
    "https://api.ebay.com/oauth/api_scope "
    "https://api.ebay.com/oauth/api_scope/sell.inventory "
    "https://api.ebay.com/oauth/api_scope/sell.account "
    "https://api.ebay.com/oauth/api_scope/sell.fulfillment",
)
EBAY_TOKEN_ENCRYPTION_KEY = os.getenv("EBAY_TOKEN_ENCRYPTION_KEY")
EBAY_ENVIRONMENT = os.getenv("EBAY_ENVIRONMENT", "production")
EBAY_RETURNS_GMAIL_LABEL = os.getenv("EBAY_RETURNS_GMAIL_LABEL", "EBAY_RETURNS_TRACKING")


# ----- Gmail -----------------------------------------------------------------

GMAIL_TOKEN_PATH = os.getenv("GMAIL_TOKEN_PATH", "./gmail_token.pickle")


# ----- AI parser -------------------------------------------------------------

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")


# ----- Scheduler -------------------------------------------------------------

AUTO_SYNC_ENABLED = os.getenv("AUTO_SYNC_ENABLED", "true").lower() == "true"
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "60"))
AUTO_DELIST_ENABLED = os.getenv("AUTO_DELIST_ENABLED", "true").lower() == "true"
AUTO_CROSSLIST_ENABLED = os.getenv("AUTO_CROSSLIST_ENABLED", "false").lower() == "true"
EMAIL_CHECK_INTERVAL_MINUTES = int(os.getenv("EMAIL_CHECK_INTERVAL_MINUTES", "3"))
CROSSLIST_CHECK_INTERVAL_MINUTES = int(os.getenv("CROSSLIST_CHECK_INTERVAL_MINUTES", "60"))
RETURN_CHECK_INTERVAL_HOURS = int(os.getenv("RETURN_CHECK_INTERVAL_HOURS", "24"))
RETURN_PROCESSING_ENABLED = os.getenv("RETURN_PROCESSING_ENABLED", "true").lower() == "true"
AUTO_SYNC_EBAY_LISTINGS = os.getenv("AUTO_SYNC_EBAY_LISTINGS", "true").lower() == "true"
AUTO_SYNC_SOLD_ITEMS = os.getenv("AUTO_SYNC_SOLD_ITEMS", "true").lower() == "true"


# ----- Selenium / Mercari / Poshmark ----------------------------------------

SELENIUM_HEADLESS = os.getenv("SELENIUM_HEADLESS", "true").lower() == "true"
