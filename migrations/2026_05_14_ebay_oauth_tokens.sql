-- 2026-05-14 — eBay OAuth 2.0 token persistence (Phase 3)
-- Stores the user-OAuth refresh token (encrypted with Fernet via the app)
-- and the latest short-lived access token + expiry.  One row max.

CREATE TABLE IF NOT EXISTS ebay_oauth_tokens (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  access_token        text NOT NULL,
  refresh_token       text NOT NULL,             -- encrypted at rest (Fernet)
  access_expires_at   timestamp NOT NULL,
  refresh_expires_at  timestamp NOT NULL,
  scope               text,
  created_at          timestamp NOT NULL DEFAULT now(),
  updated_at          timestamp NOT NULL DEFAULT now()
);
