# Returns Tracking — Patch Set

These changes implement the V1 spec corrections from `Solea_Returns_Dev_Spec.docx`.
Each item below references the ticket ID in that document.

## Database / migrations
- **R1-01 / R1-02 / R1-03** — new `migrations/01_add_external_order_number.sql`:
  - Adds `units.external_order_number` (+ index) so returns can be matched to
    the unit that was sold under that eBay order number.
  - Adds a partial unique index `uq_return_events_msg_event` on
    `return_events(email_message_id, event_type)` as a backstop against
    duplicate event rows.
  - Creates view `vw_brand_return_rates` that reports `sold_count`,
    `return_count`, `no_ship_count`, and `return_rate_percent` per brand
    by joining sold eBay units to matched returns.
- `database.py` — `Unit.external_order_number = Column(String(100), index=True)`.

## Sale flow (R1-01 wiring)
- `delisting/delist_service.py` — `_update_unit_sold` now writes
  `unit.external_order_number` from the sale email's `order_id` (or
  `order_number`) field. This is what makes future returns matchable
  by order number.

## Returns service (`returns/return_service.py`) — full rewrite
- **R1-01** new `_match_by_order_number`; matcher tries SKU → listing → order.
- **R1-03** `_create_return_event` dedupes by (`email_message_id`, `event_type`)
  before inserting.
- **R1-04** new `_pick_event_timestamp`: uses parsed body date, then the email's
  Date header (`email_received_at`), then `utcnow()` only as a last resort.
- **R1-05** the 30-day same-buyer fallback merge is gone. `_find_existing_return`
  uses `return_id` then `order_number + buyer_username` only. Unmatched cases
  get a fresh record with a populated `notes` column.
- **R1-06** the service no longer commits internally. It flushes; the caller
  owns the transaction so the return row, the event row, and the
  `email_processing_log` row commit (or roll back) together.
- **R1-07** `_STATUS_ORDER` table; updates only advance status. Late events
  are still recorded in `return_events` for audit but never demote
  `status_current`. A note is appended when an event is suppressed.
- **R2-12** brand list curated and multi-word brands matched first. No
  "first word of title" fallback.
- **R2-16** unknown event types no longer change status; only events with a
  recognized `event_type` advance `status_current` / `final_outcome`.
- **R2-17** `tracking_number` is only set when the existing value is null.
  Mismatches append a dated line to `notes` instead of overwriting.
- **R4-24** `notes` column is now actively used (unmatched reasons, suppressed
  events, tracking mismatches).
- **R4-26** parameter `return_id` in `_create_return_event` renamed to
  `return_uuid` to disambiguate from the eBay-issued return ID.

## Parser (`returns/ebay_return_parser.py`) — full rewrite
- **R2-08** generic `r'id[:\s]+(\d{10,})'` fallback removed; only patterns
  with `return`/`case`/`request` context match a return ID.
- **R2-09** buyer-username extraction scoped to eBay subject templates and
  to body windows near "buyer information" / "from the buyer" / "buyer name".
  Stop-words `ebay`, `the`, etc. are rejected.
- **R2-10** the bare `return` keyword is no longer enough; `_is_return_email`
  requires a specific phrase like `return request`, `buyer shipped your return`,
  `refund sent`, `buyer did not ship`, …
- **R2-11** `_html_to_text` strips HTML (via BeautifulSoup, including
  `<script>`/`<style>`) before any regex runs.
- **R2-13** `raw_body` stored in full (the 5000-char truncation is gone).
- **R2-14** `_extract_amount` requires refund/request context. Bare
  `total: $X` no longer wins.
- **R2-15** `_extract_date_near` tightens the proximity window to 25 chars,
  sanity-bounds parsed dates (≥ 2020, no more than 1 day in the future),
  and exposes `email_received_at` parsed from the Gmail Date header.
- **R2-16** unknown event types return `'unknown'` (not `'return_opened'`).
- **R4-25** dead `_extract_item_title` method removed.

## Email-processing log (`returns/email_processing_service.py`)
- **R1-06** `mark_email_processed` now flushes only; the caller commits.
- `is_email_processed` checks `processing_status='success'` so a previous
  failure does not block a later successful retry.

## Scheduler (`scheduler.py`)
- **R1-06** `check_return_emails` rewritten as a single-transaction loop:
  each email is committed in its own transaction; any failure rolls back
  just that email and the log row.
- **R3-21** `start_return_monitoring` now uses `CronTrigger(hour=…, minute=0)`
  driven by env var `RETURN_CHECK_HOUR` (default `3`). The old
  `IntervalTrigger(minutes=1440)` and `RETURN_CHECK_INTERVAL_HOURS` are
  gone (the env var is now deprecated; see `.env.example`).

## Gmail service (`delisting/gmail_service.py`)
- **R3-18** tokens are persisted as JSON (`Credentials.to_json` /
  `Credentials.from_authorized_user_info`). On first run, a legacy
  `gmail_token.pickle` is migrated to `gmail_token.json` automatically and
  the pickle is deleted. File mode is `0600`.
- **R3-22** `get_emails_from_label` now paginates via `nextPageToken`. A
  safety cap (default 1000) prevents runaway loops. Each page is logged.

## Flask config
- **R3-19** `app.py` and `ui.py` both raise at startup if `SECRET_KEY` is
  unset. The `dev-secret-key` fallback is gone.

## docker-compose
- **R3-20**
  - `pgadmin` moved behind a `pgadmin` profile, so it does not start with
    a default `docker-compose up`. To bring it up:
    `docker compose --profile pgadmin up -d`.
  - `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` no longer have
    `:default` fallbacks; compose refuses to start without them.
  - Both `postgres` and `pgadmin` are bound to `127.0.0.1` only.
  - `DB_PASSWORD` is `:?` required.
  - The new returns migration is mounted into `docker-entrypoint-initdb.d`.

## .env.example
- New `RETURN_CHECK_HOUR=3` (0-23) replaces the deprecated
  `RETURN_CHECK_INTERVAL_HOURS`.
- `GMAIL_TOKEN_PATH` default changed from `.pickle` to `.json`.
- `SECRET_KEY`, `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` are
  now blank-required (no placeholder defaults).

## API (`app.py`)
- **R1-02** `/api/returns/by-brand` now reports `sold_count`,
  `return_count`, `return_rate_percent`, plus per-outcome counts. Brands
  with sales but no returns appear (rate is `0`); brands with returns
  but no sales appear (rate is `null` rather than divide-by-zero).
  Backwards-compat keys `total_returns` and `percent_closed_buyer_never_shipped`
  remain in each row so existing dashboards keep working.
- **R1-06** `/api/returns/check-emails` shares the new
  single-transaction loop (same as scheduler).
- **R3-19** module-level SECRET_KEY fail-fast.

## Smoke tests
A short Python smoke test was run after the rewrite. All passed:
1. Happy-path return-opened email: HTML body stripped, return_id, order
   number, buyer username, event_type, and `email_received_at` all
   extracted correctly.
2. Email containing `Tracking ID: 1234567890` no longer captures it as a
   return_id (R2-08).
3. A "Free returns!" marketing email is rejected by `_is_return_email`
   (R2-10).
4. An email without any recognized event phrase yields `event_type=='unknown'`
   (R2-16).
5. `_extract_known_brand` returns `Nike`/`New Balance` correctly and
   returns `None` for titles whose first word is `NEW` or `EUC` (R2-12).

---

## Update: refined against real eBay sample emails

After receiving 6 representative eBay return emails from the seller mailbox, the parser was tightened. All changes are in `returns/ebay_return_parser.py` and `returns/return_service.py`.

**Subject pattern `Return <id>: <event phrase>` is now the primary signal.** eBay does not put "Return ID:" labels in the body — the return ID lives in the subject. `_extract_return_id` now accepts a subject argument and matches `^Return\s+(\d{8,15})\s*:` first.

**Event detection uses subject suffix mapping** based on the observed templates:
- `Return <id>: Refund initiated` → `refund_issued`
- `Return <id>: Return approved` → `return_opened`
- `Return <id>: Buyer shipped item` → `buyer_shipped`
- `Return <id>: Return closed` → `closed_other` (generic close, no reason)
- `Return <id>: Issue refund` → `reminder` → reclassified as `unknown` so it never advances state
- `eBay Customer Support made a decision` + body "did not return the item" → `closed_no_ship`

**Buyer username patterns rewritten** to match real phrasings:
- `The buyer <username> is returning the item`
- `A $X refund for this item to <username> has been initiated`
- `<username> has started shipping your item back to you`
- `Thank you for initiating a refund to <username>`

**Request amount pattern adjusted** — `A $15.99 refund for this item` matches `\$([\d,]+\.\d{2})\s+refund` (dollar amount before the word "refund"). Labelled forms like `Refund amount: $X` are still supported.

**Status mapping** — `closed_other` event type added (`status_current='closed_other'`, `final_outcome='closed_other'`).

**New: `returns/tests/test_ebay_return_parser.py`** — 8 test cases covering all 6 observed templates plus the two negative-path checks (marketing email rejected; greedy ID fallback removed). All 8 pass.
