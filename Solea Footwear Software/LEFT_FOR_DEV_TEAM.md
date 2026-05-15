# What's left for the dev team

The patch set in `CHANGES.md` lands every code change from the dev spec
(`Solea_Returns_Dev_Spec.docx`) - but a few things genuinely need a human
on your team to finish. They fall into four buckets.

## 1. Database migration (must run before deploying any of this code)
The new code expects:
- `units.external_order_number` (column + index)
- `uq_return_events_msg_event` (partial unique index)
- `vw_brand_return_rates` (view)

Apply the migration:
```bash
psql "$DATABASE_URL" -f migrations/01_add_external_order_number.sql
```
If you're using the Docker setup, the migration is mounted into
`docker-entrypoint-initdb.d` so a fresh `docker compose up` will run it
automatically for new databases. **Existing databases must be migrated
manually** with the command above.

Also: now is a reasonable time to backfill `units.external_order_number`
for historical sales. The eBay Post-Order API and the order ID already
stored in any existing `email_processing_log` rows are both candidates -
your judgement on which is easier.

## 2. Verify against real eBay emails (R2-09, R2-14, R2-15) — RESOLVED
This is now done. The parser was refined against 6 representative
eBay return emails from the seller's mailbox and `returns/tests/`
contains 8 test cases that all pass. Templates covered:

- `Return <id>: Refund initiated`
- `Return <id>: Return approved`
- `Return <id>: Buyer shipped item`
- `Return <id>: Return closed`
- `Return <id>: Issue refund` (treated as a non-state-changing reminder)
- `eBay Customer Support made a decision` (with body 'did not return
  the item' → closed_no_ship)

**If eBay adds a new template** (or you observe one we missed), drop
the email into `returns/tests/fixtures/` and add a corresponding test
case in `test_ebay_return_parser.py`.

## 3. Integration testing (we wrote no tests)
The patch ships pure-Python smoke checks but no unit-test suite. We
suggest:

```
returns/tests/
  test_ebay_return_parser.py    # one test per event type, one per Finding
  test_return_service.py        # mock SQLAlchemy session; verify status
                                # ordering (R1-07), dedup (R1-03), matching
                                # priority (R1-01).
  fixtures/
    return_opened.eml
    buyer_shipped.eml
    item_delivered_back.eml
    refund_issued.eml
    closed_no_ship.eml
```

The acceptance criteria in the dev spec were written to be test-shaped -
each one can become a test case directly.

## 4. End-to-end smoke run before turning the daily job back on
- Run the migration on staging.
- Set `RETURN_CHECK_HOUR` and `SECRET_KEY` in staging `.env`.
- Run `POST /api/returns/check-emails` manually.
- Verify:
  - `returns` rows have `external_order_number`-based matches when the
    sale was processed under that order number.
  - `return_events` has one row per (message_id, event_type).
  - `event_timestamp` matches the email's Date header (not the test run
    time).
  - `/api/returns/by-brand?start_date=…&end_date=…` returns
    `return_rate_percent` per brand.
  - Re-running the same call does not produce duplicate events or
    duplicate `returns` rows (R1-03, R1-05, R1-06 working together).

## 5. Items that need a product decision before they go further

- **Manual-merge UI** for returns. R1-05 removes the fuzzy 30-day
  buyer-only merge, so returns that previously would have been silently
  merged now stay separate and get `notes='Unmatched: …'`. Evan
  may want a small "merge these two returns" tool in the dashboard so
  he can fold them manually when appropriate. Out of scope for this PR.
- **Brand list extension** in `returns/return_service.py` (`_KNOWN_BRANDS`).
  The list is curated and intentionally small. Once Evan shares his full
  brand catalogue, add the rest in one commit.
- **Backfilling `units.external_order_number`** for historical sales
  (mentioned above) - depends on whether you want to walk the eBay API
  or replay existing email_processing_log payloads.
- **Returns dashboard (`returns_dashboard.html`)** - the new
  `/api/returns/by-brand` response shape includes new fields
  (`sold_count`, `return_count`, `return_rate_percent`). Backwards-
  compat keys are still present so the dashboard won't break, but you
  may want to add visible columns for the new ones.

## 6. Out-of-scope (intentionally not touched)
- The delisting pipeline beyond R1-01 wiring.
- Cross-listing to Poshmark and Mercari.
- The audit / sync / template services.
- The Selenium delist code.
- HTML/CSS/JS inside `returns_dashboard.html` (no behaviour changes were
  needed for the API to keep working).
