# Phase 1 — Modularization log

**Date:** 2026-05-14
**Outcome:** `src/` tree built, every Python module moved, all imports rewritten to `src.*` paths, every module compiles + imports.

## What landed

```
src/
  backend/
    app.py                       # was ./app.py (3,048 lines, monolithic for now)
    config.py                    # NEW — central env loading, fail-fast SECRET_KEY
    routes/                      # NEW (empty) — reserved for per-resource Blueprint split
    db/
      database.py                # was ./database.py (gutted to infrastructure: engine, Base, get_db, init_db; re-exports models)
      models/
        __init__.py              # re-exports every ORM class
        location.py              # Location, Category, ConditionGrade
        product.py               # Product
        unit.py                  # Unit
        listing.py               # Channel, Listing, ListingUnit, ListingTemplate
        sync_log.py              # SyncLog, Alert
        returns.py               # Return, ReturnEvent, EmailProcessingLog
  services/
    sync_service.py, template_service.py, audit_service.py,
    bulk_import_service.py, ai_parser_service.py, image_handler.py
    crosslisting/, delisting/, returns/        # whole packages moved
  integrations/
    ebay/   ebay_api.py, ebay_delist.py        # OAuth files (Phase 3) not yet created
    mercari/ mercari_lister.py
    poshmark/ poshmark_lister.py
    selenium/ selenium_delist.py
  jobs/
    scheduler.py, run_poshmark_ready_queue.py, run_template_refresh.py
  frontend/
    ui.py, static/returns_dashboard.html
  tests/
    legacy/  delisting_test.py, t.py, create_pickle_token.py    # (renamed from create_picke_token.py)
```

Root retains: `Dockerfile`, `docker-compose.yml`, `requirements.txt`, `schema.sql` + `*.sql`, `.env*`, all `*.md` docs.

## Acceptance — Phase 1 plan checklist

| Check | Status |
|---|---|
| `git mv` used for every file (blame survives) | ✅ |
| Every package has `__init__.py` | ✅ |
| `python -m compileall -q src` clean | ✅ |
| `from src.backend.db.database import Product, …` works (re-exports added) | ✅ |
| `python -c "import src.backend.app"` succeeds | ✅ |
| Every modularized module (services, integrations, jobs, frontend) imports cleanly | ✅ |
| `src/backend/config.py` created with fail-fast `SECRET_KEY` | ✅ |
| Vestigial root `__init__.py` (stray docstring file) deleted | ✅ |
| `create_picke_token.py` renamed → `create_pickle_token.py` and moved | ✅ |

## Deferred from Phase 1 (intentional)

**1e — Per-resource Blueprint split of [src/backend/app.py](src/backend/app.py).**
The plan called for splitting `app.py` into one Blueprint per resource under `src/backend/routes/`. `app.py` is 3,048 lines with 70+ route handlers; the split is a pure restructure with no functional change. Deferring as a follow-up so EPIC 1 + Docker hardening + OAuth migration can land first.

Plan stays intact: `routes/` already exists as an empty package; future PRs can extract one resource at a time (start with `health.py`, `dashboard.py`, `oauth.py` — the latter lands in Phase 3 anyway). Each extraction is local and reviewable on its own.

App still boots via `python -m src.backend.app` exactly as before; URL surface is identical to the legacy `app.py`.

**Inline imports inside `app.py` / `ui.py`.**
[Phase 0 F6](phase0_validation.md#f6--inline-imports-inside-apppy--uipy): selenium, `CrosslistService`, and `returns.*` imports at line ~1876+, ~2092, ~2282 are still inline. They'll move to top of file when those route groups get extracted into Blueprints.

**`ui.py` / `app.py` deduplication.**
[Phase 0 F5](phase0_validation.md#f5--apppy--uipy-near-duplicate): `ui.py` (2,375 lines) overlaps significantly with `app.py`. Reconciling the two is part of the same follow-up that does the Blueprint split.

## Notes on `src/backend/db/database.py`

The split keeps `Base`, `engine`, `SessionLocal`, `get_db`, `init_db` as the infrastructure. Models live in `src/backend/db/models/*.py`. To preserve existing call sites like `from database import Product, Unit, ...`, the new `database.py` re-exports every model class from its bottom — see [database.py](src/backend/db/database.py#L67-L74). That import sequence is safe (the import happens at module bottom, after `Base` is defined, and models pull `Base` from this module).

## Smoke check command

```powershell
$env:SECRET_KEY = "smoke-test-key"
python -m compileall -q src
python -c "import src.backend.app; print('ok')"
```

Both pass.
