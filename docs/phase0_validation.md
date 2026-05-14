# Phase 0 — First validation (read-only smoke)

**Date:** 2026-05-14
**Scope:** Codebase AS-IS (pre-modularization, pre-OAuth migration, pre-EPIC 1)
**Method:** Compile-check + import probe + env-var/import-graph audit. No source edits. No Docker boot.

---

## ✅ Pass

| Check | Result |
|---|---|
| `python -m compileall -q .` (whole repo) | Clean. All `.py` files are syntactically valid. |
| `python -c "import database"` | Imports clean. |
| `python -c "import app"` | Imports clean (stderr: `eBay API credentials not fully configured` — expected logger.warning, not a failure). |
| Python interpreter | `Python 3.14.4` on Windows. |

---

## ⚠️ Findings (non-blocking, scheduled for later phases)

### F1 — `ANTHROPIC_API_KEY` referenced but not in `.env.example`
- **Where:** [ai_parser_service.py:21](ai_parser_service.py#L21), [delisting/email_parser_service.py:29](delisting/email_parser_service.py#L29)
- **Impact:** Fresh setup from `.env.example` won't include this key; first call to AI parser fails silently or with a confusing 401.
- **Fix in:** Phase 3 (`.env.example` update during OAuth phase).

### F2 — `API_HOST=0.0.0.0` in `.env.example`
- **Where:** [.env.example:43](.env.example#L43)
- **Impact:** Default binds Flask to all interfaces, including LAN. Local dev should bind to `127.0.0.1`. Production should rely on the Docker network only.
- **Fix in:** Phase 2 (Docker hardening — app listens internal-only; compose maps `127.0.0.1:9500:9500`).

### F3 — pgAdmin literal `admin/admin` credentials
- **Where:** [docker-compose.yml:25-26](docker-compose.yml#L25-L26), [.env.example:50-51](.env.example#L50-L51)
- **Impact:** Anyone reaching pgAdmin on the host network can read DB credentials.
- **Fix in:** Phase 2.

### F4 — `SECRET_KEY` fallback to `'dev-secret-key'`
- **Where:** [app.py:48](app.py#L48), [ui.py:48](ui.py#L48)
- **Impact:** Silent insecure default if env var missing in prod.
- **Fix in:** Phase 1 (`src/backend/config.py` fails fast on missing `SECRET_KEY`).

### F5 — `app.py` ↔ `ui.py` near-duplicate
- `app.py`: 3,048 lines. `ui.py`: 2,375 lines. Diff: 2,267 lines.
- They share identical top-level imports (`scheduler`, `template_service`, `audit_service`, `bulk_import_service`, `database`, `ebay_api`, `sync_service`, all three `delisting.*` modules). Both call `Flask(__name__)` and bind `SECRET_KEY`.
- **Impact:** Forked / partially-duplicated route handlers — risk of behavior divergence.
- **Decision needed in Phase 1:** Either (a) treat `ui.py` as the embedded frontend dashboard and route its surface through Blueprints separate from the JSON API in `app.py`, or (b) delete `ui.py` if its routes are all covered by `app.py`. Manual diff required.

### F6 — Inline imports inside `app.py` / `ui.py`
- [app.py:1876-1880](app.py#L1876-L1880) — Selenium imports inside a route handler.
- [app.py:2092](app.py#L2092) — `from crosslisting.crosslist_service import CrosslistService` inside a handler.
- [app.py:2282-2284](app.py#L2282-L2284) — `returns.*` imports inside handlers.
- [ui.py:1882](ui.py#L1882) — same Selenium pattern.
- **Impact:** Hides true dependency graph; defers ImportError until runtime.
- **Fix in:** Phase 1 (during Blueprint split, hoist all imports to top of each `routes/*.py`).

### F7 — Misspelled filename `create_picke_token.py` (should be `create_pickle_token.py`)
- 309 bytes. One-shot Gmail OAuth token bootstrap. Calls `GmailService` once and exits.
- **Fix in:** Phase 1 — rename to `create_pickle_token.py` and relocate to `src/jobs/` (or delete if `gmail_service.py`'s built-in `_load_credentials` already covers the first-run flow).

### F8 — `t.py` debug script
- 69 bytes. Prints `SyncLog` table columns. Useful once; lives at repo root since.
- **Fix in:** Phase 1 — move to `src/tests/legacy/t.py`.

### F9 — `EBAY_AUTH_TOKEN` (Auth'n'Auth) is the active path
- [ebay_api.py:51](ebay_api.py#L51), [ebay_delist.py:31](ebay_delist.py#L31).
- `EBAY_OAUTH_TOKEN` env var exists ([ebay_api.py:52](ebay_api.py#L52)) but is only used at [ebay_api.py:762](ebay_api.py#L762) with a malformed `Authorization: IAF …` header (should be `Bearer …`).
- **Fix in:** Phase 3 (full OAuth migration with token store + refresh).

### F10 — Python 3.14 vs pinned deps in `requirements.txt`
- `psycopg2-binary==2.9.12` published in late 2024 — wheels for Python 3.14 may not be available, forcing source-build (requires Postgres dev headers on the host).
- `selenium==4.18.1`, `ebaysdk==2.2.0` should be OK on 3.14 but pinning to 2024-era versions is risky on a 2026-era interpreter.
- **Mitigation in Phase 2:** Docker image pins to `python:3.11.8-slim`, so the deployed app uses 3.11 with confirmed wheels. Local dev on 3.14 will work once a venv is built and any source-build failures are addressed individually.

### F11 — Mercari/Poshmark listers do not read env vars directly
- `grep -n "getenv\|environ\["` returns zero hits in `mercari_lister.py` and `poshmark_lister.py`.
- Credentials likely passed as method args or read from Selenium cookie jars; no env-var contract to break.

---

## 🚫 Not run in Phase 0

| Step | Reason |
|---|---|
| `docker compose up postgres` + `python database.py init_db()` | No Docker session active; deferred to Phase 4 second-validation when the stack is rebuilt. |
| `python app.py` + `curl /health` + `curl /api/dashboard` | Requires live DB + eBay tokens. Deferred to Phase 4. |
| `pip install -r requirements.txt` in a fresh venv | F10 risk on Python 3.14; the deployed image at `python:3.11.8-slim` is the authoritative environment. |

---

## Outcome

**No blockers.** Every finding above is captured in a downstream phase. Proceed to **Phase 1 — modularization**.
