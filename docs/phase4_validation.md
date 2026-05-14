# Phase 4 — Second validation (read-only smoke, post-refactor)

**Date:** 2026-05-14
**Scope:** Codebase after Phase 1 (modularization), Phase 2 (Docker hardening), Phase 3 (eBay OAuth migration).  Pre-EPIC 1.
**Method:** Compile-check + import probe + grep audit + smoke pytest.  No Docker boot (Docker CLI not available on this host; deferred to deployment).

---

## ✅ Pass

| # | Check | Result |
|---|---|---|
| 1 | `python -m compileall -q src` | clean |
| 2 | `python -m pytest src/tests/test_smoke.py -q` | **33 of 33 imports pass** in ~1.5s |
| 3 | `grep -r "EBAY_AUTH_TOKEN" src/` | only a comment hit at [ebay_api.py:771](src/integrations/ebay/ebay_api.py#L771) — no live code uses it |
| 4 | `grep -r "'dev-secret-key'" src/` | only the comment in [config.py:26](src/backend/config.py#L26) describing why we removed it — no live code uses it |
| 5 | `grep -r "from database import\|from sync_service import\|from ebay_api import\|from crosslisting\|from delisting\|from returns" src/` | zero hits (all legacy module-paths rewritten to `src.*`) |
| 6 | Docker compose YAML parses cleanly (via PyYAML) | postgres has `expose: ['5432']` (no host port); pgadmin binds `127.0.0.1:5050`; app binds `127.0.0.1:9500` |
| 7 | OAuth blueprint wired | `src.backend.app.app.blueprints == ['oauth']`; URL `/oauth/ebay/start` is in url_map |
| 8 | `cryptography.fernet` importable | yes — added to requirements.txt at `cryptography>=42.0` |
| 9 | `pytest` importable | yes — added to requirements.txt at `pytest>=8.0` |
| 10 | All Mercari/Poshmark/eBay listers still import cleanly under their new paths | yes |

---

## 🚫 Not run in Phase 4 (deferred to deployment)

| Check | Reason |
|---|---|
| `docker compose config` | `docker` CLI not installed on this Windows host; YAML was structurally validated with PyYAML instead. |
| `docker compose up -d` + healthcheck transitions | Same. Verify on the deployment box: `postgres`, `pgadmin`, `app` should all reach `healthy`. |
| `curl http://127.0.0.1:9500/health` | Requires the Flask app to be running. Smoke import covers code-path; live verification happens on first `docker compose up`. |
| `nc -zv <LAN-IP> 5432 / 5050` (LAN reachability check) | Deferred to deployment.  Compose config binds Postgres to no host port and pgAdmin to loopback only — LAN reach should fail by construction. |
| `docker compose exec app whoami` returning `appuser` | Deferred. Dockerfile creates uid 1001 `appuser` and the final `USER appuser` directive switches to it. |
| Real OAuth flow (`/oauth/ebay/start` → consent → callback writes row) | Requires real `EBAY_APP_ID` / `EBAY_CERT_ID` / `EBAY_OAUTH_REDIRECT_URI` on a publicly-reachable host. Out of scope for this read-only validation. |
| Forced refresh (`UPDATE ebay_oauth_tokens SET access_expires_at = now() - interval '1 hour'`) | Same. The code path in [ebay_token_store.get_valid_access_token()](src/integrations/ebay/ebay_token_store.py) triggers refresh when `access_expires_at` is within 5 minutes — verify after first consent. |

---

## Outcome

**No blockers.** Static checks all green; live HTTP / DB / OAuth verification is deferred to first deployment.  Proceed to **Phase 5 — EPIC 1**.

## Smoke commands (copy-paste)

```powershell
# From the project root
python -m compileall -q src
$env:SECRET_KEY = "smoke-test-secret-key"
$env:EBAY_TOKEN_ENCRYPTION_KEY = "k1XO9HsXMQqOXMl6Bkj_lPS5UhEKKkLAY8sZE5SXAUM="
python -m pytest src/tests/test_smoke.py -q
```
