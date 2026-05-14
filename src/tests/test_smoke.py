"""
Smoke test — verifies every module in `src/` imports cleanly.

Catches stale imports, missing env-var contracts that are evaluated at import
time, and missing dependencies.  Does NOT exercise any runtime behavior.

Required env (set automatically by the fixture below):
  SECRET_KEY                  — fail-fast in src.backend.config
  EBAY_TOKEN_ENCRYPTION_KEY   — checked lazily by ebay_token_store; importing it
                                does not require a real key, but the test sets a
                                placeholder Fernet key so any incidental
                                construction won't blow up.
"""
import importlib
import os

import pytest

MODULES = [
    # backend
    "src.backend.config",
    "src.backend.app",
    "src.backend.db.database",
    "src.backend.db.models",
    "src.backend.routes.oauth",
    # services
    "src.services.sync_service",
    "src.services.template_service",
    "src.services.audit_service",
    "src.services.bulk_import_service",
    "src.services.ai_parser_service",
    "src.services.image_handler",
    "src.services.product_id_service",
    "src.services.product_registry_service",
    "src.services.intake_service",
    "src.services.crosslisting.crosslist_service",
    "src.services.delisting.delist_service",
    "src.services.delisting.email_parser_service",
    "src.services.delisting.gmail_service",
    "src.services.delisting.ebay_email_parser",
    "src.services.delisting.mercari_email_parser",
    "src.services.delisting.poshmark_email_parser",
    "src.services.delisting.reconciliation_service",
    "src.services.returns",
    "src.services.returns.return_service",
    "src.services.returns.ebay_return_parser",
    "src.services.returns.return_classifier",
    "src.services.returns.email_processing_service",
    # integrations
    "src.integrations.ebay.ebay_api",
    "src.integrations.ebay.ebay_delist",
    "src.integrations.ebay.ebay_oauth",
    "src.integrations.ebay.ebay_token_store",
    "src.integrations.mercari.mercari_lister",
    "src.integrations.poshmark.poshmark_lister",
    "src.integrations.selenium.selenium_delist",
    # jobs
    "src.jobs.scheduler",
    # frontend
    "src.frontend.ui",
]


@pytest.fixture(autouse=True, scope="session")
def _env():
    os.environ.setdefault("SECRET_KEY", "smoke-test-secret-key")
    # A valid Fernet key — only consulted if a token is actually decrypted, but
    # we set it so the cryptography import path can construct a Fernet instance
    # without raising during smoke.
    os.environ.setdefault(
        "EBAY_TOKEN_ENCRYPTION_KEY",
        "k1XO9HsXMQqOXMl6Bkj_lPS5UhEKKkLAY8sZE5SXAUM=",
    )
    yield


@pytest.mark.parametrize("modname", MODULES)
def test_module_imports(modname):
    """Every module in the project tree should import without error."""
    importlib.import_module(modname)
