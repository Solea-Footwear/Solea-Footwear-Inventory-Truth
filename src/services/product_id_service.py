"""EPIC 1 Ticket 1.2 — deterministic Product ID generator.

product_id format:
  Interchangeable (NEW / LIKE_NEW):   BRAND-MODEL-STYLECODE-GENDER-SIZE-CONDITION
  Non-interchangeable (EXCELLENT/GOOD/FAIR): ...same...-SKUTOKEN
"""
import re

CONDITION_NEW = "NEW"
CONDITION_LIKE_NEW = "LIKE_NEW"
CONDITION_EXCELLENT = "EXCELLENT"
CONDITION_GOOD = "GOOD"
CONDITION_FAIR = "FAIR"

VALID_CONDITIONS = {CONDITION_NEW, CONDITION_LIKE_NEW, CONDITION_EXCELLENT, CONDITION_GOOD, CONDITION_FAIR}
INTERCHANGEABLE_CONDITIONS = {CONDITION_NEW, CONDITION_LIKE_NEW}

# Translate existing condition_grades.internal_code values into canonical EPIC condition_codes.
SOLEA_TO_EPIC_CONDITION = {
    "new_with_box":    CONDITION_NEW,
    "new_without_box": CONDITION_NEW,
    "excellent":       CONDITION_LIKE_NEW,
    "very_good":       CONDITION_EXCELLENT,
    "good":            CONDITION_GOOD,
    "fair":            CONDITION_FAIR,
}


def map_solea_condition(internal_code: str) -> str:
    """Translate a Solea internal_code (e.g. 'new_with_box') to a canonical EPIC condition_code."""
    if internal_code is None:
        raise ValueError("internal_code is required")
    key = internal_code.strip().lower()
    if key not in SOLEA_TO_EPIC_CONDITION:
        raise ValueError(f"Unknown Solea internal_code: {internal_code!r}")
    return SOLEA_TO_EPIC_CONDITION[key]


def _slug(s: str) -> str:
    """Strip non-alphanumeric characters and uppercase."""
    return re.sub(r'[^A-Za-z0-9]+', '', s or '').upper()


def build_base(brand: str, model: str, style_code: str, gender: str, size: str, condition: str) -> str:
    parts = [_slug(brand), _slug(model), _slug(style_code),
             _slug(gender), _slug(size), condition.upper()]
    return "-".join(p for p in parts if p)


def generate_product_id(*, brand: str, model: str, style_code: str, gender: str,
                        size: str, condition: str, sku: str = None) -> str:
    """Return a deterministic Product ID from the given attributes.

    For interchangeable conditions (NEW, LIKE_NEW) the SKU is not part of the
    ID — all units of the same style share one Product row.  For non-interchangeable
    conditions (EXCELLENT, GOOD, FAIR) the SKU token is appended so each distinct
    unit gets its own Product row.
    """
    condition = condition.upper()
    if condition not in VALID_CONDITIONS:
        raise ValueError(
            f"Unknown condition: {condition!r}. Valid values: {sorted(VALID_CONDITIONS)}"
        )
    base = build_base(brand, model, style_code, gender, size, condition)
    if condition in INTERCHANGEABLE_CONDITIONS:
        return base
    if not sku:
        raise ValueError(
            f"sku is required for non-interchangeable condition {condition!r}"
        )
    return f"{base}-{_slug(sku)}"


def is_interchangeable(condition: str) -> bool:
    """Return True when multiple SKUs may share one Product row under this condition."""
    return condition.upper() in INTERCHANGEABLE_CONDITIONS
