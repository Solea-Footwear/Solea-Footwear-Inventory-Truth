"""Unit tests for src.services.product_id_service (EPIC 1 Ticket 1.2)."""
import pytest

from src.services.product_id_service import (
    CONDITION_NEW, CONDITION_LIKE_NEW, CONDITION_EXCELLENT, CONDITION_GOOD, CONDITION_FAIR,
    generate_product_id,
    is_interchangeable,
    map_solea_condition,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nike(condition, sku=None):
    return generate_product_id(
        brand="Nike", model="Air Jordan 1", style_code="555088",
        gender="Men", size="10", condition=condition, sku=sku,
    )


# ---------------------------------------------------------------------------
# Determinism / idempotency
# ---------------------------------------------------------------------------

def test_same_inputs_same_output():
    a = _nike("NEW")
    b = _nike("NEW")
    assert a == b


def test_new_canonical_format():
    pid = _nike("NEW")
    assert pid == "NIKE-AIRJORDAN1-555088-MEN-10-NEW"


# ---------------------------------------------------------------------------
# Interchangeable conditions — SKU ignored
# ---------------------------------------------------------------------------

def test_new_two_skus_same_product_id():
    assert _nike("NEW", sku="SKU-001") == _nike("NEW", sku="SKU-002")


def test_like_new_two_skus_same_product_id():
    assert _nike("LIKE_NEW", sku="SKU-001") == _nike("LIKE_NEW", sku="SKU-002")


# ---------------------------------------------------------------------------
# Non-interchangeable conditions — SKU in suffix
# ---------------------------------------------------------------------------

def test_excellent_two_skus_different_product_ids():
    a = _nike("EXCELLENT", sku="SKU-001")
    b = _nike("EXCELLENT", sku="SKU-002")
    assert a != b
    assert a.endswith("-SKU001")
    assert b.endswith("-SKU002")


def test_good_two_skus_different_product_ids():
    assert _nike("GOOD", sku="A") != _nike("GOOD", sku="B")


def test_fair_two_skus_different_product_ids():
    assert _nike("FAIR", sku="X") != _nike("FAIR", sku="Y")


# ---------------------------------------------------------------------------
# Missing SKU errors
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("condition", ["EXCELLENT", "GOOD", "FAIR"])
def test_missing_sku_raises(condition):
    with pytest.raises(ValueError, match="sku is required"):
        _nike(condition)


# ---------------------------------------------------------------------------
# Unknown condition
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["E1", "VERY_GOOD", "mint", ""])
def test_unknown_condition_raises(bad):
    with pytest.raises(ValueError, match="Unknown condition"):
        _nike(bad)


# ---------------------------------------------------------------------------
# is_interchangeable
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("condition,expected", [
    (CONDITION_NEW, True),
    (CONDITION_LIKE_NEW, True),
    (CONDITION_EXCELLENT, False),
    (CONDITION_GOOD, False),
    (CONDITION_FAIR, False),
])
def test_is_interchangeable(condition, expected):
    assert is_interchangeable(condition) is expected


def test_is_interchangeable_case_insensitive():
    assert is_interchangeable("new") is True
    assert is_interchangeable("excellent") is False


# ---------------------------------------------------------------------------
# Whitespace / case normalisation in slug
# ---------------------------------------------------------------------------

def test_brand_whitespace_normalised():
    a = generate_product_id(brand="nike ", model="Air Jordan 1", style_code="555088",
                             gender="Men", size="10", condition="NEW")
    b = generate_product_id(brand="NIKE", model="Air Jordan 1", style_code="555088",
                             gender="Men", size="10", condition="NEW")
    assert a == b


# ---------------------------------------------------------------------------
# map_solea_condition
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("solea,expected", [
    ("new_with_box", "NEW"),
    ("new_without_box", "NEW"),
    ("excellent", "LIKE_NEW"),
    ("very_good", "EXCELLENT"),
    ("good", "GOOD"),
    ("fair", "FAIR"),
])
def test_map_solea_condition(solea, expected):
    assert map_solea_condition(solea) == expected


def test_map_solea_condition_unknown_raises():
    with pytest.raises(ValueError, match="Unknown Solea internal_code"):
        map_solea_condition("garbage")


def test_map_solea_condition_none_raises():
    with pytest.raises(ValueError, match="internal_code is required"):
        map_solea_condition(None)


def test_map_solea_condition_case_insensitive():
    assert map_solea_condition("NEW_WITH_BOX") == "NEW"
    assert map_solea_condition("  Very_Good  ") == "EXCELLENT"
