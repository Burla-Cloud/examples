"""parse_price gate. Stage 1 must not run until this passes."""
from __future__ import annotations

import math

import pytest

from src.lib.inside_airbnb import parse_price


@pytest.mark.parametrize("raw,expected", [
    ("$1,250.00", 1250.00),
    ("$95.00", 95.00),
    ("$1,250", 1250.00),
    ("$95", 95.00),
    ("1,250.00", 1250.00),
    ("1250", 1250.00),
    (" $95.00 ", 95.00),
    ("$0.00", 0.00),
    ("$10,000.00", 10000.00),
    ("$1,234,567.89", 1234567.89),
])
def test_parse_price_canonical_formats(raw, expected):
    assert parse_price(raw) == pytest.approx(expected)


@pytest.mark.parametrize("raw", ["", "null", "None", "nan", None, "abc", "$"])
def test_parse_price_returns_none_on_garbage(raw):
    assert parse_price(raw) is None


def test_parse_price_passes_through_floats():
    assert parse_price(95.0) == 95.0
    assert parse_price(1250) == 1250.0


def test_parse_price_handles_nan_float():
    assert parse_price(float("nan")) is None


@pytest.mark.parametrize("raw,expected", [
    ("\u20ac95.00", 95.00),
    ("\u00a372.00", 72.00),
    ("\u00a510000", 10000.00),
])
def test_parse_price_strips_known_currency_symbols(raw, expected):
    assert parse_price(raw) == pytest.approx(expected)
