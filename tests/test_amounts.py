from decimal import Decimal, localcontext

import pytest

from tenrai_daemon import amount_text, from_atoms, to_atoms


def test_amounts_convert_exactly_both_ways():
    assert to_atoms("1.25") == 125_000_000
    assert to_atoms(Decimal("0.00000001")) == 1
    assert to_atoms(" 200000000 ") == 20_000_000_000_000_000
    assert from_atoms(125_000_000) == "1.25000000"
    assert from_atoms(0) == "0.00000000"
    assert amount_text(Decimal("0.1")) == "0.10000000"


def test_amounts_ignore_the_callers_decimal_precision():
    with localcontext(prec=3):
        assert to_atoms("123.45678901") == 12_345_678_901
        assert amount_text(Decimal("1.250000000")) == "1.25000000"
        with pytest.raises(ValueError, match="8 decimal places"):
            to_atoms("1.000000001")
        with pytest.raises(ValueError, match="8 decimal places"):
            to_atoms("1e-10000000")


@pytest.mark.parametrize("value", ["1.123456789", "-1", "abc", "NaN", "Infinity", ""])
def test_anything_but_a_plain_amount_is_refused(value):
    with pytest.raises(ValueError):
        to_atoms(value)


def test_floats_never_become_amounts():
    with pytest.raises(TypeError):
        to_atoms(0.1)


def test_a_payment_needs_more_than_nothing():
    with pytest.raises(ValueError):
        amount_text("0")
