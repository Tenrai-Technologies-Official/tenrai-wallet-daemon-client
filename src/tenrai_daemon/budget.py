"""Exact daemon fee ceilings."""

from decimal import Decimal, InvalidOperation

from .amounts import from_atoms


def fee_cap_text(amount: str | Decimal) -> str:
    if not isinstance(amount, (str, Decimal)):
        raise TypeError("max_fee must be a TRAI string or Decimal, never a float")
    try:
        value = Decimal(amount)
    except InvalidOperation:
        raise ValueError("invalid max_fee") from None
    if not value.is_finite() or value < 0:
        raise ValueError("max_fee must be finite and nonnegative")
    # Keep the caller's Decimal precision from rounding the fee cap.
    _, digits, exponent = value.as_tuple()
    shift = exponent + 8
    raw = "".join(map(str, digits))
    if shift < 0:
        removed = -shift
        if removed > len(raw):
            if any(digits):
                raise ValueError("max_fee has more than 8 decimals")
            raw = "0"
        else:
            if any(char != "0" for char in raw[-removed:]):
                raise ValueError("max_fee has more than 8 decimals")
            raw = raw[:-removed] or "0"
    elif any(digits):
        if len(raw) + shift > 39:
            raise ValueError("max_fee exceeds u128 atom capacity")
        raw += "0" * shift
    atoms = int(raw)
    if atoms > (1 << 128) - 1:
        raise ValueError("max_fee exceeds u128 atom capacity")
    return from_atoms(atoms)
